"""ICD-9-CM (diagnosis) extractor: CMS legacy ZIPs -> per-FY silver parquet.

For each fiscal year in scope (FY2008-FY2015) this module:

    1. Downloads the per-FY CMS "Full and Abbreviated Code Titles" ZIP into
       ``data/bronze/icd9cm/fy{fy}/{download_date}/`` via
       :mod:`src.extract._http`. (For FY2010 a small supplemental ZIP carrying
       long titles is fetched alongside the v27 short-titles ZIP.)

    2. Parses the diagnosis-side fixed-width text files inside the ZIP.
       File-naming and contents differ across the era:

         - FY2008-FY2009 (v25, v26): a single short-titles file per system
           (``I9diagnosesV25.txt`` / ``V26 I-9 Diagnosis.txt``). No long
           titles available upstream — long ``description`` falls back to the
           short title.
         - FY2010 (v27): short-titles ZIP (``CMS27_DESC_SHORT_DX.txt``) plus
           supplemental ``fy2010diagnosis-procedurecodesfulltitles.zip``
           supplying long titles via CSV (``V27LONG_SHORT_DX_*.csv``).
         - FY2011-FY2015 (v28-v32): paired ``CMSnn_DESC_LONG_DX.txt`` and
           ``CMSnn_DESC_SHORT_DX.txt`` (whitespace-delimited, code + title).

    3. Writes a parquet matching the locked Phase-1 schema (see
       ``src.transform.codes_master._SCHEMA``) to
       ``data/silver/per_source/icd9cm_fy{fy}.parquet``.

CLI::

    python -m src.extract.icd9cm --fy 2014
    python -m src.extract.icd9cm --fy-range 2008-2015

ICD-9 freeze (per plan §3): ICD-9-CM was frozen for the ICD-10 transition
starting Oct 1, 2011. CMS still ships per-FY ZIPs for the frozen era; the same
codes repeat across FY2012-FY2015 and the codes_master union dedupes on the
``(code, code_system, fy_or_cy)`` triple.

ICD-9 has no machine-readable POA-exempt artifact on the CMS archive page (only
ICD-10 POA-exempt is published there); ``poa_exempt`` is emitted NULL on every
row. ``chapter`` is also NULL (ICD-9 has no machine-readable chapter list).
``addendum_action`` is NULL — the era's addenda are PDFs only.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "icd9cm"
_CODE_SYSTEM: str = "ICD-9-CM"

# Per-FY URL manifest is sourced from ``config.ICD9CM_SOURCES`` (primary ZIP
# per FY) plus ``config.ICD9CM_FILENAME_OVERRIDES`` (optional supplementary
# ZIPs — currently only FY2010's ``long_supplement`` long-titles ZIP).
#
# This module exposes ``PER_FY_URLS`` as a ``{fy: {key: url}}`` view so the
# sibling :mod:`src.extract.icd9pcs` module can iterate over the per-FY URL
# set without duplicating the config-merge logic.


def _build_per_fy_urls() -> dict[int, dict[str, str]]:
    """Compose the per-FY URL manifest from config.

    Each FY entry has at minimum a ``primary`` key (the master code-titles
    ZIP). FYs registered in :data:`config.ICD9CM_FILENAME_OVERRIDES` may also
    carry a ``long_supplement`` key (FY2010 only as of 2026-04-29).
    """
    out: dict[str, dict[str, str]] = {}
    for fy, primary_url in config.ICD9CM_SOURCES.items():
        entry: dict[str, str] = {"primary": primary_url}
        overrides = config.ICD9CM_FILENAME_OVERRIDES.get(fy) or {}
        long_supp = overrides.get("long_supplement")
        if isinstance(long_supp, str):
            entry["long_supplement"] = long_supp
        out[fy] = entry
    return out


_PER_FY_URLS: dict[int, dict[str, str]] = _build_per_fy_urls()


# Filename predicates used to locate the right member inside each per-FY ZIP.
# Each predicate receives a lowercased basename and returns True for a match.
def _is_dx_long_v28plus(name_lower: str) -> bool:
    """Match e.g. ``cms28_desc_long_dx.txt`` (v28-v32 long DX)."""
    return (
        name_lower.endswith(".txt")
        and "_long_dx" in name_lower
        and "short" not in name_lower
    )


def _is_dx_short_v28plus(name_lower: str) -> bool:
    """Match e.g. ``cms28_desc_short_dx.txt`` (v28-v32 short DX)."""
    return (
        name_lower.endswith(".txt")
        and "_short_dx" in name_lower
        and "long" not in name_lower
    )


def _is_sg_long_v28plus(name_lower: str) -> bool:
    """Match e.g. ``cms28_desc_long_sg.txt`` (v28-v32 long SG/Vol3)."""
    return (
        name_lower.endswith(".txt")
        and "_long_sg" in name_lower
        and "short" not in name_lower
    )


def _is_sg_short_v28plus(name_lower: str) -> bool:
    """Match e.g. ``cms28_desc_short_sg.txt`` (v28-v32 short SG/Vol3)."""
    return (
        name_lower.endswith(".txt")
        and "_short_sg" in name_lower
        and "long" not in name_lower
    )


def _is_dx_v25plus_only(name_lower: str) -> bool:
    """Match v25/v26/v27 single DX file (e.g. ``i9diagnosesv25.txt``,
    ``v26 i-9 diagnosis.txt``, ``cms27_desc_short_dx.txt``)."""
    if not name_lower.endswith(".txt"):
        return False
    if "diagnos" in name_lower or "_dx" in name_lower:
        # Reject any v28+ long file (they also contain "_dx").
        return "long" not in name_lower
    return False


def _is_sg_v25plus_only(name_lower: str) -> bool:
    """Match v25/v26/v27 single SG/procedure file."""
    if not name_lower.endswith(".txt"):
        return False
    if "procedure" in name_lower or "_sg" in name_lower:
        return "long" not in name_lower
    return False


def _is_dx_csv_long_short(name_lower: str) -> bool:
    """Match the FY2010 long+short DX CSV (``v27long_short_dx_*.csv``)."""
    return name_lower.endswith(".csv") and "long_short_dx" in name_lower


def _is_sg_csv_long_short(name_lower: str) -> bool:
    """Match the FY2010 long+short SG CSV (``cms27_desc_long_short_sg_*.csv``)."""
    return name_lower.endswith(".csv") and "long_short_sg" in name_lower


# ICD-9-CM code shape: 3-5 digit numeric, optionally prefixed by V or E.
# Codes here are dotless (CMS files publish them without the dot).
_ICD9CM_CODE_RE = re.compile(r"^[VE]?[0-9]{3,5}$")
_ICD9SG_CODE_RE = re.compile(r"^[0-9]{3,4}$")


@dataclass(frozen=True)
class _ParsedRow:
    code: str
    description: str
    short_description: str | None


# ---------------------------------------------------------------------------
# ZIP helpers
# ---------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"icd9cm: corrupt ZIP at {path}: {exc}") from exc


def _find_member(zf: zipfile.ZipFile, predicate) -> str | None:
    """Return the first member whose lowercased basename satisfies ``predicate``."""
    for name in zf.namelist():
        if predicate(Path(name).name.lower()):
            return name
    return None


def _read_member_bytes(zf: zipfile.ZipFile, member: str) -> bytes:
    with zf.open(member, "r") as fh:
        return fh.read()


def _decode_text(raw: bytes) -> str:
    """Decode CMS text artifacts. They are typically Latin-1 / Windows-1252."""
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_whitespace_titles(text: str, *, code_re: re.Pattern[str]) -> dict[str, str]:
    """Parse a whitespace-delimited titles file -> ``{code: title}``.

    Each row: ``<code><whitespace><title>`` where the title may contain spaces
    but the code is a single token. Lines whose code-token doesn't match
    ``code_re`` (e.g. blanks, headers) are skipped silently.
    """
    out: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue
        code = parts[0].strip().upper().replace(".", "")
        if not code_re.match(code):
            continue
        out[code] = parts[1].rstrip()
    return out


def _parse_long_short_csv(text: str, *, code_re: re.Pattern[str]) -> dict[str, dict[str, str]]:
    """Parse the FY2010 ``V27LONG_SHORT_DX_*.csv`` (or SG variant) file.

    Header row: ``DIAGNOSIS CODE,LONG DESCRIPTION,SHORT DESCRIPTION``
    (or ``PROCEDURE CODE,...``). Returns ``{code: {long, short}}``.
    """
    out: dict[str, dict[str, str]] = {}
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return out
    header = [h.strip().lower() for h in rows[0]]
    # Header layout is fixed by CMS: code, long, short — but be defensive.
    try:
        code_idx = next(i for i, h in enumerate(header) if "code" in h)
        long_idx = next(i for i, h in enumerate(header) if "long" in h)
        short_idx = next(i for i, h in enumerate(header) if "short" in h)
    except StopIteration:
        # Header not recognized; fall back to positional [0, 1, 2].
        code_idx, long_idx, short_idx = 0, 1, 2
    for row in rows[1:]:
        if len(row) <= max(code_idx, long_idx, short_idx):
            continue
        code = row[code_idx].strip().upper().replace(".", "")
        if not code_re.match(code):
            continue
        long_desc = row[long_idx].strip()
        short_desc = row[short_idx].strip()
        out[code] = {"long": long_desc, "short": short_desc}
    return out


# ---------------------------------------------------------------------------
# Per-ZIP extraction by era
# ---------------------------------------------------------------------------


def _rows_from_v25_v26_zip(
    zip_path: Path, *, want: str
) -> list[_ParsedRow]:
    """Extract DX or SG rows from a v25/v26 ZIP (short-titles only).

    ``want`` is ``"dx"`` or ``"sg"``. Long ``description`` falls back to the
    short title since CMS did not publish long titles for these vintages.
    """
    is_match = _is_dx_v25plus_only if want == "dx" else _is_sg_v25plus_only
    code_re = _ICD9CM_CODE_RE if want == "dx" else _ICD9SG_CODE_RE
    with _open_zip(zip_path) as zf:
        member = _find_member(zf, is_match)
        if member is None:
            raise RuntimeError(
                f"icd9cm: ZIP {zip_path} has no {want.upper()} short-titles file; "
                f"members were: {zf.namelist()}"
            )
        text = _decode_text(_read_member_bytes(zf, member))
    short_map = _parse_whitespace_titles(text, code_re=code_re)
    return [
        _ParsedRow(code=code, description=short, short_description=short)
        for code, short in sorted(short_map.items())
    ]


def _rows_from_v27_zips(
    primary_zip: Path,
    long_supplement_zip: Path | None,
    *,
    want: str,
) -> list[_ParsedRow]:
    """Extract DX or SG rows for FY2010 (v27).

    The primary ZIP carries only short titles; long titles arrive via the
    optional supplemental ZIP's CSV. If the supplement is missing, long
    description falls back to the short title.
    """
    is_short_match = _is_dx_v25plus_only if want == "dx" else _is_sg_v25plus_only
    is_csv_match = _is_dx_csv_long_short if want == "dx" else _is_sg_csv_long_short
    code_re = _ICD9CM_CODE_RE if want == "dx" else _ICD9SG_CODE_RE

    short_map: dict[str, str] = {}
    with _open_zip(primary_zip) as zf:
        member = _find_member(zf, is_short_match)
        if member is None:
            raise RuntimeError(
                f"icd9cm: v27 primary ZIP {primary_zip} has no {want.upper()} short file"
            )
        short_map = _parse_whitespace_titles(
            _decode_text(_read_member_bytes(zf, member)),
            code_re=code_re,
        )

    long_map: dict[str, str] = {}
    if long_supplement_zip is not None:
        with _open_zip(long_supplement_zip) as zf:
            member = _find_member(zf, is_csv_match)
            if member is None:
                # Tolerate; supplement may not carry the side we want.
                pass
            else:
                rows = _parse_long_short_csv(
                    _decode_text(_read_member_bytes(zf, member)),
                    code_re=code_re,
                )
                for code, descs in rows.items():
                    if descs.get("long"):
                        long_map[code] = descs["long"]
                    if not short_map.get(code) and descs.get("short"):
                        short_map[code] = descs["short"]

    out: list[_ParsedRow] = []
    for code in sorted(short_map.keys() | long_map.keys()):
        short = short_map.get(code) or None
        long_desc = long_map.get(code) or short or ""
        out.append(_ParsedRow(code=code, description=long_desc, short_description=short))
    return out


def _rows_from_v28plus_zip(
    zip_path: Path, *, want: str
) -> list[_ParsedRow]:
    """Extract DX or SG rows from a v28-v32 master-descriptions ZIP.

    These ZIPs carry separate ``CMSnn_DESC_LONG_*`` and ``CMSnn_DESC_SHORT_*``
    files for both diagnoses and procedures. We join on ``code``.
    """
    if want == "dx":
        long_match = _is_dx_long_v28plus
        short_match = _is_dx_short_v28plus
        code_re = _ICD9CM_CODE_RE
    else:
        long_match = _is_sg_long_v28plus
        short_match = _is_sg_short_v28plus
        code_re = _ICD9SG_CODE_RE

    with _open_zip(zip_path) as zf:
        long_member = _find_member(zf, long_match)
        short_member = _find_member(zf, short_match)
        if long_member is None and short_member is None:
            raise RuntimeError(
                f"icd9cm: ZIP {zip_path} has no {want.upper()} long or short txt; "
                f"members were: {zf.namelist()}"
            )
        long_text = _decode_text(_read_member_bytes(zf, long_member)) if long_member else ""
        short_text = _decode_text(_read_member_bytes(zf, short_member)) if short_member else ""

    long_map = _parse_whitespace_titles(long_text, code_re=code_re) if long_text else {}
    short_map = _parse_whitespace_titles(short_text, code_re=code_re) if short_text else {}

    out: list[_ParsedRow] = []
    for code in sorted(long_map.keys() | short_map.keys()):
        long_desc = long_map.get(code) or short_map.get(code) or ""
        short_desc = short_map.get(code) or None
        out.append(_ParsedRow(code=code, description=long_desc, short_description=short_desc))
    return out


def _extract_rows_for_fy(
    fy: int,
    *,
    want: str,
    bronze_paths: dict[str, Path],
) -> tuple[list[_ParsedRow], str]:
    """Dispatch to the right per-era parser.

    Returns ``(rows, source_url)``. ``source_url`` is the primary ZIP URL,
    used as the parquet's ``source`` column.
    """
    primary_path = bronze_paths["primary"]
    long_supp_path = bronze_paths.get("long_supplement")
    if fy in (2008, 2009):
        rows = _rows_from_v25_v26_zip(primary_path, want=want)
    elif fy == 2010:
        rows = _rows_from_v27_zips(primary_path, long_supp_path, want=want)
    elif 2011 <= fy <= 2015:
        rows = _rows_from_v28plus_zip(primary_path, want=want)
    else:
        raise ValueError(f"icd9cm: FY{fy} not in supported range 2008-2015")
    source_url = _PER_FY_URLS[fy]["primary"]
    return rows, source_url


# ---------------------------------------------------------------------------
# Bronze download
# ---------------------------------------------------------------------------


def _download_bronze(
    fy: int,
    *,
    session: requests.Session,
    download_date: str | None,
) -> dict[str, Path]:
    """Download every per-FY ZIP into bronze. Returns ``{key: local_path}``."""
    if fy not in _PER_FY_URLS:
        raise KeyError(
            f"icd9cm: FY{fy} not in supported range; known FYs: "
            f"{sorted(_PER_FY_URLS)}"
        )
    out: dict[str, Path] = {}
    for key, url in _PER_FY_URLS[fy].items():
        filename = Path(url).name
        try:
            path = _http.download_to_bronze(
                url,
                source=_BRONZE_SOURCE,
                fy=fy,
                filename=filename,
                download_date=download_date,
                session=session,
                period="fy",
            )
        except requests.HTTPError as exc:
            raise RuntimeError(f"icd9cm fy{fy}: {url} failed: {exc}") from exc
        out[key] = path
    return out


# ---------------------------------------------------------------------------
# Parquet emission
# ---------------------------------------------------------------------------


# Locked Phase-1 schema (mirrors src.transform.codes_master._SCHEMA exactly).
_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("code_system", pa.string(), nullable=False),
        pa.field("fy_or_cy", pa.int32(), nullable=False),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("short_description", pa.string(), nullable=True),
        pa.field("chapter", pa.string(), nullable=True),
        pa.field("addendum_action", pa.string(), nullable=True),
        pa.field("or_procedure_flag", pa.bool_(), nullable=True),
        pa.field("mcc_flag", pa.bool_(), nullable=True),
        pa.field("cc_flag", pa.bool_(), nullable=True),
        pa.field("poa_exempt", pa.bool_(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("n", pa.int32(), nullable=True),
    ]
)


def _build_table(
    rows: list[_ParsedRow],
    *,
    fy: int,
    code_system: str,
    source_url: str,
    fy_start: date,
    fy_end: date,
) -> pa.Table:
    n = len(rows)
    if n == 0:
        raise RuntimeError(
            f"icd9cm: zero rows for FY{fy} ({code_system}); parser regression"
        )
    return pa.Table.from_arrays(
        [
            pa.array([r.code for r in rows], type=pa.string()),
            pa.array([code_system] * n, type=pa.string()),
            pa.array([fy] * n, type=pa.int32()),
            pa.array([fy_start] * n, type=pa.date32()),
            pa.array([fy_end] * n, type=pa.date32()),
            pa.array([r.description for r in rows], type=pa.string()),
            pa.array([r.short_description for r in rows], type=pa.string()),
            pa.array([None] * n, type=pa.string()),  # chapter
            pa.array([None] * n, type=pa.string()),  # addendum_action
            pa.array([None] * n, type=pa.bool_()),  # or_procedure_flag
            pa.array([None] * n, type=pa.bool_()),  # mcc_flag
            pa.array([None] * n, type=pa.bool_()),  # cc_flag
            pa.array([None] * n, type=pa.bool_()),  # poa_exempt
            pa.array([source_url] * n, type=pa.string()),
            pa.array([None] * n, type=pa.int32()),  # n (Phase 5)
        ],
        schema=_PARQUET_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_fy(
    fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract ICD-9-CM diagnosis codes for one FY -> per-source parquet.

    Sibling extractor :mod:`src.extract.icd9pcs` consumes the same bronze ZIPs
    via the public helpers below to emit the procedure-side (Vol3) parquet.
    """
    sess = session or _http.make_session()
    bronze = _download_bronze(fy, session=sess, download_date=download_date)
    rows, source_url = _extract_rows_for_fy(fy, want="dx", bronze_paths=bronze)

    fy_start, fy_end = config.fy_interval(fy)
    table = _build_table(
        rows,
        fy=fy,
        code_system=_CODE_SYSTEM,
        source_url=source_url,
        fy_start=fy_start,
        fy_end=fy_end,
    )

    out_dir = output_dir if output_dir is not None else config.SILVER_PER_SOURCE
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"icd9cm_fy{fy}.parquet"
    pq.write_table(table, out_path)
    print(
        f"[icd9cm] fy{fy}: wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_fy`` over inclusive ``[start_fy, end_fy]``.

    Per-FY failures are logged and skipped so one bad FY doesn't abort the rest.
    Returns the list of successfully written parquet paths.
    """
    if end_fy < start_fy:
        raise ValueError(f"end_fy ({end_fy}) must be >= start_fy ({start_fy})")
    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for fy in range(start_fy, end_fy + 1):
        try:
            out.append(extract_fy(fy, session=sess, download_date=download_date))
        except Exception as exc:  # noqa: BLE001
            print(
                f"[icd9cm] FY{fy} FAILED: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((fy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[icd9cm] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for fy, msg in failures:
            print(f"  FY{fy}: {msg}", file=sys.stderr)
    print(
        f"[icd9cm] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
        file=sys.stderr,
    )
    return out


# Re-exported helpers used by :mod:`src.extract.icd9pcs` to avoid duplicating
# the per-era dispatch logic. Keep these public.
PER_FY_URLS = _PER_FY_URLS


def download_bronze(
    fy: int,
    *,
    session: requests.Session,
    download_date: str | None = None,
) -> dict[str, Path]:
    """Public wrapper for bronze download (used by sibling icd9pcs extractor)."""
    return _download_bronze(fy, session=session, download_date=download_date)


def extract_rows(
    fy: int,
    *,
    want: str,
    bronze_paths: dict[str, Path],
) -> tuple[list[_ParsedRow], str]:
    """Public wrapper around the per-era dispatch (used by icd9pcs)."""
    return _extract_rows_for_fy(fy, want=want, bronze_paths=bronze_paths)


def build_table(
    rows: list[_ParsedRow],
    *,
    fy: int,
    code_system: str,
    source_url: str,
) -> pa.Table:
    """Public wrapper around table assembly (used by icd9pcs)."""
    fy_start, fy_end = config.fy_interval(fy)
    return _build_table(
        rows,
        fy=fy,
        code_system=code_system,
        source_url=source_url,
        fy_start=fy_start,
        fy_end=fy_end,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_fy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range expects 'START-END' (e.g., 2008-2015), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.icd9cm",
        description=(
            "Download and parse CMS ICD-9-CM (diagnosis) code-title ZIPs for "
            "one or more fiscal years; emits "
            "data/silver/per_source/icd9cm_fy{FY}.parquet."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fy", type=int, help="Single fiscal year (e.g., 2014).")
    group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        metavar="START-END",
        help="Inclusive FY range, e.g., '2008-2015'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    session = _http.make_session()
    if args.fy is not None:
        extract_fy(args.fy, session=session, download_date=args.download_date)
    else:
        start_fy, end_fy = args.fy_range
        extract_fy_range(
            start_fy, end_fy, session=session, download_date=args.download_date
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
