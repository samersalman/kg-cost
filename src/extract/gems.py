"""GEMs (General Equivalence Mappings) extractor: frozen FY2018 release.

The GEMs program produced four bidirectional ICD-9 <-> ICD-10 mapping files
plus two Reimbursement Mapping files. CMS froze the program in FY2018; this
module is single-shot (no FY iteration) and emits one parquet at
``data/silver/gems_2018.parquet`` (NOTE: silver root, NOT
``silver/per_source/`` — see plan-v2.md design decision #1; GEMs is a
*relation* table, distinct from the codes_master schema).

Source layout (verified 2026-04-29):
    The CMS archive ships two per-system ZIPs (CM and PCS), not one combined
    archive. The Reimbursement Mappings ship as two separate ZIPs (DX and PR)
    last published for FY2016 — that is the most-recent published RM release
    and is the canonical RM artifact for FY2018-era work.

    CM ZIP   : ``2018-icd-10-cm-general-equivalence-mappings.zip``
        - ``2018_I9gem.txt``  : ICD-9-CM   -> ICD-10-CM (forward)
        - ``2018_I10gem.txt`` : ICD-10-CM  -> ICD-9-CM  (backward)
    PCS ZIP  : ``2018-icd-10-pcs-general-equivalence-mappings.zip``
        - ``gem_i9pcs.txt``   : ICD-9-Vol3 -> ICD-10-PCS (forward)
        - ``gem_pcsi9.txt``   : ICD-10-PCS -> ICD-9-Vol3 (backward)
    RM DX    : ``reimbursement_mapping_dx_2016.zip``
        - ``reimb_map_dx_2016.txt`` : ICD-10-CM -> ICD-9-CM (cluster format)
    RM PR    : ``reimbursement_mapping_pr_2016.zip``
        - ``reimb_map_pr_2016.txt`` : ICD-10-PCS -> ICD-9-Vol3 (cluster format)

GEMs row format (whitespace-delimited, three columns):
    <source_code> <target_code> <5-char-flag-string>

Flag bits (1-indexed, fixed-position digits in a 5-char string):
    1: approximate (0|1)
    2: no_map      (0|1)   — when 1, target is the literal "NoDx" or "NoPCS"
    3: combination (0|1)
    4: scenario    (0-9)
    5: choice_list (0-9)

RM row format (whitespace-delimited, variable columns):
    <icd10_source> <count> <icd9_target_1> [<icd9_target_2> ...]

    where ``count`` indicates the cluster size (1-4 in the FY2016 release).
    Each (icd10_source, icd9_target_i) pair becomes one output row with
    is_reimbursement=True. Flag fields are NULL on RM rows since the RM
    files don't carry GEMs flags.

Output schema (DISTINCT from codes_master._SCHEMA — relation, not master):
    source_code        VARCHAR
    source_system      VARCHAR  (ICD-9-CM | ICD-10-CM | ICD-9-Vol3 | ICD-10-PCS)
    target_code        VARCHAR
    target_system      VARCHAR
    approximate_flag   BOOLEAN
    no_map_flag        BOOLEAN
    combination_flag   BOOLEAN
    scenario           INTEGER
    choice_list        INTEGER
    is_reimbursement   BOOLEAN
    effective_start    DATE      (2018-10-01 — frozen)
    effective_end      DATE      (NULL)
    source             VARCHAR   (URL of the originating ZIP)

CLI::

    python -m src.extract.gems
    python -m src.extract.gems --download-date 2026-05-01

Conventions (CLAUDE.md):
    - All codes upper-case, dotless. CDC/CMS publish dotless to begin with.
    - Bronze writes are idempotent via ``_http.download_to_bronze``.
    - URLs centralized below as module constants (config.GEMS_SOURCE_URL has
      the wrong filename — see session2-notes-gems.md for the URL fix the
      next phase agent should apply).
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract._http import download_to_bronze, make_session

_SOURCE_NAME: str = "gems"
_FY: int = 2018
_EFFECTIVE_START: date = date(2018, 10, 1)
_EFFECTIVE_END: date | None = None  # frozen — no end


# ---------------------------------------------------------------------------
# URLs and ZIP-internal layout — sourced from config.GEMS_SOURCES and
# config.GEMS_FILES (verified 2026-04-29 against
# https://www.cms.gov/medicare/coding-billing/icd-10-codes/icd-10-cm-icd-10-pcs-gem-archive).
#
# CMS publishes the GEMs as four separate ZIPs: per-system GEMs (CM/PCS) plus
# per-system Reimbursement Mappings (DX/PR, last released FY2016 — the
# most-recent RM publication).
# ---------------------------------------------------------------------------


def _gems_layout() -> dict[str, list[tuple[str, str, str, bool, bool]]]:
    """Build the per-URL parse plan from ``config.GEMS_SOURCES`` / ``GEMS_FILES``.

    Returns a dict mapping each source URL to a list of
    ``(member_filename, source_system, target_system, is_reimbursement,
    is_rm_format)`` tuples. ``is_rm_format=True`` selects the cluster-format
    RM parser; otherwise the 3-column GEMs parser is used.
    """
    cm_url = config.GEMS_SOURCES["cm"]
    pcs_url = config.GEMS_SOURCES["pcs"]
    rm_dx_url = config.GEMS_SOURCES["rm_dx"]
    rm_pr_url = config.GEMS_SOURCES["rm_pr"]
    return {
        cm_url: [
            (config.GEMS_FILES[("cm", "i9_to_i10")], "ICD-9-CM", "ICD-10-CM", False, False),
            (config.GEMS_FILES[("cm", "i10_to_i9")], "ICD-10-CM", "ICD-9-CM", False, False),
        ],
        pcs_url: [
            (config.GEMS_FILES[("pcs", "i9_to_i10")], "ICD-9-Vol3", "ICD-10-PCS", False, False),
            (config.GEMS_FILES[("pcs", "i10_to_i9")], "ICD-10-PCS", "ICD-9-Vol3", False, False),
        ],
        rm_dx_url: [
            (config.GEMS_FILES[("rm_dx", "rm")], "ICD-10-CM", "ICD-9-CM", True, True),
        ],
        rm_pr_url: [
            (config.GEMS_FILES[("rm_pr", "rm")], "ICD-10-PCS", "ICD-9-Vol3", True, True),
        ],
    }


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        ("source_code", pa.string()),
        ("source_system", pa.string()),
        ("target_code", pa.string()),
        ("target_system", pa.string()),
        ("approximate_flag", pa.bool_()),
        ("no_map_flag", pa.bool_()),
        ("combination_flag", pa.bool_()),
        ("scenario", pa.int32()),
        ("choice_list", pa.int32()),
        ("is_reimbursement", pa.bool_()),
        ("effective_start", pa.date32()),
        ("effective_end", pa.date32()),
        ("source", pa.string()),
    ]
)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _local_filename(url: str) -> str:
    """Return the on-disk filename for a bronze write."""
    return url.rsplit("/", 1)[-1]


def _safe_download(
    url: str,
    *,
    session: requests.Session,
    download_date: str | None,
) -> Path:
    """Wrap ``download_to_bronze`` so HTTP errors carry GEMs context."""
    try:
        return download_to_bronze(
            url,
            _SOURCE_NAME,
            _FY,
            _local_filename(url),
            download_date=download_date,
            session=session,
            period="fy",
        )
    except requests.HTTPError as exc:
        raise RuntimeError(f"gems fy{_FY}: {url} failed: {exc}") from exc


def _download_all(
    *,
    session: requests.Session,
    download_date: str | None,
) -> dict[str, Path]:
    """Download all four GEMs/RM ZIPs into bronze. Returns ``{url: local_path}``."""
    paths: dict[str, Path] = {}
    for url in _gems_layout():
        paths[url] = _safe_download(
            url, session=session, download_date=download_date
        )
    return paths


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _read_member_bytes(zip_path: Path, member_name: str) -> bytes:
    """Return raw bytes of a named member inside ``zip_path``.

    Case-insensitive on the filename basename (CMS occasionally re-cases between
    publications). Raises ``RuntimeError`` if no matching member is found.
    """
    target = member_name.lower()
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            if info.filename.lower().endswith("/" + target) or info.filename.lower() == target:
                with zf.open(info) as fh:
                    return fh.read()
        # Fallback: substring match on the basename (some CMS publications add
        # an extra prefix or suffix).
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = info.filename.rsplit("/", 1)[-1].lower()
            if target in base:
                with zf.open(info) as fh:
                    return fh.read()
    raise RuntimeError(
        f"gems fy{_FY}: member {member_name!r} not found in {zip_path}"
    )


def _decode_text(raw: bytes) -> str:
    """Decode CMS GEMs flat files. ASCII, but tolerate latin-1 / cp1252."""
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


# A GEMs target of "NoDx" or "NoPCS" means "no map" — we keep the row but null
# the target_code (the no_map_flag bit is the truth signal).
_NO_MAP_TARGETS: frozenset[str] = frozenset({"NODX", "NOPCS"})

# Code shape: dotless alphanumeric. ICD-9 codes can include leading zeros; E and
# V prefixes are valid for ICD-9-CM.
_CODE_RE = re.compile(r"^[A-Z0-9]+$")


def _parse_gems_file(
    raw: bytes,
    *,
    source_system: str,
    target_system: str,
) -> list[dict]:
    """Parse a GEMs flat file into a list of normalized row dicts.

    Format (whitespace-delimited):
        <source_code> <target_code> <5-char-flag-string>

    Skips blank lines and any header lines (none observed in FY2018 release).
    """
    text = _decode_text(raw)
    out: list[dict] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        parts = stripped.split()
        if len(parts) != 3:
            # Defensive: skip malformed lines but surface a hint via stderr.
            print(
                f"[gems] skipping malformed line in {source_system}->{target_system}: "
                f"{raw_line!r}",
                file=sys.stderr,
            )
            continue
        src, tgt, flags = parts
        if len(flags) != 5 or not flags.isdigit():
            print(
                f"[gems] skipping malformed flag in {source_system}->{target_system}: "
                f"{raw_line!r}",
                file=sys.stderr,
            )
            continue
        src_code = src.upper().replace(".", "")
        tgt_upper = tgt.upper().replace(".", "")
        no_map = flags[1] == "1"
        target_code: str | None
        if tgt_upper in _NO_MAP_TARGETS or no_map:
            target_code = None
        else:
            target_code = tgt_upper
        out.append(
            {
                "source_code": src_code,
                "source_system": source_system,
                "target_code": target_code,
                "target_system": target_system,
                "approximate_flag": flags[0] == "1",
                "no_map_flag": no_map,
                "combination_flag": flags[2] == "1",
                "scenario": int(flags[3]),
                "choice_list": int(flags[4]),
                "is_reimbursement": False,
            }
        )
    return out


def _parse_rm_file(
    raw: bytes,
    *,
    source_system: str,
    target_system: str,
) -> list[dict]:
    """Parse a Reimbursement Mapping flat file into normalized row dicts.

    Format (whitespace-delimited, variable columns):
        <icd10_source> <count> <icd9_target_1> [<icd9_target_2> ...]

    where ``count`` (1-4) is the cluster size. We expand each cluster into N
    rows (one per icd9 target) so downstream queries can join 1:1. RM rows
    carry no GEMs flag bits — those columns are NULL.
    """
    text = _decode_text(raw)
    out: list[dict] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        parts = stripped.split()
        if len(parts) < 3:
            print(
                f"[gems] skipping malformed RM line in {source_system}->{target_system}: "
                f"{raw_line!r}",
                file=sys.stderr,
            )
            continue
        src = parts[0]
        try:
            count = int(parts[1])
        except ValueError:
            print(
                f"[gems] skipping non-integer cluster count in RM "
                f"{source_system}->{target_system}: {raw_line!r}",
                file=sys.stderr,
            )
            continue
        targets = parts[2 : 2 + count]
        if len(targets) != count:
            print(
                f"[gems] RM cluster underflow ({len(targets)}/{count}) in "
                f"{source_system}->{target_system}: {raw_line!r}",
                file=sys.stderr,
            )
            continue
        src_code = src.upper().replace(".", "")
        for tgt in targets:
            tgt_code = tgt.upper().replace(".", "")
            out.append(
                {
                    "source_code": src_code,
                    "source_system": source_system,
                    "target_code": tgt_code if tgt_code else None,
                    "target_system": target_system,
                    "approximate_flag": None,
                    "no_map_flag": None,
                    "combination_flag": None,
                    "scenario": None,
                    "choice_list": None,
                    "is_reimbursement": True,
                }
            )
    return out


# ---------------------------------------------------------------------------
# Assemble + write
# ---------------------------------------------------------------------------


def _build_table(rows: list[dict]) -> pa.Table:
    """Assemble the parquet table from normalized rows."""
    if not rows:
        raise RuntimeError(f"gems fy{_FY}: no rows parsed; check upstream files")

    n = len(rows)

    def col(key: str) -> list:
        return [r[key] for r in rows]

    arrays = {
        "source_code": pa.array(col("source_code"), type=pa.string()),
        "source_system": pa.array(col("source_system"), type=pa.string()),
        "target_code": pa.array(col("target_code"), type=pa.string()),
        "target_system": pa.array(col("target_system"), type=pa.string()),
        "approximate_flag": pa.array(col("approximate_flag"), type=pa.bool_()),
        "no_map_flag": pa.array(col("no_map_flag"), type=pa.bool_()),
        "combination_flag": pa.array(col("combination_flag"), type=pa.bool_()),
        "scenario": pa.array(col("scenario"), type=pa.int32()),
        "choice_list": pa.array(col("choice_list"), type=pa.int32()),
        "is_reimbursement": pa.array(col("is_reimbursement"), type=pa.bool_()),
        "effective_start": pa.array([_EFFECTIVE_START] * n, type=pa.date32()),
        "effective_end": pa.array([_EFFECTIVE_END] * n, type=pa.date32()),
        "source": pa.array(col("source"), type=pa.string()),
    }
    return pa.table(arrays, schema=_PARQUET_SCHEMA)


def extract_gems(
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Download + parse the FY2018 GEMs and FY2016 RM artifacts.

    Returns the path to ``data/silver/gems_2018.parquet``.
    """
    sess = session or make_session()
    paths = _download_all(session=sess, download_date=download_date)

    all_rows: list[dict] = []
    for url, members in _gems_layout().items():
        zip_path = paths[url]
        for member_name, src_sys, tgt_sys, is_rm, is_rm_format in members:
            print(
                f"[gems] parsing {member_name} from {zip_path.name} "
                f"({src_sys} -> {tgt_sys}, rm={is_rm})",
                file=sys.stderr,
            )
            raw = _read_member_bytes(zip_path, member_name)
            if is_rm_format:
                rows = _parse_rm_file(raw, source_system=src_sys, target_system=tgt_sys)
            else:
                rows = _parse_gems_file(raw, source_system=src_sys, target_system=tgt_sys)
            for row in rows:
                row["source"] = url
            print(
                f"[gems] {member_name}: {len(rows)} rows",
                file=sys.stderr,
            )
            all_rows.extend(rows)

    table = _build_table(all_rows)

    out_dir = output_dir if output_dir is not None else config.SILVER
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "gems_2018.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[gems] wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.gems",
        description=(
            "Download and parse the FY2018 GEMs and FY2016 Reimbursement "
            "Mapping artifacts; emits data/silver/gems_2018.parquet."
        ),
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help=(
            "Override the bronze date subdir (YYYY-MM-DD). Default: today. "
            "Useful for replays."
        ),
    )
    args = parser.parse_args(argv)

    session = make_session()
    extract_gems(session=session, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
