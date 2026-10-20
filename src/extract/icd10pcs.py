"""ICD-10-PCS extractor: download per-FY ZIPs from CMS and emit silver parquet.

For each FY in ``config.ICD10PCS_SOURCES`` (currently FY2016-FY2026), this module:

1. Downloads the per-FY PCS ZIP into ``data/bronze/icd10pcs/fy{fy}/{download_date}/``
   via :mod:`src.extract._http`.
2. Reads the codes file out of the ZIP (without unzipping to disk).
3. Reads the addenda file (if any) and merges action labels.
4. Writes a parquet matching the locked Phase-1 schema (see
   ``src.transform.codes_master._SCHEMA``) to
   ``data/silver/per_source/icd10pcs_fy{fy}.parquet``.

ZIP shape varies across FYs. CMS shipped a fixed-width Order File until FY2017
(which carried both short and long titles plus an inline action flag) and then
switched to a simpler ``code SP long-title`` text file from FY2018 on, with
action labels moved to a sibling ``codes_addenda_{fy}.txt``. The parser
auto-detects which layout each ZIP carries by member-name prefix
(``icd10pcs_order_*`` vs ``icd10pcs_codes_*``); the plan's stated 1-7 / 8-14 / 16
column offsets are wrong for the FY2016 Order File — actual offsets are
order-number(1-5), code(7-13), action-flag(15), short-title(17-76),
long-title(78+). See module-level constants for the verified layout.

ICD-10-PCS has no chapter analog and no POA-exempt list, so ``chapter`` and
``poa_exempt`` are NULL on every row by design. ``or_procedure_flag``,
``mcc_flag``, and ``cc_flag`` are deferred to Phase 3 and emitted NULL.

CLI::

    python -m src.extract.icd10pcs --fy 2026
    python -m src.extract.icd10pcs --fy-range 2016-2026
"""

from __future__ import annotations

import argparse
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

_BRONZE_SOURCE: str = "icd10pcs"
_CODE_SYSTEM: str = "ICD-10-PCS"

# Verified offsets for the FY2016/FY2017 fixed-width Order File
# (icd10pcs_order_{fy}.txt). 1-indexed, half-open at end so we slice [start:end].
# Empirically determined from FY2016 line 2 (`00002 0016070 1 Bypass Cereb...`):
#   - cols 1-5    : order-number   (5-digit, zero-padded)
#   - col  6      : space
#   - cols 7-13   : code            (7-char alphanumeric, no dot)
#   - col  14     : space
#   - col  15     : action-flag     (single digit 0|1|2|3)
#   - col  16     : space
#   - cols 17-76  : short title     (60-wide, space-padded)
#   - col  77     : space
#   - cols 78-end : long title
# These match the FY2017 file as well (same producer at CMS).
_ORDER_CODE_SLICE: slice = slice(6, 13)         # 0-indexed
_ORDER_FLAG_SLICE: slice = slice(14, 15)
_ORDER_SHORT_SLICE: slice = slice(16, 76)
_ORDER_LONG_SLICE: slice = slice(77, None)

_ACTION_FLAG_MAP: dict[str, str] = {
    "0": "unchanged",
    "1": "added",
    "2": "revised",
    "3": "deleted",
}

# `codes_addenda_{fy}.txt` (FY2018+) format:
#   `Add:         <code> <long title>`           -> addendum_action="added"
#   `Delete:      <code> <long title>`           -> addendum_action="deleted"
#   `Revise from: <code> <long title>`           -> ignore (paired with `Revise to`)
#   `Revise to:   <code> <long title>`           -> addendum_action="revised"
# No code in any addenda file -> "unchanged".
_ADDENDA_LINE_RE = re.compile(
    r"^(Add|Delete|Revise to|Revise from):\s+([A-Z0-9]{7})\b",
    re.IGNORECASE,
)
_ADDENDA_PREFIX_TO_ACTION: dict[str, str | None] = {
    "add": "added",
    "delete": "deleted",
    "revise to": "revised",
    "revise from": None,  # ignored
}

# `icd10pcs_codes_{fy}.txt` (FY2018+) format: `<code:7> <long title>`. Single
# space separator; long title runs to end of line.
_CODES_LINE_RE = re.compile(r"^([A-Z0-9]{7}) (.+)$")

# Short-title harvest from order_addenda_2016/2017 lines
# (`Revise from: <code> <short:60> <long>`). We don't actually use the
# order_addenda for short titles — the Order File itself carries them — but
# leave this comment as a guidepost for future maintainers.


@dataclass(frozen=True)
class _ParsedRow:
    """A single (code, long_title, short_title?, action?) tuple."""

    code: str
    long_title: str
    short_title: str | None
    addendum_action: str  # one of "added" / "revised" / "deleted" / "unchanged"


# ----------------------------------------------------------------------------
# ZIP introspection
# ----------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"icd10pcs: corrupt ZIP at {path}: {exc}") from exc


def _find_member(zf: zipfile.ZipFile, basename_prefix: str) -> str | None:
    """Return the ZIP member whose basename starts with `basename_prefix`.

    Some FYs (notably FY2021) wrap the files under a top-level subdirectory
    inside the ZIP; matching on basename rather than the full path makes the
    extractor immune to that.
    """
    for name in zf.namelist():
        if Path(name).name.lower().startswith(basename_prefix.lower()):
            return name
    return None


def _read_member_text(zf: zipfile.ZipFile, member: str) -> str:
    with zf.open(member, "r") as fh:
        raw = fh.read()
    # CMS files are ASCII or Latin-1; decode permissively so a stray byte
    # doesn't kill a 79k-row parse.
    return raw.decode("latin-1")


# ----------------------------------------------------------------------------
# FY2016 / FY2017: fixed-width Order File
# ----------------------------------------------------------------------------


def _parse_order_file(text: str) -> list[_ParsedRow]:
    """Parse the FY2016/FY2017 fixed-width ``icd10pcs_order_{fy}.txt``.

    Skips header / chapter / section rows: those have an order-number but the
    "code" slice is not a valid 7-char alphanumeric (they're padded with spaces
    or carry a 3-char section header). We require 7 alphanumerics in the code
    slice as the validity gate; non-matching lines are dropped silently.
    """
    rows: list[_ParsedRow] = []
    for line in text.splitlines():
        if len(line) < 16:
            continue
        code = line[_ORDER_CODE_SLICE].strip()
        if len(code) != 7 or not code.isalnum():
            continue
        code = code.upper()
        flag = line[_ORDER_FLAG_SLICE]
        action = _ACTION_FLAG_MAP.get(flag, "unchanged")
        # Long title continues to end-of-line; strip trailing whitespace.
        long_title = line[_ORDER_LONG_SLICE].rstrip() if len(line) > 77 else ""
        short_title_raw = line[_ORDER_SHORT_SLICE].rstrip() if len(line) > 16 else ""
        short_title = short_title_raw or None
        rows.append(
            _ParsedRow(
                code=code,
                long_title=long_title,
                short_title=short_title,
                addendum_action=action,
            )
        )
    return rows


# ----------------------------------------------------------------------------
# FY2018+: simple `icd10pcs_codes_{fy}.txt` + sibling `codes_addenda_{fy}.txt`
# ----------------------------------------------------------------------------


def _parse_codes_file(text: str) -> list[tuple[str, str]]:
    """Parse the simple FY2018+ ``icd10pcs_codes_{fy}.txt``.

    Returns a list of (code, long_title) tuples. The codes file has no short
    title and no action flag; both come (or don't come) from sibling files.
    """
    out: list[tuple[str, str]] = []
    for line in text.splitlines():
        m = _CODES_LINE_RE.match(line)
        if not m:
            continue
        out.append((m.group(1).upper(), m.group(2).rstrip()))
    return out


def _parse_codes_addenda(text: str) -> dict[str, str]:
    """Parse ``codes_addenda_{fy}.txt`` -> dict[code, addendum_action].

    A code that appears as both `Revise from` and `Revise to` ends up labeled
    "revised". `Add` -> "added", `Delete` -> "deleted". Codes absent from this
    file get "unchanged" by the caller.
    """
    actions: dict[str, str] = {}
    for line in text.splitlines():
        m = _ADDENDA_LINE_RE.match(line)
        if not m:
            continue
        prefix = m.group(1).strip().lower()
        code = m.group(2).upper()
        action = _ADDENDA_PREFIX_TO_ACTION.get(prefix)
        if action is None:
            continue
        # If a code appears under multiple verbs (e.g., revised twice), the
        # later one wins — fine for our purposes.
        actions[code] = action
    return actions


# ----------------------------------------------------------------------------
# ZIP -> rows dispatch
# ----------------------------------------------------------------------------


def _rows_from_zip(zip_path: Path, fy: int) -> list[_ParsedRow]:
    """Read the codes/order file and (if present) addenda from `zip_path`."""
    with _open_zip(zip_path) as zf:
        order_member = _find_member(zf, f"icd10pcs_order_{fy}")
        codes_member = _find_member(zf, f"icd10pcs_codes_{fy}")
        addenda_member = _find_member(zf, f"codes_addenda_{fy}")

        if order_member is not None:
            text = _read_member_text(zf, order_member)
            rows = _parse_order_file(text)
            if not rows:
                raise RuntimeError(
                    f"icd10pcs fy{fy}: parsed 0 rows from Order File "
                    f"{order_member} in {zip_path}; column offsets may have "
                    f"drifted, see _ORDER_*_SLICE in src/extract/icd10pcs.py"
                )
            return rows

        if codes_member is None:
            raise RuntimeError(
                f"icd10pcs fy{fy}: ZIP {zip_path} contains neither "
                f"icd10pcs_order_{fy}.txt nor icd10pcs_codes_{fy}.txt; "
                f"members were: {zf.namelist()}"
            )

        codes_text = _read_member_text(zf, codes_member)
        pairs = _parse_codes_file(codes_text)
        if not pairs:
            raise RuntimeError(
                f"icd10pcs fy{fy}: parsed 0 rows from codes file "
                f"{codes_member} in {zip_path}; line format may have drifted"
            )

        if addenda_member is not None:
            addenda_text = _read_member_text(zf, addenda_member)
            actions = _parse_codes_addenda(addenda_text)
        else:
            actions = {}

        return [
            _ParsedRow(
                code=code,
                long_title=long_title,
                short_title=None,
                addendum_action=actions.get(code, "unchanged"),
            )
            for code, long_title in pairs
        ]


# ----------------------------------------------------------------------------
# Parquet emission
# ----------------------------------------------------------------------------


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
    source_url: str,
    fy_start: date,
    fy_end: date,
) -> pa.Table:
    n = len(rows)
    return pa.Table.from_arrays(
        [
            pa.array([r.code for r in rows], type=pa.string()),
            pa.array([_CODE_SYSTEM] * n, type=pa.string()),
            pa.array([fy] * n, type=pa.int32()),
            pa.array([fy_start] * n, type=pa.date32()),
            pa.array([fy_end] * n, type=pa.date32()),
            pa.array([r.long_title for r in rows], type=pa.string()),
            pa.array([r.short_title for r in rows], type=pa.string()),
            pa.array([None] * n, type=pa.string()),  # chapter (always NULL for PCS)
            pa.array([r.addendum_action for r in rows], type=pa.string()),
            pa.array([None] * n, type=pa.bool_()),  # or_procedure_flag (Phase 3)
            pa.array([None] * n, type=pa.bool_()),  # mcc_flag (Phase 3)
            pa.array([None] * n, type=pa.bool_()),  # cc_flag (Phase 3)
            pa.array([None] * n, type=pa.bool_()),  # poa_exempt (PCS-NA)
            pa.array([source_url] * n, type=pa.string()),
            pa.array([None] * n, type=pa.int32()),  # n (Phase 5)
        ],
        schema=_PARQUET_SCHEMA,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def extract_fy(
    fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract ICD-10-PCS for fiscal year ``fy`` and write the silver parquet.

    Parameters
    ----------
    fy:
        Fiscal year. Must have an entry in ``config.ICD10PCS_SOURCES``.
    session:
        Optional shared ``requests.Session`` (use one per range to amortize TLS).
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze-path date stamp; defaults
        to today.
    output_dir:
        Optional override for the silver per-source directory; defaults to
        ``config.SILVER_PER_SOURCE``.

    Returns
    -------
    Path
        The silver parquet that was written.

    Raises
    ------
    KeyError
        If ``fy`` is not in ``config.ICD10PCS_SOURCES``.
    RuntimeError
        On HTTP failure (wrapped from ``requests.HTTPError`` with FY context),
        ZIP corruption, or parser failure (zero rows extracted).
    """
    try:
        url = config.ICD10PCS_SOURCES[fy]
    except KeyError as exc:
        raise KeyError(
            f"ICD10PCS_SOURCES has no entry for FY{fy}; populate config.py"
        ) from exc

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else config.SILVER_PER_SOURCE
    out_dir.mkdir(parents=True, exist_ok=True)

    # Use the URL's basename as the bronze filename so re-pulls under a new
    # `download_date` keep a stable name.
    filename = Path(url).name
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=fy,
            filename=filename,
            download_date=download_date,
            session=sess,
        )
    except requests.HTTPError as exc:
        raise RuntimeError(f"icd10pcs fy{fy}: {url} failed: {exc}") from exc

    rows = _rows_from_zip(bronze_path, fy)

    fy_start, fy_end = config.fy_interval(fy)
    table = _build_table(rows, fy=fy, source_url=url, fy_start=fy_start, fy_end=fy_end)

    out_path = out_dir / f"icd10pcs_fy{fy}.parquet"
    pq.write_table(table, out_path)
    print(
        f"[icd10pcs] fy{fy}: wrote {table.num_rows} rows to {out_path}",
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
    """Extract a contiguous inclusive range of fiscal years.

    Returns the list of silver parquet paths in extraction order. Re-uses one
    HTTP session across the range.
    """
    if start_fy > end_fy:
        raise ValueError(f"start_fy ({start_fy}) must be <= end_fy ({end_fy})")
    sess = session or _http.make_session()
    paths: list[Path] = []
    for fy in range(start_fy, end_fy + 1):
        paths.append(
            extract_fy(fy, session=sess, download_date=download_date)
        )
    return paths


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def _parse_fy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range expects 'START-END' (e.g., 2016-2026), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.icd10pcs",
        description=(
            "Download and parse CMS ICD-10-PCS code files for one fiscal year "
            "or an inclusive range, writing per-FY parquet to "
            "data/silver/per_source/."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--fy",
        type=int,
        help="Single fiscal year (e.g., 2026).",
    )
    group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        metavar="START-END",
        help="Inclusive FY range, e.g., '2016-2026'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.fy is not None:
        extract_fy(args.fy, download_date=args.download_date)
    else:
        start_fy, end_fy = args.fy_range
        extract_fy_range(start_fy, end_fy, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
