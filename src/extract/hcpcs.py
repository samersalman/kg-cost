"""HCPCS Level II extractor: per-CY CMS Annual Alpha workbook -> silver parquet.

For each calendar year in scope this module:

    1. Reads the per-CY URL from ``config.HCPCS_SOURCES``. The canonical
       store is now backfilled with verified URLs for CY2017-CY2026 and
       known-404 placeholder URLs for CY2008-CY2016 (no working CMS path or
       Wayback mirror exists for those years as of 2026-04-29). A 404 from
       the placeholder surfaces as an ``HTTPError`` that ``extract_cy_range``
       catches and logs+skips — mirroring the ``icd10cm.py`` pattern. The
       previous runtime HEAD-probe URL ladder was retired once config was
       backfilled; CMS slug drift now requires updating
       ``config.HCPCS_SOURCES``, not silent runtime rerouting.

    2. Downloads via ``_http.download_to_bronze`` with ``period="cy"``, so the
       bronze layout becomes ``data/bronze/hcpcs/cy{cy}/{download_date}/``.

    3. Reads the Annual Alpha workbook out of the ZIP. Two formats are
       supported:

         - ``.xlsx`` (CY2014+ era — read via ``openpyxl``).
         - ``.xls``  (pre-CY2014 — read via ``xlrd<2`` if installed).

       The workbook has 48+ columns; only ``HCPC``, ``LONG DESCRIPTION``, and
       ``SHORT DESCRIPTION`` are retained. Two-character HCPCS modifiers
       (``RECID == 7``) are filtered out — modifiers are properties of HCPCS
       codes per CLAUDE.md, not separate code rows.

    4. Writes a per-CY parquet to ``data/silver/per_source/hcpcs_cy{cy}.parquet``
       matching ``src.transform.codes_master._SCHEMA`` exactly.

CY2008-CY2016 currently 404 across all known CMS paths (the pre-Drupal
``Hcpc{yy}_O_AnnAlpha.zip`` releases were dropped from cms.gov and never
mirrored on web.archive.org). Those CYs log loudly and are skipped — see
``plans/session2-notes-hcpcs.md`` for the discovery log.

CLI::

    python -m src.extract.hcpcs --cy 2020
    python -m src.extract.hcpcs --cy-range 2008-2026
    python -m src.extract.hcpcs --cy 2020 --download-date 2026-04-29
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "hcpcs"
_CODE_SYSTEM: str = "HCPCS-II"

# HCPCS Level II 5-character upper-case alphanumeric. First char A-V or 0-9
# (Level I CPT codes are pure 5-digit numeric and are excluded from this file
# in practice, but the regex tolerates them for forward-compat).
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")

# Workbook columns we actually read. CMS is consistent across ANWEB releases
# from CY2017+ on header naming; we tolerate either spaces ("LONG DESCRIPTION")
# or underscores ("LONG_DESC", earlier era) with case-insensitive lookup.
_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "code": ("HCPC", "HCPCS", "HCPCS_CD"),
    "recid": ("RECID",),
    "long": ("LONG DESCRIPTION", "LONG_DESC", "LONG DESC"),
    "short": ("SHORT DESCRIPTION", "SHORT_DESC", "SHORT DESC"),
    "term_dt": ("TERM DT", "TERM_DT", "TERMDT"),
    "eff_dt": ("ACT EFF DT", "EFF_DT", "EFFDT"),
}


def _normalize_header(value: object) -> str:
    """Return a case- and whitespace-normalized header string for matching."""
    if value is None:
        return ""
    return str(value).strip().upper()


def _resolve_columns(header: list[object]) -> dict[str, int]:
    """Map our logical column names to 0-indexed positions in ``header``.

    Raises if any of the *required* columns (``code``, ``long``, ``short``)
    can't be located. ``term_dt``, ``eff_dt``, and ``recid`` are best-effort
    (may legitimately be missing in some FYs).
    """
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}
    for logical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            try:
                out[logical] = norm.index(alias.upper())
                break
            except ValueError:
                continue
    required = ("code", "long", "short")
    missing = [k for k in required if k not in out]
    if missing:
        raise RuntimeError(
            f"hcpcs: required columns {missing} not found in header; "
            f"saw {norm[:15]}..."
        )
    return out


# ----------------------------------------------------------------------------
# Workbook discovery + parse
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParsedRow:
    code: str
    long_desc: str
    short_desc: str | None


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"hcpcs: corrupt ZIP at {path}: {exc}") from exc


def _find_workbook_member(zf: zipfile.ZipFile) -> tuple[str, str]:
    """Return (member_name, suffix_lower) of the Annual Alpha workbook.

    Selection rules (in order):
      1. ``.xlsx`` member whose basename contains "ANWEB" but NOT
         "TRANS"/"Transaction"/"Corrections"/"changes" (which are auxiliary
         workbooks shipped alongside).
      2. ``.xls``  member matching the same predicate (pre-2014 era).
      3. Any ``.xlsx`` member whose basename starts with "HCPC".
      4. Any ``.xls``  member whose basename starts with "HCPC".

    Raises if no candidate is found.
    """
    members = list(zf.namelist())

    def _is_main(name: str, suffix: str) -> bool:
        bn = Path(name).name.lower()
        if not bn.endswith(suffix):
            return False
        if "anweb" not in bn:
            return False
        for skip in ("trans", "corrections", "changes", "report"):
            if skip in bn:
                return False
        return True

    for suffix in (".xlsx", ".xls"):
        for name in members:
            if _is_main(name, suffix):
                return name, suffix

    # Fallback: any HCPC*.xlsx / HCPC*.xls.
    for suffix in (".xlsx", ".xls"):
        for name in members:
            bn = Path(name).name.lower()
            if bn.endswith(suffix) and bn.startswith("hcpc"):
                return name, suffix

    raise RuntimeError(
        f"hcpcs: no ANWEB workbook found in ZIP; members were "
        f"{[Path(n).name for n in members]}"
    )


def _extract_member_to_tempfile(zf: zipfile.ZipFile, member: str, dest: Path) -> Path:
    """Extract a ZIP member to ``dest`` (idempotent — skip if already present).

    openpyxl + xlrd both want a real file path / file object; rather than
    re-stream from the ZIP each call, we land the workbook next to the
    bronze ZIP and read from there. The bronze directory is write-once per
    CLAUDE.md, so this also doubles as a cached unpack.
    """
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zf.open(member, "r") as src, dest.open("wb") as out:
        while True:
            chunk = src.read(64 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return dest


def _iter_xlsx_rows(path: Path):
    """Yield every row of the first sheet as a tuple, in workbook order.

    Uses ``openpyxl`` in read-only mode for memory efficiency on the 8k-row
    Annual Alpha workbook.
    """
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[wb.sheetnames[0]]
        for row in ws.iter_rows(values_only=True):
            yield row
    finally:
        wb.close()


def _iter_xls_rows(path: Path):
    """Yield every row of the first sheet as a tuple, in workbook order.

    Requires ``xlrd<2`` (xlrd 2.x dropped .xls support). xlrd 2.0.2 still ships
    a working .xls reader despite the changelog claim — the soft path here
    surfaces a clear error message if the import fails.
    """
    try:
        import xlrd
    except ImportError as exc:
        raise RuntimeError(
            "hcpcs: pre-2014-era HCPCS ZIPs ship .xls binary workbooks; "
            "install xlrd<2 (`uv pip install 'xlrd<2'`) or add it to "
            "pyproject.toml to enable that branch"
        ) from exc

    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)
    for r in range(sh.nrows):
        yield tuple(sh.cell_value(r, c) for c in range(sh.ncols))


def _find_header_index(rows: list[tuple]) -> int:
    """Return the 0-indexed row whose first cell is "HCPC".

    CMS prepends a multi-line legal-disclaimer block to some workbooks
    (e.g. CY2020 has the header at row 10); modern workbooks start at row 0.
    Raises if no header row is found within the first 30 rows.
    """
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        first = _normalize_header(row[0])
        if first == "HCPC" or first.startswith("HCPC"):
            # Tolerate "HCPC" or "HCPCS"/"HCPCS_CD" prefixes; reject leading
            # data rows like "HCPC2020_..." (already normalized to upper).
            if first in {h.upper() for h in _HEADER_ALIASES["code"]}:
                return i
    raise RuntimeError(
        f"hcpcs: header row not found in first 30 rows; "
        f"first rows seen: {[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


def _parse_hcpc_field(value: object) -> str | None:
    """Normalize the HCPC column value to an upper-case dotless string.

    The .xls workbook stores 2-char modifiers as right-aligned strings
    (e.g. ``"   A1"``). We strip whitespace and upper-case. Returns None
    for empty cells.
    """
    if value is None:
        return None
    s = str(value).strip().upper().replace(".", "")
    return s or None


def _parse_term_date(value: object) -> date | None:
    """Best-effort parse of CMS date columns (``TERM DT`` / ``ACT EFF DT``).

    CMS ships dates either as a string ``YYYYMMDD`` or as a numeric/date in
    the workbook. Returns None on empty / unparseable.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    # CMS canonical: YYYYMMDD as 8-digit string (with possible trailing .0
    # when xlrd returns it as float).
    if "." in s:
        s = s.split(".", 1)[0]
    if len(s) == 8 and s.isdigit():
        try:
            return datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _rows_from_workbook(workbook_path: Path, suffix: str) -> list[_ParsedRow]:
    """Parse ``workbook_path`` (.xlsx or .xls) into a list of HCPCS code rows.

    Filters:
      - ``RECID != 7``: drops 2-char modifier rows (RECID=7 is the modifier
        row type per CMS RIF; RECID=3 is the procedure code row type).
      - Code must match ``_HCPCS_CODE_RE`` (5-char [A-V0-9][0-9]{4}).
      - Codes whose ``TERM DT`` is before the workbook's CY are still emitted —
        a code that terminated mid-year is still part of that CY's master.
    """
    if suffix == ".xlsx":
        rows = list(_iter_xlsx_rows(workbook_path))
    elif suffix == ".xls":
        rows = list(_iter_xls_rows(workbook_path))
    else:
        raise RuntimeError(f"hcpcs: unsupported workbook suffix {suffix!r}")

    if not rows:
        raise RuntimeError(f"hcpcs: workbook {workbook_path} parsed to zero rows")

    header_idx = _find_header_index(rows)
    header = list(rows[header_idx])
    cols = _resolve_columns(header)

    out: list[_ParsedRow] = []
    seen: set[str] = set()
    for raw in rows[header_idx + 1:]:
        if not raw or len(raw) <= cols["code"]:
            continue
        # Filter modifier rows by RECID when available (RECID=7 == modifier).
        if "recid" in cols and len(raw) > cols["recid"]:
            recid_raw = raw[cols["recid"]]
            if recid_raw is not None:
                recid_s = str(recid_raw).strip()
                if "." in recid_s:
                    recid_s = recid_s.split(".", 1)[0]
                if recid_s == "7":
                    continue
        code = _parse_hcpc_field(raw[cols["code"]])
        if code is None or not _HCPCS_CODE_RE.match(code):
            continue
        if code in seen:
            # CMS occasionally double-lists codes under different price
            # buckets; keep the first occurrence (procedural row).
            continue
        seen.add(code)
        long_desc = ""
        if len(raw) > cols["long"] and raw[cols["long"]] is not None:
            long_desc = str(raw[cols["long"]]).strip()
        short_desc: str | None = None
        if len(raw) > cols["short"] and raw[cols["short"]] is not None:
            short_raw = str(raw[cols["short"]]).strip()
            short_desc = short_raw or None
        out.append(_ParsedRow(code=code, long_desc=long_desc, short_desc=short_desc))

    if not out:
        raise RuntimeError(
            f"hcpcs: workbook {workbook_path} yielded zero matching code rows; "
            f"check header/regex/RECID filter"
        )
    return out


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
    cy: int,
    source_url: str,
    cy_start: date,
    cy_end: date,
) -> pa.Table:
    n = len(rows)
    return pa.Table.from_arrays(
        [
            pa.array([r.code for r in rows], type=pa.string()),
            pa.array([_CODE_SYSTEM] * n, type=pa.string()),
            pa.array([cy] * n, type=pa.int32()),
            pa.array([cy_start] * n, type=pa.date32()),
            pa.array([cy_end] * n, type=pa.date32()),
            pa.array([r.long_desc for r in rows], type=pa.string()),
            pa.array([r.short_desc for r in rows], type=pa.string()),
            pa.array([None] * n, type=pa.string()),  # chapter — HCPCS has none
            pa.array([None] * n, type=pa.string()),  # addendum_action — N/A
            pa.array([None] * n, type=pa.bool_()),  # or_procedure_flag
            pa.array([None] * n, type=pa.bool_()),  # mcc_flag
            pa.array([None] * n, type=pa.bool_()),  # cc_flag
            pa.array([None] * n, type=pa.bool_()),  # poa_exempt
            pa.array([source_url] * n, type=pa.string()),
            pa.array([None] * n, type=pa.int32()),  # n (Phase 5)
        ],
        schema=_PARQUET_SCHEMA,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def extract_cy(
    cy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract HCPCS Level II for calendar year ``cy``; write silver parquet.

    Parameters
    ----------
    cy:
        Calendar year (e.g., 2020). Must be in ``config.HCPCS_CYS``.
    session:
        Optional shared ``requests.Session`` (use one per range to amortize TLS).
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze date subdir.
    output_dir:
        Optional override for ``config.SILVER_PER_SOURCE``.

    Returns
    -------
    Path
        The silver parquet path that was written.

    Raises
    ------
    KeyError
        If ``cy`` is not in ``config.HCPCS_CYS``.
    RuntimeError
        On HTTP failure (e.g. 404 from CY2008-CY2016 placeholders), ZIP
        corruption, or workbook parse failure.
    """
    if cy not in config.HCPCS_CYS:
        raise KeyError(
            f"hcpcs cy{cy}: not registered in config.HCPCS_CYS "
            f"(known CYs: {min(config.HCPCS_CYS)}-{max(config.HCPCS_CYS)})"
        )

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else config.SILVER_PER_SOURCE
    out_dir.mkdir(parents=True, exist_ok=True)

    # URLs are the canonical store in config.HCPCS_SOURCES (verified for
    # CY2017-CY2026; known-404 placeholders for CY2008-CY2016). A 404 from
    # download_to_bronze surfaces as an HTTPError that the bulk runner catches
    # and logs+skips — this mirrors the icd10cm.py pattern.
    url = config.HCPCS_SOURCES[cy]
    print(f"[hcpcs] cy{cy}: URL {url}", file=sys.stderr)

    filename = Path(url).name
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=cy,
            filename=filename,
            download_date=download_date,
            session=sess,
            period="cy",
        )
    except requests.HTTPError as exc:
        raise RuntimeError(f"hcpcs cy{cy}: {url} failed: {exc}") from exc

    with _open_zip(bronze_path) as zf:
        member, suffix = _find_workbook_member(zf)
        workbook_dest = bronze_path.parent / Path(member).name
        _extract_member_to_tempfile(zf, member, workbook_dest)
    print(
        f"[hcpcs] cy{cy}: parsing workbook {workbook_dest.name} ({suffix})",
        file=sys.stderr,
    )

    rows = _rows_from_workbook(workbook_dest, suffix)
    cy_start, cy_end = config.cy_interval(cy)
    table = _build_table(
        rows, cy=cy, source_url=url, cy_start=cy_start, cy_end=cy_end
    )

    out_path = out_dir / f"hcpcs_cy{cy}.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[hcpcs] cy{cy}: wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_cy_range(
    start_cy: int,
    end_cy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_cy`` over inclusive ``[start_cy, end_cy]``.

    Per-CY failures are logged loudly and skipped — pre-2017 CYs (currently
    unavailable on cms.gov) intentionally go through this path. The list of
    successfully written parquet paths is returned.
    """
    if end_cy < start_cy:
        raise ValueError(f"end_cy ({end_cy}) must be >= start_cy ({start_cy})")
    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for cy in range(start_cy, end_cy + 1):
        if cy not in config.HCPCS_CYS:
            print(f"[hcpcs] CY{cy}: not in config.HCPCS_CYS, skipping", file=sys.stderr)
            continue
        try:
            path = extract_cy(cy, session=sess, download_date=download_date)
            out.append(path)
        except Exception as exc:  # noqa: BLE001 — bulk run swallows per-CY errors
            print(
                f"[hcpcs] CY{cy} FAILED: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((cy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[hcpcs] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for cy, msg in failures:
            print(f"  CY{cy}: {msg}", file=sys.stderr)
    print(
        f"[hcpcs] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
        file=sys.stderr,
    )
    return out


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def _parse_cy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--cy-range expects 'START-END' (e.g., 2008-2026), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--cy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.hcpcs",
        description=(
            "Download and parse the CMS HCPCS Level II Annual Alpha workbook "
            "for one calendar year or an inclusive range, writing per-CY "
            "parquet to data/silver/per_source/."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--cy",
        type=int,
        help="Single calendar year (e.g., 2020).",
    )
    group.add_argument(
        "--cy-range",
        type=_parse_cy_range,
        metavar="START-END",
        help="Inclusive CY range, e.g., '2008-2026'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.cy is not None:
        extract_cy(args.cy, download_date=args.download_date)
    else:
        start_cy, end_cy = args.cy_range
        extract_cy_range(start_cy, end_cy, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
