"""IPPS Final Rule extractor: per-(FY, version, table_kind) ZIP -> silver parquets.

Phase 3 captures two artifacts at FY-granularity (FY2008-FY2026, V*.0 only):

    Table 5      -- one row per (MS-DRG, FY) carrying relative weight, GMLOS,
                    AMLOS, MDC, type, severity. Single ZIP per FY containing one
                    main XLSX (and a tab-delimited .txt sibling on legacy FYs).
                    ~755 DRGs/FY.
    Tables 1A-1E -- "Tables 1A through 1E": operating + capital standardized
                    amounts, labor/nonlabor shares, capital federal rate,
                    outlier thresholds, wage-index anchors, COLA factors. The
                    silver layer is *long-form scalar*: every numeric cell with
                    a recognized line-item label becomes one row keyed by
                    ``(fy, version, table, line_item, category, rate_period)``.

This extractor implements the **bundled-ZIP demux** pattern (mirrors
``src.extract.asc``) — for each ``(fy, version, table_kind)`` cell it
downloads the ZIP from ``config._IPPS_TABLE5_BY_FY`` /
``config._IPPS_TABLES1_BY_FY`` ONCE, then writes one parquet per
(table-letter | dual-period split) into
``data/silver/per_source/ipps/``.

**Per-source filename convention** (LOCKED — used by transform):

    ipps_table5_fy{YYYY}_{version_short}.parquet
    ipps_table1{a,b,c,d,e}_fy{YYYY}_{version_short}.parquet

where ``version_short`` is the version string lowercased and dot-stripped
(``V43.0`` -> ``v430``). For FY2010 V27.0 only (PPACA dual-period), Tables
1A-1E filenames split into ``_v270_1st_half`` and ``_v270_2nd_half``
suffix pairs.

**Mode A vs Mode B Tables 1A-1E packaging** (REVISED 2026-04-30):

    * Mode A (one file, single stacked worksheet) -- ONE primary file (XLSX
      or TXT) inside the ZIP whose **single worksheet** carries all 5
      tables stacked vertically, demarcated by ``TABLE 1{X}.`` section
      markers. This is the dominant real-world layout: every FY observed
      so far ships exactly ONE primary member with content stacked.
      Section-aware parser (``_parse_tables1_rows``) walks the rows,
      locates each ``TABLE 1[A-E]`` marker, and dispatches per-letter
      section extractors.
    * Mode B (multiple files, one each) -- rare fallback: separate
      ``.xlsx`` or ``.txt`` member per table letter. Each file contains
      ONE table; ``table`` is inferred from the filename.

Detection heuristic: after filtering disclaimer / footnote auxiliary
files, count the surviving primary members. ``== 1`` -> Mode A; ``> 1``
-> Mode B.

**Operating-amount emission convention** (Tables 1A / 1B / 1C):

    Each tier in CMS Tables 1A/1B/1C splits operating amounts into a
    LABOR-related component and a NONLABOR-related component. The
    silver layer stores the **SUM** (labor + nonlabor) as a single
    ``operating_full_update`` row per category, and emits separate
    ``labor_share`` / ``nonlabor_share`` rows (parsed from the section
    banner text) so Phase 7 cost-compute can reconstruct the labor-side
    contribution via wage-index adjustment.

**FY2010 PPACA dual-period**:

The FY2010 ZIP ships TEN members (5 tables x 2 rate-periods). Member
basenames carry ``1st Half`` / ``2nd Half`` substrings (case-insensitive,
also tolerated underscore-/hyphen-separated). Each (table-letter, half)
emits a SEPARATE parquet at
``ipps_table1{x}_fy2010_v270_{1st_half|2nd_half}.parquet`` and the
``rate_period`` column is populated with ``"1st_half"`` / ``"2nd_half"``.
For all other FYs the single parquet per table-letter has
``rate_period`` = NULL.

CLI::

    python -m src.extract.ipps --fy 2026 --version V43.0 --target table5
    python -m src.extract.ipps --fy 2026 --version V43.0 --target tables1
    python -m src.extract.ipps --fy 2026 --version V43.0 --target both
    python -m src.extract.ipps --fy-range 2008-2026 --target both
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "ipps"

# Footer / disclaimer row prefixes that should never be treated as data.
_FOOTER_PREFIXES: tuple[str, ...] = (
    "NOTE",
    "SEE",
    "*",
    "FOOTNOTE",
    "SOURCE",
    "ABBREVIATION",
    "ABBREVIATIONS",
)

# Table 5 MS-DRG row regex: 1-3 digit numeric (after stripping). The 3-digit
# zero-padded version is what we emit.
_MS_DRG_RE = re.compile(r"^[0-9]{1,3}$")

# Reserved-non-payable DRGs (CMS convention). Emitted with a stderr warning.
_RESERVED_DRGS: frozenset[str] = frozenset({"998", "999"})

# ----------------------------------------------------------------------------
# Locked PyArrow schemas
# ----------------------------------------------------------------------------

_TABLE5_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("ms_drg", pa.string()),
        pa.field("fy", pa.int32()),
        pa.field("version", pa.string()),
        pa.field("title", pa.string()),
        pa.field("mdc", pa.string()),
        pa.field("drg_type", pa.string()),
        pa.field("severity", pa.string()),
        pa.field("relative_weight", pa.float64()),
        pa.field("gmlos", pa.float64()),
        pa.field("amlos", pa.float64()),
        pa.field("post_acute_transfer_flag", pa.string()),
        pa.field("effective_start", pa.date32()),
        pa.field("effective_end", pa.date32()),
        pa.field("source", pa.string()),
    ]
)

_TABLE1_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("fy", pa.int32()),
        pa.field("version", pa.string()),
        pa.field("table", pa.string()),
        pa.field("line_item", pa.string()),
        pa.field("category", pa.string()),
        pa.field("rate_period", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("effective_start", pa.date32()),
        pa.field("effective_end", pa.date32()),
        pa.field("source", pa.string()),
    ]
)


# ----------------------------------------------------------------------------
# Header / column resolution
# ----------------------------------------------------------------------------


def _normalize_header(value: object) -> str:
    """Return a case- and whitespace-normalized header string for matching.

    Collapses internal runs of whitespace to a single space (CMS Final Rule
    column headers ship with ragged spacing across FYs).
    """
    if value is None:
        return ""
    return " ".join(str(value).split()).upper()


# Table 5 header alias dict (substring match, case-insensitive after normalize).
_TABLE5_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "ms_drg": ("MS-DRG", "MSDRG", "DRG"),
    "title": ("MS-DRG TITLE", "DRG TITLE", "TITLE"),
    "mdc": ("MDC",),
    "drg_type": ("TYPE", "MED/SURG", "M/S"),
    "relative_weight": ("WEIGHT",),
    "gmlos": ("GEOMETRIC", "GMLOS", "GEOMETRIC MEAN"),
    "amlos": ("ARITHMETIC", "AMLOS", "ARITHMETIC MEAN"),
    "post_acute_transfer_flag": ("POST", "TRANSFER"),
}

_TABLE5_REQUIRED_COLS: tuple[str, ...] = ("ms_drg", "title", "relative_weight")


# Tables 1A-1E line-item alias dict. First-match-wins; unknown labels are
# logged and skipped (or hard-fail under --strict at the CLI level).
_TABLE1_LINE_ITEM_ALIASES: dict[str, tuple[str, ...]] = {
    "operating_full_update": (
        "FULL UPDATE",
        "NATIONAL STANDARDIZED AMOUNT",
        "NATIONAL ADJUSTED STANDARDIZED AMOUNT",
        "NATIONAL OPERATING STANDARDIZED AMOUNT",
        "FULL MARKET BASKET",
        "OPERATING STANDARDIZED AMOUNT",
        "STANDARDIZED AMOUNT",
    ),
    "operating_partial_update": (
        "PARTIAL UPDATE",
        "REDUCED MARKET BASKET",
        "REDUCED UPDATE",
    ),
    "labor_share": (
        "LABOR-RELATED SHARE",
        "LABOR RELATED SHARE",
        "LABOR SHARE",
    ),
    "nonlabor_share": (
        "NONLABOR-RELATED SHARE",
        "NONLABOR RELATED SHARE",
        "NONLABOR SHARE",
    ),
    "capital_federal_rate": (
        "CAPITAL FEDERAL RATE",
        "FEDERAL CAPITAL RATE",
        "FEDERAL RATE",
        "NATIONAL CAPITAL FEDERAL RATE",
    ),
    "outlier_threshold": (
        "OUTLIER THRESHOLD",
        "FIXED-LOSS COST THRESHOLD",
        "FIXED LOSS THRESHOLD",
        "FIXED LOSS COST OUTLIER THRESHOLD",
    ),
    "wage_index_anchor": ("WAGE INDEX", "AVERAGE WAGE INDEX"),
    "cola_factor": ("COLA", "COST OF LIVING"),
    "national_avg_hourly_wage": (
        "AVERAGE HOURLY WAGE",
        "NATIONAL AVERAGE HOURLY WAGE",
        "NATIONAL AVG HOURLY WAGE",
    ),
    "ltch_standard_rate": (
        "LTCH",
        "LTCH PPS STANDARD",
        "STANDARD FEDERAL RATE",
    ),
}


# Unit map keyed by canonical line_item name.
_TABLE1_UNIT_MAP: dict[str, str] = {
    "operating_full_update": "dollars",
    "operating_partial_update": "dollars",
    "labor_share": "ratio",
    "nonlabor_share": "ratio",
    "capital_federal_rate": "dollars",
    "outlier_threshold": "dollars",
    "wage_index_anchor": "factor",
    "cola_factor": "factor",
    "national_avg_hourly_wage": "dollars_per_hour",
    "ltch_standard_rate": "dollars",
}


def _classify_line_item(label: str) -> str | None:
    """Return canonical line_item name for ``label`` or None on no match."""
    norm = _normalize_header(label)
    if not norm:
        return None
    for canonical, aliases in _TABLE1_LINE_ITEM_ALIASES.items():
        for alias in aliases:
            if alias.upper() in norm:
                return canonical
    return None


def _classify_category(context: str) -> str:
    """Infer category (national / rural / urban / puerto_rico / alaska_hawaii).

    ``context`` is a free-text concatenation of label + nearby header cells.
    Defaults to ``"national"`` when no positive signal found.
    """
    norm = _normalize_header(context)
    if "PUERTO RICO" in norm:
        return "puerto_rico"
    if "ALASKA" in norm or "HAWAII" in norm:
        return "alaska_hawaii"
    if "RURAL" in norm:
        return "rural"
    if "URBAN" in norm:
        return "urban"
    if " PR " in f" {norm} " or norm.endswith(" PR") or norm.startswith("PR "):
        return "puerto_rico"
    return "national"


# ----------------------------------------------------------------------------
# ZIP / member discovery
# ----------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    """Open a ZIP file with a corruption guard."""
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"ipps: corrupt ZIP at {path}: {exc}") from exc


def _extract_member_to_path(
    zf: zipfile.ZipFile, member: str, dest: Path
) -> Path:
    """Extract a ZIP member to ``dest`` (idempotent — skip if already present)."""
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


def _is_text_or_xlsx_member(name: str) -> bool:
    """Predicate: does this member name end in .xlsx / .xls / .txt / .csv?"""
    suf = Path(name).suffix.lower()
    return suf in (".xlsx", ".xls", ".txt", ".csv")


def _is_disclaimer_member(name: str) -> bool:
    """Drop classic CMS disclaimer / readme / table-of-contents files.

    Pre-FY2013 IPPS Tables 1A-1E ZIPs ship a "Description.txt" / "Descriptions.
    txt" / "Description.xls" sibling that's a one-line table of contents,
    NOT data. If we don't filter it, the .txt-preferred selector picks
    "Description.txt" over "Table 1A-1D.txt" (both rank as .txt) and emits
    empty parquets. Filter here so the candidate list contains only data
    members.
    """
    lower = Path(name).name.lower()
    if lower.startswith("__macosx") or "/__macosx/" in name.lower():
        return True
    if lower in {"readme.txt", "readme.pdf"}:
        return True
    if lower.endswith(".pdf"):
        return True
    # Description / index / TOC sentinels — never carry table data.
    stem = Path(lower).stem
    if stem in {"description", "descriptions", "index", "toc", "contents"}:
        return True
    return False


def _candidate_primary_members(zf: zipfile.ZipFile) -> list[str]:
    """Return the list of members eligible to be primary data files.

    Filters out auxiliary disclaimers / PDFs and keeps only XLSX/TXT/CSV.
    """
    out: list[str] = []
    for m in zf.namelist():
        if m.endswith("/"):
            continue  # directory entry
        if _is_disclaimer_member(m):
            continue
        if not _is_text_or_xlsx_member(m):
            continue
        out.append(m)
    return out


def _find_table5_member(zf: zipfile.ZipFile) -> str:
    """Locate the Table 5 main data member inside the ZIP.

    Priority:
        P1: filename containing ``Table 5`` (case-insensitive) AND extension
            in (.xlsx, .xls, .txt). Prefer xlsx > xls > txt.
        P2: any single XLSX (when there's exactly one).
        P3: any single TXT.

    Raises RuntimeError if no plausible member is found.
    """
    members = _candidate_primary_members(zf)
    if not members:
        raise RuntimeError(
            f"ipps: no XLSX/TXT/CSV members found in ZIP "
            f"(saw {zf.namelist()[:10]})"
        )

    # P1: "Table 5" substring.
    p1 = [
        m for m in members if "TABLE 5" in Path(m).name.upper()
    ]
    # FY2009 has stray "_-" in archive basename, but the inner member is
    # typically `Table 5.txt` or `table5_fy09.xlsx` — the P1 substring
    # catches both.
    if not p1:
        # Some FYs (FY2008-FY2010) use underscores: `table_5`, `table5`.
        p1 = [
            m
            for m in members
            if re.search(r"TABLE[\s_-]*5", Path(m).name.upper())
        ]

    if p1:
        # Prefer xlsx > xls > txt.
        ranked = sorted(
            p1, key=lambda m: _ext_rank(Path(m).suffix.lower())
        )
        return ranked[0]

    # P2: any single XLSX.
    xlsx = [m for m in members if Path(m).suffix.lower() == ".xlsx"]
    if len(xlsx) == 1:
        return xlsx[0]
    if xlsx:
        # Prefer FR (Final Rule) basename on collisions.
        fr = [m for m in xlsx if "_FR_" in Path(m).name.upper() or "-FR-" in Path(m).name.upper()]
        if fr:
            return fr[0]
        return xlsx[0]

    # P3: any single TXT.
    txt = [m for m in members if Path(m).suffix.lower() == ".txt"]
    if txt:
        return txt[0]

    raise RuntimeError(
        f"ipps: could not locate Table 5 member in ZIP; saw {members}"
    )


def _ext_rank(suffix: str) -> int:
    """Sort key for selecting among ZIP members of the same logical role.

    Order (lower = preferred): ``.xlsx > .txt > .xls > .csv``.

    Why ``.txt`` is preferred over legacy ``.xls``: CMS pre-FY2013 ``.xls``
    workbooks ship a Description sheet at index 0 with the actual data on
    sheet 1 ("Table 1A-1D" / per-half-period sheets for FY2010). The .txt
    sibling is the same content stacked vertically with section markers,
    needs no sheet selection, and is robust against the dual-period FY2010
    structure (which spreads two data sheets across one .xls). When the
    ZIP ships only ``.xls`` and no ``.txt`` (rare for IPPS), the .xls
    falls through and the xlrd-based reader handles it.
    """
    return {".xlsx": 0, ".txt": 1, ".xls": 2, ".csv": 3}.get(suffix, 9)


def _find_tables1_members(
    zf: zipfile.ZipFile, fy: int
) -> dict[str, list[str]]:
    """Return mapping of table-letter -> ordered list of candidate members.

    Two distribution modes are detected:

    * Mode A — bundled stacked file. The ZIP contains one (or two for
      FY2010 dual-period) primary member(s). Each primary member is a
      single XLSX/TXT whose content is **all 5 tables stacked vertically**
      with ``TABLE 1[A-E].`` section markers. Returned as
      ``{"__BUNDLED__": [primary_member_1, primary_member_2, ...]}``.
      Note: many ZIPs ship the same content in BOTH ``.xlsx`` and ``.txt``
      formats — the XLSX is preferred (higher fidelity dates / numeric types).

    * Mode B — per-letter files. Each member is named after its table
      letter (e.g. ``Table 1A.xlsx``). One key per letter present.

    For FY2008/FY2009, table letter ``"E"`` will be absent — the absence is
    silently fine (caller logs+skips).

    For FY2016 specifically, multiple variants (FR / CN / CCC / CAA) may
    coexist; this function prefers ``_FR_`` (Final Rule) basenames and emits
    a stderr warning when alternative variants are present.
    """
    members = _candidate_primary_members(zf)
    if not members:
        raise RuntimeError(
            f"ipps fy{fy} tables1: no candidate members in ZIP "
            f"(saw {zf.namelist()[:10]})"
        )

    # Filter FR variants first (FY2016 ships CN / CCC / CAA siblings).
    fr_members = [
        m
        for m in members
        if "_FR_" in Path(m).name.upper() or "-FR-" in Path(m).name.upper()
    ]
    non_fr = [m for m in members if m not in fr_members]
    if non_fr and fr_members:
        non_fr_names = [Path(m).name for m in non_fr]
        print(
            f"[ipps] fy{fy}: skipping non-FR variant(s) {non_fr_names}; "
            f"keeping FR-only members {[Path(m).name for m in fr_members]}",
            file=sys.stderr,
        )

    primary = fr_members if fr_members else members

    # Mode B detection: ANY member name contains a per-letter "Table 1{X}"
    # pattern with X in {A, B, C, D, E} but NOT spanning multiple letters
    # (e.g. "Table 1A - 1E" or "Tables 1A-1E" indicates Mode A bundled
    # content, not per-letter files). We filter those out first.
    bundle_re = re.compile(
        r"TABLE[S]?[\s_-]*1[A-E][\s_-]*[-—][\s_-]*1[A-E]", re.IGNORECASE
    )
    per_letter_candidates: dict[str, list[str]] = {}
    bundled_candidates: list[str] = []
    for m in primary:
        name = Path(m).name
        upper = name.upper()
        if bundle_re.search(upper):
            bundled_candidates.append(m)
            continue
        # Try per-letter match.
        matched_letter: str | None = None
        for letter in ("A", "B", "C", "D", "E"):
            if re.search(rf"TABLE[\s_-]*1{letter}\b", upper):
                matched_letter = letter
                break
        if matched_letter is not None:
            per_letter_candidates.setdefault(matched_letter, []).append(m)
        else:
            # Member doesn't match either pattern -- treat as a bundled
            # candidate (some legacy ZIPs use generic basenames).
            bundled_candidates.append(m)

    # Mode-A: prefer if any bundled-content member exists.
    if bundled_candidates:
        # Group by rate-period (FY2010 1st/2nd half), then prefer xlsx > xls
        # > csv > txt within each period bucket.
        period_buckets: dict[str | None, list[str]] = {}
        for m in bundled_candidates:
            rp = _detect_rate_period(Path(m).name)
            period_buckets.setdefault(rp, []).append(m)
        chosen: list[str] = []
        for rp, cand_list in period_buckets.items():
            cand_sorted = sorted(
                cand_list, key=lambda m: _ext_rank(Path(m).suffix.lower())
            )
            chosen.append(cand_sorted[0])
        return {"__BUNDLED__": chosen}

    # Mode B fallback: per-letter members only.
    out: dict[str, list[str]] = {}
    for letter, matches in per_letter_candidates.items():
        # Prefer xlsx > xls > csv > txt.
        matches_ranked = sorted(
            matches, key=lambda m: _ext_rank(Path(m).suffix.lower())
        )
        out[letter] = matches_ranked

    return out


# ----------------------------------------------------------------------------
# File reading: XLSX vs delimited
# ----------------------------------------------------------------------------


def _read_xlsx_rows(
    path: Path, *, sheet_name: str | None = None
) -> list[list[object]]:
    """Read an XLSX file into a list-of-lists of cell values.

    If ``sheet_name`` is provided, reads that worksheet; else the active
    (default) worksheet. Uses openpyxl read-only / data-only mode so any
    cached formula values are returned, never the formula text.
    """
    try:
        wb = openpyxl.load_workbook(
            str(path), read_only=True, data_only=True
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"ipps: failed to load XLSX {path}: {exc}"
        ) from exc
    try:
        if sheet_name is not None:
            if sheet_name not in wb.sheetnames:
                raise RuntimeError(
                    f"ipps: sheet {sheet_name!r} not found in {path} "
                    f"(saw {wb.sheetnames})"
                )
            ws = wb[sheet_name]
        else:
            ws = wb.active
        rows: list[list[object]] = []
        for tup in ws.iter_rows(values_only=True):
            rows.append(list(tup))
        return rows
    finally:
        wb.close()


def _xlsx_sheetnames(path: Path) -> list[str]:
    """Return the worksheet names in ``path`` (read-only mode)."""
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        return list(wb.sheetnames)
    finally:
        wb.close()


def _read_delimited_rows(path: Path) -> list[list[object]]:
    """Read a tab- (.txt) or comma- (.csv) delimited file into rows.

    Tolerates a UTF-8 BOM (``utf-8-sig``); falls back to Latin-1.
    """
    delimiter = "\t" if path.suffix.lower() == ".txt" else ","
    encodings = ("utf-8-sig", "latin-1")
    last_exc: Exception | None = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [
                    list(row) for row in csv.reader(fh, delimiter=delimiter)
                ]
        except UnicodeDecodeError as exc:  # noqa: PERF203
            last_exc = exc
            continue
    raise RuntimeError(
        f"ipps: could not decode {path} with any of {encodings}: {last_exc}"
    )


def _read_xls_rows(
    path: Path, *, sheet_name: str | None = None
) -> list[list[object]]:
    """Read a legacy ``.xls`` (binary) workbook via ``xlrd``.

    openpyxl rejects the BIFF-format .xls files that CMS shipped pre-FY2013
    for IPPS Final Rule artifacts. xlrd<2 (already a project dep for HCPCS
    Level II) is the canonical reader for this format.
    """
    try:
        import xlrd
    except ImportError as exc:  # pragma: no cover -- xlrd is in deps
        raise RuntimeError(
            f"ipps: cannot read {path}: xlrd is not installed "
            "(should be in pyproject.toml as 'xlrd<2')"
        ) from exc
    try:
        wb = xlrd.open_workbook(str(path))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"ipps: failed to load XLS {path}: {exc}") from exc
    try:
        if sheet_name is not None:
            try:
                ws = wb.sheet_by_name(sheet_name)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"ipps: sheet {sheet_name!r} not found in {path} "
                    f"(saw {wb.sheet_names()})"
                ) from exc
        else:
            ws = wb.sheet_by_index(0)
        rows: list[list[object]] = []
        for r in range(ws.nrows):
            row: list[object] = []
            for c in range(ws.ncols):
                cell = ws.cell(r, c)
                ctype = cell.ctype
                # xlrd type codes: 0=empty, 1=text, 2=number, 3=date,
                # 4=bool, 5=error, 6=blank.
                if ctype in (0, 6):
                    row.append("")
                elif ctype == 2:
                    # Numeric: preserve as float (downstream parses ints
                    # from string when it expects them, e.g., MS-DRG codes).
                    val = cell.value
                    # CMS embeds MS-DRG codes as text in .xlsx, but as
                    # number in some .xls workbooks. Coerce small
                    # whole-number values to int representation when
                    # rendered downstream — cell.value remains float here,
                    # the upstream parsers handle the str() conversion.
                    if val == int(val):
                        row.append(str(int(val)))
                    else:
                        row.append(val)
                elif ctype == 3:
                    # Date: convert to datetime.date — IPPS doesn't expect
                    # date cells but pass through cleanly.
                    try:
                        from datetime import datetime as _dt
                        tup = xlrd.xldate_as_tuple(cell.value, wb.datemode)
                        row.append(_dt(*tup).date())
                    except Exception:  # noqa: BLE001
                        row.append(cell.value)
                else:
                    row.append(cell.value)
            rows.append(row)
        return rows
    finally:
        wb.release_resources()


def _xls_sheetnames(path: Path) -> list[str]:
    """Return the worksheet names in a legacy .xls file via xlrd."""
    import xlrd

    wb = xlrd.open_workbook(str(path), on_demand=True)
    try:
        return list(wb.sheet_names())
    finally:
        wb.release_resources()


def _read_member_rows(
    path: Path, *, sheet_name: str | None = None
) -> list[list[object]]:
    """Dispatch on file extension to XLSX, XLS, or delimited reader."""
    suf = path.suffix.lower()
    if suf == ".xlsx":
        return _read_xlsx_rows(path, sheet_name=sheet_name)
    if suf == ".xls":
        return _read_xls_rows(path, sheet_name=sheet_name)
    if suf in (".txt", ".csv"):
        return _read_delimited_rows(path)
    raise RuntimeError(f"ipps: unsupported member extension {suf!r} for {path}")


def _pick_fr_sheet(path: Path) -> str | None:
    """Pick the canonical Final-Rule (FR) sheet from a multi-sheet workbook.

    Some IPPS Final Rule bundled XLSXs ship the FR (Final Rule) and CN
    (Correction Notice) variants as TWO sheets in ONE workbook. openpyxl
    (and xlrd) default to the workbook's "active" sheet, which CMS
    occasionally leaves set to the CN variant.

    Two distinct read paths surface this:

    - **Tables 1A-1E** (FY2015 ``FY_2015_FR_AND_CN_Table 1A - 1E.xlsx``):
      the CN sheet only carries Tables 1A-1D (no 1E), so a naive
      ``wb.active`` read silently misses Table 1E entirely.
    - **Table 5** (FY2015 / FY2016 / FY2018 / FY2020 / FY2021 / FY2025):
      both sheets carry the same DRG list, but ``relative_weight`` values
      differ at cents level between the FR and CN columns. A naive
      ``wb.active`` read silently emits CN-revised weights instead of the
      canonical FR values.

    Picker logic:
        1. If the workbook has 0 or 1 sheets → return None (caller uses the
           default active sheet — preserves prior behavior for single-sheet
           XLSXs and for any XLS legacy reads).
        2. If exactly one sheet name contains the token ``FR`` (case-insensitive,
           word-boundary-ish), prefer it.
        3. If multiple sheets carry FR markers, prefer the one whose name
           encodes the widest table-letter range (e.g. ``Table 1A-1E FR``
           beats ``Table 1A-1D FR``). Harmless no-op when the workbook
           contains no per-letter range markers (Table 5 case).
        4. If no FR-marked sheet is identifiable, fall back to None
           (caller uses default active sheet — no change vs. prior behavior).

    Returns the chosen sheet name, or None to defer to the caller's default.
    """
    suf = path.suffix.lower()
    if suf == ".xlsx":
        try:
            sheetnames = _xlsx_sheetnames(path)
        except Exception:  # noqa: BLE001
            return None
    elif suf == ".xls":
        try:
            sheetnames = _xls_sheetnames(path)
        except Exception:  # noqa: BLE001
            return None
    else:
        return None

    if len(sheetnames) <= 1:
        return None

    fr_re = re.compile(r"(?:^|[\s_\-])FR(?:$|[\s_\-])", re.IGNORECASE)
    range_re = re.compile(r"1\s*([A-E])\s*[-–—]\s*1?\s*([A-E])", re.IGNORECASE)

    fr_sheets = [s for s in sheetnames if fr_re.search(s)]
    if not fr_sheets:
        return None

    if len(fr_sheets) == 1:
        return fr_sheets[0]

    # Tiebreak: prefer widest letter range (e.g. 1A-1E > 1A-1D).
    def _span(name: str) -> int:
        m = range_re.search(name)
        if not m:
            return 0
        a, b = m.group(1).upper(), m.group(2).upper()
        return ord(b) - ord(a)

    fr_sheets.sort(key=lambda s: (-_span(s), sheetnames.index(s)))
    return fr_sheets[0]


# Backward-compat alias: prior name documented in CLAUDE.md and referenced
# in test docstrings. The picker's responsibility broadened beyond Tables
# 1A-1E (now also drives Table 5's FR-sheet selection); the new name
# ``_pick_fr_sheet`` is canonical going forward.
_pick_bundled_tables1_sheet = _pick_fr_sheet


# ----------------------------------------------------------------------------
# Cell-level value parsers
# ----------------------------------------------------------------------------


def _parse_money_or_float(value: object) -> float | None:
    """Parse a numeric cell, stripping ``$`` / commas / ``%`` / whitespace.

    Returns None for empty / sentinel cells (``NA``, ``--``, ``.``, ``-``).
    Returns None for unparseable strings.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (value != value):  # NaN check
            return None
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    if s.upper() in {"NA", "N/A", "NULL", "--", "-", "."}:
        return None
    s = s.replace("$", "").replace(",", "").replace("%", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_text(value: object) -> str | None:
    """Strip whitespace from a free-text column. Return None if empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _parse_ms_drg(value: object) -> str | None:
    """Normalize an MS-DRG cell to 3-digit zero-padded string, or None."""
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        if value.is_integer():
            s = str(int(value))
        else:
            s = str(value)
    elif isinstance(value, int):
        s = str(value)
    else:
        s = str(value).strip()
    s = s.strip()
    if not s:
        return None
    if not _MS_DRG_RE.match(s):
        return None
    return s.zfill(3)


def _parse_mdc(value: object) -> str | None:
    """Normalize an MDC cell.

    ``"PRE"`` is preserved verbatim. Numeric values are zero-padded to 2
    digits. Anything else returns None.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:
            return None
        if value.is_integer():
            s = str(int(value))
        else:
            s = str(value)
    elif isinstance(value, int):
        s = str(value)
    else:
        s = str(value).strip()
    s = s.strip().upper()
    if not s:
        return None
    if s == "PRE":
        return "PRE"
    if s.isdigit():
        n = int(s)
        if 0 <= n <= 99:
            return f"{n:02d}"
    return None


def _parse_drg_type(value: object) -> str | None:
    """Normalize MED/SURG indicator: ``"MED"`` / ``"SURG"`` or None.

    CMS uses ``"M"`` / ``"S"`` in some FYs; map them.
    """
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    if s in {"MED", "M", "MEDICAL"}:
        return "MED"
    if s in {"SURG", "S", "SURGICAL"}:
        return "SURG"
    return None


def _parse_post_acute_flag(value: object) -> str | None:
    """Normalize post-acute transfer flag: yes / no / special / None."""
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    if s in {"YES", "Y"}:
        return "yes"
    if s in {"NO", "N"}:
        return "no"
    if s in {"SPECIAL", "SPEC", "SPECIAL PAYMENT"} or "SPECIAL" in s:
        return "special"
    return None


def _infer_severity(title: str | None) -> str | None:
    """Infer DRG severity from the title text via regex match."""
    if not title:
        return None
    t = title.upper()
    has_mcc = "WITH MCC" in t
    has_with_cc = "WITH CC" in t  # note: "WITH MCC" matches this too
    has_without_mcc = "WITHOUT MCC" in t
    has_without_cc_and_mcc = (
        "WITHOUT CC/MCC" in t or "W/O CC/MCC" in t
    )
    has_without_cc = "WITHOUT CC" in t

    if has_without_cc_and_mcc:
        return "NoCC"
    if has_without_mcc and has_without_cc:
        return "NoCC"
    if has_mcc and not has_without_mcc:
        return "MCC"
    # "WITH CC" (and not "WITH MCC")
    if has_with_cc and not has_mcc:
        return "CC"
    return None


# ----------------------------------------------------------------------------
# Per-row dataclasses
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParsedTable5Row:
    ms_drg: str
    title: str
    mdc: str | None
    drg_type: str | None
    severity: str | None
    relative_weight: float | None
    gmlos: float | None
    amlos: float | None
    post_acute_transfer_flag: str | None


@dataclass(frozen=True)
class _ParsedTable1Row:
    table: str  # "1A" / "1B" / "1C" / "1D" / "1E"
    line_item: str
    category: str | None
    rate_period: str | None
    value: float
    unit: str


# ----------------------------------------------------------------------------
# Table 5 parser
# ----------------------------------------------------------------------------


def _resolve_table5_columns(header: list[object]) -> dict[str, int]:
    """Resolve column indices for a Table 5 header row.

    Substring match on ``_normalize_header(cell)`` against the alias table.
    First match wins for each logical column.

    Required: ``ms_drg``, ``title``, ``relative_weight``. Best-effort for
    the rest.
    """
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}
    for logical, aliases in _TABLE5_HEADER_ALIASES.items():
        for i, h in enumerate(norm):
            if not h:
                continue
            for alias in aliases:
                if alias.upper() in h:
                    out[logical] = i
                    break
            if logical in out:
                break

    missing = [c for c in _TABLE5_REQUIRED_COLS if c not in out]
    if missing:
        raise RuntimeError(
            f"ipps table5: required column(s) {missing} not found in header; "
            f"saw {norm[:20]}..."
        )
    return out


def _find_table5_header_index(rows: list[list[object]]) -> int:
    """Return the 0-indexed row whose first non-empty cell matches MS-DRG.

    The title banner row that precedes the column header (e.g. "TABLE 5.—
    LIST OF MEDICARE SEVERITY DIAGNOSIS-RELATED GROUPS (MS-DRGS), ...") is
    rejected by a length cap on the normalized cell; the substring fallback
    is also kept tight (no bare "DRG", which matches "MS-DRGS" inside the
    banner). Real Table 5 column headers are at most ~10 characters in the
    first cell ("MS-DRG", "MS-DRG ", "MS DRG", "MSDRG").
    """
    primary_aliases = {"MS-DRG", "MSDRG", "MS DRG"}
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        for cell in row:
            norm = _normalize_header(cell)
            if not norm:
                continue
            # Reject banner rows by length cap. Header cells are short.
            if len(norm) > 30:
                break
            # Exact match against primary aliases.
            if norm in primary_aliases:
                return i
            # Substring fallback for stacked-header variants like
            # "MS-DRG CODE" or "MS-DRG NUMBER" collapsed by _normalize_header.
            if any(a in norm for a in primary_aliases):
                return i
            break  # only check first non-empty cell per row
    raise RuntimeError(
        f"ipps table5: header row not found in first 30 rows; "
        f"first-row first-cells seen: "
        f"{[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


def _parse_table5_rows(
    rows: list[list[object]], *, fy: int
) -> list[_ParsedTable5Row]:
    """Parse Table 5 rows into ``_ParsedTable5Row`` records.

    Filter rules:
        * Skip rows where ``ms_drg`` doesn't match ``^[0-9]{1,3}$`` after
          stripping (footer / blank rows).
        * Reserved DRGs ``998`` / ``999`` are EMITTED with a stderr warning;
          they're CMS reserved-non-payable.
        * Rows where ms_drg is ``"NA"`` / ``"-"`` (placeholder) are silently
          dropped (some FYs ship them).
    """
    if not rows:
        raise RuntimeError(f"ipps fy{fy} table5: file parsed to zero rows")

    header_idx = _find_table5_header_index(rows)
    header = list(rows[header_idx])
    cols = _resolve_table5_columns(header)

    out: list[_ParsedTable5Row] = []
    reserved_seen: list[str] = []
    for raw in rows[header_idx + 1 :]:
        if not raw:
            continue
        # Footer guard: if the first non-empty cell starts with a known
        # disclaimer prefix, terminate the data section (footers tend to
        # appear at the END of the table).
        first_nonempty: str | None = None
        for cell in raw:
            text = _normalize_header(cell)
            if text:
                first_nonempty = text
                break
        if first_nonempty is None:
            continue
        if any(first_nonempty.startswith(p) for p in _FOOTER_PREFIXES):
            continue

        if cols["ms_drg"] >= len(raw):
            continue
        ms_drg = _parse_ms_drg(raw[cols["ms_drg"]])
        if ms_drg is None:
            continue
        if ms_drg in _RESERVED_DRGS:
            reserved_seen.append(ms_drg)

        title = (
            _parse_text(raw[cols["title"]])
            if cols["title"] < len(raw)
            else None
        )
        if title is None:
            # Title is required by schema; skip silently.
            continue

        mdc = (
            _parse_mdc(raw[cols["mdc"]])
            if "mdc" in cols and cols["mdc"] < len(raw)
            else None
        )
        drg_type = (
            _parse_drg_type(raw[cols["drg_type"]])
            if "drg_type" in cols and cols["drg_type"] < len(raw)
            else None
        )
        relative_weight = (
            _parse_money_or_float(raw[cols["relative_weight"]])
            if cols["relative_weight"] < len(raw)
            else None
        )
        gmlos = (
            _parse_money_or_float(raw[cols["gmlos"]])
            if "gmlos" in cols and cols["gmlos"] < len(raw)
            else None
        )
        amlos = (
            _parse_money_or_float(raw[cols["amlos"]])
            if "amlos" in cols and cols["amlos"] < len(raw)
            else None
        )
        post_acute = (
            _parse_post_acute_flag(raw[cols["post_acute_transfer_flag"]])
            if "post_acute_transfer_flag" in cols
            and cols["post_acute_transfer_flag"] < len(raw)
            else None
        )
        severity = _infer_severity(title)

        out.append(
            _ParsedTable5Row(
                ms_drg=ms_drg,
                title=title,
                mdc=mdc,
                drg_type=drg_type,
                severity=severity,
                relative_weight=relative_weight,
                gmlos=gmlos,
                amlos=amlos,
                post_acute_transfer_flag=post_acute,
            )
        )

    if reserved_seen:
        print(
            f"[ipps] fy{fy} table5: emitted {len(reserved_seen)} reserved "
            f"DRG row(s) {sorted(set(reserved_seen))} (CMS-reserved 998/999)",
            file=sys.stderr,
        )

    return out


# ----------------------------------------------------------------------------
# Tables 1A-1E parser
# ----------------------------------------------------------------------------


# Regex matching a "TABLE 1A.", "TABLE 1B,", "TABLE 1C-", "TABLE 1D ", etc.
# section marker. Run against ``_normalize_header(cell)`` (uppercase + collapsed
# whitespace), so we only need to tolerate the punctuation/spacing immediately
# following the letter.
_TABLE_SECTION_MARKER_RE: re.Pattern[str] = re.compile(
    r"^TABLE 1([A-E])(?:\b|[\s.,;:\-—])"
)

# Regex extracting "66.0 PERCENT LABOR SHARE" / "34.0 PERCENT NONLABOR SHARE".
_LABOR_SHARE_RE: re.Pattern[str] = re.compile(
    r"([0-9]+(?:\.[0-9]+)?)\s*PERCENT\s*LABOR\s*SHARE", re.IGNORECASE
)
_NONLABOR_SHARE_RE: re.Pattern[str] = re.compile(
    r"([0-9]+(?:\.[0-9]+)?)\s*PERCENT\s*NONLABOR\s*SHARE", re.IGNORECASE
)


def _row_to_text(row: list[object]) -> str:
    """Join a row's non-empty cell text values with single spaces."""
    parts: list[str] = []
    for cell in row:
        text = _parse_text(cell)
        if text is None:
            continue
        parts.append(text)
    return " ".join(parts)


def _section_marker_letter(row: list[object]) -> str | None:
    """If ``row`` begins with a ``TABLE 1[A-E]`` marker, return the letter.

    Walks all cells in the row (some sheets ship the marker in column 0,
    others in a wrapped cell that ``_normalize_header`` collapses). Returns
    None if no marker matches.
    """
    if not row:
        return None
    for cell in row:
        norm = _normalize_header(cell)
        if not norm:
            continue
        m = _TABLE_SECTION_MARKER_RE.match(norm)
        if m:
            return m.group(1).upper()
        # First non-empty cell did not match -- this row isn't a marker.
        return None
    return None


def _split_into_sections(
    rows: list[list[object]],
) -> list[tuple[str, str, list[list[object]]]]:
    """Split a stacked-tables sheet into per-section row groups.

    Returns a list of ``(letter, banner_text, section_rows)`` tuples in
    document order. ``banner_text`` is the full text of the marker row
    (used by 1A/1B for labor-share regex extraction). ``section_rows``
    starts AT the marker row (so the section extractors can re-read the
    banner if needed) and runs up to (but not including) the next
    marker row, end-of-rows.
    """
    # First pass: identify marker indices.
    markers: list[tuple[int, str, str]] = []  # (idx, letter, banner_text)
    for i, row in enumerate(rows):
        letter = _section_marker_letter(row)
        if letter is None:
            continue
        markers.append((i, letter, _row_to_text(row)))

    sections: list[tuple[str, str, list[list[object]]]] = []
    for k, (idx, letter, banner) in enumerate(markers):
        end_idx = markers[k + 1][0] if k + 1 < len(markers) else len(rows)
        sections.append((letter, banner, rows[idx:end_idx]))
    return sections


def _first_numeric_data_row(
    section_rows: list[list[object]], *, start_idx: int = 1
) -> tuple[int, list[object]] | None:
    """Locate the first row in a section whose cells contain >=1 numeric value.

    Used by the simple-shape sections (1A/1B/1C/1D/1E). Returns
    ``(absolute_index_within_section, row)`` or None.
    """
    for i in range(start_idx, len(section_rows)):
        row = section_rows[i]
        if not row:
            continue
        # Footer guard: label-only rows starting with disclaimer prefixes.
        first_norm: str | None = None
        for cell in row:
            t = _normalize_header(cell)
            if t:
                first_norm = t
                break
        if first_norm is None:
            continue
        if any(first_norm.startswith(p) for p in _FOOTER_PREFIXES):
            continue
        # Count numeric cells.
        for cell in row:
            if _parse_money_or_float(cell) is not None and not isinstance(cell, str):
                return (i, row)
            # Also allow numeric strings like "4456.72".
            if isinstance(cell, str) and _parse_money_or_float(cell) is not None:
                # Only count if not the leading label cell (some 1C/1D rows
                # have a label in col 0 + numeric in col 1+). Just return
                # the row -- caller iterates all cells.
                return (i, row)
    return None


def _emit_labor_share_rows(
    banner: str,
    *,
    table: str,
    category: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Parse ``"66 PERCENT LABOR SHARE / 34 PERCENT NONLABOR SHARE"`` from banner.

    Emits zero, one, or two rows depending on which percentages are present.
    Values are normalized to ratios (``0.66`` / ``0.34``).
    """
    out: list[_ParsedTable1Row] = []
    norm = banner.upper()
    m_labor = _LABOR_SHARE_RE.search(norm)
    if m_labor:
        try:
            pct = float(m_labor.group(1))
            out.append(
                _ParsedTable1Row(
                    table=table,
                    line_item="labor_share",
                    category=category,
                    rate_period=rate_period,
                    value=pct / 100.0,
                    unit="ratio",
                )
            )
        except ValueError:
            pass
    m_non = _NONLABOR_SHARE_RE.search(norm)
    if m_non:
        try:
            pct = float(m_non.group(1))
            out.append(
                _ParsedTable1Row(
                    table=table,
                    line_item="nonlabor_share",
                    category=category,
                    rate_period=rate_period,
                    value=pct / 100.0,
                    unit="ratio",
                )
            )
        except ValueError:
            pass
    return out


def _extract_table_1a_section(
    section_rows: list[list[object]],
    *,
    banner: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Tables 1A: National wage-index > 1.

    Layout (FY2026 reference):
        Row 0: TABLE 1A. ... banner
        Rows 1-2: stacked headers (4 update tiers x 2 labor/nonlabor)
        Row 3: 8 numeric values

    Emit:
        * operating_full_update (category=national, unit=dollars) =
          labor + nonlabor of the FIRST tier (index 0+1).
        * labor_share / nonlabor_share rows from banner regex.
    """
    out: list[_ParsedTable1Row] = []
    out.extend(
        _emit_labor_share_rows(
            banner, table="1A", category="national", rate_period=rate_period
        )
    )

    found = _first_numeric_data_row(section_rows, start_idx=1)
    if found is not None:
        _, row = found
        nums = [v for v in (_parse_money_or_float(c) for c in row) if v is not None]
        if len(nums) >= 2:
            total = nums[0] + nums[1]  # labor + nonlabor of the first tier
            out.append(
                _ParsedTable1Row(
                    table="1A",
                    line_item="operating_full_update",
                    category="national",
                    rate_period=rate_period,
                    value=total,
                    unit="dollars",
                )
            )
    return out


def _extract_table_1b_section(
    section_rows: list[list[object]],
    *,
    banner: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Tables 1B: National wage-index <= 1.

    Same layout as 1A. Emitted with category=rural (the schema's vocabulary
    uses `rural` for the wage-index-low-side amount).
    """
    out: list[_ParsedTable1Row] = []
    out.extend(
        _emit_labor_share_rows(
            banner, table="1B", category="rural", rate_period=rate_period
        )
    )

    found = _first_numeric_data_row(section_rows, start_idx=1)
    if found is not None:
        _, row = found
        nums = [v for v in (_parse_money_or_float(c) for c in row) if v is not None]
        if len(nums) >= 2:
            total = nums[0] + nums[1]
            out.append(
                _ParsedTable1Row(
                    table="1B",
                    line_item="operating_full_update",
                    category="rural",
                    rate_period=rate_period,
                    value=total,
                    unit="dollars",
                )
            )
    return out


def _extract_table_1c_section(
    section_rows: list[list[object]],
    *,
    banner: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Tables 1C: Puerto Rico variant.

    Layout (FY2026 reference):
        Row 0: TABLE 1C. ... banner
        Rows 1-2: stacked headers (3 tiers x labor/nonlabor)
        Row 3: ["National1", "Not Applicable", "Not Applicable", "4186.62",
                "2565.99", "4085.63", "2504.09"]

    Emit operating_full_update with category=puerto_rico, taking the first
    pair of numeric cells in the data row (skipping "Not Applicable"
    placeholders).
    """
    out: list[_ParsedTable1Row] = []

    found = _first_numeric_data_row(section_rows, start_idx=1)
    if found is not None:
        _, row = found
        nums = [v for v in (_parse_money_or_float(c) for c in row) if v is not None]
        if len(nums) >= 2:
            total = nums[0] + nums[1]
            out.append(
                _ParsedTable1Row(
                    table="1C",
                    line_item="operating_full_update",
                    category="puerto_rico",
                    rate_period=rate_period,
                    value=total,
                    unit="dollars",
                )
            )
    return out


def _extract_table_1d_section(
    section_rows: list[list[object]],
    *,
    banner: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Tables 1D: Capital Standard Federal Payment Rate.

    Layout (FY2026 reference):
        Row 0: TABLE 1D. ... banner
        Row 1: ["", "Rate"]
        Row 2: ["National", "524.15"]
    """
    out: list[_ParsedTable1Row] = []

    found = _first_numeric_data_row(section_rows, start_idx=1)
    if found is not None:
        _, row = found
        nums = [v for v in (_parse_money_or_float(c) for c in row) if v is not None]
        if nums:
            out.append(
                _ParsedTable1Row(
                    table="1D",
                    line_item="capital_federal_rate",
                    category="national",
                    rate_period=rate_period,
                    value=nums[0],
                    unit="dollars",
                )
            )
    return out


def _extract_table_1e_section(
    section_rows: list[list[object]],
    *,
    banner: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Tables 1E: LTCH PPS Standard Federal Payment Rate.

    Layout (FY2026 reference):
        Row 0: TABLE 1E - LTCH PPS STANDARD FEDERAL PAYMENT RATE
        Row 1: ["", "", "", "", "Full Update (2.7 Percent)",
                "Reduced Update* (0.7 Percent)"]
        Row 2: ["Standard Federal Rate*", "", "", "", "50824.51", "49834.74"]

    Emit ltch_standard_rate (category=national) using the FULL UPDATE
    value (first numeric cell in the data row).
    """
    out: list[_ParsedTable1Row] = []

    found = _first_numeric_data_row(section_rows, start_idx=1)
    if found is not None:
        _, row = found
        nums = [v for v in (_parse_money_or_float(c) for c in row) if v is not None]
        if nums:
            out.append(
                _ParsedTable1Row(
                    table="1E",
                    line_item="ltch_standard_rate",
                    category="national",
                    rate_period=rate_period,
                    value=nums[0],
                    unit="dollars",
                )
            )
    return out


_SECTION_EXTRACTORS: dict[
    str,
    "callable",  # type: ignore[name-defined]
] = {
    "A": _extract_table_1a_section,
    "B": _extract_table_1b_section,
    "C": _extract_table_1c_section,
    "D": _extract_table_1d_section,
    "E": _extract_table_1e_section,
}


def _parse_tables1_rows(
    rows: list[list[object]],
    *,
    fy: int,
    version: str,
    rate_period: str | None,
) -> list[_ParsedTable1Row]:
    """Parse a stacked Tables 1A-1E sheet into long-form scalar rows.

    Strategy: walk the rows once, segment into sections by ``TABLE 1[A-E]``
    markers, dispatch each section to a per-letter extractor, and return
    a flat list of ``_ParsedTable1Row`` records. Sections that emit zero
    rows are tolerated with a stderr warning. Sections that aren't present
    (e.g. Table 1E for FY2008/FY2009) are silently absent.
    """
    if not rows:
        raise RuntimeError(
            f"ipps fy{fy} {version} tables1: file parsed to zero rows"
        )

    sections = _split_into_sections(rows)
    if not sections:
        raise RuntimeError(
            f"ipps fy{fy} {version} tables1: no `TABLE 1[A-E]` section "
            f"markers found in {len(rows)} rows; first cells seen: "
            f"{[(_normalize_header(r[0]) if r else '') for r in rows[:10]]}"
        )

    out: list[_ParsedTable1Row] = []
    seen_letters: set[str] = set()
    for letter, banner, section_rows in sections:
        seen_letters.add(letter)
        extractor = _SECTION_EXTRACTORS.get(letter)
        if extractor is None:
            print(
                f"[ipps] fy{fy} {version} tables1: unknown section letter "
                f"{letter!r}; skipping",
                file=sys.stderr,
            )
            continue
        section_out = extractor(
            section_rows, banner=banner, rate_period=rate_period
        )
        if not section_out:
            print(
                f"[ipps] fy{fy} {version} table1{letter}: section emitted "
                f"zero rows (banner: {banner[:80]!r})",
                file=sys.stderr,
            )
            continue
        out.extend(section_out)

    return out


def _parse_table1_rows(
    rows: list[list[object]],
    *,
    fy: int,
    table_letter: str,
    rate_period: str | None,
    strict: bool = False,
) -> list[_ParsedTable1Row]:
    """Mode-B fallback parser: ONE table per file (filename hints the letter).

    Wraps ``_parse_tables1_rows`` and filters the output to only the rows
    whose ``table`` matches the caller-supplied ``table_letter``. If the
    section markers are missing (which can happen for some legacy per-letter
    files), synthesize a marker row at the top of ``rows`` so the dispatch
    still works.
    """
    if not rows:
        raise RuntimeError(
            f"ipps fy{fy} table1{table_letter}: file parsed to zero rows"
        )

    # Detect whether the file already contains a marker for this letter.
    sections = _split_into_sections(rows)
    if not any(letter == table_letter.upper() for letter, _, _ in sections):
        # Synthesize a marker row at index 0 so the section dispatch picks
        # up this letter.
        synthetic = [[f"TABLE 1{table_letter.upper()}. (synthesized marker)"]]
        rows = synthetic + list(rows)

    out = _parse_tables1_rows(
        rows, fy=fy, version="(mode-B)", rate_period=rate_period
    )
    out = [r for r in out if r.table == f"1{table_letter.upper()}"]

    if not out:
        msg = (
            f"[ipps] fy{fy} table1{table_letter}: section parser emitted "
            f"zero rows after filtering"
        )
        if strict:
            raise RuntimeError(msg)
        print(msg, file=sys.stderr)

    return out


# ----------------------------------------------------------------------------
# Parquet emission
# ----------------------------------------------------------------------------


def _build_table5_arrow(
    rows: list[_ParsedTable5Row],
    *,
    fy: int,
    version: str,
    eff_start: date,
    eff_end: date,
    source: str,
) -> pa.Table:
    """Project ``_ParsedTable5Row`` records into the locked _TABLE5_SCHEMA."""
    ms_drg_col: list[str] = []
    fy_col: list[int] = []
    ver_col: list[str] = []
    title_col: list[str] = []
    mdc_col: list[str | None] = []
    type_col: list[str | None] = []
    sev_col: list[str | None] = []
    rw_col: list[float | None] = []
    gmlos_col: list[float | None] = []
    amlos_col: list[float | None] = []
    pat_col: list[str | None] = []
    es_col: list[date] = []
    ee_col: list[date] = []
    src_col: list[str] = []

    # Sort by ms_drg for deterministic output.
    rows_sorted = sorted(rows, key=lambda r: r.ms_drg)
    for r in rows_sorted:
        ms_drg_col.append(r.ms_drg)
        fy_col.append(fy)
        ver_col.append(version)
        title_col.append(r.title)
        mdc_col.append(r.mdc)
        type_col.append(r.drg_type)
        sev_col.append(r.severity)
        rw_col.append(r.relative_weight)
        gmlos_col.append(r.gmlos)
        amlos_col.append(r.amlos)
        pat_col.append(r.post_acute_transfer_flag)
        es_col.append(eff_start)
        ee_col.append(eff_end)
        src_col.append(source)

    return pa.Table.from_arrays(
        [
            pa.array(ms_drg_col, type=pa.string()),
            pa.array(fy_col, type=pa.int32()),
            pa.array(ver_col, type=pa.string()),
            pa.array(title_col, type=pa.string()),
            pa.array(mdc_col, type=pa.string()),
            pa.array(type_col, type=pa.string()),
            pa.array(sev_col, type=pa.string()),
            pa.array(rw_col, type=pa.float64()),
            pa.array(gmlos_col, type=pa.float64()),
            pa.array(amlos_col, type=pa.float64()),
            pa.array(pat_col, type=pa.string()),
            pa.array(es_col, type=pa.date32()),
            pa.array(ee_col, type=pa.date32()),
            pa.array(src_col, type=pa.string()),
        ],
        schema=_TABLE5_SCHEMA,
    )


def _build_table1_arrow(
    rows: list[_ParsedTable1Row],
    *,
    fy: int,
    version: str,
    eff_start: date,
    eff_end: date,
    source: str,
) -> pa.Table:
    """Project ``_ParsedTable1Row`` records into the locked _TABLE1_SCHEMA."""
    fy_col: list[int] = []
    ver_col: list[str] = []
    table_col: list[str] = []
    li_col: list[str] = []
    cat_col: list[str | None] = []
    rp_col: list[str | None] = []
    val_col: list[float] = []
    unit_col: list[str] = []
    es_col: list[date] = []
    ee_col: list[date] = []
    src_col: list[str] = []

    # Sort by (table, line_item, category, rate_period) for deterministic output.
    def _sort_key(
        r: _ParsedTable1Row,
    ) -> tuple[str, str, str, str]:
        return (
            r.table,
            r.line_item,
            r.category or "",
            r.rate_period or "",
        )

    rows_sorted = sorted(rows, key=_sort_key)
    for r in rows_sorted:
        fy_col.append(fy)
        ver_col.append(version)
        table_col.append(r.table)
        li_col.append(r.line_item)
        cat_col.append(r.category)
        rp_col.append(r.rate_period)
        val_col.append(r.value)
        unit_col.append(r.unit)
        es_col.append(eff_start)
        ee_col.append(eff_end)
        src_col.append(source)

    return pa.Table.from_arrays(
        [
            pa.array(fy_col, type=pa.int32()),
            pa.array(ver_col, type=pa.string()),
            pa.array(table_col, type=pa.string()),
            pa.array(li_col, type=pa.string()),
            pa.array(cat_col, type=pa.string()),
            pa.array(rp_col, type=pa.string()),
            pa.array(val_col, type=pa.float64()),
            pa.array(unit_col, type=pa.string()),
            pa.array(es_col, type=pa.date32()),
            pa.array(ee_col, type=pa.date32()),
            pa.array(src_col, type=pa.string()),
        ],
        schema=_TABLE1_SCHEMA,
    )


# ----------------------------------------------------------------------------
# Bronze + helper utilities
# ----------------------------------------------------------------------------


def _output_dir_default() -> Path:
    return config.SILVER_PER_SOURCE / _BRONZE_SOURCE


def _version_short(version: str) -> str:
    """Return ``"V43.0"`` -> ``"v430"`` lowercased+dot-stripped."""
    return version.replace(".", "").lower()


def _table5_source_name(fy: int, version: str) -> str:
    return f"ipps_table5_fy{fy}_{_version_short(version)}"


def _table1_source_name(
    fy: int, version: str, *, rate_period: str | None = None
) -> str:
    base = f"ipps_table1_fy{fy}_{_version_short(version)}"
    if rate_period is None:
        return base
    return f"{base}_{rate_period}"


def _synthesize_bronze_filename(
    url: str, fy: int, version: str, table_kind: str
) -> str:
    """Derive a bronze-tree-safe filename for a given IPPS URL.

    Most cells have a clean URL filename; defends against degenerate URL
    basenames (literal slash, empty, or non-zip extension) by falling back
    to a canonical ``ipps_{table5|tables1}_fy{YYYY}_{vshort}.zip``.
    """
    name = Path(url).name
    short = _version_short(version)
    canonical = f"ipps_{table_kind}_fy{fy}_{short}.zip"
    degenerate = (
        not name
        or "/" in name
        or not name.lower().endswith((".zip", ".zip-0"))
    )
    if degenerate:
        return canonical
    return name


def _detect_rate_period(member_name: str) -> str | None:
    """Return ``"1st_half"`` / ``"2nd_half"`` / None for an FY2010 member."""
    upper = member_name.upper()
    if re.search(r"1\s*ST\s*[\s_-]*HALF", upper) or "1ST HALF" in upper or "1ST_HALF" in upper:
        return "1st_half"
    if "FIRST HALF" in upper or "FIRST_HALF" in upper:
        return "1st_half"
    if re.search(r"2\s*ND\s*[\s_-]*HALF", upper) or "2ND HALF" in upper or "2ND_HALF" in upper:
        return "2nd_half"
    if "SECOND HALF" in upper or "SECOND_HALF" in upper:
        return "2nd_half"
    return None


_FY2010_INTERVALS: dict[str, tuple[date, date]] = {
    "1st_half": (date(2009, 10, 1), date(2010, 3, 31)),
    "2nd_half": (date(2010, 4, 1), date(2010, 9, 30)),
}


# ----------------------------------------------------------------------------
# Public API: per-cell extraction
# ----------------------------------------------------------------------------


def _extract_table5(
    *,
    fy: int,
    version: str,
    out_dir: Path,
    session: requests.Session,
    download_date: str | None,
) -> list[Path]:
    """Download + parse Table 5 ZIP for ``(fy, version)``. Writes ONE parquet."""
    key = (fy, version)
    if key not in config._IPPS_TABLE5_BY_FY:
        raise KeyError(
            f"ipps fy{fy} version{version!r}: missing entry in "
            f"config._IPPS_TABLE5_BY_FY"
        )
    url = config._IPPS_TABLE5_BY_FY[key]
    print(
        f"[ipps] fy{fy} {version} table5: URL {url}",
        file=sys.stderr,
    )

    filename = _synthesize_bronze_filename(url, fy, version, "table5")
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=fy,
            filename=filename,
            download_date=download_date,
            session=session,
            period="fy",
        )
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"ipps fy{fy} {version} table5: {url} failed: {exc}"
        ) from exc

    eff_start, eff_end = config.ipps_fy_interval(fy, version)
    source_name = _table5_source_name(fy, version)

    with _open_zip(bronze_path) as zf:
        member = _find_table5_member(zf)
        text_dest = bronze_path.parent / Path(member).name
        _extract_member_to_path(zf, member, text_dest)
        # FR/CN multi-sheet picker. Six FYs (2015/2016/2018/2020/2021/2025)
        # ship Table 5 as a 2-sheet workbook (FR + CN) where CMS leaves
        # ``wb.active`` set to the CN variant. Without this picker, naive
        # ``wb.active`` reads emit CN-revised ``relative_weight`` values
        # (cents-level shifts vs the canonical FR weights). Reuses the same
        # FR-name predicate as the Tables 1A-1E read path; returns None for
        # single-sheet workbooks (most pre-FY2015 FYs) so behavior is a
        # no-op there.
        fr_sheet = _pick_fr_sheet(text_dest)
        if fr_sheet is not None:
            print(
                f"[ipps] fy{fy} {version} table5: parsing {text_dest.name}; "
                f"selecting FR sheet {fr_sheet!r} from multi-sheet workbook",
                file=sys.stderr,
            )
        else:
            print(
                f"[ipps] fy{fy} {version} table5: parsing {text_dest.name}",
                file=sys.stderr,
            )
        rows = _read_member_rows(text_dest, sheet_name=fr_sheet)

    parsed = _parse_table5_rows(rows, fy=fy)
    if not parsed:
        raise RuntimeError(
            f"ipps fy{fy} {version} table5: zero parsed rows from {member}"
        )

    table = _build_table5_arrow(
        parsed,
        fy=fy,
        version=version,
        eff_start=eff_start,
        eff_end=eff_end,
        source=source_name,
    )
    out_path = out_dir / f"{source_name}.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[ipps] fy{fy} {version} table5: wrote {table.num_rows} rows to "
        f"{out_path}",
        file=sys.stderr,
    )
    return [out_path]


def _extract_tables1(
    *,
    fy: int,
    version: str,
    out_dir: Path,
    session: requests.Session,
    download_date: str | None,
    strict: bool = False,
) -> list[Path]:
    """Download + parse Tables 1A-1E ZIP. Writes up to 5 (or 10 for FY2010) parquets."""
    key = (fy, version)
    if key not in config._IPPS_TABLES1_BY_FY:
        raise KeyError(
            f"ipps fy{fy} version{version!r}: missing entry in "
            f"config._IPPS_TABLES1_BY_FY"
        )
    url = config._IPPS_TABLES1_BY_FY[key]
    print(
        f"[ipps] fy{fy} {version} tables1: URL {url}",
        file=sys.stderr,
    )

    filename = _synthesize_bronze_filename(url, fy, version, "tables1")
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=fy,
            filename=filename,
            download_date=download_date,
            session=session,
            period="fy",
        )
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"ipps fy{fy} {version} tables1: {url} failed: {exc}"
        ) from exc

    eff_start_full, eff_end_full = config.ipps_fy_interval(fy, version)
    short = _version_short(version)
    written: list[Path] = []

    with _open_zip(bronze_path) as zf:
        members_map = _find_tables1_members(zf, fy)
        if not members_map:
            raise RuntimeError(
                f"ipps fy{fy} {version} tables1: no per-table members "
                f"resolved from ZIP (saw {zf.namelist()[:10]})"
            )

        # Mode A: ONE primary file (XLSX or TXT) with a single stacked
        # worksheet/file containing all 5 tables marked by ``TABLE 1[A-E].``.
        # FY2010 ships TWO bundled members (1st_half / 2nd_half) -- iterate
        # all of them and pass `rate_period` per-member.
        if "__BUNDLED__" in members_map:
            bundled_members = members_map["__BUNDLED__"]
            for bundled_member in bundled_members:
                text_dest = bronze_path.parent / Path(bundled_member).name
                _extract_member_to_path(zf, bundled_member, text_dest)
                rp = _detect_rate_period(Path(bundled_member).name)
                rp_label = rp if rp is not None else "single"
                bundled_sheet = _pick_fr_sheet(text_dest)
                if bundled_sheet is not None:
                    print(
                        f"[ipps] fy{fy} {version} tables1: bundled-mode "
                        f"({text_dest.name}, rate_period={rp_label}); "
                        f"selecting FR sheet {bundled_sheet!r} from "
                        f"multi-sheet workbook",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"[ipps] fy{fy} {version} tables1: bundled-mode "
                        f"({text_dest.name}, rate_period={rp_label}); parsing "
                        f"stacked sections",
                        file=sys.stderr,
                    )
                rows = _read_member_rows(text_dest, sheet_name=bundled_sheet)
                parsed = _parse_tables1_rows(
                    rows,
                    fy=fy,
                    version=version,
                    rate_period=rp,
                )

                # Effective dates per (rate_period, fy).
                if rp is not None and fy == 2010:
                    es, ee = _FY2010_INTERVALS[rp]
                else:
                    es, ee = eff_start_full, eff_end_full

                # Bucket parsed rows by table letter so each emits its own
                # parquet (the silver layer files one per (table, rate_period)).
                emitted_letters: set[str] = set()
                for letter in ("A", "B", "C", "D", "E"):
                    letter_rows = [
                        r for r in parsed if r.table == f"1{letter}"
                    ]
                    if not letter_rows:
                        if fy in (2008, 2009) and letter == "E":
                            print(
                                f"[ipps] fy{fy}: Table 1E intentionally "
                                f"absent (pre-PPACA, by design)",
                                file=sys.stderr,
                            )
                        else:
                            print(
                                f"[ipps] fy{fy} table1{letter} "
                                f"({rp_label}): no rows emitted from "
                                f"stacked sheet; skipping",
                                file=sys.stderr,
                            )
                        continue
                    if rp is not None:
                        source_name = (
                            f"ipps_table1{letter.lower()}_fy{fy}_{short}_{rp}"
                        )
                    else:
                        source_name = (
                            f"ipps_table1{letter.lower()}_fy{fy}_{short}"
                        )
                    table = _build_table1_arrow(
                        letter_rows,
                        fy=fy,
                        version=version,
                        eff_start=es,
                        eff_end=ee,
                        source=source_name,
                    )
                    out_path = out_dir / f"{source_name}.parquet"
                    pq.write_table(table, str(out_path))
                    print(
                        f"[ipps] fy{fy} table1{letter} ({rp_label}): wrote "
                        f"{table.num_rows} rows to {out_path}",
                        file=sys.stderr,
                    )
                    written.append(out_path)
                    emitted_letters.add(letter)
            return written

        # Mode B: separate per-table-letter members.
        for letter in ("A", "B", "C", "D", "E"):
            candidates = members_map.get(letter)
            if not candidates:
                if fy in (2008, 2009) and letter == "E":
                    print(
                        f"[ipps] fy{fy}: Table 1E intentionally absent "
                        f"(pre-PPACA, by design)",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"[ipps] fy{fy} table1{letter}: member not found "
                        f"in ZIP; skipping",
                        file=sys.stderr,
                    )
                continue

            # Group by rate_period (FY2010 dual-period). For non-FY2010 the
            # detection returns None -> single bucket.
            buckets: dict[str | None, list[str]] = {}
            for cand in candidates:
                rp = _detect_rate_period(Path(cand).name)
                buckets.setdefault(rp, []).append(cand)

            # When fy == 2010 and we get None bucket, that means we couldn't
            # detect — fall through and emit with rate_period=None as best
            # effort (caller / transform will catch).
            for rp, cand_list in buckets.items():
                # Prefer xlsx > xls > csv > txt within bucket.
                cand_list_sorted = sorted(
                    cand_list, key=lambda m: _ext_rank(Path(m).suffix.lower())
                )
                chosen = cand_list_sorted[0]
                text_dest = bronze_path.parent / Path(chosen).name
                _extract_member_to_path(zf, chosen, text_dest)
                rp_label = rp if rp is not None else "single"
                print(
                    f"[ipps] fy{fy} table1{letter} {rp_label}: parsing "
                    f"{text_dest.name}",
                    file=sys.stderr,
                )

                rows = _read_member_rows(text_dest)
                parsed = _parse_table1_rows(
                    rows,
                    fy=fy,
                    table_letter=letter,
                    rate_period=rp,
                    strict=strict,
                )
                if not parsed:
                    print(
                        f"[ipps] fy{fy} table1{letter} {rp_label}: zero "
                        f"parsed rows from {text_dest.name}; skipping",
                        file=sys.stderr,
                    )
                    continue

                # Effective dates.
                if rp is not None and fy == 2010:
                    es, ee = _FY2010_INTERVALS[rp]
                else:
                    es, ee = eff_start_full, eff_end_full

                # Filename suffix.
                if rp is not None:
                    out_name = (
                        f"ipps_table1{letter.lower()}_fy{fy}_{short}_{rp}"
                    )
                else:
                    out_name = (
                        f"ipps_table1{letter.lower()}_fy{fy}_{short}"
                    )

                table = _build_table1_arrow(
                    parsed,
                    fy=fy,
                    version=version,
                    eff_start=es,
                    eff_end=ee,
                    source=out_name,
                )
                out_path = out_dir / f"{out_name}.parquet"
                pq.write_table(table, str(out_path))
                print(
                    f"[ipps] fy{fy} table1{letter} {rp_label}: wrote "
                    f"{table.num_rows} rows to {out_path}",
                    file=sys.stderr,
                )
                written.append(out_path)

    return written


def extract_fy_version(
    fy: int,
    version: str,
    *,
    target: str = "both",
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
    strict: bool = False,
) -> list[Path]:
    """Extract IPPS Final Rule artifacts for one ``(fy, version)`` cell.

    Parameters
    ----------
    fy:
        Federal fiscal year. Must be in ``config.IPPS_FYS``.
    version:
        Version string (e.g. ``"V43.0"``). Must be in
        ``config.IPPS_VERSIONS_BY_FY[fy]``.
    target:
        One of ``"table5"``, ``"tables1"``, ``"both"`` (default).
    session:
        Optional shared ``requests.Session``.
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze date subdir.
    output_dir:
        Optional override for ``config.SILVER_PER_SOURCE / "ipps"``.
    strict:
        If True, hard-fail on unknown line-item labels in Tables 1A-1E
        (default: warn + skip).

    Returns
    -------
    list[Path]
        Silver parquet paths that were written. Cardinality:
            * ``target="table5"``     -> 1
            * ``target="tables1"``    -> up to 5 (10 for FY2010 dual-period;
                                                  4 for FY2008/FY2009)
            * ``target="both"``       -> sum of the above

    Raises
    ------
    KeyError
        If ``(fy, version)`` is not registered in
        ``config._IPPS_TABLE5_BY_FY`` / ``config._IPPS_TABLES1_BY_FY``.
    RuntimeError
        On HTTP failure, ZIP corruption, or text-member parse failure.
    """
    target_norm = target.strip().lower()
    if target_norm not in {"table5", "tables1", "both"}:
        raise ValueError(
            f"ipps: --target must be 'table5' / 'tables1' / 'both', "
            f"got {target!r}"
        )

    if fy not in config.IPPS_FYS:
        raise KeyError(
            f"ipps fy{fy}: not in config.IPPS_FYS "
            f"(known {min(config.IPPS_FYS)}-{max(config.IPPS_FYS)})"
        )
    versions = config.IPPS_VERSIONS_BY_FY.get(fy, ())
    if version not in versions:
        raise KeyError(
            f"ipps fy{fy} version{version!r}: not in "
            f"config.IPPS_VERSIONS_BY_FY[{fy}] = {versions}"
        )

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _output_dir_default()
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    if target_norm in {"table5", "both"}:
        written.extend(
            _extract_table5(
                fy=fy,
                version=version,
                out_dir=out_dir,
                session=sess,
                download_date=download_date,
            )
        )

    if target_norm in {"tables1", "both"}:
        written.extend(
            _extract_tables1(
                fy=fy,
                version=version,
                out_dir=out_dir,
                session=sess,
                download_date=download_date,
                strict=strict,
            )
        )

    return written


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    target: str = "both",
    session: requests.Session | None = None,
    download_date: str | None = None,
    strict: bool = False,
) -> list[Path]:
    """Loop ``extract_fy_version`` over FYs in ``config.IPPS_VERSIONS_BY_FY``.

    For each FY in the inclusive range, every version in
    ``config.IPPS_VERSIONS_BY_FY[fy]`` is processed (currently V*.0 only).
    Per-cell failures are logged loudly and skipped (mirrors the ASC / PFS
    bulk semantics). Returns the list of parquet paths actually written.
    """
    if end_fy < start_fy:
        raise ValueError(
            f"end_fy ({end_fy}) must be >= start_fy ({start_fy})"
        )

    sess = session or _http.make_session()
    written: list[Path] = []
    failures: list[tuple[int, str, str]] = []
    for fy in range(start_fy, end_fy + 1):
        if fy not in config.IPPS_FYS:
            print(
                f"[ipps] FY{fy}: not in config.IPPS_FYS, skipping",
                file=sys.stderr,
            )
            continue
        for version in config.IPPS_VERSIONS_BY_FY.get(fy, ()):
            try:
                paths = extract_fy_version(
                    fy,
                    version,
                    target=target,
                    session=sess,
                    download_date=download_date,
                    strict=strict,
                )
                written.extend(paths)
            except Exception as exc:  # noqa: BLE001 — bulk run swallows
                print(
                    f"[ipps] FY{fy} {version} FAILED: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                failures.append(
                    (fy, version, f"{type(exc).__name__}: {exc}")
                )
    if failures:
        print(
            f"[ipps] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for fy, version, msg in failures:
            print(f"  FY{fy} {version}: {msg}", file=sys.stderr)
    print(
        f"[ipps] bulk run summary: {len(written)} parquet(s) written, "
        f"{len(failures)} (fy, version) cell(s) failed",
        file=sys.stderr,
    )
    return written


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def _parse_fy_range(spec: str) -> tuple[int, int]:
    """Parse a ``"START-END"`` FY range spec for argparse."""
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range expects 'START-END' (e.g., 2008-2026), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for ``python -m src.extract.ipps``."""
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.ipps",
        description=(
            "Download and parse the CMS IPPS Final Rule Table 5 and/or "
            "Tables 1A-1E ZIP for one (FY, version) cell or a bulk range, "
            "demultiplexing the bundled members into per-source parquets in "
            "data/silver/per_source/ipps/."
        ),
    )
    fy_group = parser.add_mutually_exclusive_group(required=True)
    fy_group.add_argument(
        "--fy",
        type=int,
        help="Single fiscal year (e.g., 2026).",
    )
    fy_group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        metavar="START-END",
        help="Inclusive FY range, e.g., '2008-2026'.",
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help=(
            "IPPS version string (e.g., 'V43.0'). Default: first version "
            "from config.IPPS_VERSIONS_BY_FY[fy]."
        ),
    )
    parser.add_argument(
        "--target",
        type=str,
        default="both",
        choices=("table5", "tables1", "both"),
        help="Which artifact to extract (default: both).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Hard-fail on unknown line-item labels in Tables 1A-1E "
            "(default: warn + skip)."
        ),
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    single_fy = args.fy is not None
    if single_fy:
        fy = args.fy
        if fy not in config.IPPS_FYS:
            parser.error(
                f"--fy must be in {min(config.IPPS_FYS)}-"
                f"{max(config.IPPS_FYS)}, got {fy}"
            )
        # Resolve version.
        versions = config.IPPS_VERSIONS_BY_FY.get(fy, ())
        if not versions:
            parser.error(
                f"ipps fy{fy}: no versions registered in "
                f"config.IPPS_VERSIONS_BY_FY"
            )
        if args.version is not None:
            version = args.version.strip()
            if version not in versions:
                parser.error(
                    f"--version {args.version!r} not in "
                    f"config.IPPS_VERSIONS_BY_FY[{fy}] = {versions}"
                )
        else:
            version = versions[0]
        extract_fy_version(
            fy,
            version,
            target=args.target,
            download_date=args.download_date,
            strict=args.strict,
        )
        return 0

    start_fy, end_fy = args.fy_range
    if args.version is not None:
        # Bulk + explicit single version: only process FYs whose version
        # tuple contains it.
        sess = _http.make_session()
        written: list[Path] = []
        failures: list[tuple[int, str, str]] = []
        for fy in range(start_fy, end_fy + 1):
            if fy not in config.IPPS_FYS:
                continue
            versions = config.IPPS_VERSIONS_BY_FY.get(fy, ())
            if args.version not in versions:
                print(
                    f"[ipps] FY{fy}: version {args.version!r} not in "
                    f"{versions}, skipping",
                    file=sys.stderr,
                )
                continue
            try:
                paths = extract_fy_version(
                    fy,
                    args.version,
                    target=args.target,
                    session=sess,
                    download_date=args.download_date,
                    strict=args.strict,
                )
                written.extend(paths)
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[ipps] FY{fy} {args.version} FAILED: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                failures.append(
                    (fy, args.version, f"{type(exc).__name__}: {exc}")
                )
        if failures:
            print(
                f"[ipps] bulk run completed with {len(failures)} "
                f"failure(s):",
                file=sys.stderr,
            )
            for fy, ver, msg in failures:
                print(f"  FY{fy} {ver}: {msg}", file=sys.stderr)
        print(
            f"[ipps] bulk run summary: {len(written)} parquet(s) written, "
            f"{len(failures)} (fy, version) cell(s) failed",
            file=sys.stderr,
        )
        return 0

    extract_fy_range(
        start_fy,
        end_fy,
        target=args.target,
        download_date=args.download_date,
        strict=args.strict,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
