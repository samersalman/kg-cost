"""PFS RVU extractor: per-(CY, quarter) CMS RVU{YY}{Q}.zip -> silver parquet.

For each (calendar year, quarter) cell in scope this module:

    1. Reads the per-(cy, quarter) URL from ``config.PFS_SOURCES``. The
       canonical store carries placeholder URLs for every (cy, quarter) cell
       in CY2008-CY2026 x {A, B, C, D}. CMS has used (at least) three URL
       eras (pre-Drupal, legacy ``Downloads/``, modern ``files/zip/``); a 404
       from any cell surfaces as an ``HTTPError`` that
       ``extract_cy_quarter_range`` catches and logs+skips - mirroring the
       HCPCS pattern. Per-(cy, quarter) URL drift now requires updating
       ``config.PFS_SOURCES``, not silent runtime rerouting.

    2. Downloads via ``_http.download_to_bronze`` with ``period="cy"``. The
       quarter is encoded into the bronze ZIP filename
       (``RVU{YY}{Q}.zip``) rather than into the bronze directory tree, so
       all four quarters of a given CY land at
       ``data/bronze/pfs_rvu/cy{cy}/{download_date}/RVU{YY}{Q}.zip``.

    3. Parses the inner ``PPRRVU{YY}.csv`` member. PPRRVU column-name drift
       across years is documented as a Phase-2 landmine; this module
       auto-detects required columns by header-keyword matching rather than
       by exact name (mirrors HCPCS's ``_HEADER_ALIASES`` pattern).

    4. Computes per-row allowed amounts in long form (one row per setting):
       ``allowed_amount = (work_rvu + pe_rvu_setting + mp_rvu) * cf`` for
       both ``pfs_facility`` (``pe_rvu_setting = pe_f``) and
       ``pfs_nonfacility`` (``pe_rvu_setting = pe_nf``). For CY2026, every
       row is doubled into ``conversion_factor_kind in {qp, nonqp}`` using
       ``config.PFS_CONVERSION_FACTORS[2026]`` (a tuple of QP/non-QP values
       per the CY2026 PFS Final Rule).

    5. Writes a per-(cy, quarter) parquet to
       ``data/silver/per_source/pfs_rvu/pfs_rates_cy{cy}_{q}.parquet``
       matching the locked ``silver/pfs_rates.parquet`` schema. The
       ``codes_master`` glob (``silver/per_source/*.parquet``, non-recursive)
       does NOT recurse into the ``pfs_rvu/`` subdirectory, so PFS rows are
       isolated from the codes_master union.

Filters:
    - Codes must match either CPT (5-digit numeric) or HCPCS-II
      (``[A-V0-9][0-9]{4}``) regex.
    - When a status-indicator column is present, rows with status
      ``I`` (invalid) or ``N`` (non-covered) are dropped. When the column
      is missing (rare, but tolerated), all rows pass.
    - Modifier kept as a property column (NULL when blank). Leading dashes
      and whitespace are stripped, value upper-cased.

Per-(cy, quarter) failures in ``extract_cy_quarter_range`` are logged loudly
and skipped - pre-2017 quarters (currently of unknown availability on
cms.gov) intentionally go through that path. ``extract_cy_quarter`` (single
cell) raises on failure rather than swallowing.

CLI::

    python -m src.extract.pfs_rvu --cy 2024 --quarter D
    python -m src.extract.pfs_rvu --cy 2024 --quarter-range A-D
    python -m src.extract.pfs_rvu --cy-range 2008-2026 --quarter D
    python -m src.extract.pfs_rvu --cy-range 2008-2026 --quarter-range A-D
    python -m src.extract.pfs_rvu --cy 2024 --quarter D --download-date 2026-04-29
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

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "pfs_rvu"

# CPT Level I: 5-digit pure numeric.
_CPT_CODE_RE = re.compile(r"^[0-9]{5}$")
# HCPCS Level II: 5-character upper-case alphanumeric. First char A-V or 0-9.
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")

# Status indicators that exclude a row from the PFS allowed-amount layer.
# `I` = invalid HCPCS code; `N` = non-covered service. Per Phase-2 plan
# (plans/plan-v3.md), this filter is opinionated but documented; easy to relax.
_DROPPED_STATUS_INDICATORS: frozenset[str] = frozenset({"I", "N"})

# PPRRVU CSV column-keyword aliases. Case- and whitespace-normalized lookup
# (see `_normalize_header`). Required columns are gated by `_resolve_columns`;
# `modifier` and `status` are optional (some early-CY CSVs omit them).
_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "code": ("HCPCS", "HCPCS_CD", "HCPC"),
    "modifier": ("MOD", "MODIFIER"),
    "status": ("STATUS CODE", "STATUS", "STATUS_CD", "STATUS CD"),
    "work": ("WORK RVU", "WORK_RVU", "WORK RVU_3"),
    "pe_nf": (
        "NON-FAC PE RVU",
        "NONFAC PE RVU",
        "NON-FACILITY PE RVU",
        "NON_FAC_PE_RVU",
    ),
    "pe_f": ("FAC PE RVU", "FACILITY PE RVU", "FAC_PE_RVU"),
    "mp": ("MP RVU", "MP_RVU", "MAL PRACTICE RVU"),
}


def _normalize_header(value: object) -> str:
    """Return a case- and whitespace-normalized header string for matching."""
    if value is None:
        return ""
    return str(value).strip().upper()


def _resolve_columns(
    header: list[object],
    *,
    required: tuple[str, ...] = ("code", "work", "pe_nf", "pe_f", "mp"),
) -> dict[str, int]:
    """Map our logical column names to 0-indexed positions in ``header``.

    ``required`` defaults to the RVU columns that we need to compute
    ``allowed_amount`` plus the join key (``code``). ``modifier`` and
    ``status`` are best-effort - their absence in older PPRRVU CSVs is
    tolerated, and the parser falls back to ``modifier=NULL`` /
    "no status filter" respectively.
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
    missing = [k for k in required if k not in out]
    if missing:
        raise RuntimeError(
            f"pfs_rvu: required columns {missing} not found in PPRRVU header; "
            f"saw {norm[:20]}..."
        )
    return out


# ----------------------------------------------------------------------------
# ZIP / CSV discovery + parse
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParsedRow:
    code: str
    modifier: str | None
    status: str | None
    work: float | None
    pe_f: float | None
    pe_nf: float | None
    mp: float | None


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"pfs_rvu: corrupt ZIP at {path}: {exc}") from exc


def _find_pprrvu_member(zf: zipfile.ZipFile, cy: int) -> str:
    """Return the ZIP member name of the PPRRVU CSV for calendar year ``cy``.

    Selection rules (in order):
      1. Exact ``PPRRVU{YY}.csv`` (case-insensitive) where ``YY = cy % 100``
         zero-padded.
      2. Any ``*PPRRVU*.csv`` (case-insensitive) member.
      3. Any ``.csv`` member whose basename starts with ``PPRRVU``.

    Raises if no candidate is found. CMS has been consistent on the PPRRVU
    member name across the modern era, but the suffix-predicate fallbacks
    absorb any pre-2014-era ZIP repacks where the year-suffix may differ.
    """
    members = list(zf.namelist())
    yy = f"{cy % 100:02d}"
    target_exact = f"PPRRVU{yy}.csv"

    for name in members:
        if Path(name).name.lower() == target_exact.lower():
            return name

    for name in members:
        bn = Path(name).name.lower()
        if "pprrvu" in bn and bn.endswith(".csv"):
            return name

    for name in members:
        bn = Path(name).name.lower()
        if bn.startswith("pprrvu") and bn.endswith(".csv"):
            return name

    raise RuntimeError(
        f"pfs_rvu cy{cy}: PPRRVU CSV not found in ZIP; members were "
        f"{[Path(n).name for n in members]}"
    )


def _extract_csv_member_to_tempfile(
    zf: zipfile.ZipFile, member: str, dest: Path
) -> Path:
    """Extract a ZIP member to ``dest`` (idempotent - skip if already present).

    Mirrors hcpcs._extract_member_to_tempfile. The PPRRVU CSV is small enough
    to fit in memory but we land it next to the bronze ZIP for parity with
    the HCPCS workbook unpack pattern, doubling as a cached unpack since
    bronze is write-once per CLAUDE.md.
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


def _read_csv_rows(path: Path) -> list[list[str]]:
    """Read ``path`` as CSV rows (list of lists of strings).

    Tolerates a UTF-8 BOM (``utf-8-sig``); falls back to Latin-1 on a
    UnicodeDecodeError to absorb any odd CMS exports. Returns the raw rows
    without any header detection - that's the caller's job (see
    ``_find_header_index``).
    """
    encodings = ("utf-8-sig", "latin-1")
    last_exc: Exception | None = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [list(row) for row in csv.reader(fh)]
        except UnicodeDecodeError as exc:  # noqa: PERF203 — small encoding ladder
            last_exc = exc
            continue
    raise RuntimeError(
        f"pfs_rvu: could not decode {path} with any of {encodings}: {last_exc}"
    )


def _find_header_index(rows: list[list[str]]) -> int:
    """Return the 0-indexed row whose first non-empty cell is a known
    code-column alias (``HCPCS`` / ``HCPCS_CD`` / ``HCPC``).

    PPRRVU CSVs ship with a 1- to ~10-row legal/disclaimer block prepended
    before the column header in some CYs. Scan the first 30 rows to locate
    the BOTTOM row of the column header (the row whose first cell is the
    code-column alias) - in modern PPRRVU CSVs this is preceded by stacked
    multi-row sub-headers (e.g. row above carries "WORK", target row carries
    "RVU"). See ``_assemble_stacked_header`` for the column-name assembly.
    """
    code_aliases = {a.upper() for a in _HEADER_ALIASES["code"]}
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        for cell in row:
            norm = _normalize_header(cell)
            if not norm:
                continue
            # First non-empty cell decides whether this row is the header.
            if norm in code_aliases:
                return i
            break
    raise RuntimeError(
        f"pfs_rvu: PPRRVU header row not found in first 30 rows; "
        f"first-row first-cells seen: "
        f"{[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


def _assemble_stacked_header(
    rows: list[list[str]],
    bottom_idx: int,
    *,
    max_lookback: int = 6,
) -> list[str]:
    """Stitch the multi-row stacked PPRRVU header into a flat header list.

    Modern PPRRVU CSVs ship a multi-row column header where the bottom row
    (``rows[bottom_idx]``, identified by ``HCPCS`` as its first cell)
    carries the leaf names (``RVU``, ``PE RVU``, ``CODE``, etc.) and the
    rows above carry stacked qualifiers (``WORK``, ``NON-FAC``, ``FACILITY``,
    ``STATUS``, etc.). For each column index we walk upward from
    ``bottom_idx`` collecting non-empty cells and joining them with single
    spaces. Walks stop at a row whose first cell is empty (the legal block).

    The walk-back is bounded by ``max_lookback`` to avoid pulling unrelated
    legal-block text into a header cell when CMS uses a deeper disclaimer
    prefix than expected. Trailing whitespace is collapsed.

    For pre-modern PPRRVU CSVs that ship a single-row header, the loop
    finds nothing above and returns the bottom row verbatim - so this is a
    superset of the single-row case.
    """
    bottom = list(rows[bottom_idx])
    n_cols = len(bottom)

    # Walk back at most `max_lookback` rows above the bottom row. The
    # legal/disclaimer rows further up have empty cells in our target
    # columns, so they contribute "" and are dropped by the `if norm`
    # filter below. Bounding the lookback here just limits how much
    # disclaimer text could leak into a non-target column's assembled name
    # if CMS ever uses a non-empty disclaimer cell aligned with a header
    # column - it doesn't affect correctness for our target aliases.
    upper_bound = max(0, bottom_idx - max_lookback)

    out: list[str] = []
    for col in range(n_cols):
        parts: list[str] = []
        for i in range(upper_bound, bottom_idx + 1):
            row = rows[i] if i < len(rows) else []
            cell = row[col] if col < len(row) else ""
            norm = _normalize_header(cell)
            if norm:
                parts.append(norm)
        out.append(" ".join(parts))
    return out


def _parse_code_field(value: object) -> str | None:
    """Normalize the HCPCS column value to upper-case, no-dot, no-whitespace."""
    if value is None:
        return None
    s = str(value).strip().upper().replace(".", "")
    return s or None


def _parse_modifier(value: object) -> str | None:
    """Normalize the modifier column. Strip leading dashes/whitespace, upper.

    PPRRVU emits modifiers like ``"-26"`` or ``" TC "`` (sometimes blank).
    Returns None for empty / whitespace-only / dash-only.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.lstrip("-").strip().upper()
    return s or None


def _parse_status(value: object) -> str | None:
    """Normalize the status indicator column to a single upper-case letter."""
    if value is None:
        return None
    s = str(value).strip().upper()
    return s or None


def _parse_rvu(value: object) -> float | None:
    """Best-effort float parse for an RVU column value.

    PPRRVU RVU columns are typically simple decimal strings (e.g. ``"0.97"``).
    Empty strings and unparseable values return None. Negative RVUs are not
    expected but are passed through as-is - downstream logic flags them via
    nullability rather than substituting zero.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Some CMS CSV exports use ``NA`` / ``N/A`` for missing.
    if s.upper() in {"NA", "N/A", "NULL"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify_code_system(code: str) -> str | None:
    """Return ``"CPT"`` for a 5-digit numeric, ``"HCPCS-II"`` for the
    Level-II alphanumeric pattern, or None for codes that match neither
    (those rows are dropped upstream)."""
    if _CPT_CODE_RE.match(code):
        return "CPT"
    if _HCPCS_CODE_RE.match(code):
        return "HCPCS-II"
    return None


def _rows_from_csv(csv_path: Path) -> list[_ParsedRow]:
    """Parse ``csv_path`` (a PPRRVU{YY}.csv) into a list of raw rows.

    Filters applied here:
      - Drop rows where ``code`` is null/empty/non-conformant (must match
        CPT or HCPCS-II regex).
      - Drop rows whose status indicator (when the column is present) is in
        ``_DROPPED_STATUS_INDICATORS`` (``I`` / ``N``).

    Modifier / RVUs / status are returned as-parsed; long-form expansion
    into the per-setting silver schema happens in ``_build_table``.
    """
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"pfs_rvu: CSV {csv_path} parsed to zero rows")

    header_idx = _find_header_index(rows)
    # Modern PPRRVU CSVs ship a multi-row stacked header (e.g. CY2024:
    # rows above the HCPCS row carry "WORK", "NON-FAC", "FACILITY", "MP",
    # "STATUS"; the HCPCS row carries the leaves "RVU", "PE RVU", "CODE",
    # etc.). Stitch them together so a single-string header lookup against
    # `_HEADER_ALIASES` finds e.g. "WORK RVU" / "NON-FAC PE RVU" / etc.
    header = _assemble_stacked_header(rows, header_idx)
    cols = _resolve_columns(header)

    has_modifier = "modifier" in cols
    has_status = "status" in cols

    out: list[_ParsedRow] = []
    for raw in rows[header_idx + 1:]:
        if not raw or len(raw) <= cols["code"]:
            continue
        code = _parse_code_field(raw[cols["code"]])
        if code is None or _classify_code_system(code) is None:
            continue

        status: str | None = None
        if has_status and len(raw) > cols["status"]:
            status = _parse_status(raw[cols["status"]])
            if status in _DROPPED_STATUS_INDICATORS:
                continue

        modifier: str | None = None
        if has_modifier and len(raw) > cols["modifier"]:
            modifier = _parse_modifier(raw[cols["modifier"]])

        def _safe(idx_key: str) -> float | None:
            i = cols.get(idx_key)
            if i is None or len(raw) <= i:
                return None
            return _parse_rvu(raw[i])

        work = _safe("work")
        pe_f = _safe("pe_f")
        pe_nf = _safe("pe_nf")
        mp = _safe("mp")

        out.append(
            _ParsedRow(
                code=code,
                modifier=modifier,
                status=status,
                work=work,
                pe_f=pe_f,
                pe_nf=pe_nf,
                mp=mp,
            )
        )

    if not out:
        raise RuntimeError(
            f"pfs_rvu: PPRRVU CSV {csv_path} yielded zero matching rows; "
            f"check header detection / regex / status filter"
        )
    return out


# ----------------------------------------------------------------------------
# Parquet emission
# ----------------------------------------------------------------------------


# Locked Phase-2 schema — see plans/plan-v3.md lines 92-120. Column order is
# load-bearing (downstream consumers and `pfs_rates` unifier expect it).
_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("modifier", pa.string(), nullable=True),
        pa.field("code_system", pa.string(), nullable=False),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("setting", pa.string(), nullable=False),
        pa.field("status_indicator", pa.string(), nullable=True),
        pa.field("work_rvu", pa.float64(), nullable=True),
        pa.field("pe_rvu", pa.float64(), nullable=True),
        pa.field("mp_rvu", pa.float64(), nullable=True),
        pa.field("total_rvu", pa.float64(), nullable=True),
        pa.field("conversion_factor", pa.float64(), nullable=False),
        pa.field("conversion_factor_kind", pa.string(), nullable=False),
        pa.field("allowed_amount", pa.float64(), nullable=True),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("n", pa.int32(), nullable=True),
    ]
)

# (setting_label, attribute on _ParsedRow that holds the PE-RVU for that setting)
_SETTINGS: tuple[tuple[str, str], ...] = (
    ("pfs_facility", "pe_f"),
    ("pfs_nonfacility", "pe_nf"),
)


def _cf_variants(cy: int) -> list[tuple[str, float]]:
    """Return the list of ``(kind, value)`` conversion-factor variants for ``cy``.

    For a single-CF CY this is ``[("single", cf)]`` (one row per setting).
    For CY2026+ where ``PFS_CONVERSION_FACTORS[cy]`` is a tuple, returns
    ``[("qp", qp_value), ("nonqp", nonqp_value)]`` (two rows per setting,
    so each input CSV row produces 4 silver rows: 2 settings x 2 CF kinds).
    """
    cf = config.PFS_CONVERSION_FACTORS[cy]
    if isinstance(cf, tuple):
        qp_value, nonqp_value = cf
        return [("qp", float(qp_value)), ("nonqp", float(nonqp_value))]
    return [("single", float(cf))]


def _build_table(
    rows: list[_ParsedRow],
    *,
    cy: int,
    quarter: str,
    source_url: str,
    eff_start: date,
    eff_end: date,
) -> pa.Table:
    cf_variants = _cf_variants(cy)

    code_col: list[str] = []
    modifier_col: list[str | None] = []
    code_system_col: list[str] = []
    cy_col: list[int] = []
    quarter_col: list[str] = []
    setting_col: list[str] = []
    status_col: list[str | None] = []
    work_col: list[float | None] = []
    pe_col: list[float | None] = []
    mp_col: list[float | None] = []
    total_col: list[float | None] = []
    cf_col: list[float] = []
    cf_kind_col: list[str] = []
    allowed_col: list[float | None] = []
    eff_start_col: list[date] = []
    eff_end_col: list[date] = []
    source_col: list[str] = []
    n_col: list[int | None] = []

    for r in rows:
        code_system = _classify_code_system(r.code)
        # Should never be None here — _rows_from_csv already filtered.
        if code_system is None:  # pragma: no cover — defensive
            continue
        for setting_label, pe_attr in _SETTINGS:
            pe_value: float | None = getattr(r, pe_attr)
            # Compute total_rvu / allowed only when every component is non-null.
            if r.work is None or pe_value is None or r.mp is None:
                total_rvu: float | None = None
            else:
                total_rvu = r.work + pe_value + r.mp
            for cf_kind, cf_value in cf_variants:
                if total_rvu is None:
                    allowed: float | None = None
                else:
                    allowed = total_rvu * cf_value
                code_col.append(r.code)
                modifier_col.append(r.modifier)
                code_system_col.append(code_system)
                cy_col.append(cy)
                quarter_col.append(quarter)
                setting_col.append(setting_label)
                status_col.append(r.status)
                work_col.append(r.work)
                pe_col.append(pe_value)
                mp_col.append(r.mp)
                total_col.append(total_rvu)
                cf_col.append(cf_value)
                cf_kind_col.append(cf_kind)
                allowed_col.append(allowed)
                eff_start_col.append(eff_start)
                eff_end_col.append(eff_end)
                source_col.append(source_url)
                n_col.append(None)

    return pa.Table.from_arrays(
        [
            pa.array(code_col, type=pa.string()),
            pa.array(modifier_col, type=pa.string()),
            pa.array(code_system_col, type=pa.string()),
            pa.array(cy_col, type=pa.int32()),
            pa.array(quarter_col, type=pa.string()),
            pa.array(setting_col, type=pa.string()),
            pa.array(status_col, type=pa.string()),
            pa.array(work_col, type=pa.float64()),
            pa.array(pe_col, type=pa.float64()),
            pa.array(mp_col, type=pa.float64()),
            pa.array(total_col, type=pa.float64()),
            pa.array(cf_col, type=pa.float64()),
            pa.array(cf_kind_col, type=pa.string()),
            pa.array(allowed_col, type=pa.float64()),
            pa.array(eff_start_col, type=pa.date32()),
            pa.array(eff_end_col, type=pa.date32()),
            pa.array(source_col, type=pa.string()),
            pa.array(n_col, type=pa.int32()),
        ],
        schema=_PARQUET_SCHEMA,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def _output_dir_default() -> Path:
    return config.SILVER_PER_SOURCE / _BRONZE_SOURCE


def extract_cy_quarter(
    cy: int,
    quarter: str,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract PFS RVU for ``(cy, quarter)``; write per-cell silver parquet.

    Parameters
    ----------
    cy:
        Calendar year (e.g., 2024). Must be in ``config.PFS_CYS``.
    quarter:
        Quarter letter (one of ``A``, ``B``, ``C``, ``D``). Must be in
        ``config.PFS_QUARTERS``.
    session:
        Optional shared ``requests.Session`` (use one per range to amortize TLS).
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze date subdir.
    output_dir:
        Optional override for ``config.SILVER_PER_SOURCE / "pfs_rvu"``.

    Returns
    -------
    Path
        The silver parquet path that was written.

    Raises
    ------
    KeyError
        If ``(cy, quarter)`` is not in ``config.PFS_SOURCES``.
    RuntimeError
        On HTTP failure (e.g. 404 from a placeholder), ZIP corruption, or
        CSV parse failure. The URL and (cy, quarter) are included in the
        message.
    """
    quarter_upper = quarter.strip().upper()
    if cy not in config.PFS_CYS:
        raise KeyError(
            f"pfs_rvu cy{cy}: not registered in config.PFS_CYS "
            f"(known CYs: {min(config.PFS_CYS)}-{max(config.PFS_CYS)})"
        )
    if quarter_upper not in config.PFS_QUARTERS:
        raise KeyError(
            f"pfs_rvu cy{cy} quarter{quarter!r}: not in config.PFS_QUARTERS "
            f"({config.PFS_QUARTERS})"
        )
    key = (cy, quarter_upper)
    if key not in config.PFS_SOURCES:
        raise KeyError(
            f"pfs_rvu cy{cy} quarter{quarter_upper}: missing entry in "
            f"config.PFS_SOURCES"
        )

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _output_dir_default()
    out_dir.mkdir(parents=True, exist_ok=True)

    url = config.PFS_SOURCES[key]
    print(
        f"[pfs_rvu] cy{cy} q{quarter_upper}: URL {url}",
        file=sys.stderr,
    )

    # Most cells have a clean URL filename (e.g. `rvu24a.zip`); CY2024 Q2 ships
    # under a path with literal `/` characters in the filename
    # (`rvu24b-updated-03/18/2024.zip`), so `Path(url).name` collapses to
    # `2024.zip` — which is non-descriptive and would collide with anything
    # else following the same naming. When the URL's basename does not start
    # with `rvu`, fall back to a synthesized canonical filename so the bronze
    # tree stays self-describing.
    filename = Path(url).name
    if not filename.lower().startswith("rvu"):
        filename = f"rvu{cy % 100:02d}{quarter_upper.lower()}.zip"
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
        raise RuntimeError(
            f"pfs_rvu cy{cy} q{quarter_upper}: {url} failed: {exc}"
        ) from exc

    with _open_zip(bronze_path) as zf:
        member = _find_pprrvu_member(zf, cy)
        csv_dest = bronze_path.parent / Path(member).name
        _extract_csv_member_to_tempfile(zf, member, csv_dest)
    print(
        f"[pfs_rvu] cy{cy} q{quarter_upper}: parsing {csv_dest.name}",
        file=sys.stderr,
    )

    parsed = _rows_from_csv(csv_dest)
    eff_start, eff_end = config.pfs_quarter_interval(cy, quarter_upper)
    table = _build_table(
        parsed,
        cy=cy,
        quarter=quarter_upper,
        source_url=url,
        eff_start=eff_start,
        eff_end=eff_end,
    )

    out_path = out_dir / f"pfs_rates_cy{cy}_{quarter_upper}.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[pfs_rvu] cy{cy} q{quarter_upper}: wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_cy_quarter_range(
    start_cy: int,
    end_cy: int,
    *,
    quarters: list[str] | None = None,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_cy_quarter`` over ``[start_cy, end_cy]`` x ``quarters``.

    ``quarters`` defaults to ``config.PFS_QUARTERS`` (all four). Per-(cy, q)
    failures are logged loudly and skipped - pre-2017 cells (canonical
    placeholders that may 404) intentionally go through this path. The list
    of successfully written parquet paths is returned.
    """
    if end_cy < start_cy:
        raise ValueError(f"end_cy ({end_cy}) must be >= start_cy ({start_cy})")
    qs = list(quarters) if quarters is not None else list(config.PFS_QUARTERS)
    qs_normalized: list[str] = []
    for q in qs:
        qu = q.strip().upper()
        if qu not in config.PFS_QUARTERS:
            raise ValueError(
                f"pfs_rvu: quarter {q!r} not in config.PFS_QUARTERS "
                f"({config.PFS_QUARTERS})"
            )
        qs_normalized.append(qu)

    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str, str]] = []
    for cy in range(start_cy, end_cy + 1):
        if cy not in config.PFS_CYS:
            print(
                f"[pfs_rvu] CY{cy}: not in config.PFS_CYS, skipping",
                file=sys.stderr,
            )
            continue
        for quarter in qs_normalized:
            try:
                path = extract_cy_quarter(
                    cy,
                    quarter,
                    session=sess,
                    download_date=download_date,
                )
                out.append(path)
            except Exception as exc:  # noqa: BLE001 — bulk run swallows per-cell errors
                print(
                    f"[pfs_rvu] CY{cy} Q{quarter} FAILED: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                failures.append((cy, quarter, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[pfs_rvu] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for cy, q, msg in failures:
            print(f"  CY{cy} Q{q}: {msg}", file=sys.stderr)
    print(
        f"[pfs_rvu] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
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


def _parse_quarter_range(spec: str) -> tuple[str, str]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--quarter-range expects 'START-END' (e.g., 'A-D'), got {spec!r}"
        )
    a, b = parts[0].strip().upper(), parts[1].strip().upper()
    for q in (a, b):
        if q not in config.PFS_QUARTERS:
            raise argparse.ArgumentTypeError(
                f"--quarter-range letters must be in {config.PFS_QUARTERS}, got {spec!r}"
            )
    if config.PFS_QUARTERS.index(a) > config.PFS_QUARTERS.index(b):
        raise argparse.ArgumentTypeError(
            f"--quarter-range must be ascending; got {spec!r}"
        )
    return a, b


def _quarters_from_range(qr: tuple[str, str]) -> list[str]:
    a, b = qr
    i = config.PFS_QUARTERS.index(a)
    j = config.PFS_QUARTERS.index(b)
    return list(config.PFS_QUARTERS[i : j + 1])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.pfs_rvu",
        description=(
            "Download and parse the CMS PFS RVU{YY}{Q}.zip release for one "
            "(calendar-year, quarter) cell or a bulk range, writing per-cell "
            "parquet to data/silver/per_source/pfs_rvu/."
        ),
    )
    cy_group = parser.add_mutually_exclusive_group(required=True)
    cy_group.add_argument(
        "--cy",
        type=int,
        help="Single calendar year (e.g., 2024).",
    )
    cy_group.add_argument(
        "--cy-range",
        type=_parse_cy_range,
        metavar="START-END",
        help="Inclusive CY range, e.g., '2008-2026'.",
    )
    q_group = parser.add_mutually_exclusive_group(required=True)
    q_group.add_argument(
        "--quarter",
        type=str,
        help="Single PFS quarter letter (one of A/B/C/D).",
    )
    q_group.add_argument(
        "--quarter-range",
        type=_parse_quarter_range,
        metavar="START-END",
        help="Inclusive quarter-letter range, e.g., 'A-D'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    # Resolve CY scope.
    single_cy = args.cy is not None
    if single_cy:
        start_cy = end_cy = args.cy
    else:
        start_cy, end_cy = args.cy_range

    # Resolve quarter scope.
    single_quarter = args.quarter is not None
    if single_quarter:
        q_upper = args.quarter.strip().upper()
        if q_upper not in config.PFS_QUARTERS:
            parser.error(
                f"--quarter must be one of {config.PFS_QUARTERS}, got {args.quarter!r}"
            )
        quarters = [q_upper]
    else:
        quarters = _quarters_from_range(args.quarter_range)

    # Single (cy, quarter) cell: use the non-swallowing API so failures exit non-zero.
    if single_cy and single_quarter:
        extract_cy_quarter(
            start_cy,
            quarters[0],
            download_date=args.download_date,
        )
        return 0

    # Anything else: bulk path with log+skip semantics (matches HCPCS).
    extract_cy_quarter_range(
        start_cy,
        end_cy,
        quarters=quarters,
        download_date=args.download_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
