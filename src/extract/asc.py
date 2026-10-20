"""ASC extractor: per-(CY, quarter) CMS ASC Addenda ZIP -> silver parquet x6.

Each ASC quarter ships ONE ZIP that bundles SIX addenda as separate internal
members:

    AA   -- ASC Covered Surgical Procedures + Payment Rates (HCPCS-keyed).
    BB   -- ASC Covered Ancillary Services + Payment Rates (HCPCS-keyed).
    DD1  -- ASC Payment Indicator definitions (reference table).
    DD2  -- ASC Status Indicator definitions (reference table).
    EE   -- Final ASC Excluded Procedures (HCPCS-only).
    FF   -- ASC Drugs/Devices Pass-Through Pricing (HCPCS-keyed; subset of BB).

For each ``(cy, quarter)`` cell this module:

    1. Reads the per-cell ZIP URL from ``config._ASC_ZIP_BY_CY_QUARTER``
       (same URL across all 6 addenda - download ONCE, demux 6).
       ``config.ASC_SOURCES`` is the comprehensive (cy, q, addendum) -> URL
       manifest, but it would over-download 6x; do not use it as the
       download driver. A 404 from any cell surfaces as ``HTTPError`` that
       ``extract_cy_quarter_range`` catches and logs+skips.

    2. Downloads via ``_http.download_to_bronze`` with ``period="cy"``. The
       bronze ZIP filename is derived from the URL basename when sane.
       Several cells (CY2024 Q4, CY2025 Q2, CY2026 Q1, etc.) have URL
       basenames containing literal ``/`` from dated suffixes (e.g.
       ``...-updated-09/24/2024.zip``); ``_synthesize_bronze_filename``
       replaces those with a canonical ``asc_addenda_cy{cy}_{q}.zip``.

    3. Locates each addendum's text member inside the ZIP via
       ``_find_addendum_member`` (case-insensitive token match):
        * Priority 1: ``Addendum AA|BB|DD1|DD2|EE|FF`` substring (CY2025+
          subdirectory layout).
        * Priority 2: word-boundary letter immediately before ``.txt`` /
          ``.csv`` extension (CY2021-CY2024 flat layout).
        * Priority 3: ``EE.txt`` token (CY2024 Q1 EE anomaly: the file is
          ``January 2024 EE.txt`` without the ``ASC`` token).

    4. Parses the inner text member:
        * ``.txt`` -> tab-delimited; ``.csv`` -> comma. Auto-detect by
          extension (mirror ``opps._read_csv_rows``).
        * 1-4 banner / disclaimer rows precede the column header.
        * Header detection per addendum kind (see ``_HEADER_ALIASES_*``).
        * DD1 / DD2 may have NO column header at all - treat row 0 as data.

    5. Filters / coercions:
        * AA / BB / FF: code must match either CPT (5-digit numeric) or
          HCPCS-II (``[A-V0-9][0-9]{4}``). Modifier is nullable.
        * Payment rate: strip ``$`` and commas; ``--`` / ``NA`` / empty -> None.
        * EE: HCPCS-only; same code regex pair.
        * DD1 / DD2: no row filtering (reference tables).

    6. Writes per-cell parquets at:
       ``data/silver/per_source/asc/asc_{aa|bb|dd1|dd2|ee|ff}_cy{cy}_{q}.parquet``.

CLI::

    python -m src.extract.asc --cy 2024 --quarter A
    python -m src.extract.asc --cy 2024 --quarter-range A-D --addendum AA,BB
    python -m src.extract.asc --cy-range 2008-2026 --quarter-range A-D
    python -m src.extract.asc --cy 2024 --quarter A --addendum DD1,DD2,EE
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

_BRONZE_SOURCE: str = "asc"

# CPT Level I: 5-digit pure numeric.
_CPT_CODE_RE = re.compile(r"^[0-9]{5}$")
# HCPCS Level II: 5-character upper-case alphanumeric. First char A-V or 0-9.
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")

# Footer / disclaimer prefixes that should never be treated as data rows.
_FOOTER_PREFIXES: tuple[str, ...] = ("NOTE", "SEE", "*", "FOOTNOTE", "SOURCE")

# Header alias dicts (case- + whitespace-normalized lookup; see
# ``_normalize_header``). For AA/BB/FF the ``payment_indicator`` and
# ``payment_rate`` columns ship with month-prefixed CMS labels (e.g.
# ``"April 2026 Payment Indicator"``); we match those by **substring** in
# ``_resolve_columns_payment``, not via this alias dict.
_HEADER_ALIASES_PAYMENT: dict[str, tuple[str, ...]] = {
    "code": ("HCPCS", "HCPCS CODE", "HCPC", "CPT/HCPCS"),
    "modifier": ("MOD", "MODIFIER"),
    "short_descriptor": (
        "SHORT DESCRIPTOR",
        "SHORT DESCRIPTION",
        "DESCRIPTION",
        "SHORT DESC",
    ),
}

# EE addendum: HCPCS + descriptor only.
_HEADER_ALIASES_EE: dict[str, tuple[str, ...]] = {
    "code": ("HCPCS", "HCPCS CODE", "CPT/HCPCS", "HCPC"),
    "short_descriptor": (
        "SHORT DESCRIPTOR",
        "SHORT DESCRIPTION",
        "DESCRIPTION",
        "SHORT DESC",
    ),
}


def _normalize_header(value: object) -> str:
    """Return a case- and whitespace-normalized header string for matching.

    Collapses internal runs of whitespace to a single space (CMS Addendum
    headers ship with ragged spacing - e.g. ``" April 2026  Payment Rate "``).
    """
    if value is None:
        return ""
    return " ".join(str(value).split()).upper()


def _resolve_columns_payment(header: list[object]) -> dict[str, int]:
    """Resolve column positions for AA / BB / FF (HCPCS-keyed payment).

    The literal aliases (``HCPCS`` / ``MOD`` / ``SHORT DESCRIPTOR``) come
    from ``_HEADER_ALIASES_PAYMENT``. ``payment_indicator`` and
    ``payment_rate`` are matched by substring because CMS prefixes them
    with the month/year (e.g. ``"April 2026 Payment Indicator"``).

    Required: ``code``. All others best-effort.
    """
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}

    # Literal alias resolution.
    for logical, alts in _HEADER_ALIASES_PAYMENT.items():
        for alias in alts:
            try:
                out[logical] = norm.index(alias.upper())
                break
            except ValueError:
                continue

    # Substring resolution for month-prefixed columns.
    for logical, needle in (
        ("payment_indicator", "PAYMENT INDICATOR"),
        ("payment_rate", "PAYMENT RATE"),
    ):
        if logical in out:
            continue
        for i, h in enumerate(norm):
            if needle in h:
                out[logical] = i
                break

    if "code" not in out:
        raise RuntimeError(
            f"asc: required column 'code' not found in payment header; "
            f"saw {norm[:20]}..."
        )
    return out


def _resolve_columns_ee(header: list[object]) -> dict[str, int]:
    """Resolve column positions for EE (HCPCS-only excluded procedures)."""
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}
    for logical, alts in _HEADER_ALIASES_EE.items():
        for alias in alts:
            try:
                out[logical] = norm.index(alias.upper())
                break
            except ValueError:
                continue
    if "code" not in out:
        raise RuntimeError(
            f"asc: required column 'code' not found in EE header; "
            f"saw {norm[:20]}..."
        )
    return out


# ----------------------------------------------------------------------------
# ZIP / member discovery
# ----------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"asc: corrupt ZIP at {path}: {exc}") from exc


# Per-addendum filename predicates. Order of precedence inside the function:
#   P1: "Addendum {LETTER}" substring (CY2025+ Drupal-era subdirectory layout).
#   P2: " {LETTER}." or "_{LETTER}." or "-{LETTER}." right before .txt / .csv
#       (CY2021-CY2024 flat layout, e.g. ``January 2024 ASC AA.txt``).
#   P3: For EE only, accept a bare ``EE.{txt|csv}`` token to absorb the
#       CY2024 Q1 anomaly (``January 2024 EE.txt`` drops the ASC token).
def _find_addendum_member(zf: zipfile.ZipFile, addendum: str) -> str | None:
    """Locate the ZIP member for a given addendum. Returns None if absent.

    Members under the ``508 Version ... ASC Addenda/`` subdirectory are
    fair game; the predicate operates on the FULL member path including
    subdirectory prefix.

    The ``.xlsx`` master at the ZIP root is intentionally NOT picked up;
    this scanner only considers ``.txt`` and ``.csv`` members.
    """
    addendum_upper = addendum.strip().upper()
    members = list(zf.namelist())
    text_members = [
        m
        for m in members
        if Path(m).name.lower().endswith((".txt", ".csv"))
    ]
    if not text_members:
        return None

    addendum_substr = f"ADDENDUM {addendum_upper}"
    p1 = [m for m in text_members if addendum_substr in m.upper()]
    if p1:
        # Prefer non-508 if both present.
        non_508 = [m for m in p1 if "508" not in Path(m).name.upper()]
        return (non_508 or p1)[0]

    # P2: word-boundary letter right before extension.
    # Pattern: separator (space, dash, underscore) + LETTER + extension.
    # Use re.IGNORECASE on the basename to absorb mixed casing.
    p2_re = re.compile(
        rf"(?:^|[\s_\-]){re.escape(addendum_upper)}\.(?:txt|csv)$",
        re.IGNORECASE,
    )
    p2 = [m for m in text_members if p2_re.search(Path(m).name)]
    if p2:
        non_508 = [m for m in p2 if "508" not in Path(m).name.upper()]
        return (non_508 or p2)[0]

    # P3: EE bare-token fallback (CY2024 Q1 anomaly).
    if addendum_upper == "EE":
        p3_re = re.compile(r"(?:^|[\s_\-])EE\.(?:txt|csv)$", re.IGNORECASE)
        p3 = [m for m in text_members if p3_re.search(Path(m).name)]
        if p3:
            non_508 = [m for m in p3 if "508" not in Path(m).name.upper()]
            return (non_508 or p3)[0]

    return None


def _extract_member_to_path(
    zf: zipfile.ZipFile, member: str, dest: Path
) -> Path:
    """Extract a ZIP member to ``dest`` (idempotent - skip if already present)."""
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
    """Read ``path`` as delimited rows. ``.txt`` -> tab; otherwise comma.

    Tolerates a UTF-8 BOM (``utf-8-sig``); falls back to Latin-1 on a
    UnicodeDecodeError.
    """
    delimiter = "\t" if path.suffix.lower() == ".txt" else ","
    encodings = ("utf-8-sig", "latin-1")
    last_exc: Exception | None = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [list(row) for row in csv.reader(fh, delimiter=delimiter)]
        except UnicodeDecodeError as exc:  # noqa: PERF203
            last_exc = exc
            continue
    raise RuntimeError(
        f"asc: could not decode {path} with any of {encodings}: {last_exc}"
    )


def _find_payment_header_index(rows: list[list[str]]) -> int:
    """Return the 0-indexed row whose first non-empty cell is HCPCS / HCPC.

    AA / BB / FF / EE addenda all use a HCPCS-coded first column under one of
    the ``_HEADER_ALIASES_PAYMENT["code"]`` aliases. Scan the first 30 rows.
    """
    primary_aliases = {a.upper() for a in _HEADER_ALIASES_PAYMENT["code"]}
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        for cell in row:
            norm = _normalize_header(cell)
            if not norm:
                continue
            if norm in primary_aliases:
                return i
            break
    raise RuntimeError(
        f"asc: payment header row not found in first 30 rows; "
        f"first-row first-cells seen: "
        f"{[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


def _find_ee_header_index(rows: list[list[str]]) -> int:
    """Return the 0-indexed row whose first non-empty cell is HCPCS / HCPC."""
    primary_aliases = {a.upper() for a in _HEADER_ALIASES_EE["code"]}
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        for cell in row:
            norm = _normalize_header(cell)
            if not norm:
                continue
            if norm in primary_aliases:
                return i
            break
    raise RuntimeError(
        f"asc: EE header row not found in first 30 rows; "
        f"first-row first-cells seen: "
        f"{[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


def _find_definition_header_index(rows: list[list[str]]) -> int | None:
    """Return the 0-indexed row of the column header for DD1 / DD2.

    DD1 / DD2 are reference tables - they MAY not have a column header at
    all (some CY/quarter releases jump straight into data). When a header
    is present its first cell is typically
    ``"Payment Indicator"`` / ``"Status Indicator"`` (or a near-variant).
    Returns None when no plausible header row is found in the first 10
    rows; caller then treats row 0 as data.
    """
    needles_first = ("PAYMENT INDICATOR", "STATUS INDICATOR", "INDICATOR")
    needles_second = ("DESCRIPTION", "DEFINITION", "DESCRIPTOR")
    for i, row in enumerate(rows[:10]):
        if not row:
            continue
        first = _normalize_header(row[0]) if len(row) > 0 else ""
        second = _normalize_header(row[1]) if len(row) > 1 else ""
        if first in needles_first or any(n == first for n in needles_first):
            return i
        if any(n in first for n in needles_first) and any(
            n in second for n in needles_second
        ):
            return i
    return None


# ----------------------------------------------------------------------------
# Cell-level value parsers
# ----------------------------------------------------------------------------


def _parse_code_field(value: object) -> str | None:
    """Normalize a HCPCS/CPT code to upper-case, no-dot, no-whitespace."""
    if value is None:
        return None
    s = str(value).strip().upper().replace(".", "")
    return s or None


def _parse_modifier(value: object) -> str | None:
    """Normalize the modifier column. Strip leading dashes/whitespace, upper."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.lstrip("-").strip().upper()
    return s or None


def _parse_text(value: object) -> str | None:
    """Strip whitespace from a free-text column. Return None if empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _parse_payment_indicator(value: object) -> str | None:
    """Normalize an ASC PI code (e.g. ``A2``, ``J7``) to upper-case."""
    if value is None:
        return None
    s = str(value).strip().upper()
    return s or None


def _parse_money(value: object) -> float | None:
    """Best-effort float parse of a payment_rate column.

    Strips ``$`` / commas / whitespace. Treats ``NA`` / ``N/A`` / ``--`` /
    ``.`` / ``NULL`` / empty as None. Unparseable values return None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.upper() in {"NA", "N/A", "NULL", "--", "-", "."}:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify_code_system(code: str) -> str | None:
    """Return ``"CPT"`` / ``"HCPCS-II"`` / None for an ASC payment code."""
    if _CPT_CODE_RE.match(code):
        return "CPT"
    if _HCPCS_CODE_RE.match(code):
        return "HCPCS-II"
    return None


# ----------------------------------------------------------------------------
# Per-row dataclasses
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParsedPaymentRow:
    code: str
    modifier: str | None
    code_system: str
    payment_indicator: str | None
    payment_rate: float | None
    short_descriptor: str | None


@dataclass(frozen=True)
class _ParsedEERow:
    code: str
    short_descriptor: str | None


@dataclass(frozen=True)
class _ParsedDefinitionRow:
    indicator: str
    description: str | None


def _rows_payment_from_csv(csv_path: Path) -> list[_ParsedPaymentRow]:
    """Parse an AA / BB / FF text member into ``_ParsedPaymentRow`` records.

    Within-CSV `(code, modifier)` duplicates are dropped first-occurrence-wins
    with a stderr warning. This mirrors the OPPS extractor pattern and handles
    the BB Addendum case where CMS publishes multiple rows per J-code (drug
    formulations) per CY-quarter — the silver layer's compound-uniqueness key
    `(code, modifier, cy, quarter, addendum)` requires exactly one row per
    triple.
    """
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"asc: file {csv_path} parsed to zero rows")

    header_idx = _find_payment_header_index(rows)
    header = list(rows[header_idx])
    cols = _resolve_columns_payment(header)

    has_modifier = "modifier" in cols
    has_pi = "payment_indicator" in cols
    has_rate = "payment_rate" in cols
    has_short = "short_descriptor" in cols

    out: list[_ParsedPaymentRow] = []
    seen: set[tuple[str, str | None]] = set()
    duplicates_skipped = 0
    for raw in rows[header_idx + 1 :]:
        if not raw or len(raw) <= cols["code"]:
            continue
        first_cell = _normalize_header(raw[cols["code"]])
        if not first_cell:
            continue
        if any(first_cell.startswith(p) for p in _FOOTER_PREFIXES):
            continue

        code = _parse_code_field(raw[cols["code"]])
        if code is None:
            continue
        code_system = _classify_code_system(code)
        if code_system is None:
            continue

        modifier: str | None = None
        if has_modifier and len(raw) > cols["modifier"]:
            modifier = _parse_modifier(raw[cols["modifier"]])

        dup_key = (code, modifier)
        if dup_key in seen:
            duplicates_skipped += 1
            continue
        seen.add(dup_key)

        pi: str | None = None
        if has_pi and len(raw) > cols["payment_indicator"]:
            pi = _parse_payment_indicator(raw[cols["payment_indicator"]])

        rate: float | None = None
        if has_rate and len(raw) > cols["payment_rate"]:
            rate = _parse_money(raw[cols["payment_rate"]])

        short: str | None = None
        if has_short and len(raw) > cols["short_descriptor"]:
            short = _parse_text(raw[cols["short_descriptor"]])

        out.append(
            _ParsedPaymentRow(
                code=code,
                modifier=modifier,
                code_system=code_system,
                payment_indicator=pi,
                payment_rate=rate,
                short_descriptor=short,
            )
        )

    if duplicates_skipped:
        print(
            f"[asc] {csv_path.name}: dropped {duplicates_skipped} within-CSV "
            f"(code, modifier) duplicate row(s) (first-occurrence-wins)",
            file=sys.stderr,
        )

    return out


def _rows_ee_from_csv(csv_path: Path) -> list[_ParsedEERow]:
    """Parse an EE text member into ``_ParsedEERow`` records (HCPCS-only)."""
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"asc: file {csv_path} parsed to zero rows")

    header_idx = _find_ee_header_index(rows)
    header = list(rows[header_idx])
    cols = _resolve_columns_ee(header)
    has_short = "short_descriptor" in cols

    out: list[_ParsedEERow] = []
    for raw in rows[header_idx + 1 :]:
        if not raw or len(raw) <= cols["code"]:
            continue
        first_cell = _normalize_header(raw[cols["code"]])
        if not first_cell:
            continue
        if any(first_cell.startswith(p) for p in _FOOTER_PREFIXES):
            continue
        code = _parse_code_field(raw[cols["code"]])
        if code is None:
            continue
        if _classify_code_system(code) is None:
            continue

        short: str | None = None
        if has_short and len(raw) > cols["short_descriptor"]:
            short = _parse_text(raw[cols["short_descriptor"]])

        out.append(_ParsedEERow(code=code, short_descriptor=short))

    return out


def _rows_definition_from_csv(csv_path: Path) -> list[_ParsedDefinitionRow]:
    """Parse a DD1 / DD2 text member into ``_ParsedDefinitionRow`` records.

    These reference tables may have NO column header at all - in which
    case row 0 is treated as data. When a header IS present
    (``Payment Indicator`` / ``Status Indicator`` + ``Description`` style),
    skip it. First column = indicator code; second column = description.
    """
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"asc: file {csv_path} parsed to zero rows")

    header_idx = _find_definition_header_index(rows)
    data_start = (header_idx + 1) if header_idx is not None else 0

    out: list[_ParsedDefinitionRow] = []
    for raw in rows[data_start:]:
        if not raw:
            continue
        first = str(raw[0]).strip().upper() if len(raw) > 0 else ""
        if not first:
            continue
        if any(first.startswith(p) for p in _FOOTER_PREFIXES):
            continue
        # Second column is description (may be empty).
        desc = _parse_text(raw[1]) if len(raw) > 1 else None
        # Drop rows whose indicator is ridiculously long - those are
        # almost certainly disclaimer prose that slipped past the
        # banner-row filter.
        if len(first) > 8:
            continue
        out.append(_ParsedDefinitionRow(indicator=first, description=desc))

    return out


# ----------------------------------------------------------------------------
# Parquet schemas + emission
# ----------------------------------------------------------------------------


# AA / BB / FF: HCPCS-keyed payment rows.
_PARQUET_SCHEMA_PAYMENT: pa.Schema = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("modifier", pa.string(), nullable=True),
        pa.field("code_system", pa.string(), nullable=False),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("addendum", pa.string(), nullable=False),
        pa.field("payment_indicator", pa.string(), nullable=True),
        pa.field("payment_rate", pa.float64(), nullable=True),
        pa.field("short_descriptor", pa.string(), nullable=True),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)

# DD1: ASC Payment Indicator definitions.
_PARQUET_SCHEMA_DD1: pa.Schema = pa.schema(
    [
        pa.field("payment_indicator", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)

# DD2: ASC Status Indicator definitions.
_PARQUET_SCHEMA_DD2: pa.Schema = pa.schema(
    [
        pa.field("status_indicator", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)

# EE: Excluded procedures (HCPCS-only).
_PARQUET_SCHEMA_EE: pa.Schema = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("short_descriptor", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
    ]
)


def _build_table_payment(
    rows: list[_ParsedPaymentRow],
    *,
    cy: int,
    quarter: str,
    addendum: str,
    source_url: str,
    eff_start: date,
    eff_end: date,
) -> pa.Table:
    code_col: list[str] = []
    mod_col: list[str | None] = []
    cs_col: list[str] = []
    cy_col: list[int] = []
    q_col: list[str] = []
    add_col: list[str] = []
    pi_col: list[str | None] = []
    rate_col: list[float | None] = []
    short_col: list[str | None] = []
    eff_start_col: list[date] = []
    eff_end_col: list[date] = []
    source_col: list[str] = []

    for r in rows:
        code_col.append(r.code)
        mod_col.append(r.modifier)
        cs_col.append(r.code_system)
        cy_col.append(cy)
        q_col.append(quarter)
        add_col.append(addendum)
        pi_col.append(r.payment_indicator)
        rate_col.append(r.payment_rate)
        short_col.append(r.short_descriptor)
        eff_start_col.append(eff_start)
        eff_end_col.append(eff_end)
        source_col.append(source_url)

    return pa.Table.from_arrays(
        [
            pa.array(code_col, type=pa.string()),
            pa.array(mod_col, type=pa.string()),
            pa.array(cs_col, type=pa.string()),
            pa.array(cy_col, type=pa.int32()),
            pa.array(q_col, type=pa.string()),
            pa.array(add_col, type=pa.string()),
            pa.array(pi_col, type=pa.string()),
            pa.array(rate_col, type=pa.float64()),
            pa.array(short_col, type=pa.string()),
            pa.array(eff_start_col, type=pa.date32()),
            pa.array(eff_end_col, type=pa.date32()),
            pa.array(source_col, type=pa.string()),
        ],
        schema=_PARQUET_SCHEMA_PAYMENT,
    )


def _build_table_definition(
    rows: list[_ParsedDefinitionRow],
    *,
    cy: int,
    quarter: str,
    source_url: str,
    indicator_field: str,
) -> pa.Table:
    """Build a DD1 (``payment_indicator``) or DD2 (``status_indicator``) table."""
    schema = _PARQUET_SCHEMA_DD1 if indicator_field == "payment_indicator" else _PARQUET_SCHEMA_DD2
    ind_col: list[str] = []
    desc_col: list[str | None] = []
    cy_col: list[int] = []
    q_col: list[str] = []
    source_col: list[str] = []
    for r in rows:
        ind_col.append(r.indicator)
        desc_col.append(r.description)
        cy_col.append(cy)
        q_col.append(quarter)
        source_col.append(source_url)
    return pa.Table.from_arrays(
        [
            pa.array(ind_col, type=pa.string()),
            pa.array(desc_col, type=pa.string()),
            pa.array(cy_col, type=pa.int32()),
            pa.array(q_col, type=pa.string()),
            pa.array(source_col, type=pa.string()),
        ],
        schema=schema,
    )


def _build_table_ee(
    rows: list[_ParsedEERow],
    *,
    cy: int,
    quarter: str,
    source_url: str,
) -> pa.Table:
    code_col: list[str] = []
    cy_col: list[int] = []
    q_col: list[str] = []
    short_col: list[str | None] = []
    source_col: list[str] = []
    for r in rows:
        code_col.append(r.code)
        cy_col.append(cy)
        q_col.append(quarter)
        short_col.append(r.short_descriptor)
        source_col.append(source_url)
    return pa.Table.from_arrays(
        [
            pa.array(code_col, type=pa.string()),
            pa.array(cy_col, type=pa.int32()),
            pa.array(q_col, type=pa.string()),
            pa.array(short_col, type=pa.string()),
            pa.array(source_col, type=pa.string()),
        ],
        schema=_PARQUET_SCHEMA_EE,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def _output_dir_default() -> Path:
    return config.SILVER_PER_SOURCE / _BRONZE_SOURCE


def _synthesize_bronze_filename(url: str, cy: int, quarter: str) -> str:
    """Derive a bronze-tree-safe filename for a given ASC URL.

    Most cells have a clean URL filename (e.g. ``2024-Jan-ASC-Addenda.zip``);
    a handful (CY2024 Q4 ``...-updated-09/24/2024.zip``, CY2025 Q2
    ``...-updated-04/07/2025.zip``) carry literal ``/`` from dated
    suffixes that ``Path(url).name`` would collapse to ``"2024.zip"``,
    colliding across cells. URLs with ``-0`` Drupal uniqueness suffix
    (CY2025 Q4, CY2026 Q1) are not degenerate (zip-0 is a valid filename)
    but we still sanity-check.
    """
    name = Path(url).name
    degenerate = (
        not name
        or "/" in name
        or not name.lower().endswith((".zip", ".zip-0"))
    )
    if degenerate:
        return f"asc_addenda_cy{cy}_{quarter.lower()}.zip"
    return name


def extract_cy_quarter(
    cy: int,
    quarter: str,
    *,
    addenda: list[str] | None = None,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Extract ASC addenda for ``(cy, quarter)`` - downloads ONE ZIP, emits up to 6 parquets.

    Parameters
    ----------
    cy:
        Calendar year. Must be in ``config.ASC_CYS``.
    quarter:
        Quarter letter (one of ``A``, ``B``, ``C``, ``D``).
    addenda:
        Subset of ``config.ASC_ADDENDA`` to emit. None = all 6.
    session:
        Optional shared ``requests.Session``.
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze date subdir.
    output_dir:
        Optional override for ``config.SILVER_PER_SOURCE / "asc"``.

    Returns
    -------
    list[Path]
        Silver parquet paths that were written (one per requested addendum
        whose member was located in the ZIP).

    Raises
    ------
    KeyError
        If ``(cy, quarter)`` is not in ``config._ASC_ZIP_BY_CY_QUARTER``.
    RuntimeError
        On HTTP failure (e.g. 404 from a placeholder), ZIP corruption, or
        text-member parse failure. The URL and (cy, quarter) are included.
    """
    quarter_upper = quarter.strip().upper()
    if cy not in config.ASC_CYS:
        raise KeyError(
            f"asc cy{cy}: not registered in config.ASC_CYS "
            f"(known CYs: {min(config.ASC_CYS)}-{max(config.ASC_CYS)})"
        )
    if quarter_upper not in config.ASC_QUARTERS:
        raise KeyError(
            f"asc cy{cy} quarter{quarter!r}: not in config.ASC_QUARTERS "
            f"({config.ASC_QUARTERS})"
        )

    addenda_norm: list[str]
    if addenda is None:
        addenda_norm = list(config.ASC_ADDENDA)
    else:
        addenda_norm = []
        for a in addenda:
            au = a.strip().upper()
            if au not in config.ASC_ADDENDA:
                raise KeyError(
                    f"asc cy{cy} addendum{a!r}: not in config.ASC_ADDENDA "
                    f"({config.ASC_ADDENDA})"
                )
            addenda_norm.append(au)

    key = (cy, quarter_upper)
    if key not in config._ASC_ZIP_BY_CY_QUARTER:
        raise KeyError(
            f"asc cy{cy} q{quarter_upper}: missing entry in "
            f"config._ASC_ZIP_BY_CY_QUARTER"
        )

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _output_dir_default()
    out_dir.mkdir(parents=True, exist_ok=True)

    url = config._ASC_ZIP_BY_CY_QUARTER[key]
    print(
        f"[asc] cy{cy} q{quarter_upper}: URL {url}",
        file=sys.stderr,
    )

    filename = _synthesize_bronze_filename(url, cy, quarter_upper)
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
            f"asc cy{cy} q{quarter_upper}: {url} failed: {exc}"
        ) from exc

    eff_start, eff_end = config.asc_quarter_interval(cy, quarter_upper)
    written: list[Path] = []
    missing: list[str] = []

    with _open_zip(bronze_path) as zf:
        for addendum in addenda_norm:
            member = _find_addendum_member(zf, addendum)
            if member is None:
                missing.append(addendum)
                print(
                    f"[asc] cy{cy} q{quarter_upper} {addendum}: "
                    f"member not found in ZIP, skipping",
                    file=sys.stderr,
                )
                continue
            # Extract member to bronze sibling path (preserve basename only).
            text_dest = bronze_path.parent / Path(member).name
            _extract_member_to_path(zf, member, text_dest)
            print(
                f"[asc] cy{cy} q{quarter_upper} {addendum}: "
                f"parsing {text_dest.name}",
                file=sys.stderr,
            )

            try:
                if addendum in {"AA", "BB", "FF"}:
                    parsed_p = _rows_payment_from_csv(text_dest)
                    if not parsed_p:
                        raise RuntimeError(
                            f"asc cy{cy} q{quarter_upper} {addendum}: "
                            f"zero matching rows; check header / regex"
                        )
                    table = _build_table_payment(
                        parsed_p,
                        cy=cy,
                        quarter=quarter_upper,
                        addendum=addendum,
                        source_url=url,
                        eff_start=eff_start,
                        eff_end=eff_end,
                    )
                elif addendum == "EE":
                    parsed_e = _rows_ee_from_csv(text_dest)
                    if not parsed_e:
                        raise RuntimeError(
                            f"asc cy{cy} q{quarter_upper} EE: "
                            f"zero matching rows; check header / regex"
                        )
                    table = _build_table_ee(
                        parsed_e,
                        cy=cy,
                        quarter=quarter_upper,
                        source_url=url,
                    )
                elif addendum == "DD1":
                    parsed_d = _rows_definition_from_csv(text_dest)
                    if not parsed_d:
                        raise RuntimeError(
                            f"asc cy{cy} q{quarter_upper} DD1: zero rows"
                        )
                    table = _build_table_definition(
                        parsed_d,
                        cy=cy,
                        quarter=quarter_upper,
                        source_url=url,
                        indicator_field="payment_indicator",
                    )
                elif addendum == "DD2":
                    parsed_d = _rows_definition_from_csv(text_dest)
                    if not parsed_d:
                        raise RuntimeError(
                            f"asc cy{cy} q{quarter_upper} DD2: zero rows"
                        )
                    table = _build_table_definition(
                        parsed_d,
                        cy=cy,
                        quarter=quarter_upper,
                        source_url=url,
                        indicator_field="status_indicator",
                    )
                else:  # pragma: no cover — guarded above
                    raise RuntimeError(f"asc: unknown addendum {addendum!r}")
            except Exception as exc:
                # Re-raise with extra context so bulk-mode logs point at the
                # right (cy, q, addendum) tuple.
                raise RuntimeError(
                    f"asc cy{cy} q{quarter_upper} {addendum}: parse failed: {exc}"
                ) from exc

            out_path = (
                out_dir
                / f"asc_{addendum.lower()}_cy{cy}_{quarter_upper}.parquet"
            )
            pq.write_table(table, str(out_path))
            print(
                f"[asc] cy{cy} q{quarter_upper} {addendum}: "
                f"wrote {table.num_rows} rows to {out_path}",
                file=sys.stderr,
            )
            written.append(out_path)

    if missing:
        print(
            f"[asc] cy{cy} q{quarter_upper}: addenda not found in ZIP: "
            f"{missing}",
            file=sys.stderr,
        )
    return written


def extract_cy_quarter_range(
    start_cy: int,
    end_cy: int,
    *,
    quarters: list[str] | None = None,
    addenda: list[str] | None = None,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_cy_quarter`` over CYs x quarters.

    ``quarters`` defaults to ``config.ASC_QUARTERS``; ``addenda`` defaults
    to ``config.ASC_ADDENDA``. Per-cell failures are logged loudly and
    skipped (mirrors HCPCS / PFS / OPPS bulk semantics). Returns the list
    of successfully written parquet paths.
    """
    if end_cy < start_cy:
        raise ValueError(f"end_cy ({end_cy}) must be >= start_cy ({start_cy})")

    qs_input = list(quarters) if quarters is not None else list(config.ASC_QUARTERS)
    qs_normalized: list[str] = []
    for q in qs_input:
        qu = q.strip().upper()
        if qu not in config.ASC_QUARTERS:
            raise ValueError(
                f"asc: quarter {q!r} not in config.ASC_QUARTERS "
                f"({config.ASC_QUARTERS})"
            )
        qs_normalized.append(qu)

    ad_input = list(addenda) if addenda is not None else list(config.ASC_ADDENDA)
    ad_normalized: list[str] = []
    for a in ad_input:
        au = a.strip().upper()
        if au not in config.ASC_ADDENDA:
            raise ValueError(
                f"asc: addendum {a!r} not in config.ASC_ADDENDA "
                f"({config.ASC_ADDENDA})"
            )
        ad_normalized.append(au)

    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str, str]] = []
    for cy in range(start_cy, end_cy + 1):
        if cy not in config.ASC_CYS:
            print(
                f"[asc] CY{cy}: not in config.ASC_CYS, skipping",
                file=sys.stderr,
            )
            continue
        for quarter in qs_normalized:
            try:
                paths = extract_cy_quarter(
                    cy,
                    quarter,
                    addenda=ad_normalized,
                    session=sess,
                    download_date=download_date,
                )
                out.extend(paths)
            except Exception as exc:  # noqa: BLE001 — bulk run swallows
                print(
                    f"[asc] CY{cy} Q{quarter} FAILED: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                failures.append(
                    (cy, quarter, f"{type(exc).__name__}: {exc}")
                )
    if failures:
        print(
            f"[asc] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for cy, q, msg in failures:
            print(f"  CY{cy} Q{q}: {msg}", file=sys.stderr)
    print(
        f"[asc] bulk run summary: {len(out)} parquet(s) written, "
        f"{len(failures)} (cy, q) cell(s) failed",
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
        if q not in config.ASC_QUARTERS:
            raise argparse.ArgumentTypeError(
                f"--quarter-range letters must be in {config.ASC_QUARTERS}, "
                f"got {spec!r}"
            )
    if config.ASC_QUARTERS.index(a) > config.ASC_QUARTERS.index(b):
        raise argparse.ArgumentTypeError(
            f"--quarter-range must be ascending; got {spec!r}"
        )
    return a, b


def _quarters_from_range(qr: tuple[str, str]) -> list[str]:
    a, b = qr
    i = config.ASC_QUARTERS.index(a)
    j = config.ASC_QUARTERS.index(b)
    return list(config.ASC_QUARTERS[i : j + 1])


def _parse_addendum_spec(spec: str) -> list[str]:
    """Parse a comma-separated addendum spec like ``"AA,BB"`` -> ``["AA","BB"]``."""
    parts = [p.strip().upper() for p in spec.split(",") if p.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(
            f"--addendum needs at least one of {config.ASC_ADDENDA} (got {spec!r})"
        )
    for p in parts:
        if p not in config.ASC_ADDENDA:
            raise argparse.ArgumentTypeError(
                f"--addendum letters must be in {config.ASC_ADDENDA}, got {spec!r}"
            )
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.asc",
        description=(
            "Download and parse the CMS ASC Addenda ZIP for one (calendar-year, "
            "quarter) cell or a bulk range, demultiplexing the bundled "
            "AA/BB/DD1/DD2/EE/FF members into per-cell parquets in "
            "data/silver/per_source/asc/."
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
        help="Single ASC quarter letter (one of A/B/C/D).",
    )
    q_group.add_argument(
        "--quarter-range",
        type=_parse_quarter_range,
        metavar="START-END",
        help="Inclusive quarter-letter range, e.g., 'A-D'.",
    )
    parser.add_argument(
        "--addendum",
        type=_parse_addendum_spec,
        default=list(config.ASC_ADDENDA),
        help=(
            "Comma-separated addendum letters "
            "(default: 'AA,BB,DD1,DD2,EE,FF')."
        ),
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
        if q_upper not in config.ASC_QUARTERS:
            parser.error(
                f"--quarter must be one of {config.ASC_QUARTERS}, "
                f"got {args.quarter!r}"
            )
        quarters = [q_upper]
    else:
        quarters = _quarters_from_range(args.quarter_range)

    addenda: list[str] = list(args.addendum)

    # Single-cell path: only when one CY and one quarter (any subset of addenda).
    if single_cy and single_quarter:
        extract_cy_quarter(
            start_cy,
            quarters[0],
            addenda=addenda,
            download_date=args.download_date,
        )
        return 0

    # Anything else: bulk path with log+skip semantics.
    extract_cy_quarter_range(
        start_cy,
        end_cy,
        quarters=quarters,
        addenda=addenda,
        download_date=args.download_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
