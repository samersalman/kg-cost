"""OPPS extractor: per-(CY, quarter, addendum) CMS Addendum A/B ZIP -> silver parquet.

For each ``(calendar_year, quarter, addendum)`` cell in scope this module:

    1. Reads the per-cell URL from ``config.OPPS_SOURCES``. Keys are
       ``(cy, "A"|"B"|"C"|"D", "A"|"B")``. CMS has used (at least) two URL
       eras for OPPS Addendum publishing - legacy
       ``hospitaloutpatientpps/downloads/`` (mixed and lower-cased path
       variants per CY) and modern Drupal ``files/zip/{slug}.zip`` - and the
       per-CY filename-slug drift is documented in CLAUDE.md. A 404 from any
       cell surfaces as ``HTTPError`` that ``extract_cy_quarter_addendum_range``
       catches and logs+skips (mirrors HCPCS / PFS).

    2. Downloads via ``_http.download_to_bronze`` with ``period="cy"``. The
       bronze ZIP filename is derived from the URL basename when sane. When
       the URL basename is empty, contains literal ``/``, or is a known
       degenerate placeholder (e.g. ``addendum.zip`` for CY2024 Q2 A), the
       extractor synthesizes a canonical
       ``opps_addendum_{a|b}_cy{cy}_{q}.zip`` instead so the bronze tree
       stays self-describing.

    3. Parses the inner Addendum CSV. Unlike PPRRVU (PFS), OPPS Addenda ship
       a SINGLE-row column header (no stacked qualifiers) preceded by a 2-4
       row legal/disclaimer block. Header detection scans the first ~30 rows
       for the row whose first non-empty cell is a known column-alias
       keyword (``HCPCS`` / ``HCPCS CODE`` for B, ``APC`` / ``APC CODE`` for
       A) - then returns rows after it as data.

    4. Filters:
        - Addendum B: code must match either CPT (5-digit numeric) or
          HCPCS-II (``[A-V0-9][0-9]{4}``). Status indicators ``E1`` / ``E2``
          (non-payment) are dropped when a status column is present.
        - Addendum A: ``apc`` must match ``^[0-9]{4,5}$`` after zero-padding;
          rows with empty / footer-keyword first cells are dropped.

    5. Writes a per-cell parquet to either
       ``data/silver/per_source/opps/opps_a_cy{cy}_{q}.parquet`` or
       ``opps_b_cy{cy}_{q}.parquet``. Schema is locked
       (plans/plan-v5.md lines 100-149); column order is load-bearing.

CLI::

    python -m src.extract.opps --cy 2024 --quarter D
    python -m src.extract.opps --cy 2024 --quarter-range A-D --addendum A,B
    python -m src.extract.opps --cy-range 2008-2026 --quarter-range A-D
    python -m src.extract.opps --cy 2024 --quarter D --addendum B --download-date 2026-04-30
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

_BRONZE_SOURCE: str = "opps"

# CPT Level I: 5-digit pure numeric.
_CPT_CODE_RE = re.compile(r"^[0-9]{5}$")
# HCPCS Level II: 5-character upper-case alphanumeric. First char A-V or 0-9.
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")
# APC code (post zero-pad): 4 or 5 digits.
_APC_CODE_RE = re.compile(r"^[0-9]{4,5}$")

# OPPS status indicators that exclude a row from the payment layer.
# `E1` = not paid by Medicare when submitted on outpatient claims;
# `E2` = not paid by Medicare when submitted on outpatient claims (any
# outpatient bill type). Both are non-payment SIs. Easy to relax by editing.
_DROPPED_STATUS_INDICATORS_OPPS: frozenset[str] = frozenset({"E1", "E2"})

# Footer / disclaimer prefixes that should never be treated as APC rows.
_FOOTER_PREFIXES: tuple[str, ...] = ("NOTE", "SEE", "*", "FOOTNOTE", "SOURCE")

# Header alias dicts. Case- and whitespace-normalized lookup
# (see ``_normalize_header``). ``required`` per addendum is gated by
# ``_resolve_columns``; everything else is best-effort.
_HEADER_ALIASES_A: dict[str, tuple[str, ...]] = {
    "apc": ("APC", "APC CODE", "APC NUMBER"),
    "apc_title": ("GROUP TITLE", "TITLE", "APC TITLE", "DESCRIPTION"),
    "status_indicator": ("SI", "STATUS INDICATOR", "STATUS"),
    "relative_weight": (
        "RELATIVE WEIGHT",
        "WEIGHT",
        "REL WEIGHT",
        "RELATIVE_WEIGHT",
    ),
    "payment_rate": (
        "PAYMENT RATE",
        "NATIONAL UNADJUSTED PAYMENT RATE",
        "PAYMENT",
    ),
    "national_unadjusted_copayment": (
        "NATIONAL UNADJUSTED COPAYMENT",
        "UNADJUSTED COPAYMENT",
        "NATL COPAY",
        "COPAYMENT",
    ),
    "minimum_unadjusted_copayment": (
        "MINIMUM UNADJUSTED COPAYMENT",
        "MIN COPAY",
        "MINIMUM COPAYMENT",
    ),
}

_HEADER_ALIASES_B: dict[str, tuple[str, ...]] = {
    "code": ("HCPCS", "HCPCS CODE", "HCPC", "HCPCS_CD", "CPT/HCPCS"),
    "modifier": ("MOD", "MODIFIER"),
    "short_descriptor": (
        "SHORT DESCRIPTOR",
        "SHORT DESCRIPTION",
        "DESCRIPTION",
        "SHORT DESC",
    ),
    "status_indicator": ("SI", "STATUS INDICATOR", "STATUS", "STATUS CODE"),
    "apc": ("APC", "APC CODE", "APC NUMBER"),
    "relative_weight": (
        "RELATIVE WEIGHT",
        "WEIGHT",
        "REL WEIGHT",
        "RELATIVE_WEIGHT",
    ),
    "payment_rate": (
        "PAYMENT RATE",
        "NATIONAL UNADJUSTED PAYMENT RATE",
        "PAYMENT",
    ),
}


def _normalize_header(value: object) -> str:
    """Return a case- and whitespace-normalized header string for matching.

    Collapses internal runs of whitespace to a single space (CMS Addendum
    headers occasionally ship with double-spaces, e.g. ``" Relative  Weight "``).
    """
    if value is None:
        return ""
    return " ".join(str(value).split()).upper()


def _resolve_columns(
    header: list[object],
    *,
    aliases: dict[str, tuple[str, ...]],
    required: tuple[str, ...],
) -> dict[str, int]:
    """Map our logical column names to 0-indexed positions in ``header``.

    Looks up each logical name's alias tuple in the case-normalized header.
    The first matching alias wins. ``required`` is the (small) set of
    columns whose absence aborts parsing; all others are best-effort.
    """
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}
    for logical, alts in aliases.items():
        for alias in alts:
            try:
                out[logical] = norm.index(alias.upper())
                break
            except ValueError:
                continue
    missing = [k for k in required if k not in out]
    if missing:
        raise RuntimeError(
            f"opps: required columns {missing} not found in header; "
            f"saw {norm[:20]}..."
        )
    return out


# ----------------------------------------------------------------------------
# ZIP / CSV discovery + parse
# ----------------------------------------------------------------------------


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"opps: corrupt ZIP at {path}: {exc}") from exc


def _find_addendum_csv_member(zf: zipfile.ZipFile, addendum: str) -> str:
    """Return the ZIP member name of the Addendum text member.

    Accepts BOTH ``.csv`` AND ``.txt`` members. Pre-CY2018 OPPS releases
    legacy-era ZIPs ship tab-delimited ``.txt`` (and an ``.xlsx`` next to
    it) instead of ``.csv``; the column structure is identical so the
    parser handles both via delimiter auto-detection in ``_read_csv_rows``.
    Modern (CY2018+) releases ship ``.csv`` directly. When both are present,
    ``.csv`` wins (newer files are typically the canonical version).

    Selection rules (in order):
      1. Members ending in ``.csv`` whose name contains ``"addendum"``
         (case-insensitive). Prefer non-508 variants. If none, fall through.
      2. Members ending in ``.txt`` whose name contains ``"addendum"``
         (case-insensitive). Prefer non-508 variants.
      3. Any text member with the single-letter addendum tag near the end.
      4. Any ``.csv`` member as a last resort; then any ``.txt`` member.

    Raises if no acceptable member is found.
    """
    addendum_lower = addendum.strip().lower()
    members = list(zf.namelist())
    csvs = [m for m in members if Path(m).name.lower().endswith(".csv")]
    txts = [m for m in members if Path(m).name.lower().endswith(".txt")]
    if not csvs and not txts:
        raise RuntimeError(
            f"opps: no .csv or .txt members in ZIP; members were "
            f"{[Path(n).name for n in members]}"
        )

    def _named_addendum(pool: list[str]) -> list[str]:
        named = [m for m in pool if "addendum" in Path(m).name.lower()]
        non_508 = [m for m in named if "508" not in Path(m).name.lower()]
        return non_508 or named

    # Pass 1: CSV named addendum.
    if csvs:
        named = _named_addendum(csvs)
        if named:
            return named[0]

    # Pass 2: TXT named addendum (legacy era).
    if txts:
        named = _named_addendum(txts)
        if named:
            return named[0]

    # Pass 3: tagged-letter member, CSV first then TXT.
    for pool in (csvs, txts):
        tagged = [
            m
            for m in pool
            if (
                Path(m).stem.lower().endswith(f"_{addendum_lower}")
                or Path(m).stem.lower().endswith(f"-{addendum_lower}")
                or Path(m).stem.lower().endswith(addendum_lower)
            )
        ]
        if tagged:
            return tagged[0]

    # Pass 4: any CSV; then any TXT.
    if csvs:
        return csvs[0]
    return txts[0]


def _extract_csv_member_to_tempfile(
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
    """Read ``path`` as delimited rows (list of lists of strings).

    Auto-detects delimiter from extension: ``.txt`` → tab, anything else
    → comma. Pre-CY2018 OPPS Addendum releases ship tab-delimited ``.txt``
    members with the same column structure as the modern ``.csv`` releases.

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
        f"opps: could not decode {path} with any of {encodings}: {last_exc}"
    )


def _find_header_index(
    rows: list[list[str]], aliases: dict[str, tuple[str, ...]]
) -> int:
    """Return the 0-indexed row whose first non-empty cell is a known
    code/apc-column alias.

    Unlike PPRRVU, OPPS Addenda use a SINGLE-row header preceded by a 2-4
    row disclaimer block. We scan the first 30 rows for the row whose
    first non-empty cell normalizes to one of the alias keywords for the
    PRIMARY (required) column of that addendum.
    """
    # The first key in `aliases` is the primary required column for that
    # addendum (`apc` for A, `code` for B). Use its alias set.
    primary_logical = next(iter(aliases))
    primary_aliases = {a.upper() for a in aliases[primary_logical]}

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
        f"opps: header row not found in first 30 rows; "
        f"first-row first-cells seen: "
        f"{[(_normalize_header(r[0]) if r else '') for r in rows[:30]]}"
    )


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


def _parse_status(value: object) -> str | None:
    """Normalize the status indicator to upper-case (1-2 chars typical)."""
    if value is None:
        return None
    s = str(value).strip().upper()
    return s or None


def _parse_text(value: object) -> str | None:
    """Strip whitespace from a free-text column. Return None if empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _parse_apc(value: object) -> str | None:
    """Normalize an APC value: strip, upper, zero-pad to 4 digits if numeric.

    Returns None for empty / whitespace-only inputs. Returns the raw
    upper-cased string for values that don't look like pure digits (caller
    filters via ``_APC_CODE_RE`` before emitting).
    """
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    if s.isdigit() and len(s) < 4:
        s = s.zfill(4)
    return s


def _parse_money(value: object) -> float | None:
    """Best-effort float parse of a payment / weight / copay column.

    Strips ``$`` / commas / whitespace. Treats ``NA`` / ``N/A`` / ``--`` /
    ``NULL`` / empty string as None. Unparseable values return None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.upper() in {"NA", "N/A", "NULL", "--", "-"}:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify_code_system(code: str) -> str | None:
    """Return ``"CPT"`` / ``"HCPCS-II"`` / None for an Addendum B code value."""
    if _CPT_CODE_RE.match(code):
        return "CPT"
    if _HCPCS_CODE_RE.match(code):
        return "HCPCS-II"
    return None


# ----------------------------------------------------------------------------
# Per-row dataclasses + CSV iteration
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParsedRowA:
    apc: str
    status_indicator: str | None
    apc_title: str | None
    relative_weight: float | None
    payment_rate: float | None
    national_unadjusted_copayment: float | None
    minimum_unadjusted_copayment: float | None


@dataclass(frozen=True)
class _ParsedRowB:
    code: str
    modifier: str | None
    code_system: str
    status_indicator: str | None
    apc: str | None
    relative_weight: float | None
    payment_rate: float | None
    short_descriptor: str | None


def _rows_a_from_csv(csv_path: Path) -> list[_ParsedRowA]:
    """Parse an Addendum A CSV into ``_ParsedRowA`` records."""
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"opps: CSV {csv_path} parsed to zero rows")

    header_idx = _find_header_index(rows, _HEADER_ALIASES_A)
    header = list(rows[header_idx])
    cols = _resolve_columns(
        header, aliases=_HEADER_ALIASES_A, required=("apc",)
    )

    has_status = "status_indicator" in cols
    has_title = "apc_title" in cols
    has_weight = "relative_weight" in cols
    has_payment = "payment_rate" in cols
    has_natl_copay = "national_unadjusted_copayment" in cols
    has_min_copay = "minimum_unadjusted_copayment" in cols

    out: list[_ParsedRowA] = []
    for raw in rows[header_idx + 1 :]:
        if not raw or len(raw) <= cols["apc"]:
            continue
        first_cell = _normalize_header(raw[cols["apc"]])
        if not first_cell:
            continue
        if any(first_cell.startswith(p) for p in _FOOTER_PREFIXES):
            continue

        apc = _parse_apc(raw[cols["apc"]])
        if apc is None or not _APC_CODE_RE.match(apc):
            continue

        def _safe_money(idx_key: str) -> float | None:
            i = cols.get(idx_key)
            if i is None or len(raw) <= i:
                return None
            return _parse_money(raw[i])

        status: str | None = None
        if has_status and len(raw) > cols["status_indicator"]:
            status = _parse_status(raw[cols["status_indicator"]])

        title: str | None = None
        if has_title and len(raw) > cols["apc_title"]:
            title = _parse_text(raw[cols["apc_title"]])

        out.append(
            _ParsedRowA(
                apc=apc,
                status_indicator=status,
                apc_title=title,
                relative_weight=_safe_money("relative_weight") if has_weight else None,
                payment_rate=_safe_money("payment_rate") if has_payment else None,
                national_unadjusted_copayment=(
                    _safe_money("national_unadjusted_copayment")
                    if has_natl_copay
                    else None
                ),
                minimum_unadjusted_copayment=(
                    _safe_money("minimum_unadjusted_copayment")
                    if has_min_copay
                    else None
                ),
            )
        )

    if not out:
        raise RuntimeError(
            f"opps: Addendum A CSV {csv_path} yielded zero matching rows; "
            f"check header detection / regex"
        )
    return out


def _rows_b_from_csv(csv_path: Path) -> list[_ParsedRowB]:
    """Parse an Addendum B CSV into ``_ParsedRowB`` records.

    Same-key duplicates within a single CSV are not silently deduped; we
    keep the first ``(code, modifier)`` occurrence and warn to stderr on
    subsequent collisions, deferring the "what to do with collisions"
    decision to the transform layer.
    """
    rows = _read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"opps: CSV {csv_path} parsed to zero rows")

    header_idx = _find_header_index(rows, _HEADER_ALIASES_B)
    header = list(rows[header_idx])
    cols = _resolve_columns(
        header, aliases=_HEADER_ALIASES_B, required=("code",)
    )

    has_modifier = "modifier" in cols
    has_status = "status_indicator" in cols
    has_apc = "apc" in cols
    has_weight = "relative_weight" in cols
    has_payment = "payment_rate" in cols
    has_short = "short_descriptor" in cols

    seen: set[tuple[str, str | None]] = set()
    duplicates: list[tuple[str, str | None]] = []

    out: list[_ParsedRowB] = []
    for raw in rows[header_idx + 1 :]:
        if not raw or len(raw) <= cols["code"]:
            continue
        code = _parse_code_field(raw[cols["code"]])
        if code is None:
            continue
        code_system = _classify_code_system(code)
        if code_system is None:
            continue

        status: str | None = None
        if has_status and len(raw) > cols["status_indicator"]:
            status = _parse_status(raw[cols["status_indicator"]])
            if status in _DROPPED_STATUS_INDICATORS_OPPS:
                continue

        modifier: str | None = None
        if has_modifier and len(raw) > cols["modifier"]:
            modifier = _parse_modifier(raw[cols["modifier"]])

        key = (code, modifier)
        if key in seen:
            duplicates.append(key)
            continue
        seen.add(key)

        apc: str | None = None
        if has_apc and len(raw) > cols["apc"]:
            apc = _parse_apc(raw[cols["apc"]])

        def _safe_money(idx_key: str) -> float | None:
            i = cols.get(idx_key)
            if i is None or len(raw) <= i:
                return None
            return _parse_money(raw[i])

        short: str | None = None
        if has_short and len(raw) > cols["short_descriptor"]:
            short = _parse_text(raw[cols["short_descriptor"]])

        out.append(
            _ParsedRowB(
                code=code,
                modifier=modifier,
                code_system=code_system,
                status_indicator=status,
                apc=apc,
                relative_weight=_safe_money("relative_weight") if has_weight else None,
                payment_rate=_safe_money("payment_rate") if has_payment else None,
                short_descriptor=short,
            )
        )

    if duplicates:
        # Print a small sample - never spam if a CSV happens to have many.
        sample = duplicates[:5]
        print(
            f"[opps] Addendum B {csv_path.name}: dropped "
            f"{len(duplicates)} duplicate (code, modifier) row(s); "
            f"sample: {sample}",
            file=sys.stderr,
        )

    if not out:
        raise RuntimeError(
            f"opps: Addendum B CSV {csv_path} yielded zero matching rows; "
            f"check header detection / regex / status filter"
        )
    return out


# ----------------------------------------------------------------------------
# Parquet emission
# ----------------------------------------------------------------------------


# Locked Phase-2 schema - plans/plan-v5.md lines 100-149. Column order is
# load-bearing.
_PARQUET_SCHEMA_A: pa.Schema = pa.schema(
    [
        pa.field("apc", pa.string(), nullable=False),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("status_indicator", pa.string(), nullable=True),
        pa.field("apc_title", pa.string(), nullable=True),
        pa.field("relative_weight", pa.float64(), nullable=True),
        pa.field("payment_rate", pa.float64(), nullable=True),
        pa.field("national_unadjusted_copayment", pa.float64(), nullable=True),
        pa.field("minimum_unadjusted_copayment", pa.float64(), nullable=True),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)


_PARQUET_SCHEMA_B: pa.Schema = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("modifier", pa.string(), nullable=True),
        pa.field("code_system", pa.string(), nullable=False),
        pa.field("cy", pa.int32(), nullable=False),
        pa.field("quarter", pa.string(), nullable=False),
        pa.field("status_indicator", pa.string(), nullable=True),
        pa.field("apc", pa.string(), nullable=True),
        pa.field("relative_weight", pa.float64(), nullable=True),
        pa.field("payment_rate", pa.float64(), nullable=True),
        pa.field("short_descriptor", pa.string(), nullable=True),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)


def _build_table_a(
    rows: list[_ParsedRowA],
    *,
    cy: int,
    quarter: str,
    source_url: str,
    eff_start: date,
    eff_end: date,
) -> pa.Table:
    apc_col: list[str] = []
    cy_col: list[int] = []
    q_col: list[str] = []
    si_col: list[str | None] = []
    title_col: list[str | None] = []
    weight_col: list[float | None] = []
    pay_col: list[float | None] = []
    natl_col: list[float | None] = []
    min_col: list[float | None] = []
    eff_start_col: list[date] = []
    eff_end_col: list[date] = []
    source_col: list[str] = []

    for r in rows:
        apc_col.append(r.apc)
        cy_col.append(cy)
        q_col.append(quarter)
        si_col.append(r.status_indicator)
        title_col.append(r.apc_title)
        weight_col.append(r.relative_weight)
        pay_col.append(r.payment_rate)
        natl_col.append(r.national_unadjusted_copayment)
        min_col.append(r.minimum_unadjusted_copayment)
        eff_start_col.append(eff_start)
        eff_end_col.append(eff_end)
        source_col.append(source_url)

    return pa.Table.from_arrays(
        [
            pa.array(apc_col, type=pa.string()),
            pa.array(cy_col, type=pa.int32()),
            pa.array(q_col, type=pa.string()),
            pa.array(si_col, type=pa.string()),
            pa.array(title_col, type=pa.string()),
            pa.array(weight_col, type=pa.float64()),
            pa.array(pay_col, type=pa.float64()),
            pa.array(natl_col, type=pa.float64()),
            pa.array(min_col, type=pa.float64()),
            pa.array(eff_start_col, type=pa.date32()),
            pa.array(eff_end_col, type=pa.date32()),
            pa.array(source_col, type=pa.string()),
        ],
        schema=_PARQUET_SCHEMA_A,
    )


def _build_table_b(
    rows: list[_ParsedRowB],
    *,
    cy: int,
    quarter: str,
    source_url: str,
    eff_start: date,
    eff_end: date,
) -> pa.Table:
    code_col: list[str] = []
    mod_col: list[str | None] = []
    cs_col: list[str] = []
    cy_col: list[int] = []
    q_col: list[str] = []
    si_col: list[str | None] = []
    apc_col: list[str | None] = []
    weight_col: list[float | None] = []
    pay_col: list[float | None] = []
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
        si_col.append(r.status_indicator)
        apc_col.append(r.apc)
        weight_col.append(r.relative_weight)
        pay_col.append(r.payment_rate)
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
            pa.array(si_col, type=pa.string()),
            pa.array(apc_col, type=pa.string()),
            pa.array(weight_col, type=pa.float64()),
            pa.array(pay_col, type=pa.float64()),
            pa.array(short_col, type=pa.string()),
            pa.array(eff_start_col, type=pa.date32()),
            pa.array(eff_end_col, type=pa.date32()),
            pa.array(source_col, type=pa.string()),
        ],
        schema=_PARQUET_SCHEMA_B,
    )


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def _output_dir_default() -> Path:
    return config.SILVER_PER_SOURCE / _BRONZE_SOURCE


def _synthesize_bronze_filename(url: str, cy: int, quarter: str, addendum: str) -> str:
    """Derive a bronze-tree-safe filename for a given OPPS URL.

    Most cells have a clean URL filename (e.g. ``january-2024-web-addendum.zip``);
    a handful of cells have URL basenames that are empty (trailing slash),
    contain a literal ``/`` (e.g. CY2023 Q1 ships
    ``january-2023-opps-addendum-updated-01/20/2023.zip`` so
    ``Path(url).name`` collapses to ``20`` or ``2023.zip``), or use a
    degenerate placeholder (CY2024 Q2 A points at ``/files/zip/addendum.zip``
    which collides with anything else following the same scheme). In those
    cases synthesize a canonical filename so the bronze tree stays
    self-describing.
    """
    name = Path(url).name
    degenerate = (
        not name
        or "/" in name
        or name.lower() in {"addendum.zip", "addendum-b.zip"}
    )
    if degenerate:
        return f"opps_addendum_{addendum.lower()}_cy{cy}_{quarter.lower()}.zip"
    return name


def extract_cy_quarter(
    cy: int,
    quarter: str,
    addendum: str,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract OPPS Addendum A or B for ``(cy, quarter, addendum)``.

    Parameters
    ----------
    cy:
        Calendar year (e.g., 2024). Must be in ``config.OPPS_CYS``.
    quarter:
        Quarter letter (one of ``A``, ``B``, ``C``, ``D``).
    addendum:
        Either ``"A"`` (APC-keyed) or ``"B"`` (HCPCS/CPT-keyed).
    session:
        Optional shared ``requests.Session``.
    download_date:
        Optional ``YYYY-MM-DD`` override for the bronze date subdir.
    output_dir:
        Optional override for ``config.SILVER_PER_SOURCE / "opps"``.

    Returns
    -------
    Path
        The silver parquet path that was written.

    Raises
    ------
    KeyError
        If ``(cy, quarter, addendum)`` is not in ``config.OPPS_SOURCES``.
    RuntimeError
        On HTTP failure (e.g. 404 from a placeholder), ZIP corruption, or
        CSV parse failure. The URL and (cy, quarter, addendum) are included.
    """
    quarter_upper = quarter.strip().upper()
    addendum_upper = addendum.strip().upper()
    if cy not in config.OPPS_CYS:
        raise KeyError(
            f"opps cy{cy}: not registered in config.OPPS_CYS "
            f"(known CYs: {min(config.OPPS_CYS)}-{max(config.OPPS_CYS)})"
        )
    if quarter_upper not in config.OPPS_QUARTERS:
        raise KeyError(
            f"opps cy{cy} quarter{quarter!r}: not in config.OPPS_QUARTERS "
            f"({config.OPPS_QUARTERS})"
        )
    if addendum_upper not in config.OPPS_ADDENDA:
        raise KeyError(
            f"opps cy{cy} addendum{addendum!r}: not in config.OPPS_ADDENDA "
            f"({config.OPPS_ADDENDA})"
        )
    key = (cy, quarter_upper, addendum_upper)
    if key not in config.OPPS_SOURCES:
        raise KeyError(
            f"opps cy{cy} q{quarter_upper} add{addendum_upper}: missing "
            f"entry in config.OPPS_SOURCES"
        )

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _output_dir_default()
    out_dir.mkdir(parents=True, exist_ok=True)

    url = config.OPPS_SOURCES[key]
    print(
        f"[opps] cy{cy} q{quarter_upper} add{addendum_upper}: URL {url}",
        file=sys.stderr,
    )

    filename = _synthesize_bronze_filename(url, cy, quarter_upper, addendum_upper)
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
            f"opps cy{cy} q{quarter_upper} add{addendum_upper}: {url} failed: {exc}"
        ) from exc

    with _open_zip(bronze_path) as zf:
        member = _find_addendum_csv_member(zf, addendum_upper)
        csv_dest = bronze_path.parent / Path(member).name
        _extract_csv_member_to_tempfile(zf, member, csv_dest)
    print(
        f"[opps] cy{cy} q{quarter_upper} add{addendum_upper}: "
        f"parsing {csv_dest.name}",
        file=sys.stderr,
    )

    eff_start, eff_end = config.opps_quarter_interval(cy, quarter_upper)

    if addendum_upper == "A":
        rows_a = _rows_a_from_csv(csv_dest)
        table = _build_table_a(
            rows_a,
            cy=cy,
            quarter=quarter_upper,
            source_url=url,
            eff_start=eff_start,
            eff_end=eff_end,
        )
        out_path = out_dir / f"opps_a_cy{cy}_{quarter_upper}.parquet"
    else:
        rows_b = _rows_b_from_csv(csv_dest)
        table = _build_table_b(
            rows_b,
            cy=cy,
            quarter=quarter_upper,
            source_url=url,
            eff_start=eff_start,
            eff_end=eff_end,
        )
        out_path = out_dir / f"opps_b_cy{cy}_{quarter_upper}.parquet"

    pq.write_table(table, str(out_path))
    print(
        f"[opps] cy{cy} q{quarter_upper} add{addendum_upper}: "
        f"wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_cy_quarter_addendum_range(
    start_cy: int,
    end_cy: int,
    *,
    quarters: list[str] | None = None,
    addenda: list[str] | None = None,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_cy_quarter`` over CYs x quarters x addenda.

    ``quarters`` defaults to ``config.OPPS_QUARTERS``; ``addenda`` defaults
    to ``config.OPPS_ADDENDA``. Per-cell failures are logged loudly and
    skipped (mirrors HCPCS / PFS bulk semantics). Returns the list of
    successfully written parquet paths.
    """
    if end_cy < start_cy:
        raise ValueError(f"end_cy ({end_cy}) must be >= start_cy ({start_cy})")

    qs_input = list(quarters) if quarters is not None else list(config.OPPS_QUARTERS)
    qs_normalized: list[str] = []
    for q in qs_input:
        qu = q.strip().upper()
        if qu not in config.OPPS_QUARTERS:
            raise ValueError(
                f"opps: quarter {q!r} not in config.OPPS_QUARTERS "
                f"({config.OPPS_QUARTERS})"
            )
        qs_normalized.append(qu)

    ad_input = list(addenda) if addenda is not None else list(config.OPPS_ADDENDA)
    ad_normalized: list[str] = []
    for a in ad_input:
        au = a.strip().upper()
        if au not in config.OPPS_ADDENDA:
            raise ValueError(
                f"opps: addendum {a!r} not in config.OPPS_ADDENDA "
                f"({config.OPPS_ADDENDA})"
            )
        ad_normalized.append(au)

    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str, str, str]] = []
    for cy in range(start_cy, end_cy + 1):
        if cy not in config.OPPS_CYS:
            print(
                f"[opps] CY{cy}: not in config.OPPS_CYS, skipping",
                file=sys.stderr,
            )
            continue
        for quarter in qs_normalized:
            for addendum in ad_normalized:
                try:
                    path = extract_cy_quarter(
                        cy,
                        quarter,
                        addendum,
                        session=sess,
                        download_date=download_date,
                    )
                    out.append(path)
                except Exception as exc:  # noqa: BLE001 — bulk run swallows
                    print(
                        f"[opps] CY{cy} Q{quarter} ADD{addendum} FAILED: "
                        f"{type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )
                    failures.append(
                        (cy, quarter, addendum, f"{type(exc).__name__}: {exc}")
                    )
    if failures:
        print(
            f"[opps] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for cy, q, a, msg in failures:
            print(f"  CY{cy} Q{q} ADD{a}: {msg}", file=sys.stderr)
    print(
        f"[opps] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
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
        if q not in config.OPPS_QUARTERS:
            raise argparse.ArgumentTypeError(
                f"--quarter-range letters must be in {config.OPPS_QUARTERS}, "
                f"got {spec!r}"
            )
    if config.OPPS_QUARTERS.index(a) > config.OPPS_QUARTERS.index(b):
        raise argparse.ArgumentTypeError(
            f"--quarter-range must be ascending; got {spec!r}"
        )
    return a, b


def _quarters_from_range(qr: tuple[str, str]) -> list[str]:
    a, b = qr
    i = config.OPPS_QUARTERS.index(a)
    j = config.OPPS_QUARTERS.index(b)
    return list(config.OPPS_QUARTERS[i : j + 1])


def _parse_addendum_spec(spec: str) -> list[str]:
    """Parse a comma-separated addendum spec like ``"A,B"`` -> ``["A","B"]``."""
    parts = [p.strip().upper() for p in spec.split(",") if p.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(
            f"--addendum needs at least one of A/B (got {spec!r})"
        )
    for p in parts:
        if p not in config.OPPS_ADDENDA:
            raise argparse.ArgumentTypeError(
                f"--addendum letters must be in {config.OPPS_ADDENDA}, got {spec!r}"
            )
    # De-dup while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.opps",
        description=(
            "Download and parse the CMS OPPS Addendum A / Addendum B ZIP for one "
            "(calendar-year, quarter, addendum) cell or a bulk range, writing "
            "per-cell parquet to data/silver/per_source/opps/."
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
        help="Single OPPS quarter letter (one of A/B/C/D).",
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
        default=list(config.OPPS_ADDENDA),
        help="Comma-separated addendum letters (default: 'A,B').",
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
        if q_upper not in config.OPPS_QUARTERS:
            parser.error(
                f"--quarter must be one of {config.OPPS_QUARTERS}, "
                f"got {args.quarter!r}"
            )
        quarters = [q_upper]
    else:
        quarters = _quarters_from_range(args.quarter_range)

    addenda: list[str] = list(args.addendum)

    # Single-cell path: only when one CY, one quarter, and one addendum.
    if single_cy and single_quarter and len(addenda) == 1:
        extract_cy_quarter(
            start_cy,
            quarters[0],
            addenda[0],
            download_date=args.download_date,
        )
        return 0

    # Anything else: bulk path with log+skip semantics.
    extract_cy_quarter_addendum_range(
        start_cy,
        end_cy,
        quarters=quarters,
        addenda=addenda,
        download_date=args.download_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
