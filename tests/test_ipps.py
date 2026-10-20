"""Phase 3 tests for the IPPS Final Rule extractor.

Mirrors ``tests/test_asc.py`` shape:

  - Module-level ``pytest.importorskip`` for ``src.extract.ipps`` so test
    collection doesn't ImportError while the extractor is being authored.
  - All data-dependent assertions skip via ``pytest.skip`` when the per-(fy,
    version, table_kind) parquet doesn't exist (so the suite passes both pre-
    and post-extract).

Spot-check target: FY2026 V43.0 — modern Drupal era, easy to spot-check
against the published IPPS Final Rule (Table 5 + Tables 1A-1E).
"""

from __future__ import annotations

import re
from datetime import date

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Use importorskip at module level so test collection is robust to the IPPS
# extractor not yet being on disk (this test file is authored concurrently
# with `src/extract/ipps.py`).
pytest.importorskip("src.extract.ipps")

_FY = 2026
_VERSION = "V43.0"
_VERSION_SHORT = "v430"

_PARQUET_TABLE5 = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table5_fy{_FY}_{_VERSION_SHORT}.parquet"
)
_PARQUET_TABLE1A = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table1a_fy{_FY}_{_VERSION_SHORT}.parquet"
)
_PARQUET_TABLE1B = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table1b_fy{_FY}_{_VERSION_SHORT}.parquet"
)
_PARQUET_TABLE1C = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table1c_fy{_FY}_{_VERSION_SHORT}.parquet"
)
_PARQUET_TABLE1D = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table1d_fy{_FY}_{_VERSION_SHORT}.parquet"
)
_PARQUET_TABLE1E = (
    config.SILVER_PER_SOURCE
    / "ipps"
    / f"ipps_table1e_fy{_FY}_{_VERSION_SHORT}.parquet"
)

_FY_START, _FY_END = config.fy_interval(_FY)

# Locked Table 5 schema: 14 columns.
_EXPECTED_COLUMNS_TABLE5: frozenset[str] = frozenset(
    {
        "ms_drg",
        "fy",
        "version",
        "title",
        "mdc",
        "drg_type",
        "severity",
        "relative_weight",
        "gmlos",
        "amlos",
        "post_acute_transfer_flag",
        "effective_start",
        "effective_end",
        "source",
    }
)

# Locked Tables 1A-1E schema: 11 columns (long-form scalars per line item).
_EXPECTED_COLUMNS_TABLE1: frozenset[str] = frozenset(
    {
        "fy",
        "version",
        "table",
        "line_item",
        "category",
        "rate_period",
        "value",
        "unit",
        "effective_start",
        "effective_end",
        "source",
    }
)

# MS-DRG codes are 1-3 digit numeric (CMS publishes either zero-padded "001"
# or unpadded "1"; the extractor MAY normalize to one or the other).
_DRG_RE = re.compile(r"^[0-9]{1,3}$")


def _skip_if_missing(parquet_path, label: str) -> None:
    if not parquet_path.exists():
        pytest.skip(
            f"{parquet_path} not present; run "
            f"`python -m src.extract.ipps --fy {_FY} --version {_VERSION}` "
            f"first ({label})"
        )


def test_extract_module_importable() -> None:
    """``src.extract.ipps`` exports the documented public API surface."""
    from src.extract import ipps

    assert hasattr(ipps, "extract_fy_version"), (
        "src.extract.ipps must export `extract_fy_version`"
    )
    assert hasattr(ipps, "extract_fy_range"), (
        "src.extract.ipps must export `extract_fy_range`"
    )
    assert hasattr(ipps, "main"), "src.extract.ipps must export `main`"


# --------------------------------------------------------------------------
# Table 5 (MS-DRG weights) tests
# --------------------------------------------------------------------------


def test_table5_parquet_schema() -> None:
    """The FY2026 V43.0 Table 5 parquet has the locked 14-column schema."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    pf = pq.ParquetFile(_PARQUET_TABLE5)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_TABLE5 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_TABLE5
    assert not missing, (
        f"Missing columns in {_PARQUET_TABLE5.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_TABLE5.name}: {sorted(extra)}"
    )

    # Type sanity for load-bearing columns.
    t_fy = str(schema.field("fy").type).lower()
    assert "int" in t_fy, f"`fy` must be an integer type, got {t_fy}"

    t_start = str(schema.field("effective_start").type).lower()
    assert "date" in t_start, (
        f"`effective_start` must be a date type, got {t_start}"
    )
    t_end = str(schema.field("effective_end").type).lower()
    assert "date" in t_end, f"`effective_end` must be a date type, got {t_end}"

    t_rw = str(schema.field("relative_weight").type).lower()
    assert "float" in t_rw or "double" in t_rw or "decimal" in t_rw, (
        f"`relative_weight` must be float/double/decimal, got {t_rw}"
    )

    t_drg = str(schema.field("ms_drg").type).lower()
    assert "string" in t_drg, f"`ms_drg` must be a string type, got {t_drg}"


def test_table5_row_count_in_range() -> None:
    """FY2026 V43.0 Table 5 publishes ~755 payable MS-DRGs."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    n = pq.ParquetFile(_PARQUET_TABLE5).metadata.num_rows
    assert 700 <= n <= 850, (
        f"Unexpected FY{_FY} {_VERSION} Table 5 row count: {n} "
        f"(expected ~755; 700-850 tolerated)"
    )


def test_table5_drg_codes_conformant() -> None:
    """Every ``ms_drg`` is 1-3 digit numeric, with consistent zero-padding.

    The extractor MAY emit zero-padded ("001") OR unpadded ("1") codes —
    either is acceptable, but mixing within a single FY's parquet is not.
    """
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    table = pq.read_table(_PARQUET_TABLE5, columns=["ms_drg"])
    codes = table.column("ms_drg").to_pylist()
    bad = [c for c in codes if c is None or not _DRG_RE.match(c)]
    assert not bad, (
        f"Found {len(bad)} non-conforming MS-DRG codes "
        f"(must match ^[0-9]{{1,3}}$; sample: {bad[:5]})"
    )

    # Padding-consistency: every non-empty code is either 3-char (zero-padded)
    # or matches its int-string form (unpadded). All codes within the parquet
    # must share one convention.
    padded = sum(1 for c in codes if c is not None and len(c) == 3)
    unpadded = sum(
        1 for c in codes if c is not None and c == str(int(c))
        and len(c) != 3
    )
    # If any unpadded codes (single- or double-digit DRGs published as "1"
    # or "23") exist, padded count must be zero, and vice versa.
    assert padded == 0 or unpadded == 0, (
        f"Mixed zero-padding convention in ms_drg column: {padded} 3-char "
        f"(zero-padded) and {unpadded} short (unpadded) codes coexist"
    )


def test_table5_version_value() -> None:
    """All rows in the FY2026 V43.0 parquet have ``version == 'V43.0'``."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    table = pq.read_table(_PARQUET_TABLE5, columns=["version"])
    versions = set(table.column("version").to_pylist())
    assert versions == {_VERSION}, (
        f"FY{_FY} Table 5 should have version=={{{_VERSION!r}}}, "
        f"got {versions}"
    )


def test_table5_effective_dates() -> None:
    """All rows span the FY2026 federal fiscal year (2025-10-01 .. 2026-09-30)."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    table = pq.read_table(
        _PARQUET_TABLE5, columns=["effective_start", "effective_end"]
    )
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_FY_START}, (
        f"effective_start should be {{{_FY_START}}}, got {starts}"
    )
    assert ends == {_FY_END}, (
        f"effective_end should be {{{_FY_END}}}, got {ends}"
    )
    # Sanity: the fy_interval helper returns these dates for FY2026.
    assert _FY_START == date(2025, 10, 1)
    assert _FY_END == date(2026, 9, 30)


def test_table5_compound_key_unique() -> None:
    """``(ms_drg, fy, version)`` is unique within Table 5."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT ms_drg, fy, version, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_PARQUET_TABLE5)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (ms_drg, fy, version) compound keys in "
        f"{_PARQUET_TABLE5.name} (sample): {dups}"
    )


def test_table5_drg_470_spot_check() -> None:
    """MS-DRG 470 (Major Joint Replacement) sits in a known sanity range.

    For FY2026 V43.0 the published values are roughly:
      relative_weight ~ 1.83-2.05, gmlos ~ 1.7, amlos ~ 1.9,
      title contains 'JOINT REPLACEMENT'.
    """
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    con = duckdb.connect()
    try:
        # Match either zero-padded '470' or unpadded '470' (3-digit DRGs are
        # the same string under both conventions, but be defensive in case
        # the extractor pads to 4 chars or similar).
        rows = con.execute(
            "SELECT ms_drg, title, relative_weight, gmlos, amlos "
            "FROM read_parquet(?) "
            "WHERE ms_drg = '470' OR ms_drg = '0470'",
            [str(_PARQUET_TABLE5)],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 1, (
        f"Expected exactly 1 row for MS-DRG 470 in FY{_FY} Table 5, "
        f"got {len(rows)}"
    )
    _drg, title, rw, gmlos, amlos = rows[0]
    assert title is not None and "JOINT REPLACEMENT" in title.upper(), (
        f"DRG 470 title should mention 'JOINT REPLACEMENT'; got {title!r}"
    )
    assert rw is not None and 1.5 <= rw <= 2.5, (
        f"DRG 470 relative_weight {rw} outside sanity range [1.5, 2.5]"
    )
    assert gmlos is not None and 1.0 <= gmlos <= 4.0, (
        f"DRG 470 gmlos {gmlos} outside sanity range [1.0, 4.0]"
    )
    assert amlos is not None and 1.0 <= amlos <= 4.5, (
        f"DRG 470 amlos {amlos} outside sanity range [1.0, 4.5]"
    )


def test_table5_drg_type_cardinality() -> None:
    """``drg_type`` is a closed vocabulary: subset of {'MED', 'SURG'}."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT drg_type FROM read_parquet(?) "
                "WHERE drg_type IS NOT NULL",
                [str(_PARQUET_TABLE5)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"MED", "SURG"}
    extra = observed - allowed
    assert not extra, (
        f"`drg_type` must be subset of {sorted(allowed)}; got extras: "
        f"{sorted(extra)}"
    )


def test_table5_severity_cardinality() -> None:
    """``severity`` is a closed vocabulary: subset of {'MCC', 'CC', 'NoCC'}."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT severity FROM read_parquet(?) "
                "WHERE severity IS NOT NULL",
                [str(_PARQUET_TABLE5)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"MCC", "CC", "NoCC"}
    extra = observed - allowed
    assert not extra, (
        f"`severity` must be subset of {sorted(allowed)}; got extras: "
        f"{sorted(extra)}"
    )


def test_table5_mdc_cardinality() -> None:
    """``mdc`` distinct values cap at ~30 (25 numeric MDCs + 'PRE' + margin)."""
    _skip_if_missing(_PARQUET_TABLE5, "Table 5")

    con = duckdb.connect()
    try:
        n_distinct = con.execute(
            "SELECT COUNT(DISTINCT mdc) FROM read_parquet(?) "
            "WHERE mdc IS NOT NULL",
            [str(_PARQUET_TABLE5)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_distinct <= 30, (
        f"`mdc` distinct cardinality {n_distinct} exceeds expected cap of 30 "
        "(25 numeric MDCs + 'PRE' + safety margin)"
    )


# --------------------------------------------------------------------------
# Table 1A (operating standardized amount — national) tests
# --------------------------------------------------------------------------


def test_table1a_parquet_schema() -> None:
    """The FY2026 V43.0 Table 1A parquet has the locked 11-column schema."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    pf = pq.ParquetFile(_PARQUET_TABLE1A)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_TABLE1 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_TABLE1
    assert not missing, (
        f"Missing columns in {_PARQUET_TABLE1A.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_TABLE1A.name}: {sorted(extra)}"
    )

    string_cols = (
        "version",
        "table",
        "line_item",
        "category",
        "unit",
        "source",
    )
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    t_fy = str(schema.field("fy").type).lower()
    assert "int" in t_fy, f"`fy` must be an integer type, got {t_fy}"

    t_value = str(schema.field("value").type).lower()
    assert "float" in t_value or "double" in t_value or "decimal" in t_value, (
        f"`value` must be float/double/decimal, got {t_value}"
    )

    for col in ("effective_start", "effective_end"):
        t = str(schema.field(col).type).lower()
        assert "date" in t, (
            f"`{col}` must be a date type, got {schema.field(col).type}"
        )


def test_table1a_row_count_in_range() -> None:
    """Tables 1A-1E are long-form scalars — Table 1A has ~3-30 rows per FY."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    n = pq.ParquetFile(_PARQUET_TABLE1A).metadata.num_rows
    assert 3 <= n <= 30, (
        f"Unexpected FY{_FY} {_VERSION} Table 1A row count: {n} "
        f"(expected ~5-15; 3-30 tolerated)"
    )


def test_table1a_table_value() -> None:
    """All rows in the Table 1A parquet have ``table == '1A'``."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    table = pq.read_table(_PARQUET_TABLE1A, columns=["table"])
    tables = set(table.column("table").to_pylist())
    assert tables == {"1A"}, (
        f"ipps_table1a_*.parquet should have table=={{'1A'}}, got {tables}"
    )


def test_table1a_unit_cardinality() -> None:
    """``unit`` is a closed vocabulary across Tables 1A-1E."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT unit FROM read_parquet(?) "
                "WHERE unit IS NOT NULL",
                [str(_PARQUET_TABLE1A)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"dollars", "ratio", "factor", "dollars_per_hour"}
    extra = observed - allowed
    assert not extra, (
        f"`unit` must be subset of {sorted(allowed)}; got extras: "
        f"{sorted(extra)}"
    )


def test_table1a_rate_period_null_for_fy2026() -> None:
    """FY2026 is not a dual-period FY — every ``rate_period`` is NULL."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    con = duckdb.connect()
    try:
        n_non_null = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE rate_period IS NOT NULL",
            [str(_PARQUET_TABLE1A)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_non_null == 0, (
        f"FY{_FY} Table 1A should have all-NULL rate_period (only FY2010 "
        f"is dual-period); found {n_non_null} non-null rows"
    )


def test_table1a_operating_full_update_present() -> None:
    """The operating full-update standardized amount sits in the dollars range.

    For FY2026 V43.0 the published value is ~$6,752 (operating, full update,
    national). Sanity range $5,000-$8,000 catches mis-extractions while
    tolerating cross-FY drift.
    """
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT line_item, value, unit FROM read_parquet(?) "
            "WHERE line_item = 'operating_full_update' "
            "  AND category = 'national'",
            [str(_PARQUET_TABLE1A)],
        ).fetchall()
    finally:
        con.close()

    assert rows, (
        "Expected at least one row with line_item='operating_full_update' "
        "AND category='national' in FY{_FY} Table 1A; found none"
    )
    matched = [
        r for r in rows
        if r[1] is not None and 5000.0 <= r[1] <= 8000.0 and r[2] == "dollars"
    ]
    assert matched, (
        f"No 'operating_full_update'/'national' row had value in "
        f"[5000.0, 8000.0] dollars; got rows: {rows}"
    )


def test_table1a_compound_key_unique() -> None:
    """``(fy, version, table, line_item, category, rate_period)`` is unique."""
    _skip_if_missing(_PARQUET_TABLE1A, "Table 1A")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT fy, version, \"table\", line_item, category, rate_period, "
            "       COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4,5,6 HAVING c>1 LIMIT 5",
            [str(_PARQUET_TABLE1A)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (fy, version, table, line_item, category, rate_period) "
        f"compound keys in {_PARQUET_TABLE1A.name} (sample): {dups}"
    )
