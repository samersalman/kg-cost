"""Phase 3 tests for the unioned IPPS Final Rule silver parquets.

Two unified outputs:

  - ``data/silver/ms_drg_weights.parquet`` — Table 5 MS-DRG weights (14 cols).
  - ``data/silver/ipps_std_amounts.parquet`` — Tables 1A-1E standardized
    amounts in long form (11 cols).

Mirrors ``tests/test_asc_rates.py``: locked-schema enforcement, compound-key
uniqueness, parity vs per-source rows, cardinality assertions.

The transform module (``src.transform.ipps_tables``) is loaded via
``pytest.importorskip`` at module level so test collection doesn't ImportError
while the transform is concurrently authored. Once it lands,
``test_transform_module_importable`` asserts the documented public API surface.

Three regimes are supported:

  1. Pre-build — neither silver parquet exists yet. All data-dependent tests
     skip cleanly.
  2. Post-build, partial coverage — only spot-check FYs are present (e.g.
     FY2026 V43.0). Schema, key-uniqueness and cardinality assertions run.
  3. Bulk — all 19 FYs are unioned. Cross-FY assertions (FY2010 dual rate
     period, FY2008/FY2009 1E absence) also run.
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# importorskip at module level: the transform is concurrent with this test.
pytest.importorskip("src.transform.ipps_tables")

_WEIGHTS_PARQUET = config.SILVER / "ms_drg_weights.parquet"
_STD_AMOUNTS_PARQUET = config.SILVER / "ipps_std_amounts.parquet"

_PER_SOURCE_TABLE5_GLOB = str(
    config.SILVER_PER_SOURCE / "ipps" / "ipps_table5_*.parquet"
)
_PER_SOURCE_TABLE1_GLOBS = [
    str(config.SILVER_PER_SOURCE / "ipps" / f"ipps_table1{ltr}_*.parquet")
    for ltr in ("a", "b", "c", "d", "e")
]

# Locked Table 5 MS-DRG weights schema: 14 columns.
_EXPECTED_COLUMNS_WEIGHTS: frozenset[str] = frozenset(
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

# Locked Tables 1A-1E IPPS standardized amounts schema: 11 columns.
_EXPECTED_COLUMNS_STD_AMOUNTS: frozenset[str] = frozenset(
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

# IPPS grouper versions covering FY2008-FY2026 (V25.0 through V43.0).
_EXPECTED_VERSIONS: frozenset[str] = frozenset(f"V{v}.0" for v in range(25, 44))

# IPPS Final Rule Tables 1A-1E.
_EXPECTED_TABLES: frozenset[str] = frozenset({"1A", "1B", "1C", "1D", "1E"})

# Allowed unit vocabulary for std_amounts.value.
_EXPECTED_UNITS: frozenset[str] = frozenset(
    {"dollars", "ratio", "factor", "dollars_per_hour"}
)

# Allowed category vocabulary for std_amounts.
_EXPECTED_CATEGORIES: frozenset[str] = frozenset(
    {"national", "rural", "urban", "puerto_rico", "alaska_hawaii"}
)

# Allowed rate-period vocabulary for std_amounts.
_EXPECTED_RATE_PERIODS: frozenset[str] = frozenset({"1st_half", "2nd_half"})

# Allowed DRG type vocabulary for ms_drg_weights.
_EXPECTED_DRG_TYPES: frozenset[str] = frozenset({"MED", "SURG"})

# Allowed severity vocabulary for ms_drg_weights.
_EXPECTED_SEVERITY: frozenset[str] = frozenset({"MCC", "CC", "NoCC"})


_SKIP_HINT = (
    "Run `python3 -m src.extract.ipps --fy-range 2008-2026 --target both` "
    "followed by `python3 -m src.transform.ipps_tables` to produce the "
    "parquets."
)


def _skip_if_missing(parquet_path, label: str) -> None:
    """Skip the calling test cleanly when ``parquet_path`` doesn't exist."""
    if not parquet_path.exists():
        pytest.skip(f"{label} not present at {parquet_path}; {_SKIP_HINT}")


# --------------------------------------------------------------------------
# Module surface
# --------------------------------------------------------------------------


def test_transform_module_importable() -> None:
    """``src.transform.ipps_tables`` exports the documented public API."""
    from src.transform import ipps_tables

    for fn in ("build_ms_drg_weights", "build_ipps_std_amounts", "main"):
        assert hasattr(ipps_tables, fn), (
            f"src.transform.ipps_tables must export `{fn}`. "
            "If the transform renamed it, update this test."
        )


# --------------------------------------------------------------------------
# Locked schemas
# --------------------------------------------------------------------------


def test_ms_drg_weights_locked_schema() -> None:
    """``ms_drg_weights.parquet`` has exactly the locked 14-column schema."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    pf = pq.ParquetFile(_WEIGHTS_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)

    missing = _EXPECTED_COLUMNS_WEIGHTS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_WEIGHTS
    assert not missing, (
        f"ms_drg_weights.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"ms_drg_weights.parquet has unexpected columns: {sorted(extra)}"
    )

    # Spot-check dtypes for fields the downstream cost-compute relies on.
    fy_field = schema.field("fy")
    assert str(fy_field.type) == "int32", (
        f"ms_drg_weights.parquet `fy` must be int32; got {fy_field.type}"
    )
    eff_start_field = schema.field("effective_start")
    assert str(eff_start_field.type) == "date32[day]", (
        "ms_drg_weights.parquet `effective_start` must be date32[day]; "
        f"got {eff_start_field.type}"
    )
    rw_field = schema.field("relative_weight")
    assert str(rw_field.type) == "double", (
        "ms_drg_weights.parquet `relative_weight` must be float64 (double); "
        f"got {rw_field.type}"
    )


def test_ipps_std_amounts_locked_schema() -> None:
    """``ipps_std_amounts.parquet`` has exactly the locked 11-column schema."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    pf = pq.ParquetFile(_STD_AMOUNTS_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)

    missing = _EXPECTED_COLUMNS_STD_AMOUNTS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_STD_AMOUNTS
    assert not missing, (
        f"ipps_std_amounts.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"ipps_std_amounts.parquet has unexpected columns: {sorted(extra)}"
    )

    fy_field = schema.field("fy")
    assert str(fy_field.type) == "int32", (
        f"ipps_std_amounts.parquet `fy` must be int32; got {fy_field.type}"
    )
    eff_start_field = schema.field("effective_start")
    assert str(eff_start_field.type) == "date32[day]", (
        "ipps_std_amounts.parquet `effective_start` must be date32[day]; "
        f"got {eff_start_field.type}"
    )


# --------------------------------------------------------------------------
# Compound-key uniqueness
# --------------------------------------------------------------------------


def test_ms_drg_weights_compound_key_unique() -> None:
    """``(ms_drg, fy, version)`` is unique in the unioned weights parquet."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT ms_drg, fy, version, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_WEIGHTS_PARQUET)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (ms_drg, fy, version) compound keys found in "
        f"ms_drg_weights.parquet (sample): {dups}"
    )


def test_ipps_std_amounts_compound_key_unique() -> None:
    """``(fy, version, table, line_item, category, rate_period)`` is unique.

    DuckDB's GROUP BY treats NULL=NULL natively, so nullable category /
    rate_period columns participate in grouping without a COALESCE shim.
    """
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        dups = con.execute(
            'SELECT fy, version, "table", line_item, category, rate_period, '
            "       COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4,5,6 HAVING c>1 LIMIT 5",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (fy, version, table, line_item, category, rate_period) "
        f"compound keys found in ipps_std_amounts.parquet (sample): {dups}"
    )


# --------------------------------------------------------------------------
# Parity (unified row count == sum of per-source rows)
# --------------------------------------------------------------------------


def test_ms_drg_weights_parity() -> None:
    """Unified weights row count == sum of per-source ``ipps_table5_*`` rows."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [_PER_SOURCE_TABLE5_GLOB]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {_PER_SOURCE_TABLE5_GLOB}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_PER_SOURCE_TABLE5_GLOB],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_WEIGHTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: ms_drg_weights has {master_n} rows, "
        f"per_source/ipps/ipps_table5_*.parquet sums to {per_source_n}."
    )


def test_ipps_std_amounts_parity() -> None:
    """Unified std_amounts row count == sum of per-source ``ipps_table1{a..e}_*`` rows."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        per_source_files: list[str] = []
        for glob in _PER_SOURCE_TABLE1_GLOBS:
            per_source_files.extend(
                row[0]
                for row in con.execute(
                    "SELECT file FROM glob(?)", [glob]
                ).fetchall()
            )
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets matching {_PER_SOURCE_TABLE1_GLOBS}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_PER_SOURCE_TABLE1_GLOBS],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: ipps_std_amounts has {master_n} rows, "
        f"per_source/ipps/ipps_table1{{a..e}}_*.parquet sums to "
        f"{per_source_n}."
    )


# --------------------------------------------------------------------------
# Domain-vocabulary cardinality
# --------------------------------------------------------------------------


def test_ms_drg_weights_versions_in_range() -> None:
    """Distinct ``version`` values are a subset of V25.0 through V43.0."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        versions = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT version FROM read_parquet(?) "
                "WHERE version IS NOT NULL",
                [str(_WEIGHTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = versions - _EXPECTED_VERSIONS
    assert not unexpected, (
        f"ms_drg_weights: version has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_VERSIONS)}"
    )


def test_ipps_std_amounts_versions_in_range() -> None:
    """Distinct ``version`` values are a subset of V25.0 through V43.0."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        versions = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT version FROM read_parquet(?) "
                "WHERE version IS NOT NULL",
                [str(_STD_AMOUNTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = versions - _EXPECTED_VERSIONS
    assert not unexpected, (
        f"ipps_std_amounts: version has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_VERSIONS)}"
    )


def test_ipps_std_amounts_tables_in_range() -> None:
    """Distinct ``table`` values are a subset of {1A, 1B, 1C, 1D, 1E}.

    Subset (not equality) is critical: FY2008/FY2009 ship 1A-1D only — 1E
    didn't exist pre-PPACA. Partial coverage should not fail this assertion.
    """
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        tables = {
            row[0]
            for row in con.execute(
                'SELECT DISTINCT "table" FROM read_parquet(?) '
                'WHERE "table" IS NOT NULL',
                [str(_STD_AMOUNTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = tables - _EXPECTED_TABLES
    assert not unexpected, (
        f"ipps_std_amounts: table has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_TABLES)}"
    )


def test_ipps_std_amounts_units_in_range() -> None:
    """Distinct non-null ``unit`` values are a subset of the locked vocabulary."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        units = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT unit FROM read_parquet(?) "
                "WHERE unit IS NOT NULL",
                [str(_STD_AMOUNTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = units - _EXPECTED_UNITS
    assert not unexpected, (
        f"ipps_std_amounts: unit has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_UNITS)}"
    )


def test_ipps_std_amounts_categories_in_range() -> None:
    """Distinct non-null ``category`` values are a subset of the locked vocabulary."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        categories = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT category FROM read_parquet(?) "
                "WHERE category IS NOT NULL",
                [str(_STD_AMOUNTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = categories - _EXPECTED_CATEGORIES
    assert not unexpected, (
        f"ipps_std_amounts: category has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_CATEGORIES)}"
    )


def test_ipps_std_amounts_rate_periods_in_range() -> None:
    """Distinct non-null ``rate_period`` values are a subset of {1st_half, 2nd_half}."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        periods = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT rate_period FROM read_parquet(?) "
                "WHERE rate_period IS NOT NULL",
                [str(_STD_AMOUNTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = periods - _EXPECTED_RATE_PERIODS
    assert not unexpected, (
        f"ipps_std_amounts: rate_period has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_RATE_PERIODS)}"
    )


def test_ms_drg_weights_drg_types_in_range() -> None:
    """Distinct non-null ``drg_type`` values are a subset of {MED, SURG}."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        drg_types = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT drg_type FROM read_parquet(?) "
                "WHERE drg_type IS NOT NULL",
                [str(_WEIGHTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = drg_types - _EXPECTED_DRG_TYPES
    assert not unexpected, (
        f"ms_drg_weights: drg_type has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_DRG_TYPES)}"
    )


def test_ms_drg_weights_severity_in_range() -> None:
    """Distinct non-null ``severity`` values are a subset of {MCC, CC, NoCC}."""
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        severities = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT severity FROM read_parquet(?) "
                "WHERE severity IS NOT NULL",
                [str(_WEIGHTS_PARQUET)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = severities - _EXPECTED_SEVERITY
    assert not unexpected, (
        f"ms_drg_weights: severity has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(_EXPECTED_SEVERITY)}"
    )


# --------------------------------------------------------------------------
# Required-field non-nullness
# --------------------------------------------------------------------------


def test_ms_drg_weights_no_null_required_fields() -> None:
    """Identity columns (ms_drg, fy, version) have zero nulls.

    Data columns like ``relative_weight``, ``gmlos``, ``amlos`` are allowed
    to be null — CMS publishes some DRGs with no weight (reserved 998/999
    in many FYs ship RW=0 with no GMLOS; some FY+DRG combinations also
    legitimately omit LOS metrics). Phase 7 cost-compute treats null RW as
    "cost cannot be computed for this DRG" rather than as a data integrity
    failure.
    """
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE ms_drg IS NULL "
            "   OR fy IS NULL "
            "   OR version IS NULL",
            [str(_WEIGHTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()
    assert n == 0, (
        f"ms_drg_weights: {n} rows have NULLs in identity fields "
        f"(ms_drg, fy, version)."
    )


def test_ipps_std_amounts_no_null_required_fields() -> None:
    """Required (non-nullable) columns have zero nulls in ipps_std_amounts."""
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE fy IS NULL "
            "   OR version IS NULL "
            '   OR "table" IS NULL '
            "   OR line_item IS NULL "
            "   OR value IS NULL",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()
    assert n == 0, (
        f"ipps_std_amounts: {n} rows have NULLs in required fields "
        f"(fy, version, table, line_item, value)."
    )


# --------------------------------------------------------------------------
# Cross-FY historical-quirk assertions (data-conditional)
# --------------------------------------------------------------------------


def test_fy2010_dual_rate_period_present_when_data_exists() -> None:
    """FY2010 V27.0 published two mid-year rate periods (1st_half + 2nd_half).

    Skips cleanly when FY2010 hasn't been ingested yet.
    """
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        fy2010_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE fy = 2010",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
        if fy2010_n == 0:
            pytest.skip(
                "FY2010 not yet ingested into ipps_std_amounts; "
                "skipping dual rate-period assertion."
            )
        n_periods = con.execute(
            "SELECT COUNT(DISTINCT rate_period) FROM read_parquet(?) "
            "WHERE fy = 2010 AND rate_period IS NOT NULL",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_periods == 2, (
        f"ipps_std_amounts: FY2010 must have both 1st_half and 2nd_half "
        f"rate_period populated (PPACA mid-year rate change); "
        f"found {n_periods} distinct non-null rate_period values."
    )


def test_fy2008_2009_table_1e_absent_when_data_exists() -> None:
    """FY2008/FY2009 predate PPACA — Table 1E should not exist for those FYs.

    Skips cleanly when neither FY has been ingested.
    """
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        coverage_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE fy IN (2008, 2009)",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
        if coverage_n == 0:
            pytest.skip(
                "Neither FY2008 nor FY2009 ingested into ipps_std_amounts; "
                "skipping Table 1E absence assertion."
            )
        n_1e = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE fy IN (2008, 2009) AND \"table\" = '1E'",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_1e == 0, (
        f"ipps_std_amounts: FY2008/FY2009 must not have Table 1E rows "
        f"(PPACA pre-history); found {n_1e}."
    )


def test_fy2015_table_1e_present_after_fix() -> None:
    """FY2015 V32.0 Table 1E LTCH standard rate is present (regression test).

    The FY2015 ZIP ``fy2015-fr-table-1a-1e.zip`` ships a combined FR+CN
    bundled XLSX with TWO sheets (``Table 1A-1E FR`` and ``Table 1A-1D CN``)
    where CMS leaves the workbook's "active" sheet set to the CN variant.
    The CN sheet only carries Tables 1A-1D, so a naive ``wb.active`` read
    misses Table 1E entirely. ``_pick_fr_sheet`` in
    ``src/extract/ipps.py`` resolves the FR sheet by name; this test
    asserts the resulting silver row exists.

    Skips cleanly when FY2015 hasn't been ingested.
    """
    _skip_if_missing(_STD_AMOUNTS_PARQUET, "ipps_std_amounts.parquet")

    con = duckdb.connect()
    try:
        fy2015_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE fy = 2015",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
        if fy2015_n == 0:
            pytest.skip(
                "FY2015 not yet ingested into ipps_std_amounts; "
                "skipping Table 1E presence assertion."
            )
        n_1e_ltch = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE fy = 2015 AND \"table\" = '1E' "
            "  AND line_item = 'ltch_standard_rate'",
            [str(_STD_AMOUNTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_1e_ltch == 1, (
        f"ipps_std_amounts: FY2015 must have exactly 1 row for "
        f"(table='1E', line_item='ltch_standard_rate'); found {n_1e_ltch}. "
        f"If 0, the FR/CN multi-sheet picker in "
        f"src/extract/ipps.py:_pick_fr_sheet may have regressed."
    )


# Hand-curated FR-sheet ground-truth ``relative_weight`` triples for the six
# multi-sheet FY workbooks. Sourced 2026-04-30 by direct openpyxl read of the
# bronze XLSX FR sheet (the canonical Final Rule weights — NOT the CN-revised
# Correction Notice values that ``wb.active`` defaults to). Picked
# representative DRGs per FY: PRE-MDC #001 (heart transplant w MCC), #003
# (ECMO), surgical workhorses #460/#470 (joint replacement), and medical
# #871 (sepsis w MCC). Where FR and CN coincide for a given FY/DRG (FY2015,
# FY2016, FY2020 mostly), the test still defends the FR-selection contract
# — a regression that selects CN would still pass for those triples but
# fail on the FY2018 / FY2021 / FY2025 entries where the values diverge.
#
# Format: (fy, ms_drg, expected_fr_relative_weight).
_TABLE5_FR_GROUND_TRUTH: tuple[tuple[int, str, float], ...] = (
    # FY2015 V32.0: FR == CN for these DRGs (correction left weights alone).
    (2015, "001", 25.392),
    (2015, "003", 17.6399),
    (2015, "470", 2.1137),
    # FY2016 V33.0: FR == CN for these DRGs.
    (2016, "001", 26.2466),
    (2016, "470", 2.0816),
    # FY2018 V35.0: FR and CN DIVERGE — these defend the picker explicitly.
    # CN-revised values would be 25.4484 / 17.6245 / 4.0149 / 2.0543.
    (2018, "001", 25.4241),
    (2018, "003", 17.6331),
    (2018, "460", 4.0153),
    (2018, "470", 2.0544),
    # FY2020 V37.0: FR == CN for these DRGs.
    (2020, "001", 27.6339),
    (2020, "470", 1.9684),
    # FY2021 V38.0: FR and CN DIVERGE at the ten-thousandths place.
    # CN-revised values would be 28.9651 / 18.9911.
    (2021, "001", 28.9652),
    (2021, "003", 18.9912),
    # FY2025 V42.0: FR and CN DIVERGE — these defend the picker explicitly.
    # CN-revised values would be 28.1683 / 9.4045 / 21.4316 / 1.9621.
    (2025, "001", 28.1664),
    (2025, "002", 9.4038),
    (2025, "003", 21.4302),
    (2025, "871", 1.9623),
)


def test_table5_fr_sheet_selected_for_multi_sheet_fys() -> None:
    """Table 5 ``relative_weight`` matches FR-sheet ground truth for the six
    multi-sheet workbook FYs (FY2015/16/18/20/21/25).

    Six FYs ship Table 5 as a 2-sheet workbook: ``Table 5 FR`` (canonical
    Final Rule) + ``Table 5 CN`` (Correction Notice with cents-level revised
    weights). CMS leaves ``wb.active`` set to the CN sheet, so a naive
    openpyxl read silently emits CN-revised weights. The picker
    ``_pick_fr_sheet`` in ``src/extract/ipps.py`` now selects the FR sheet by
    name; this test asserts the resulting persisted ``relative_weight``
    values match the FR ground truth for hand-curated ``(fy, ms_drg)``
    pairs.

    A regression of the picker (i.e., CN sheet picked instead) would
    manifest as FY2018 / FY2021 / FY2025 assertion failures where FR and CN
    diverge; the FY2015 / FY2016 / FY2020 entries still defend the
    selection contract by asserting on triples where FR == CN.

    Skips cleanly when ``ms_drg_weights.parquet`` hasn't been built or
    when none of the six affected FYs are ingested.
    """
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        affected_fys = sorted({fy for fy, _, _ in _TABLE5_FR_GROUND_TRUTH})
        ingested_fys = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT fy FROM read_parquet(?) WHERE fy IN ("
                + ",".join(str(fy) for fy in affected_fys)
                + ")",
                [str(_WEIGHTS_PARQUET)],
            ).fetchall()
        }
        if not ingested_fys:
            pytest.skip(
                "None of the six multi-sheet FYs (2015/16/18/20/21/25) are "
                "ingested into ms_drg_weights.parquet; skipping FR-sheet "
                "ground-truth assertion."
            )

        observed: dict[tuple[int, str], float | None] = {}
        for fy, ms_drg, _ in _TABLE5_FR_GROUND_TRUTH:
            if fy not in ingested_fys:
                continue
            row = con.execute(
                "SELECT relative_weight FROM read_parquet(?) "
                "WHERE fy = ? AND ms_drg = ?",
                [str(_WEIGHTS_PARQUET), fy, ms_drg],
            ).fetchone()
            observed[(fy, ms_drg)] = row[0] if row is not None else None
    finally:
        con.close()

    mismatches: list[str] = []
    for fy, ms_drg, expected in _TABLE5_FR_GROUND_TRUTH:
        if fy not in ingested_fys:
            continue
        got = observed.get((fy, ms_drg))
        if got is None:
            mismatches.append(
                f"FY{fy} DRG {ms_drg}: expected FR weight {expected}, "
                f"row missing from ms_drg_weights.parquet"
            )
            continue
        # Cents-level tolerance: FR vs CN diverge at the 4th-5th decimal.
        # 1e-6 absolute tolerance is tight enough to catch any CN-bleed.
        if abs(got - expected) > 1e-6:
            mismatches.append(
                f"FY{fy} DRG {ms_drg}: expected FR weight {expected}, "
                f"got {got} (delta={got - expected:+.6f}). "
                f"If this looks like the CN-revised value, "
                f"src/extract/ipps.py:_pick_fr_sheet may have regressed "
                f"on the Table 5 read path."
            )

    assert not mismatches, (
        "Table 5 FR-sheet picker regressed for one or more multi-sheet "
        "FYs:\n" + "\n".join(mismatches)
    )


def test_fy2008_reserved_drgs_zero_weight() -> None:
    """FY2008 reserved DRGs 998/999 carry ``relative_weight = 0.0``.

    FLAG 5 (Session 9.5): the raw FY2008 IPPS Table 5 ships these two
    reserved DRGs with the sentinel value ``42.0`` rather than the
    FY2009+ canonical ``0.0``. ``src.transform.ipps_tables`` overrides
    the value to ``0.0`` at the projection layer so downstream cost-compute
    sees a uniform "zero-weight reserved DRG" convention across all FYs.
    Mutating the bronze/per_source parquet is intentionally avoided —
    bronze is write-once.
    """
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT ms_drg, relative_weight FROM read_parquet(?) "
            "WHERE fy = 2008 AND ms_drg IN ('998', '999') "
            "ORDER BY ms_drg",
            [str(_WEIGHTS_PARQUET)],
        ).fetchall()
    finally:
        con.close()

    if not rows:
        pytest.skip(
            "FY2008 not yet ingested into ms_drg_weights.parquet; "
            "skipping FLAG 5 reserved-DRG zero-weight assertion."
        )
    assert rows == [("998", 0.0), ("999", 0.0)], (
        f"FY2008 reserved DRGs 998/999 must ship relative_weight=0.0 "
        f"(FLAG 5 / Session 9.5 override of raw 42.0 sentinel); got {rows}."
    )


def test_ms_drg_weights_relative_weight_nonnegative() -> None:
    """``relative_weight`` is non-negative for all DRGs.

    Reserved DRGs 998 ("Principal diagnosis invalid as discharge diagnosis")
    and 999 ("Ungroupable") legitimately ship with ``relative_weight = 0.0``
    in some FYs (CMS publishes them as zero-weight to flag groupingfailures
    rather than omitting them). Excluding the reserved pair, all real DRGs
    must have ``relative_weight > 0``.
    """
    _skip_if_missing(_WEIGHTS_PARQUET, "ms_drg_weights.parquet")

    con = duckdb.connect()
    try:
        min_rw = con.execute(
            "SELECT MIN(relative_weight) FROM read_parquet(?) "
            "WHERE relative_weight IS NOT NULL",
            [str(_WEIGHTS_PARQUET)],
        ).fetchone()[0]
        # Exclude reserved 998 / 999 from the strict-positivity check.
        min_rw_nonreserved = con.execute(
            "SELECT MIN(relative_weight) FROM read_parquet(?) "
            "WHERE relative_weight IS NOT NULL "
            "  AND ms_drg NOT IN ('998', '999')",
            [str(_WEIGHTS_PARQUET)],
        ).fetchone()[0]
    finally:
        con.close()

    if min_rw is None:
        pytest.skip(
            "ms_drg_weights has no non-null relative_weight values; "
            "cannot validate positivity."
        )
    assert min_rw >= 0, (
        f"ms_drg_weights: relative_weight must be non-negative; "
        f"observed minimum {min_rw}."
    )
    if min_rw_nonreserved is not None:
        assert min_rw_nonreserved > 0, (
            f"ms_drg_weights: relative_weight must be strictly positive "
            f"for non-reserved DRGs (excluding 998/999); observed minimum "
            f"{min_rw_nonreserved}."
        )
