"""Phase 2 (Session 4a) tests for the OPPS extractor.

Mirrors ``tests/test_pfs_rvu.py`` shape:

  - Module-level ``pytest.importorskip`` for ``src.extract.opps`` so test
    collection doesn't ImportError while the extractor is being authored.
  - All data-dependent assertions skip via ``pytest.skip`` when the per-(cy, q,
    addendum) parquet doesn't exist (so the suite passes both pre- and
    post-extract).

Test target: CY2024 quarter D — mid-range, modern era, easy to spot-check
against published CMS OPPS Addendum A / Addendum B values.
"""

from __future__ import annotations

import re

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Use importorskip at module level so test collection is robust to the OPPS
# extractor not yet being on disk (Batch 2a is concurrent with this test file).
# Once `src/extract/opps.py` lands the actual API surface check moves into
# `test_extract_module_importable` below.
pytest.importorskip("src.extract.opps")

_CY = 2024
_QUARTER = "D"
_PARQUET_A = (
    config.SILVER_PER_SOURCE / "opps" / f"opps_a_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_B = (
    config.SILVER_PER_SOURCE / "opps" / f"opps_b_cy{_CY}_{_QUARTER}.parquet"
)
_CY_START, _CY_END = config.opps_quarter_interval(_CY, _QUARTER)

# Locked OPPS Addendum B schema (plan-v5.md lines 118-133): 13 columns.
_EXPECTED_COLUMNS_B: frozenset[str] = frozenset(
    {
        "code",
        "modifier",
        "code_system",
        "cy",
        "quarter",
        "status_indicator",
        "apc",
        "relative_weight",
        "payment_rate",
        "short_descriptor",
        "effective_start",
        "effective_end",
        "source",
    }
)

# Locked OPPS Addendum A schema (plan-v5.md lines 102-116): 12 columns.
_EXPECTED_COLUMNS_A: frozenset[str] = frozenset(
    {
        "apc",
        "cy",
        "quarter",
        "status_indicator",
        "apc_title",
        "relative_weight",
        "payment_rate",
        "national_unadjusted_copayment",
        "minimum_unadjusted_copayment",
        "effective_start",
        "effective_end",
        "source",
    }
)

# CPT 5-digit numeric or HCPCS-II 5-char [A-V0-9][0-9]{4}.
_CPT_RE = re.compile(r"^[0-9]{5}$")
_HCPCS_RE = re.compile(r"^[A-V0-9][0-9]{4}$")
_APC_RE = re.compile(r"^[0-9]{4,5}$")


def _skip_if_missing_b() -> None:
    if not _PARQUET_B.exists():
        pytest.skip(
            f"{_PARQUET_B} not present; run "
            f"`python -m src.extract.opps --cy {_CY} --quarter {_QUARTER} "
            f"--addendum B` first"
        )


def _skip_if_missing_a() -> None:
    if not _PARQUET_A.exists():
        pytest.skip(
            f"{_PARQUET_A} not present; run "
            f"`python -m src.extract.opps --cy {_CY} --quarter {_QUARTER} "
            f"--addendum A` first"
        )


def test_extract_module_importable() -> None:
    """``src.extract.opps`` exports the documented public API surface.

    Once Batch 2a lands ``src/extract/opps.py`` this asserts the two CLI
    entry-point functions referenced in plan-v5.md (extract_cy_quarter and
    extract_cy_quarter_addendum_range) are present.
    """
    from src.extract import opps

    assert hasattr(opps, "extract_cy_quarter"), (
        "src.extract.opps must export `extract_cy_quarter`"
    )
    assert hasattr(opps, "extract_cy_quarter_addendum_range"), (
        "src.extract.opps must export `extract_cy_quarter_addendum_range`"
    )


# --------------------------------------------------------------------------
# Addendum B (HCPCS/CPT level) tests
# --------------------------------------------------------------------------


def test_cy2024_d_addendum_b_parquet_schema() -> None:
    """The CY2024 Q4 OPPS Addendum B parquet has the locked 13-column schema."""
    _skip_if_missing_b()

    pf = pq.ParquetFile(_PARQUET_B)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_B - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_B
    assert not missing, (
        f"Missing columns in {_PARQUET_B.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_B.name}: {sorted(extra)}"
    )

    # Type sanity for load-bearing columns.
    string_cols = ("code", "code_system", "quarter", "source")
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    int_cols = ("cy",)
    for col in int_cols:
        t = str(schema.field(col).type).lower()
        assert "int" in t, (
            f"`{col}` must be an integer type, got {schema.field(col).type}"
        )

    float_cols = ("relative_weight", "payment_rate")
    for col in float_cols:
        t = str(schema.field(col).type).lower()
        assert "float" in t or "double" in t or "decimal" in t, (
            f"`{col}` must be a float/double type, got {schema.field(col).type}"
        )

    date_cols = ("effective_start", "effective_end")
    for col in date_cols:
        t = str(schema.field(col).type).lower()
        assert "date" in t, (
            f"`{col}` must be a date type, got {schema.field(col).type}"
        )


def test_cy2024_d_addendum_b_row_count_in_range() -> None:
    """OPPS Addendum B contains ~5,000-10,000 codes per quarter."""
    _skip_if_missing_b()

    n = pq.ParquetFile(_PARQUET_B).metadata.num_rows
    assert 1_000 <= n <= 25_000, (
        f"Unexpected OPPS Addendum B CY{_CY} Q{_QUARTER} row count: {n} "
        f"(expected ~5,000-10,000; 1,000-25,000 tolerated)"
    )


def test_cy2024_d_addendum_b_codes_conformant() -> None:
    """Every ``code`` is CPT 5-digit numeric or HCPCS-II 5-char."""
    _skip_if_missing_b()

    table = pq.read_table(_PARQUET_B, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [
        c
        for c in codes
        if c is None or not (_CPT_RE.match(c) or _HCPCS_RE.match(c))
    ]
    assert not bad, (
        f"Found {len(bad)} non-conforming OPPS Addendum B codes "
        f"(must be CPT [0-9]{{5}} or HCPCS-II [A-V0-9][0-9]{{4}}; "
        f"sample: {bad[:5]})"
    )


def test_cy2024_d_addendum_b_effective_dates() -> None:
    """All rows span the CY2024 Q4 interval (2024-10-01 .. 2024-12-31)."""
    _skip_if_missing_b()

    table = pq.read_table(
        _PARQUET_B, columns=["effective_start", "effective_end"]
    )
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_CY_START}, (
        f"effective_start should be {{{_CY_START}}}, got {starts}"
    )
    assert ends == {_CY_END}, (
        f"effective_end should be {{{_CY_END}}}, got {ends}"
    )


def test_cy2024_d_addendum_b_no_dropped_status() -> None:
    """``E1`` and ``E2`` (non-payable items) are dropped by the extractor.

    Other status indicators (J1, J2, V, S, T, Q1-Q4, etc.) and possibly NULL
    are allowed.
    """
    _skip_if_missing_b()

    table = pq.read_table(_PARQUET_B, columns=["status_indicator"])
    statuses = set(table.column("status_indicator").to_pylist())
    forbidden = {"E1", "E2"} & statuses
    assert not forbidden, (
        f"OPPS extractor must drop status_indicator in {{E1, E2}}; "
        f"found {forbidden}"
    )


def test_cy2024_d_addendum_b_compound_key_unique() -> None:
    """``(code, modifier, cy, quarter)`` is unique within Addendum B.

    DuckDB GROUP BY treats NULL=NULL natively, so no COALESCE shim is needed
    for the (often-null) modifier column.
    """
    _skip_if_missing_b()

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, modifier, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4 HAVING c>1 LIMIT 5",
            [str(_PARQUET_B)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, modifier, cy, quarter) compound keys found "
        f"in {_PARQUET_B.name} (sample): {dups}"
    )


# --------------------------------------------------------------------------
# Addendum A (APC level) tests
# --------------------------------------------------------------------------


def test_cy2024_d_addendum_a_parquet_schema() -> None:
    """The CY2024 Q4 OPPS Addendum A parquet has the locked 12-column schema."""
    _skip_if_missing_a()

    pf = pq.ParquetFile(_PARQUET_A)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_A - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_A
    assert not missing, (
        f"Missing columns in {_PARQUET_A.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_A.name}: {sorted(extra)}"
    )

    # Type sanity for load-bearing columns.
    string_cols = ("apc", "quarter", "source")
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    int_cols = ("cy",)
    for col in int_cols:
        t = str(schema.field(col).type).lower()
        assert "int" in t, (
            f"`{col}` must be an integer type, got {schema.field(col).type}"
        )

    float_cols = (
        "relative_weight",
        "payment_rate",
        "national_unadjusted_copayment",
        "minimum_unadjusted_copayment",
    )
    for col in float_cols:
        t = str(schema.field(col).type).lower()
        assert "float" in t or "double" in t or "decimal" in t, (
            f"`{col}` must be a float/double type, got {schema.field(col).type}"
        )

    date_cols = ("effective_start", "effective_end")
    for col in date_cols:
        t = str(schema.field(col).type).lower()
        assert "date" in t, (
            f"`{col}` must be a date type, got {schema.field(col).type}"
        )


def test_cy2024_d_addendum_a_apc_conformant() -> None:
    """Every ``apc`` is 4 or 5 digits."""
    _skip_if_missing_a()

    table = pq.read_table(_PARQUET_A, columns=["apc"])
    apcs = table.column("apc").to_pylist()
    bad = [a for a in apcs if a is None or not _APC_RE.match(a)]
    assert not bad, (
        f"Found {len(bad)} non-conforming OPPS APC codes "
        f"(must match [0-9]{{4,5}}; sample: {bad[:5]})"
    )


def test_cy2024_d_addendum_a_compound_key_unique() -> None:
    """``(apc, cy, quarter)`` is unique within Addendum A."""
    _skip_if_missing_a()

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT apc, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_PARQUET_A)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (apc, cy, quarter) compound keys found "
        f"in {_PARQUET_A.name} (sample): {dups}"
    )
