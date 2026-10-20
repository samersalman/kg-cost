"""Phase 2 (Session 4b) tests for the ASC extractor.

Mirrors ``tests/test_opps.py`` shape:

  - Module-level ``pytest.importorskip`` for ``src.extract.asc`` so test
    collection doesn't ImportError while the extractor is being authored.
  - All data-dependent assertions skip via ``pytest.skip`` when the per-(cy, q,
    addendum) parquet doesn't exist (so the suite passes both pre- and
    post-extract).

Test target: CY2024 quarter A (Q1) — modern era, easy to spot-check against
published CMS ASC Addendum AA / BB / FF / DD1 / DD2 / EE values.
"""

from __future__ import annotations

import re

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Use importorskip at module level so test collection is robust to the ASC
# extractor not yet being on disk (Batch 3a is concurrent with this test file).
# Once `src/extract/asc.py` lands the actual API surface check moves into
# `test_extract_module_importable` below.
pytest.importorskip("src.extract.asc")

_CY = 2024
_QUARTER = "A"
_PARQUET_AA = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_aa_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_BB = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_bb_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_FF = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_ff_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_DD1 = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_dd1_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_DD2 = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_dd2_cy{_CY}_{_QUARTER}.parquet"
)
_PARQUET_EE = (
    config.SILVER_PER_SOURCE / "asc" / f"asc_ee_cy{_CY}_{_QUARTER}.parquet"
)
_CY_START, _CY_END = config.asc_quarter_interval(_CY, _QUARTER)

# Locked ASC AA/BB/FF schema: 12 columns.
_EXPECTED_COLUMNS_RATES: frozenset[str] = frozenset(
    {
        "code",
        "modifier",
        "code_system",
        "cy",
        "quarter",
        "addendum",
        "payment_indicator",
        "payment_rate",
        "short_descriptor",
        "effective_start",
        "effective_end",
        "source",
    }
)

# Locked ASC DD1 schema: 5 columns (payment indicator definitions).
_EXPECTED_COLUMNS_DD1: frozenset[str] = frozenset(
    {
        "payment_indicator",
        "description",
        "cy",
        "quarter",
        "source",
    }
)

# Locked ASC DD2 schema: 5 columns (status indicator definitions).
_EXPECTED_COLUMNS_DD2: frozenset[str] = frozenset(
    {
        "status_indicator",
        "description",
        "cy",
        "quarter",
        "source",
    }
)

# Locked ASC EE schema: 5 columns (excluded procedures).
_EXPECTED_COLUMNS_EE: frozenset[str] = frozenset(
    {
        "code",
        "cy",
        "quarter",
        "short_descriptor",
        "source",
    }
)

# CPT 5-digit numeric or HCPCS-II 5-char [A-V0-9][0-9]{4}.
_CPT_RE = re.compile(r"^[0-9]{5}$")
_HCPCS_RE = re.compile(r"^[A-V0-9][0-9]{4}$")


def _skip_if_missing(parquet_path, addendum_label: str) -> None:
    if not parquet_path.exists():
        pytest.skip(
            f"{parquet_path} not present; run "
            f"`python -m src.extract.asc --cy {_CY} --quarter {_QUARTER}` "
            f"first (addendum {addendum_label})"
        )


def test_extract_module_importable() -> None:
    """``src.extract.asc`` exports the documented public API surface."""
    from src.extract import asc

    assert hasattr(asc, "extract_cy_quarter"), (
        "src.extract.asc must export `extract_cy_quarter`"
    )
    assert hasattr(asc, "extract_cy_quarter_range"), (
        "src.extract.asc must export `extract_cy_quarter_range`"
    )


# --------------------------------------------------------------------------
# Addendum AA (HCPCS/CPT-level surgical procedures) tests
# --------------------------------------------------------------------------


def test_cy2024_a_aa_parquet_schema() -> None:
    """The CY2024 Q1 ASC Addendum AA parquet has the locked 12-column schema."""
    _skip_if_missing(_PARQUET_AA, "AA")

    pf = pq.ParquetFile(_PARQUET_AA)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_RATES - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_RATES
    assert not missing, (
        f"Missing columns in {_PARQUET_AA.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_AA.name}: {sorted(extra)}"
    )

    # Type sanity for load-bearing columns.
    string_cols = ("code", "code_system", "quarter", "addendum", "source")
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

    float_cols = ("payment_rate",)
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


def test_cy2024_a_aa_row_count_in_range() -> None:
    """ASC Addendum AA contains ~3,000-8,000 codes per quarter."""
    _skip_if_missing(_PARQUET_AA, "AA")

    n = pq.ParquetFile(_PARQUET_AA).metadata.num_rows
    assert 1_000 <= n <= 15_000, (
        f"Unexpected ASC Addendum AA CY{_CY} Q{_QUARTER} row count: {n} "
        f"(expected ~3,000-8,000; 1,000-15,000 tolerated)"
    )


def test_cy2024_a_aa_codes_conformant() -> None:
    """Every ``code`` is CPT 5-digit numeric or HCPCS-II 5-char."""
    _skip_if_missing(_PARQUET_AA, "AA")

    table = pq.read_table(_PARQUET_AA, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [
        c
        for c in codes
        if c is None or not (_CPT_RE.match(c) or _HCPCS_RE.match(c))
    ]
    assert not bad, (
        f"Found {len(bad)} non-conforming ASC Addendum AA codes "
        f"(must be CPT [0-9]{{5}} or HCPCS-II [A-V0-9][0-9]{{4}}; "
        f"sample: {bad[:5]})"
    )


def test_cy2024_a_aa_addendum_value() -> None:
    """Every row in the AA parquet has ``addendum = 'AA'``."""
    _skip_if_missing(_PARQUET_AA, "AA")

    table = pq.read_table(_PARQUET_AA, columns=["addendum"])
    addenda = set(table.column("addendum").to_pylist())
    assert addenda == {"AA"}, (
        f"asc_aa_*.parquet should have addendum=={{'AA'}}, got {addenda}"
    )


def test_cy2024_a_aa_effective_dates() -> None:
    """All rows span the CY2024 Q1 interval (2024-01-01 .. 2024-03-31)."""
    _skip_if_missing(_PARQUET_AA, "AA")

    table = pq.read_table(
        _PARQUET_AA, columns=["effective_start", "effective_end"]
    )
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_CY_START}, (
        f"effective_start should be {{{_CY_START}}}, got {starts}"
    )
    assert ends == {_CY_END}, (
        f"effective_end should be {{{_CY_END}}}, got {ends}"
    )


def test_cy2024_a_aa_compound_key_unique() -> None:
    """``(code, modifier, cy, quarter, addendum)`` is unique within Addendum AA.

    DuckDB GROUP BY treats NULL=NULL natively, so no COALESCE shim is needed
    for the (often-null) modifier column.
    """
    _skip_if_missing(_PARQUET_AA, "AA")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, modifier, cy, quarter, addendum, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4,5 HAVING c>1 LIMIT 5",
            [str(_PARQUET_AA)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, modifier, cy, quarter, addendum) compound keys "
        f"found in {_PARQUET_AA.name} (sample): {dups}"
    )


# --------------------------------------------------------------------------
# Addendum DD1 (payment indicator definitions) tests
# --------------------------------------------------------------------------


def test_cy2024_a_dd1_parquet_schema() -> None:
    """The CY2024 Q1 ASC Addendum DD1 parquet has the locked 5-column schema."""
    _skip_if_missing(_PARQUET_DD1, "DD1")

    pf = pq.ParquetFile(_PARQUET_DD1)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_DD1 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_DD1
    assert not missing, (
        f"Missing columns in {_PARQUET_DD1.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_DD1.name}: {sorted(extra)}"
    )

    string_cols = ("payment_indicator", "description", "quarter", "source")
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    t = str(schema.field("cy").type).lower()
    assert "int" in t, f"`cy` must be int, got {schema.field('cy').type}"


def test_cy2024_a_dd1_row_count_in_range() -> None:
    """ASC Addendum DD1 is a small reference table (~5-50 rows)."""
    _skip_if_missing(_PARQUET_DD1, "DD1")

    n = pq.ParquetFile(_PARQUET_DD1).metadata.num_rows
    assert 1 <= n <= 100, (
        f"Unexpected ASC Addendum DD1 CY{_CY} Q{_QUARTER} row count: {n} "
        f"(expected ~5-50; 1-100 tolerated)"
    )


# --------------------------------------------------------------------------
# Addendum DD2 (status indicator definitions) tests
# --------------------------------------------------------------------------


def test_cy2024_a_dd2_parquet_schema() -> None:
    """The CY2024 Q1 ASC Addendum DD2 parquet has the locked 5-column schema."""
    _skip_if_missing(_PARQUET_DD2, "DD2")

    pf = pq.ParquetFile(_PARQUET_DD2)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_DD2 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_DD2
    assert not missing, (
        f"Missing columns in {_PARQUET_DD2.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_DD2.name}: {sorted(extra)}"
    )

    string_cols = ("status_indicator", "description", "quarter", "source")
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    t = str(schema.field("cy").type).lower()
    assert "int" in t, f"`cy` must be int, got {schema.field('cy').type}"


# --------------------------------------------------------------------------
# Addendum EE (excluded procedures) tests
# --------------------------------------------------------------------------


def test_cy2024_a_ee_parquet_schema() -> None:
    """The CY2024 Q1 ASC Addendum EE parquet has the locked 5-column schema."""
    _skip_if_missing(_PARQUET_EE, "EE")

    pf = pq.ParquetFile(_PARQUET_EE)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS_EE - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_EE
    assert not missing, (
        f"Missing columns in {_PARQUET_EE.name}: {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected columns in {_PARQUET_EE.name}: {sorted(extra)}"
    )

    string_cols = ("code", "short_descriptor", "quarter", "source")
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, (
            f"`{col}` must be a string type, got {schema.field(col).type}"
        )

    t = str(schema.field("cy").type).lower()
    assert "int" in t, f"`cy` must be int, got {schema.field('cy').type}"


def test_cy2024_a_ee_codes_conformant() -> None:
    """Every ``code`` in the EE parquet is CPT 5-digit or HCPCS-II 5-char."""
    _skip_if_missing(_PARQUET_EE, "EE")

    table = pq.read_table(_PARQUET_EE, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [
        c
        for c in codes
        if c is None or not (_CPT_RE.match(c) or _HCPCS_RE.match(c))
    ]
    assert not bad, (
        f"Found {len(bad)} non-conforming ASC Addendum EE codes "
        f"(must be CPT [0-9]{{5}} or HCPCS-II [A-V0-9][0-9]{{4}}; "
        f"sample: {bad[:5]})"
    )
