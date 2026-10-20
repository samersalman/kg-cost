"""Phase 2 (Session 4b) tests for the unioned ASC silver parquets.

Four unified outputs:

  - ``data/silver/asc_rates.parquet`` — UNION of AA + BB + FF (12 cols).
  - ``data/silver/asc_payment_indicators.parquet`` — DD1 (5 cols).
  - ``data/silver/asc_status_indicators.parquet`` — DD2 (5 cols).
  - ``data/silver/asc_excluded_procedures.parquet`` — EE (5 cols).

Mirrors ``tests/test_opps_rates.py``: locked-schema enforcement, compound-key
uniqueness, parity vs per-source rows, cardinality assertions.

The transform module (``src.transform.asc_rates``) is loaded via
``pytest.importorskip`` at module level so test collection doesn't ImportError
while Batch 3b is concurrent. Once it lands, ``test_transform_module_importable``
asserts the documented public API surface.
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# importorskip at module level: Batch 3b is concurrent with this test file.
pytest.importorskip("src.transform.asc_rates")

_ASC_RATES = config.SILVER / "asc_rates.parquet"
_ASC_DD1 = config.SILVER / "asc_payment_indicators.parquet"
_ASC_DD2 = config.SILVER / "asc_status_indicators.parquet"
_ASC_EE = config.SILVER / "asc_excluded_procedures.parquet"

_ASC_RATES_GLOBS = [
    str(config.SILVER_PER_SOURCE / "asc" / "asc_aa_*.parquet"),
    str(config.SILVER_PER_SOURCE / "asc" / "asc_bb_*.parquet"),
    str(config.SILVER_PER_SOURCE / "asc" / "asc_ff_*.parquet"),
]
_ASC_DD1_GLOB = str(config.SILVER_PER_SOURCE / "asc" / "asc_dd1_*.parquet")
_ASC_DD2_GLOB = str(config.SILVER_PER_SOURCE / "asc" / "asc_dd2_*.parquet")
_ASC_EE_GLOB = str(config.SILVER_PER_SOURCE / "asc" / "asc_ee_*.parquet")

# Locked unified ASC rates schema: 12 columns.
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

# Locked DD1 schema: 5 columns.
_EXPECTED_COLUMNS_DD1: frozenset[str] = frozenset(
    {
        "payment_indicator",
        "description",
        "cy",
        "quarter",
        "source",
    }
)

# Locked DD2 schema: 5 columns.
_EXPECTED_COLUMNS_DD2: frozenset[str] = frozenset(
    {
        "status_indicator",
        "description",
        "cy",
        "quarter",
        "source",
    }
)

# Locked EE schema: 5 columns.
_EXPECTED_COLUMNS_EE: frozenset[str] = frozenset(
    {
        "code",
        "cy",
        "quarter",
        "short_descriptor",
        "source",
    }
)

_SKIP_HINT = (
    "Run `python -m src.extract.asc --cy-range 2008-2026 --quarter-range A-D` "
    "followed by `python -m src.transform.asc_rates` to produce the parquets."
)


def test_transform_module_importable() -> None:
    """``src.transform.asc_rates`` exports the documented public API surface."""
    from src.transform import asc_rates

    for fn in (
        "build_asc_rates",
        "build_asc_payment_indicators",
        "build_asc_status_indicators",
        "build_asc_excluded_procedures",
    ):
        assert hasattr(asc_rates, fn), (
            f"src.transform.asc_rates must export `{fn}`. "
            "TODO: if Batch 3b renamed it, update this test."
        )


# --------------------------------------------------------------------------
# Locked schemas
# --------------------------------------------------------------------------


def test_asc_rates_locked_schema() -> None:
    """The unioned ASC rates parquet has exactly the locked 12-column schema."""
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_ASC_RATES)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_RATES - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_RATES
    assert not missing, (
        f"asc_rates.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"asc_rates.parquet has unexpected columns: {sorted(extra)}"
    )


def test_asc_payment_indicators_locked_schema() -> None:
    """The unioned ASC DD1 parquet has exactly the locked 5-column schema."""
    if not _ASC_DD1.exists():
        pytest.skip(f"{_ASC_DD1} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_ASC_DD1)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_DD1 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_DD1
    assert not missing, (
        f"asc_payment_indicators.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"asc_payment_indicators.parquet has unexpected columns: "
        f"{sorted(extra)}"
    )


def test_asc_status_indicators_locked_schema() -> None:
    """The unioned ASC DD2 parquet has exactly the locked 5-column schema."""
    if not _ASC_DD2.exists():
        pytest.skip(f"{_ASC_DD2} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_ASC_DD2)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_DD2 - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_DD2
    assert not missing, (
        f"asc_status_indicators.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"asc_status_indicators.parquet has unexpected columns: "
        f"{sorted(extra)}"
    )


def test_asc_excluded_procedures_locked_schema() -> None:
    """The unioned ASC EE parquet has exactly the locked 5-column schema."""
    if not _ASC_EE.exists():
        pytest.skip(f"{_ASC_EE} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_ASC_EE)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_EE - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_EE
    assert not missing, (
        f"asc_excluded_procedures.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"asc_excluded_procedures.parquet has unexpected columns: "
        f"{sorted(extra)}"
    )


# --------------------------------------------------------------------------
# Compound-key uniqueness
# --------------------------------------------------------------------------


def test_asc_rates_compound_key_unique() -> None:
    """``(code, modifier, cy, quarter, addendum)`` is unique in the unioned rates."""
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, modifier, cy, quarter, addendum, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4,5 HAVING c>1 LIMIT 5",
            [str(_ASC_RATES)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, modifier, cy, quarter, addendum) compound keys "
        f"found in asc_rates.parquet (sample): {dups}"
    )


def test_asc_payment_indicators_compound_key_unique() -> None:
    """``(payment_indicator, cy, quarter)`` is unique in the unioned DD1."""
    if not _ASC_DD1.exists():
        pytest.skip(f"{_ASC_DD1} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT payment_indicator, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_ASC_DD1)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (payment_indicator, cy, quarter) compound keys found in "
        f"asc_payment_indicators.parquet (sample): {dups}"
    )


def test_asc_status_indicators_compound_key_unique() -> None:
    """``(status_indicator, cy, quarter)`` is unique in the unioned DD2."""
    if not _ASC_DD2.exists():
        pytest.skip(f"{_ASC_DD2} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT status_indicator, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_ASC_DD2)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (status_indicator, cy, quarter) compound keys found in "
        f"asc_status_indicators.parquet (sample): {dups}"
    )


def test_asc_excluded_procedures_compound_key_unique() -> None:
    """``(code, cy, quarter)`` is unique in the unioned EE."""
    if not _ASC_EE.exists():
        pytest.skip(f"{_ASC_EE} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_ASC_EE)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, cy, quarter) compound keys found in "
        f"asc_excluded_procedures.parquet (sample): {dups}"
    )


# --------------------------------------------------------------------------
# Parity (unified row count == sum of per-source rows)
# --------------------------------------------------------------------------


def test_asc_rates_parity() -> None:
    """Unified ASC rates row count == sum of per-source AA+BB+FF rows."""
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        per_source_files: list[str] = []
        for glob in _ASC_RATES_GLOBS:
            per_source_files.extend(
                row[0]
                for row in con.execute(
                    "SELECT file FROM glob(?)", [glob]
                ).fetchall()
            )
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets matching {_ASC_RATES_GLOBS}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_ASC_RATES_GLOBS],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_ASC_RATES)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: asc_rates has {master_n} rows, "
        f"per_source/asc/asc_(aa|bb|ff)_*.parquet sums to {per_source_n}."
    )


def test_asc_payment_indicators_parity() -> None:
    """Unified DD1 row count == sum of per-source ``asc_dd1_*.parquet`` rows."""
    if not _ASC_DD1.exists():
        pytest.skip(f"{_ASC_DD1} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [_ASC_DD1_GLOB]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {_ASC_DD1_GLOB}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_ASC_DD1_GLOB],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_ASC_DD1)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: asc_payment_indicators has {master_n} rows, "
        f"per_source/asc/asc_dd1_*.parquet sums to {per_source_n}."
    )


def test_asc_status_indicators_parity() -> None:
    """Unified DD2 row count == sum of per-source ``asc_dd2_*.parquet`` rows."""
    if not _ASC_DD2.exists():
        pytest.skip(f"{_ASC_DD2} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [_ASC_DD2_GLOB]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {_ASC_DD2_GLOB}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_ASC_DD2_GLOB],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_ASC_DD2)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: asc_status_indicators has {master_n} rows, "
        f"per_source/asc/asc_dd2_*.parquet sums to {per_source_n}."
    )


def test_asc_excluded_procedures_parity() -> None:
    """Unified EE row count == sum of per-source ``asc_ee_*.parquet`` rows."""
    if not _ASC_EE.exists():
        pytest.skip(f"{_ASC_EE} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [_ASC_EE_GLOB]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {_ASC_EE_GLOB}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [_ASC_EE_GLOB],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_ASC_EE)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: asc_excluded_procedures has {master_n} rows, "
        f"per_source/asc/asc_ee_*.parquet sums to {per_source_n}."
    )


# --------------------------------------------------------------------------
# Cross-cutting cardinality / domain assertions
# --------------------------------------------------------------------------


def test_asc_rates_addendum_values() -> None:
    """``addendum`` is restricted to {AA, BB, FF} in the unioned rates."""
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        addenda = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT addendum FROM read_parquet(?)",
                [str(_ASC_RATES)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"AA", "BB", "FF"}
    unexpected = addenda - allowed
    assert not unexpected, (
        f"asc_rates: addendum has unexpected values: {sorted(unexpected)}; "
        f"allowed are {sorted(allowed)}"
    )


def test_asc_rates_code_systems() -> None:
    """``code_system`` is restricted to {CPT, HCPCS-II} in the unioned rates."""
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        systems = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?)",
                [str(_ASC_RATES)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"CPT", "HCPCS-II"}
    unexpected = systems - allowed
    assert not unexpected, (
        f"asc_rates: code_system has unexpected values: "
        f"{sorted(unexpected)}; allowed are {sorted(allowed)}"
    )


def test_asc_rates_quarters_valid() -> None:
    """``quarter`` is restricted to {A, B, C, D} in all four unified parquets."""
    allowed = {"A", "B", "C", "D"}
    checked_any = False

    for master in (_ASC_RATES, _ASC_DD1, _ASC_DD2, _ASC_EE):
        if not master.exists():
            continue
        checked_any = True
        con = duckdb.connect()
        try:
            quarters = {
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT quarter FROM read_parquet(?)",
                    [str(master)],
                ).fetchall()
            }
        finally:
            con.close()
        unexpected = quarters - allowed
        assert not unexpected, (
            f"{master.name}: quarter has unexpected values: "
            f"{sorted(unexpected)}; allowed are {sorted(allowed)}"
        )

    if not checked_any:
        pytest.skip(
            f"None of the four unified ASC parquets present; {_SKIP_HINT}"
        )


def test_asc_rates_no_null_required() -> None:
    """Required (non-nullable) columns must have zero nulls in asc_rates.

    Per the locked schema, nullable columns intentionally excluded:
    ``modifier``, ``payment_indicator``, ``payment_rate``, ``short_descriptor``.
    """
    if not _ASC_RATES.exists():
        pytest.skip(f"{_ASC_RATES} not present; {_SKIP_HINT}")

    required = [
        "code",
        "code_system",
        "cy",
        "quarter",
        "addendum",
        "effective_start",
        "effective_end",
        "source",
    ]

    con = duckdb.connect()
    try:
        select_cols = ", ".join(
            f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "{c}"'
            for c in required
        )
        row = con.execute(
            f"SELECT {select_cols} FROM read_parquet(?)",
            [str(_ASC_RATES)],
        ).fetchone()
    finally:
        con.close()

    null_counts = dict(zip(required, row))
    offenders = {col: n for col, n in null_counts.items() if n}
    assert not offenders, (
        f"Required asc_rates columns have nulls: {offenders}"
    )


def test_asc_dd1_no_null_required() -> None:
    """Required (non-nullable) columns must have zero nulls in DD1.

    Nullable columns intentionally excluded: ``description``.
    """
    if not _ASC_DD1.exists():
        pytest.skip(f"{_ASC_DD1} not present; {_SKIP_HINT}")

    required = [
        "payment_indicator",
        "cy",
        "quarter",
        "source",
    ]

    con = duckdb.connect()
    try:
        select_cols = ", ".join(
            f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "{c}"'
            for c in required
        )
        row = con.execute(
            f"SELECT {select_cols} FROM read_parquet(?)",
            [str(_ASC_DD1)],
        ).fetchone()
    finally:
        con.close()

    null_counts = dict(zip(required, row))
    offenders = {col: n for col, n in null_counts.items() if n}
    assert not offenders, (
        f"Required asc_payment_indicators columns have nulls: {offenders}"
    )
