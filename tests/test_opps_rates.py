"""Phase 2 (Session 4a) tests for the unioned OPPS silver parquets.

Two unified outputs:

  - ``data/silver/opps_apc_rates.parquet`` — Addendum A (APC-level), 12 columns.
  - ``data/silver/opps_hcpcs_rates.parquet`` — Addendum B (HCPCS-level), 13 columns.

Mirrors ``tests/test_pfs_rates.py``: locked-schema enforcement, compound-key
uniqueness, parity vs per-source rows, cardinality assertions.

The transform module (``src.transform.opps_rates``) is loaded via
``pytest.importorskip`` at module level so test collection doesn't ImportError
while Batch 2b is concurrent. Once it lands, ``test_transform_module_importable``
asserts the documented public API surface.
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# importorskip at module level: Batch 2b is concurrent with this test file.
# Plain `from src.transform.opps_rates import ...` would fail collection if
# the module is not yet on disk. Use `pytest.importorskip` so the suite
# collects cleanly pre-Batch-2b.
pytest.importorskip("src.transform.opps_rates")

_APC_MASTER = config.SILVER / "opps_apc_rates.parquet"
_HCPCS_MASTER = config.SILVER / "opps_hcpcs_rates.parquet"
_APC_PER_SOURCE_GLOB = config.SILVER_PER_SOURCE / "opps" / "opps_a_*.parquet"
_HCPCS_PER_SOURCE_GLOB = config.SILVER_PER_SOURCE / "opps" / "opps_b_*.parquet"

# Locked OPPS Addendum A schema (plan-v5.md lines 102-116): 12 columns.
_EXPECTED_COLUMNS_APC: frozenset[str] = frozenset(
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

# Locked OPPS Addendum B schema (plan-v5.md lines 118-133): 13 columns.
_EXPECTED_COLUMNS_HCPCS: frozenset[str] = frozenset(
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

_SKIP_HINT = (
    "Run `python -m src.extract.opps --cy-range 2008-2026 --quarter-range A-D` "
    "followed by `python -m src.transform.opps_rates` to produce the parquets."
)


def test_transform_module_importable() -> None:
    """``build_opps_apc_rates`` and ``build_opps_hcpcs_rates`` are the
    documented public entry points."""
    from src.transform import opps_rates

    assert hasattr(opps_rates, "build_opps_apc_rates"), (
        "src.transform.opps_rates must export `build_opps_apc_rates`. "
        "TODO: if Batch 2b renamed it, update this test."
    )
    assert hasattr(opps_rates, "build_opps_hcpcs_rates"), (
        "src.transform.opps_rates must export `build_opps_hcpcs_rates`. "
        "TODO: if Batch 2b renamed it, update this test."
    )


# --------------------------------------------------------------------------
# Addendum A (APC) — opps_apc_rates.parquet
# --------------------------------------------------------------------------


def test_opps_apc_rates_locked_schema() -> None:
    """The unioned APC parquet has exactly the locked 12-column schema."""
    if not _APC_MASTER.exists():
        pytest.skip(f"{_APC_MASTER} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_APC_MASTER)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_APC - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_APC
    assert not missing, (
        f"opps_apc_rates.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"opps_apc_rates.parquet has unexpected columns: {sorted(extra)}"
    )


def test_opps_apc_rates_compound_key_unique() -> None:
    """``(apc, cy, quarter)`` is the natural key — must be unique."""
    if not _APC_MASTER.exists():
        pytest.skip(f"{_APC_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT apc, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_APC_MASTER)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (apc, cy, quarter) compound keys found in "
        f"opps_apc_rates.parquet (sample): {dups}"
    )


def test_opps_apc_rates_parity() -> None:
    """Unified APC row count == sum of per-source ``opps_a_*.parquet`` rows."""
    if not _APC_MASTER.exists():
        pytest.skip(f"{_APC_MASTER} not present; {_SKIP_HINT}")

    per_source_glob = str(_APC_PER_SOURCE_GLOB)
    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [per_source_glob]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {per_source_glob}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [per_source_glob],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_APC_MASTER)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: opps_apc_rates has {master_n} rows, "
        f"per_source/opps/opps_a_*.parquet sums to {per_source_n}."
    )


def test_opps_apc_rates_no_null_required() -> None:
    """Required (non-nullable) columns must have zero nulls.

    Per the locked schema, nullable columns intentionally excluded:
    ``status_indicator``, ``apc_title``, ``relative_weight``, ``payment_rate``,
    ``national_unadjusted_copayment``, ``minimum_unadjusted_copayment``.
    """
    if not _APC_MASTER.exists():
        pytest.skip(f"{_APC_MASTER} not present; {_SKIP_HINT}")

    required = [
        "apc",
        "cy",
        "quarter",
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
            [str(_APC_MASTER)],
        ).fetchone()
    finally:
        con.close()

    null_counts = dict(zip(required, row))
    offenders = {col: n for col, n in null_counts.items() if n}
    assert not offenders, (
        f"Required APC columns have nulls: {offenders}"
    )


# --------------------------------------------------------------------------
# Addendum B (HCPCS) — opps_hcpcs_rates.parquet
# --------------------------------------------------------------------------


def test_opps_hcpcs_rates_locked_schema() -> None:
    """The unioned HCPCS parquet has exactly the locked 13-column schema."""
    if not _HCPCS_MASTER.exists():
        pytest.skip(f"{_HCPCS_MASTER} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_HCPCS_MASTER)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS_HCPCS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS_HCPCS
    assert not missing, (
        f"opps_hcpcs_rates.parquet missing columns: {sorted(missing)}"
    )
    assert not extra, (
        f"opps_hcpcs_rates.parquet has unexpected columns: {sorted(extra)}"
    )


def test_opps_hcpcs_rates_compound_key_unique() -> None:
    """``(code, modifier, cy, quarter)`` is the natural key — must be unique.

    DuckDB GROUP BY treats NULL=NULL natively, no COALESCE shim needed.
    """
    if not _HCPCS_MASTER.exists():
        pytest.skip(f"{_HCPCS_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, modifier, cy, quarter, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4 HAVING c>1 LIMIT 5",
            [str(_HCPCS_MASTER)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, modifier, cy, quarter) compound keys found in "
        f"opps_hcpcs_rates.parquet (sample): {dups}"
    )


def test_opps_hcpcs_rates_parity() -> None:
    """Unified HCPCS row count == sum of per-source ``opps_b_*.parquet`` rows."""
    if not _HCPCS_MASTER.exists():
        pytest.skip(f"{_HCPCS_MASTER} not present; {_SKIP_HINT}")

    per_source_glob = str(_HCPCS_PER_SOURCE_GLOB)
    con = duckdb.connect()
    try:
        per_source_files = [
            row[0]
            for row in con.execute(
                "SELECT file FROM glob(?)", [per_source_glob]
            ).fetchall()
        ]
        if not per_source_files:
            pytest.skip(
                f"No per-source parquets at {per_source_glob}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [per_source_glob],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_HCPCS_MASTER)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: opps_hcpcs_rates has {master_n} rows, "
        f"per_source/opps/opps_b_*.parquet sums to {per_source_n}."
    )


def test_opps_hcpcs_rates_code_systems() -> None:
    """``code_system`` is restricted to {CPT, HCPCS-II}."""
    if not _HCPCS_MASTER.exists():
        pytest.skip(f"{_HCPCS_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        systems = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?)",
                [str(_HCPCS_MASTER)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"CPT", "HCPCS-II"}
    unexpected = systems - allowed
    assert not unexpected, (
        f"code_system has unexpected values: {sorted(unexpected)}; "
        f"allowed values are {sorted(allowed)}"
    )


def test_opps_hcpcs_rates_no_null_required() -> None:
    """Required (non-nullable) columns must have zero nulls.

    Nullable columns intentionally excluded: ``modifier`` (rare in OPPS),
    ``apc`` (some HCPCS rows have no APC mapping), ``status_indicator``,
    ``relative_weight``, ``payment_rate``, ``short_descriptor``.
    """
    if not _HCPCS_MASTER.exists():
        pytest.skip(f"{_HCPCS_MASTER} not present; {_SKIP_HINT}")

    required = [
        "code",
        "code_system",
        "cy",
        "quarter",
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
            [str(_HCPCS_MASTER)],
        ).fetchone()
    finally:
        con.close()

    null_counts = dict(zip(required, row))
    offenders = {col: n for col, n in null_counts.items() if n}
    assert not offenders, (
        f"Required HCPCS columns have nulls: {offenders}"
    )


# --------------------------------------------------------------------------
# Cross-cutting cardinality assertions
# --------------------------------------------------------------------------


def test_opps_quarters_valid() -> None:
    """``quarter`` is restricted to {A, B, C, D} in both unified outputs."""
    allowed = {"A", "B", "C", "D"}
    checked_any = False

    for master in (_APC_MASTER, _HCPCS_MASTER):
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
            f"Neither {_APC_MASTER} nor {_HCPCS_MASTER} present; {_SKIP_HINT}"
        )
