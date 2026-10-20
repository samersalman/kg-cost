"""Phase 2 tests for the unioned ``silver/pfs_rates.parquet``.

The transform module (``src.transform.pfs_rates``) must be importable
unconditionally — it ships in Batch 2b alongside this test file. Data-dependent
assertions skip when ``silver/pfs_rates.parquet`` is absent (the file is
written only after the per-(cy, quarter) extractor and the unifier CLI have
been run).
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Plain import (NOT importorskip): if this fails at discovery, it's a real
# Batch 2 bug, not a missing-data condition.
from src.transform.pfs_rates import build_pfs_rates  # noqa: F401

_MASTER = config.SILVER / "pfs_rates.parquet"
_PER_SOURCE_GLOB = config.SILVER_PER_SOURCE / "pfs_rvu" / "*.parquet"

# The locked Phase-2 schema (plan-v3.md lines 92-120). pfs_rates must have
# *exactly* these 18 columns — extra columns would mean a schema drift bug,
# missing columns would break downstream Phase 7/8 cost-observation loaders.
_EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "code",
        "modifier",
        "code_system",
        "cy",
        "quarter",
        "setting",
        "status_indicator",
        "work_rvu",
        "pe_rvu",
        "mp_rvu",
        "total_rvu",
        "conversion_factor",
        "conversion_factor_kind",
        "allowed_amount",
        "effective_start",
        "effective_end",
        "source",
        "n",
    }
)

_SKIP_HINT = (
    "Run `python -m src.extract.pfs_rvu --cy-range 2008-2026 --quarter-range A-D` "
    "followed by `python -m src.transform.pfs_rates` to produce the parquet."
)


def test_transform_module_importable() -> None:
    """``build_pfs_rates`` is the documented public entry point."""
    from src.transform import pfs_rates

    assert hasattr(pfs_rates, "build_pfs_rates"), (
        "src.transform.pfs_rates must export `build_pfs_rates`. "
        "TODO: if Batch 2 renamed it, update this test."
    )


def test_pfs_rates_exists_and_has_locked_schema() -> None:
    """The unioned parquet has exactly the locked 18-column schema."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    pf = pq.ParquetFile(_MASTER)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS
    assert not missing, f"pfs_rates.parquet missing columns: {sorted(missing)}"
    assert not extra, f"pfs_rates.parquet has unexpected columns: {sorted(extra)}"


def test_no_duplicate_compound_keys() -> None:
    """``(code, modifier, cy, quarter, setting, conversion_factor_kind)`` is the
    natural key — must be unique. CY2026 emits two rows per cell (qp + nonqp);
    earlier CYs emit one (single)."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, modifier, cy, quarter, setting, conversion_factor_kind, "
            "COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4,5,6 HAVING c>1 LIMIT 5",
            [str(_MASTER)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        f"Duplicate (code, modifier, cy, quarter, setting, conversion_factor_kind) "
        f"compound keys found (sample): {dups}"
    )


def test_total_rowcount_matches_per_source() -> None:
    """Every per-source row must land in pfs_rates — no silent drops."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    per_source_glob = str(_PER_SOURCE_GLOB)
    con = duckdb.connect()
    try:
        per_source_files = [
            row[0] for row in con.execute("SELECT file FROM glob(?)", [per_source_glob]).fetchall()
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
            [str(_MASTER)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: pfs_rates has {master_n} rows, "
        f"per_source/pfs_rvu/*.parquet sums to {per_source_n}."
    )


def test_both_settings_present() -> None:
    """pfs_rates spans both pfs_facility and pfs_nonfacility settings."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        settings = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT setting FROM read_parquet(?)",
                [str(_MASTER)],
            ).fetchall()
        }
    finally:
        con.close()

    assert "pfs_facility" in settings, f"pfs_facility missing from setting; got {settings}"
    assert "pfs_nonfacility" in settings, f"pfs_nonfacility missing from setting; got {settings}"


def test_conversion_factor_kind_values() -> None:
    """``conversion_factor_kind`` is restricted to {single, qp, nonqp}.

    Acceptable subsets: ``{single}`` (only pre-2026 data), ``{qp, nonqp}``
    (only CY2026 data), or the full ``{single, qp, nonqp}``. Any other value
    is a schema drift bug.
    """
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        kinds = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT conversion_factor_kind FROM read_parquet(?)",
                [str(_MASTER)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"single", "qp", "nonqp"}
    unexpected = kinds - allowed
    assert not unexpected, (
        f"conversion_factor_kind has unexpected values: {sorted(unexpected)}; "
        f"allowed values are {sorted(allowed)}"
    )


def test_code_systems_present() -> None:
    """``code_system`` is restricted to {CPT, HCPCS-II}.

    Both should typically be present, but the test only forbids unknown
    systems — a parquet that, e.g., only has CPT rows is still valid.
    """
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        systems = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?)",
                [str(_MASTER)],
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


def test_quarters_valid() -> None:
    """``quarter`` is restricted to {A, B, C, D}."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    con = duckdb.connect()
    try:
        quarters = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT quarter FROM read_parquet(?)",
                [str(_MASTER)],
            ).fetchall()
        }
    finally:
        con.close()

    allowed = {"A", "B", "C", "D"}
    unexpected = quarters - allowed
    assert not unexpected, (
        f"quarter has unexpected values: {sorted(unexpected)}; "
        f"allowed values are {sorted(allowed)}"
    )


def test_no_null_required_columns() -> None:
    """Required (non-nullable) columns must have zero nulls.

    Nullable columns intentionally excluded: ``modifier`` (NULL = base row),
    ``status_indicator`` (may be missing in older PPRRVU files), all RVU
    columns (status-B/T rows can have null RVUs), ``allowed_amount`` (derived,
    null when total_rvu is null), and ``n`` (Phase 5 fills this).
    """
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; {_SKIP_HINT}")

    required = [
        "code",
        "code_system",
        "cy",
        "quarter",
        "setting",
        "conversion_factor",
        "conversion_factor_kind",
        "effective_start",
        "effective_end",
        "source",
    ]

    con = duckdb.connect()
    try:
        select_cols = ", ".join(
            f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "{c}"' for c in required
        )
        row = con.execute(
            f"SELECT {select_cols} FROM read_parquet(?)",
            [str(_MASTER)],
        ).fetchone()
    finally:
        con.close()

    null_counts = dict(zip(required, row))
    offenders = {col: n for col, n in null_counts.items() if n}
    assert not offenders, f"Required columns have nulls: {offenders}"
