"""Phase 1 tests for the unioned ``silver/codes_master.parquet``.

The transform module (``src.transform.codes_master``) must be importable
unconditionally — it ships in Batch 2 alongside this test file. Data-dependent
assertions skip when ``silver/codes_master.parquet`` is absent (the file is
written only after the extractors and the transform CLI have been run).
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Plain import (NOT importorskip): if this fails at discovery, it's a real
# Batch 2 bug, not a missing-data condition.
from src.transform.codes_master import build_codes_master  # noqa: F401

_MASTER = config.SILVER / "codes_master.parquet"

# The locked Phase-1 schema (plan-v1.md lines 105-123). Codes_master must have
# *exactly* these columns — extra columns would mean a schema drift bug, missing
# columns would break Session 2's append.
_EXPECTED_COLUMNS: set[str] = {
    "code",
    "code_system",
    "fy_or_cy",
    "effective_start",
    "effective_end",
    "description",
    "short_description",
    "chapter",
    "addendum_action",
    "or_procedure_flag",
    "mcc_flag",
    "cc_flag",
    "poa_exempt",
    "source",
    "n",
}


def test_transform_module_importable() -> None:
    """``build_codes_master`` is the documented public entry point."""
    from src.transform import codes_master

    assert hasattr(codes_master, "build_codes_master"), (
        "src.transform.codes_master must export `build_codes_master`. "
        "TODO: if Batch 2 renamed it, update this test."
    )


def test_codes_master_exists() -> None:
    """The unioned parquet has exactly the locked schema columns."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; run `python -m src.transform.codes_master` first")

    pf = pq.ParquetFile(_MASTER)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS
    assert not missing, f"codes_master.parquet missing columns: {sorted(missing)}"
    assert not extra, f"codes_master.parquet has unexpected columns: {sorted(extra)}"


def test_no_duplicate_triples() -> None:
    """``(code, code_system, fy_or_cy)`` is the natural key — must be unique."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; run transform first")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, code_system, fy_or_cy, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_MASTER)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, f"Duplicate (code, code_system, fy_or_cy) triples found (sample): {dups}"


def test_total_rowcount_matches_per_source() -> None:
    """Every per-source row must land in codes_master — no silent drops."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; run transform first")

    per_source_glob = str(config.SILVER_PER_SOURCE / "*.parquet")
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
        f"Row-count mismatch: codes_master has {master_n} rows, "
        f"per_source/*.parquet sums to {per_source_n}."
    )


def test_both_systems_present() -> None:
    """codes_master spans both ICD-10-CM and ICD-10-PCS for the Phase-1 slice."""
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; run transform first")

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

    assert "ICD-10-CM" in systems, f"ICD-10-CM missing from code_system; got {systems}"
    assert "ICD-10-PCS" in systems, f"ICD-10-PCS missing from code_system; got {systems}"


def test_codes_master_unaffected_by_phase5() -> None:
    """Phase 5 PUF extractors emit to silver/per_source/puf/<source>/ subdirs;
    DuckDB's non-recursive `silver/per_source/*.parquet` glob in
    codes_master MUST NOT pick them up. Locked invariant: codes_master
    row count stays at 1,843,825 post-Phase-5.
    """
    if not _MASTER.exists():
        pytest.skip(f"{_MASTER} not present; run `python -m src.transform.codes_master` first")

    con = duckdb.connect()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(_MASTER)]
        ).fetchone()[0]
    finally:
        con.close()
    assert n == 1_843_825, (
        f"codes_master row count is {n:,}, expected 1,843,825. "
        "Phase 5 extractors may be writing into silver/per_source/*.parquet "
        "(non-puf subdir) -- check subdir isolation."
    )
