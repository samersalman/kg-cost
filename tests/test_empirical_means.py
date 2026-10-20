"""Phase 5 tests for the unioned ``silver/empirical_means.parquet``.

The transform module (``src.transform.empirical_means``) must be importable
unconditionally — it ships in Batch 3 alongside this test file.
Data-dependent assertions skip when ``silver/empirical_means.parquet`` is
absent (the file is written only after the four PUF extractors and the
transform CLI have been run).
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Plain import (NOT importorskip): if this fails at discovery, it's a real
# Batch 3 bug, not a missing-data condition.
from src.transform.empirical_means import build_empirical_means  # noqa: F401

_OUTPUT = config.SILVER / "empirical_means.parquet"

# The locked Phase-5 schema (plan-v11 §Schema, Session 8.5 schema rename).
# The unioned empirical_means parquet must have *exactly* these 27
# columns — extra columns would mean a schema drift bug, missing columns
# would break Phase 7 cost-compute downstream. ``source_host`` was added
# in plan-v11 (Session 8.5) between ``source`` and ``effective_start``.
# Session 8.5 DEV 4 dropped ``mean_allowed`` (semantic-overloaded) and
# replaced it with ``payment_amount`` (NOT NULL) + ``payment_amount_concept``
# enum + ``total_payment_amount`` (self-documenting alias on inpatient).
_EXPECTED_COLUMNS: set[str] = {
    "scope",
    "code",
    "code_system",
    "year",
    "place_of_srvc",
    "n",
    "mean_charge",
    "mean_payment",
    "payment_amount",
    "payment_amount_concept",
    "total_payment_amount",
    "mean_payment_stdzd",
    "p10_charge",
    "p25_charge",
    "p50_charge",
    "p75_charge",
    "p90_charge",
    "p10_payment",
    "p25_payment",
    "p50_payment",
    "p75_payment",
    "p90_payment",
    "n_providers",
    "source",
    "source_host",
    "effective_start",
    "effective_end",
}

_EXPECTED_CODE_SYSTEMS: set[str] = {"MS-DRG", "APC", "HCPCS-II"}
# Session 8.5 PI verdict on DEV 1: ``puf_all_drgs`` renamed to
# ``puf_all_drgs_ry23`` to capture the RY23-unified-release distinct
# methodology. ``puf_all_drgs_legacy`` is reserved as a future tag for
# hypothetical fallback to original-vintage annual releases (no rows
# emitted in this session).
_EXPECTED_SOURCES: frozenset[str] = frozenset({
    "puf_top100",
    "puf_all_drgs_ry23",
    "puf_top100_summary",
    "puf_outpatient",
    "puf_physician",
})
_EXPECTED_SOURCE_HOSTS: frozenset[str] = frozenset({"cms_gov", "nber", "wayback"})
_EXPECTED_CONCEPTS: frozenset[str] = frozenset(
    {"medicare_allowed", "total_payment_allpayer"}
)


def _skip_if_missing() -> None:
    if not _OUTPUT.exists():
        pytest.skip(
            f"{_OUTPUT} not present; run "
            "`python -m src.transform.empirical_means` first"
        )


def test_transform_module_importable() -> None:
    """``build_empirical_means`` is the documented public entry point."""
    from src.transform import empirical_means

    assert hasattr(empirical_means, "build_empirical_means"), (
        "src.transform.empirical_means must export `build_empirical_means`. "
        "TODO: if Batch 3 renamed it, update this test."
    )


def test_empirical_means_schema_correct() -> None:
    """The unioned parquet has exactly the 27 locked schema columns."""
    _skip_if_missing()

    pf = pq.ParquetFile(_OUTPUT)
    actual_columns = set(pf.schema_arrow.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    extra = actual_columns - _EXPECTED_COLUMNS
    assert not missing, f"empirical_means.parquet missing columns: {sorted(missing)}"
    assert not extra, f"empirical_means.parquet has unexpected columns: {sorted(extra)}"


def test_no_duplicate_compound_keys() -> None:
    """``(code, code_system, year, place_of_srvc, source)`` is the natural key."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, code_system, year, place_of_srvc, source, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1, 2, 3, 4, 5 HAVING c > 1 LIMIT 5",
            [str(_OUTPUT)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, (
        "Duplicate (code, code_system, year, place_of_srvc, source) 5-tuples "
        f"found (sample): {dups}"
    )


def test_code_system_subset() -> None:
    """Distinct ``code_system`` ⊆ {MS-DRG, APC, HCPCS-II}."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?) "
                "WHERE code_system IS NOT NULL",
                [str(_OUTPUT)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = observed - _EXPECTED_CODE_SYSTEMS
    assert not unexpected, (
        f"empirical_means contains unexpected code_system values: "
        f"{sorted(unexpected)}. Expected subset of: "
        f"{sorted(_EXPECTED_CODE_SYSTEMS)}."
    )


def test_source_subset() -> None:
    """Distinct ``source`` ⊆ the 5 locked PUF source literals."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT source FROM read_parquet(?) "
                "WHERE source IS NOT NULL",
                [str(_OUTPUT)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = observed - _EXPECTED_SOURCES
    assert not unexpected, (
        f"empirical_means contains unexpected source values: "
        f"{sorted(unexpected)}. Expected subset of: "
        f"{sorted(_EXPECTED_SOURCES)}."
    )


def test_source_host_subset() -> None:
    """Distinct ``source_host`` ⊆ {cms_gov, nber, wayback} (plan-v11 Session 8.5)."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT source_host FROM read_parquet(?) "
                "WHERE source_host IS NOT NULL",
                [str(_OUTPUT)],
            ).fetchall()
        }
    finally:
        con.close()

    unexpected = observed - _EXPECTED_SOURCE_HOSTS
    assert not unexpected, (
        f"empirical_means contains unexpected source_host values: "
        f"{sorted(unexpected)}. Expected subset of: "
        f"{sorted(_EXPECTED_SOURCE_HOSTS)}."
    )


def test_scope_always_national() -> None:
    """Plan-v10 §Schema line 70 locks ``scope`` to ``"national"`` for Phase 5."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT scope FROM read_parquet(?)",
                [str(_OUTPUT)],
            ).fetchall()
        }
    finally:
        con.close()

    assert observed == {"national"}, (
        f"empirical_means.scope must be exactly {{'national'}} per plan-v10; "
        f"observed: {sorted(observed)}."
    )


def test_total_rowcount_matches_per_source() -> None:
    """Every per-source PUF row must land in empirical_means — no silent drops."""
    _skip_if_missing()

    puf_dir = config.SILVER_PER_SOURCE / "puf"
    per_source_globs = [
        str(puf_dir / "inpatient" / "puf_inpatient_fy*.parquet"),
        str(puf_dir / "inpatient_summary" / "puf_inpatient_summary_fy*.parquet"),
        str(puf_dir / "outpatient" / "puf_outpatient_cy*.parquet"),
        str(puf_dir / "physician" / "puf_physician_cy*.parquet"),
    ]

    con = duckdb.connect()
    try:
        per_source_files: list[str] = []
        for g in per_source_globs:
            per_source_files.extend(
                row[0]
                for row in con.execute(
                    "SELECT file FROM glob(?)", [g]
                ).fetchall()
            )
        if not per_source_files:
            pytest.skip(
                f"No per-source PUF parquets at {per_source_globs}; "
                "cannot validate row-count parity."
            )
        per_source_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [per_source_files],
        ).fetchone()[0]
        master_n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(_OUTPUT)],
        ).fetchone()[0]
    finally:
        con.close()

    assert master_n == per_source_n, (
        f"Row-count mismatch: empirical_means has {master_n} rows, "
        f"per-source PUF parquets sum to {per_source_n}."
    )


def test_payment_amount_concept_subset() -> None:
    """Distinct payment_amount_concept ⊆ {medicare_allowed,
    total_payment_allpayer} (Phase 5 Session 8.5 enum contract; never NULL)."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT payment_amount_concept FROM read_parquet(?)",
                [str(_OUTPUT)],
            ).fetchall()
        }
    finally:
        con.close()

    assert None not in observed, (
        "empirical_means has rows with NULL payment_amount_concept; "
        "Session 8.5 contract requires concept set per row."
    )
    unexpected = observed - _EXPECTED_CONCEPTS
    assert not unexpected, (
        f"empirical_means contains unexpected payment_amount_concept "
        f"values: {sorted(unexpected)}. Expected subset of: "
        f"{sorted(_EXPECTED_CONCEPTS)}."
    )


def test_payment_amount_never_null() -> None:
    """payment_amount IS NOT NULL on every row (Phase 5 Session 8.5
    contract; transform-layer assertion enforces this)."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        n_null = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE payment_amount IS NULL",
            [str(_OUTPUT)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_null == 0, (
        f"empirical_means has {n_null} rows with NULL payment_amount; "
        "Session 8.5 contract requires payment_amount NOT NULL on every row."
    )


def test_total_payment_amount_semantic_alias_contract() -> None:
    """total_payment_amount is a self-documenting alias: when
    concept=total_payment_allpayer it equals payment_amount; when
    concept=medicare_allowed it IS NULL. Phase 5 Session 8.5 contract.
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        violations = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?) WHERE
            (payment_amount_concept = 'total_payment_allpayer' AND
             (total_payment_amount IS NULL OR total_payment_amount != payment_amount))
            OR (payment_amount_concept = 'medicare_allowed' AND
                total_payment_amount IS NOT NULL)
            """,
            [str(_OUTPUT)],
        ).fetchone()[0]
    finally:
        con.close()

    assert violations == 0, (
        f"empirical_means has {violations} rows violating the "
        f"total_payment_amount semantic-alias contract."
    )
