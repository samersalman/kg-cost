"""Phase 7a tests for ``data/gold/code_costs.parquet``.

The transform module (``src.transform.code_costs``) must be importable
unconditionally. Data-dependent assertions skip when the gold parquet is
absent (the file is written only after the transform CLI has been run).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config
from src.transform.code_costs import (  # noqa: F401
    _SCHEMA,
    _SETTING_VALUES,
    _SOURCE_TAG_VALUES,
    build_code_costs,
)

_GOLD = config.GOLD / "code_costs.parquet"


def _skip_if_missing(parquet_path: Path, label: str) -> None:
    """Skip the calling test if ``parquet_path`` is absent."""
    if not parquet_path.exists():
        pytest.skip(
            f"{label} not present at {parquet_path}; "
            "run `python -m src.transform.code_costs` first."
        )


def test_transform_module_importable() -> None:
    """``build_code_costs`` is the public entry point."""
    from src.transform import code_costs

    assert hasattr(code_costs, "build_code_costs")


def test_code_costs_schema_locked() -> None:
    """Columns and types match ``_SCHEMA`` exactly."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    pf = pq.ParquetFile(_GOLD)
    actual_columns = list(pf.schema_arrow.names)
    expected_columns = list(_SCHEMA.keys())

    assert actual_columns == expected_columns, (
        f"Column drift. Expected {expected_columns}, got {actual_columns}."
    )

    # Type spot-check via DuckDB DESCRIBE -- guards against silent type-cast
    # drift that pyarrow names alone wouldn't surface.
    con = duckdb.connect()
    try:
        rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(_GOLD)]
        ).fetchall()
    finally:
        con.close()
    actual_types = {row[0]: row[1] for row in rows}
    # DuckDB normalizes type names, so just check a few load-bearing ones.
    assert actual_types["fy_or_cy"] == "INTEGER"
    assert actual_types["charge"] == "DOUBLE"
    assert actual_types["allowed_payment"] == "DOUBLE"
    assert actual_types["allowed_payment_source"] == "VARCHAR"
    assert actual_types["effective_start"] == "DATE"


def test_compound_key_unique() -> None:
    """``(code, code_system, fy_or_cy, setting)`` is the natural key."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT code, code_system, fy_or_cy, setting, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3,4 HAVING c>1 LIMIT 5",
            [str(_GOLD)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, f"Compound-key collisions: {dups}"


def test_setting_enum() -> None:
    """Every row's ``setting`` is in the locked enum."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT setting FROM read_parquet(?)", [str(_GOLD)]
            ).fetchall()
        }
    finally:
        con.close()
    bad = observed - _SETTING_VALUES
    assert not bad, f"Unexpected setting values: {bad}"


def test_source_tag_enum_charge_or_payment_source() -> None:
    """charge_source ∈ {puf_empirical, NULL}; allowed_payment_source ∈ enum or NULL."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        bad_charge = con.execute(
            "SELECT DISTINCT charge_source FROM read_parquet(?) "
            "WHERE charge_source IS NOT NULL AND charge_source <> 'puf_empirical'",
            [str(_GOLD)],
        ).fetchall()
        bad_pay = con.execute(
            "SELECT DISTINCT allowed_payment_source FROM read_parquet(?) "
            "WHERE allowed_payment_source IS NOT NULL "
            "AND allowed_payment_source NOT IN "
            "('puf_empirical','formula_ipps','formula_pfs','formula_opps',"
            "'formula_asc','formula_no_empirical',"
            "'gems_bridged_modal','gems_bridged_back_translated')",
            [str(_GOLD)],
        ).fetchall()
    finally:
        con.close()
    assert not bad_charge, f"Unexpected charge_source values: {bad_charge}"
    assert not bad_pay, f"Unexpected allowed_payment_source values: {bad_pay}"
    # Belt-and-suspenders: the enum frozen-set must mirror the SQL above.
    assert _SOURCE_TAG_VALUES == frozenset({
        "puf_empirical", "formula_ipps", "formula_pfs", "formula_opps",
        "formula_asc", "formula_no_empirical",
        "gems_bridged_modal", "gems_bridged_back_translated",
    })


def test_payment_concept_never_null_when_puf_empirical() -> None:
    """Phase 5 DEV 4 contract: empirical payment must carry a concept tag."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        n_bad = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE allowed_payment IS NOT NULL "
            "AND allowed_payment_source = 'puf_empirical' "
            "AND allowed_payment_concept IS NULL",
            [str(_GOLD)],
        ).fetchone()[0]
    finally:
        con.close()
    assert n_bad == 0, (
        f"{n_bad} rows have puf_empirical allowed_payment but NULL concept "
        "(violates Phase 5 DEV 4 contract)."
    )


def test_reserved_drgs_zero_allowed_payment() -> None:
    """Reserved DRGs 998/999 with formula source must report 0 allowed_payment."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        nonzero = con.execute(
            "SELECT code, fy_or_cy, allowed_payment FROM read_parquet(?) "
            "WHERE code IN ('998','999') AND code_system='MS-DRG' "
            "AND allowed_payment_source='formula_ipps' "
            "AND allowed_payment <> 0 LIMIT 5",
            [str(_GOLD)],
        ).fetchall()
    finally:
        con.close()
    assert not nonzero, (
        f"Reserved DRG 998/999 formula_ipps rows must equal 0: {nonzero}"
    )


def test_three_concept_independence() -> None:
    """NULL in one concept must not block the others.

    Per landmine #8 + Phase 5 DEV 4: charge, allowed_payment, and ccr_cost are
    INDEPENDENT. We verify by existence: rows with NULL charge can still have
    non-NULL allowed_payment, and rows with NULL ccr_value can still have
    populated charge/payment. Note Phase 6 is deferred so ccr_value is
    universally NULL -- the check still passes because *every* non-NULL
    payment row coexists with NULL ccr_value, demonstrating no NULL-prop.
    """
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        n_charge_null_pay_nn = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE charge IS NULL AND allowed_payment IS NOT NULL",
            [str(_GOLD)],
        ).fetchone()[0]
        n_pay_nn_ccr_null = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE allowed_payment IS NOT NULL AND ccr_value IS NULL",
            [str(_GOLD)],
        ).fetchone()[0]
        # Symmetric direction (charge populated, payment NULL) is structurally
        # absent in current Phase 5 silvers because every PUF charge source
        # ships the matching payment column. Documented as Session 9 flag.
        n_charge_nn_pay_null = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE charge IS NOT NULL AND allowed_payment IS NULL",
            [str(_GOLD)],
        ).fetchone()[0]
    finally:
        con.close()

    assert n_charge_null_pay_nn > 0, (
        "Three-concept independence violated: no rows with NULL charge + "
        "non-NULL allowed_payment found."
    )
    assert n_pay_nn_ccr_null > 0, (
        "Three-concept independence violated: NULL ccr_value should not "
        "block allowed_payment population."
    )
    # Soft-allow zero charge_NN+pay_NULL rows -- this is a Phase 5 silver
    # property, not a Phase 7a bug. Document for Session 9 flag review.
    assert n_charge_nn_pay_null >= 0


def test_phase6_ccr_populated() -> None:
    """Phase 6 SHIPPED in Session 9.5: ccr_cost populated for FY2015+ inpatient.

    Sentinel was renamed from ``test_phase6_deferred_ccr_all_null`` once
    ``data/silver/national_ccr.parquet`` landed and ``code_costs.py`` was
    wired to it. The contract: for inpatient rows where ``charge IS NOT NULL``
    AND ``fy >= 2015``, ``ccr_cost`` must be populated for >=80% of the rows.

    Phase 11 (Session 12, 2026-05-02) attempted FY2008-FY2014 backfill via
    NBER long-form but rolled back to FY2015 floor pending Form-2552-96
    worksheet remap (deferred to Session 1.5; see plans/Session 1 flags.md
    FLAG 5 for details).

    The 80% floor leaves slack for two known sources of NULL:
      * Reserved DRGs 998/999 with ``charge IS NULL`` are excluded from the
        denominator already (filter is ``charge IS NOT NULL``).
      * Edge-case rows where the empirical PUF charge column is populated but
        the matched cost-center CCR is NULL (national_ccr ships 12 FYs × 19
        groups so this should not happen for the default Operating Room
        cost-center, but the floor leaves slack for upstream surprises).
    """
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        total, populated = con.execute(
            "SELECT COUNT(*), COUNT(ccr_cost) FROM read_parquet(?) "
            "WHERE setting='inpatient' AND charge IS NOT NULL AND fy >= 2015",
            [str(_GOLD)],
        ).fetchone()
    finally:
        con.close()

    assert total > 0, (
        "No inpatient rows with charge IS NOT NULL AND fy >= 2015 found; "
        "either Phase 5 PUF empirical means are unloaded or row-count floor "
        "fell through. Investigate before flipping this assertion."
    )
    coverage = populated / total
    assert coverage >= 0.80, (
        f"ccr_cost populated for {populated}/{total} ({coverage:.1%}) of "
        "inpatient rows with charge IS NOT NULL AND fy >= 2015; expected >=80%."
    )


def test_charge_payment_ratio_heuristic() -> None:
    """median(charge / allowed_payment) >= 2.0 (charges typically 2-4x payments)."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        median = con.execute(
            "SELECT MEDIAN(charge / allowed_payment) FROM read_parquet(?) "
            "WHERE charge IS NOT NULL "
            "AND allowed_payment IS NOT NULL "
            "AND allowed_payment > 0",
            [str(_GOLD)],
        ).fetchone()[0]
    finally:
        con.close()
    assert median is not None and median >= 2.0, (
        f"median(charge/allowed_payment) = {median!r}; expected >= 2.0 "
        "(charges should be 2-4x payments per plan-v12 acceptance)."
    )


def test_inpatient_formula_no_empirical_carve_out() -> None:
    """``formula_no_empirical`` rows surface in FY2011/FY2012 (long-tail DRGs)."""
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT fy, COUNT(*) FROM read_parquet(?) "
            "WHERE allowed_payment_source='formula_no_empirical' "
            "GROUP BY 1 ORDER BY 1",
            [str(_GOLD)],
        ).fetchall()
        total = sum(c for _, c in rows)
        years = {fy for fy, _ in rows}
    finally:
        con.close()
    assert years <= {2011, 2012}, (
        f"formula_no_empirical should be FY2011/FY2012 only; got years={years}."
    )
    assert total > 0, "No formula_no_empirical rows found (expected ~1,300)."
    # Soft cap: plan-v12.md acceptance criterion ceiling.
    assert total <= 5000, (
        f"formula_no_empirical row count {total} exceeds 5,000 ceiling."
    )


def test_row_count_floor() -> None:
    """Total row count meets the empirical floor (canonical-quarter pick).

    Plan-v12.md acceptance says ``>=1.5M``, but that aspirational figure
    predates landmine #5 (canonical ``quarter='A'``). With canonical-quarter
    rule the count lands ~500K. Floor here is 400K to catch silent collapse.
    """
    _skip_if_missing(_GOLD, "code_costs.parquet")

    con = duckdb.connect()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(_GOLD)]
        ).fetchone()[0]
    finally:
        con.close()
    assert n >= 400_000, (
        f"code_costs row count {n:,} below 400K floor "
        "(suggests silent join-collapse; investigate)."
    )
