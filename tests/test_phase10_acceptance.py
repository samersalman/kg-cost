"""Phase 10 acceptance umbrella — single regression surface for the four
PHASES.md Phase 10 acceptance criteria. Each test reuses existing test
machinery rather than duplicating logic.

Run after every change touching Phase 10 deliverables:
    pytest tests/test_phase10_acceptance.py -v

PHASES.md Phase 10 acceptance criteria:
  1. Grouper MIMIC-IV match >= 0.97 on Java FY2023+
  2. PUF empirical drift silver -> graph < 0.5%
  3. Smoke queries return non-empty results in credible ranges
  4. cohort_annotator coverage >= 0.95 on expected_match=true subset
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.config import GOLD, NEO4J_IMPORT, ROOT, SILVER


# ============================================================
# Criterion 1: Grouper MIMIC-IV match >= 0.97
# PHASES.md Phase 10 acceptance criterion #1
# ============================================================


def test_acceptance_grouper_match_above_threshold():
    """Phase 10 acceptance: grouper MIMIC-IV stratified match >= 0.97.

    The actual end-to-end match is asserted by ``tests/test_grouper_validation.py``
    (Java FY2023+ harness); this umbrella test guards the threshold constant
    so a silent lowering in that file is caught at the acceptance layer.
    """
    from tests.test_grouper_validation import _OVERALL_PASS_THRESHOLD

    assert _OVERALL_PASS_THRESHOLD >= 0.97, (
        f"Grouper threshold has been silently lowered to {_OVERALL_PASS_THRESHOLD}; "
        "PHASES.md acceptance is >= 0.97."
    )


# ============================================================
# Criterion 2: PUF drift silver -> graph < 0.5%
# PHASES.md Phase 10 acceptance criterion #2
# ============================================================


@pytest.mark.skipif(
    not (GOLD / "code_costs.parquet").exists(),
    reason="data/gold/code_costs.parquet not built",
)
@pytest.mark.skipif(
    not (SILVER / "empirical_means.parquet").exists(),
    reason="data/silver/empirical_means.parquet not built",
)
def test_acceptance_puf_drift_below_threshold():
    """Phase 10 acceptance: PUF empirical means drift silver -> gold < 0.5%.

    The graph is built from gold/code_costs.parquet, which is built from
    silver/empirical_means.parquet. This test catches build-pipeline drift
    (NaN handling, unit conversion, aggregation regressions) by re-aggregating
    silver and comparing against the gold rows tagged ``charge_source =
    'puf_empirical'``. Live-bolt parity is enforced separately by the smoke
    queries (criterion 3).
    """
    import duckdb

    silver = duckdb.sql(
        f"""
        SELECT code, code_system, year, mean_charge, payment_amount
        FROM '{SILVER / "empirical_means.parquet"}'
        WHERE source = 'puf_all_drgs_ry23'
          AND year >= 2015
          AND mean_charge IS NOT NULL
        """
    ).df()

    gold = duckdb.sql(
        f"""
        SELECT code, code_system, fy AS year, charge, allowed_payment
        FROM '{GOLD / "code_costs.parquet"}'
        WHERE setting = 'inpatient'
          AND charge_source = 'puf_empirical'
          AND charge IS NOT NULL
        """
    ).df()

    if len(silver) == 0 or len(gold) == 0:
        pytest.skip("no PUF inpatient rows to compare in either silver or gold")

    merged = silver.merge(
        gold, on=["code", "code_system", "year"], suffixes=("_silver", "_gold")
    )
    if len(merged) == 0:
        pytest.skip("no overlapping (code, code_system, year) rows")

    # Charges
    denom_charge = merged["mean_charge"].abs().replace(0, 1)
    merged["charge_drift"] = (merged["mean_charge"] - merged["charge"]).abs() / denom_charge
    max_charge_drift = float(merged["charge_drift"].max())
    assert max_charge_drift < 0.005, (
        f"max PUF charge drift {max_charge_drift:.6f} exceeds 0.5% threshold "
        f"(n={len(merged)} merged rows)"
    )

    # Payments
    denom_pay = merged["payment_amount"].abs().replace(0, 1)
    merged["payment_drift"] = (
        (merged["payment_amount"] - merged["allowed_payment"]).abs() / denom_pay
    )
    max_payment_drift = float(merged["payment_drift"].max())
    assert max_payment_drift < 0.005, (
        f"max PUF payment drift {max_payment_drift:.6f} exceeds 0.5% threshold "
        f"(n={len(merged)} merged rows)"
    )


# ============================================================
# Criterion 3: Smoke queries non-empty + credible ranges
# PHASES.md Phase 10 acceptance criterion #3
# ============================================================

_SMOKE_PATH = NEO4J_IMPORT / "_smoke_results.json"


@pytest.mark.skipif(not _SMOKE_PATH.exists(), reason="_smoke_results.json not present")
def test_acceptance_smoke_queries_credible_ranges():
    """Phase 10 acceptance: 4 sample queries return non-empty results in
    credible ranges. Re-asserts the per-query credibility ranges established
    in ``tests/test_neo4j_smoke.py`` over the cached ``_smoke_results.json``.
    """
    smoke = json.loads(_SMOKE_PATH.read_text())

    # Q1: I21.4 modal anchor — at least one 3-digit DRG, non-empty title
    q1 = smoke["q1_i214_modal"]
    assert len(q1) >= 1, "Q1 (I21.4 modal) returned no rows"
    for r in q1:
        drg = str(r["msdrg"])
        assert drg.isdigit() and len(drg) == 3, f"Q1 DRG {drg!r} not 3-digit numeric"
        assert r["drg.title"], "Q1 DRG title empty"

    # Q2: 27447 PFS facility — value 800-2000 USD plausible
    q2 = smoke["q2_27447_pfs"]
    assert len(q2) >= 1, "Q2 (CPT 27447 PFS) returned no rows"
    val = q2[0].get("obs.value")
    assert val is not None and 800 <= val <= 2000, (
        f"Q2 PFS value {val!r} out of credible range [800, 2000]"
    )

    # Q3: I21 anchors — >= 3 distinct DRGs reachable (I21.* MI family)
    q3 = smoke["q3_i21_anchors"]
    assert len(q3) >= 3, f"Q3 (I21 anchors) returned only {len(q3)} DRGs (need >=3)"
    for r in q3:
        drg = str(r["drg.code"])
        assert drg.isdigit() and len(drg) == 3, f"Q3 DRG {drg!r} not 3-digit numeric"

    # Q4: 410.41 GEMs forward-map — target starts with 'I' (ICD-10 ischemic)
    q4 = smoke["q4_41041_gems"]
    assert len(q4) >= 1, "Q4 (410.41 GEMs) returned no rows"
    assert str(q4[0]["tgt.code"]).startswith("I"), (
        f"Q4 GEMs target {q4[0]['tgt.code']!r} does not start with 'I'"
    )


# ============================================================
# Criterion 4: cohort_annotator coverage >= 0.95
# PHASES.md Phase 10 acceptance criterion #4
# ============================================================

_FIXTURE = ROOT / "tests" / "fixtures" / "sample_cohort.csv"


@pytest.mark.skipif(not _FIXTURE.exists(), reason="sample_cohort.csv not present")
@pytest.mark.skipif(
    not (GOLD / "code_costs.parquet").exists(),
    reason="data/gold/code_costs.parquet not built",
)
def test_acceptance_cohort_coverage_above_threshold():
    """Phase 10 acceptance: cohort_annotator coverage >= 0.95 on the
    expected_match=true subset of the sample fixture. Runs in parquet mode
    (always-on, no Neo4j required); live-bolt parity is enforced separately
    by ``tests/test_cohort_annotator.py``.
    """
    from src.query.cohort_annotator import annotate_cohort

    df = pd.read_csv(_FIXTURE)
    result = annotate_cohort(df, source="parquet", default_year=2024)

    em_col = result.df["expected_match"]
    if em_col.dtype == object:
        em = em_col.astype(str).str.lower().isin(["true", "1", "t"])
    else:
        em = em_col.astype(bool)

    em_true = result.df[em]
    n = len(em_true)
    n_resolved = int(em_true["annotation_status"].isin(["ok", "partial"]).sum())
    coverage = n_resolved / n if n else 0.0
    assert coverage >= 0.95, (
        f"cohort coverage on expected_match=true = {coverage:.3f} "
        f"({n_resolved}/{n}); PHASES.md acceptance is >= 0.95"
    )
