"""Tests for src/query/cohort_annotator.py.

Always-on (parquet mode, no Neo4j required):
  - Run annotator on tests/fixtures/sample_cohort.csv (32 rows)
  - Assert 19 cost columns appended in spec order (incl.
    inpatient_confidence and outpatient_confidence)
  - Assert coverage on expected_match=true subset >= 0.95 (>=29/30)
  - Assert P009/P010 route via GEMs Branch D to confidence=approximate
  - Assert P032 routes via Branch B back-translation to FY2017 cost
  - Assert P030 (X9999) and P031 (V9988) tag no_match
  - Assert no silent NaN in cost columns when annotation_status == 'ok'
  - Assert P001 (I214) inpatient_drg is in {'282','283','284'}

Live-bolt (gated by KG_COST_RUN_NEO4J=1 + reachable target):
  - Run same fixture with --source bolt
  - Assert same coverage
  - Assert bolt vs parquet numeric columns match within 0.5%
"""

from __future__ import annotations

import math
import os
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from src.query.cohort_annotator import AnnotationResult, annotate_cohort

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "sample_cohort.csv"
_CODE_COSTS = ROOT / "data" / "gold" / "code_costs.parquet"
_ICD_MODAL = ROOT / "data" / "gold" / "icd_principal_drg_modal.parquet"

_EXPECTED_OUTPUT_COLS = [
    "inpatient_drg",
    "inpatient_drg_title",
    "inpatient_allowed_mean",
    "inpatient_allowed_p10",
    "inpatient_allowed_p90",
    "inpatient_allowed_n",
    "inpatient_allowed_source",
    "inpatient_confidence",
    "inpatient_charge_mean",
    "inpatient_ccr_cost",
    "pfs_facility_allowed_total",
    "pfs_nonfacility_allowed_total",
    "outpatient_apc_allowed_total",
    "asc_allowed_total",
    "outpatient_confidence",
    "n_cpts_matched",
    "n_cpts_missing",
    "annotation_status",
    "annotation_notes",
]

# Skip parquet-mode tests if gold parquets aren't built yet (data-dependent).
_PARQUET_SKIP = pytest.mark.skipif(
    not (_CODE_COSTS.exists() and _ICD_MODAL.exists()),
    reason="data/gold/code_costs.parquet or icd_principal_drg_modal.parquet missing",
)


@pytest.fixture(scope="module")
def cohort_df() -> pd.DataFrame:
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"
    # Read with FY as numeric so the annotator's int-coercion path works
    df = pd.read_csv(FIXTURE)
    return df


@pytest.fixture(scope="module")
def parquet_result(cohort_df) -> AnnotationResult:
    if not (_CODE_COSTS.exists() and _ICD_MODAL.exists()):
        pytest.skip("gold parquets missing")
    return annotate_cohort(cohort_df.copy(), source="parquet", default_year=2024)


# ============================================================
# Always-on parquet-mode tests
# ============================================================


@_PARQUET_SKIP
def test_annotator_returns_32_rows(parquet_result):
    assert parquet_result.n_rows == 32
    assert len(parquet_result.df) == 32


@_PARQUET_SKIP
def test_19_output_columns_appended_in_spec_order(parquet_result, cohort_df):
    appended = list(parquet_result.df.columns)[len(cohort_df.columns):]
    assert appended == _EXPECTED_OUTPUT_COLS, (
        f"output cols mismatch.\n  got:      {appended}\n  expected: {_EXPECTED_OUTPUT_COLS}"
    )


@_PARQUET_SKIP
def test_inpatient_confidence_column_exists(parquet_result):
    """Session 2 (Phase 12 routing): inpatient_confidence is at index 7,
    enum subset of {high, approximate, none}, never NaN/None.
    """
    df = parquet_result.df
    assert "inpatient_confidence" in df.columns
    # Position check (after the cohort columns, the 8th appended col is index 7)
    appended = list(df.columns)[-len(_EXPECTED_OUTPUT_COLS):]
    assert appended[7] == "inpatient_confidence", (
        f"inpatient_confidence should be at appended-index 7; got {appended[7]!r}"
    )
    confidence = df["inpatient_confidence"]
    # Never null
    assert not confidence.isna().any(), "inpatient_confidence has NaN/None values"
    # Enum subset
    seen = set(confidence.astype(str).str.lower().unique())
    assert seen.issubset({"high", "approximate", "none"}), (
        f"inpatient_confidence outside enum {{high, approximate, none}}: {seen}"
    )


@_PARQUET_SKIP
def test_outpatient_confidence_column_exists(parquet_result):
    """Session 2 (Phase 12 routing): outpatient_confidence is at index 14
    (reserved-schema for Phase 8+; all None / NaN this phase).
    """
    df = parquet_result.df
    assert "outpatient_confidence" in df.columns
    appended = list(df.columns)[-len(_EXPECTED_OUTPUT_COLS):]
    assert appended[14] == "outpatient_confidence", (
        f"outpatient_confidence should be at appended-index 14; got {appended[14]!r}"
    )
    # All entries are None / NaN this phase
    col = df["outpatient_confidence"]
    # Accept either NaN (float-typed) or None (object). If object, all should be None.
    if col.dtype == object:
        assert all(v is None or (isinstance(v, float) and math.isnan(v)) for v in col), (
            f"outpatient_confidence should be all None/NaN this phase; sample: {col.head().tolist()}"
        )
    else:
        assert col.isna().all(), (
            f"outpatient_confidence should be all NaN this phase; sample: {col.head().tolist()}"
        )


def test_parquet_backend_gems_index_built():
    """ParquetBackend must build a populated GEMs index at construction.
    AMI subendocardial 41041 (ICD-9-CM) -> I2119 (ICD-10-CM) is a known
    forward-GEMs entry in gems_2018.parquet."""
    if not (_CODE_COSTS.exists() and _ICD_MODAL.exists()):
        pytest.skip("gold parquets missing")
    gems_path = ROOT / "data" / "silver" / "gems_2018.parquet"
    if not gems_path.exists():
        pytest.skip("data/silver/gems_2018.parquet missing")
    from src.query.cohort_annotator import _ParquetBackend
    backend = _ParquetBackend()
    assert len(backend._gems_index) > 1000, (
        f"GEMs index too small: {len(backend._gems_index)} entries; expected >=1000"
    )
    targets = backend.lookup_gems_targets("41041")
    assert len(targets) >= 1, "41041 should have >=1 ICD-10-CM forward target"
    target_codes = {t.get("target_code") for t in targets}
    assert any(t and t.startswith("I21") for t in target_codes), (
        f"41041 should map to ICD-10-CM AMI family (I21*); got {target_codes}"
    )


@_PARQUET_SKIP
def test_no_silent_nan_when_status_ok(parquet_result):
    """When annotation_status == 'ok' and a setting was resolved, at least
    one cost field for that setting must be non-NaN. We allow NaN in the
    *unresolved* settings (e.g. inpatient cols on a CPT-only row)."""
    df = parquet_result.df
    ok_rows = df[df.annotation_status == "ok"]
    assert len(ok_rows) > 0, "no ok rows — annotator produced no successful matches"
    for _, row in ok_rows.iterrows():
        # If inpatient_drg is set, at least one inpatient-cost cell should be populated
        if isinstance(row.inpatient_drg, str) and row.inpatient_drg:
            costs = [
                row.inpatient_allowed_mean,
                row.inpatient_charge_mean,
                row.inpatient_ccr_cost,
            ]
            populated = [c for c in costs if not (isinstance(c, float) and math.isnan(c))]
            assert populated, (
                f"row {row.patient_id}: inpatient_drg={row.inpatient_drg!r} "
                f"but all inpatient cost cells NaN (silent NaN)"
            )


@_PARQUET_SKIP
def test_p001_i214_routes_to_modal_drg_282_283_or_284(parquet_result):
    """P001 (I214 NSTEMI, FY2024, no PCS) must route via modal-DRG to AMI cluster."""
    df = parquet_result.df
    p001 = df[df.patient_id == "P001"].iloc[0]
    assert str(p001.inpatient_drg) in {"282", "283", "284"}, (
        f"P001 (I214) should route to AMI w/o MCC modal DRG; got {p001.inpatient_drg!r}"
    )


# ============================================================
# Grouper-integration tests (post-Session 11 follow-up)
# ============================================================
# These tests validate that PCS-bearing rows route via grouper.assign() to
# surgical DRGs (not the medical modal DRG). FY2024 Percutaneous
# Cardiovascular Procedure DRGs are {250, 251, 321, 322}; the original spec
# list {246, 247, 248} predates FY2008 and is no longer in the grouper's
# output set.

# FY2024 PCI surgical DRGs (Percutaneous Cardiovascular Procedures family).
_FY2024_PCI_DRGS = {"250", "251", "321", "322"}
# FY2024 TKA surgical DRGs (Major Hip and Knee Joint Replacement family).
_FY2024_TKA_DRGS = {"469", "470", "521", "522"}


@_PARQUET_SKIP
def test_p002_stemi_with_pci_routes_to_surgical_drg(parquet_result):
    """P002 has STEMI dx (I213) + PCI procedure (02703DZ); grouper should route
    to a Percutaneous Cardiovascular Procedure DRG, NOT the modal medical
    DRG 282 (AMI w/o MCC)."""
    df = parquet_result.df
    p002 = df[df.patient_id == "P002"].iloc[0]
    drg = str(p002.inpatient_drg).split(".")[0]  # strip pandas float suffix
    assert drg in _FY2024_PCI_DRGS, (
        f"P002 (STEMI + PCI) should route to surgical PCI DRG via grouper; "
        f"got {drg!r}, expected one of {_FY2024_PCI_DRGS}"
    )
    assert "drg_via_grouper" in str(p002.annotation_notes), (
        f"P002 notes should record grouper invocation; got {p002.annotation_notes!r}"
    )


@_PARQUET_SKIP
def test_p001_no_pcs_uses_modal_path(parquet_result):
    """P001 has no PCS; should use modal lookup (DRG 282/283/284), notes
    should NOT contain drg_via_grouper."""
    df = parquet_result.df
    p001 = df[df.patient_id == "P001"].iloc[0]
    drg = str(p001.inpatient_drg).split(".")[0]
    assert drg in {"282", "283", "284"}, (
        f"P001 (no PCS) should land on AMI modal DRG; got {drg!r}"
    )
    assert "drg_via_grouper" not in str(p001.annotation_notes), (
        f"P001 (no PCS) should NOT invoke grouper; notes={p001.annotation_notes!r}"
    )


@_PARQUET_SKIP
def test_p003_tka_routes_to_surgical_drg(parquet_result):
    """P003 (M1711 + 0SRC07Z TKA) should route to a Major Hip/Knee Joint
    Replacement DRG via the grouper."""
    df = parquet_result.df
    p003 = df[df.patient_id == "P003"].iloc[0]
    drg = str(p003.inpatient_drg).split(".")[0]
    assert drg in _FY2024_TKA_DRGS, (
        f"P003 (TKA) should route to TKA surgical DRG; got {drg!r}, "
        f"expected one of {_FY2024_TKA_DRGS}"
    )
    assert "drg_via_grouper" in str(p003.annotation_notes), (
        f"P003 notes should record grouper invocation; got {p003.annotation_notes!r}"
    )


# ---------------------------------------------------------------------------
# Session 5.5 FLAG 1 end-to-end annotator regression.
#
# Pre-fix: NTDB-shape FY2023 row with dotted ICD-10-CM principal diagnosis
# (`S02.2XXA`) propagated through `cohort_annotator.annotate_cohort()` ->
# `_invoke_grouper()` -> Java grouper, which rejected the dotted code and
# returned DRG 999. `_invoke_grouper` only filters '000', so '999' was
# treated as a real DRG; `lookup_drg_cost('999', fy)` resolved against the
# zero-cost reserved-DRG row in code_costs.parquet, leaving
# `inpatient_drg='999'` and `inpatient_allowed_mean=0.0`. See
# `plans/Session 5.5 FLAG 1 root cause.md`.
#
# Post-fix: `_normalize_code` strips the dot before invoking the grouper,
# so the code groups normally and the annotator pulls a real cost.
# ---------------------------------------------------------------------------


@_PARQUET_SKIP
def test_dotted_icd10cm_fy2023_routes_to_real_drg(tmp_path):
    """Synthetic NTDB-shape FY2023 row with dotted principal diagnosis
    (`S02.2XXA`, fracture of nasal bones) must NOT land on DRG 999. After
    the FLAG 1 fix, `_normalize_code` strips the embedded decimal before
    the grouper runs, so the row resolves to a real DRG (156, Other ENT
    Diagnoses w/o CC/MCC, per the dotless probe in the diagnostic memo)."""
    cohort = pd.DataFrame(
        [{
            "patient_id": "FLAG1_DOTTED_S02_2XXA",
            "dx_principal": "S02.2XXA",
            "dx_principal_system": "ICD-10-CM",
            "dx_secondary": "T07.XXXA",
            "pcs": "",
            "cpts": "",
            "sex": 1,
            "age": 70,
            "discharge_disp": "01",
            "fy": 2023,
        }]
    )
    result = annotate_cohort(cohort, source="parquet", default_year=2024)
    assert result.n_rows == 1
    row = result.df.iloc[0]
    drg = str(row.inpatient_drg).split(".")[0]
    assert drg not in {"999", "000", ""}, (
        f"dotted ICD-10-CM (S02.2XXA) FY2023 should NOT land on '999'/'000'/'' "
        f"after Session 5.5 FLAG 1 fix; got inpatient_drg={row.inpatient_drg!r}, "
        f"annotation_notes={row.annotation_notes!r}"
    )


@_PARQUET_SKIP
def test_p009_p010_icd9_routes_via_gems(parquet_result):
    """ICD-9 rows must route via GEMs Branch D to confidence='approximate'
    with source='gems_bridged_modal' and a 'gems_bridged:' note."""
    df = parquet_result.df
    for pid in ("P009", "P010"):
        row = df[df.patient_id == pid].iloc[0]
        assert row.annotation_status in ("ok", "partial"), (
            f"{pid} should resolve via GEMs Branch D, got status={row.annotation_status!r}, "
            f"notes={row.annotation_notes!r}"
        )
        assert str(row.inpatient_confidence) == "approximate", (
            f"{pid} should be inpatient_confidence='approximate'; "
            f"got {row.inpatient_confidence!r}"
        )
        assert str(row.inpatient_allowed_source) == "gems_bridged_modal", (
            f"{pid} should be inpatient_allowed_source='gems_bridged_modal'; "
            f"got {row.inpatient_allowed_source!r}"
        )
        assert "gems_bridged:" in str(row.annotation_notes), (
            f"{pid} notes should mention gems_bridged:; got {row.annotation_notes!r}"
        )


@_PARQUET_SKIP
def test_gems_bridged_drg_for_p009(parquet_result):
    """P009 (41041 ICD-9 acute MI subendocardial → I2119 → AMI modal DRG)
    must produce a 28x-family DRG and confidence='approximate'."""
    df = parquet_result.df
    p009 = df[df.patient_id == "P009"].iloc[0]
    drg = str(p009.inpatient_drg).split(".")[0]
    # AMI modal cluster (FY2019 modal for I2119 = 282; cost lookup at FY2014).
    assert drg in {"280", "281", "282", "283", "284"}, (
        f"P009 (41041 → I2119) should land on AMI cluster DRG; got {drg!r}"
    )
    assert str(p009.inpatient_confidence) == "approximate"
    assert str(p009.inpatient_allowed_source) == "gems_bridged_modal"


@_PARQUET_SKIP
def test_gems_bridged_drg_for_p010(parquet_result):
    """P010 (486 ICD-9 pneumonia → J189 → modal DRG 195) must produce
    DRG in pneumonia cluster and confidence='approximate'."""
    df = parquet_result.df
    p010 = df[df.patient_id == "P010"].iloc[0]
    drg = str(p010.inpatient_drg).split(".")[0]
    # Simple pneumonia cluster: 193/194/195 (Simple Pneumonia & Pleurisy).
    assert drg in {"193", "194", "195"}, (
        f"P010 (486 → J189) should land on simple pneumonia DRG; got {drg!r}"
    )
    assert str(p010.inpatient_confidence) == "approximate"
    assert str(p010.inpatient_allowed_source) == "gems_bridged_modal"


@_PARQUET_SKIP
def test_fy2017_back_translated_drg_lookup(parquet_result):
    """P032 (I213 STEMI, FY2017) must back-translate via FY2019 modal
    (DRG 282) and pull cost from FY2017."""
    df = parquet_result.df
    p032 = df[df.patient_id == "P032"].iloc[0]
    drg = str(p032.inpatient_drg).split(".")[0]
    assert drg in {"280", "281", "282", "283", "284"}, (
        f"P032 (I213 FY2017) should back-translate to AMI cluster; got {drg!r}"
    )
    assert str(p032.inpatient_confidence) == "approximate", (
        f"P032 should be confidence='approximate'; got {p032.inpatient_confidence!r}"
    )
    assert str(p032.inpatient_allowed_source) == "gems_bridged_back_translated", (
        f"P032 should be source='gems_bridged_back_translated'; "
        f"got {p032.inpatient_allowed_source!r}"
    )
    assert "gems_bridged_back_translated" in str(p032.annotation_notes), (
        f"P032 notes should mention gems_bridged_back_translated; "
        f"got {p032.annotation_notes!r}"
    )


@_PARQUET_SKIP
def test_confidence_high_for_native_fy2024_routing(parquet_result):
    """P001 (I214, FY2024, no PCS) routes via Branch A (native modal);
    confidence must be 'high'."""
    df = parquet_result.df
    p001 = df[df.patient_id == "P001"].iloc[0]
    assert str(p001.inpatient_confidence) == "high", (
        f"P001 (FY2024 native) should be confidence='high'; "
        f"got {p001.inpatient_confidence!r}"
    )


@_PARQUET_SKIP
def test_p030_bogus_icd_no_match(parquet_result):
    """X9999 (bogus ICD) must produce no_match with confidence='none'."""
    df = parquet_result.df
    p030 = df[df.patient_id == "P030"].iloc[0]
    assert p030.annotation_status == "no_match", (
        f"P030 (X9999 bogus) should be no_match; got {p030.annotation_status!r}"
    )
    assert str(p030.inpatient_confidence) == "none", (
        f"P030 should be confidence='none'; got {p030.inpatient_confidence!r}"
    )


@_PARQUET_SKIP
def test_gems_no_target_v_code(parquet_result):
    """P031 (V9988, V-code with zero GEMs forward entries) must tag
    gems_no_target and stay confidence='none'."""
    df = parquet_result.df
    p031 = df[df.patient_id == "P031"].iloc[0]
    assert p031.annotation_status == "no_match", (
        f"P031 (V-code) should be no_match; got {p031.annotation_status!r}"
    )
    assert str(p031.inpatient_confidence) == "none", (
        f"P031 should be confidence='none'; got {p031.inpatient_confidence!r}"
    )
    assert "gems_no_target" in str(p031.annotation_notes), (
        f"P031 notes should mention gems_no_target; got {p031.annotation_notes!r}"
    )


@_PARQUET_SKIP
def test_coverage_on_expected_match_true_subset(parquet_result):
    """≥0.95 coverage on the expected_match=true subset (≥29/30 after
    PHASES-2 Session 2 GEMs Branch D ship)."""
    df = parquet_result.df
    if "expected_match" not in df.columns:
        pytest.skip("fixture lacks expected_match column")
    em = df.expected_match
    if em.dtype == object:
        em = em.astype(str).str.lower().isin(["true", "1", "t"])
    em_true = df[em]
    n_true = len(em_true)
    assert n_true == 30, (
        f"expected_match=true denominator should be 30 (was 27 pre-Session 2); got {n_true}"
    )
    n_resolved = int(em_true.annotation_status.isin(["ok", "partial"]).sum())
    coverage = n_resolved / n_true
    assert coverage >= 0.95, (
        f"coverage on expected_match=true subset = {coverage:.3f} "
        f"({n_resolved}/{n_true}); need >= 0.95"
    )


@_PARQUET_SKIP
def test_total_coverage_at_least_85_percent(parquet_result):
    """All-rows coverage. With 2/32 expected misses, expected coverage ≈ 0.94."""
    assert parquet_result.coverage >= 0.85, (
        f"all-rows coverage {parquet_result.coverage:.3f} < 0.85"
    )


@_PARQUET_SKIP
def test_two_no_match_rows_exactly(parquet_result):
    """Exactly 2 rows tagged no_match after Session 2: P030 (X9999),
    P031 (V9988). P009/P010 now resolve via GEMs Branch D."""
    df = parquet_result.df
    no_match = df[df.annotation_status == "no_match"]
    pids = set(no_match.patient_id.astype(str))
    assert pids == {"P030", "P031"}, (
        f"expected no_match rows == {{P030, P031}}, got {pids}"
    )


# ============================================================
# Phase 13 sensitivity-floor tests (Batch 5; always-on, parquet-mode)
# ============================================================
# --confidence-floor + --scope Decision #3 in plan-v2-session2-confidence:
#   * floor='high' rejects approximate + none (the 3 GEMs-bridged + back-translated
#     fixture rows P009 / P010 / P032).
#   * Rejection nulls inpatient cost cols, resets inpatient_confidence to "none",
#     flips status to no_match, and appends `confidence_floor_exceeded` to notes.
#   * --scope='ip' (default) leaves outpatient/CPT totals untouched.
#   * --scope='all' also zeros the 4 outpatient totals (NOT NaN — 0.0).


@pytest.fixture(scope="module")
def parquet_result_floor_high(cohort_df) -> AnnotationResult:
    if not (_CODE_COSTS.exists() and _ICD_MODAL.exists()):
        pytest.skip("gold parquets missing")
    return annotate_cohort(
        cohort_df.copy(),
        source="parquet",
        default_year=2024,
        confidence_floor="high",
        scope="ip",
    )


@pytest.fixture(scope="module")
def parquet_result_floor_high_scope_all(cohort_df) -> AnnotationResult:
    if not (_CODE_COSTS.exists() and _ICD_MODAL.exists()):
        pytest.skip("gold parquets missing")
    return annotate_cohort(
        cohort_df.copy(),
        source="parquet",
        default_year=2024,
        confidence_floor="high",
        scope="all",
    )


@_PARQUET_SKIP
def test_confidence_floor_high_excludes_bridged_rows(
    parquet_result_floor_high, parquet_result
):
    """floor='high' rejects all GEMs-bridged + back-translated rows.

    P009 (ICD-9 41041 → AMI), P010 (ICD-9 486 → pneumonia), and P032
    (ICD-10 FY2017 back-translation) all carry confidence='approximate'
    in the default run; floor='high' must flip them to no_match. P001
    (FY2024 native, confidence='high') stays ok. The 9 inpatient cost
    cols on rejected rows must be nulled and inpatient_confidence reset
    to 'none'.
    """
    df = parquet_result_floor_high.df

    # Sanity: the default-run baseline has these three rows at confidence=approximate.
    base_df = parquet_result.df
    for pid in ("P009", "P010", "P032"):
        base = base_df[base_df.patient_id == pid].iloc[0]
        assert str(base.inpatient_confidence) == "approximate", (
            f"baseline pre-floor: {pid} should be approximate; "
            f"got {base.inpatient_confidence!r}"
        )

    for pid in ("P009", "P010", "P032"):
        row = df[df.patient_id == pid].iloc[0]
        assert row.annotation_status == "no_match", (
            f"floor=high should flip {pid} to no_match; "
            f"got status={row.annotation_status!r}"
        )
        assert "confidence_floor_exceeded" in str(row.annotation_notes), (
            f"{pid} notes should contain confidence_floor_exceeded; "
            f"got {row.annotation_notes!r}"
        )
        # Inpatient cost cols nulled. drg/drg_title/source -> empty string;
        # numerics -> NaN; confidence reset to "none".
        assert row.inpatient_drg in ("", None) or (
            isinstance(row.inpatient_drg, float) and math.isnan(row.inpatient_drg)
        ), f"{pid} inpatient_drg should be empty/NaN; got {row.inpatient_drg!r}"
        assert row.inpatient_drg_title in ("", None) or (
            isinstance(row.inpatient_drg_title, float)
            and math.isnan(row.inpatient_drg_title)
        ), (
            f"{pid} inpatient_drg_title should be empty/NaN; "
            f"got {row.inpatient_drg_title!r}"
        )
        assert math.isnan(row.inpatient_allowed_mean), (
            f"{pid} inpatient_allowed_mean should be NaN; "
            f"got {row.inpatient_allowed_mean!r}"
        )
        assert math.isnan(row.inpatient_charge_mean), (
            f"{pid} inpatient_charge_mean should be NaN; "
            f"got {row.inpatient_charge_mean!r}"
        )
        assert math.isnan(row.inpatient_ccr_cost), (
            f"{pid} inpatient_ccr_cost should be NaN; "
            f"got {row.inpatient_ccr_cost!r}"
        )
        assert str(row.inpatient_confidence) == "none", (
            f"{pid} inpatient_confidence should reset to 'none'; "
            f"got {row.inpatient_confidence!r}"
        )

    # P001 (high) stays ok.
    p001 = df[df.patient_id == "P001"].iloc[0]
    assert p001.annotation_status == "ok", (
        f"P001 (high) should stay ok under floor=high; got {p001.annotation_status!r}"
    )
    assert str(p001.inpatient_confidence) == "high"


@_PARQUET_SKIP
def test_confidence_floor_scope_ip_default_keeps_outpatient_costs(
    parquet_result_floor_high, parquet_result
):
    """Under --scope='ip' (the default), the rejection filter does NOT
    touch the 4 outpatient/CPT cost cols. They retain whatever value the
    per-row annotator assigned (NaN for rows with no CPTs; populated
    floats for rows with CPTs). Asserts identity to the default-run
    baseline for rejected rows AND for a known non-rejected CPT row.
    """
    df = parquet_result_floor_high.df
    base_df = parquet_result.df

    outpatient_cols = (
        "pfs_facility_allowed_total",
        "pfs_nonfacility_allowed_total",
        "outpatient_apc_allowed_total",
        "asc_allowed_total",
    )

    def _equal_or_both_nan(a, b) -> bool:
        a_nan = isinstance(a, float) and math.isnan(a)
        b_nan = isinstance(b, float) and math.isnan(b)
        if a_nan and b_nan:
            return True
        if a_nan ^ b_nan:
            return False
        return float(a) == float(b)

    # Rejected rows: outpatient totals identical to the default run
    # (i.e. --scope=ip leaves them alone).
    for pid in ("P009", "P010", "P032"):
        floor_row = df[df.patient_id == pid].iloc[0]
        base_row = base_df[base_df.patient_id == pid].iloc[0]
        for col in outpatient_cols:
            assert _equal_or_both_nan(floor_row[col], base_row[col]), (
                f"--scope=ip: {pid}.{col} should be unchanged from baseline; "
                f"floor={floor_row[col]!r} base={base_row[col]!r}"
            )

    # Non-rejected CPT row: outpatient totals identical to the default run.
    p017_floor = df[df.patient_id == "P017"].iloc[0]
    p017_base = base_df[base_df.patient_id == "P017"].iloc[0]
    for col in outpatient_cols:
        assert _equal_or_both_nan(p017_floor[col], p017_base[col]), (
            f"P017 (not rejected) {col} should be unchanged; "
            f"floor={p017_floor[col]!r} base={p017_base[col]!r}"
        )


@_PARQUET_SKIP
def test_confidence_floor_scope_all_zeros_outpatient(
    parquet_result_floor_high_scope_all,
):
    """Under --scope='all', rejected rows have the 4 outpatient/CPT
    totals zeroed (set to 0.0). The fixture's rejected rows (P009/P010/
    P032) have no CPTs so they're already 0.0; the assertion still
    pins the contract: not NaN, exactly 0.0.
    """
    df = parquet_result_floor_high_scope_all.df
    for pid in ("P009", "P010", "P032"):
        row = df[df.patient_id == pid].iloc[0]
        assert row.annotation_status == "no_match", (
            f"floor=high+scope=all: {pid} should be no_match; "
            f"got {row.annotation_status!r}"
        )
        for col in (
            "pfs_facility_allowed_total",
            "pfs_nonfacility_allowed_total",
            "outpatient_apc_allowed_total",
            "asc_allowed_total",
        ):
            v = row[col]
            assert not (isinstance(v, float) and math.isnan(v)), (
                f"--scope=all: {pid}.{col} should NOT be NaN; got {v!r}"
            )
            assert float(v) == 0.0, (
                f"--scope=all: {pid}.{col} expected 0.0; got {v!r}"
            )


def test_annotate_cohort_validates_confidence_floor():
    """Bad --confidence-floor value must raise ValueError early."""
    df = pd.DataFrame({"patient_id": ["X1"], "fy": [2024]})
    with pytest.raises(ValueError, match="confidence_floor"):
        annotate_cohort(df, source="parquet", confidence_floor="bogus")


def test_annotate_cohort_validates_scope():
    """Bad --scope value must raise ValueError early."""
    df = pd.DataFrame({"patient_id": ["X1"], "fy": [2024]})
    with pytest.raises(ValueError, match="scope"):
        annotate_cohort(df, source="parquet", scope="bogus")


# ============================================================
# Live-bolt tests (gated)
# ============================================================


def _docker_reachable() -> bool:
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _live_target_reachable() -> bool:
    try:
        from src.load._config import get_neo4j_config
        cfg = get_neo4j_config()
    except (ValueError, ImportError):
        return False
    if cfg.import_mode == "local-admin":
        return cfg.neo4j_home is not None and (cfg.neo4j_home / "bin/neo4j-admin").exists()
    if cfg.import_mode == "docker":
        return _docker_reachable()
    return False


_LIVE_SKIP_ENV = pytest.mark.skipif(
    os.getenv("KG_COST_RUN_NEO4J") != "1",
    reason="set KG_COST_RUN_NEO4J=1 to run live-bolt tests",
)
_LIVE_SKIP_TARGET = pytest.mark.skipif(
    not _live_target_reachable(),
    reason="Neo4j target not reachable",
)


@pytest.fixture(scope="module")
def bolt_result(cohort_df) -> AnnotationResult:
    if os.getenv("KG_COST_RUN_NEO4J") != "1" or not _live_target_reachable():
        pytest.skip("live-bolt prereqs not met")
    return annotate_cohort(cohort_df.copy(), source="bolt", default_year=2024)


@_LIVE_SKIP_ENV
@_LIVE_SKIP_TARGET
def test_bolt_mode_32_rows(bolt_result):
    assert bolt_result.n_rows == 32


@_LIVE_SKIP_ENV
@_LIVE_SKIP_TARGET
def test_bolt_coverage_on_expected_match_true_subset(bolt_result):
    df = bolt_result.df
    em = df.expected_match
    if em.dtype == object:
        em = em.astype(str).str.lower().isin(["true", "1", "t"])
    em_true = df[em]
    n_resolved = int(em_true.annotation_status.isin(["ok", "partial"]).sum())
    coverage = n_resolved / len(em_true) if len(em_true) else 0.0
    assert coverage >= 0.95, (
        f"bolt coverage on expected_match=true = {coverage:.3f}; need >= 0.95"
    )


@_LIVE_SKIP_ENV
@_LIVE_SKIP_TARGET
def test_bolt_parquet_agree_on_gems_bridge(bolt_result, parquet_result):
    """Branch D parity: GEMs-bridged ICD-9 rows must agree across backends.

    P009 (41041 → AMI cluster) and P010 (486 → simple pneumonia cluster)
    exercise the new bolt-side ``lookup_gems_targets`` + ``lookup_modal_drg``
    wired in Batch 4. P032 exercises Branch B back-translation (ICD-10
    + FY2017 → FY2019 modal). All three must produce identical
    DRG / confidence / source / cost values across the two backends.
    """
    p_df = parquet_result.df
    b_df = bolt_result.df
    for pid in ("P009", "P010", "P032"):
        p = p_df[p_df.patient_id == pid].iloc[0]
        b = b_df[b_df.patient_id == pid].iloc[0]
        assert str(p["inpatient_drg"]) == str(b["inpatient_drg"]), (
            f"{pid} DRG mismatch: parquet={p['inpatient_drg']!r} "
            f"vs bolt={b['inpatient_drg']!r}"
        )
        assert str(p["inpatient_confidence"]) == str(b["inpatient_confidence"]), (
            f"{pid} confidence mismatch: parquet={p['inpatient_confidence']!r} "
            f"vs bolt={b['inpatient_confidence']!r}"
        )
        assert str(p["inpatient_allowed_source"]) == str(b["inpatient_allowed_source"]), (
            f"{pid} source mismatch: parquet={p['inpatient_allowed_source']!r} "
            f"vs bolt={b['inpatient_allowed_source']!r}"
        )
        for col in ("inpatient_allowed_mean", "inpatient_charge_mean", "inpatient_ccr_cost"):
            pv = p[col]
            bv = b[col]
            pv_nan = isinstance(pv, float) and math.isnan(pv)
            bv_nan = isinstance(bv, float) and math.isnan(bv)
            if pv_nan and bv_nan:
                continue
            assert not (pv_nan ^ bv_nan), (
                f"{pid} {col}: NaN-mismatch parquet={pv!r} bolt={bv!r}"
            )
            assert abs(float(pv) - float(bv)) < 1e-6 or pv == bv, (
                f"{pid} {col} mismatch: parquet={pv!r} vs bolt={bv!r}"
            )


@_LIVE_SKIP_ENV
@_LIVE_SKIP_TARGET
def test_bolt_vs_parquet_parity(bolt_result, parquet_result):
    """Same input, two backends, same gold parquet — values must match within 0.5%."""
    b = bolt_result.df
    p = parquet_result.df
    assert b.shape == p.shape, f"shape mismatch: bolt {b.shape} vs parquet {p.shape}"
    numeric_cols = [
        "inpatient_allowed_mean",
        "inpatient_charge_mean",
        "inpatient_ccr_cost",
        "pfs_facility_allowed_total",
        "pfs_nonfacility_allowed_total",
        "outpatient_apc_allowed_total",
        "asc_allowed_total",
    ]
    for col in numeric_cols:
        bs = b[col].fillna(0).astype(float)
        ps = p[col].fillna(0).astype(float)
        diffs = (bs - ps).abs()
        denom = ps.where(ps != 0, 1).abs()
        rel_diffs = diffs / denom
        max_rel = float(rel_diffs.max()) if len(rel_diffs) else 0.0
        assert max_rel < 0.005, (
            f"{col}: max relative diff {max_rel:.6f} > 0.5% (bolt vs parquet)"
        )


# ---------------------------------------------------------------------------
# Batch 2 — _pick_best_gems_target / _load_gems_overrides unit tests
# (always-on; pure-Python; no parquet/bolt deps)
# ---------------------------------------------------------------------------


def _gems_row(
    target_code: str,
    *,
    no_map_flag: bool = False,
    approximate_flag: bool = False,
    combination_flag: bool = False,
    scenario: int = 1,
    choice_list: int = 1,
) -> dict:
    """Build a synthetic GEMs target row matching `_ParquetBackend._gems_index` shape."""
    return {
        "target_code": target_code,
        "no_map_flag": no_map_flag,
        "approximate_flag": approximate_flag,
        "combination_flag": combination_flag,
        "scenario": scenario,
        "choice_list": choice_list,
    }


@pytest.mark.parametrize(
    "case_name,targets,icd9,overrides,expected_target,expected_tier",
    [
        # --- override hit: short-circuits all GEMs logic
        (
            "override_hit",
            [_gems_row("I21.4")],
            "41001",
            {"41001": "I21.0"},
            "I21.0",
            "override",
        ),
        # --- Tier 0: every candidate is no_map -> None handled in dedicated test;
        # here we exercise the "all no_map -> None" path:
        # (covered by test_gems_no_map_returns_none below)
        # --- Tier 1: 3 candidates, only one with (scenario=1, choice_list=1)
        (
            "tier1_narrows",
            [
                _gems_row("J18.0", scenario=1, choice_list=1),
                _gems_row("J18.1", scenario=1, choice_list=2),
                _gems_row("J18.9", scenario=2, choice_list=1),
            ],
            "486",
            {},
            "J18.0",
            1,
        ),
        # --- Tier 2: among (s=1,cl=1), prefer combination_flag=False
        (
            "tier2_prefers_non_combination",
            [
                _gems_row("S72.001A", combination_flag=True),
                _gems_row("S72.002A", combination_flag=False),
            ],
            "82000",
            {},
            "S72.002A",
            2,
        ),
        # --- Tier 3: among single-target rows, prefer approximate_flag=False
        (
            "tier3_prefers_exact",
            [
                _gems_row("E11.9", approximate_flag=True),
                _gems_row("E11.8", approximate_flag=False),
            ],
            "25000",
            {},
            "E11.8",
            3,
        ),
        # --- Tier 4: longest target_code within same root (I21)
        (
            "tier4_longest_within_root",
            [
                _gems_row("I21.4"),
                _gems_row("I21.41"),
                _gems_row("I21.49"),
            ],
            "41071",
            {},
            # Two 5-char codes survive Tier 4 (I21.41, I21.49); Tier 5 lex-min picks I21.41
            "I21.41",
            5,
        ),
        # --- Tier 5: lex-min final tiebreak (all same length, same root)
        (
            "tier5_lex_min",
            [
                _gems_row("J18.9"),
                _gems_row("J18.1"),
                _gems_row("J18.0"),
            ],
            "486",
            {},
            "J18.0",
            5,
        ),
    ],
)
def test_pick_best_gems_target_tier_logic(
    case_name, targets, icd9, overrides, expected_target, expected_tier
):
    from src.query.cohort_annotator import _pick_best_gems_target

    result = _pick_best_gems_target(targets, icd9, overrides)
    assert result is not None, f"case={case_name}: expected hit, got None"
    assert result["target_code"] == expected_target, (
        f"case={case_name}: got target={result['target_code']!r}, "
        f"expected {expected_target!r}"
    )
    assert result["tier"] == expected_tier, (
        f"case={case_name}: got tier={result['tier']!r}, expected {expected_tier!r}"
    )


def test_gems_no_map_returns_none():
    """Single-candidate no_map_flag=True should return None (Tier 0)."""
    from src.query.cohort_annotator import _pick_best_gems_target

    targets = [_gems_row("NoDx", no_map_flag=True)]
    result = _pick_best_gems_target(targets, "V123", overrides={})
    assert result is None

    # Multiple candidates, all no_map -> still None
    targets_multi = [
        _gems_row("NoDx", no_map_flag=True),
        _gems_row("NoDx", no_map_flag=True),
    ]
    assert _pick_best_gems_target(targets_multi, "V123", overrides={}) is None


def test_gems_combination_truncated():
    """Multi-candidate combination_flag=True should expose truncated_n > 1."""
    from src.query.cohort_annotator import _pick_best_gems_target

    # All survive Tier 1 (s=1,cl=1) and all carry combination_flag=True;
    # Tier 2 won't shrink (no non-combination rows); Tier 3 won't shrink;
    # Tier 4/5 cascade picks the lex-min within the I21 root.
    targets = [
        _gems_row("I21.01", combination_flag=True),
        _gems_row("I21.02", combination_flag=True),
        _gems_row("I21.09", combination_flag=True),
    ]
    result = _pick_best_gems_target(targets, "41001", overrides={})
    assert result is not None
    assert result["truncated_n"] is not None and result["truncated_n"] > 1, (
        f"expected truncated_n > 1, got {result['truncated_n']!r}"
    )
    # First-by-tier-cascade should be a real I21 code (lex-min after
    # length tiebreak — all 5-char so lex-min wins)
    assert result["target_code"].startswith("I21")


def test_post_2018_icd10_tagged():
    """Empty `targets` (e.g., post-2018 ICD-10 code with no GEMs entry)
    must return None — defends the helper's empty-input behavior. The
    caller (_annotate_row) is responsible for tagging
    `icd10_post_gems_era` based on (FY>=2019 attempted GEMs lookup ->
    empty result); this test only pins the helper contract.
    """
    from src.query.cohort_annotator import _pick_best_gems_target

    # No matching GEMs rows for a hypothetical post-2018-introduced ICD-10
    assert _pick_best_gems_target([], "Z9999", overrides={}) is None
    # Override is consulted regardless of whether targets is empty
    result = _pick_best_gems_target(
        [], "Z9999", overrides={"Z9999": "Z99.89"}
    )
    assert result is not None and result["target_code"] == "Z99.89"
    assert result["tier"] == "override"


def test_load_gems_overrides_returns_dict():
    """Overrides loader should always return a dict (default empty)."""
    from src.query.cohort_annotator import _load_gems_overrides

    out = _load_gems_overrides()
    assert isinstance(out, dict)
    # File ships with `trauma: {}` only; flattening yields an empty mapping.
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in out.items())


# ---------------------------------------------------------------------------
# Session 3.5 FLAG 1 — ICD-9 decimal-form normalization tests
# (always-on; pure-Python except for the parquet-backed lookup tests)
# ---------------------------------------------------------------------------


@_PARQUET_SKIP
def test_gems_lookup_strips_decimal_icd9():
    """Decimal-form (`916.0`) and no-decimal-form (`9160`) inputs must
    return the same non-empty GEMs target list (Session 3.5 FLAG 1).

    GEMs silver keys ICD-9-CM source codes in no-decimal form; NTDB
    delivers them in decimal form. ``_normalize_icd9`` strips the
    decimal at lookup so both forms hit the same index entries.
    """
    from src.query.cohort_annotator import _ParquetBackend

    backend = _ParquetBackend()
    decimal_targets = backend.lookup_gems_targets("916.0")
    nodot_targets = backend.lookup_gems_targets("9160")

    assert nodot_targets, "9160 must have GEMs forward targets in gems_2018.parquet"
    assert decimal_targets == nodot_targets, (
        "decimal-form 916.0 must yield identical targets to no-decimal 9160; "
        f"got {len(decimal_targets)} vs {len(nodot_targets)} entries"
    )


@_PARQUET_SKIP
def test_gems_lookup_no_decimal_passthrough():
    """Regression guard: ``lookup_gems_targets("9160")`` (already
    no-decimal) must continue to return a non-empty list.

    Defends against accidentally over-aggressive normalization (e.g.,
    stripping more than just `.`).
    """
    from src.query.cohort_annotator import _ParquetBackend

    backend = _ParquetBackend()
    targets = backend.lookup_gems_targets("9160")
    assert targets, "9160 (no-decimal canonical form) must return GEMs targets"


def test_gems_override_decimal_form_keys():
    """Override YAML keys can be either `9160:` or `916.0:` — the
    lookup-side ``_normalize_icd9`` accepts both.

    Tests that ``_pick_best_gems_target`` consults overrides.get()
    after normalizing the input ICD-9, so override keys can use either
    form without dropping rows.
    """
    from src.query.cohort_annotator import _pick_best_gems_target

    # YAML key in no-decimal form, lookup with decimal-form input
    overrides_nodot = {"9160": "S72.001A"}
    result = _pick_best_gems_target(
        targets=[], icd9="916.0", overrides=overrides_nodot
    )
    assert result is not None
    assert result["target_code"] == "S72.001A"
    assert result["tier"] == "override"

    # YAML key in decimal form, lookup with no-decimal input
    overrides_decimal = {"916.0": "S72.001A"}
    result = _pick_best_gems_target(
        targets=[], icd9="9160", overrides=overrides_decimal
    )
    assert result is not None
    assert result["target_code"] == "S72.001A"
    assert result["tier"] == "override"

    # Unrelated key must NOT match (sanity check that normalization
    # doesn't accidentally collapse distinct codes)
    result_miss = _pick_best_gems_target(
        targets=[], icd9="916.0", overrides={"99999": "Z99.89"}
    )
    assert result_miss is None


@_LIVE_SKIP_ENV
@_LIVE_SKIP_TARGET
def test_bolt_parquet_parity_decimal_icd9():
    """Live-bolt parity: decimal-form ICD-9 (`916.0`) must yield the
    same target_code SET across both backends (Session 3.5 FLAG 1).

    Bolt-side ``_normalize_icd9`` is applied to both the cache key and
    the Cypher parameter; parquet-side ``_normalize_icd9`` is applied
    to the index lookup. Both paths must converge.
    """
    from src.query.cohort_annotator import _BoltBackend, _ParquetBackend

    parquet = _ParquetBackend()
    bolt = _BoltBackend()
    try:
        p_targets = parquet.lookup_gems_targets("916.0")
        b_targets = bolt.lookup_gems_targets("916.0")
        assert p_targets, "parquet: decimal-form 916.0 must yield GEMs targets"
        assert b_targets, "bolt: decimal-form 916.0 must yield GEMs targets"
        p_codes = {str(t.get("target_code")) for t in p_targets}
        b_codes = {str(t.get("target_code")) for t in b_targets}
        assert p_codes == b_codes, (
            f"target_code SET mismatch: parquet={sorted(p_codes)} "
            f"vs bolt={sorted(b_codes)}"
        )
    finally:
        bolt.close()


# ---------------------------------------------------------------------------
# Session 4 FLAG 3 — Branch B widened modal proxy (lex-min sweep)
# Closes the icd10_back_translation_no_modal gap for trauma S/T-prefix codes
# that have empty FY2019 modal but populate FY2020+. Cost lookup at the
# call site stays at the encounter's native FY; only modal DRG resolution
# is widened to the FY range [_INPATIENT_MIN_FY, _MODAL_MAX_FY].
# ---------------------------------------------------------------------------


class TestBranchBWidening:
    """Branch B back-translation now sweeps FY2019..FY2026 for the modal
    DRG (lex-min tie-break) and tags ``back_translation_modal_fy=<fy>`` in
    annotation_notes. Cost lookup remains at encounter native FY."""

    @_PARQUET_SKIP
    def test_branch_b_widening_resolves_trauma_st_codes(self):
        """End-to-end via the real parquet backend: an FY2017 cohort row
        with ICD-10 ``S02121A`` (skull fracture, occipital condyle, right,
        closed) must back-translate. ``S02121A`` has no FY2019 row in
        ``data/gold/icd_principal_drg_modal.parquet`` but a stable
        modal_drg ``087`` from FY2020 onward — pre-Session-4 this row
        would tag ``icd10_back_translation_no_modal``.
        """
        cohort = pd.DataFrame(
            [
                {
                    "patient_id": "TBW001",
                    "fy": 2017,
                    "age": 45,
                    "sex": "M",
                    "discharge_disp": 1,
                    "dx_principal": "S02121A",
                    "dx_principal_system": "ICD-10-CM",
                    "dx_secondary": "",
                    "pcs": "",
                    "cpts": "",
                }
            ]
        )
        result = annotate_cohort(cohort, source="parquet", default_year=2017)
        row = result.df.iloc[0]
        assert str(row.inpatient_confidence) == "approximate", (
            f"S02121A FY2017 should back-translate to confidence='approximate'; "
            f"got {row.inpatient_confidence!r} (notes: {row.annotation_notes!r})"
        )
        assert str(row.inpatient_allowed_source) == "gems_bridged_back_translated", (
            f"S02121A FY2017 should be source='gems_bridged_back_translated'; "
            f"got {row.inpatient_allowed_source!r}"
        )
        # The chosen modal DRG must be 087 (consistent FY2020-FY2026 modal).
        drg = str(row.inpatient_drg).split(".")[0]
        assert drg == "087", (
            f"S02121A back-translation should yield DRG 087 (consistent across "
            f"FY2020-FY2026 modal); got {drg!r}"
        )
        # The widening tag must record which FY proxied. FY2020 is the
        # first FY ascending that yields modal 087.
        notes = str(row.annotation_notes)
        assert "back_translation_modal_fy=2020" in notes, (
            f"Expected 'back_translation_modal_fy=2020' in notes; got {notes!r}"
        )
        # Cost lookup must remain at the encounter's native FY (2017).
        assert "fy=2017" in notes, (
            f"Cost lookup should be at native FY 2017; got {notes!r}"
        )

    def test_branch_b_lex_min_deterministic(self):
        """Pure-helper test against a stub backend. With FYs 2019/2020/2021
        returning ``["872", "871", "873"]`` and all other FYs ``None``,
        ``_lex_min_modal_drg`` must pick ``"871"`` (lex-min) and report
        ``hit_fy=2020`` (FIRST FY ascending that yielded ``"871"``).
        """
        from src.query.cohort_annotator import (
            _INPATIENT_MIN_FY,
            _MODAL_MAX_FY,
            _LookupBackend,
            _lex_min_modal_drg,
        )

        class _StubBackend(_LookupBackend):
            mode = "parquet"

            def __init__(self, plan: dict):
                self.plan = plan

            def lookup_modal_drg(self, icd_cm: str, fy: int):
                return self.plan.get(fy)

        stub = _StubBackend({2019: "872", 2020: "871", 2021: "873"})
        drg, hit_fy = _lex_min_modal_drg(stub, "X.YZ", _INPATIENT_MIN_FY, _MODAL_MAX_FY)
        assert (drg, hit_fy) == ("871", 2020), (
            f"lex-min over (872 @2019, 871 @2020, 873 @2021) should be "
            f"('871', 2020); got ({drg!r}, {hit_fy!r})"
        )

    def test_branch_b_no_modal_anywhere_still_no_match(self):
        """When NO FY in [2019, _MODAL_MAX_FY] yields a modal DRG, an
        FY2017 cohort row must remain ``no_match`` with confidence
        ``'none'`` and tag ``icd10_back_translation_no_modal``. Drives
        the annotator with a stub backend whose ``lookup_modal_drg``
        returns ``None`` for every (icd, fy)."""
        from src.query.cohort_annotator import (
            _LookupBackend,
            _OUTPUT_COLUMNS,
            _annotate_row,
        )

        class _EmptyBackend(_LookupBackend):
            mode = "parquet"

            def lookup_inpatient(self, icd_code, fy):
                from src.query.cohort_annotator import _InpatientCols

                return _InpatientCols()

            def lookup_drg_cost(self, drg, fy):
                from src.query.cohort_annotator import _InpatientCols

                return _InpatientCols()

            def lookup_cpt_setting(self, cpt, year, setting):
                return None

            def lookup_gems_targets(self, icd9):
                return []

            def lookup_modal_drg(self, icd_cm, fy):
                return None

        row = pd.Series(
            {
                "patient_id": "TBW003",
                "fy": 2017,
                "age": 50,
                "sex": "F",
                "discharge_disp": 1,
                "dx_principal": "S99999X",
                "dx_principal_system": "ICD-10-CM",
                "dx_secondary": "",
                "pcs": "",
                "cpts": "",
            }
        )
        out = _annotate_row(row, _EmptyBackend(), default_year=2017, fy_column="fy")
        # Sanity: shape preserved.
        for col in _OUTPUT_COLUMNS:
            assert col in out
        assert out["annotation_status"] == "no_match", (
            f"All-None modal sweep must remain no_match; got {out['annotation_status']!r}"
        )
        assert out["inpatient_confidence"] == "none", (
            f"All-None modal sweep must keep confidence='none'; got "
            f"{out['inpatient_confidence']!r}"
        )
        notes = str(out["annotation_notes"])
        assert "icd10_back_translation_no_modal" in notes, (
            f"Expected icd10_back_translation_no_modal tag; got {notes!r}"
        )
        # Negative: must NOT carry the widening tag (no FY proxied).
        assert "back_translation_modal_fy=" not in notes, (
            f"No FY should be tagged when sweep returns nothing; got {notes!r}"
        )

    @_PARQUET_SKIP
    def test_branch_b_widening_handles_dotted_icd10cm(self):
        """NTDB delivers ICD-10-CM codes with dots ("S02.121A"); the modal
        index parquet stores them dotless ("S02121A"). Pre-fix any dotted
        input MISSED the modal lookup unconditionally. Post-fix the
        ``_normalize_icd10cm`` helper strips the dot at lookup time.

        FY2017 cohort row with dotted ``S02.121A``: must back-translate to
        the same DRG (087) and ``back_translation_modal_fy=2020`` as the
        undotted variant.
        """
        cohort = pd.DataFrame(
            [
                {
                    "patient_id": "TBW004",
                    "fy": 2017,
                    "age": 45,
                    "sex": "M",
                    "discharge_disp": 1,
                    "dx_principal": "S02.121A",  # DOTTED form
                    "dx_principal_system": "ICD-10-CM",
                    "dx_secondary": "",
                    "pcs": "",
                    "cpts": "",
                }
            ]
        )
        result = annotate_cohort(cohort, source="parquet", default_year=2017)
        row = result.df.iloc[0]
        assert str(row.inpatient_confidence) == "approximate", (
            f"Dotted S02.121A FY2017 should back-translate to "
            f"confidence='approximate'; got {row.inpatient_confidence!r} "
            f"(notes: {row.annotation_notes!r})"
        )
        assert str(row.inpatient_allowed_source) == "gems_bridged_back_translated", (
            f"Dotted S02.121A FY2017 should be source="
            f"'gems_bridged_back_translated'; got {row.inpatient_allowed_source!r}"
        )
        drg = str(row.inpatient_drg).split(".")[0]
        assert drg == "087", (
            f"Dotted S02.121A back-translation should yield DRG 087; got {drg!r}"
        )
        notes = str(row.annotation_notes)
        assert "back_translation_modal_fy=2020" in notes, (
            f"Expected 'back_translation_modal_fy=2020' in notes; got {notes!r}"
        )

    def test_normalize_icd10cm_strips_dots(self):
        """Unit test for the new _normalize_icd10cm helper. Strips dots,
        uppercases, handles None and empty string defensively. Mirrors the
        Session 3.5 _normalize_icd9 contract."""
        from src.query.cohort_annotator import _normalize_icd10cm

        assert _normalize_icd10cm("S06.5X1A") == "S065X1A"
        assert _normalize_icd10cm("s065x1a") == "S065X1A"
        assert _normalize_icd10cm(None) == ""
        assert _normalize_icd10cm("") == ""

    @_PARQUET_SKIP
    def test_parquet_backend_lookup_modal_drg_normalizes_icd10cm(self):
        """The parquet backend's lookup_modal_drg must return identical
        results for the dotted and undotted forms of the same code, and
        both must resolve to a non-None DRG."""
        from src.query.cohort_annotator import _ParquetBackend

        backend = _ParquetBackend()
        dotted = backend.lookup_modal_drg("S02.121A", 2020)
        undotted = backend.lookup_modal_drg("S02121A", 2020)
        assert dotted == undotted, (
            f"Dotted and undotted lookups must agree: "
            f"dotted={dotted!r} undotted={undotted!r}"
        )
        assert dotted is not None, (
            f"S02.121A FY2020 must resolve to a non-None DRG; got {dotted!r}"
        )
