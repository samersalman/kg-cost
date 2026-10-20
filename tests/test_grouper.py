"""Phase 4 part 1 + 2 — V43.1 MS-DRG grouper smoke (10 hand-verified records).

Each ``_Case`` carries expected ``(ms_drg, mdc, severity_tier, or_flag)``
values determined empirically by routing the ``ClaimRecord`` through the
V43.1 grouper itself — the grouper is the canonical authority. These tests
pin ``version="V43.1"`` explicitly so they remain stable after Session 7's
default-routing switch to V*.0.

Skip when the V43.1 JAR working tree is absent. Run
``python3 -m src.extract.grouper --fy 2026 --version V43.1`` to populate.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from src import config
from src.grouper.grouper import (
    ClaimRecord,
    GrouperError,
    GrouperResult,
    _normalize_code,
    assign,
)


_JAR = config.grouper_jar_dir(2026, "V43.1") / "msdrg-core-43.1.0.0.jar"

pytestmark = pytest.mark.skipif(
    not _JAR.exists(),
    reason=(
        "V43.1 JAR working tree absent. Run "
        "`python3 -m src.extract.grouper --fy 2026 --version V43.1` first."
    ),
)


@dataclass(frozen=True)
class _Case:
    name: str
    record: ClaimRecord
    expected_ms_drg: str
    expected_mdc: str
    expected_severity_tier: str
    expected_or_flag: bool


# Heart transplant: DRG 002 (Pre-MDC range) reported under MDC 05, not "00".
_CASE_HEART_TRANSPLANT = _Case(
    name="pre_mdc_heart_transplant",
    record=ClaimRecord(
        pdx="I509", sdx=(), pcs=("02YA0Z0",), sex="M", age=55,
        discharge_disp="01", los=20,
    ),
    expected_ms_drg="002",
    expected_mdc="05",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=True,
)

# Hip replacement + J9601 (acute resp failure) MCC sdx promotes to DRG 469.
_CASE_SURG_WITH_MCC = _Case(
    name="surgical_hip_replacement_with_mcc",
    record=ClaimRecord(
        pdx="M1611", sdx=("J9601",), pcs=("0SR9019",), sex="F", age=72,
        discharge_disp="01", los=4,
    ),
    expected_ms_drg="469",
    expected_mdc="08",
    expected_severity_tier="with_mcc",
    expected_or_flag=True,
)

# Same hip replacement minus the MCC sdx → DRG 470.
_CASE_SURG_WITHOUT_MCC = _Case(
    name="surgical_hip_replacement_without_mcc",
    record=ClaimRecord(
        pdx="M1611", sdx=(), pcs=("0SR9019",), sex="F", age=72,
        discharge_disp="01", los=2,
    ),
    expected_ms_drg="470",
    expected_mdc="08",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=True,
)

# Sepsis pdx + J9601 sdx → DRG 871 (medical, MDC 18).
_CASE_MED_WITH_MCC = _Case(
    name="medical_sepsis_with_mcc",
    record=ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=7,
    ),
    expected_ms_drg="871",
    expected_mdc="18",
    expected_severity_tier="with_mcc",
    expected_or_flag=False,
)

# Sepsis pdx alone → DRG 872 (Step-3 smoke baseline).
_CASE_MED_WITHOUT_MCC = _Case(
    name="medical_sepsis_without_mcc",
    record=ClaimRecord(
        pdx="A419", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    ),
    expected_ms_drg="872",
    expected_mdc="18",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=False,
)

# Newborn (MDC 15) — pdx Z3800 = normal liveborn vaginal. Exercises the
# age_days_admit / age_days_discharge contract widening → DRG 795.
_CASE_NEWBORN_NORMAL = _Case(
    name="newborn_normal_liveborn",
    record=ClaimRecord(
        pdx="Z3800", sdx=(), pcs=(), sex="M", age=0,
        discharge_disp="01", los=2,
        age_days_admit=0, age_days_discharge=2,
    ),
    expected_ms_drg="795",
    expected_mdc="15",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=False,
)

# Newborn w/ respiratory distress (P220) → DRG 790 (higher-acuity neonatal).
_CASE_NEWBORN_RDS = _Case(
    name="newborn_respiratory_distress",
    record=ClaimRecord(
        pdx="P220", sdx=(), pcs=(), sex="M", age=0,
        discharge_disp="01", los=10,
        age_days_admit=0, age_days_discharge=10,
    ),
    expected_ms_drg="790",
    expected_mdc="15",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=False,
)

# ECMO continuous (5A1522F) → DRG 003 (Pre-MDC range), MDC reported as 04
# because pdx J9601 is an MDC-04 condition; Pre-MDC = DRG-range, not MDC=00.
_CASE_ECMO = _Case(
    name="ecmo_pre_mdc",
    record=ClaimRecord(
        pdx="J9601", sdx=(), pcs=("5A1522F",), sex="M", age=55,
        discharge_disp="01", los=10,
    ),
    expected_ms_drg="003",
    expected_mdc="04",
    expected_severity_tier="without_cc_mcc",
    expected_or_flag=True,
)

# Same medical-MCC case but discharge_disp="62" (IRF transfer). V43.1 returns
# the SAME DRG as disp="01" — post-acute transfer is a payment-side
# adjustment, not a DRG-routing one. Locks plumbing + non-routing behaviour.
_CASE_DISP_TRANSFER = _Case(
    name="medical_disp_irf_transfer_same_drg",
    record=ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="62", los=7,
    ),
    expected_ms_drg="871",
    expected_mdc="18",
    expected_severity_tier="with_mcc",
    expected_or_flag=False,
)

# Discharge disposition "20" (died), sex="F" — round-trips through the
# wrapper's sex/disp mapping; medical-MCC routing → DRG 871.
_CASE_DISP_DIED = _Case(
    name="medical_disp_died",
    record=ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="F", age=68,
        discharge_disp="20", los=3,
    ),
    expected_ms_drg="871",
    expected_mdc="18",
    expected_severity_tier="with_mcc",
    expected_or_flag=False,
)


_CASES: tuple[_Case, ...] = (
    _CASE_HEART_TRANSPLANT,
    _CASE_SURG_WITH_MCC,
    _CASE_SURG_WITHOUT_MCC,
    _CASE_MED_WITH_MCC,
    _CASE_MED_WITHOUT_MCC,
    _CASE_NEWBORN_NORMAL,
    _CASE_NEWBORN_RDS,
    _CASE_ECMO,
    _CASE_DISP_TRANSFER,
    _CASE_DISP_DIED,
)


@pytest.mark.parametrize("case", _CASES, ids=[c.name for c in _CASES])
def test_grouper_record(case: _Case) -> None:
    """Each fixture record matches its hand-verified DRG / MDC / severity / OR."""
    result = assign(case.record, 2026, version="V43.1")
    assert result.ms_drg == case.expected_ms_drg, (
        f"{case.name}: ms_drg expected {case.expected_ms_drg!r} got {result.ms_drg!r}"
    )
    assert result.mdc == case.expected_mdc, (
        f"{case.name}: mdc expected {case.expected_mdc!r} got {result.mdc!r}"
    )
    assert result.severity_tier == case.expected_severity_tier, (
        f"{case.name}: severity expected {case.expected_severity_tier!r} "
        f"got {result.severity_tier!r}"
    )
    assert result.or_procedure_flag == case.expected_or_flag, (
        f"{case.name}: or_procedure_flag expected {case.expected_or_flag} "
        f"got {result.or_procedure_flag}"
    )
    assert result.sdc_indicators.get("final_grc") == "OK", (
        f"{case.name}: final_grc expected 'OK' got "
        f"{result.sdc_indicators.get('final_grc')!r}"
    )


def test_grouper_invalid_pcs_raises() -> None:
    """Wrapper-side guard: PCS != 7 chars rejected before the grouper runs."""
    record = ClaimRecord(
        pdx="A419", sdx=(), pcs=("ABC",), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    with pytest.raises(GrouperError, match="(?i)pcs"):
        assign(record, 2026, version="V43.1")


def test_grouper_fy_out_of_scope_rejected() -> None:
    """FY < 2008 is the CMS-DRG era (out of scope per PHASES.md year tiers).
    The wrapper rejects any FY not in ``GROUPER_VERSIONS_BY_FY``."""
    record = ClaimRecord(
        pdx="A419", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    with pytest.raises(GrouperError, match="(?i)not in GROUPER_VERSIONS_BY_FY"):
        assign(record, 2007)


def test_grouper_explicit_version_override_must_be_in_mapping() -> None:
    """Caller-supplied ``version`` must be one of ``GROUPER_VERSIONS_BY_FY[fy]``.
    Locks the routing surface against silent fallbacks."""
    record = ClaimRecord(
        pdx="A419", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    with pytest.raises(GrouperError, match="(?i)not in GROUPER_VERSIONS_BY_FY"):
        assign(record, 2026, version="V99.9")


def test_grouper_pre_v381_fy_rejected() -> None:
    """FY2008-FY2022 + FY2018 (V35) lack a Java MS-DRG grouper. Per the Phase 4
    part 2 scope reduction (Session 7, PI-approved 2026-04-30), these FYs are
    in ``GROUPER_VERSIONS_BY_FY`` with an empty tuple to signal "no Java
    grouper available". The wrapper raises a clear message pointing at the
    drgpy fallback / formula-only Phase 7 path."""
    record = ClaimRecord(
        pdx="A419", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    with pytest.raises(GrouperError, match="(?i)no Java MS-DRG grouper"):
        assign(record, 2018)


def test_grouper_returns_dataclass_shape() -> None:
    """Defends the public API surface against silent regressions."""
    result = assign(_CASES[0].record, 2026, version="V43.1")
    assert isinstance(result, GrouperResult)
    assert isinstance(result.sdc_indicators, dict)
    expected_indicator_keys = {
        "initial_drg",
        "initial_mdc",
        "initial_severity",
        "final_grc",
        "initial_grc",
        "hac_status",
        "num_hac_categories",
        "final_base_drg",
        "initial_base_drg",
    }
    assert expected_indicator_keys.issubset(set(result.sdc_indicators.keys())), (
        f"sdc_indicators missing keys: "
        f"{expected_indicator_keys - set(result.sdc_indicators.keys())}"
    )


def test_poa_n_drives_hac_eligibility() -> None:
    """POA=N on a HAC-list secondary dx (T83.511A, catheter-associated UTI)
    flips ``sdc_indicators['hac_status']`` away from ``NOT_APPLICABLE`` and
    increments ``num_hac_categories``. POA=Y on the same code yields
    ``NOT_APPLICABLE`` (present-on-admission disqualifies HAC)."""
    record_y = ClaimRecord(
        pdx="A419", sdx=("T83511A",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5, poa=("Y", "Y"),
    )
    record_n = ClaimRecord(
        pdx="A419", sdx=("T83511A",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5, poa=("Y", "N"),
    )
    r_y = assign(record_y, 2026, version="V43.1")
    r_n = assign(record_n, 2026, version="V43.1")
    assert r_y.sdc_indicators["hac_status"] == "NOT_APPLICABLE"
    assert r_y.sdc_indicators["num_hac_categories"] == 0
    assert r_n.sdc_indicators["hac_status"] != "NOT_APPLICABLE", (
        f"POA=N should engage HAC; got hac_status="
        f"{r_n.sdc_indicators['hac_status']!r}"
    )
    assert r_n.sdc_indicators["num_hac_categories"] >= 1, (
        f"POA=N should report >=1 HAC category; got "
        f"{r_n.sdc_indicators['num_hac_categories']}"
    )


def test_poa_w_exempt_does_not_route() -> None:
    """POA=W on a POA-exempt-list dx (congenital Q-code) leaves grouping
    unchanged vs POA=Y. Defends the POA wiring against accidental routing
    side-effects on exempt codes."""
    record_y = ClaimRecord(
        pdx="I509", sdx=("Q249",), pcs=(), sex="M", age=55,
        discharge_disp="01", los=3, poa=("Y", "Y"),
    )
    record_w = ClaimRecord(
        pdx="I509", sdx=("Q249",), pcs=(), sex="M", age=55,
        discharge_disp="01", los=3, poa=("Y", "W"),
    )
    r_y = assign(record_y, 2026, version="V43.1")
    r_w = assign(record_w, 2026, version="V43.1")
    assert r_y.ms_drg == r_w.ms_drg
    assert r_y.mdc == r_w.mdc
    assert r_y.severity_tier == r_w.severity_tier


def test_poa_unknown_raises() -> None:
    """Unknown POA values are rejected at the Python boundary before any
    grouper call. Locks the validator against a regression."""
    record = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5, poa=("Y", "Q"),
    )
    with pytest.raises(GrouperError, match="invalid POA: Q"):
        assign(record, 2026, version="V43.1")


def test_strict_raises_on_gfc_build_failure(tmp_path, monkeypatch) -> None:
    """When ``strict=True`` and both Maven and javac fallback fail to build
    gfc-base-api, ``_build_gfc_base_api`` raises ``RuntimeError`` instead of
    the default stderr-warn-and-return-None path."""
    from src.extract import grouper as extract_grouper

    vendor_dir = tmp_path / "vendor"  # empty -> bypass already-vendored skip
    vendor_dir.mkdir()
    fake_gfc_root = tmp_path / "gfc_root"
    fake_gfc_root.mkdir()

    monkeypatch.setattr(
        extract_grouper, "_unzip_gfc_source",
        lambda gfc_zip, work_root: fake_gfc_root,
    )
    monkeypatch.setattr(
        extract_grouper, "_check_mvn_available", lambda: True,
    )
    monkeypatch.setattr(
        extract_grouper, "_build_gfc_with_mvn",
        lambda gfc_root, target_jar: False,
    )
    monkeypatch.setattr(
        extract_grouper, "_build_gfc_with_javac",
        lambda gfc_root, target_jar: False,
    )

    with pytest.raises(RuntimeError, match=r"GFC build failed under --strict"):
        extract_grouper._build_gfc_base_api(
            gfc_zip=tmp_path / "fake.zip",
            vendor_dir=vendor_dir,
            work_root=tmp_path / "work",
            strict=True,
        )


def test_grouper_disp_transfer_matches_disp_home() -> None:
    """``discharge_disp`` is plumbed through but does NOT alter DRG routing
    in V43.1 — post-acute transfer is a payment-side adjustment, not a
    DRG-routing one. Locks this surprising-but-correct behaviour against
    regression."""
    home = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=7,
    )
    irf = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="62", los=7,
    )
    r_home = assign(home, 2026, version="V43.1")
    r_irf = assign(irf, 2026, version="V43.1")
    assert r_home.ms_drg == r_irf.ms_drg, (
        "discharge_disp 01 vs 62 should yield the same DRG in V43.1 "
        f"(payment-side adjustment, not DRG-routing); "
        f"got {r_home.ms_drg} vs {r_irf.ms_drg}"
    )


# ---------------------------------------------------------------------------
# Per-version smoke (Phase 4 part 2 Step 6) -- canonical sepsis+MCC and hip
# arthroplasty cases, parametrized across all V40+ versions. Locks the
# reflection-based driver against per-version dispatch regressions.
#
# DRG 871 (sepsis + acute resp failure MCC) and DRG 470 (uncomplicated hip
# replacement) are version-stable across V40-V43; these tests fail only on
# driver-routing breakage, not on inter-version DRG-definition drift.
# ---------------------------------------------------------------------------


_PER_VERSION_VENDORED_VERSIONS: tuple[tuple[int, str], ...] = tuple(
    (fy, ver)
    for fy, versions in config.GROUPER_VERSIONS_BY_FY.items()
    for ver in versions
)


@pytest.mark.parametrize(
    ("fy", "version"),
    _PER_VERSION_VENDORED_VERSIONS,
    ids=[f"{fy}-{v}" for fy, v in _PER_VERSION_VENDORED_VERSIONS],
)
def test_per_version_sepsis_with_mcc(fy: int, version: str) -> None:
    """Sepsis (A419) + acute respiratory failure (J9601) -> DRG 871, MDC 18,
    with_mcc. Stable across V40-V43 per CMS Definitions Manual."""
    jar_dir = config.grouper_jar_dir(fy, version)
    if not jar_dir.is_dir():
        pytest.skip(f"{version} JARs not vendored at {jar_dir}")
    record = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    result = assign(record, fy, version=version)
    assert result.ms_drg == "871", (
        f"{version}: sepsis+MCC expected DRG 871, got {result.ms_drg}"
    )
    assert result.mdc == "18"
    assert result.severity_tier == "with_mcc"


@pytest.mark.parametrize(
    ("fy", "version"),
    _PER_VERSION_VENDORED_VERSIONS,
    ids=[f"{fy}-{v}" for fy, v in _PER_VERSION_VENDORED_VERSIONS],
)
def test_per_version_hip_arthroplasty(fy: int, version: str) -> None:
    """Hip osteoarthritis (M1611) + total hip arthroplasty (0SR9019) -> DRG
    470, MDC 08, without_cc_mcc, OR-flag True. Stable across V40-V43."""
    jar_dir = config.grouper_jar_dir(fy, version)
    if not jar_dir.is_dir():
        pytest.skip(f"{version} JARs not vendored at {jar_dir}")
    record = ClaimRecord(
        pdx="M1611", sdx=(), pcs=("0SR9019",), sex="F", age=72,
        discharge_disp="01", los=4,
    )
    result = assign(record, fy, version=version)
    assert result.ms_drg == "470", (
        f"{version}: hip-replacement expected DRG 470, got {result.ms_drg}"
    )
    assert result.mdc == "08"
    assert result.or_procedure_flag is True


# ---------------------------------------------------------------------------
# Session 5.5 FLAG 1 regression tests — ICD-10-CM dotted-input handling.
#
# Diagnostic: `plans/Session 5.5 FLAG 1 root cause.md`. The Java MS-DRG
# grouper (V40+) rejects dotted ICD-10-CM input as invalid and returns DRG
# 999 ("UNGROUPABLE"). NTDB delivers dotted codes (`S02.2XXA`); the original
# `_normalize_code` upper-cased and stripped whitespace but did NOT strip
# the decimal point, despite the `ClaimRecord` docstring contracting "no
# decimal point". The patch (`src/grouper/grouper.py:_normalize_code`) adds
# `.replace(".", "")` so both Java and drgpy code paths see canonical
# dotless input. drgpy was already dot-tolerant (and does its own dot
# stripping in `drgpy_wrapper._normalize_code`), so the FY2019-FY2022
# fallback path is unaffected by the fix but still pinned below as a
# regression guard.
# ---------------------------------------------------------------------------


def test_normalize_code_strips_decimal_dot() -> None:
    """`_normalize_code` strips embedded decimals, upper-cases, and trims
    whitespace. Dotted ICD-10-CM (`S02.2XXA` from NTDB) round-trips to the
    dotless form (`S022XXA`) the Java grouper expects. Already-dotless input
    passes through unchanged."""
    assert _normalize_code("S02.2XXA") == "S022XXA"
    assert _normalize_code("s02.2xxa") == "S022XXA"
    assert _normalize_code("  S02.2XXA  ") == "S022XXA"
    # Already-dotless input passes through unchanged.
    assert _normalize_code("S022XXA") == "S022XXA"
    # PCS is always dotless in canonical form; the no-op stays a no-op.
    assert _normalize_code("0SR9019") == "0SR9019"
    # ICD-9-CM dotted form ("486" pneumonia, "410.41" subendocardial AMI) —
    # no Java grouper sees ICD-9 (FY2008-FY2018 routes elsewhere), but the
    # normalizer must not mangle the dotless mapping anyway.
    assert _normalize_code("410.41") == "41041"


def test_grouper_dotted_icd10_cm_groups_correctly() -> None:
    """FY2023 NTDB-shape record with a dotted ICD-10-CM principal diagnosis
    (`S02.2XXA`, fracture of nasal bones, initial encounter) groups to a
    real DRG instead of returning DRG 999 "UNGROUPABLE".

    Pre-fix (per `plans/Session 5.5 FLAG 1 root cause.md`):
      - Dotted `S02.2XXA` → Java grouper → ms_drg=999.
    Post-fix:
      - `_normalize_code` strips the dot → `S022XXA` → ms_drg=156
        (Other ENT Diagnoses w/o CC/MCC).
    """
    record = ClaimRecord(
        pdx="S02.2XXA", sdx=("T07.XXXA",), pcs=(), sex="M", age=70,
        discharge_disp="01", los=2,
    )
    result = assign(record, 2023)
    assert result.ms_drg != "999", (
        f"dotted ICD-10-CM input should NOT return DRG 999 (UNGROUPABLE) "
        f"after the Session 5.5 FLAG 1 fix; got ms_drg={result.ms_drg!r}"
    )
    assert result.ms_drg != "000", (
        f"grouper returned ungroupable sentinel '000'; "
        f"got ms_drg={result.ms_drg!r}"
    )


def test_grouper_fy2022_drgpy_fallback_unchanged() -> None:
    """drgpy fallback (FY2019-FY2022) stays dot-tolerant. drgpy's own
    ``_normalize_code`` already strips decimals, so the Session 5.5 fix to
    ``grouper.py:_normalize_code`` is a no-op on this code path. Pin a
    representative trauma record (humerus fracture, FY2022) to defend
    against accidental cross-fallout from the Java-side patch.

    Routing per `_DRGPY_FALLBACK_FYS = {2019, 2020, 2021, 2022}` at
    ``grouper.py:149``."""
    record = ClaimRecord(
        pdx="S42.471A", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=4,
    )
    result = assign(record, 2022)
    assert result.ms_drg != "999", (
        f"FY2022 drgpy fallback should not surface DRG 999; "
        f"got ms_drg={result.ms_drg!r}"
    )
    assert result.ms_drg != "000", (
        f"FY2022 drgpy returned ungroupable sentinel '000'; "
        f"got ms_drg={result.ms_drg!r}"
    )
    # Sanity: drgpy returns a 3-digit zero-padded DRG (per
    # `drgpy_wrapper.py:220` `str(drg).zfill(3)`).
    assert len(result.ms_drg) == 3 and result.ms_drg.isdigit(), (
        f"FY2022 drgpy DRG should be 3-digit zero-padded; "
        f"got {result.ms_drg!r}"
    )
