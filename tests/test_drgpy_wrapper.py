"""Phase 7b Session 9.5 -- drgpy MS-DRG wrapper unit tests.

Mirrors the contract checked by ``tests/test_grouper.py`` for the Java
wrapper. Validates:

  - canonical hand-verified DRG round-trips per drgpy version (v36..v40)
  - ClaimRecord/GrouperResult contract parity (ms_drg/mdc/severity/or_flag)
  - dispatcher routing in ``src.grouper.grouper.assign`` for FY2019..FY2022
  - pre-MDC DRG MDC-from-features fallback (heart transplant)
  - newborn (P-prefix) MDC-15 routing
  - explicit ``v40`` drgpy override for FY2023 cross-check
  - error surfaces (missing pdx, unknown FY)

Skips cleanly when drgpy is not installed.
"""

from __future__ import annotations

import pytest

from src.grouper.drgpy_wrapper import (
    _FY_TO_DRGPY_VERSION,
    _normalize_code,
    assign as drgpy_assign,
)
from src.grouper.grouper import (
    ClaimRecord,
    GrouperError,
    GrouperResult,
    assign as grouper_assign,
)


# ---------------------------------------------------------------------------
# Skip predicate
# ---------------------------------------------------------------------------


def _drgpy_importable() -> bool:
    try:
        import drgpy.msdrg  # noqa: F401
        return True
    except Exception:  # pragma: no cover -- environment-dependent
        return False


pytestmark = pytest.mark.skipif(
    not _drgpy_importable(),
    reason="drgpy not installed; install with `uv pip install drgpy`.",
)


# ---------------------------------------------------------------------------
# 1. FY -> drgpy version map sanity
# ---------------------------------------------------------------------------


def test_fy_to_drgpy_version_map_locked() -> None:
    """The FY->drgpy mapping must cover FY2019..FY2023 inclusive."""
    assert _FY_TO_DRGPY_VERSION == {
        2019: "v36",
        2020: "v37",
        2021: "v38",
        2022: "v39",
        2023: "v40",
    }


def test_unknown_fy_raises() -> None:
    rec = ClaimRecord(pdx="A419", sdx=(), pcs=(), sex="M", age=70,
                      discharge_disp="01", los=5)
    with pytest.raises(GrouperError, match="not in drgpy coverage"):
        drgpy_assign(rec, 2018)
    with pytest.raises(GrouperError, match="not in drgpy coverage"):
        drgpy_assign(rec, 2027)


def test_empty_pdx_raises() -> None:
    rec = ClaimRecord(pdx="", sdx=(), pcs=(), sex="M", age=70,
                      discharge_disp="01", los=5)
    with pytest.raises(GrouperError, match="pdx required"):
        drgpy_assign(rec, 2020)


# ---------------------------------------------------------------------------
# 2. Code normalization
# ---------------------------------------------------------------------------


def test_normalize_strips_dot_and_uppercases() -> None:
    assert _normalize_code("i21.4") == "I214"
    assert _normalize_code("  m16.11 ") == "M1611"
    assert _normalize_code("0SR9019") == "0SR9019"
    assert _normalize_code("") == ""
    assert _normalize_code(None) == ""  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 3. Canonical hand-verified round-trips per FY/drgpy version
# ---------------------------------------------------------------------------


# Each: (fy, pdx, sdx, pcs, sex, age, expected_drg, expected_mdc,
#        expected_severity, expected_or_flag, label).
_CANONICAL: list[tuple] = [
    # Sepsis A419 + J9601 MCC -> DRG 871 stable v36..v40 (mdc 18, with_mcc).
    (2019, "A419", ("J9601",), (), "M", 70, "871", "18", "with_mcc", False, "sepsis_with_mcc_v36"),
    (2020, "A419", ("J9601",), (), "M", 70, "871", "18", "with_mcc", False, "sepsis_with_mcc_v37"),
    (2021, "A419", ("J9601",), (), "M", 70, "871", "18", "with_mcc", False, "sepsis_with_mcc_v38"),
    (2022, "A419", ("J9601",), (), "M", 70, "871", "18", "with_mcc", False, "sepsis_with_mcc_v39"),
    # Hip replacement (no MCC) -> DRG 470 stable, MDC 08, surgical.
    (2019, "M1611", (), ("0SR9019",), "F", 72, "470", "08", "without_cc_mcc", True, "hip_v36"),
    (2022, "M1611", (), ("0SR9019",), "F", 72, "470", "08", "without_cc_mcc", True, "hip_v39"),
    # AMI (medical) -> DRG 282 (no CC/MCC) baseline.
    (2020, "I214", (), (), "M", 65, "282", "05", "without_cc_mcc", False, "ami_v37"),
    # Newborn (P-prefix) -> DRG 794 MDC 15.
    (2021, "P0500", (), (), "F", 0, "794", "15", "without_cc_mcc", False, "newborn_v38"),
]


@pytest.mark.parametrize(
    "fy,pdx,sdx,pcs,sex,age,exp_drg,exp_mdc,exp_sev,exp_or,label",
    _CANONICAL,
)
def test_canonical_drg_roundtrip(
    fy: int, pdx: str, sdx: tuple, pcs: tuple,
    sex: str, age: int,
    exp_drg: str, exp_mdc: str, exp_sev: str, exp_or: bool,
    label: str,
) -> None:
    rec = ClaimRecord(
        pdx=pdx, sdx=sdx, pcs=pcs,
        sex=sex, age=age, discharge_disp="01", los=5,
    )
    res = drgpy_assign(rec, fy)
    assert isinstance(res, GrouperResult), label
    assert res.ms_drg == exp_drg, f"{label}: expected drg={exp_drg} got {res.ms_drg}"
    assert res.mdc == exp_mdc, f"{label}: expected mdc={exp_mdc} got {res.mdc}"
    assert res.severity_tier == exp_sev, (
        f"{label}: expected severity={exp_sev} got {res.severity_tier}"
    )
    assert res.or_procedure_flag == exp_or, (
        f"{label}: expected or_flag={exp_or} got {res.or_procedure_flag}"
    )
    # sdc_indicators is intentionally empty (drgpy is not an MCE pipeline).
    assert res.sdc_indicators == {}


# ---------------------------------------------------------------------------
# 4. Pre-MDC DRG MDC-from-features fallback
# ---------------------------------------------------------------------------


def test_pre_mdc_heart_transplant_routes_to_mdc05() -> None:
    """Pre-MDC DRG 002 (heart transplant) must report MDC 05, not '00'.

    Phase 4 locked landmine #2: pre-MDC DRGs (001-018, 981-989) report a
    non-'00' MDC despite drgpy's Appendix A entry blanking the mdc field.
    The drgpy_wrapper falls back to feature-based inference (`_MDC05`).
    """
    rec = ClaimRecord(
        pdx="I509", sdx=(), pcs=("02YA0Z0",),
        sex="M", age=55, discharge_disp="01", los=20,
    )
    res = drgpy_assign(rec, 2020)
    assert res.ms_drg == "002", f"expected DRG 002 (heart transplant), got {res.ms_drg}"
    assert res.mdc == "05", f"expected MDC 05 for pre-MDC heart transplant, got {res.mdc}"
    assert res.or_procedure_flag is True


# ---------------------------------------------------------------------------
# 5. Discharge dispo = '20' (expired) flips is_alive to False
# ---------------------------------------------------------------------------


def test_discharge_dispo_expired_passes_is_alive_false() -> None:
    """Sepsis with MCC + expired discharge: drgpy's is_alive=False can
    re-route to a different DRG. Just exercise the path; canonical sepsis+MCC
    is stable across is_alive in v40.
    """
    rec = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(),
        sex="M", age=70, discharge_disp="20", los=2,
    )
    res = drgpy_assign(rec, 2020)
    # Sepsis+MCC stays at 871 regardless of is_alive (verified empirically).
    assert res.ms_drg == "871"


# ---------------------------------------------------------------------------
# 6. Dispatcher in src.grouper.grouper.assign routes FY2019-FY2022 -> drgpy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fy", [2019, 2020, 2021, 2022])
def test_dispatcher_routes_fy2019_to_2022_to_drgpy(fy: int) -> None:
    """src.grouper.grouper.assign() must dispatch FY2019-FY2022 to drgpy.

    Pre-Session 9.5, those FYs raised GrouperError("FY ... has no Java
    grouper"). After 9.5, they round-trip via the drgpy fallback.
    """
    rec = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(),
        sex="M", age=70, discharge_disp="01", los=5,
    )
    res = grouper_assign(rec, fy)
    assert res.ms_drg == "871", f"FY{fy} sepsis+MCC drgpy dispatch failed"
    assert res.mdc == "18"
    assert res.severity_tier == "with_mcc"


def test_explicit_drgpy_v40_override_for_fy2023() -> None:
    """version='v40' on FY2023 forces drgpy path even when Java is preferred."""
    rec = ClaimRecord(
        pdx="A419", sdx=("J9601",), pcs=(),
        sex="M", age=70, discharge_disp="01", los=5,
    )
    res = grouper_assign(rec, 2023, version="v40")
    assert res.ms_drg == "871"
    # sdc_indicators empty proves we routed through drgpy (Java path populates).
    assert res.sdc_indicators == {}


def test_engine_cache_returns_same_instance() -> None:
    """Two assigns on the same FY share one DRGEngine (engines are heavy)."""
    from src.grouper.drgpy_wrapper import _ENGINE_CACHE, _get_engine
    e1 = _get_engine("v40")
    e2 = _get_engine("v40")
    assert e1 is e2
    assert "v40" in _ENGINE_CACHE
