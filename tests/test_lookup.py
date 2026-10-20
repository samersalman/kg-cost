"""Tests for src/query/lookup.py — single-code cost lookup API.

Always-on (parquet mode, no Neo4j required):
  - Happy-path resolution per code_type (cpt, drg, icd10cm)
  - ICD-10-PCS returns confidence='none' with the documented note (no
    direct cost mapping)
  - APC happy-path (silver parquet) — skipped if silver missing
  - ICD code normalization (dotted == dotless)
  - Coverage-gap cases (unknown code, out-of-range FY, pre-ICD-10 era)
    return confidence='none' cleanly — no exceptions
  - Output schema invariants (all 12 keys present, confidence enum)
  - Programmer-error inputs raise ValueError
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.query.lookup import _APC_RATES_PATH, get_cost

ROOT = Path(__file__).resolve().parents[1]
_CODE_COSTS = ROOT / "data" / "gold" / "code_costs.parquet"
_ICD_MODAL = ROOT / "data" / "gold" / "icd_principal_drg_modal.parquet"

_PARQUET_SKIP = pytest.mark.skipif(
    not (_CODE_COSTS.exists() and _ICD_MODAL.exists()),
    reason="data/gold/code_costs.parquet or icd_principal_drg_modal.parquet missing",
)

_APC_SKIP = pytest.mark.skipif(
    not _APC_RATES_PATH.exists(),
    reason="data/silver/opps_apc_rates.parquet missing",
)

_EXPECTED_KEYS = {
    "code",
    "code_type",
    "fy",
    "setting",
    "mean",
    "median",
    "p10",
    "p90",
    "n",
    "source",
    "confidence",
    "notes",
}


def _assert_schema(result: dict) -> None:
    """Every result dict has the same 12 keys with valid confidence enum."""
    assert set(result.keys()) == _EXPECTED_KEYS, (
        f"output schema drift; got {set(result.keys())}, expected {_EXPECTED_KEYS}"
    )
    assert result["confidence"] in {"high", "approximate", "none"}, (
        f"confidence outside enum: {result['confidence']!r}"
    )
    assert isinstance(result["notes"], list)


# ============================================================
# Happy-path tests — one per code type that should resolve
# ============================================================


@_PARQUET_SKIP
def test_get_cost_cpt_happy_path() -> None:
    """CPT 27447 (TKA) has PUF empirical data through FY2023."""
    result = get_cost(code="27447", code_type="cpt", fy=2023)
    _assert_schema(result)
    assert result["code"] == "27447"
    assert result["code_type"] == "cpt"
    assert result["confidence"] == "high"
    assert result["mean"] is not None
    assert result["mean"] > 0
    assert result["setting"] in {"pfs_facility", "pfs_nonfacility", "opps", "asc"}


@_PARQUET_SKIP
def test_get_cost_drg_happy_path() -> None:
    """DRG 470 (major joint replacement) has cost across all FY2008+ years."""
    result = get_cost(code="470", code_type="drg", fy=2020)
    _assert_schema(result)
    assert result["code"] == "470"
    assert result["code_type"] == "drg"
    assert result["setting"] == "inpatient"
    assert result["confidence"] == "high"
    assert result["mean"] is not None
    assert result["mean"] > 0


@_PARQUET_SKIP
def test_get_cost_icd10cm_happy_path() -> None:
    """S72.001A (closed fracture of right femoral head) resolves via modal DRG."""
    result = get_cost(code="S72.001A", code_type="icd10cm", fy=2023)
    _assert_schema(result)
    assert result["code"] == "S72001A"  # normalized (dot stripped, upper)
    assert result["code_type"] == "icd10cm"
    assert result["confidence"] == "high"
    assert result["setting"] == "inpatient"
    assert result["mean"] is not None
    assert result["mean"] > 0
    # Modal DRG provenance recorded
    assert any("resolved_via_modal_drg" in n for n in result["notes"])


@_PARQUET_SKIP
def test_get_cost_icd10pcs_no_direct_mapping() -> None:
    """ICD-10-PCS has no direct cost mapping; returns confidence='none' cleanly."""
    result = get_cost(code="0SR9069", code_type="icd10pcs", fy=2023)
    _assert_schema(result)
    assert result["code_type"] == "icd10pcs"
    assert result["confidence"] == "none"
    assert result["mean"] is None
    assert "icd10pcs_no_direct_cost_mapping" in result["notes"]


@_APC_SKIP
def test_get_cost_apc_happy_path() -> None:
    """APC 5114 (level 4 musculoskeletal) is present in CY2023 silver."""
    result = get_cost(code="5114", code_type="apc", fy=2023)
    _assert_schema(result)
    assert result["code_type"] == "apc"
    assert result["setting"] == "opps"
    assert result["confidence"] == "high"
    assert result["mean"] is not None and result["mean"] > 0


# ============================================================
# Normalization
# ============================================================


@_PARQUET_SKIP
def test_icd10cm_dotted_and_dotless_match() -> None:
    """S72.001A and S72001A must resolve to the same cost record."""
    dotted = get_cost(code="S72.001A", code_type="icd10cm", fy=2023)
    dotless = get_cost(code="S72001A", code_type="icd10cm", fy=2023)
    assert dotted["mean"] == dotless["mean"]
    assert dotted["code"] == dotless["code"] == "S72001A"
    assert dotted["confidence"] == dotless["confidence"]


@_PARQUET_SKIP
def test_cpt_uppercase_normalization() -> None:
    """CPT lookup is case-insensitive for the (rare) alpha-suffix codes."""
    # Plain numeric CPT — exercise the .strip().upper() path
    r_upper = get_cost(code="27447", code_type="cpt", fy=2023)
    r_padded = get_cost(code="  27447  ", code_type="cpt", fy=2023)
    assert r_upper["mean"] == r_padded["mean"]


# ============================================================
# Coverage-gap cases — must return confidence='none', no exceptions
# ============================================================


@_PARQUET_SKIP
def test_unknown_cpt_returns_none() -> None:
    """A fabricated CPT (99999 is not a valid HCPCS) returns confidence='none'."""
    result = get_cost(code="99999", code_type="cpt", fy=2023)
    _assert_schema(result)
    assert result["confidence"] == "none"
    assert result["mean"] is None


@_PARQUET_SKIP
def test_unknown_icd10cm_returns_none() -> None:
    """A made-up ICD-10-CM code returns confidence='none' without crashing."""
    result = get_cost(code="Z99.999XX", code_type="icd10cm", fy=2023)
    _assert_schema(result)
    assert result["confidence"] == "none"
    assert result["mean"] is None


@_PARQUET_SKIP
def test_fy_out_of_range_returns_none() -> None:
    """FY=2050 has no data; returns confidence='none' cleanly."""
    result = get_cost(code="470", code_type="drg", fy=2050)
    _assert_schema(result)
    assert result["confidence"] == "none"
    assert result["mean"] is None


@_PARQUET_SKIP
def test_pre_icd10_era_returns_none() -> None:
    """FY<2017 ICD-10-CM is pre-ICD-10 era; tagged and skipped."""
    result = get_cost(code="S72.001A", code_type="icd10cm", fy=2010)
    _assert_schema(result)
    assert result["confidence"] == "none"
    assert any("pre_icd10_era" in n for n in result["notes"])


@_PARQUET_SKIP
def test_icd9pcs_no_direct_mapping() -> None:
    """ICD-9-PCS mirrors ICD-10-PCS — no direct mapping."""
    result = get_cost(code="81.54", code_type="icd9pcs", fy=2012)
    _assert_schema(result)
    assert result["confidence"] == "none"
    assert "icd9pcs_no_direct_cost_mapping" in result["notes"]


# ============================================================
# Programmer-error inputs — these MUST raise ValueError
# ============================================================


def test_invalid_code_type_raises() -> None:
    """Unknown code_type is a programmer error, not a data miss."""
    with pytest.raises(ValueError, match="code_type must be one of"):
        get_cost(code="27447", code_type="xyz", fy=2023)  # type: ignore[arg-type]


def test_empty_code_raises() -> None:
    """Empty / whitespace-only code is a programmer error."""
    with pytest.raises(ValueError, match="non-empty"):
        get_cost(code="", code_type="cpt", fy=2023)
    with pytest.raises(ValueError, match="non-empty"):
        get_cost(code="   ", code_type="cpt", fy=2023)


def test_non_int_fy_raises() -> None:
    """fy must be int (not a numpy type, str, float, etc.)."""
    with pytest.raises(ValueError, match="fy must be int"):
        get_cost(code="27447", code_type="cpt", fy="2023")  # type: ignore[arg-type]


# ============================================================
# Public-API export
# ============================================================


def test_get_cost_exported_from_package() -> None:
    """src.query.get_cost is the public name documented in plan."""
    from src.query import get_cost as exported_get_cost
    from src.query.lookup import get_cost as direct_get_cost

    assert exported_get_cost is direct_get_cost
