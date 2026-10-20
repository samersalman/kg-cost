"""Smoke tests for FY2023-2024 MS-DRG grouper integration (V40+).

Phase 1a regression guard for the downstream fracture-cost study's v3 pipeline. Locks the current
passing state of the FY2023/FY2024 Java grouper paths so a future
classpath / model-JAR ABI regression cannot silently re-surface the
DRG=999 / inpatient_confidence='none' pathology described in KG-COST FLAG 1.

These tests intentionally DO NOT pin a specific ``version=...`` argument:
they exercise the default routing (``GROUPER_VERSIONS_BY_FY[fy][0]``) so a
classpath drop of V40.0 / V41.0 (the V*.0 default for FY2023 / FY2024)
would be caught even if V*.1 is still working.

Skip cleanly when V43.1 JAR working tree is absent (mirrors
``tests/test_grouper.py`` policy).
"""

from __future__ import annotations

import pytest

from src import config
from src.grouper.grouper import ClaimRecord, assign


_JAR = config.grouper_jar_dir(2026, "V43.1") / "msdrg-core-43.1.0.0.jar"

pytestmark = pytest.mark.skipif(
    not _JAR.exists(),
    reason=(
        "V43.1 JAR working tree absent. Run "
        "`python3 -m src.extract.grouper --fy 2026 --version V43.1` first."
    ),
)


_HIP_FX_RECORD = ClaimRecord(
    pdx="S72001A",  # right femoral neck fracture, initial encounter
    sdx=(),
    pcs=(),
    sex="F",
    age=78,
    discharge_disp="01",
    los=4,
)

_SEPSIS_MCC_RECORD = ClaimRecord(
    pdx="A419",
    sdx=("J9601",),
    pcs=(),
    sex="M",
    age=70,
    discharge_disp="01",
    los=5,
)


def test_fy2024_common_icd10_routes_to_high_confidence_drg() -> None:
    """A common ICD-10 hip fracture must route to a non-999 DRG in FY2024.

    Default FY2024 routing -> V41.0 grouper. Defends against the
    KG-COST FLAG 1 pathology (DRG=999 + confidence='none' for 100% of
    FY2024 records). Guards both the default-version dispatch and the
    classpath construction in ``_vendored_version_dirs``.
    """
    result = assign(_HIP_FX_RECORD, 2024)
    assert result.ms_drg != "999", (
        f"FY2024 default routing returned UNGROUPABLE DRG 999 for a common "
        f"ICD-10 hip fracture (S72.001A). Likely a classpath / dispatch "
        f"regression in src/grouper/grouper.py."
    )
    assert result.ms_drg != "000", (
        f"FY2024 default routing returned the failure-path sentinel '000' "
        f"(any-error fallthrough). Got ms_drg={result.ms_drg!r}."
    )
    assert result.sdc_indicators.get("final_grc") == "OK", (
        f"FY2024 grouper final_grc != 'OK'; got "
        f"{result.sdc_indicators.get('final_grc')!r}."
    )


def test_fy2023_common_icd10_routes_to_high_confidence_drg() -> None:
    """Same guard as FY2024 for FY2023 (V40.0 default routing)."""
    result = assign(_HIP_FX_RECORD, 2023)
    assert result.ms_drg != "999", (
        f"FY2023 default routing returned UNGROUPABLE DRG 999 for a common "
        f"ICD-10 hip fracture (S72.001A). Got ms_drg={result.ms_drg!r}."
    )
    assert result.ms_drg != "000", (
        f"FY2023 default routing returned failure-path sentinel '000'. "
        f"Got ms_drg={result.ms_drg!r}."
    )
    assert result.sdc_indicators.get("final_grc") == "OK", (
        f"FY2023 grouper final_grc != 'OK'; got "
        f"{result.sdc_indicators.get('final_grc')!r}."
    )


def test_fy2024_sepsis_with_mcc_drg_871_default_routing() -> None:
    """Sepsis (A419) + acute respiratory failure (J9601) MCC -> DRG 871, MDC 18,
    with_mcc. Stable across V40-V43 per CMS Definitions Manual. Locks the
    default V41.0 dispatch end-to-end (classpath + reflection + Builder API)."""
    result = assign(_SEPSIS_MCC_RECORD, 2024)
    assert result.ms_drg == "871", (
        f"FY2024 sepsis+MCC default routing expected DRG 871, "
        f"got {result.ms_drg!r}"
    )
    assert result.mdc == "18"
    assert result.severity_tier == "with_mcc"


def test_fy2023_sepsis_with_mcc_drg_871_default_routing() -> None:
    """Same guard for FY2023 (V40.0 default routing)."""
    result = assign(_SEPSIS_MCC_RECORD, 2023)
    assert result.ms_drg == "871", (
        f"FY2023 sepsis+MCC default routing expected DRG 871, "
        f"got {result.ms_drg!r}"
    )
    assert result.mdc == "18"
    assert result.severity_tier == "with_mcc"


def test_fy2022_baseline_unchanged() -> None:
    """Sanity: the existing FY2022 drgpy fallback path still works.

    FY2022 routes to drgpy (V39 / no Java grouper available per CMS).
    This guards against accidental Java-side patches breaking the
    FY2019-FY2022 fallback window.
    """
    record = ClaimRecord(
        pdx="S42471A",  # displaced fracture of medial epicondyle of right humerus
        sdx=(),
        pcs=(),
        sex="M",
        age=70,
        discharge_disp="01",
        los=4,
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
    assert len(result.ms_drg) == 3 and result.ms_drg.isdigit(), (
        f"FY2022 drgpy DRG should be 3-digit zero-padded; "
        f"got {result.ms_drg!r}"
    )
