"""Phase 4 part 2 — drgpy cross-check sanity tier.

drgpy is a pure-Python ICD-10 MS-DRG grouper supporting up to v40 (FY2023).
We run drgpy v40 against our Java V41.0 (FY2024) grouper on a sample of
FY2024 MIMIC-IV records. Plan-v9.md A5 calls for ~96-98% agreement and
expects edge cases (newborn DRGs 789-793, extensive fusion handling) to
account for the gap.

Skips cleanly when:
  - drgpy not installed
  - V41.0 grouper JARs not vendored (Step 3 bulk download not yet run)
  - MIMIC-IV not present (``KG_COST_MIMIC_HOSP_DIR`` unset, or
    ``drgcodes.csv.gz`` missing from it)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from src import config
from src.grouper.grouper import ClaimRecord, GrouperError, assign


_TARGET_FY = 2024
_TARGET_JAVA_VERSION = "V41.0"
_DRGPY_VERSION = "v40"  # latest drgpy supports
_AGREEMENT_FLOOR = 0.96  # plan-v9.md A5 acceptance criterion
_SAMPLE_CAP = 300  # smaller than MIMIC harness (single-FY sanity tier)


# ---------------------------------------------------------------------------
# Skip predicates
# ---------------------------------------------------------------------------


def _drgpy_importable() -> bool:
    try:
        import drgpy.msdrg  # noqa: F401
        return True
    except Exception:  # pragma: no cover -- environment-dependent
        return False


def _v41_vendored() -> bool:
    from src.extract.grouper import _find_core_jar
    jar_dir = config.grouper_jar_dir(_TARGET_FY, _TARGET_JAVA_VERSION)
    if not jar_dir.is_dir():
        return False
    return _find_core_jar(jar_dir, _TARGET_JAVA_VERSION) is not None


def _mimic_present() -> bool:
    hosp_dir = os.environ.get("KG_COST_MIMIC_HOSP_DIR", "").strip()
    if not hosp_dir:
        return False
    return (Path(hosp_dir) / "drgcodes.csv.gz").exists()


# ---------------------------------------------------------------------------
# Fixture: load FY2024 MIMIC sample reusing the validation harness loader.
# ---------------------------------------------------------------------------


def _load_fy2024_sample() -> list:
    """Reuse the MIMIC loader, then filter to real_fy=2024."""
    from tests.test_grouper_validation import _load_mimic_rows
    rows = _load_mimic_rows()
    fy_rows = [r for r in rows if r.real_fy == _TARGET_FY]
    if len(fy_rows) > _SAMPLE_CAP:
        # Deterministic head -- the underlying loader already sampled with seed.
        fy_rows = fy_rows[:_SAMPLE_CAP]
    return fy_rows


# ---------------------------------------------------------------------------
# Cross-check loop
# ---------------------------------------------------------------------------


def _run_cross_check(rows: list) -> tuple[int, int, list[tuple[str, str, str]]]:
    """Return (n_compared, n_agree, list[(pdx, our_drg, drgpy_drg)] for disagreements)."""
    from drgpy.msdrg import DRGEngine
    engine = DRGEngine(version=_DRGPY_VERSION)
    n_compared = 0
    n_agree = 0
    disagreements: list[tuple[str, str, str]] = []
    for r in rows:
        record = ClaimRecord(
            pdx=r.pdx, sdx=r.sdx, pcs=r.pcs,
            sex=r.sex, age=r.age,
            discharge_disp=r.discharge_disp, los=r.los_days,
        )
        try:
            our = assign(record, _TARGET_FY).ms_drg
        except GrouperError:
            continue
        try:
            their_raw = engine.get_drg(
                dx_lst=[r.pdx, *r.sdx],
                pr_lst=list(r.pcs),
                gender=r.sex if r.sex in ("M", "F") else "F",
                is_alive=(r.discharge_disp != "20"),
            )
        except Exception:  # noqa: BLE001 -- drgpy may bail on unfamiliar codes
            continue
        their = str(their_raw).strip().zfill(3) if their_raw else "000"
        n_compared += 1
        if our == their:
            n_agree += 1
        else:
            if len(disagreements) < 50:
                disagreements.append((r.pdx, our, their))
    return n_compared, n_agree, disagreements


# ---------------------------------------------------------------------------
# Pytest entry
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _drgpy_importable(), reason="drgpy not installed")
@pytest.mark.skipif(
    not _v41_vendored(),
    reason=(
        f"V41.0 grouper JARs not vendored. Run "
        f"`python3 -m src.extract.grouper --fy {_TARGET_FY} --version "
        f"{_TARGET_JAVA_VERSION}` first."
    ),
)
@pytest.mark.skipif(not _mimic_present(), reason="MIMIC-IV not present")
def test_drgpy_v40_vs_java_v41_fy2024_agreement(capsys) -> None:
    """drgpy v40 vs Java V41.0 on FY2024 MIMIC records: ≥96% agreement."""
    rows = _load_fy2024_sample()
    if not rows:
        pytest.skip(f"No FY{_TARGET_FY} records in MIMIC sample")
    n_compared, n_agree, disagreements = _run_cross_check(rows)
    if not n_compared:
        pytest.skip("No records survived both groupers (all errored on one or both)")
    pct = 100.0 * n_agree / n_compared
    with capsys.disabled():
        sys.stderr.write(
            f"\n[drgpy-cross] FY{_TARGET_FY}: {n_agree}/{n_compared} agree "
            f"({pct:.2f}%); {len(disagreements)} disagreements logged\n"
        )
        for i, (pdx, ours, theirs) in enumerate(disagreements[:10]):
            sys.stderr.write(
                f"  [{i+1:>2}] pdx={pdx}: ours={ours} theirs={theirs}\n"
            )
    assert pct / 100.0 >= _AGREEMENT_FLOOR, (
        f"drgpy v40 vs V41.0 agreement {pct:.2f}% below threshold "
        f"{_AGREEMENT_FLOOR * 100:.0f}%; first disagreements: {disagreements[:5]}"
    )


@pytest.mark.skipif(not _drgpy_importable(), reason="drgpy not installed")
def test_drgpy_v40_engine_smoke() -> None:
    """drgpy v40 returns sane DRG strings on canonical cases.

    Defends the drgpy-side plumbing without depending on Java JAR vendoring;
    catches a broken drgpy install before the heavy cross-check runs.
    """
    from drgpy.msdrg import DRGEngine
    engine = DRGEngine(version=_DRGPY_VERSION)

    # Sepsis with MCC: J9601 + A419 -> DRG 871 (canonical, version-stable).
    drg = engine.get_drg(
        dx_lst=["A419", "J9601"], pr_lst=[],
        gender="M", is_alive=True,
    )
    assert str(drg) == "871", (
        f"sepsis+MCC expected DRG 871 from drgpy v40, got {drg!r}"
    )

    # Hip replacement (no MCC): M1611 + 0SR9019 -> DRG 470.
    drg = engine.get_drg(
        dx_lst=["M1611"], pr_lst=["0SR9019"],
        gender="F", is_alive=True,
    )
    assert str(drg) == "470", (
        f"hip-replacement expected DRG 470 from drgpy v40, got {drg!r}"
    )
