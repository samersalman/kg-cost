"""Tests for ``src.query.run_phase14c`` and ``src.query._phase14c_coverage``.

Four always-on unit tests exercise the FY-sharded wrapper against a
synthetic NTDB fixture (no real 10M-row source needed). One gated test
optionally exercises the real ``annual_cohort.parquet`` when
``KG_COST_RUN_NTDB=1`` and the source file exists.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

from src.query import _phase14c_coverage, run_phase14c

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_PARQUET = _REPO_ROOT / "tests" / "fixtures" / "ntdb_synthetic_100.parquet"
# The user's own NTDB annual_cohort.parquet (restricted data, not in this repo).
# Point KG_COST_NTDB_COHORT_PARQUET at it; the plain relative default does not
# exist, so the live test skips.
_REAL_NTDB_PARQUET = Path(
    os.environ.get("KG_COST_NTDB_COHORT_PARQUET", "annual_cohort.parquet")
)
_GOLD_CODE_COSTS = _REPO_ROOT / "data" / "gold" / "code_costs.parquet"
_GOLD_MODAL = _REPO_ROOT / "data" / "gold" / "icd_principal_drg_modal.parquet"

# The wrapper depends on parquet-mode lookups (code_costs + icd_principal_drg_modal).
# On a fresh checkout these may not exist; skip the data-dependent tests cleanly.
_GOLD_AVAILABLE = _GOLD_CODE_COSTS.exists() and _GOLD_MODAL.exists()
_FIXTURE_AVAILABLE = _FIXTURE_PARQUET.exists()


def _build_two_fy_synthetic(target_dir: Path, fys: tuple[int, ...]) -> Path:
    """Slice the 100-row synthetic NTDB fixture down to ``fys`` and write to a
    fresh parquet under ``target_dir``. Returns the new parquet path.

    All rows for the requested FYs are preserved (useful for the union test
    so we have a non-trivial row count to compare against).
    """
    df = pd.read_parquet(_FIXTURE_PARQUET)
    sliced = df[df["YEAR"].isin(list(fys))].copy()
    if sliced.empty:
        raise RuntimeError(
            f"synthetic fixture has no rows for fys={fys}; "
            f"available years: {sorted(df['YEAR'].unique())}"
        )
    out = target_dir / "ntdb_synthetic_subset.parquet"
    sliced.to_parquet(out, index=False)
    return out


@pytest.mark.skipif(
    not (_FIXTURE_AVAILABLE and _GOLD_AVAILABLE),
    reason="needs synthetic NTDB fixture + gold code_costs / modal parquets",
)
def test_fy_shard_idempotence(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Re-running the wrapper for the same FY skips and preserves checkpoint mtime."""
    src_parquet = _build_two_fy_synthetic(tmp_path, fys=(2010,))
    output_dir = tmp_path / "derived"
    checkpoint_dir = tmp_path / "checkpoints"

    # First run — checkpoint should be written.
    run_phase14c.run(
        source_parquet=src_parquet,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
        fy_range=(2010, 2010),
        attach_procedures=False,
    )
    ckpt = checkpoint_dir / "cohort_annotated_fy2010.parquet"
    assert ckpt.exists(), "first run should produce a per-FY checkpoint"
    mtime_before = ckpt.stat().st_mtime_ns

    # Drain captured output.
    capsys.readouterr()

    # Second run — should skip via the on-disk-checkpoint guard.
    run_phase14c.run(
        source_parquet=src_parquet,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
        fy_range=(2010, 2010),
        attach_procedures=False,
    )
    captured = capsys.readouterr()
    assert "[skip] fy=2010" in captured.out, (
        "second run should log [skip] fy=2010; got:\n" + captured.out
    )
    mtime_after = ckpt.stat().st_mtime_ns
    assert mtime_after == mtime_before, (
        "skip path must NOT rewrite the checkpoint file"
    )


@pytest.mark.skipif(
    not (_FIXTURE_AVAILABLE and _GOLD_AVAILABLE),
    reason="needs synthetic NTDB fixture + gold code_costs / modal parquets",
)
def test_fy_shard_union(tmp_path: Path) -> None:
    """Union of per-FY checkpoints equals sum of their row counts."""
    src_parquet = _build_two_fy_synthetic(tmp_path, fys=(2010, 2024))
    output_dir = tmp_path / "derived"
    checkpoint_dir = tmp_path / "checkpoints"

    final_path, n_total, n_shards = run_phase14c.run(
        source_parquet=src_parquet,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
        fy_range=(2010, 2024),
        attach_procedures=False,
    )

    # Both per-FY checkpoints exist.
    ckpt_2010 = checkpoint_dir / "cohort_annotated_fy2010.parquet"
    ckpt_2024 = checkpoint_dir / "cohort_annotated_fy2024.parquet"
    assert ckpt_2010.exists()
    assert ckpt_2024.exists()

    df_2010 = pd.read_parquet(ckpt_2010)
    df_2024 = pd.read_parquet(ckpt_2024)
    df_union = pd.read_parquet(final_path)

    expected = len(df_2010) + len(df_2024)
    # Empty FYs in the requested range (e.g., 2011-2023) emit zero-row checkpoints
    # which contribute 0 rows to the union, so the sum-of-non-empty-FYs equality
    # holds even though n_shards counts every iterated FY.
    assert len(df_union) == expected, (
        f"union row count ({len(df_union)}) should equal "
        f"sum of per-FY checkpoints ({expected})"
    )
    assert n_total == len(df_union)
    assert n_shards == (2024 - 2010 + 1)


def test_source_parquet_is_required_cli_argument(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--source-parquet`` has no built-in default: omitting it is an argparse error."""
    with pytest.raises(SystemExit) as excinfo:
        run_phase14c.main([])
    assert excinfo.value.code == 2
    assert "--source-parquet" in capsys.readouterr().err


def test_coverage_diagnostic_shape(tmp_path: Path) -> None:
    """Coverage diagnostic emits the locked column set and per-FY pct sums to ~1."""
    annotated = pd.DataFrame(
        {
            "fy": [2020, 2020, 2020, 2021, 2021, 2022, 2022, 2022],
            "inpatient_confidence": [
                "high", "high", "approximate",
                "high", "none",
                "approximate", "approximate", "none",
            ],
            "annotation_status": [
                "ok", "partial", "no_match",
                "ok", "no_match",
                "ok", "partial", "no_match",
            ],
        }
    )
    input_path = tmp_path / "annotated.parquet"
    annotated.to_parquet(input_path, index=False)

    output_path = tmp_path / "coverage.parquet"
    result = _phase14c_coverage.build_coverage(input_path, output_path)

    assert output_path.exists()
    on_disk = pd.read_parquet(output_path)

    expected_cols = {"fy", "inpatient_confidence", "annotation_status", "n_rows", "pct_of_fy"}
    assert expected_cols == set(on_disk.columns)
    assert expected_cols == set(result.columns)

    assert on_disk["fy"].nunique() >= 3
    assert on_disk["inpatient_confidence"].nunique() >= 2

    # pct_of_fy sums to ~1.0 within each FY group.
    fy_sums = on_disk.groupby("fy")["pct_of_fy"].sum()
    for fy, total in fy_sums.items():
        assert abs(total - 1.0) < 1e-9, (
            f"pct_of_fy for fy={fy} sums to {total}, expected ~1.0"
        )


@pytest.mark.skipif(
    os.environ.get("KG_COST_RUN_NTDB") != "1" or not _REAL_NTDB_PARQUET.exists(),
    reason="live NTDB run not enabled (set KG_COST_RUN_NTDB=1 + ensure annual_cohort.parquet exists)",
)
def test_phase14c_full_run_smoke(tmp_path: Path) -> None:
    """Smoke-run the wrapper against the real NTDB source for a single FY."""
    output_dir = tmp_path / "derived"
    checkpoint_dir = tmp_path / "checkpoints"

    final_path, n_total, n_shards = run_phase14c.run(
        source_parquet=_REAL_NTDB_PARQUET,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
        fy_range=(2024, 2024),
        attach_procedures=True,
    )
    assert final_path.exists()
    ckpt = checkpoint_dir / "cohort_annotated_fy2024.parquet"
    assert ckpt.exists()
    assert n_total > 100_000, (
        f"FY2024 NTDB should contain >100k rows; got n_total={n_total}"
    )
    assert n_shards == 1
