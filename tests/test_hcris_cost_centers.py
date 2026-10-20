"""Phase 6 (Session 9.5) tests for the HCRIS cost-center supplemental silver.

Validates the per-FY parquet shape, row-count floor, and the FY2024 sub-flag 1b
content sniff (definition table size < 100 KB; rejects the 23 MB provider-level
data file). The transform module is importable unconditionally; data-dependent
assertions skip when the silver parquet is absent.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import config
from src.extract.hcris_cost_centers import (
    _DEFINITION_MAX_BYTES,
    _FY2024_BAD_LOOKALIKE,
    _HCRIS_COST_CENTER_URLS,
    _PARQUET_SCHEMA,
    _PROVIDER_DATA_MIN_BYTES,
)

_SILVER = config.SILVER / "hcris_cost_centers.parquet"
_BRONZE = config.BRONZE / "hcris_cost_centers"


def _skip_if_missing(p: Path, label: str) -> None:
    if not p.exists():
        pytest.skip(f"{label} not present at {p}")


def test_module_importable() -> None:
    from src.extract import hcris_cost_centers

    assert hasattr(hcris_cost_centers, "extract_fy")
    assert hasattr(hcris_cost_centers, "build_unioned_silver")


def test_url_manifest_covers_fy2021_to_fy2026() -> None:
    """Definition table is published FY2021+ only."""
    assert sorted(_HCRIS_COST_CENTER_URLS.keys()) == [2021, 2022, 2023, 2024, 2025, 2026]
    for fy, url in _HCRIS_COST_CENTER_URLS.items():
        assert url.startswith("https://www.cms.gov/"), f"FY{fy}: {url}"
        assert url.endswith(".zip"), f"FY{fy}: {url}"


def test_fy2024_bad_lookalike_is_distinct() -> None:
    """FY2024 sub-flag 1b: the proposed-rule lookalike must NOT match the canonical URL."""
    assert _HCRIS_COST_CENTER_URLS[2024] != _FY2024_BAD_LOOKALIKE


def test_size_bands_consistent() -> None:
    assert _DEFINITION_MAX_BYTES < _PROVIDER_DATA_MIN_BYTES, (
        "Definition file size cap must be less than provider-level data file floor."
    )
    assert _DEFINITION_MAX_BYTES <= 100_000
    assert _PROVIDER_DATA_MIN_BYTES >= 1_000_000


def test_silver_schema() -> None:
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    pf = pq.ParquetFile(_SILVER)
    assert list(pf.schema_arrow.names) == [f.name for f in _PARQUET_SCHEMA]


def test_silver_row_count_floor() -> None:
    """Each FY publishes ~85-88 rows; 6 FYs => ~510-528 rows total."""
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    table = pq.read_table(_SILVER)
    assert table.num_rows >= 400, (
        f"hcris_cost_centers.parquet has only {table.num_rows} rows; "
        "expected ~510 across FY2021-FY2026 (each FY has ~85-88 rows)."
    )


def test_silver_per_fy_count() -> None:
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    fys = sorted(df["fy"].unique().tolist())
    # Form 2552-96 era (FY2008-FY2010) added by Session 1.5 Batch 1C; ~42 rows
    # per FY. Form 2552-10 era (FY2021+) carries ~85-88 rows per FY.
    assert fys == [2008, 2009, 2010, 2021, 2022, 2023, 2024, 2025, 2026], (
        f"FYs present: {fys}"
    )
    for fy in fys:
        n = len(df[df["fy"] == fy])
        assert 30 <= n <= 200, f"FY{fy}: {n} rows outside [30, 200] band"


def test_silver_required_columns_non_null() -> None:
    """``fy`` and ``cost_center_group`` must be non-null on every row."""
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    assert df["fy"].notna().all()
    assert df["cost_center_group"].notna().all()
    assert df["source"].eq("hcris_cost_centers").all()


def test_silver_known_cost_center_groups_present() -> None:
    """Sample of canonical 19 cost-center groups should all appear."""
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    canonical_substrings = ("Routine Days", "Operating Room", "Drugs", "Laboratory", "Emergency Room")
    seen = df["cost_center_group"].astype(str).tolist()
    seen_text = " | ".join(seen)
    for s in canonical_substrings:
        assert s in seen_text, f"Canonical cost-center substring '{s}' not found"


def test_silver_cell_refs_match_pattern() -> None:
    """``hcris_cost_cell_ref`` populated rows match ``C_{n}_C{c}_{line}`` style."""
    _skip_if_missing(_SILVER, "hcris_cost_centers.parquet")
    import re

    pat = re.compile(r"^[A-Z][A-Z0-9_]*_C[0-9]+_[0-9]+(?:\.[0-9]+)?$")
    df = pq.read_table(_SILVER).to_pandas()
    refs = df["hcris_cost_cell_ref"].dropna().astype(str).tolist()
    assert refs, "No hcris_cost_cell_ref values populated"
    bad = [r for r in refs if not pat.match(r.strip())]
    assert not bad[:5], f"hcris_cost_cell_ref values failing pattern: {bad[:5]}"


def test_bronze_size_sniff_definition_under_cap() -> None:
    """Sub-flag 1b: every bronze ZIP under FY{2021..2026} must be <100 KB.

    If a future re-pull lands the 23 MB provider-level data file under this
    bronze directory, this test catches it before the parser corrupts silver.
    """
    if not _BRONZE.exists():
        pytest.skip(f"{_BRONZE} not yet populated")
    sizes: list[tuple[Path, int]] = []
    for zip_path in _BRONZE.rglob("*.zip"):
        sizes.append((zip_path, zip_path.stat().st_size))
    if not sizes:
        pytest.skip("No bronze ZIPs present")
    too_big = [(p, s) for p, s in sizes if s >= _PROVIDER_DATA_MIN_BYTES]
    assert not too_big, (
        f"Bronze ZIPs >= {_PROVIDER_DATA_MIN_BYTES:,} bytes detected (sub-flag 1b drift): {too_big}"
    )
