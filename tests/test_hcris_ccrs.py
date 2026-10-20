"""Phase 6 (Session 9.5) tests for ``data/silver/national_ccr.parquet``.

Validates the joined CCR table shape, FY coverage (FY2015-FY2026), 19-cost-center
group canonicalization, and per-CCR sanity bands (each CCR in [0.01, 1.0]).
Skips when the silver parquet is absent.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import config
from src.transform.hcris_ccrs import (
    _CANONICAL_GROUP_RENAMES,
    _DEFINITION_FALLBACK_FY,
    _FORWARD_FILL_BACK_TO_FY,
    _PARQUET_SCHEMA,
    _canonicalize_group,
    _split_cells,
)

_SILVER = config.SILVER / "national_ccr.parquet"


def _skip_if_missing(p: Path, label: str) -> None:
    if not p.exists():
        pytest.skip(f"{label} not present at {p}")


def test_module_importable() -> None:
    from src.transform import hcris_ccrs

    assert hasattr(hcris_ccrs, "build_national_ccr")


def test_split_cells_extracts_tokens() -> None:
    assert _split_cells("C_1_C5_30") == ["C_1_C5_30"]
    assert _split_cells("C_1_C6_30, C_1_C7_30") == ["C_1_C6_30", "C_1_C7_30"]
    assert _split_cells("C_1_C5_92.01") == ["C_1_C5_92.01"]
    assert _split_cells(None) == []
    assert _split_cells("") == []
    # Dedup
    assert _split_cells("C_1_C5_30 C_1_C5_30") == ["C_1_C5_30"]


def test_canonicalize_group_handles_renames() -> None:
    """FY2024+ verbose names fold back to FY2021-FY2023 canonical short form."""
    assert _canonicalize_group("Operating Room (excluding Labor & Delivery)") == "Operating Room"
    assert _canonicalize_group("Cardiac Catheteri-zation") == "Cardiac Catheterization"
    assert _canonicalize_group("Drugs") == "Drugs"
    # Whitespace normalization
    assert _canonicalize_group("Drugs  ") == "Drugs"


def test_canonicalize_group_renames_constant() -> None:
    """All keys in ``_CANONICAL_GROUP_RENAMES`` map to a non-self target."""
    for raw, canonical in _CANONICAL_GROUP_RENAMES.items():
        assert raw != canonical


def test_constants_consistent() -> None:
    assert _FORWARD_FILL_BACK_TO_FY == 2008
    assert _DEFINITION_FALLBACK_FY == 2021


def test_split_cells_handles_2552_96_cell_tokens() -> None:
    """Widened _CELL_TOKEN_RE must capture Form 2552-96 alphanumeric tokens."""
    # Alphanumeric column suffix (e.g., C_1_C5A_30 from FY2008-FY2010 inputs)
    assert _split_cells("C_1_C5A_30") == ["C_1_C5A_30"]
    # Alpha line indicator (e.g., D0_HOS_C2_A185)
    assert _split_cells("D0_HOS_C2_A185") == ["D0_HOS_C2_A185"]
    # Worksheet D-1 / D-4 prefixes
    assert _split_cells("D1_HOS_C2_A181") == ["D1_HOS_C2_A181"]
    assert _split_cells("D4_HOS_C2_A180") == ["D4_HOS_C2_A180"]
    # Backward-compatible: Form 2552-10 still parses cleanly
    assert _split_cells("C_1_C5_30, D3_HOS_C2_30") == ["C_1_C5_30", "D3_HOS_C2_30"]


def test_silver_schema() -> None:
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    pf = pq.ParquetFile(_SILVER)
    assert list(pf.schema_arrow.names) == [f.name for f in _PARQUET_SCHEMA]


def test_silver_row_count_floor() -> None:
    """19 FYs × 19 cost-center groups = 361 rows (FY2008-FY2026; Session 1.5 shipped the FY2008-FY2014 backfill via Form 2552-96 worksheet mapper)."""
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    table = pq.read_table(_SILVER)
    assert table.num_rows == 361, (
        f"Expected 361 rows (19 FYs × 19 groups), got {table.num_rows}."
    )


def test_silver_per_fy_19_groups() -> None:
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    fys = sorted(df["fy"].unique().tolist())
    assert fys == list(range(2008, 2027)), f"Expected FY2008-FY2026, got {fys}"
    for fy in fys:
        n = len(df[df["fy"] == fy])
        assert n == 19, f"FY{fy}: {n} cost-center groups (expected 19)"


def test_silver_canonical_groups_only() -> None:
    """Verbose FY2024+ names should NOT appear post-canonicalization."""
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    seen = set(df["cost_center_group"].astype(str).unique())
    for raw in _CANONICAL_GROUP_RENAMES:
        assert raw not in seen, f"Verbose group name {raw!r} leaked into silver"


def test_silver_ccr_value_in_band() -> None:
    """National CCRs should land in [0.01, 1.0]; outside that band suggests a join bug.

    Form 2552-96 era (FY2008-FY2010) "Other Services" is a known exception:
    the 2552-96 cost-center aggregation pools cost from many sub-cost-centers
    but charges from fewer, yielding CCR ~3.0-3.3. Documented in
    plans/hcris-url-discovery.md "Session 1.5 Form 2552-96 discovery".
    """
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    sub = df[df["ccr_value"].notna()]
    assert len(sub) > 0
    out_of_band = sub[(sub["ccr_value"] < 0.01) | (sub["ccr_value"] > 1.0)]
    # Allow the Form 2552-96 era "Other Services" exception.
    out_of_band = out_of_band[~(
        out_of_band["fy"].isin([2008, 2009, 2010]) &
        (out_of_band["cost_center_group"] == "Other Services")
    )]
    assert out_of_band.empty, (
        f"{len(out_of_band)} CCRs outside [0.01, 1.0] (excluding the documented "
        f"Form 2552-96 'Other Services' exception): "
        f"{out_of_band[['fy', 'cost_center_group', 'ccr_value']].to_dict('records')[:5]}"
    )


def test_silver_operating_room_ccr_present_every_fy() -> None:
    """Operating Room is the inpatient default cost-center; must exist for every FY."""
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    or_df = df[df["cost_center_group"] == "Operating Room"]
    assert len(or_df) == 19, (
        f"Expected 19 'Operating Room' rows (one per FY2008-FY2026; Session 1.5 shipped FY2008-FY2014 backfill), got {len(or_df)}"
    )
    assert or_df["ccr_value"].notna().all()


def test_silver_n_providers_floor() -> None:
    """Every group should reflect >=1000 providers (HCRIS covers ~6,000 hospitals).

    Form 2552-96 era (FY2008-FY2010) exception: 4 cost-center groups have no
    dedicated Worksheet C lines in 2552-96 (they are sub-codes within other
    groups) so n_providers = 0 for those (fy, group) pairs:
      - Cardiac Catheterization (HCRIS code 3120-3129; sub-code of line 53)
      - Computed Tomography (CT) Scan (HCRIS code 3230-3239; sub-code of line 41)
      - Magnetic Resonance Imaging (MRI) (HCRIS code 3430-3439; sub-code of line 41)
      - Implantable Devices (HCRIS code 5530-5539; not a worksheet C line)
    Documented in plans/hcris-url-discovery.md "Session 1.5 Form 2552-96 discovery".
    """
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    _form_2552_96_zero_provider_exceptions = {
        (fy, grp)
        for fy in (2008, 2009, 2010)
        for grp in (
            "Cardiac Catheterization",
            "Computed Tomography (CT) Scan",
            "Magnetic Resonance Imaging (MRI)",
            "Implantable Devices",
        )
    }
    low = df[df["n_providers"] < 1000]
    leaked = low[~low.apply(
        lambda r: (r["fy"], r["cost_center_group"]) in _form_2552_96_zero_provider_exceptions,
        axis=1,
    )]
    assert leaked.empty, (
        f"{len(leaked)} (fy, cost_center_group) rows with n_providers < 1000 "
        f"(excluding the documented Form 2552-96 zero-provider exceptions): "
        f"{leaked[['fy', 'cost_center_group', 'n_providers']].to_dict('records')[:5]}"
    )


def test_silver_source_columns() -> None:
    _skip_if_missing(_SILVER, "national_ccr.parquet")
    df = pq.read_table(_SILVER).to_pandas()
    assert df["source"].eq("hcris_ccr_national").all()
    assert df["source_host"].eq("cms_gov_hcris").all()
