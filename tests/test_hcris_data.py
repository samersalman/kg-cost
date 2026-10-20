"""Phase 6 (Session 9.5) tests for the HCRIS provider-level data silvers.

Validates per-FY parquet shape, row-count floors (~6,000 hospitals per FY), and
column-prefix retention (the trimmed schema keeps Worksheet C / D-3 cells only).
Skips data-dependent tests when ``pyreadstat`` is unavailable or the silver
parquets are absent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import config

pyreadstat = pytest.importorskip("pyreadstat")

from src.extract.hcris_data import (  # noqa: E402
    _HCRIS_DATA_URLS,
    _ID_COLS,
    _KEEP_PREFIXES,
    _PROVIDER_DATA_MIN_BYTES,
)

_SILVER_DIR = config.SILVER_PER_SOURCE / "hcris" / "data"
_BRONZE = config.BRONZE / "hcris_data"


def test_module_importable() -> None:
    from src.extract import hcris_data

    assert hasattr(hcris_data, "extract_fy")


def test_url_manifest_covers_fy2015_to_fy2026() -> None:
    fys = sorted(_HCRIS_DATA_URLS.keys())
    assert fys == list(range(2015, 2027)), f"FYs in manifest: {fys}"
    for fy, url in _HCRIS_DATA_URLS.items():
        assert url.startswith("https://www.cms.gov/"), f"FY{fy}: {url}"
        assert url.endswith(".zip"), f"FY{fy}: {url}"


def test_dispatcher_routes_pre_fy2015_to_nber() -> None:
    """Phase 11: hcris_data.extract_fy(<=2014) must delegate to hcris_data_nber."""
    from unittest.mock import patch

    from src.extract import hcris_data

    with patch(
        "src.extract.hcris_data_nber.extract_fy",
        return_value=None,
    ) as mock_nber:
        hcris_data.extract_fy(2014)
        mock_nber.assert_called_once()
        args = mock_nber.call_args.args
        kwargs = mock_nber.call_args.kwargs
        assert (args and args[0] == 2014) or kwargs.get("fy") == 2014, (
            f"Expected fy=2014, got args={args!r} kwargs={kwargs!r}"
        )


def test_bronze_size_sniff_provider_data_above_floor() -> None:
    """Sub-flag 1b: every bronze ZIP under hcris_data/ must be >= 1 MB."""
    if not _BRONZE.exists():
        pytest.skip(f"{_BRONZE} not yet populated")
    sizes: list[tuple[Path, int]] = []
    for zip_path in _BRONZE.rglob("*.zip"):
        sizes.append((zip_path, zip_path.stat().st_size))
    if not sizes:
        pytest.skip("No bronze ZIPs present")
    too_small = [(p, s) for p, s in sizes if s < _PROVIDER_DATA_MIN_BYTES]
    assert not too_small, (
        f"Bronze ZIPs < {_PROVIDER_DATA_MIN_BYTES:,} bytes detected (sub-flag 1b drift): {too_small}"
    )


def _silver_paths_present() -> list[Path]:
    if not _SILVER_DIR.exists():
        return []
    return sorted(_SILVER_DIR.glob("hcris_data_fy*.parquet"))


def test_silver_per_fy_row_count_band() -> None:
    paths = _silver_paths_present()
    if not paths:
        pytest.skip("No hcris_data_fy*.parquet present")
    for p in paths:
        n = pq.ParquetFile(p).metadata.num_rows
        # FY2010 NBER long-form carries ~3,800 provider rows (smaller mid-
        # 2552-96/2552-10-transition cohort year; NBER source CSV is 240 MB
        # vs ~400-800 MB for neighboring FYs). Floor lowered to 3,500 to
        # accommodate.
        assert 3_500 <= n <= 8_000, (
            f"{p.name}: {n} provider rows outside [3500, 8000] band "
            "(IPPS provider count has hovered around 6,000 for years; "
            "FY2010 NBER long-form runs ~3,800)"
        )


def test_silver_keep_prefixes_retained() -> None:
    paths = _silver_paths_present()
    if not paths:
        pytest.skip("No hcris_data_fy*.parquet present")
    p = paths[-1]  # newest FY is the most likely to drift
    cols = pq.ParquetFile(p).schema_arrow.names
    # At least some C_1_C5_*, C_1_C6_*, D3_HOS_C2_* columns should survive
    # the trim. We don't pin exact line numbers since the cell-set drifts
    # FY-over-FY (Cell Therapy Drugs added FY2026 etc).
    counts = {prefix: sum(1 for c in cols if c.startswith(prefix)) for prefix in _KEEP_PREFIXES}
    for prefix, count in counts.items():
        assert count >= 30, f"{p.name}: only {count} columns matching prefix {prefix!r}"


def test_silver_constant_columns_present() -> None:
    paths = _silver_paths_present()
    if not paths:
        pytest.skip("No hcris_data_fy*.parquet present")
    expected_constants = {"fy", "source", "effective_start", "effective_end"}
    for p in paths:
        cols = set(pq.ParquetFile(p).schema_arrow.names)
        missing = expected_constants - cols
        assert not missing, f"{p.name}: missing constants {missing}"


def test_silver_id_columns_present() -> None:
    paths = _silver_paths_present()
    if not paths:
        pytest.skip("No hcris_data_fy*.parquet present")
    p = paths[-1]
    cols = set(pq.ParquetFile(p).schema_arrow.names)
    # rpt_rec_num + prvdr_num are the load-bearing identifiers; the others
    # are nice-to-have. We only assert on the load-bearing two so a future
    # SAS-release rename to e.g. fy_bgn_dt -> bgn_dt doesn't break the test.
    assert "rpt_rec_num" in cols
    assert "prvdr_num" in cols


def test_silver_fy_matches_filename() -> None:
    paths = _silver_paths_present()
    if not paths:
        pytest.skip("No hcris_data_fy*.parquet present")
    pat = re.compile(r"hcris_data_fy(\d{4})\.parquet$")
    for p in paths:
        m = pat.search(p.name)
        assert m, f"Cannot parse FY from filename {p.name}"
        fy_from_name = int(m.group(1))
        df_fys = set(
            pq.read_table(p, columns=["fy"]).to_pandas()["fy"].astype(int).unique().tolist()
        )
        assert df_fys == {fy_from_name}, (
            f"{p.name}: filename FY={fy_from_name} but parquet contains FYs {df_fys}"
        )


def test_id_cols_constant_set() -> None:
    """Schema lock: ``_ID_COLS`` matches our expected identifier list."""
    assert _ID_COLS == (
        "rpt_rec_num",
        "prvdr_num",
        "rpt_stus_cd",
        "fy_bgn_dt",
        "fy_end_dt",
    )


def test_keep_prefixes_constant_set() -> None:
    """Schema lock: ``_KEEP_PREFIXES`` covers cost (col 5), charges (cols 6/7), Medicare charges (D-3)."""
    assert _KEEP_PREFIXES == (
        "C_1_C5_",
        "C_1_C6_",
        "C_1_C7_",
        "D3_HOS_C2_",
    )
