"""Phase 11 (Session 12) tests for the NBER long-form HCRIS extractor.

Validates URL manifest coverage (FY2008-FY2014, two form-versions), per-FY
parquet shape, row-count floors, and column-prefix retention. Skips
data-dependent tests when the FY2008-FY2014 silver parquets are absent
(orchestrator runs the regen after Batch A + Batch B both finish).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from src import config
from src.extract import hcris_data_nber
from src.extract.hcris_data import _KEEP_PREFIXES
from src.extract.hcris_data_nber import _HCRIS_NBER_LONG_URLS, _build_cell_name

_SILVER_DIR = config.SILVER_PER_SOURCE / "hcris" / "data"
_BRONZE = config.BRONZE / "hcris_data_nber"
_NBER_FYS: tuple[int, ...] = tuple(range(2008, 2015))


def test_module_importable() -> None:
    assert hasattr(hcris_data_nber, "extract_fy")
    assert hasattr(hcris_data_nber, "extract_fy_range")


@pytest.mark.parametrize(
    "wksht_cd,clmn_num,line_num,expected",
    [
        # Bare-integer fixture inputs (test convenience).
        ("C000001", 5, 30, "C_1_C5_30"),
        ("C000001", 5.0, 30.0, "C_1_C5_30"),  # float-cast invariant
        ("C000001", 6, 92, "C_1_C6_92"),
        ("D300000", 2, 30, "D3_HOS_C2_30"),
        ("D300001", 2, 30, "D3_HOS_C2_30"),  # any D3* prefix routes the same
        # NBER 5-char zero-padded format (the format actually on disk for
        # FY2011-FY2014 long-form CSVs).
        ("C000001", "00500", "03000", "C_1_C5_30"),
        ("C000001", "00100", "03000", "C_1_C1_30"),
        ("C000001", "00500", "04201", "C_1_C5_4201"),  # subline 01 -> SAS concat
        ("D30A180", "00200", "03000", "D3_HOS_C2_30"),  # any D3* sub-variant
        ("D30B180", "00200", "09201", "D3_HOS_C2_9201"),
    ],
)
def test_build_cell_name_invariants(wksht_cd, clmn_num, line_num, expected):
    """Worksheet-prefix dispatch + NBER zero-pad decoding on Form 2552-10 path."""
    assert _build_cell_name(wksht_cd, clmn_num, line_num) == expected


@pytest.mark.parametrize("wksht_cd", ["A000000", "B000000", "Z000000", ""])
def test_build_cell_name_unknown_wksht_returns_none(wksht_cd):
    """Unknown wksht_cd prefixes must not produce a cell name."""
    out = _build_cell_name(wksht_cd, 5, 30)
    assert out is None


@pytest.mark.parametrize(
    "wksht_cd,clmn_num,line_num",
    [
        # FY2008 NBER long-form carries alphanumeric sub-line indicators like
        # "5A00" / "5B00" — must be dropped, not raise.
        ("C000001", "5A00", "30"),
        ("C000001", "5", "30A"),
        ("C000001", None, 30),
        ("C000001", "", "30"),
    ],
)
def test_build_cell_name_alphanumeric_inputs_return_none(
    wksht_cd, clmn_num, line_num
):
    """Sub-line / alpha column indicators must drop cleanly (FY2008 landmine)."""
    assert _build_cell_name(wksht_cd, clmn_num, line_num) is None


def test_url_manifest_covers_fy2008_to_fy2014() -> None:
    fys = sorted(_HCRIS_NBER_LONG_URLS.keys())
    assert fys == list(range(2008, 2015)), f"FYs in manifest: {fys}"
    for fy, url in _HCRIS_NBER_LONG_URLS.items():
        assert url.startswith("https://data.nber.org/hcris/"), f"FY{fy}: {url}"
        assert url.endswith(".csv"), f"FY{fy}: {url}"
    # Form 2552-96 covers FY2008-FY2010; Form 2552-10 covers FY2011+.
    for fy in (2008, 2009, 2010):
        assert "2552-96" in _HCRIS_NBER_LONG_URLS[fy], (
            f"FY{fy} expected Form 2552-96, got {_HCRIS_NBER_LONG_URLS[fy]}"
        )
    for fy in (2011, 2012, 2013, 2014):
        assert "2552-10" in _HCRIS_NBER_LONG_URLS[fy], (
            f"FY{fy} expected Form 2552-10, got {_HCRIS_NBER_LONG_URLS[fy]}"
        )


def test_dispatcher_routes_pre_fy2015_to_nber() -> None:
    """``hcris_data.extract_fy(2014)`` must delegate to ``hcris_data_nber``."""
    from src.extract import hcris_data

    with patch(
        "src.extract.hcris_data_nber.extract_fy",
        return_value=None,
    ) as mock_nber:
        hcris_data.extract_fy(2014)
        mock_nber.assert_called_once()
        kwargs = mock_nber.call_args.kwargs
        args = mock_nber.call_args.args
        # Either positional fy=2014 or kwargs fy=2014; tolerate both.
        assert (args and args[0] == 2014) or kwargs.get("fy") == 2014, (
            f"Expected fy=2014, got args={args!r} kwargs={kwargs!r}"
        )


@pytest.mark.parametrize("fy", [2008, 2009, 2010])
def test_extract_fy_form_2552_96_runs_through_2552_96_mapper(fy):
    """FY2008-FY2010 (Form 2552-96) routes through ``_build_cell_name_2552_96``
    instead of short-circuiting (Session 1.5, 2026-05-02).

    Validates that:
      * the 2552-96 mapper exists and is callable;
      * the FY-set sentinel covers exactly FY2008-FY2010;
      * ``extract_fy`` actually reaches the download / pivot codepath (no
        early-return) and passes ``fy`` through to ``_load_and_pivot`` so
        the form-version dispatch can fire inside the pivot helper.

    ``download_to_bronze``, ``_load_and_pivot``, AND ``pq.write_table`` are
    mocked so the test never writes a stub silver to the real data dir
    (Session 1.5 fix-flags pass — earlier revision leaked 1-row stub
    parquets that broke row-count-band tests downstream).
    """
    import pandas as pd
    from src.extract import _http
    from src.extract.hcris_data_nber import _build_cell_name_2552_96

    # Mapper exists + is wired into the dispatcher.
    assert callable(_build_cell_name_2552_96)
    assert hcris_data_nber._FORM_2552_96_FYS == frozenset({2008, 2009, 2010})

    with patch.object(_http, "download_to_bronze") as mock_download, \
         patch.object(hcris_data_nber, "_load_and_pivot") as mock_pivot, \
         patch.object(hcris_data_nber.pq, "write_table") as mock_write:
        mock_download.return_value = Path("/tmp/fake_2552_96.csv")
        mock_pivot.return_value = pd.DataFrame({"rpt_rec_num": ["FAKE-RPT-1"]})
        try:
            hcris_data_nber.extract_fy(fy)
        except Exception:
            # Any downstream error on the toy frame is fine; we only care
            # that download + pivot were invoked (i.e., no short-circuit /
            # no early return).
            pass

    mock_download.assert_called_once()
    mock_pivot.assert_called_once()
    # Confirm pivot received the FY arg so its dispatcher can route to the
    # 2552-96 mapper. Tolerate both positional and keyword passing.
    pivot_args = mock_pivot.call_args.args
    pivot_kwargs = mock_pivot.call_args.kwargs
    received_fy = (
        pivot_args[1] if len(pivot_args) >= 2 else pivot_kwargs.get("fy")
    )
    assert received_fy == fy, (
        f"Expected fy={fy} passed to _load_and_pivot, "
        f"got args={pivot_args!r} kwargs={pivot_kwargs!r}"
    )
    # No silver should have been written to the real data dir. The mock
    # absorbs any write attempt so the silver dir is left clean for the
    # real Batch-2 NBER pull.
    assert not (_SILVER_DIR / f"hcris_data_fy{fy}.parquet").exists() or \
        pq.ParquetFile(_SILVER_DIR / f"hcris_data_fy{fy}.parquet").metadata.num_rows > 1, (
        f"Stub parquet leaked to {_SILVER_DIR}/hcris_data_fy{fy}.parquet"
    )


@pytest.mark.parametrize(
    "wksht_cd,clmn_num,line_num,expected",
    [
        # Worksheet C Part 1 — same shape as 2552-10 for digit-only inputs
        ("C000001", "5", "30", "C_1_C5_30"),
        ("C000001", "6", "92", "C_1_C6_92"),
        # Alphanumeric column ("5A00" -> "5A" after trailing-zero strip)
        ("C000001", "5A00", "30", "C_1_C5A_30"),
        ("C000001", "5B00", "30", "C_1_C5B_30"),
        # Alpha line indicator preserved literally on D-0/D-1/D-4
        ("D000001", "2", "A185", "D0_HOS_C2_A185"),
        ("D100001", "2", "A181", "D1_HOS_C2_A181"),
        ("D400001", "2", "A180", "D4_HOS_C2_A180"),
        # Any DXX prefix routes the same way (defensive prefix match)
        ("D000099", "2", "30", "D0_HOS_C2_30"),
    ],
)
def test_build_cell_name_2552_96_invariants(
    wksht_cd, clmn_num, line_num, expected
):
    """Form 2552-96 mapper preserves alphanumeric tokens + worksheet topology."""
    from src.extract.hcris_data_nber import _build_cell_name_2552_96
    assert _build_cell_name_2552_96(wksht_cd, clmn_num, line_num) == expected


@pytest.mark.parametrize(
    "wksht_cd,clmn_num,line_num",
    [
        # Unknown worksheet families -> drop
        ("A000000", "1", "1"),
        ("Z000000", "5", "30"),
        ("D200000", "5", "30"),  # D-2 is not in the 2552-96 retain set
        # Missing components -> drop
        ("C000001", "", "30"),
        ("C000001", "5", ""),
        ("C000001", None, "30"),
        ("", "5", "30"),
    ],
)
def test_build_cell_name_2552_96_unknown_returns_none(
    wksht_cd, clmn_num, line_num
):
    """Form 2552-96 mapper returns None on unknown wksht_cd / missing tokens."""
    from src.extract.hcris_data_nber import _build_cell_name_2552_96
    assert _build_cell_name_2552_96(wksht_cd, clmn_num, line_num) is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        # FY2011+ (Form 2552-10) NBER format: 5-char zero-padded numeric.
        # Last 2 chars are the sub-indicator; "00" means bare line/column.
        ("00100", "1"),    # column 1, sub 00 -> bare
        ("00500", "5"),
        ("03000", "30"),   # line 30, sub 00 -> bare
        ("03100", "31"),
        ("03001", "3001"), # line 30, sub 01 -> SAS concat form (no dot)
        ("04201", "4201"), # line 42, sub 01 -> matches SAS C_1_C5_4201 column
        ("09201", "9201"), # line 92, sub 01 -> matches SAS C_1_C5_9201 column
        ("55300", "553"),  # line 553, sub 00 -> bare
        # FY2008/2552-96 NBER format: 4-char zero-padded numeric.
        ("0100", "1"),     # column 1
        ("0200", "2"),
        ("0500", "5"),
        ("02500", "25"),   # line 25 (Routine Days)
        ("03700", "37"),   # line 37 (Operating Room)
        # Alphanumeric (FY2008/2552-96 sub-column letters).
        ("5A00", "5A"),    # column 5A, sub 00 -> bare
        ("5B00", "5B"),
        ("5A01", "5A01"),  # column 5A, sub 01 -> concat
        ("A185", "A185"),  # leading alpha (hypothetical) -> literal
        # Numeric Python types (test fixture compatibility — real NBER is always string).
        (5, "5"),
        (5.0, "5"),
        (30, "30"),
        # Edge cases.
        ("", None),
        (None, None),
    ],
)
def test_decode_nber_token(raw, expected):
    """Decoder for NBER zero-padded line/column tokens.

    Validates the round-trip from NBER long-form encoding to SAS-shape
    canonical form for both Form 2552-10 (FY2011+, 5-char tokens) and
    Form 2552-96 (FY2008-FY2010, 4-char tokens with occasional alpha
    sub-column letters).
    """
    from src.extract.hcris_data_nber import _decode_nber_token
    assert _decode_nber_token(raw) == expected


def _nber_silver_paths_present() -> list[Path]:
    if not _SILVER_DIR.exists():
        return []
    paths: list[Path] = []
    for fy in _NBER_FYS:
        p = _SILVER_DIR / f"hcris_data_fy{fy}.parquet"
        if p.exists():
            paths.append(p)
    return paths


def test_silver_per_fy_row_count_band() -> None:
    """Each NBER-derived per-FY parquet should carry >= 3,500 provider reports.

    FY2008-FY2009 + FY2011-FY2014 land 6,000-6,200 provider rows; FY2010 is the
    transitional year between Form 2552-96 and Form 2552-10 with only ~3,800
    provider reports filed (NBER source CSV is 240 MB vs ~400-800 MB for
    neighboring FYs). 3,500 floor accommodates that one outlier.
    """
    paths = _nber_silver_paths_present()
    if not paths:
        pytest.skip("No FY2008-FY2014 hcris_data parquets present (NBER pull deferred)")
    for p in paths:
        n = pq.ParquetFile(p).metadata.num_rows
        assert n >= 3_500, (
            f"{p.name}: {n} provider rows below 3,500 floor "
            "(FY2010 transitional year ~3,800; other FYs ~6k)"
        )


def test_silver_keep_prefixes_retained() -> None:
    """FY2014 NBER-derived parquet must retain >= 30 cols of each kept prefix."""
    paths = _nber_silver_paths_present()
    if not paths:
        pytest.skip("No FY2008-FY2014 hcris_data parquets present (NBER pull deferred)")
    fy2014 = _SILVER_DIR / "hcris_data_fy2014.parquet"
    if not fy2014.exists():
        pytest.skip("FY2014 NBER-derived parquet not present")
    cols = pq.ParquetFile(fy2014).schema_arrow.names
    counts = {prefix: sum(1 for c in cols if c.startswith(prefix)) for prefix in _KEEP_PREFIXES}
    for prefix, count in counts.items():
        assert count >= 30, f"{fy2014.name}: only {count} columns matching prefix {prefix!r}"


def test_silver_constant_columns_present() -> None:
    """NBER-derived parquets carry the same constants as SAS-derived parquets.

    ``source`` value should be ``"hcris_data_nber"`` (not ``"hcris_data"``) so
    downstream can distinguish the two eras.
    """
    paths = _nber_silver_paths_present()
    if not paths:
        pytest.skip("No FY2008-FY2014 hcris_data parquets present (NBER pull deferred)")
    expected_constants = {"fy", "source", "effective_start", "effective_end"}
    for p in paths:
        cols = set(pq.ParquetFile(p).schema_arrow.names)
        missing = expected_constants - cols
        assert not missing, f"{p.name}: missing constants {missing}"
        # source column value
        df = pq.read_table(p, columns=["source"]).to_pandas()
        assert df["source"].eq("hcris_data_nber").all(), (
            f"{p.name}: expected source='hcris_data_nber', got {df['source'].unique().tolist()}"
        )


def test_silver_prvdr_num_aliases_rpt_rec_num() -> None:
    """Schema interop contract: NBER long-form has no prvdr_num column, so
    we alias it to rpt_rec_num so hcris_ccrs.py line 197 can SELECT prvdr_num
    without a special case for the NBER FYs."""
    paths = _nber_silver_paths_present()
    if not paths:
        pytest.skip("No NBER-derived hcris_data_fy*.parquet present")
    for p in paths:
        # Only test paths whose source column is hcris_data_nber (not the SAS path).
        df_src = pq.read_table(p, columns=["source"]).to_pandas()
        if df_src["source"].iloc[0] != "hcris_data_nber":
            continue
        df = pq.read_table(p, columns=["rpt_rec_num", "prvdr_num"]).to_pandas()
        assert df["rpt_rec_num"].equals(df["prvdr_num"]), (
            f"{p.name}: prvdr_num column does not equal rpt_rec_num "
            "(NBER schema-interop contract violated)"
        )
