"""Phase 5 (Batch 2c) tests for the Medicare Outpatient PUF extractor.

Mirrors ``tests/test_opps.py`` skip pattern:

  - Module-level ``pytest.importorskip`` for ``src.extract.puf_outpatient`` so
    test collection doesn't ImportError before the extractor is on disk.
  - Data-dependent assertions skip via ``pytest.skip`` when the per-CY parquet
    isn't on disk yet (so the suite passes pre- and post-extract).

Test target: CY2023 (RY25) — most recent published year.
"""

from __future__ import annotations

import re

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

pytest.importorskip("src.extract.puf_outpatient")

_CY = 2023
_PARQUET = (
    config.SILVER_PER_SOURCE / "puf" / "outpatient" / f"puf_outpatient_cy{_CY}.parquet"
)

_EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "scope",
        "code",
        "code_system",
        "year",
        "place_of_srvc",
        "n",
        "mean_charge",
        "mean_payment",
        "payment_amount",
        "payment_amount_concept",
        "total_payment_amount",
        "mean_payment_stdzd",
        "p10_charge",
        "p25_charge",
        "p50_charge",
        "p75_charge",
        "p90_charge",
        "p10_payment",
        "p25_payment",
        "p50_payment",
        "p75_payment",
        "p90_payment",
        "n_providers",
        "source",
        "source_host",
        "effective_start",
        "effective_end",
    }
)

_APC_RE = re.compile(r"^[0-9]{4,5}$")


def _skip_if_missing() -> None:
    if not _PARQUET.exists():
        pytest.skip(
            f"{_PARQUET} not present; run "
            f"`python -m src.extract.puf_outpatient --cy {_CY}` first"
        )


# ---------------------------------------------------------------------------
# Data-independent tests
# ---------------------------------------------------------------------------


def test_extract_module_importable() -> None:
    """``src.extract.puf_outpatient`` exports the documented public API."""
    from src.extract import puf_outpatient

    assert hasattr(puf_outpatient, "extract_cy"), (
        "src.extract.puf_outpatient must export `extract_cy`"
    )
    assert hasattr(puf_outpatient, "extract_cy_range"), (
        "src.extract.puf_outpatient must export `extract_cy_range`"
    )


def test_url_dict_covers_expected_range() -> None:
    """Every CY in ``config.PUF_OUTPATIENT_CYS`` is keyed in the module-local
    URL dict, and at least one populated entry exists.
    """
    from src.extract import puf_outpatient

    url_dict = puf_outpatient._PUF_OUTPATIENT_SOURCES
    expected = set(config.PUF_OUTPATIENT_CYS)
    keyed = set(url_dict.keys())
    missing = expected - keyed
    assert not missing, (
        f"PUF_OUTPATIENT URL dict missing CYs: {sorted(missing)}"
    )
    populated = [cy for cy, url in url_dict.items() if url]
    assert len(populated) >= 1, "URL dict has zero populated entries"


def test_resolver_returns_known_hosts() -> None:
    """``_resolve_outpatient_url`` returns a (url, source_host) tuple whose
    second element is one of {cms_gov, nber, wayback}, or None when every
    tier misses. Documents the locked NBER fallback for CY2011-CY2013 and
    the unrecoverable status of CY2014.
    """
    from src.extract import puf_outpatient

    valid_hosts = {"cms_gov", "nber", "wayback"}
    # CMS-era cells (CY2015+) resolve to cms_gov.
    for cy in (2015, 2020, 2023):
        out = puf_outpatient._resolve_outpatient_url(cy)
        assert out is not None, f"CY{cy} should resolve via cms.gov"
        url, host = out
        assert host == "cms_gov", f"CY{cy} expected cms_gov, got {host}"
        assert url.startswith("https://data.cms.gov/"), url

    # NBER fallback for CY2011-CY2013.
    for cy in (2011, 2012, 2013):
        out = puf_outpatient._resolve_outpatient_url(cy)
        assert out is not None, f"CY{cy} should resolve via NBER fallback"
        url, host = out
        assert host == "nber", f"CY{cy} expected nber, got {host}"
        assert url.startswith("https://data.nber.org/providerchargepuf/"), url
        assert host in valid_hosts

    # CY2014: unrecoverable as of 2026-05-01 (no CMS, no NBER, no Wayback).
    assert puf_outpatient._resolve_outpatient_url(2014) is None


def test_resolve_columns_missing_required_raises() -> None:
    """``_resolve_columns`` raises RuntimeError when a required logical column
    has no header alias hit in the input.
    """
    from src.extract.puf_outpatient import _resolve_columns

    # Headers omit any APC column -> required "apc_cd" cannot resolve.
    bogus_headers = ["Provider_Id", "Tot_Srvcs", "Avg_Submtd_Chrg"]
    with pytest.raises(RuntimeError, match="required columns"):
        _resolve_columns(bogus_headers)


def test_percentiles_null_when_n_providers_under_11() -> None:
    """``_mask_quantiles_low_n_providers`` masks per-provider quantiles to
    NULL whenever ``n_providers`` is below the CMS-suppression threshold
    (11). Data-independent in-memory roundtrip.
    """
    import pandas as pd
    from src.extract.puf_outpatient import _mask_quantiles_low_n_providers

    df = pd.DataFrame(
        {
            "code": ["0001", "0002"],
            "n": [100, 100],
            "mean_charge": [1.0, 2.0],
            "mean_payment": [1.0, 2.0],
            "payment_amount": [1.0, 2.0],
            "p10_charge": [10.0, 10.0],
            "p25_charge": [10.0, 10.0],
            "p50_charge": [10.0, 10.0],
            "p75_charge": [10.0, 10.0],
            "p90_charge": [10.0, 10.0],
            "p10_payment": [5.0, 5.0],
            "p25_payment": [5.0, 5.0],
            "p50_payment": [5.0, 5.0],
            "p75_payment": [5.0, 5.0],
            "p90_payment": [5.0, 5.0],
            "n_providers": [5, 50],
        }
    )
    out = _mask_quantiles_low_n_providers(df)

    qcols = [c for c in out.columns if c.startswith(("p10_", "p25_", "p50_", "p75_", "p90_"))]
    # Row 0 (n_providers=5): all percentile cols masked to NULL.
    for c in qcols:
        assert pd.isna(out.loc[0, c]), f"row 0 {c} should be NULL when n_providers < 11"
    # Row 1 (n_providers=50): percentile cols preserved.
    for c in qcols:
        assert not pd.isna(out.loc[1, c]), (
            f"row 1 {c} should NOT be masked when n_providers >= 11"
        )
    # Mean columns unaffected on both rows.
    for c in ("mean_charge", "mean_payment", "payment_amount"):
        assert not pd.isna(out.loc[0, c])
        assert not pd.isna(out.loc[1, c])


# ---------------------------------------------------------------------------
# Data-dependent tests (skip when parquet missing)
# ---------------------------------------------------------------------------


def test_silver_schema_correct() -> None:
    """The CY2023 Outpatient PUF parquet has the locked 27-column schema."""
    _skip_if_missing()

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual
    extra = actual - _EXPECTED_COLUMNS
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"
    assert not extra, f"Unexpected columns in {_PARQUET.name}: {sorted(extra)}"


def test_apc_codes_4_or_5_digits() -> None:
    """Every emitted ``code`` matches the post-pad APC regex ``^[0-9]{4,5}$``."""
    _skip_if_missing()

    con = duckdb.connect()
    bad = con.execute(
        f"""
        SELECT code FROM read_parquet('{_PARQUET}')
        WHERE NOT REGEXP_MATCHES(code, '^[0-9]{{4,5}}$')
        LIMIT 5
        """
    ).fetchall()
    con.close()
    assert not bad, f"APC codes failing regex {_APC_RE.pattern}: {bad}"


def test_code_system_is_apc() -> None:
    """Every row's ``code_system`` is the literal ``"APC"``."""
    _skip_if_missing()

    con = duckdb.connect()
    distinct = con.execute(
        f"SELECT DISTINCT code_system FROM read_parquet('{_PARQUET}')"
    ).fetchall()
    con.close()
    assert distinct == [("APC",)], (
        f"Expected distinct code_system == [('APC',)], got {distinct}"
    )


def test_apc_count_per_cy_in_band() -> None:
    """The distinct-code count for the CY is in the [25, 70] coverage band.

    Historical drift: NBER CY2011-CY2013 ships 30 APCs; CMS RY19 CY2015-CY2016
    ships 25-33; CMS CY2017+ broadened to 58-68. Plan-v11 documents this as
    known scope, not a landmine.
    """
    _skip_if_missing()

    con = duckdb.connect()
    n = con.execute(
        f"SELECT COUNT(DISTINCT code) FROM read_parquet('{_PARQUET}')"
    ).fetchone()[0]
    con.close()
    assert 25 <= n <= 70, (
        f"APC count {n} for CY{_CY} outside expected band [25, 70] "
        f"-- likely PUF schema drift"
    )


def test_payment_amount_non_null() -> None:
    """Outpatient PUF carries Avg_Mdcr_Alowd_Amt natively, so ``payment_amount``
    must be populated on the vast majority of rows. The Session 8.5 extractor
    drops null rows pre-emit, so the contract is even stronger now (effectively
    100%); the >=90% floor preserves a safety margin against silent drift.
    """
    _skip_if_missing()

    con = duckdb.connect()
    total, non_null = con.execute(
        f"""
        SELECT COUNT(*), COUNT(payment_amount) FROM read_parquet('{_PARQUET}')
        """
    ).fetchone()
    con.close()
    assert total > 0
    # Any reasonable PUF will have >90% payment_amount populated.
    assert non_null / total >= 0.9, (
        f"payment_amount populated on only {non_null}/{total} rows; "
        f"Outpatient PUF should carry Avg_Mdcr_Alowd_Amt natively"
    )


def test_source_host_populated() -> None:
    """Every emitted row has a non-null ``source_host`` whose value is in
    {cms_gov, nber, wayback}. Phase 5 Session 8.5 schema-widening contract.
    """
    _skip_if_missing()

    con = duckdb.connect()
    null_count, distinct_hosts = con.execute(
        f"""
        SELECT
            SUM(CASE WHEN source_host IS NULL THEN 1 ELSE 0 END),
            LIST(DISTINCT source_host)
        FROM read_parquet('{_PARQUET}')
        """
    ).fetchone()
    con.close()
    assert null_count == 0, f"source_host has {null_count} NULL rows"
    valid = {"cms_gov", "nber", "wayback"}
    bogus = set(distinct_hosts) - valid
    assert not bogus, (
        f"source_host has values outside {sorted(valid)}: {sorted(bogus)}"
    )


def test_payment_amount_concept_medicare_allowed_for_cms_era() -> None:
    """For CY2015+ (cms.gov era), payment_amount_concept = 'medicare_allowed'
    and total_payment_amount IS NULL (contract: medicare-allowed is not the
    same concept as the all-payer total). Locked Phase 5 Session 8.5."""
    _skip_if_missing()  # CY2023 parquet — cms.gov era

    con = duckdb.connect()
    try:
        distinct = con.execute(
            f"""
            SELECT DISTINCT payment_amount_concept
            FROM read_parquet('{_PARQUET}')
            """
        ).fetchall()
        concepts = {row[0] for row in distinct}
        assert concepts == {"medicare_allowed"}, (
            f"CMS-era CY{_CY}: concept distinct = {concepts}, "
            f"expected {{'medicare_allowed'}}"
        )
        non_null_total = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{_PARQUET}')
            WHERE total_payment_amount IS NOT NULL
            """
        ).fetchone()[0]
        assert non_null_total == 0, (
            f"CMS-era CY{_CY}: {non_null_total} rows have non-null "
            f"total_payment_amount; expected ALL NULL when "
            f"concept=medicare_allowed"
        )
    finally:
        con.close()


def test_payment_amount_concept_total_payment_for_nber_era() -> None:
    """For NBER-era CY2011-CY2013, payment_amount_concept =
    'total_payment_allpayer' (NBER files publish only Average Total Payments)
    and total_payment_amount = payment_amount on every row."""
    nber_parquets = [
        config.SILVER_PER_SOURCE / "puf" / "outpatient" / f"puf_outpatient_cy{cy}.parquet"
        for cy in (2011, 2012, 2013)
    ]
    extant = [p for p in nber_parquets if p.exists()]
    if not extant:
        pytest.skip("no NBER-era CY2011-CY2013 outpatient PUF parquets present")

    con = duckdb.connect()
    try:
        for parquet in extant:
            distinct = con.execute(
                f"""
                SELECT DISTINCT payment_amount_concept
                FROM read_parquet('{parquet}')
                """
            ).fetchall()
            concepts = {row[0] for row in distinct}
            assert concepts == {"total_payment_allpayer"}, (
                f"{parquet.name}: NBER-era concept distinct = {concepts}, "
                f"expected {{'total_payment_allpayer'}}"
            )
            mismatch = con.execute(
                f"""
                SELECT COUNT(*) FROM read_parquet('{parquet}')
                WHERE total_payment_amount IS DISTINCT FROM payment_amount
                """
            ).fetchone()[0]
            assert mismatch == 0, (
                f"{parquet.name}: {mismatch} rows have "
                f"total_payment_amount != payment_amount despite "
                f"concept=total_payment_allpayer"
            )
    finally:
        con.close()


def test_payment_amount_non_null_all_cys() -> None:
    """payment_amount is NOT NULL on every row across all extant per-CY
    parquets. Locked Phase 5 Session 8.5; extractor drops null rows pre-emit."""
    parquet_dir = config.SILVER_PER_SOURCE / "puf" / "outpatient"
    parquets = sorted(parquet_dir.glob("puf_outpatient_cy*.parquet"))
    if not parquets:
        pytest.skip(f"no parquets in {parquet_dir}")

    con = duckdb.connect()
    try:
        for parquet in parquets:
            n_null = con.execute(
                f"""
                SELECT COUNT(*) FROM read_parquet('{parquet}')
                WHERE payment_amount IS NULL
                """
            ).fetchone()[0]
            assert n_null == 0, (
                f"{parquet.name}: {n_null} rows with NULL payment_amount"
            )
    finally:
        con.close()
