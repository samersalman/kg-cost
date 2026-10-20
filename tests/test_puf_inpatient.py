"""Phase 5 (Batch 2a) tests for the Medicare Inpatient PUF extractor.

Mirrors ``tests/test_puf_outpatient.py`` skip pattern:

  - Module-level ``pytest.importorskip`` for ``src.extract.puf_inpatient`` so
    test collection doesn't ImportError before the extractor is on disk.
  - Data-dependent assertions skip via ``pytest.skip`` when the per-FY parquet
    isn't on disk yet (so the suite passes pre- and post-extract).

Test target: FY2023 (RY25 / DY23) — most recent published year that lives
under the modern ``MUP_INP_RY*_..._PrvSvc.CSV`` slug.
"""

from __future__ import annotations

import re

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

pytest.importorskip("src.extract.puf_inpatient")

_FY = 2023
_PARQUET_DIR = config.SILVER_PER_SOURCE / "puf" / "inpatient"
_PARQUETS = sorted(_PARQUET_DIR.glob("puf_inpatient_fy*.parquet"))
_PARQUET = _PARQUET_DIR / f"puf_inpatient_fy{_FY}.parquet"

_EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "scope", "code", "code_system", "year", "place_of_srvc", "n",
        "mean_charge", "mean_payment", "payment_amount",
        "payment_amount_concept", "total_payment_amount", "mean_payment_stdzd",
        "p10_charge", "p25_charge", "p50_charge", "p75_charge", "p90_charge",
        "p10_payment", "p25_payment", "p50_payment", "p75_payment", "p90_payment",
        "n_providers", "source", "source_host", "effective_start", "effective_end",
    }
)

_DRG_RE = re.compile(r"^[0-9]{3}$")


def _skip_if_missing() -> None:
    if not _PARQUETS:
        pytest.skip(
            f"no inpatient PUF parquets in {_PARQUET_DIR}; "
            f"run `python -m src.extract.puf_inpatient --fy {_FY}` first"
        )


# ---------------------------------------------------------------------------
# Data-independent tests
# ---------------------------------------------------------------------------


def test_extract_module_importable() -> None:
    """``src.extract.puf_inpatient`` exports the documented public API."""
    from src.extract import puf_inpatient

    assert hasattr(puf_inpatient, "extract_fy")
    assert hasattr(puf_inpatient, "extract_fy_range")
    assert hasattr(puf_inpatient, "main")


def test_url_dict_covers_expected_range() -> None:
    """Every FY in ``config.PUF_INPATIENT_FYS`` is keyed in the module-local
    URL dict, and at least one populated entry exists.
    """
    from src.extract import puf_inpatient

    url_dict = puf_inpatient._PUF_INPATIENT_SOURCES
    expected = set(config.PUF_INPATIENT_FYS)
    keyed = set(url_dict.keys())
    missing = expected - keyed
    assert not missing, f"PUF_INPATIENT URL dict missing FYs: {sorted(missing)}"
    populated = [fy for fy, url in url_dict.items() if url]
    assert len(populated) >= 1, "URL dict has zero populated entries"


def test_resolve_columns_missing_required_raises() -> None:
    """``_resolve_columns`` raises RuntimeError when required logical columns
    have no header alias hit, and the message names every missing logical name.
    """
    from src.extract.puf_inpatient import _resolve_columns

    with pytest.raises(RuntimeError) as excinfo:
        _resolve_columns(["foo", "bar"])
    msg = str(excinfo.value)
    for logical in (
        "drg_cd",
        "provider_id",
        "tot_dschrgs",
        "avg_submtd_cvrd_chrg",
        "avg_mdcr_pymt_amt",
        "avg_tot_pymt_amt",
    ):
        assert logical in msg, f"error message must list missing '{logical}': {msg}"


# ---------------------------------------------------------------------------
# Data-dependent tests (skip when parquet missing)
# ---------------------------------------------------------------------------


def test_silver_schema_correct() -> None:
    """The Inpatient PUF parquet has the locked 27-column schema."""
    _skip_if_missing()

    pf = pq.ParquetFile(_PARQUETS[0])
    schema = pf.schema_arrow
    actual = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual
    extra = actual - _EXPECTED_COLUMNS
    assert not missing, f"Missing columns in {_PARQUETS[0].name}: {sorted(missing)}"
    assert not extra, f"Unexpected columns in {_PARQUETS[0].name}: {sorted(extra)}"


def test_drg_codes_zero_padded_3_digits() -> None:
    """Every emitted ``code`` matches the 3-digit MS-DRG regex ``^[0-9]{3}$``."""
    _skip_if_missing()

    con = duckdb.connect()
    bad = con.execute(
        f"""
        SELECT code FROM read_parquet('{_PARQUETS[0]}')
        WHERE code IS NULL OR NOT REGEXP_MATCHES(code, '^[0-9]{{3}}$')
        LIMIT 5
        """
    ).fetchall()
    con.close()
    assert not bad, f"DRG codes failing regex {_DRG_RE.pattern}: {bad}"


def test_n_non_null_and_positive() -> None:
    """Every row has a non-null, strictly-positive ``n`` (sum of discharges)."""
    _skip_if_missing()

    con = duckdb.connect()
    bad = con.execute(
        f"""
        SELECT code, n FROM read_parquet('{_PARQUETS[0]}')
        WHERE n IS NULL OR n <= 0 LIMIT 5
        """
    ).fetchall()
    con.close()
    assert not bad, f"Found rows with n IS NULL or n <= 0: {bad}"


def test_percentiles_null_when_n_providers_under_11() -> None:
    """Per-provider quantiles are NULL whenever ``n_providers < 11`` (CMS
    suppression convention).
    """
    _skip_if_missing()

    con = duckdb.connect()
    bad = con.execute(
        f"""
        SELECT code, n_providers, p50_charge, p50_payment
        FROM read_parquet('{_PARQUETS[0]}')
        WHERE n_providers IS NOT NULL AND n_providers < 11
          AND (
              p10_charge IS NOT NULL OR p25_charge IS NOT NULL OR
              p50_charge IS NOT NULL OR p75_charge IS NOT NULL OR
              p90_charge IS NOT NULL OR
              p10_payment IS NOT NULL OR p25_payment IS NOT NULL OR
              p50_payment IS NOT NULL OR p75_payment IS NOT NULL OR
              p90_payment IS NOT NULL
          )
        LIMIT 5
        """
    ).fetchall()
    con.close()
    assert not bad, (
        f"Found rows with n_providers < 11 but non-null percentiles "
        f"(CMS suppression violated): {bad}"
    )


def test_code_system_is_msdrg() -> None:
    """Every row's ``code_system`` is the literal ``"MS-DRG"``."""
    _skip_if_missing()

    con = duckdb.connect()
    distinct = con.execute(
        f"SELECT DISTINCT code_system FROM read_parquet('{_PARQUETS[0]}')"
    ).fetchall()
    con.close()
    assert distinct == [("MS-DRG",)], (
        f"Expected distinct code_system == [('MS-DRG',)], got {distinct}"
    )


def test_source_tag_distribution_by_fy() -> None:
    """Per-FY ``source`` tag is exactly ``puf_top100`` for FY2011-FY2012 and
    exactly ``puf_all_drgs_ry23`` for FY2013+ (renamed Session 8.5 per PI
    verdict on DEV 1: RY23 retroactive republication uses distinct
    methodology — ``puf_all_drgs_legacy`` reserved as a future tag). No flat
    ``puf_inpatient`` rows.

    Note: FY2013 is sourced from CMS's RY23 unified release (data.cms.gov,
    2023-05) which retroactively re-published DY2013 as full-DRG, so it
    carries ``puf_all_drgs_ry23`` despite plan-v11.md's earlier presumption.
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        for parquet in _PARQUETS:
            # FY embedded in basename: puf_inpatient_fy{YYYY}.parquet
            m = re.search(r"puf_inpatient_fy(\d{4})\.parquet$", parquet.name)
            assert m, f"unexpected parquet basename: {parquet.name}"
            fy = int(m.group(1))
            distinct = con.execute(
                f"SELECT DISTINCT source FROM read_parquet('{parquet}')"
            ).fetchall()
            tags = {row[0] for row in distinct}
            expected = {"puf_top100"} if fy in (2011, 2012) else {"puf_all_drgs_ry23"}
            assert tags == expected, (
                f"FY{fy} parquet {parquet.name}: source tags {tags} != expected {expected}"
            )
    finally:
        con.close()


def test_source_host_populated() -> None:
    """Every row has a non-null ``source_host`` in the locked vocabulary
    ``{cms_gov, nber, wayback}``.
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        for parquet in _PARQUETS:
            null_rows = con.execute(
                f"""
                SELECT COUNT(*) FROM read_parquet('{parquet}')
                WHERE source_host IS NULL
                """
            ).fetchone()[0]
            assert null_rows == 0, (
                f"{parquet.name}: {null_rows} rows have NULL source_host"
            )
            distinct = con.execute(
                f"SELECT DISTINCT source_host FROM read_parquet('{parquet}')"
            ).fetchall()
            hosts = {row[0] for row in distinct}
            allowed = {"cms_gov", "nber", "wayback"}
            unexpected = hosts - allowed
            assert not unexpected, (
                f"{parquet.name}: source_host values {unexpected} not in {allowed}"
            )
    finally:
        con.close()


def test_payment_amount_populated_for_recent_fys() -> None:
    """For any FY2014+ parquet, >=90% of rows have non-null ``payment_amount``
    (PUF carries ``Avg_Tot_Pymt_Amt`` natively for the full-DRG era; the
    Session 8.5 schema rename routes this CMS column into ``payment_amount``
    with concept ``total_payment_allpayer``).
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        recent = [
            p for p in _PARQUETS
            if (m := re.search(r"puf_inpatient_fy(\d{4})\.parquet$", p.name))
            and int(m.group(1)) >= 2014
        ]
        if not recent:
            pytest.skip("no FY2014+ inpatient PUF parquets present")
        for parquet in recent:
            row = con.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN payment_amount IS NOT NULL THEN 1 ELSE 0 END) AS populated
                FROM read_parquet('{parquet}')
                """
            ).fetchone()
            total, populated = row
            assert total > 0, f"{parquet.name}: zero rows"
            ratio = populated / total
            assert ratio >= 0.90, (
                f"{parquet.name}: only {populated}/{total} = {ratio:.2%} rows "
                f"have non-null payment_amount (expected >=90%)"
            )
    finally:
        con.close()


def test_payment_amount_concept_is_total_payment_allpayer() -> None:
    """Every Inpatient PUF row carries concept='total_payment_allpayer' and
    total_payment_amount = payment_amount (locked Phase 5 Session 8.5 contract).
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        for parquet in _PARQUETS:
            distinct = con.execute(
                f"""
                SELECT DISTINCT payment_amount_concept
                FROM read_parquet('{parquet}')
                """
            ).fetchall()
            tags = {row[0] for row in distinct}
            assert tags == {"total_payment_allpayer"}, (
                f"{parquet.name}: payment_amount_concept = {tags}, "
                f"expected exactly {{'total_payment_allpayer'}}"
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


def test_payment_amount_non_null() -> None:
    """payment_amount is NOT NULL on every Inpatient PUF row (locked Phase 5
    Session 8.5 contract; nullable=False at parquet schema level)."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        for parquet in _PARQUETS:
            n_null = con.execute(
                f"""
                SELECT COUNT(*) FROM read_parquet('{parquet}')
                WHERE payment_amount IS NULL
                """
            ).fetchone()[0]
            assert n_null == 0, (
                f"{parquet.name}: {n_null} rows have NULL payment_amount"
            )
    finally:
        con.close()
