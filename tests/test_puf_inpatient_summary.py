"""Phase 5 Batch 2b tests for the Inpatient PUF National Summary extractor.

Mirrors ``tests/test_opps.py`` shape: module-level ``pytest.importorskip``
keeps test collection robust, and every data-dependent assertion skips via
``_skip_if_missing`` when the per-FY parquet doesn't exist.

Test target FY: 2013 (last FY covered by Batch 2b; closest to plan-v10.md's
nominal "FY2013 backfill" framing).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import config

pytest.importorskip("src.extract.puf_inpatient_summary")

_FYS: tuple[int, ...] = (2011, 2012, 2013)


def _silver_path(fy: int) -> Path:
    return (
        config.SILVER_PER_SOURCE
        / "puf"
        / "inpatient_summary"
        / f"puf_inpatient_summary_fy{fy}.parquet"
    )


def _skip_if_missing(fy: int) -> None:
    p = _silver_path(fy)
    if not p.exists():
        pytest.skip(
            f"{p} not present; run "
            f"`python -m src.extract.puf_inpatient_summary --fy {fy}` first"
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


def test_extract_module_importable() -> None:
    """``src.extract.puf_inpatient_summary`` exposes the documented API."""
    from src.extract import puf_inpatient_summary

    assert hasattr(puf_inpatient_summary, "extract_fy"), (
        "src.extract.puf_inpatient_summary must export `extract_fy`"
    )
    assert hasattr(puf_inpatient_summary, "extract_fy_range"), (
        "src.extract.puf_inpatient_summary must export `extract_fy_range`"
    )


def test_url_dict_covers_2011_2013() -> None:
    """Module-local ``_PUF_INPATIENT_SUMMARY_SOURCES`` covers Batch 2b scope.

    Plan-v10.md scope is FY2011-FY2013 inclusive. The dict must be a subset
    of that range and ship NBER-mirror URLs (the original cms.gov Downloads/
    URLs are retired as of 2026-05-01; Phase 5 Session 8.5 resolution).
    """
    from src.extract.puf_inpatient_summary import _PUF_INPATIENT_SUMMARY_SOURCES

    keys = set(_PUF_INPATIENT_SUMMARY_SOURCES.keys())
    assert keys, "_PUF_INPATIENT_SUMMARY_SOURCES is empty"
    assert keys.issubset({2011, 2012, 2013}), (
        f"_PUF_INPATIENT_SUMMARY_SOURCES keys {keys} must subset "
        f"{{2011, 2012, 2013}}"
    )
    for fy, url in _PUF_INPATIENT_SUMMARY_SOURCES.items():
        assert url.startswith("https://data.nber.org/"), (
            f"FY{fy} URL must be NBER-mirror (https://data.nber.org/...): "
            f"{url!r}"
        )


def test_source_tag_is_puf_top100_summary() -> None:
    """Silver ``source`` column emits the ``puf_top100_summary`` literal.

    The bronze tree path keeps the ``puf_inpatient_summary`` literal
    (see ``_BRONZE_SOURCE``); only the row-level silver ``source`` value
    changes to ``puf_top100_summary`` per Phase 5 Session 8.5 resolution.
    """
    fy = 2013
    _skip_if_missing(fy)
    table = pq.read_table(_silver_path(fy), columns=["source"])
    distinct = set(table.column("source").to_pylist())
    assert distinct == {"puf_top100_summary"}, (
        f"`source` distinct values {distinct} must equal "
        f"{{'puf_top100_summary'}}"
    )


def test_source_host_is_nber() -> None:
    """Silver ``source_host`` column emits the ``nber`` literal everywhere."""
    fy = 2013
    _skip_if_missing(fy)
    table = pq.read_table(_silver_path(fy), columns=["source_host"])
    distinct = set(table.column("source_host").to_pylist())
    assert distinct == {"nber"}, (
        f"`source_host` distinct values {distinct} must equal {{'nber'}}"
    )


def test_silver_schema_correct() -> None:
    """The silver parquet has the locked 27-column schema (Session 8.5)."""
    fy = 2013
    _skip_if_missing(fy)

    pf = pq.ParquetFile(_silver_path(fy))
    schema = pf.schema_arrow
    actual = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual
    extra = actual - _EXPECTED_COLUMNS
    assert not missing, f"Missing columns: {sorted(missing)}"
    assert not extra, f"Unexpected columns: {sorted(extra)}"

    # Type sanity for load-bearing columns.
    for col in ("scope", "code", "code_system", "source"):
        t = str(schema.field(col).type).lower()
        assert "string" in t, f"`{col}` must be string, got {schema.field(col).type}"
    for col in ("year",):
        t = str(schema.field(col).type).lower()
        assert "int32" in t, f"`{col}` must be int32, got {schema.field(col).type}"
    for col in ("n",):
        t = str(schema.field(col).type).lower()
        assert "int64" in t, f"`{col}` must be int64, got {schema.field(col).type}"
    for col in ("mean_charge", "mean_payment"):
        t = str(schema.field(col).type).lower()
        assert "double" in t or "float64" in t, (
            f"`{col}` must be float64, got {schema.field(col).type}"
        )
    for col in ("effective_start", "effective_end"):
        t = str(schema.field(col).type).lower()
        assert "date" in t, f"`{col}` must be date, got {schema.field(col).type}"


def test_percentiles_all_null() -> None:
    """All ``p*_charge`` and ``p*_payment`` values are NULL.

    The National Summary aggregate has no per-provider distribution by design.
    """
    fy = 2013
    _skip_if_missing(fy)

    pct_cols = [
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
    ]
    table = pq.read_table(_silver_path(fy), columns=pct_cols)
    for col in pct_cols:
        nulls = table.column(col).null_count
        n = table.num_rows
        assert nulls == n, (
            f"{col} should be 100% NULL but has {n - nulls}/{n} populated"
        )


def test_n_providers_all_null() -> None:
    """``n_providers`` is NULL on every row (no provider-level data)."""
    fy = 2013
    _skip_if_missing(fy)
    table = pq.read_table(_silver_path(fy), columns=["n_providers"])
    nulls = table.column("n_providers").null_count
    assert nulls == table.num_rows, (
        f"n_providers should be 100% NULL but has "
        f"{table.num_rows - nulls}/{table.num_rows} populated"
    )


def test_means_non_null() -> None:
    """``mean_charge`` and ``mean_payment`` populated for >=95% of FY2013 rows.

    FY2013 ships both Average Covered Charges AND Average Medicare Payments
    columns. (FY2011's NBER ZIP carries only "Average Total Payments" -> our
    `payment_amount`; `mean_payment` is 100% NULL for FY2011 by design.
    Defended by `test_fy2011_mean_payment_all_null_payment_amount_populated`.)
    """
    fy = 2013
    _skip_if_missing(fy)
    table = pq.read_table(_silver_path(fy), columns=["mean_charge", "mean_payment"])
    n = table.num_rows
    for col in ("mean_charge", "mean_payment"):
        nulls = table.column(col).null_count
        # Allow up to 5% nulls but require a non-trivial populated population.
        populated = n - nulls
        assert populated > 0, f"{col} has zero populated rows"
        assert populated >= 0.95 * n, (
            f"{col} has only {populated}/{n} populated rows (<95%)"
        )


def test_fy2011_mean_payment_all_null_payment_amount_populated() -> None:
    """FY2011 NBER ZIP ships only 'Average Total Payments' — no Medicare-only column.

    Locked Phase 5 Session 8.5 invariant: for FY2011 every row's
    ``mean_payment`` is NULL while ``payment_amount`` is populated for the
    bulk of rows AND ``payment_amount_concept = 'total_payment_allpayer'``
    AND ``total_payment_amount`` equals ``payment_amount``.
    """
    fy = 2011
    _skip_if_missing(fy)
    table = pq.read_table(
        _silver_path(fy),
        columns=[
            "mean_payment",
            "payment_amount",
            "payment_amount_concept",
            "total_payment_amount",
        ],
    )
    n = table.num_rows
    mp_nulls = table.column("mean_payment").null_count
    pa_nulls = table.column("payment_amount").null_count
    assert mp_nulls == n, (
        f"FY2011 `mean_payment` should be 100% NULL "
        f"({mp_nulls}/{n} actually NULL)"
    )
    populated = n - pa_nulls
    assert populated >= 0.95 * n, (
        f"FY2011 `payment_amount` populated on {populated}/{n} rows (<95%); "
        f"NBER ZIP ships Avg Total Payments natively"
    )
    concepts = set(table.column("payment_amount_concept").to_pylist())
    assert concepts == {"total_payment_allpayer"}, (
        f"FY2011 `payment_amount_concept` distinct = {concepts}, "
        f"expected {{'total_payment_allpayer'}}"
    )
    pas = table.column("payment_amount").to_pylist()
    tots = table.column("total_payment_amount").to_pylist()
    for i, (pa_v, t_v) in enumerate(zip(pas, tots)):
        if pa_v is None:
            continue
        assert t_v == pa_v, (
            f"FY2011 row {i}: total_payment_amount={t_v} != payment_amount={pa_v}"
        )


def test_row_count_per_fy_above_threshold() -> None:
    """Each FY parquet has at least the FY2011-FY2013 top-100 cardinality.

    Note: plan-v10.md asserted >= 600 rows per FY based on the (now-known
    incorrect) premise that the National Summary backfills the missing
    ~650 DRGs. CMS's FY2011-FY2013 National Summary is *also* top-100 only
    (XLSX basename ``..._DRG100_..._FY{YYYY}.xlsx``); the all-DRG variant
    (``..._DRGALL_..._FY{YYYY}.xlsx``) starts FY2014. The threshold here is
    set to ``>= 80`` so we still defend against silent regression below the
    empirical ~100-DRG cardinality. See module docstring for full deviation
    notes.
    """
    skipped = 0
    for fy in _FYS:
        p = _silver_path(fy)
        if not p.exists():
            skipped += 1
            continue
        n = pq.ParquetFile(p).metadata.num_rows
        assert n >= 80, (
            f"FY{fy} row count {n} is below empirical FY2011-FY2013 floor of 80"
        )
    if skipped == len(_FYS):
        pytest.skip("no per-FY parquets present yet")


def test_payment_amount_concept_is_total_payment_allpayer_all_fys() -> None:
    """Every Inpatient PUF Summary row across FY2011-FY2013 carries
    concept='total_payment_allpayer' and total_payment_amount = payment_amount.
    Per Phase 5 Session 8.5 PI verdict on DEV 4: NBER summary ZIP sources from
    'Average Total Payments' (all-payer total)."""
    skipped = 0
    for fy in _FYS:
        p = _silver_path(fy)
        if not p.exists():
            skipped += 1
            continue
        table = pq.read_table(
            p, columns=["payment_amount_concept", "payment_amount", "total_payment_amount"]
        )
        concepts = set(table.column("payment_amount_concept").to_pylist())
        assert concepts == {"total_payment_allpayer"}, (
            f"FY{fy}: payment_amount_concept distinct = {concepts}, "
            f"expected {{'total_payment_allpayer'}}"
        )
        # total_payment_amount must equal payment_amount on every populated row.
        pas = table.column("payment_amount").to_pylist()
        tots = table.column("total_payment_amount").to_pylist()
        for i, (pa_v, t_v) in enumerate(zip(pas, tots)):
            if pa_v is None:
                continue
            assert t_v == pa_v, (
                f"FY{fy} row {i}: total_payment_amount={t_v} != payment_amount={pa_v}"
            )
    if skipped == len(_FYS):
        pytest.skip("no per-FY parquets present yet")


def test_payment_amount_non_null() -> None:
    """payment_amount is NOT NULL on every row across all 3 FYs (locked Phase 5
    Session 8.5 contract; nullable=False at parquet schema level; extractor
    drops null-payment rows pre-emit)."""
    skipped = 0
    for fy in _FYS:
        p = _silver_path(fy)
        if not p.exists():
            skipped += 1
            continue
        table = pq.read_table(p, columns=["payment_amount"])
        nulls = table.column("payment_amount").null_count
        assert nulls == 0, f"FY{fy}: {nulls} rows with NULL payment_amount"
    if skipped == len(_FYS):
        pytest.skip("no per-FY parquets present yet")
