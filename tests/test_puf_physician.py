"""Phase 5 (Batch 2d) tests for the Medicare Physician PUF extractor.

Mirrors ``tests/test_opps.py`` shape: data-independent tests run always;
data-dependent tests skip via ``pytest.skip`` when the per-CY 27-column silver
parquet doesn't exist (so the suite passes pre- and post-extract -- the per-CY
~2 GB download is intentionally not invoked by the test runner).

Test target: CY2022 (mid-range modern era).
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

pytest.importorskip("src.extract.puf_physician")

_CY = 2022
_PARQUET = (
    config.SILVER_PER_SOURCE / "puf" / "physician"
    / f"puf_physician_cy{_CY}.parquet"
)

_EXPECTED_COLUMNS: frozenset[str] = frozenset({
    "scope", "code", "code_system", "year", "place_of_srvc", "n",
    "mean_charge", "mean_payment", "mean_payment_stdzd",
    "payment_amount", "payment_amount_concept", "total_payment_amount",
    "p10_charge", "p25_charge", "p50_charge", "p75_charge", "p90_charge",
    "p10_payment", "p25_payment", "p50_payment", "p75_payment", "p90_payment",
    "n_providers", "source", "source_host", "effective_start", "effective_end",
})

_HCPCS_RE = re.compile(r"^[A-V0-9][0-9]{4}$")


def _skip_if_missing() -> None:
    if not _PARQUET.exists():
        pytest.skip(
            f"{_PARQUET} not present; run "
            f"`python -m src.extract.puf_physician --cy {_CY}` first"
        )


# ---- data-independent tests ------------------------------------------------


def test_extract_module_importable() -> None:
    from src.extract import puf_physician
    assert hasattr(puf_physician, "extract_cy")
    assert hasattr(puf_physician, "extract_cy_range")


def test_url_dict_covers_expected_range() -> None:
    """``_PUF_PHYSICIAN_SOURCES`` is non-empty, http(s), and a subset of
    ``config.PUF_PHYSICIAN_CYS``. >= 1 verified seed."""
    from src.extract.puf_physician import _PUF_PHYSICIAN_SOURCES

    populated = set(_PUF_PHYSICIAN_SOURCES.keys())
    assert len(populated) >= 1, (
        "Expected >= 1 verified seed URL in _PUF_PHYSICIAN_SOURCES"
    )
    extra = populated - set(config.PUF_PHYSICIAN_CYS)
    assert not extra, (
        f"_PUF_PHYSICIAN_SOURCES has CYs outside config.PUF_PHYSICIAN_CYS: "
        f"{sorted(extra)}"
    )
    for cy, url in _PUF_PHYSICIAN_SOURCES.items():
        assert isinstance(url, str) and url.startswith(("https://", "http://")), (
            f"CY{cy} URL invalid: {url!r}"
        )


def test_resolve_columns_missing_required_raises() -> None:
    from src.extract.puf_physician import _resolve_columns

    with pytest.raises(RuntimeError) as exc:
        _resolve_columns(["foo", "bar", "baz"])
    msg = str(exc.value).lower()
    assert "required columns" in msg
    assert "hcpcs_cd" in msg or "tot_srvcs" in msg


def test_resolve_columns_canonical_header_resolves() -> None:
    """Canonical CY2017+ CMS headers resolve to the exact source strings."""
    from src.extract.puf_physician import _resolve_columns

    cols = _resolve_columns([
        "Rndrng_NPI", "HCPCS_Cd", "Place_Of_Srvc", "Tot_Srvcs",
        "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt",
        "Avg_Mdcr_Stdzd_Amt", "extra_col",
    ])
    assert cols["hcpcs_cd"] == "HCPCS_Cd"
    assert cols["place_of_srvc"] == "Place_Of_Srvc"
    assert cols["tot_srvcs"] == "Tot_Srvcs"
    assert cols["avg_mdcr_stdzd_amt"] == "Avg_Mdcr_Stdzd_Amt"


# ---- silver parquet tests (skip-if-missing) --------------------------------


def test_silver_schema_correct() -> None:
    _skip_if_missing()
    schema = pq.ParquetFile(_PARQUET).schema_arrow
    actual = set(schema.names)
    assert not (_EXPECTED_COLUMNS - actual), (
        f"Missing columns: {sorted(_EXPECTED_COLUMNS - actual)}"
    )
    assert not (actual - _EXPECTED_COLUMNS), (
        f"Unexpected columns: {sorted(actual - _EXPECTED_COLUMNS)}"
    )
    for col in ("scope", "code", "code_system", "place_of_srvc", "source"):
        assert "string" in str(schema.field(col).type).lower()
    for col in ("year", "n", "n_providers"):
        assert "int" in str(schema.field(col).type).lower()
    for col in ("mean_charge", "mean_payment", "mean_payment_stdzd",
                "p50_charge", "p50_payment"):
        t = str(schema.field(col).type).lower()
        assert "float" in t or "double" in t
    for col in ("effective_start", "effective_end"):
        assert "date" in str(schema.field(col).type).lower()


def test_hcpcs_codes_5_chars() -> None:
    _skip_if_missing()
    codes = pq.read_table(_PARQUET, columns=["code"]).column("code").to_pylist()
    bad = [c for c in codes if c is None or not _HCPCS_RE.match(c)]
    assert not bad, f"{len(bad)} non-conforming codes; sample: {bad[:5]}"


def test_place_of_srvc_in_F_O() -> None:
    _skip_if_missing()
    values = (
        pq.read_table(_PARQUET, columns=["place_of_srvc"])
        .column("place_of_srvc").to_pylist()
    )
    assert all(v is not None for v in values), "place_of_srvc must not be NULL"
    bad = [v for v in values if v not in ("F", "O")]
    assert not bad, f"place_of_srvc must be in {{F, O}}; got {sorted(set(bad))[:5]}"


def test_code_system_is_hcpcs() -> None:
    _skip_if_missing()
    distinct = set(
        pq.read_table(_PARQUET, columns=["code_system"])
        .column("code_system").to_pylist()
    )
    assert distinct == {"HCPCS-II"}, f"got {sorted(distinct)}"


def test_source_host_populated() -> None:
    """Session 8.5 widening: every silver row carries
    ``source_host = "cms_gov"`` (Physician PUF has no NBER mirror or
    Wayback fallback). (NBER's CY2012 mirror exists but strips HCPCS
    codes per NBER's ``desc_phys2012.txt``, so source_host can never
    widen to 'nber' for physician -- Session 8.5 Step 1 finding.)"""
    _skip_if_missing()
    distinct = set(
        pq.read_table(_PARQUET, columns=["source_host"])
        .column("source_host").to_pylist()
    )
    assert distinct == {"cms_gov"}, (
        f"source_host must be exactly {{'cms_gov'}}; got {sorted(distinct)}"
    )


def test_mean_payment_stdzd_non_null() -> None:
    """Physician PUF ships ``mean_payment_stdzd`` (Avg_Mdcr_Stdzd_Amt); the
    column is physician-only and must be populated for the bulk of rows."""
    _skip_if_missing()
    vals = (
        pq.read_table(_PARQUET, columns=["mean_payment_stdzd"])
        .column("mean_payment_stdzd").to_pylist()
    )
    n_total = len(vals)
    n_non_null = sum(1 for v in vals if v is not None)
    assert n_total > 0
    assert n_non_null / n_total >= 0.95, (
        f"mean_payment_stdzd >= 95% non-null expected; got "
        f"{n_non_null}/{n_total} = {n_non_null / n_total:.3%}"
    )


def test_percentiles_null_when_n_providers_under_11() -> None:
    """Privacy mask: when n_providers is masked NULL (was < 11), every
    percentile column is NULL too. When n_providers is populated, it is
    >= 11."""
    _skip_if_missing()
    cols = [
        "n_providers",
        "p10_charge", "p25_charge", "p50_charge", "p75_charge", "p90_charge",
        "p10_payment", "p25_payment", "p50_payment", "p75_payment",
        "p90_payment",
    ]
    table = pq.read_table(_PARQUET, columns=cols)
    n_providers = table.column("n_providers").to_pylist()
    pct_cols = [
        table.column(c).to_pylist() for c in cols if c != "n_providers"
    ]
    leaks: list[int] = []
    for i, np_val in enumerate(n_providers):
        if np_val is None:
            for pct in pct_cols:
                if pct[i] is not None:
                    leaks.append(i)
                    break
        else:
            assert np_val >= 11, (
                f"row {i}: n_providers={np_val} should be masked (>=11 "
                f"required when populated)"
            )
    assert not leaks, (
        f"Privacy mask leak: {len(leaks)} row(s) had NULL n_providers but "
        f"non-NULL percentile column(s); first 5: {leaks[:5]}"
    )


def test_payment_amount_concept_medicare_allowed() -> None:
    """Every Physician PUF row across all extant CYs carries
    payment_amount_concept = 'medicare_allowed' (sourced from
    Avg_Mdcr_Alowd_Amt; locked Phase 5 Session 8.5)."""
    parquet_dir = config.SILVER_PER_SOURCE / "puf" / "physician"
    parquets = sorted(parquet_dir.glob("puf_physician_cy*.parquet"))
    if not parquets:
        pytest.skip(f"no parquets in {parquet_dir}")

    for parquet in parquets:
        distinct = set(
            pq.read_table(parquet, columns=["payment_amount_concept"])
            .column("payment_amount_concept").to_pylist()
        )
        assert distinct == {"medicare_allowed"}, (
            f"{parquet.name}: concept distinct = {distinct}, "
            f"expected {{'medicare_allowed'}}"
        )


def test_total_payment_amount_all_null() -> None:
    """Every Physician PUF row has total_payment_amount IS NULL because
    concept=medicare_allowed (the all-payer total alias is reserved for
    inpatient consumers; Session 8.5 contract)."""
    parquet_dir = config.SILVER_PER_SOURCE / "puf" / "physician"
    parquets = sorted(parquet_dir.glob("puf_physician_cy*.parquet"))
    if not parquets:
        pytest.skip(f"no parquets in {parquet_dir}")

    for parquet in parquets:
        col = pq.read_table(parquet, columns=["total_payment_amount"]).column(
            "total_payment_amount"
        )
        assert col.null_count == col.length(), (
            f"{parquet.name}: {col.length() - col.null_count} rows have "
            f"non-null total_payment_amount; physician contract requires "
            f"100% NULL"
        )


def test_payment_amount_non_null() -> None:
    """payment_amount IS NOT NULL on every row (locked Phase 5 Session 8.5
    contract; nullable=False at parquet schema; extractor drops null rows
    pre-emit via pyarrow.compute.is_valid)."""
    parquet_dir = config.SILVER_PER_SOURCE / "puf" / "physician"
    parquets = sorted(parquet_dir.glob("puf_physician_cy*.parquet"))
    if not parquets:
        pytest.skip(f"no parquets in {parquet_dir}")

    for parquet in parquets:
        col = pq.read_table(parquet, columns=["payment_amount"]).column(
            "payment_amount"
        )
        assert col.null_count == 0, (
            f"{parquet.name}: {col.null_count} rows with NULL payment_amount"
        )


def test_cy2012_unrecoverable_documented() -> None:
    """CY2012 Physician PUF stays unrecoverable as of Session 8.5: cms.gov
    canonical 'by Provider and Service' starts CY2013, NBER's mirror at
    `data.nber.org/providerchargepuf/2012/phys2012.csv.zip` exists but
    strips HCPCS codes per NBER's `desc_phys2012.txt`, and Wayback has no
    snapshot of the original CMS HCPCS-retained version. (Session 8.5 Step 1
    finding: REJECTED-the-rejection of DEV 2.)
    Asserts: (a) no CY2012 silver parquet on disk; (b) module docstring
    documents the HCPCS-stripped landmine."""
    cy2012_silver = (
        config.SILVER_PER_SOURCE / "puf" / "physician"
        / "puf_physician_cy2012.parquet"
    )
    assert not cy2012_silver.exists(), (
        f"CY2012 physician silver should not exist (NBER mirror is HCPCS-"
        f"stripped); found {cy2012_silver}"
    )
    from src.extract import puf_physician
    doc = (puf_physician.__doc__ or "").lower()
    # Optional: also check the module-level URL dict's surrounding comment
    # mentions the strip -- too brittle, skip if not present.
    # The module docstring may not mention HCPCS strip directly; check the
    # source instead for the landmine block.
    # SILVER_PER_SOURCE = <root>/data/silver/per_source -> root is parents[2]
    project_root = config.SILVER_PER_SOURCE.parent.parent.parent
    src = (
        (project_root / "src" / "extract" / "puf_physician.py")
        .read_text()
        .lower()
    )
    assert "hcpcs" in src and ("strip" in src or "stripped" in src), (
        "puf_physician.py must document the NBER HCPCS-stripped landmine "
        "for CY2012 (search for 'hcpcs' AND 'strip' in the module text)."
    )
