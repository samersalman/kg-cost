"""Phase 2 (Session 3) tests for the PFS RVU extractor.

Mirrors ``tests/test_hcpcs.py`` shape:

  - ``test_extract_module_importable`` skips until ``src.extract.pfs_rvu`` lands.
  - All data-dependent assertions skip via ``pytest.skip`` when the per-(cy, q)
    parquet doesn't exist (so the suite passes both pre- and post-extract).

Test target: CY2024 quarter D (mid-range, modern era, easy to spot-check
against published CMS values).
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config
from src.extract.pfs_rvu import (
    _extract_csv_member_to_tempfile,
    _open_zip,
    _rows_from_csv,
)

_CY = 2024
_QUARTER = "D"
_PARQUET = (
    config.SILVER_PER_SOURCE / "pfs_rvu" / f"pfs_rates_cy{_CY}_{_QUARTER}.parquet"
)
_CY_START, _CY_END = config.pfs_quarter_interval(_CY, _QUARTER)

# Locked PFS schema (plan-v3.md lines 92-120).
_EXPECTED_COLUMNS: set[str] = {
    "code",
    "modifier",
    "code_system",
    "cy",
    "quarter",
    "setting",
    "status_indicator",
    "work_rvu",
    "pe_rvu",
    "mp_rvu",
    "total_rvu",
    "conversion_factor",
    "conversion_factor_kind",
    "allowed_amount",
    "effective_start",
    "effective_end",
    "source",
    "n",
}

# CPT 5-digit numeric or HCPCS-II 5-char [A-V0-9][0-9]{4}.
_CPT_RE = re.compile(r"^[0-9]{5}$")
_HCPCS_RE = re.compile(r"^[A-V0-9][0-9]{4}$")


def _skip_if_missing() -> None:
    if not _PARQUET.exists():
        pytest.skip(
            f"{_PARQUET} not present; run "
            f"`python -m src.extract.pfs_rvu --cy {_CY} --quarter {_QUARTER}` first"
        )


def test_extract_module_importable() -> None:
    """Skips until ``src.extract.pfs_rvu`` lands."""
    pytest.importorskip("src.extract.pfs_rvu")


def test_cy2024_d_parquet_schema() -> None:
    """The CY2024 quarter D PFS parquet has every column in the locked schema."""
    _skip_if_missing()

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    # Type sanity for load-bearing columns.
    string_cols = (
        "code",
        "code_system",
        "quarter",
        "setting",
        "conversion_factor_kind",
        "source",
    )
    for col in string_cols:
        t = str(schema.field(col).type).lower()
        assert "string" in t, f"`{col}` must be a string type, got {schema.field(col).type}"

    int_cols = ("cy",)
    for col in int_cols:
        t = str(schema.field(col).type).lower()
        assert "int" in t, f"`{col}` must be an integer type, got {schema.field(col).type}"

    float_cols = (
        "work_rvu",
        "pe_rvu",
        "mp_rvu",
        "total_rvu",
        "conversion_factor",
        "allowed_amount",
    )
    for col in float_cols:
        t = str(schema.field(col).type).lower()
        assert "float" in t or "double" in t or "decimal" in t, (
            f"`{col}` must be a float/double type, got {schema.field(col).type}"
        )

    date_cols = ("effective_start", "effective_end")
    for col in date_cols:
        t = str(schema.field(col).type).lower()
        assert "date" in t, f"`{col}` must be a date type, got {schema.field(col).type}"


def test_cy2024_d_row_count_in_range() -> None:
    """PFS RVU files contain ~10-11k codes; long-form ×2 settings → ~18-25k rows."""
    _skip_if_missing()

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 10_000 < n < 35_000, (
        f"Unexpected PFS CY{_CY} quarter {_QUARTER} row count: {n} "
        f"(expected ~18-25k; 10k-35k tolerated)"
    )


def test_cy2024_d_codes_conformant() -> None:
    """Every ``code`` is CPT 5-digit numeric or HCPCS-II 5-char."""
    _skip_if_missing()

    table = pq.read_table(_PARQUET, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [c for c in codes if c is None or not (_CPT_RE.match(c) or _HCPCS_RE.match(c))]
    assert not bad, (
        f"Found {len(bad)} non-conforming PFS codes "
        f"(must be CPT [0-9]{{5}} or HCPCS-II [A-V0-9][0-9]{{4}}; "
        f"sample: {bad[:5]})"
    )


def test_cy2024_d_settings_long_form() -> None:
    """Distinct ``setting`` values are exactly the two PFS settings."""
    _skip_if_missing()

    table = pq.read_table(_PARQUET, columns=["setting"])
    settings = set(table.column("setting").to_pylist())
    assert settings == {"pfs_facility", "pfs_nonfacility"}, (
        f"Expected settings {{pfs_facility, pfs_nonfacility}}, got {settings}"
    )


def test_cy2024_d_settings_balanced() -> None:
    """Facility and non-facility row counts are within 1% of each other."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT setting, COUNT(*) FROM read_parquet(?) GROUP BY setting",
            [str(_PARQUET)],
        ).fetchall()
    finally:
        con.close()

    counts = {setting: n for setting, n in rows}
    f = counts.get("pfs_facility", 0)
    nf = counts.get("pfs_nonfacility", 0)
    assert f > 0 and nf > 0, f"One setting has 0 rows: {counts}"
    skew = abs(f - nf) / max(f, nf)
    assert skew < 0.01, (
        f"Facility/non-facility row counts unbalanced (>1%): "
        f"facility={f}, nonfacility={nf}, skew={skew:.4f}"
    )


def test_cy2024_d_conversion_factor_single() -> None:
    """CY2024 is a single-CF year with the PFS Final Rule annual CF."""
    _skip_if_missing()

    table = pq.read_table(
        _PARQUET, columns=["conversion_factor", "conversion_factor_kind"]
    )
    kinds = set(table.column("conversion_factor_kind").to_pylist())
    assert kinds == {"single"}, (
        f"CY2024 must have only `single` conversion_factor_kind, got {kinds}"
    )

    expected_cf = config.PFS_CONVERSION_FACTORS[_CY]
    assert isinstance(expected_cf, float), (
        f"CY2024 PFS_CONVERSION_FACTORS entry must be a single float, got {expected_cf!r}"
    )
    cfs = set(table.column("conversion_factor").to_pylist())
    assert len(cfs) == 1, f"CY2024 conversion_factor should be constant; got {cfs}"
    actual_cf = next(iter(cfs))
    assert abs(actual_cf - expected_cf) < 1e-4, (
        f"CY2024 conversion_factor mismatch: parquet={actual_cf}, "
        f"config={expected_cf}"
    )


def test_cy2024_d_status_filter() -> None:
    """``I`` (invalid) and ``N`` (non-covered) status codes are dropped by the extractor."""
    _skip_if_missing()

    table = pq.read_table(_PARQUET, columns=["status_indicator"])
    statuses = set(table.column("status_indicator").to_pylist())
    forbidden = {"I", "N"} & statuses
    assert not forbidden, (
        f"PFS extractor must drop status_indicator in {{I, N}}; found {forbidden}"
    )


def test_cy2024_d_total_rvu_consistency() -> None:
    """``total_rvu ≈ work_rvu + pe_rvu + mp_rvu`` within $0.01."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        bad = con.execute(
            """
            SELECT code, modifier, setting, work_rvu, pe_rvu, mp_rvu, total_rvu
            FROM read_parquet(?)
            WHERE work_rvu IS NOT NULL
              AND pe_rvu IS NOT NULL
              AND mp_rvu IS NOT NULL
              AND total_rvu IS NOT NULL
              AND ABS(total_rvu - (work_rvu + pe_rvu + mp_rvu)) > 0.01
            LIMIT 5
            """,
            [str(_PARQUET)],
        ).fetchall()
    finally:
        con.close()

    assert not bad, (
        f"total_rvu inconsistent with work+pe+mp (>$0.01) — sample: {bad}"
    )


def test_cy2024_d_allowed_amount_consistency() -> None:
    """``allowed_amount ≈ total_rvu * conversion_factor`` within $0.01."""
    _skip_if_missing()

    con = duckdb.connect()
    try:
        bad = con.execute(
            """
            SELECT code, modifier, setting, total_rvu, conversion_factor, allowed_amount
            FROM read_parquet(?)
            WHERE total_rvu IS NOT NULL
              AND conversion_factor IS NOT NULL
              AND allowed_amount IS NOT NULL
              AND ABS(allowed_amount - (total_rvu * conversion_factor)) > 0.01
            LIMIT 5
            """,
            [str(_PARQUET)],
        ).fetchall()
    finally:
        con.close()

    assert not bad, (
        f"allowed_amount inconsistent with total_rvu*CF (>$0.01) — sample: {bad}"
    )


def test_cy2024_d_effective_dates() -> None:
    """All rows span the CY2024 Q4 interval (2024-10-01 .. 2024-12-31)."""
    _skip_if_missing()

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_CY_START}, (
        f"effective_start should be {{{_CY_START}}}, got {starts}"
    )
    assert ends == {_CY_END}, (
        f"effective_end should be {{{_CY_END}}}, got {ends}"
    )


def test_cy2024_d_cpt_99213_spot_check() -> None:
    """CPT 99213 (established office visit, level 3) is the canonical PFS spot-check.

    Per CMS published CY2024 PFS national rates: facility ≈ $57, non-facility ≈ $92.
    Tolerance bands are wider than the plan's "$5" because (a) we use an
    approximate annual CF, (b) GPCI=1.0 vs. CMS's locality-weighted national
    mean, (c) Medicare can adjust mid-year for some quarters.
    """
    _skip_if_missing()

    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT setting, allowed_amount
            FROM read_parquet(?)
            WHERE code = '99213' AND modifier IS NULL
            ORDER BY setting
            """,
            [str(_PARQUET)],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 2, (
        f"Expected exactly 2 rows for code=99213 modifier IS NULL "
        f"(one per setting); got {len(rows)}: {rows}"
    )

    by_setting = {setting: amt for setting, amt in rows}
    facility = by_setting.get("pfs_facility")
    nonfacility = by_setting.get("pfs_nonfacility")
    assert facility is not None, f"Missing pfs_facility row for 99213; got {rows}"
    assert nonfacility is not None, (
        f"Missing pfs_nonfacility row for 99213; got {rows}"
    )
    assert 50 <= facility <= 65, (
        f"99213 facility allowed_amount out of range [50, 65]: {facility}"
    )
    assert 85 <= nonfacility <= 100, (
        f"99213 non-facility allowed_amount out of range [85, 100]: {nonfacility}"
    )


def test_cy2026_doubling() -> None:
    """CY2026 doubles every (code, modifier, setting) into qp + nonqp rows."""
    cy2026_dir = config.SILVER_PER_SOURCE / "pfs_rvu"
    if not cy2026_dir.exists():
        pytest.skip(f"{cy2026_dir} not present; run PFS extractor first")
    cy2026_files = sorted(cy2026_dir.glob("pfs_rates_cy2026_*.parquet"))
    if not cy2026_files:
        pytest.skip(
            "No CY2026 PFS parquets present; run "
            "`python -m src.extract.pfs_rvu --cy 2026 --quarter D` first"
        )

    target = cy2026_files[0]
    con = duckdb.connect()
    try:
        sample = con.execute(
            """
            SELECT code, modifier, setting
            FROM read_parquet(?)
            GROUP BY code, modifier, setting
            LIMIT 100
            """,
            [str(target)],
        ).fetchall()
        if not sample:
            pytest.skip(f"{target} has 0 rows")

        bad: list[tuple[str, str | None, str, list[str]]] = []
        for code, modifier, setting in sample:
            kinds = con.execute(
                """
                SELECT conversion_factor_kind
                FROM read_parquet(?)
                WHERE code = ?
                  AND setting = ?
                  AND modifier IS NOT DISTINCT FROM ?
                """,
                [str(target), code, setting, modifier],
            ).fetchall()
            kind_set = sorted({k for (k,) in kinds})
            if kind_set != ["nonqp", "qp"]:
                bad.append((code, modifier, setting, kind_set))
    finally:
        con.close()

    assert not bad, (
        f"CY2026 must double each (code, modifier, setting) into "
        f"{{qp, nonqp}}; offenders: {bad[:5]}"
    )


def test_cy2026_qpp_nonqpp_rvus_identical(tmp_path: Path) -> None:
    """Regression for Open Q #1: do CY2026 QPP and nonQPP CSVs differ on the RVU side?

    The CY2026 ``rvu26a.zip`` ships TWO PPRRVU CSVs:
    ``PPRRVU2026_Jan_QPP.csv`` and ``PPRRVU2026_Jan_nonQPP.csv``. The current
    ``src.extract.pfs_rvu`` reads whichever member-listing surfaces first and
    applies BOTH ``qp`` and ``nonqp`` conversion factors to the same RVU set.
    That behavior is correct ONLY if the two CSVs share identical RVUs and
    differ only on the conversion factor.

    This test parses both CSVs through the same ``_rows_from_csv`` machinery
    used by the extractor and asserts the RVU tuple
    ``(work, pe_f, pe_nf, mp)`` is identical (within 1e-6 per component) for
    every (code, modifier) key in their intersection.

    If this fails, CMS has split QPP and nonQPP RVUs and ``pfs_rvu.py`` needs
    to source RVUs per-CSV instead of reusing one for both qp/nonqp rows.
    """
    # Locate the CY2026 bronze rvu26a.zip via a glob across download_date.
    cy2026_bronze = config.BRONZE / "pfs_rvu" / "cy2026"
    if not cy2026_bronze.exists():
        pytest.skip(
            f"{cy2026_bronze} not present; run "
            "`python -m src.extract.pfs_rvu --cy 2026 --quarter A` first"
        )
    candidates = sorted(cy2026_bronze.glob("*/rvu26a.zip"))
    if not candidates:
        pytest.skip(
            f"No rvu26a.zip under {cy2026_bronze}/*/; run "
            "`python -m src.extract.pfs_rvu --cy 2026 --quarter A` first"
        )
    # Use the most recent download_date.
    bronze_zip = candidates[-1]

    with _open_zip(bronze_zip) as zf:
        members = zf.namelist()
        qpp_member: str | None = None
        nonqpp_member: str | None = None
        for name in members:
            bn = Path(name).name
            if not bn.lower().endswith(".csv"):
                continue
            if "PPRRVU" not in bn.upper():
                continue
            if "nonqpp" in bn.lower():
                nonqpp_member = name
            elif "qpp" in bn.lower():
                qpp_member = name

        if qpp_member is None or nonqpp_member is None:
            pytest.skip(
                f"rvu26a.zip missing one of the dual PPRRVU CSVs "
                f"(qpp={qpp_member!r}, nonqpp={nonqpp_member!r}); "
                f"members: {[Path(n).name for n in members if n.lower().endswith('.csv')]}"
            )

        qpp_csv = tmp_path / Path(qpp_member).name
        nonqpp_csv = tmp_path / Path(nonqpp_member).name
        _extract_csv_member_to_tempfile(zf, qpp_member, qpp_csv)
        _extract_csv_member_to_tempfile(zf, nonqpp_member, nonqpp_csv)

    qpp_rows = _rows_from_csv(qpp_csv)
    nonqpp_rows = _rows_from_csv(nonqpp_csv)

    def _rvus_by_key(
        rows: list,
    ) -> dict[tuple[str, str | None], tuple[float | None, float | None, float | None, float | None]]:
        out: dict[tuple[str, str | None], tuple[float | None, float | None, float | None, float | None]] = {}
        for r in rows:
            out[(r.code, r.modifier)] = (r.work, r.pe_f, r.pe_nf, r.mp)
        return out

    qpp_map = _rvus_by_key(qpp_rows)
    nonqpp_map = _rvus_by_key(nonqpp_rows)

    common_keys = set(qpp_map) & set(nonqpp_map)
    assert common_keys, (
        "QPP and nonQPP CSVs share zero (code, modifier) keys — "
        "header detection or row filtering likely broken"
    )

    tol = 1e-6
    mismatches: list[tuple[tuple[str, str | None], tuple, tuple]] = []
    for key in common_keys:
        q = qpp_map[key]
        nq = nonqpp_map[key]
        for qv, nqv in zip(q, nq):
            if qv is None and nqv is None:
                continue
            if qv is None or nqv is None:
                mismatches.append((key, q, nq))
                break
            if abs(qv - nqv) > tol:
                mismatches.append((key, q, nq))
                break

    assert not mismatches, (
        f"CY2026 QPP and nonQPP RVUs DIFFER in {len(mismatches)} of "
        f"{len(common_keys)} common (code, modifier) keys — pfs_rvu.py "
        f"must source RVUs per-CSV. Sample mismatches: {mismatches[:5]}"
    )
