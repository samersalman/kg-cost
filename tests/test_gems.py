"""Tests for the GEMs extractor (Phase 1B, Session 2).

Mirrors the dual-mode shape of ``tests/test_icd10cm.py``:

1. **Without real data present**: every assertion that requires the silver
   parquet to exist is wrapped in ``pytest.skip(...)``. Module import uses
   ``pytest.importorskip`` so CI stays green pre-extractor.

2. **With real data present**: after running ``python -m src.extract.gems``
   the data-dependent assertions exercise schema, row count band, code
   casing, system pair coverage, effective dates, and a known mapping.
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

_PARQUET = config.SILVER / "gems_2018.parquet"

_EXPECTED_COLUMNS: set[str] = {
    "source_code",
    "source_system",
    "target_code",
    "target_system",
    "approximate_flag",
    "no_map_flag",
    "combination_flag",
    "scenario",
    "choice_list",
    "is_reimbursement",
    "effective_start",
    "effective_end",
    "source",
}

_EFFECTIVE_START = __import__("datetime").date(2018, 10, 1)

# Code regex: upper-case alphanumeric, dotless. ICD-9 / ICD-10 both publish
# dotless. Allows leading zeros (ICD-9-CM is heavily zero-padded).
_CODE_RE = re.compile(r"^[A-Z0-9]+$")

# Expected (source_system, target_system) pairs once both GEMs (4) and RM (2)
# files are loaded. Note: RM files cover ICD-10 -> ICD-9 in both CM and PCS
# directions, which overlap with the backward GEMs files. We assert presence,
# not exclusivity.
_EXPECTED_SYSTEM_PAIRS: set[tuple[str, str]] = {
    ("ICD-9-CM", "ICD-10-CM"),    # gems forward CM
    ("ICD-10-CM", "ICD-9-CM"),    # gems backward CM + RM DX
    ("ICD-9-Vol3", "ICD-10-PCS"), # gems forward PCS
    ("ICD-10-PCS", "ICD-9-Vol3"), # gems backward PCS + RM PR
}


def test_extract_module_importable() -> None:
    """``src.extract.gems`` imports cleanly."""
    pytest.importorskip("src.extract.gems")


def test_parquet_schema() -> None:
    """The silver parquet has every locked-schema column."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run `python -m src.extract.gems` first")

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    # Spot-check column types.
    assert "string" in str(schema.field("source_code").type).lower()
    assert "bool" in str(schema.field("approximate_flag").type).lower()
    assert "int" in str(schema.field("scenario").type).lower()
    assert "date" in str(schema.field("effective_start").type).lower()


def test_row_count_in_range() -> None:
    """Total row count is in 350k-600k band (4 GEMs files + 2 RM files).

    Empirically (FY2018 release):
      I9->I10 CM   ~ 24,860
      I10->I9 CM   ~ 81,593
      I9->I10 PCS  ~ 73,593
      I10->I9 PCS  ~ 101,025
      RM DX 2016   ~ 69,823 source rows -> ~73k after cluster expansion
      RM PR 2016   ~ 71,974 source rows -> ~74k after cluster expansion
      Total        ~ 425k - 430k
    """
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 350_000 <= n <= 600_000, f"Unexpected GEMs row count: {n} (expected 350k-600k)"


def test_codes_uppercase() -> None:
    """Every ``source_code`` is upper-case alphanumeric, dotless.

    ``target_code`` may be NULL on no-map rows; checked codes are non-null.
    """
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["source_code", "target_code"])
    src_codes = table.column("source_code").to_pylist()
    bad_src = [c for c in src_codes if c is None or not _CODE_RE.match(c)]
    assert not bad_src, f"Found {len(bad_src)} non-conforming source codes (sample: {bad_src[:5]})"

    tgt_codes = [c for c in table.column("target_code").to_pylist() if c is not None]
    bad_tgt = [c for c in tgt_codes if not _CODE_RE.match(c)]
    assert not bad_tgt, f"Found {len(bad_tgt)} non-conforming target codes (sample: {bad_tgt[:5]})"


def test_systems_present() -> None:
    """All four expected source/target system pairs appear in the parquet."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["source_system", "target_system"])
    pairs = set(
        zip(
            table.column("source_system").to_pylist(),
            table.column("target_system").to_pylist(),
        )
    )
    missing = _EXPECTED_SYSTEM_PAIRS - pairs
    assert not missing, f"Missing system pairs: {sorted(missing)} (got: {sorted(pairs)})"


def test_effective_dates() -> None:
    """``effective_start`` is 2018-10-01 for every row; ``effective_end`` is NULL."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    assert starts == {_EFFECTIVE_START}, (
        f"effective_start should be {{{_EFFECTIVE_START}}}, got {starts}"
    )
    ends = table.column("effective_end").to_pylist()
    non_null = [e for e in ends if e is not None]
    assert not non_null, f"effective_end should be all NULL, found {len(non_null)} non-null"


def test_known_mapping_diabetes() -> None:
    """Sanity: ICD-9-CM 25000 (diabetes mellitus) maps to ICD-10-CM E119."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    import pyarrow.compute as pc

    table = pq.read_table(_PARQUET, columns=["source_code", "source_system", "target_code", "target_system", "is_reimbursement"])
    mask = pc.and_kleene(
        pc.and_kleene(
            pc.equal(table.column("source_code"), "25000"),
            pc.equal(table.column("source_system"), "ICD-9-CM"),
        ),
        pc.equal(table.column("is_reimbursement"), False),
    )
    filtered = table.filter(mask)
    targets = set(filtered.column("target_code").to_pylist())
    assert "E119" in targets, (
        f"Expected ICD-9-CM 25000 -> ICD-10-CM E119 in GEMs forward map; got targets {targets}"
    )


def test_known_mapping_mi() -> None:
    """Sanity: ICD-9-CM 41001 (acute MI) maps to ICD-10-CM I2109."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    import pyarrow.compute as pc

    table = pq.read_table(_PARQUET, columns=["source_code", "source_system", "target_code", "is_reimbursement"])
    mask = pc.and_kleene(
        pc.and_kleene(
            pc.equal(table.column("source_code"), "41001"),
            pc.equal(table.column("source_system"), "ICD-9-CM"),
        ),
        pc.equal(table.column("is_reimbursement"), False),
    )
    filtered = table.filter(mask)
    targets = set(filtered.column("target_code").to_pylist())
    assert "I2109" in targets, (
        f"Expected ICD-9-CM 41001 -> ICD-10-CM I2109 in GEMs forward map; got targets {targets}"
    )


def test_no_map_target_nulled() -> None:
    """When no_map_flag is True, target_code is NULL (not "NoDx" / "NoPCS")."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    import pyarrow.compute as pc

    table = pq.read_table(_PARQUET, columns=["target_code", "no_map_flag"])
    mask = pc.equal(table.column("no_map_flag"), True)
    filtered = table.filter(mask)
    if filtered.num_rows == 0:
        pytest.skip("No no-map rows in parquet; cannot exercise this assertion")

    non_null = [t for t in filtered.column("target_code").to_pylist() if t is not None]
    assert not non_null, (
        f"Found {len(non_null)} no-map rows with non-NULL target_code "
        f"(sample: {non_null[:5]}); expected literal 'NoDx'/'NoPCS' to be nulled"
    )


def test_rm_rows_have_null_flags() -> None:
    """Reimbursement Mapping rows have NULL flag columns (RM has no GEMs flags)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    import pyarrow.compute as pc

    table = pq.read_table(
        _PARQUET,
        columns=["is_reimbursement", "approximate_flag", "no_map_flag", "scenario"],
    )
    mask = pc.equal(table.column("is_reimbursement"), True)
    filtered = table.filter(mask)
    if filtered.num_rows == 0:
        pytest.skip("No RM rows in parquet")

    for col in ("approximate_flag", "no_map_flag", "scenario"):
        non_null = sum(1 for v in filtered.column(col).to_pylist() if v is not None)
        assert non_null == 0, (
            f"RM rows should have NULL {col}, found {non_null} non-null values"
        )
