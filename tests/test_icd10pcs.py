"""Phase 1 tests for the ICD-10-PCS extractor.

Tests run in two modes:

1. **Without real data present**: data-dependent assertions skip via
   ``pytest.skip``; the module-import test uses ``pytest.importorskip`` so it
   skips until the extractor lands.

2. **With real data present**: validates row count, fixed-width 7-char codes,
   the PCS-specific NULL-``chapter`` invariant, and the FY interval.
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

# FY2026 covers Oct 1, 2025 — Sep 30, 2026.
_FY = 2026
_PARQUET = config.SILVER_PER_SOURCE / f"icd10pcs_fy{_FY}.parquet"
_FY_START, _FY_END = config.fy_interval(_FY)

_EXPECTED_COLUMNS: set[str] = {
    "code",
    "code_system",
    "fy_or_cy",
    "effective_start",
    "effective_end",
    "description",
    "short_description",
    "chapter",
    "addendum_action",
    "or_procedure_flag",
    "mcc_flag",
    "cc_flag",
    "poa_exempt",
    "source",
    "n",
}

# ICD-10-PCS is fixed-width 7-character upper-case alphanumeric (no dot).
_PCS_CODE_RE = re.compile(r"^[A-Z0-9]{7}$")


def test_extract_module_importable() -> None:
    """Skips until ``src.extract.icd10pcs`` lands."""
    pytest.importorskip("src.extract.icd10pcs")


def test_fy2026_parquet_schema() -> None:
    """The FY2026 PCS parquet has every column in the locked schema."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run `python -m src.extract.icd10pcs --fy {_FY}` first")

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    assert "string" in str(schema.field("code").type).lower() or str(schema.field("code").type) in (
        "string",
        "large_string",
    ), f"`code` must be a string type, got {schema.field('code').type}"
    assert "int" in str(schema.field("fy_or_cy").type).lower(), (
        f"`fy_or_cy` must be an integer type, got {schema.field('fy_or_cy').type}"
    )
    assert "date" in str(schema.field("effective_start").type).lower(), (
        f"`effective_start` must be a date type, got {schema.field('effective_start').type}"
    )
    assert "date" in str(schema.field("effective_end").type).lower(), (
        f"`effective_end` must be a date type, got {schema.field('effective_end').type}"
    )


def test_fy2026_row_count_in_range() -> None:
    """ICD-10-PCS FY2026 has roughly 78,000 codes (plan-v1.md: ``≈ 78,000``)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 60_000 < n < 90_000, f"Unexpected ICD-10-PCS FY{_FY} row count: {n} (expected ~78k)"


def test_fy2026_codes_7char_uppercase() -> None:
    """Every PCS ``code`` is exactly 7 upper-case alphanumeric characters."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [c for c in codes if c is None or not _PCS_CODE_RE.match(c)]
    assert not bad, (
        f"Found {len(bad)} non-conforming PCS codes "
        f"(must be 7-char upper alnum; sample: {bad[:5]})"
    )


def test_fy2026_chapter_null() -> None:
    """``chapter`` is ICD-10-CM-only — must be NULL for every PCS row."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["chapter"])
    n = table.num_rows
    null_count = table.column("chapter").null_count
    assert null_count == n, (
        f"Expected all {n} PCS rows to have NULL chapter, but {n - null_count} are non-null"
    )


def test_fy2026_effective_dates() -> None:
    """All rows span the FY2026 interval (2025-10-01 .. 2026-09-30)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_FY_START}, f"effective_start should be {{{_FY_START}}}, got {starts}"
    assert ends == {_FY_END}, f"effective_end should be {{{_FY_END}}}, got {ends}"
