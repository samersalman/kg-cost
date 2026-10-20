"""Phase 1 tests for the ICD-10-CM extractor.

These tests are designed to run in two modes:

1. **Without real data present**: every assertion that requires the per-source
   parquet to exist is wrapped in ``pytest.skip(...)``. The module-import test
   uses ``pytest.importorskip`` so it skips (rather than errors) until the
   ``src.extract.icd10cm`` extractor lands.

2. **With real data present**: after running
   ``python -m src.extract.icd10cm --fy 2026`` the data-dependent assertions
   exercise row counts, casing, FY interval, and chapter coverage.
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

# FY2026 covers Oct 1, 2025 — Sep 30, 2026.
_FY = 2026
_PARQUET = config.SILVER_PER_SOURCE / f"icd10cm_fy{_FY}.parquet"
_FY_START, _FY_END = config.fy_interval(_FY)

# The locked Phase-1 schema column set (plan-v1.md lines 105-123). Tests assert
# that each column name appears in the parquet's arrow schema; type checks are
# kept loose because pyarrow surfaces DATE as ``date32`` and INTEGER as
# ``int32``/``int64`` depending on the writer.
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

# Code regex: upper-case alphanumeric, no internal dot (CDC files are dotless).
_CODE_RE = re.compile(r"^[A-Z0-9]+$")


def test_extract_module_importable() -> None:
    """The extractor module must import cleanly once Batch 1 ships it.

    Until then we skip rather than fail so CI stays green during the bootstrap
    window.
    """
    pytest.importorskip("src.extract.icd10cm")


def test_fy2026_parquet_schema() -> None:
    """The FY2026 per-source parquet has every column in the locked schema."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run `python -m src.extract.icd10cm --fy {_FY}` first")

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    # Spot-check column types: code must be string, fy_or_cy must be integer,
    # effective_{start,end} must be a date type.
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
    """ICD-10-CM FY2026 has roughly 74,000 codes (plan-v1.md: ``≈ 74,000``)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 60_000 < n < 90_000, f"Unexpected ICD-10-CM FY{_FY} row count: {n} (expected ~74k)"


def test_fy2026_codes_uppercase() -> None:
    """Every ``code`` value is upper-case alphanumeric, with no internal dot.

    CDC publishes ICD-10-CM codes dotless; we mirror that. The conventions in
    ``CLAUDE.md`` further require upper-case, left-padded text.
    """
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [c for c in codes if c is None or not _CODE_RE.match(c)]
    assert not bad, f"Found {len(bad)} non-conforming codes (sample: {bad[:5]})"


def test_fy2026_effective_dates() -> None:
    """All rows span the FY2026 interval (2025-10-01 .. 2026-09-30)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_FY_START}, f"effective_start should be {{{_FY_START}}}, got {starts}"
    assert ends == {_FY_END}, f"effective_end should be {{{_FY_END}}}, got {ends}"


def test_fy2026_chapter_nonnull() -> None:
    """``chapter`` must be populated for every ICD-10-CM row (sourced from tabular XML)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["chapter"])
    null_count = table.column("chapter").null_count
    assert null_count == 0, f"Found {null_count} rows with NULL chapter in ICD-10-CM FY{_FY}"
