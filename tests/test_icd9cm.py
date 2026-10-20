"""Phase 1 tests for the ICD-9-CM (diagnosis) extractor.

Tests run in two modes:

1. **Without real data present**: data-dependent assertions skip via
   ``pytest.skip``; the module-import test uses ``pytest.importorskip`` so it
   skips until the extractor lands.

2. **With real data present**: validates schema parity with the locked
   ``codes_master._SCHEMA``, row-count band, code regex, FY interval, and the
   ``code_system="ICD-9-CM"`` invariant.
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

# FY2014 sits cleanly in the frozen-update era (FY2012-FY2015) and is the
# canonical first-pass smoke test per the Phase B3 spec.
_FY = 2014
_PARQUET = config.SILVER_PER_SOURCE / f"icd9cm_fy{_FY}.parquet"
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

# ICD-9-CM diagnosis codes are dotless, upper-case, alphanumeric (V/E prefix
# allowed). Per CLAUDE.md conventions: no internal dot, upper-case.
_CODE_RE = re.compile(r"^[A-Z0-9]+$")


def test_extract_module_importable() -> None:
    """Skips until ``src.extract.icd9cm`` lands."""
    pytest.importorskip("src.extract.icd9cm")


def test_fy2014_parquet_schema() -> None:
    """The FY2014 parquet has every column in the locked schema."""
    if not _PARQUET.exists():
        pytest.skip(
            f"{_PARQUET} not present; run `python -m src.extract.icd9cm --fy {_FY}` first"
        )

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    assert "string" in str(schema.field("code").type).lower() or str(
        schema.field("code").type
    ) in ("string", "large_string"), (
        f"`code` must be a string type, got {schema.field('code').type}"
    )
    assert "int" in str(schema.field("fy_or_cy").type).lower(), (
        f"`fy_or_cy` must be an integer type, got {schema.field('fy_or_cy').type}"
    )
    assert "date" in str(schema.field("effective_start").type).lower(), (
        f"`effective_start` must be a date type, got {schema.field('effective_start').type}"
    )
    assert "date" in str(schema.field("effective_end").type).lower(), (
        f"`effective_end` must be a date type, got {schema.field('effective_end').type}"
    )


def test_fy2014_row_count_in_range() -> None:
    """ICD-9-CM FY2014 has roughly 14k-17k diagnosis codes (band 12k-25k)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 12_000 < n < 25_000, (
        f"Unexpected ICD-9-CM FY{_FY} row count: {n} (expected ~14k-17k)"
    )


def test_fy2014_codes_uppercase() -> None:
    """Every ICD-9-CM ``code`` is upper-case alphanumeric, no internal dot."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [c for c in codes if c is None or not _CODE_RE.match(c)]
    assert not bad, (
        f"Found {len(bad)} non-conforming codes (sample: {bad[:5]})"
    )


def test_fy2014_effective_dates() -> None:
    """All rows span the FY2014 interval (2013-10-01 .. 2014-09-30)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_FY_START}, f"effective_start should be {{{_FY_START}}}, got {starts}"
    assert ends == {_FY_END}, f"effective_end should be {{{_FY_END}}}, got {ends}"


def test_fy2014_code_system() -> None:
    """Every row carries ``code_system='ICD-9-CM'``."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["code_system"])
    systems = set(table.column("code_system").to_pylist())
    assert systems == {"ICD-9-CM"}, f"Expected only ICD-9-CM, got {systems}"
