"""Phase 1 (Session 2) tests for the HCPCS Level II extractor.

Mirrors ``tests/test_icd10pcs.py`` shape:

  - ``test_extract_module_importable`` skips until ``src.extract.hcpcs`` lands.
  - All data-dependent assertions skip via ``pytest.skip`` when the per-CY
    parquet doesn't exist (so the suite passes both pre- and post-extract).

Test target: CY2020 (modern .xls era, mid-range, easy to validate).
"""

from __future__ import annotations

import re

import pyarrow.parquet as pq
import pytest

from src import config

_CY = 2020
_PARQUET = config.SILVER_PER_SOURCE / f"hcpcs_cy{_CY}.parquet"
_CY_START, _CY_END = config.cy_interval(_CY)

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

# HCPCS Level II shape: 5-char [A-V0-9][0-9]{4} upper-case dotless.
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")


def test_extract_module_importable() -> None:
    """Skips until ``src.extract.hcpcs`` lands."""
    pytest.importorskip("src.extract.hcpcs")


def test_cy2020_parquet_schema() -> None:
    """The CY2020 HCPCS parquet has every column in the locked schema."""
    if not _PARQUET.exists():
        pytest.skip(
            f"{_PARQUET} not present; run `python -m src.extract.hcpcs --cy {_CY}` first"
        )

    pf = pq.ParquetFile(_PARQUET)
    schema = pf.schema_arrow
    actual_columns = set(schema.names)
    missing = _EXPECTED_COLUMNS - actual_columns
    assert not missing, f"Missing columns in {_PARQUET.name}: {sorted(missing)}"

    # Type sanity. We only check load-bearing types — the rest are validated by
    # the codes_master union step.
    assert "string" in str(schema.field("code").type).lower() or str(
        schema.field("code").type
    ) in ("string", "large_string"), (
        f"`code` must be a string type, got {schema.field('code').type}"
    )
    assert "int" in str(schema.field("fy_or_cy").type).lower(), (
        f"`fy_or_cy` must be an integer type, got {schema.field('fy_or_cy').type}"
    )
    assert "date" in str(schema.field("effective_start").type).lower(), (
        f"`effective_start` must be a date type, got "
        f"{schema.field('effective_start').type}"
    )
    assert "date" in str(schema.field("effective_end").type).lower(), (
        f"`effective_end` must be a date type, got {schema.field('effective_end').type}"
    )


def test_cy2020_row_count_in_range() -> None:
    """HCPCS Level II has roughly 9k codes/year (modifiers excluded)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    n = pq.ParquetFile(_PARQUET).metadata.num_rows
    assert 5_000 < n < 15_000, (
        f"Unexpected HCPCS CY{_CY} row count: {n} (expected ~9k, "
        f"5k-15k tolerated)"
    )


def test_cy2020_codes_5char_uppercase() -> None:
    """Every HCPCS Level II ``code`` matches ``^[A-V0-9][0-9]{4}$``."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["code"])
    codes = table.column("code").to_pylist()
    bad = [c for c in codes if c is None or not _HCPCS_CODE_RE.match(c)]
    assert not bad, (
        f"Found {len(bad)} non-conforming HCPCS codes "
        f"(must be 5-char [A-V0-9][0-9]{{4}}; sample: {bad[:5]})"
    )


def test_cy2020_chapter_null() -> None:
    """``chapter`` is ICD-10-CM-only — must be NULL for every HCPCS row."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["chapter"])
    n = table.num_rows
    null_count = table.column("chapter").null_count
    assert null_count == n, (
        f"Expected all {n} HCPCS rows to have NULL chapter, but "
        f"{n - null_count} are non-null"
    )


def test_cy2020_effective_dates() -> None:
    """All rows span the CY2020 interval (2020-01-01 .. 2020-12-31)."""
    if not _PARQUET.exists():
        pytest.skip(f"{_PARQUET} not present; run extractor first")

    table = pq.read_table(_PARQUET, columns=["effective_start", "effective_end"])
    starts = set(table.column("effective_start").to_pylist())
    ends = set(table.column("effective_end").to_pylist())
    assert starts == {_CY_START}, (
        f"effective_start should be {{{_CY_START}}}, got {starts}"
    )
    assert ends == {_CY_END}, (
        f"effective_end should be {{{_CY_END}}}, got {ends}"
    )
