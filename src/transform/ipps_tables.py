"""Union per-(fy, version) IPPS Final Rule parquets into two silver tables.

Reads per-source parquets in ``config.SILVER_PER_SOURCE / "ipps"`` (written
by ``src.extract.ipps``), casts each row to its locked schema, asserts
compound-key uniqueness, and writes two unioned parquets into
``config.SILVER``:

- ``ms_drg_weights.parquet`` -- one row per (MS-DRG, FY, version) carrying
  the relative weight, GMLOS, AMLOS, MDC, type, and severity columns
  parsed from each FY's IPPS Final Rule Table 5. Compound key:
  ``(ms_drg, fy, version)``.
- ``ipps_std_amounts.parquet`` -- long-form scalar rates parsed from
  Tables 1A-1E (operating + capital standardized amounts, labor /
  nonlabor shares, capital federal rate, outlier thresholds, wage-index
  anchors, COLA factors). Each scalar value lives in one row keyed by
  ``(fy, version, table, line_item, category, rate_period)``. NULL
  ``category`` and NULL ``rate_period`` are valid grouping values --
  DuckDB GROUP BY treats NULL as a single bucket.

Why TWO tables (heterogeneous content): Table 5 is per-DRG row-keyed
data with a fixed column shape (one row per DRG, columns are weight /
GMLOS / AMLOS / MDC / type / severity). Tables 1A-1E are scalar
rate-anchor pivots: each line item is its own scalar (e.g., "Operating
Standardized Amount Labor", "Capital Federal Rate", "FY Cost Outlier
Threshold") with optional category (national / rural / urban /
puerto_rico / alaska_hawaii) and -- for FY2010 only -- a 1st_half /
2nd_half PPACA split. The two artifacts share no column set; forcing
them into a single unioned schema would produce NULL-heavy unions and
confuse downstream node-vs-edge construction in Phase 8.

The input globs are intentionally rooted at
``silver/per_source/ipps/*.parquet`` rather than the bare
``silver/per_source/*.parquet`` used by codes_master. DuckDB's
``read_parquet`` glob is non-recursive (``*`` doesn't traverse
subdirectories), so the per-source ``ipps/`` subdirectory is enough
isolation -- IPPS rows do not leak into the codes_master union, and
codes_master rows do not leak in here. Mirrors the PFS / OPPS / ASC
subdirectory trick -- no edits to ``src/transform/codes_master.py`` are
required.

For ``ipps_std_amounts``, the input is a list-of-globs covering Tables
1A, 1B, 1C, 1D, and 1E separately rather than a single ``ipps_table1*``
pattern. The list-of-globs form is more explicit (each table is its
own per-source basename family) and avoids accidentally picking up
``ipps_table5_*.parquet`` if a single ``ipps_table*`` glob were
mistakenly used. Same pattern as ``asc_rates._default_rates_globs()``
which separately enumerates AA / BB / FF.

**Schema deviations from plan-v6.md** (logged in
``plans/ipps-url-discovery.md`` and CLAUDE.md landmines, surfaced to PI):

1. ``IPPS_VERSIONS_BY_FY`` is V*.0-only for Phase 3. V*.1 mid-year
   refreshes (April 1 each FY) ship grouper / code-edits ONLY -- they
   do NOT republish Table 5 or Tables 1A-1E. Both silver schemas keep
   ``version`` as VARCHAR with room for ``V*.1`` if CMS ever changes
   that practice; the cardinality assertion is subset of
   ``{V25.0 ... V43.0}``, not equality.
2. FY2008 + FY2009 ship Tables 1A-1D only (Table 1E was added in FY2010
   to expose the Affordable Care Act adjustment). The cardinality
   assertion on ``table`` is subset of ``{1A, 1B, 1C, 1D, 1E}``, not
   equality. Earlier-FY rows simply never carry ``table = "1E"``.
3. FY2010 ships dual rate periods (1st_half pre-PPACA, 2nd_half
   post-PPACA) inside a single ZIP. The locked ``ipps_std_amounts``
   schema carries a nullable ``rate_period`` column whose values are
   ``{"1st_half", "2nd_half", null}``. NULL for all FYs except FY2010.
   The compound key includes ``rate_period`` so that the two FY2010
   half-period rows for the same (fy, version, table, line_item,
   category) tuple are distinguishable.

CLI::

    python -m src.transform.ipps_tables                          # builds both
    python -m src.transform.ipps_tables --target weights         # ms_drg_weights only
    python -m src.transform.ipps_tables --target std_amounts     # ipps_std_amounts only
    python -m src.transform.ipps_tables --target weights --output /tmp/x.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# Locked Phase-3 MS-DRG weights schema. Compound key: (ms_drg, fy, version).
# One row per DRG per FY-version; the ``version`` column is V*.0-only today
# (per Deviation 1) but is sized to absorb a hypothetical V*.1 republication.
_MS_DRG_WEIGHTS_SCHEMA: dict[str, str] = {
    "ms_drg": "VARCHAR",
    "fy": "INTEGER",
    "version": "VARCHAR",
    "title": "VARCHAR",
    "mdc": "VARCHAR",
    "drg_type": "VARCHAR",
    "severity": "VARCHAR",
    "relative_weight": "DOUBLE",
    "gmlos": "DOUBLE",
    "amlos": "DOUBLE",
    "post_acute_transfer_flag": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
}

# Locked Phase-3 IPPS standardized-amounts schema. Compound key:
# (fy, version, table, line_item, category, rate_period). NULL category and
# NULL rate_period are valid grouping values; DuckDB GROUP BY treats NULL
# as a single bucket natively (no COALESCE shim needed). The ``unit``
# column is a closed-vocabulary discriminator on the value's measurement
# kind (dollars vs ratio vs factor vs dollars_per_hour).
_IPPS_STD_AMOUNTS_SCHEMA: dict[str, str] = {
    "fy": "INTEGER",
    "version": "VARCHAR",
    "table": "VARCHAR",
    "line_item": "VARCHAR",
    "category": "VARCHAR",
    "rate_period": "VARCHAR",
    "value": "DOUBLE",
    "unit": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
}


# Cardinality assertions. All are subset (not equality) -- partial coverage
# during incremental backfills should not block transform builds. Closed
# sets are tight enough to catch extractor bugs (mis-classified rows,
# stray sentinel values) without overfitting to any single FY's coverage.

# V25.0 (FY2008) ... V43.0 (FY2026). Phase 3 is V*.0-only per Deviation 1;
# the upper bound (V43) tracks IPPS_FYS = 2008-2026.
_EXPECTED_VERSIONS: frozenset[str] = frozenset(f"V{v}.0" for v in range(25, 44))

# Table 5 type column. Most modern-era DRGs are MED or SURG; null is allowed
# (older FYs may not surface a type column for every row).
_EXPECTED_DRG_TYPES: frozenset[str] = frozenset({"MED", "SURG"})

# Table 5 severity column. Three-tier MS-DRG severity buckets; null allowed
# for DRGs without a severity split.
_EXPECTED_SEVERITY: frozenset[str] = frozenset({"MCC", "CC", "NoCC"})

# Tables 1A-1E. FY2008+FY2009 ship 1A-1D only -- 1E was added FY2010 for
# PPACA -- so the assertion is subset, not equality. The extractor never
# emits any other ``table`` value; any other observed value indicates a
# member-classification bug.
_EXPECTED_TABLES: frozenset[str] = frozenset({"1A", "1B", "1C", "1D", "1E"})

# Closed-vocabulary unit discriminator on each scalar's measurement kind.
# ``dollars`` covers operating + capital standardized amounts and outlier
# thresholds; ``ratio`` covers labor / nonlabor shares; ``factor`` covers
# COLA and budget-neutrality multipliers; ``dollars_per_hour`` covers
# hourly wage-index anchor amounts.
_EXPECTED_UNITS: frozenset[str] = frozenset(
    {"dollars", "ratio", "factor", "dollars_per_hour"}
)

# Category discriminator for the standardized-amounts pivot. NULL is a
# valid value (and is asserted separately as "expected"); the closed set
# below is checked only for non-null observations.
_EXPECTED_CATEGORIES: frozenset[str] = frozenset(
    {"national", "rural", "urban", "puerto_rico", "alaska_hawaii"}
)

# Rate-period discriminator. NULL is the expected value for every FY
# except FY2010; FY2010 ships exactly the two values below (1st_half =
# pre-PPACA Oct 1 2009 - Mar 31 2010; 2nd_half = post-PPACA Apr 1 2010 -
# Sep 30 2010). The closed set is checked only for non-null observations.
_EXPECTED_RATE_PERIODS: frozenset[str] = frozenset({"1st_half", "2nd_half"})


def _projection_sql(
    schema: dict[str, str],
    overrides: dict[str, str] | None = None,
) -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from any per-(fy,
    version) parquet are read as NULL by ``read_parquet``, so the explicit
    CAST here both enforces the schema and guarantees missing-column
    tolerance for any older FY's files.

    ``overrides`` lets a caller substitute a custom SQL expression for a
    specific column. The override expression must already produce the
    locked dtype (no extra CAST is wrapped around it). Keys not present in
    ``overrides`` fall back to the default ``CAST("col" AS dtype) AS "col"``.
    """
    overrides = overrides or {}
    parts: list[str] = []
    for col, dtype in schema.items():
        if col in overrides:
            parts.append(f'{overrides[col]} AS "{col}"')
        else:
            parts.append(f'CAST("{col}" AS {dtype}) AS "{col}"')
    return ",\n        ".join(parts)


# FLAG 5 (Session 9.5): FY2008 ships 42.0 sentinel for reserved DRGs 998/999
# in raw IPPS Table 5; force to 0.0 to match FY2009+ convention.
_MS_DRG_WEIGHTS_OVERRIDES: dict[str, str] = {
    "relative_weight": (
        "CAST(CASE WHEN fy = 2008 AND ms_drg IN ('998', '999') "
        "THEN 0.0 ELSE relative_weight END AS DOUBLE)"
    ),
}


def _default_weights_glob() -> str:
    """Return the default glob feeding ``build_ms_drg_weights``.

    Single string -- Table 5 has one per-source basename family
    (``ipps_table5_*.parquet``).
    """
    return str(config.SILVER_PER_SOURCE / "ipps" / "ipps_table5_*.parquet")


def _default_std_amounts_globs() -> list[str]:
    """Return the default list of globs feeding ``build_ipps_std_amounts``.

    Tables 1A-1E are filed under separate per-source basenames
    (``ipps_table1a_*.parquet``, ``ipps_table1b_*.parquet``, etc.). A
    list-of-5-globs is the most portable form across DuckDB versions
    (brace expansion is supported but not the documented surface). The
    explicit list also defends against a single ``ipps_table1*`` glob
    accidentally picking up Table 5 (which has a different schema).
    """
    ipps_dir = config.SILVER_PER_SOURCE / "ipps"
    return [
        str(ipps_dir / "ipps_table1a_*.parquet"),
        str(ipps_dir / "ipps_table1b_*.parquet"),
        str(ipps_dir / "ipps_table1c_*.parquet"),
        str(ipps_dir / "ipps_table1d_*.parquet"),
        str(ipps_dir / "ipps_table1e_*.parquet"),
    ]


def build_ms_drg_weights(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(fy, version) IPPS Table 5 parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "ipps" / "ipps_table5_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "ms_drg_weights.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(ms_drg, fy, version)`` triples are found, if the
        row count of the unioned output does not equal the sum of input
        row counts, if no rows are produced, or if any observed
        ``version``, ``drg_type``, or ``severity`` falls outside its
        expected closed set.
    """
    glob = (
        input_glob
        if input_glob is not None
        else _default_weights_glob()
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "ms_drg_weights.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        # Pre-flight: enumerate input files so we can both fail loudly on
        # an empty match and compute the sum-of-inputs row count for the
        # parity check.
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.ipps --fy-range 2008-2026 "
                "--table table5) first."
            )

        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # Duplicate-tuple check on the unioned input. ORDER BY references
        # only GROUP BY columns (Session 3 fix from pfs_rates.py:163) --
        # DuckDB's binder rejects ORDER BY on non-grouped columns.
        dups = con.execute(
            f"""
            SELECT ms_drg, fy, version, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_MS_DRG_WEIGHTS_SCHEMA, _MS_DRG_WEIGHTS_OVERRIDES)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, fy, version, ms_drg
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (ms_drg, fy, version) triples in per-source "
                f"union (showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet. Output
        # path is inlined into the SQL because DuckDB's parameter binding
        # mis-resolves a second `?` in COPY ... TO ? -- the binding ends
        # up routed back into read_parquet(). Single-bind avoids it. See
        # codes_master.py landmine and pfs_rates.py:181 for the original
        # note.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_MS_DRG_WEIGHTS_SCHEMA, _MS_DRG_WEIGHTS_OVERRIDES)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [glob],
        )

        # Row-count parity: every input row must land in the output.
        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, output has {output_row_count}."
            )

        # Cheap non-empty assertion -- guards against the failure mode
        # where every per-source parquet exists but each is empty.
        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned ms_drg_weights {out} has zero rows. "
                "Re-run src.extract.ipps before continuing."
            )

        # Version cardinality assertion: observed ⊆ {V25.0 ... V43.0}.
        observed_versions = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT version FROM read_parquet(?) "
                "WHERE version IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_versions = observed_versions - _EXPECTED_VERSIONS
        if unexpected_versions:
            raise RuntimeError(
                f"Unioned ms_drg_weights {out} contains unexpected "
                f"version values: {sorted(unexpected_versions)}. "
                f"Expected subset of: {sorted(_EXPECTED_VERSIONS)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # DRG type cardinality assertion: non-null observed ⊆ {MED, SURG}.
        observed_drg_types = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT drg_type FROM read_parquet(?) "
                "WHERE drg_type IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_drg_types = observed_drg_types - _EXPECTED_DRG_TYPES
        if unexpected_drg_types:
            raise RuntimeError(
                f"Unioned ms_drg_weights {out} contains unexpected "
                f"drg_type values: {sorted(unexpected_drg_types)}. "
                f"Expected subset of: {sorted(_EXPECTED_DRG_TYPES)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # Severity cardinality assertion: non-null observed ⊆ {MCC, CC, NoCC}.
        observed_severity = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT severity FROM read_parquet(?) "
                "WHERE severity IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_severity = observed_severity - _EXPECTED_SEVERITY
        if unexpected_severity:
            raise RuntimeError(
                f"Unioned ms_drg_weights {out} contains unexpected "
                f"severity values: {sorted(unexpected_severity)}. "
                f"Expected subset of: {sorted(_EXPECTED_SEVERITY)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # MDC cardinality bound: at most 26 distinct values (25 numeric
        # MDCs 01-25 + the literal "PRE" pre-MDC bucket). Subset of an
        # explicit closed set isn't enforced -- MDC label spelling drifts
        # across FYs (zero-padded vs unpadded numeric, "MDC 01" vs "01")
        # and the line_item alias dict is not the source of truth here.
        # A cardinality bound catches the failure mode where the
        # extractor leaks descriptive prose into the MDC column.
        observed_mdc_count: int = con.execute(
            "SELECT COUNT(DISTINCT mdc) FROM read_parquet(?) "
            "WHERE mdc IS NOT NULL",
            [str(out)],
        ).fetchone()[0]
        if observed_mdc_count > 26:
            raise RuntimeError(
                f"Unioned ms_drg_weights {out} contains {observed_mdc_count} "
                "distinct mdc values; expected at most 26 (25 numeric MDCs "
                "+ PRE). Re-run src.extract.ipps before continuing."
            )
    finally:
        con.close()

    return out


def build_ipps_std_amounts(
    input_glob: str | list[str] | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(fy, version) IPPS Tables 1A-1E parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression (or list of globs) passed to DuckDB's
        ``read_parquet``. Defaults to a 5-element list of globs covering
        ``ipps_table1a_*.parquet`` ... ``ipps_table1e_*.parquet`` under
        ``config.SILVER_PER_SOURCE / "ipps"``. A single string may be
        supplied to override (e.g., for tests).
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "ipps_std_amounts.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob(s) match no files.
    RuntimeError
        If duplicate ``(fy, version, table, line_item, category,
        rate_period)`` 6-tuples are found, if the row count of the
        unioned output does not equal the sum of input row counts, if no
        rows are produced, or if any observed ``version``, ``table``,
        ``unit``, ``category``, or ``rate_period`` falls outside its
        expected closed set.
    """
    if input_glob is None:
        globs: list[str] = _default_std_amounts_globs()
    elif isinstance(input_glob, str):
        globs = [input_glob]
    else:
        globs = list(input_glob)

    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "ipps_std_amounts.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        # Pre-flight: enumerate input files across every glob so we can
        # both fail loudly on an empty match and compute the sum-of-inputs
        # row count for the parity check.
        input_files: list[str] = []
        for g in globs:
            input_files.extend(
                row[0]
                for row in con.execute("SELECT file FROM glob(?)", [g]).fetchall()
            )
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob(s): {globs!r}. "
                "Run the extract module (src.extract.ipps --fy-range 2008-2026 "
                "--table tables1) first."
            )

        # Sum of per-file row counts. This is the invariant we check
        # against the unioned output: the projection must not silently
        # drop rows. Pass the materialized file list directly to
        # read_parquet -- DuckDB accepts a list of paths the same way it
        # accepts a list of globs.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [input_files],
        ).fetchone()[0]

        # Duplicate-tuple check on the unioned input. GROUP BY treats
        # NULL category / NULL rate_period as single buckets -- DuckDB's
        # native NULL-equal-NULL grouping semantics -- so the FY2010
        # 1st_half / 2nd_half rows are correctly distinguished from each
        # other AND from FY2011+ rows where rate_period is NULL. ORDER BY
        # references only GROUP BY columns (Session 3 fix from
        # pfs_rates.py:163).
        dups = con.execute(
            f"""
            SELECT fy, version, "table", line_item, category, rate_period,
                   COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_IPPS_STD_AMOUNTS_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3, 4, 5, 6
            HAVING c > 1
            ORDER BY c DESC, fy, version, "table", line_item
            LIMIT 5
            """,
            [input_files],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (fy, version, table, line_item, category, "
                "rate_period) 6-tuples in per-source union (showing up "
                f"to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet. Output
        # path inlined (single-bind avoidance bug) -- see codes_master.py
        # landmine and pfs_rates.py:181 for the original note.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_IPPS_STD_AMOUNTS_SCHEMA)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [input_files],
        )

        # Row-count parity: every input row must land in the output.
        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, output has {output_row_count}."
            )

        # Cheap non-empty assertion -- guards against the failure mode
        # where every per-source parquet exists but each is empty.
        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} has zero rows. "
                "Re-run src.extract.ipps before continuing."
            )

        # Version cardinality assertion: observed ⊆ {V25.0 ... V43.0}.
        observed_versions = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT version FROM read_parquet(?) "
                "WHERE version IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_versions = observed_versions - _EXPECTED_VERSIONS
        if unexpected_versions:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} contains unexpected "
                f"version values: {sorted(unexpected_versions)}. "
                f"Expected subset of: {sorted(_EXPECTED_VERSIONS)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # Table cardinality assertion: observed ⊆ {1A, 1B, 1C, 1D, 1E}.
        # Subset (not equality) is REQUIRED -- FY2008/FY2009 ship 1A-1D
        # only (Table 1E was added FY2010 for PPACA).
        observed_tables = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT \"table\" FROM read_parquet(?) "
                "WHERE \"table\" IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_tables = observed_tables - _EXPECTED_TABLES
        if unexpected_tables:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} contains unexpected "
                f"table values: {sorted(unexpected_tables)}. "
                f"Expected subset of: {sorted(_EXPECTED_TABLES)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # Unit cardinality assertion: non-null observed ⊆ closed set.
        observed_units = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT unit FROM read_parquet(?) "
                "WHERE unit IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_units = observed_units - _EXPECTED_UNITS
        if unexpected_units:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} contains unexpected "
                f"unit values: {sorted(unexpected_units)}. "
                f"Expected subset of: {sorted(_EXPECTED_UNITS)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # Category cardinality assertion: non-null observed ⊆ closed set.
        # NULL is allowed and not asserted against -- many line items
        # (e.g., "Capital Federal Rate") have no geographic split.
        observed_categories = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT category FROM read_parquet(?) "
                "WHERE category IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_categories = observed_categories - _EXPECTED_CATEGORIES
        if unexpected_categories:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} contains unexpected "
                f"category values: {sorted(unexpected_categories)}. "
                f"Expected subset of: {sorted(_EXPECTED_CATEGORIES)}. "
                "Re-run src.extract.ipps before continuing."
            )

        # Rate-period cardinality assertion: non-null observed ⊆
        # {1st_half, 2nd_half}. NULL is allowed and is the EXPECTED
        # value for every FY except FY2010.
        observed_rate_periods = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT rate_period FROM read_parquet(?) "
                "WHERE rate_period IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_rate_periods = observed_rate_periods - _EXPECTED_RATE_PERIODS
        if unexpected_rate_periods:
            raise RuntimeError(
                f"Unioned ipps_std_amounts {out} contains unexpected "
                f"rate_period values: {sorted(unexpected_rate_periods)}. "
                f"Expected subset of: {sorted(_EXPECTED_RATE_PERIODS)}. "
                "Re-run src.extract.ipps before continuing."
            )
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.ipps_tables",
        description=(
            "Union per-(fy, version) IPPS Final Rule parquets into "
            "data/silver/ms_drg_weights.parquet and "
            "data/silver/ipps_std_amounts.parquet."
        ),
    )
    parser.add_argument(
        "--target",
        choices=("weights", "std_amounts", "all"),
        default="all",
        help="Which silver table(s) to build. Default: all.",
    )
    parser.add_argument(
        "--input-glob",
        default=None,
        help=(
            "Glob for per-source parquets. Applied only to the chosen target "
            "(invalid with --target all). "
            "Defaults: data/silver/per_source/ipps/ipps_table5_*.parquet for "
            "weights, 5-element list for std_amounts (Tables 1A-1E)."
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help=(
            "Output parquet path. Applied only to the chosen target "
            "(invalid with --target all). "
            "Defaults: data/silver/ms_drg_weights.parquet for weights, "
            "data/silver/ipps_std_amounts.parquet for std_amounts."
        ),
    )
    args = parser.parse_args(argv)

    if args.target == "all" and (
        args.input_glob is not None or args.output is not None
    ):
        parser.error(
            "--input-glob and --output cannot be used with --target all. "
            "Pick a single --target weights|std_amounts."
        )

    outputs: list[Path] = []
    if args.target in ("weights", "all"):
        outputs.append(
            build_ms_drg_weights(
                input_glob=args.input_glob if args.target == "weights" else None,
                output_path=args.output if args.target == "weights" else None,
            )
        )
    if args.target in ("std_amounts", "all"):
        outputs.append(
            build_ipps_std_amounts(
                input_glob=args.input_glob if args.target == "std_amounts" else None,
                output_path=args.output if args.target == "std_amounts" else None,
            )
        )

    con = duckdb.connect()
    try:
        for out in outputs:
            n = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(out)]
            ).fetchone()[0]
            print(f"Wrote {n} rows to {out}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
