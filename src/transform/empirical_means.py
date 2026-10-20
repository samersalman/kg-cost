"""Union the four Phase-5 PUF per-source subdirs into ``silver/empirical_means.parquet``.

Reads per-source parquets emitted by the four Phase-5 PUF extractors:

- ``src.extract.puf_inpatient`` →
  ``silver/per_source/puf/inpatient/puf_inpatient_fy*.parquet``
- ``src.extract.puf_inpatient_summary`` →
  ``silver/per_source/puf/inpatient_summary/puf_inpatient_summary_fy*.parquet``
- ``src.extract.puf_outpatient`` →
  ``silver/per_source/puf/outpatient/puf_outpatient_cy*.parquet``
- ``src.extract.puf_physician`` →
  ``silver/per_source/puf/physician/puf_physician_cy*.parquet``

All four extractors emit the identical 27-column locked schema (Session 8.5
schema rename: dropped ``mean_allowed``; added ``payment_amount`` +
``payment_amount_concept`` enum + ``total_payment_amount`` per Phase 5
Session 8.5 PI verdict on DEV 4). This transform unions them, casts to
the locked schema, and asserts:

- ``code_system`` ⊆ ``{"MS-DRG", "APC", "HCPCS-II"}``
- ``source`` ⊆ ``{"puf_top100", "puf_all_drgs_ry23",
  "puf_top100_summary", "puf_outpatient", "puf_physician"}``
- ``(code, code_system, year, place_of_srvc, source)`` is unique. ``source``
  is in the key so per-DRG rows from ``puf_top100`` /
  ``puf_all_drgs_ry23`` (per-provider) can coexist with the
  ``puf_top100_summary`` row for the same (DRG, FY).
- Output row count == sum of per-source input row counts (no silent drops).
- ``payment_amount`` IS NOT NULL on every row.
- ``payment_amount_concept`` ∈ ``{"medicare_allowed",
  "total_payment_allpayer"}`` on every row (never null).

Per-source NULL semantics differ (e.g., inpatient nulls ``mean_allowed``;
summary nulls all percentiles + ``n_providers``); the transform does NOT
attempt to fill or reconcile those. Phase 7 cost-compute decides
preference (per-provider over summary, etc.).

The input globs are intentionally rooted at
``silver/per_source/puf/<source>/*.parquet`` rather than the bare
``silver/per_source/*.parquet`` used by codes_master. DuckDB's
``read_parquet`` glob is non-recursive (``*`` doesn't traverse
subdirectories), so the per-source ``puf/<source>/`` subdirectory tree is
enough isolation — PUF rows do not leak into the codes_master union, and
codes_master rows do not leak in here. Mirrors the PFS / OPPS / ASC /
IPPS subdirectory trick — no edits to ``src/transform/codes_master.py``
are required.

CLI::

    python -m src.transform.empirical_means
    python -m src.transform.empirical_means --output /tmp/x.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from src import config

# The locked Phase-5 schema (plan-v11 §Schema, Session 8.5 schema
# rename) — column name -> DuckDB type. The SELECT projection below
# mirrors this dict in order. 27 columns total. ``source_host`` was
# inserted between ``source`` and ``effective_start`` in plan-v11 to
# distinguish cms.gov vs NBER vs Wayback origin per-row. Session 8.5
# DEV 4 dropped ``mean_allowed`` (semantic-overloaded) and replaced it
# with ``payment_amount`` (NOT NULL) + ``payment_amount_concept`` enum
# + ``total_payment_amount`` (self-documenting alias on inpatient).
_SCHEMA: dict[str, str] = {
    "scope": "VARCHAR",
    "code": "VARCHAR",
    "code_system": "VARCHAR",
    "year": "INTEGER",
    "place_of_srvc": "VARCHAR",
    "n": "BIGINT",
    "mean_charge": "DOUBLE",
    "mean_payment": "DOUBLE",
    "payment_amount": "DOUBLE",
    "payment_amount_concept": "VARCHAR",
    "total_payment_amount": "DOUBLE",
    "mean_payment_stdzd": "DOUBLE",
    "p10_charge": "DOUBLE",
    "p25_charge": "DOUBLE",
    "p50_charge": "DOUBLE",
    "p75_charge": "DOUBLE",
    "p90_charge": "DOUBLE",
    "p10_payment": "DOUBLE",
    "p25_payment": "DOUBLE",
    "p50_payment": "DOUBLE",
    "p75_payment": "DOUBLE",
    "p90_payment": "DOUBLE",
    "n_providers": "INTEGER",
    "source": "VARCHAR",
    "source_host": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
}

# Cardinality assertions. Any other value indicates an extractor bug
# (mis-classified scope or mis-labeled source string).
_EXPECTED_CODE_SYSTEMS: frozenset[str] = frozenset({"MS-DRG", "APC", "HCPCS-II"})
# Session 8.5 PI verdict on DEV 1: ``puf_all_drgs`` renamed to
# ``puf_all_drgs_ry23`` to capture the RY23-unified-release distinct
# methodology. ``puf_all_drgs_legacy`` is reserved as a future tag for
# hypothetical fallback to original-vintage annual releases (no rows
# emitted in this session).
_EXPECTED_SOURCES: frozenset[str] = frozenset({
    "puf_top100",
    "puf_all_drgs_ry23",
    "puf_top100_summary",
    "puf_outpatient",
    "puf_physician",
})

# Session 8.5 PI verdict on DEV 4: ``payment_amount_concept`` enum
# distinguishes Medicare-allowed (PFS / OPPS CY2015+ outpatient) from
# all-payer total payment (inpatient PUF / NBER outpatient pre-CY2014).
# Phase 7 cost-compute MUST never silently average across the two.
_EXPECTED_CONCEPTS: frozenset[str] = frozenset(
    {"medicare_allowed", "total_payment_allpayer"}
)


def _projection_sql(schema: dict[str, str]) -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from any per-source
    parquet are read as NULL by ``read_parquet``, so the explicit CAST here
    both enforces the schema and guarantees missing-column tolerance for any
    older per-source file. Mirrors ``asc_rates._projection_sql`` /
    ``codes_master._projection_sql``.
    """
    return ",\n        ".join(
        f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in schema.items()
    )


def _default_input_globs() -> list[str]:
    """Return the 4-element list of globs feeding ``build_empirical_means``.

    Each PUF extractor writes into its own subdirectory under
    ``silver/per_source/puf/<source>/`` to keep subdirectory isolation from
    codes_master's bare ``silver/per_source/*.parquet`` glob (which is
    non-recursive in DuckDB).
    """
    puf_dir = config.SILVER_PER_SOURCE / "puf"
    return [
        str(puf_dir / "inpatient" / "puf_inpatient_fy*.parquet"),
        str(puf_dir / "inpatient_summary" / "puf_inpatient_summary_fy*.parquet"),
        str(puf_dir / "outpatient" / "puf_outpatient_cy*.parquet"),
        str(puf_dir / "physician" / "puf_physician_cy*.parquet"),
    ]


def build_empirical_means(
    *,
    input_globs: list[str] | None = None,
    output_path: Path | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """Union all PUF per-source parquets into ``silver/empirical_means.parquet``.

    Parameters
    ----------
    input_globs:
        List of glob expressions passed to DuckDB's ``read_parquet``.
        Defaults to a 4-element list covering every PUF per-source subdir
        under ``config.SILVER_PER_SOURCE / "puf"``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "empirical_means.parquet"``.
    con:
        Optional pre-existing DuckDB connection (useful for tests). When
        ``None``, a fresh in-memory connection is opened and closed.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    RuntimeError
        If ALL input globs are empty (none of the 4 per-source subdirs
        contain parquets); if duplicate
        ``(code, code_system, year, place_of_srvc, source)`` 5-tuples
        are found; if the output row count does not equal the sum of
        per-source input row counts; if any observed ``code_system`` is
        outside ``{MS-DRG, APC, HCPCS-II}``; if any observed ``source``
        is outside the 5 expected literals; if any row has NULL
        ``payment_amount``; if any row has a ``payment_amount_concept``
        outside ``{medicare_allowed, total_payment_allpayer}`` (NULL
        included); or if ``total_payment_amount`` violates its
        self-documenting-alias contract relative to
        ``payment_amount_concept``.
    """
    globs: list[str] = (
        list(input_globs) if input_globs is not None else _default_input_globs()
    )

    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "empirical_means.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    owns_connection = con is None
    if con is None:
        con = duckdb.connect()

    try:
        # Pre-flight: enumerate input files per glob so we can fail loudly
        # on a fully-empty match, warn on partially-empty matches, and
        # compute the sum-of-inputs row count for the parity check.
        per_glob_files: list[tuple[str, list[str]]] = []
        for g in globs:
            files = [
                row[0]
                for row in con.execute("SELECT file FROM glob(?)", [g]).fetchall()
            ]
            per_glob_files.append((g, files))

        all_files: list[str] = [f for _, files in per_glob_files for f in files]
        if not all_files:
            raise RuntimeError(
                f"No per-source PUF parquets found in any of {globs!r}. "
                "Run the extract modules first, e.g. "
                "`python -m src.extract.puf_inpatient --fy 2022` etc."
            )

        empty_globs = [g for g, files in per_glob_files if not files]
        if empty_globs:
            print(
                f"empirical_means: WARNING — {len(empty_globs)} of {len(globs)} "
                f"input globs matched zero files: {empty_globs!r}. "
                "Continuing with partial coverage; rebuild after running "
                "the missing extractors.",
                file=sys.stderr,
            )

        # Sum of per-file row counts. Invariant we check against the
        # unioned output: the projection must not silently drop rows.
        # Pass the materialized file list directly to read_parquet —
        # DuckDB accepts a list of paths the same way it accepts a glob.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [all_files],
        ).fetchone()[0]

        # Duplicate-key check on the unioned input. GROUP BY treats NULL
        # ``place_of_srvc`` as a single bucket — DuckDB's native
        # NULL-equal-NULL grouping semantics — so inpatient/summary rows
        # (place_of_srvc IS NULL) collide only against themselves on
        # duplicate (code, year, source). ``source`` is in the key so a
        # puf_top100 / puf_all_drgs row and the puf_top100_summary row
        # for the same (DRG, FY) do NOT collide.
        # ORDER BY references only GROUP BY columns (Session 3 fix
        # propagated from pfs_rates / asc_rates).
        dups = con.execute(
            f"""
            SELECT code, code_system, year, place_of_srvc, source, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3, 4, 5
            HAVING c > 1
            ORDER BY c DESC, code_system, year, source, code, place_of_srvc
            LIMIT 5
            """,
            [all_files],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, code_system, year, place_of_srvc, source) "
                f"5-tuples in per-source PUF union (showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet.
        # Output path is inlined into the SQL because DuckDB's parameter
        # binding mis-resolves a second `?` in COPY ... TO ? — the
        # binding ends up routed back into read_parquet(). Single-bind
        # avoids it. See codes_master / asc_rates for the original note.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_SCHEMA)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [all_files],
        )

        # Row-count parity: every input row must land in the output.
        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, "
                f"output has {output_row_count}."
            )

        # code_system cardinality assertion: observed ⊆ {MS-DRG, APC, HCPCS-II}.
        observed_code_systems = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?) "
                "WHERE code_system IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_code_systems = observed_code_systems - _EXPECTED_CODE_SYSTEMS
        if unexpected_code_systems:
            raise RuntimeError(
                f"Unioned empirical_means {out} contains unexpected "
                f"code_system values: {sorted(unexpected_code_systems)}. "
                f"Expected subset of: {sorted(_EXPECTED_CODE_SYSTEMS)}. "
                "Re-run the relevant PUF extractor before continuing."
            )

        # payment_amount non-null invariant: every row carries a numeric value.
        # Plan-v11 Session 8.5 contract: payment_amount NOT NULL; concept enum
        # enforced separately so Phase 7 cost-compute can never silently average
        # medicare_allowed and total_payment_allpayer.
        null_payment_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE payment_amount IS NULL",
            [str(out)],
        ).fetchone()[0]
        if null_payment_count:
            raise RuntimeError(
                f"Unioned empirical_means {out} has {null_payment_count} rows "
                f"with NULL payment_amount; Phase 5 Session 8.5 contract requires "
                "payment_amount IS NOT NULL on every row. Re-run the relevant PUF "
                "extractor."
            )

        # payment_amount_concept enum: every row carries one of the two locked
        # semantic labels. NULL counts as a violation -- the concept must be
        # determinate.
        observed_concepts = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT payment_amount_concept FROM read_parquet(?)",
                [str(out)],
            ).fetchall()
        }
        if None in observed_concepts:
            raise RuntimeError(
                f"Unioned empirical_means {out} has rows with NULL "
                "payment_amount_concept. Phase 5 Session 8.5 contract requires "
                "concept set per row. Re-run the relevant PUF extractor."
            )
        unexpected_concepts = observed_concepts - _EXPECTED_CONCEPTS
        if unexpected_concepts:
            raise RuntimeError(
                f"Unioned empirical_means {out} contains unexpected "
                f"payment_amount_concept values: {sorted(unexpected_concepts)}. "
                f"Expected subset of: {sorted(_EXPECTED_CONCEPTS)}. "
                "Re-run the relevant PUF extractor before continuing."
            )

        # total_payment_amount semantics: when concept=total_payment_allpayer,
        # the column is a self-documenting alias for payment_amount; when
        # concept=medicare_allowed, the column is NULL.
        violations = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE "
            "(payment_amount_concept = 'total_payment_allpayer' AND "
            " (total_payment_amount IS NULL OR total_payment_amount != payment_amount)) "
            "OR (payment_amount_concept = 'medicare_allowed' AND "
            "    total_payment_amount IS NOT NULL)",
            [str(out)],
        ).fetchone()[0]
        if violations:
            raise RuntimeError(
                f"Unioned empirical_means {out} has {violations} rows violating "
                "the total_payment_amount semantic contract: must equal "
                "payment_amount when concept=total_payment_allpayer, NULL when "
                "concept=medicare_allowed."
            )

        # source cardinality assertion: observed ⊆ 5 locked literals.
        observed_sources = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT source FROM read_parquet(?) "
                "WHERE source IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_sources = observed_sources - _EXPECTED_SOURCES
        if unexpected_sources:
            raise RuntimeError(
                f"Unioned empirical_means {out} contains unexpected "
                f"source values: {sorted(unexpected_sources)}. "
                f"Expected subset of: {sorted(_EXPECTED_SOURCES)}. "
                "Re-run the relevant PUF extractor before continuing."
            )
    finally:
        if owns_connection:
            con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.empirical_means",
        description=(
            "Union the four Phase-5 PUF per-source subdirs into "
            "data/silver/empirical_means.parquet."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override output path (default: data/silver/empirical_means.parquet).",
    )
    args = parser.parse_args(argv)
    out = build_empirical_means(output_path=args.output)
    con = duckdb.connect()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(out)]
        ).fetchone()[0]
        print(f"Wrote {n} rows to {out}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
