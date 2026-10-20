"""Union per-(cy, quarter) OPPS Addendum A and B parquets into silver.

Reads every parquet in ``config.SILVER_PER_SOURCE / "opps"`` (written by
``src.extract.opps``), casts each row to its locked Phase-2 schema, asserts
compound-key uniqueness, and writes two unioned parquets into
``config.SILVER``:

- ``opps_apc_rates.parquet`` — Addendum A (APC-keyed payment rates).
  Compound key: ``(apc, cy, quarter)``. Sourced from per-source parquets
  matching ``opps_a_*.parquet``.
- ``opps_hcpcs_rates.parquet`` — Addendum B (HCPCS/CPT-keyed payment rows
  with APC foreign key). Compound key: ``(code, modifier, cy, quarter)``.
  Sourced from per-source parquets matching ``opps_b_*.parquet``.

Why the split (plan-v5.md lines 65-76): Addendum A is keyed on APC and
Addendum B is keyed on HCPCS with APC as a foreign key. Forcing them into a
single table produces NULL-heavy unions and confuses downstream node-vs-edge
construction in Phase 8 (``:APC`` node + ``(:HCPCS)-[:MAPPED_TO_APC]->(:APC)``
edge). Two clean schemas keep node and edge construction trivial.

The input glob is intentionally rooted at ``silver/per_source/opps/*.parquet``
rather than the bare ``silver/per_source/*.parquet`` used by codes_master.
DuckDB's ``read_parquet`` glob is non-recursive (``*`` doesn't traverse
subdirectories), so the per-source ``opps/`` subdirectory is enough isolation
— OPPS rows do not leak into the codes_master union, and codes_master rows do
not leak in here. Mirrors the PFS subdirectory trick — no edits to
``src/transform/codes_master.py`` are required.

Schema notes (locked by plan-v5.md lines 102-132):
- Addendum A schema is APC-keyed; it has no ``code`` / ``modifier`` /
  ``code_system`` columns. APC is a 4-digit string keyed independently of
  HCPCS/CPT.
- Addendum B's ``modifier`` is nullable (modifiers are rare in OPPS but
  permitted by the CMS schema). The duplicate-tuple guard's ``GROUP BY``
  treats NULL-equal-NULL natively (DuckDB groups NULLs together), so a
  duplicated base row (``modifier IS NULL``) collides with itself the same
  way as a duplicated populated modifier.
- ``code_system`` for Addendum B is one of ``CPT`` or ``HCPCS-II`` —
  classified by the extractor with the same regex used by PFS.

CLI::

    python -m src.transform.opps_rates                         # builds both
    python -m src.transform.opps_rates --target apc            # APC only
    python -m src.transform.opps_rates --target hcpcs          # HCPCS only
    python -m src.transform.opps_rates --target apc --output /tmp/x.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# Locked Phase-2 OPPS Addendum A schema (plan-v5.md lines 102-116).
# Column name -> DuckDB type. The SELECT projection mirrors this dict in order.
_APC_SCHEMA: dict[str, str] = {
    "apc": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "status_indicator": "VARCHAR",
    "apc_title": "VARCHAR",
    "relative_weight": "DOUBLE",
    "payment_rate": "DOUBLE",
    "national_unadjusted_copayment": "DOUBLE",
    "minimum_unadjusted_copayment": "DOUBLE",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
}

# Locked Phase-2 OPPS Addendum B schema (plan-v5.md lines 118-132).
_HCPCS_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "modifier": "VARCHAR",
    "code_system": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "status_indicator": "VARCHAR",
    "apc": "VARCHAR",
    "relative_weight": "DOUBLE",
    "payment_rate": "DOUBLE",
    "short_descriptor": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
}

# Cardinality assertions. Quarter set is the same as PFS — A/B/C/D — and any
# other value indicates an extractor bug (URL slug or filename mis-parse).
_EXPECTED_QUARTERS: frozenset[str] = frozenset({"A", "B", "C", "D"})

# Allowed code_system values for Addendum B. The extractor classifies via
# the same HCPCS-vs-CPT regex used by PFS, so any other value is unexpected.
# We assert observed ⊆ this set (subset, not equality) — not every (cy, q)
# parquet will contain both, mirroring pfs_rates' subset posture.
_EXPECTED_CODE_SYSTEMS: frozenset[str] = frozenset({"CPT", "HCPCS-II"})


def _projection_sql(schema: dict[str, str]) -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from any per-(cy, q)
    parquet are read as NULL by ``read_parquet``, so the explicit CAST here
    both enforces the schema and guarantees missing-column tolerance for any
    older quarterly files (which may legitimately lack, e.g.,
    ``status_indicator`` if the upstream CSV header drifts).
    """
    return ",\n        ".join(
        f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in schema.items()
    )


def build_opps_apc_rates(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) OPPS Addendum A parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "opps" / "opps_a_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "opps_apc_rates.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(apc, cy, quarter)`` triples are found, if the row
        count of the unioned output does not equal the sum of input row
        counts, if no rows are produced, or if any observed ``quarter`` is
        outside ``{A, B, C, D}``.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "opps" / "opps_a_*.parquet")
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "opps_apc_rates.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        # Pre-flight: enumerate input files so we can both fail loudly on an
        # empty glob and compute the sum-of-inputs row count for parity check.
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.opps --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        # Sum of per-file row counts. This is the invariant we check against
        # the unioned output: the projection must not silently drop rows.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # Duplicate-triple check on the unioned input. Cheaper than writing
        # then re-reading. ORDER BY references only GROUP BY columns —
        # see Session 3 fix in pfs_rates.py:163. Adding a non-grouped column
        # to ORDER BY would fail DuckDB's binder.
        dups = con.execute(
            f"""
            SELECT apc, cy, quarter, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_APC_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, apc
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (apc, cy, quarter) triples in per-source union "
                f"(showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet.
        # Output path is inlined into the SQL because DuckDB's parameter
        # binding mis-resolves a second `?` in COPY ... TO ? — the binding
        # ends up routed back into read_parquet(). Single-bind avoids it.
        # See codes_master.py landmine and pfs_rates.py:181 for the
        # original note.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_APC_SCHEMA)}
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

        # Cheap non-empty assertion — guards against the failure mode where
        # every per-source parquet exists but each is empty.
        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned opps_apc_rates {out} has zero rows. "
                "Re-run src.extract.opps before continuing."
            )

        # Quarter cardinality assertion: every observed quarter must be in
        # {A, B, C, D}. Surfaces the failure mode where the extractor's
        # CSV-filename parser silently emits an unexpected token (e.g., the
        # bronze ZIP filename drift CMS pulls every couple of years).
        observed_quarters = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT quarter FROM read_parquet(?) "
                "WHERE quarter IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_quarters = observed_quarters - _EXPECTED_QUARTERS
        if unexpected_quarters:
            raise RuntimeError(
                f"Unioned opps_apc_rates {out} contains unexpected quarter "
                f"values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.opps before continuing."
            )
    finally:
        con.close()

    return out


def build_opps_hcpcs_rates(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) OPPS Addendum B parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "opps" / "opps_b_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "opps_hcpcs_rates.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(code, modifier, cy, quarter)`` 4-tuples are found,
        if the row count of the unioned output does not equal the sum of
        input row counts, if no rows are produced, if any observed
        ``quarter`` is outside ``{A, B, C, D}``, or if any observed
        ``code_system`` is outside ``{CPT, HCPCS-II}``.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "opps" / "opps_b_*.parquet")
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "opps_hcpcs_rates.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        # Pre-flight: enumerate input files so we can both fail loudly on an
        # empty glob and compute the sum-of-inputs row count for parity check.
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.opps --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        # Sum of per-file row counts. This is the invariant we check against
        # the unioned output: the projection must not silently drop rows.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # Duplicate-tuple check on the unioned input. GROUP BY treats NULL
        # modifiers as a single bucket — DuckDB's native NULL-equal-NULL
        # grouping semantics — so a duplicated base row (modifier IS NULL)
        # collides with itself the same way a duplicated populated modifier
        # would. ORDER BY references only GROUP BY columns (Session 3 fix).
        # Do NOT include code_system in ORDER BY since it's not in GROUP BY.
        dups = con.execute(
            f"""
            SELECT code, modifier, cy, quarter, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_HCPCS_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3, 4
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, code, modifier
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, modifier, cy, quarter) tuples in per-source "
                f"union (showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet.
        # Output path is inlined into the SQL because DuckDB's parameter
        # binding mis-resolves a second `?` in COPY ... TO ? — the binding
        # ends up routed back into read_parquet(). Single-bind avoids it.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_HCPCS_SCHEMA)}
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

        # Cheap non-empty assertion — guards against the failure mode where
        # every per-source parquet exists but each is empty.
        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned opps_hcpcs_rates {out} has zero rows. "
                "Re-run src.extract.opps before continuing."
            )

        # Quarter cardinality assertion: every observed quarter must be in
        # {A, B, C, D}.
        observed_quarters = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT quarter FROM read_parquet(?) "
                "WHERE quarter IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_quarters = observed_quarters - _EXPECTED_QUARTERS
        if unexpected_quarters:
            raise RuntimeError(
                f"Unioned opps_hcpcs_rates {out} contains unexpected quarter "
                f"values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.opps before continuing."
            )

        # Code-system cardinality assertion: observed ⊆ {CPT, HCPCS-II}.
        # Mirrors the subset posture of pfs_rates' code-system check —
        # not every per-(cy, q) parquet must contain both, but no other
        # values are permitted.
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
                f"Unioned opps_hcpcs_rates {out} contains unexpected "
                f"code_system values: {sorted(unexpected_code_systems)}. "
                f"Expected subset of: {sorted(_EXPECTED_CODE_SYSTEMS)}. "
                "Re-run src.extract.opps before continuing."
            )
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.opps_rates",
        description=(
            "Union per-(cy, quarter) OPPS Addendum A and/or B parquets "
            "into data/silver/opps_apc_rates.parquet and "
            "data/silver/opps_hcpcs_rates.parquet."
        ),
    )
    parser.add_argument(
        "--target",
        choices=("apc", "hcpcs", "both"),
        default="both",
        help="Which silver table(s) to build. Default: both.",
    )
    parser.add_argument(
        "--input-glob",
        default=None,
        help=(
            "Glob for per-source parquets. Applied only to the chosen target "
            "(invalid with --target both). "
            "Defaults: data/silver/per_source/opps/opps_a_*.parquet for apc, "
            "data/silver/per_source/opps/opps_b_*.parquet for hcpcs."
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help=(
            "Output parquet path. Applied only to the chosen target "
            "(invalid with --target both). "
            "Defaults: data/silver/opps_apc_rates.parquet for apc, "
            "data/silver/opps_hcpcs_rates.parquet for hcpcs."
        ),
    )
    args = parser.parse_args(argv)

    if args.target == "both" and (args.input_glob is not None or args.output is not None):
        parser.error(
            "--input-glob and --output cannot be used with --target both. "
            "Pick a single --target apc or --target hcpcs."
        )

    outputs: list[Path] = []
    if args.target in ("apc", "both"):
        outputs.append(
            build_opps_apc_rates(
                input_glob=args.input_glob if args.target == "apc" else None,
                output_path=args.output if args.target == "apc" else None,
            )
        )
    if args.target in ("hcpcs", "both"):
        outputs.append(
            build_opps_hcpcs_rates(
                input_glob=args.input_glob if args.target == "hcpcs" else None,
                output_path=args.output if args.target == "hcpcs" else None,
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
