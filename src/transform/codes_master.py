"""Union per-source code-master parquets into a single ``silver/codes_master.parquet``.

Reads every parquet in ``config.SILVER_PER_SOURCE`` (written by the
``src.extract.icd10cm`` and ``src.extract.icd10pcs`` extractors), casts each row
to the locked Phase-1 schema, asserts uniqueness on
``(code, code_system, fy_or_cy)``, and writes a single unioned parquet to
``config.SILVER / "codes_master.parquet"``.

The schema is forward-compatible with Session 2's additions (ICD-9-CM,
ICD-9-Vol3, GEMs, HCPCS Level II) and with the Phase-3 backfill of
``or_procedure_flag`` / ``mcc_flag`` / ``cc_flag``.

Phase-3 deferred columns (``or_procedure_flag``, ``mcc_flag``, ``cc_flag``) and
the empirical ``n`` column (Phase 5) are emitted as nullable typed columns now
so the schema is stable across phases.

CLI::

    python -m src.transform.codes_master
    python -m src.transform.codes_master --input-glob 'data/silver/per_source/*.parquet'
    python -m src.transform.codes_master --output data/silver/codes_master.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# The locked Phase-1 schema — column name -> DuckDB type. The SELECT projection
# below mirrors this dict in order. Keep in sync with plan-v1.md lines 105-123.
_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "code_system": "VARCHAR",
    "fy_or_cy": "INTEGER",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "description": "VARCHAR",
    "short_description": "VARCHAR",
    "chapter": "VARCHAR",
    "addendum_action": "VARCHAR",
    "or_procedure_flag": "BOOLEAN",
    "mcc_flag": "BOOLEAN",
    "cc_flag": "BOOLEAN",
    "poa_exempt": "BOOLEAN",
    "source": "VARCHAR",
    "n": "INTEGER",
}

# Minimum expected `code_system` values in the unioned output. Surfacing this
# as an explicit assertion guards against the failure mode where a new
# extractor stops writing per-source parquet but the union step silently
# completes with one fewer system. The set is the floor, not a strict equality
# check — future systems (CPT Level I, etc.) may legitimately add to it.
_EXPECTED_CODE_SYSTEMS: frozenset[str] = frozenset({
    "ICD-10-CM",
    "ICD-10-PCS",
    "ICD-9-CM",
    "ICD-9-Vol3",
    "HCPCS-II",
})


def _projection_sql() -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from a per-source
    parquet are read as NULL by ``read_parquet``, so the explicit CAST here
    both enforces the schema and guarantees missing-column tolerance for
    older-FY files (which may lack, e.g., ``poa_exempt``).
    """
    return ",\n        ".join(f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in _SCHEMA.items())


def build_codes_master(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-source parquets into ``silver/codes_master.parquet``.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "*.parquet")``.
    output_path:
        Output parquet path. Defaults to ``config.SILVER / "codes_master.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(code, code_system, fy_or_cy)`` triples are found, or if
        the row count of the unioned output does not equal the sum of input row
        counts.
    """
    glob = input_glob if input_glob is not None else str(config.SILVER_PER_SOURCE / "*.parquet")
    out = output_path if output_path is not None else (config.SILVER / "codes_master.parquet")

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
                "Run the extract modules (src.extract.icd10cm / icd10pcs) first."
            )

        # Sum of per-file row counts. This is the invariant we check against
        # the unioned output: the projection must not silently drop rows.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # Duplicate-triple check on the unioned input. Cheaper than writing
        # then re-reading, and surfaces the offending triples in the error.
        dups = con.execute(
            f"""
            SELECT code, code_system, fy_or_cy, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql()}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, code_system, fy_or_cy, code
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, code_system, fy_or_cy) triples in per-source "
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
                    {_projection_sql()}
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

        # Code-system cardinality assertion: the unioned output must contain
        # at least every system in :data:`_EXPECTED_CODE_SYSTEMS`. Catches the
        # failure mode where an extractor silently stopped writing parquet.
        observed = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT code_system FROM read_parquet(?)",
                [str(out)],
            ).fetchall()
        }
        missing = _EXPECTED_CODE_SYSTEMS - observed
        if missing:
            raise RuntimeError(
                f"Unioned codes_master {out} is missing expected code_system "
                f"values: {sorted(missing)}. Observed: {sorted(observed)}. "
                f"Re-run the relevant extractors before continuing."
            )
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.codes_master",
        description=(
            "Union per-source ICD/HCPCS code-master parquets into "
            "data/silver/codes_master.parquet."
        ),
    )
    parser.add_argument(
        "--input-glob",
        default=None,
        help=(
            "Glob for per-source parquets. "
            "Default: data/silver/per_source/*.parquet"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help="Output parquet path. Default: data/silver/codes_master.parquet",
    )
    args = parser.parse_args(argv)

    out = build_codes_master(input_glob=args.input_glob, output_path=args.output)

    con = duckdb.connect()
    try:
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out)]).fetchone()[0]
    finally:
        con.close()
    print(f"Wrote {n} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
