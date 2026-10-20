"""Union per-(cy, quarter) PFS RVU parquets into ``silver/pfs_rates.parquet``.

Reads every parquet in ``config.SILVER_PER_SOURCE / "pfs_rvu"`` (written by
``src.extract.pfs_rvu``), casts each row to the locked Phase-2 schema, asserts
uniqueness on ``(code, modifier, cy, quarter, setting, conversion_factor_kind)``
— the compound 6-key — and writes a single unioned parquet to
``config.SILVER / "pfs_rates.parquet"``.

The input glob is intentionally rooted at ``silver/per_source/pfs_rvu/*.parquet``
rather than the bare ``silver/per_source/*.parquet`` used by codes_master.
DuckDB's ``read_parquet`` glob is non-recursive (``*`` doesn't traverse
subdirectories), so the per-source subdirectory is enough isolation — PFS rows
do not leak into the codes_master union, and codes_master rows do not leak in
here. No edits to ``src/transform/codes_master.py`` are required.

Schema notes (locked by plan-v3.md lines 92-120):
- ``modifier`` is nullable; the duplicate-triple guard's ``GROUP BY`` treats
  NULL-equal-NULL natively (DuckDB groups NULLs together), so the base row
  (``modifier IS NULL``) collides with itself if duplicated and is checked
  the same as a populated modifier like ``26`` or ``TC``.
- ``conversion_factor_kind`` is part of the compound key because CY2026 emits
  TWO rows per (code, modifier, cy, quarter, setting) — one ``qp`` and one
  ``nonqp`` — per the CY2026 PFS Final Rule split. CY2008-CY2025 rows use
  ``conversion_factor_kind = "single"``.
- ``setting`` must contain BOTH ``pfs_facility`` and ``pfs_nonfacility`` in
  the unioned output; the long-form emission produces one row per setting.

CLI::

    python -m src.transform.pfs_rates
    python -m src.transform.pfs_rates --input-glob 'data/silver/per_source/pfs_rvu/*.parquet'
    python -m src.transform.pfs_rates --output data/silver/pfs_rates.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# The locked Phase-2 schema — column name -> DuckDB type. The SELECT projection
# below mirrors this dict in order. Keep in sync with plan-v3.md lines 92-120.
_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "modifier": "VARCHAR",
    "code_system": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "setting": "VARCHAR",
    "status_indicator": "VARCHAR",
    "work_rvu": "DOUBLE",
    "pe_rvu": "DOUBLE",
    "mp_rvu": "DOUBLE",
    "total_rvu": "DOUBLE",
    "conversion_factor": "DOUBLE",
    "conversion_factor_kind": "VARCHAR",
    "allowed_amount": "DOUBLE",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
    "n": "INTEGER",
}

# Minimum expected `setting` values in the unioned output. PFS is the
# locked-schema reference for both facility and non-facility settings; the
# long-form extractor emits one row per setting per code, so both must be
# present in any non-trivial union. Surfacing this as an explicit assertion
# guards against the failure mode where the extractor silently stops emitting
# one of the two settings (e.g., a column-rename in the CSV breaks the
# non-facility branch) but the union step completes anyway.
_EXPECTED_SETTINGS: frozenset[str] = frozenset({"pfs_facility", "pfs_nonfacility"})


def _projection_sql() -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from a per-(cy, q)
    parquet are read as NULL by ``read_parquet``, so the explicit CAST here
    both enforces the schema and guarantees missing-column tolerance for any
    older quarterly files (which may legitimately lack, e.g., ``status_indicator``
    if the upstream CSV header drifts).
    """
    return ",\n        ".join(f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in _SCHEMA.items())


def build_pfs_rates(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) PFS RVU parquets into ``silver/pfs_rates.parquet``.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "pfs_rvu" / "*.parquet")``.
    output_path:
        Output parquet path. Defaults to ``config.SILVER / "pfs_rates.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(code, modifier, cy, quarter, setting,
        conversion_factor_kind)`` 6-tuples are found, if the row count of the
        unioned output does not equal the sum of input row counts, or if the
        output is missing one of the expected ``setting`` values.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "pfs_rvu" / "*.parquet")
    )
    out = output_path if output_path is not None else (config.SILVER / "pfs_rates.parquet")

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
                "Run the extract module (src.extract.pfs_rvu --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        # Sum of per-file row counts. This is the invariant we check against
        # the unioned output: the projection must not silently drop rows.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # Duplicate-tuple check on the unioned input. Cheaper than writing
        # then re-reading, and surfaces the offending tuples in the error.
        # GROUP BY treats NULL modifiers as a single bucket — DuckDB's
        # native NULL-equal-NULL grouping semantics — so a duplicated base row
        # (modifier IS NULL) collides with itself the same way a duplicated
        # `26` modifier row would.
        dups = con.execute(
            f"""
            SELECT code, modifier, cy, quarter, setting, conversion_factor_kind, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql()}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3, 4, 5, 6
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, code, modifier
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, modifier, cy, quarter, setting, "
                f"conversion_factor_kind) tuples in per-source union "
                f"(showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet.
        # Output path is inlined into the SQL because DuckDB's parameter
        # binding mis-resolves a second `?` in COPY ... TO ? — the binding
        # ends up routed back into read_parquet(). Single-bind avoids it.
        # See codes_master.py:154-167 for the original landmine note.
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

        # Setting cardinality assertion: the unioned output must contain at
        # least every value in :data:`_EXPECTED_SETTINGS`. Catches the failure
        # mode where the extractor's facility OR non-facility branch silently
        # broke (column rename, header drift) but the per-(cy, q) parquets
        # still wrote with one setting only.
        observed_settings = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT setting FROM read_parquet(?)",
                [str(out)],
            ).fetchall()
        }
        missing = _EXPECTED_SETTINGS - observed_settings
        if missing:
            raise RuntimeError(
                f"Unioned pfs_rates {out} is missing expected setting "
                f"values: {sorted(missing)}. Observed: {sorted(observed_settings)}. "
                f"Re-run src.extract.pfs_rvu before continuing."
            )

        # Conversion-factor-kind cardinality assertion: at least one value
        # must be present. Cheap sanity check that the projection is
        # populating the column (catches a wholly-NULL conversion_factor_kind
        # column that would otherwise pass the duplicate-tuple guard, since
        # NULL-equal-NULL collapses duplicates rather than surfacing them).
        observed_cf_kinds = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT conversion_factor_kind FROM read_parquet(?) "
                "WHERE conversion_factor_kind IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        if not observed_cf_kinds:
            raise RuntimeError(
                f"Unioned pfs_rates {out} has no non-NULL conversion_factor_kind "
                "values. Expected at least one of {'single', 'qp', 'nonqp'}. "
                "Re-run src.extract.pfs_rvu before continuing."
            )
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.pfs_rates",
        description=(
            "Union per-(cy, quarter) PFS RVU parquets into "
            "data/silver/pfs_rates.parquet."
        ),
    )
    parser.add_argument(
        "--input-glob",
        default=None,
        help=(
            "Glob for per-source parquets. "
            "Default: data/silver/per_source/pfs_rvu/*.parquet"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help="Output parquet path. Default: data/silver/pfs_rates.parquet",
    )
    args = parser.parse_args(argv)

    out = build_pfs_rates(input_glob=args.input_glob, output_path=args.output)

    con = duckdb.connect()
    try:
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out)]).fetchone()[0]
    finally:
        con.close()
    print(f"Wrote {n} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
