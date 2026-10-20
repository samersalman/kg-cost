"""Union per-(cy, quarter) ASC parquets into four silver tables.

Reads per-source parquets in ``config.SILVER_PER_SOURCE / "asc"`` (written
by ``src.extract.asc``), casts each row to its locked schema, asserts
compound-key uniqueness, and writes four unioned parquets into
``config.SILVER``:

- ``asc_rates.parquet`` — Unioned ASC payment rows from Addenda AA, BB,
  and FF. AA = covered surgical procedures, BB = covered ancillary
  services, FF = drugs/biologicals payable separately. All three share
  the same row schema (HCPCS/CPT-keyed payment data); the originating
  addendum is captured by the ``addendum`` discriminator column.
  Compound key: ``(code, modifier, cy, quarter, addendum)``.
- ``asc_payment_indicators.parquet`` — Addendum DD1 payment-indicator
  reference table. Compound key: ``(payment_indicator, cy, quarter)``.
- ``asc_status_indicators.parquet`` — Addendum DD2 status-indicator
  reference table. Compound key: ``(status_indicator, cy, quarter)``.
- ``asc_excluded_procedures.parquet`` — Addendum EE list of HCPCS/CPT
  codes excluded from ASC payment. Compound key: ``(code, cy, quarter)``.

Why FOUR tables (heterogeneous content): ASC publishes one workbook per
quarter that ships several addenda with very different shapes. AA/BB/FF
are payment-rate tables that share a row schema (and so are unionable on
an ``addendum`` discriminator). DD1 and DD2 are reference lookups
(payment-indicator and status-indicator dictionaries) — distinct from
each other and from the rate rows, with no shared column set. EE is a
flat exclusion list. Forcing them into a single table would produce
NULL-heavy unions and confuse downstream node-vs-edge construction in
Phase 8. Four clean schemas keep node and edge construction trivial.

Why AA + BB + FF unioned with an ``addendum`` discriminator: the three
addenda all carry the same per-row schema (``code``, ``modifier``,
``payment_indicator``, ``payment_rate``, ``short_descriptor``, …) — they
differ only in *which* HCPCS/CPT lines CMS chooses to publish in each.
A single unioned table with ``addendum ∈ {AA, BB, FF}`` is the natural
shape for downstream cost-observation construction, and it lets a single
``(code, modifier, cy, quarter, addendum)`` 5-tuple uniqueness invariant
catch any extractor double-emit.

The input glob is intentionally rooted at ``silver/per_source/asc/*.parquet``
rather than the bare ``silver/per_source/*.parquet`` used by codes_master.
DuckDB's ``read_parquet`` glob is non-recursive (``*`` doesn't traverse
subdirectories), so the per-source ``asc/`` subdirectory is enough
isolation — ASC rows do not leak into the codes_master union, and
codes_master rows do not leak in here. Mirrors the PFS and OPPS
subdirectory trick — no edits to ``src/transform/codes_master.py`` are
required.

Plan-v5 schema deviation (cadence): plan-v5.md describes ASC as an
annual publication. URL discovery during the extract phase established
that CMS actually publishes ASC quarterly (Addenda AA/BB/DD1/DD2/EE/FF
each ship in quarterly workbooks A/B/C/D, mirroring OPPS/PFS cadence).
Every silver schema in this module therefore carries a ``quarter``
column and every compound key includes ``quarter``. This is a known
deviation from the plan; the locked schemas in this file are the
source of truth.

CLI::

    python -m src.transform.asc_rates                        # builds all four
    python -m src.transform.asc_rates --target rates         # AA+BB+FF only
    python -m src.transform.asc_rates --target dd1           # payment indicators
    python -m src.transform.asc_rates --target dd2           # status indicators
    python -m src.transform.asc_rates --target ee            # excluded procedures
    python -m src.transform.asc_rates --target rates --output /tmp/x.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# Locked Phase-2 ASC rates schema (AA + BB + FF unioned). Compound key:
# (code, modifier, cy, quarter, addendum). The ``addendum`` discriminator
# column lets a single union carry all three rate addenda without any
# NULL-heavy column shenanigans.
_ASC_RATES_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "modifier": "VARCHAR",
    "code_system": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "addendum": "VARCHAR",
    "payment_indicator": "VARCHAR",
    "payment_rate": "DOUBLE",
    "short_descriptor": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
    "source": "VARCHAR",
}

# Locked Phase-2 ASC Addendum DD1 (payment-indicator dictionary) schema.
# Compound key: (payment_indicator, cy, quarter).
_ASC_DD1_SCHEMA: dict[str, str] = {
    "payment_indicator": "VARCHAR",
    "description": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "source": "VARCHAR",
}

# Locked Phase-2 ASC Addendum DD2 (status-indicator dictionary) schema.
# Compound key: (status_indicator, cy, quarter).
_ASC_DD2_SCHEMA: dict[str, str] = {
    "status_indicator": "VARCHAR",
    "description": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "source": "VARCHAR",
}

# Locked Phase-2 ASC Addendum EE (excluded-procedures list) schema.
# Compound key: (code, cy, quarter).
_ASC_EE_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "cy": "INTEGER",
    "quarter": "VARCHAR",
    "short_descriptor": "VARCHAR",
    "source": "VARCHAR",
}

# Cardinality assertions. Quarter set is the same as PFS/OPPS — A/B/C/D —
# and any other value indicates an extractor bug (URL slug or filename
# mis-parse).
_EXPECTED_QUARTERS: frozenset[str] = frozenset({"A", "B", "C", "D"})

# Allowed addendum discriminator values for the unioned rates table.
_EXPECTED_ADDENDA: frozenset[str] = frozenset({"AA", "BB", "FF"})

# Allowed code_system values for the rates table. The extractor classifies
# via the same HCPCS-vs-CPT regex used by PFS/OPPS, so any other value is
# unexpected. We assert observed ⊆ this set (subset, not equality) — not
# every (cy, q) parquet will contain both, mirroring pfs_rates' /
# opps_rates' subset posture.
_EXPECTED_CODE_SYSTEMS: frozenset[str] = frozenset({"CPT", "HCPCS-II"})


def _projection_sql(schema: dict[str, str]) -> str:
    """Return the SELECT projection that casts each column to the locked schema.

    Uses ``union_by_name=true`` semantics: columns missing from any per-(cy, q)
    parquet are read as NULL by ``read_parquet``, so the explicit CAST here
    both enforces the schema and guarantees missing-column tolerance for any
    older quarterly files.
    """
    return ",\n        ".join(
        f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in schema.items()
    )


def _default_rates_globs() -> list[str]:
    """Return the default list of globs feeding ``build_asc_rates``.

    AA / BB / FF are filed under separate per-source basenames
    (``asc_aa_*.parquet``, ``asc_bb_*.parquet``, ``asc_ff_*.parquet``).
    A list-of-globs is the most portable form across DuckDB versions
    (brace expansion is supported but not the documented surface).
    """
    asc_dir = config.SILVER_PER_SOURCE / "asc"
    return [
        str(asc_dir / "asc_aa_*.parquet"),
        str(asc_dir / "asc_bb_*.parquet"),
        str(asc_dir / "asc_ff_*.parquet"),
    ]


def build_asc_rates(
    input_glob: str | list[str] | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) ASC AA + BB + FF parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression (or list of globs) passed to DuckDB's
        ``read_parquet``. Defaults to a 3-element list of globs covering
        ``asc_aa_*.parquet`` + ``asc_bb_*.parquet`` + ``asc_ff_*.parquet``
        under ``config.SILVER_PER_SOURCE / "asc"``. A single string may be
        supplied to override (e.g., for tests).
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "asc_rates.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob(s) match no files.
    RuntimeError
        If duplicate ``(code, modifier, cy, quarter, addendum)`` 5-tuples
        are found, if the row count of the unioned output does not equal
        the sum of input row counts, if no rows are produced, if any
        observed ``quarter`` is outside ``{A, B, C, D}``, if any observed
        ``addendum`` is outside ``{AA, BB, FF}``, or if any observed
        ``code_system`` is outside ``{CPT, HCPCS-II}``.
    """
    if input_glob is None:
        globs: list[str] = _default_rates_globs()
    elif isinstance(input_glob, str):
        globs = [input_glob]
    else:
        globs = list(input_glob)

    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "asc_rates.parquet")
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
                "Run the extract module (src.extract.asc --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        # Sum of per-file row counts. This is the invariant we check
        # against the unioned output: the projection must not silently
        # drop rows. Pass the materialized file list directly to
        # read_parquet — DuckDB accepts a list of paths the same way it
        # accepts a list of globs.
        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [input_files],
        ).fetchone()[0]

        # Duplicate-tuple check on the unioned input. GROUP BY treats NULL
        # modifiers as a single bucket — DuckDB's native NULL-equal-NULL
        # grouping semantics — so a duplicated base row (modifier IS NULL)
        # collides with itself the same way a duplicated populated
        # modifier would. ORDER BY references only GROUP BY columns
        # (Session 3 fix from pfs_rates.py:163). Do NOT include
        # code_system, payment_rate, etc. in ORDER BY since they're not
        # in GROUP BY.
        dups = con.execute(
            f"""
            SELECT code, modifier, cy, quarter, addendum, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_ASC_RATES_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3, 4, 5
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, addendum, code, modifier
            LIMIT 5
            """,
            [input_files],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, modifier, cy, quarter, addendum) tuples "
                f"in per-source union (showing up to 5): {dups}"
            )

        # Materialize the unioned, projected output to parquet.
        # Output path is inlined into the SQL because DuckDB's parameter
        # binding mis-resolves a second `?` in COPY ... TO ? — the
        # binding ends up routed back into read_parquet(). Single-bind
        # avoids it. See codes_master.py landmine and pfs_rates.py:181
        # for the original note.
        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_ASC_RATES_SCHEMA)}
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

        # Cheap non-empty assertion — guards against the failure mode
        # where every per-source parquet exists but each is empty.
        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned asc_rates {out} has zero rows. "
                "Re-run src.extract.asc before continuing."
            )

        # Quarter cardinality assertion: every observed quarter must be
        # in {A, B, C, D}.
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
                f"Unioned asc_rates {out} contains unexpected quarter "
                f"values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.asc before continuing."
            )

        # Addendum cardinality assertion: every observed addendum must
        # be in {AA, BB, FF}. Surfaces the failure mode where the
        # extractor mis-classifies a sheet (e.g., DD1 leaking into the
        # rates union with addendum = "DD1").
        observed_addenda = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT addendum FROM read_parquet(?) "
                "WHERE addendum IS NOT NULL",
                [str(out)],
            ).fetchall()
        }
        unexpected_addenda = observed_addenda - _EXPECTED_ADDENDA
        if unexpected_addenda:
            raise RuntimeError(
                f"Unioned asc_rates {out} contains unexpected addendum "
                f"values: {sorted(unexpected_addenda)}. "
                f"Expected subset of: {sorted(_EXPECTED_ADDENDA)}. "
                "Re-run src.extract.asc before continuing."
            )

        # Code-system cardinality assertion: observed ⊆ {CPT, HCPCS-II}.
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
                f"Unioned asc_rates {out} contains unexpected "
                f"code_system values: {sorted(unexpected_code_systems)}. "
                f"Expected subset of: {sorted(_EXPECTED_CODE_SYSTEMS)}. "
                "Re-run src.extract.asc before continuing."
            )
    finally:
        con.close()

    return out


def build_asc_payment_indicators(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) ASC Addendum DD1 parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "asc" / "asc_dd1_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "asc_payment_indicators.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(payment_indicator, cy, quarter)`` triples are
        found, if the row count of the unioned output does not equal the
        sum of input row counts, if no rows are produced, or if any
        observed ``quarter`` is outside ``{A, B, C, D}``.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "asc" / "asc_dd1_*.parquet")
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "asc_payment_indicators.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.asc --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        # ORDER BY references only GROUP BY columns (Session 3 fix).
        dups = con.execute(
            f"""
            SELECT payment_indicator, cy, quarter, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_ASC_DD1_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, payment_indicator
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (payment_indicator, cy, quarter) triples in "
                f"per-source union (showing up to 5): {dups}"
            )

        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_ASC_DD1_SCHEMA)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [glob],
        )

        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, output has {output_row_count}."
            )

        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned asc_payment_indicators {out} has zero rows. "
                "Re-run src.extract.asc before continuing."
            )

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
                f"Unioned asc_payment_indicators {out} contains unexpected "
                f"quarter values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.asc before continuing."
            )
    finally:
        con.close()

    return out


def build_asc_status_indicators(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) ASC Addendum DD2 parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "asc" / "asc_dd2_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "asc_status_indicators.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(status_indicator, cy, quarter)`` triples are
        found, if the row count of the unioned output does not equal the
        sum of input row counts, if no rows are produced, or if any
        observed ``quarter`` is outside ``{A, B, C, D}``.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "asc" / "asc_dd2_*.parquet")
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "asc_status_indicators.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.asc --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        dups = con.execute(
            f"""
            SELECT status_indicator, cy, quarter, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_ASC_DD2_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, status_indicator
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (status_indicator, cy, quarter) triples in "
                f"per-source union (showing up to 5): {dups}"
            )

        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_ASC_DD2_SCHEMA)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [glob],
        )

        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, output has {output_row_count}."
            )

        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned asc_status_indicators {out} has zero rows. "
                "Re-run src.extract.asc before continuing."
            )

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
                f"Unioned asc_status_indicators {out} contains unexpected "
                f"quarter values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.asc before continuing."
            )
    finally:
        con.close()

    return out


def build_asc_excluded_procedures(
    input_glob: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Union per-(cy, quarter) ASC Addendum EE parquets into silver.

    Parameters
    ----------
    input_glob:
        Glob expression passed to DuckDB's ``read_parquet``. Defaults to
        ``str(config.SILVER_PER_SOURCE / "asc" / "asc_ee_*.parquet")``.
    output_path:
        Output parquet path. Defaults to
        ``config.SILVER / "asc_excluded_procedures.parquet"``.

    Returns
    -------
    Path
        The output path that was written.

    Raises
    ------
    FileNotFoundError
        If the input glob matches no files.
    RuntimeError
        If duplicate ``(code, cy, quarter)`` triples are found, if the
        row count of the unioned output does not equal the sum of input
        row counts, if no rows are produced, or if any observed
        ``quarter`` is outside ``{A, B, C, D}``.
    """
    glob = (
        input_glob
        if input_glob is not None
        else str(config.SILVER_PER_SOURCE / "asc" / "asc_ee_*.parquet")
    )
    out = (
        output_path
        if output_path is not None
        else (config.SILVER / "asc_excluded_procedures.parquet")
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        input_files = [
            row[0]
            for row in con.execute("SELECT file FROM glob(?)", [glob]).fetchall()
        ]
        if not input_files:
            raise FileNotFoundError(
                f"No parquet files matched input glob: {glob!r}. "
                "Run the extract module (src.extract.asc --cy-range 2008-2026 "
                "--quarter-range A-D) first."
            )

        input_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [glob],
        ).fetchone()[0]

        dups = con.execute(
            f"""
            SELECT code, cy, quarter, COUNT(*) AS c
            FROM (SELECT
                    {_projection_sql(_ASC_EE_SCHEMA)}
                  FROM read_parquet(?, union_by_name=true))
            GROUP BY 1, 2, 3
            HAVING c > 1
            ORDER BY c DESC, cy, quarter, code
            LIMIT 5
            """,
            [glob],
        ).fetchall()
        if dups:
            raise RuntimeError(
                "Duplicate (code, cy, quarter) triples in per-source union "
                f"(showing up to 5): {dups}"
            )

        out_sql = str(out).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    {_projection_sql(_ASC_EE_SCHEMA)}
                FROM read_parquet(?, union_by_name=true)
            ) TO '{out_sql}' (FORMAT PARQUET)
            """,
            [glob],
        )

        output_row_count: int = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(out)],
        ).fetchone()[0]
        if output_row_count != input_row_count:
            raise RuntimeError(
                f"Row-count mismatch building {out}: "
                f"inputs total {input_row_count} rows, output has {output_row_count}."
            )

        if output_row_count == 0:
            raise RuntimeError(
                f"Unioned asc_excluded_procedures {out} has zero rows. "
                "Re-run src.extract.asc before continuing."
            )

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
                f"Unioned asc_excluded_procedures {out} contains unexpected "
                f"quarter values: {sorted(unexpected_quarters)}. "
                f"Expected subset of: {sorted(_EXPECTED_QUARTERS)}. "
                "Re-run src.extract.asc before continuing."
            )
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.asc_rates",
        description=(
            "Union per-(cy, quarter) ASC parquets into "
            "data/silver/asc_rates.parquet, "
            "data/silver/asc_payment_indicators.parquet, "
            "data/silver/asc_status_indicators.parquet, and "
            "data/silver/asc_excluded_procedures.parquet."
        ),
    )
    parser.add_argument(
        "--target",
        choices=("rates", "dd1", "dd2", "ee", "all"),
        default="all",
        help="Which silver table(s) to build. Default: all.",
    )
    parser.add_argument(
        "--input-glob",
        default=None,
        help=(
            "Glob for per-source parquets. Applied only to the chosen target "
            "(invalid with --target all). "
            "Defaults: AA+BB+FF list for rates, "
            "data/silver/per_source/asc/asc_dd1_*.parquet for dd1, "
            "data/silver/per_source/asc/asc_dd2_*.parquet for dd2, "
            "data/silver/per_source/asc/asc_ee_*.parquet for ee."
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help=(
            "Output parquet path. Applied only to the chosen target "
            "(invalid with --target all). "
            "Defaults: data/silver/asc_rates.parquet for rates, "
            "data/silver/asc_payment_indicators.parquet for dd1, "
            "data/silver/asc_status_indicators.parquet for dd2, "
            "data/silver/asc_excluded_procedures.parquet for ee."
        ),
    )
    args = parser.parse_args(argv)

    if args.target == "all" and (
        args.input_glob is not None or args.output is not None
    ):
        parser.error(
            "--input-glob and --output cannot be used with --target all. "
            "Pick a single --target rates|dd1|dd2|ee."
        )

    outputs: list[Path] = []
    if args.target in ("rates", "all"):
        outputs.append(
            build_asc_rates(
                input_glob=args.input_glob if args.target == "rates" else None,
                output_path=args.output if args.target == "rates" else None,
            )
        )
    if args.target in ("dd1", "all"):
        outputs.append(
            build_asc_payment_indicators(
                input_glob=args.input_glob if args.target == "dd1" else None,
                output_path=args.output if args.target == "dd1" else None,
            )
        )
    if args.target in ("dd2", "all"):
        outputs.append(
            build_asc_status_indicators(
                input_glob=args.input_glob if args.target == "dd2" else None,
                output_path=args.output if args.target == "dd2" else None,
            )
        )
    if args.target in ("ee", "all"):
        outputs.append(
            build_asc_excluded_procedures(
                input_glob=args.input_glob if args.target == "ee" else None,
                output_path=args.output if args.target == "ee" else None,
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
