"""Build the first GOLD table: ``data/gold/code_costs.parquet``.

Joins seven silver tables (empirical_means, ms_drg_weights, ipps_std_amounts,
pfs_rates, opps_hcpcs_rates, asc_rates, national_ccr) into a unified per-code,
per-year, per-setting cost view holding three INDEPENDENT cost concepts:
``charge``, ``allowed_payment``, and ``ccr_cost``. Per the Phase 5 resolved-Q
(DEV 4 contract), the three concepts must NOT cross-substitute -- each is NULL
where its own primary source is missing.

Phase 6 (CCR overlay) SHIPPED in Session 9.5 (2026-05-02): inpatient rows for
FY2015+ now carry ``ccr_value`` (national-mean CCR for the inpatient default
cost-center "Operating Room") and ``ccr_cost = charge * ccr_value``. FY2008-FY2014
inpatient remains ``ccr_cost IS NULL`` because NBER long-form HCRIS for those FYs
is in the 4 GB range (deferred to a follow-up session); see
``plans/hcris-url-discovery.md`` Session 9.5 NBER pre-check.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src import config

# Locked Phase 7a schema (plan-v12.md lines 152-176). Ordering matters: the
# UNION ALL CTEs below project columns in this exact order, and tests pin both
# names and types.
_SCHEMA: dict[str, str] = {
    "code": "VARCHAR",
    "code_system": "VARCHAR",
    "fy_or_cy": "INTEGER",
    "fy": "INTEGER",
    "cy": "INTEGER",
    "setting": "VARCHAR",
    "charge": "DOUBLE",
    "charge_n": "BIGINT",
    "charge_source": "VARCHAR",
    "charge_p10": "DOUBLE",
    "charge_p25": "DOUBLE",
    "charge_p50": "DOUBLE",
    "charge_p75": "DOUBLE",
    "charge_p90": "DOUBLE",
    "allowed_payment": "DOUBLE",
    "allowed_payment_n": "BIGINT",
    "allowed_payment_source": "VARCHAR",
    "allowed_payment_concept": "VARCHAR",
    "payment_p10": "DOUBLE",
    "payment_p25": "DOUBLE",
    "payment_p50": "DOUBLE",
    "payment_p75": "DOUBLE",
    "payment_p90": "DOUBLE",
    "ccr_cost": "DOUBLE",
    "ccr_value": "DOUBLE",
    "ccr_cost_center": "VARCHAR",
    "effective_start": "DATE",
    "effective_end": "DATE",
}

_SETTING_VALUES: frozenset[str] = frozenset({
    "inpatient",
    "opps",
    "asc",
    "pfs_facility",
    "pfs_nonfacility",
})

_SOURCE_TAG_VALUES: frozenset[str] = frozenset({
    "puf_empirical",
    "formula_ipps",
    "formula_pfs",
    "formula_opps",
    "formula_asc",
    "formula_no_empirical",
    "gems_bridged_modal",
    "gems_bridged_back_translated",
})

# Phase 6 default cost-center for inpatient ccr_cost. Locked at "Operating Room"
# per plan-v12.md PI-checkpoint B item; future work may switch to per-MS-DRG
# cost-center routing once the MS-DRG -> cost-center mapping table ships.
_INPATIENT_DEFAULT_CCR_COST_CENTER: str = "Operating Room"


def _projection_sql() -> str:
    """SELECT projection casting each column to the locked schema, in order."""
    return ",\n        ".join(
        f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in _SCHEMA.items()
    )


def _build_union_sql(silver_paths: dict[str, str]) -> str:
    """Return the full UNION ALL SQL across the five settings.

    ``silver_paths`` maps logical name -> single-quoted parquet path string
    (already escaped). The CTEs mirror the locked column order in _SCHEMA.
    """
    em = silver_paths["empirical_means"]
    drg = silver_paths["ms_drg_weights"]
    ipps = silver_paths["ipps_std_amounts"]
    pfs = silver_paths["pfs_rates"]
    opps = silver_paths["opps_hcpcs_rates"]
    asc = silver_paths["asc_rates"]
    ccr = silver_paths.get("national_ccr")  # Phase 6 (Session 9.5); optional
    ccr_join = (
        f"""
        LEFT JOIN (
            SELECT fy, cost_center_group, ccr_value
            FROM read_parquet('{ccr}')
            WHERE cost_center_group = '{_INPATIENT_DEFAULT_CCR_COST_CENTER}'
        ) c
            ON c.fy = d.fy
        """
        if ccr
        else ""
    )
    ccr_value_expr = "c.ccr_value" if ccr else "CAST(NULL AS DOUBLE)"
    ccr_cost_expr = (
        "CASE WHEN e.mean_charge IS NOT NULL AND c.ccr_value IS NOT NULL "
        "THEN e.mean_charge * c.ccr_value END"
        if ccr
        else "CAST(NULL AS DOUBLE)"
    )
    ccr_cost_center_expr = (
        f"CASE WHEN c.ccr_value IS NOT NULL THEN '{_INPATIENT_DEFAULT_CCR_COST_CENTER}' END"
        if ccr
        else "CAST(NULL AS VARCHAR)"
    )

    return f"""
    WITH
    -- Pre-aggregate empirical_means MS-DRG: one row per (code, year). Multiple
    -- sources can co-exist for FY2011/FY2012 (top100 + top100_summary) and
    -- FY2013 (all_drgs_ry23 + top100_summary). Prefer the broadest source.
    em_msdrg_ranked AS (
        SELECT
            code, year, payment_amount, payment_amount_concept,
            mean_charge, n,
            p10_charge, p25_charge, p50_charge, p75_charge, p90_charge,
            p10_payment, p25_payment, p50_payment, p75_payment, p90_payment,
            CASE source
                WHEN 'puf_all_drgs_ry23' THEN 1
                WHEN 'puf_top100' THEN 2
                WHEN 'puf_top100_summary' THEN 3
                ELSE 9
            END AS prio
        FROM read_parquet('{em}')
        WHERE code_system = 'MS-DRG'
    ),
    em_msdrg AS (
        SELECT *
        FROM em_msdrg_ranked
        QUALIFY ROW_NUMBER() OVER (PARTITION BY code, year ORDER BY prio) = 1
    ),
    em_apc AS (
        SELECT
            code, year, payment_amount, payment_amount_concept,
            mean_charge, n,
            p10_charge, p25_charge, p50_charge, p75_charge, p90_charge,
            p10_payment, p25_payment, p50_payment, p75_payment, p90_payment
        FROM read_parquet('{em}')
        WHERE code_system = 'APC'
    ),
    em_hcpcs AS (
        SELECT
            code, year, place_of_srvc,
            payment_amount, payment_amount_concept,
            mean_charge, n,
            p10_charge, p25_charge, p50_charge, p75_charge, p90_charge,
            p10_payment, p25_payment, p50_payment, p75_payment, p90_payment
        FROM read_parquet('{em}')
        WHERE code_system = 'HCPCS-II'
    ),
    -- IPPS national std-amount components, pivoted: one row per fy with
    -- operating_std + capital_std as scalars. Per landmine #3, default
    -- FY2010 to '2nd_half'; for all other FYs rate_period IS NULL.
    ipps_pivot AS (
        SELECT
            fy,
            MAX(CASE WHEN line_item = 'operating_full_update' THEN value END) AS operating_std,
            MAX(CASE WHEN line_item = 'capital_federal_rate' THEN value END) AS capital_std
        FROM read_parquet('{ipps}')
        WHERE category = 'national'
          AND line_item IN ('operating_full_update', 'capital_federal_rate')
          AND ((fy = 2010 AND rate_period = '2nd_half')
               OR (fy <> 2010 AND rate_period IS NULL))
        GROUP BY fy
    ),

    ----------------------------------------------------------------
    -- 1) inpatient: ms_drg_weights LEFT JOIN empirical + ipps formula
    ----------------------------------------------------------------
    inpatient_cte AS (
        SELECT
            d.ms_drg AS code,
            CAST('MS-DRG' AS VARCHAR) AS code_system,
            d.fy AS fy_or_cy,
            d.fy AS fy,
            CAST(NULL AS INTEGER) AS cy,
            CAST('inpatient' AS VARCHAR) AS setting,

            -- charge: empirical mean_charge if present, else NULL (Phase 5
            -- three-concept-independence rule: no formula fallback for charge)
            e.mean_charge AS charge,
            e.n AS charge_n,
            CASE WHEN e.mean_charge IS NOT NULL THEN 'puf_empirical' END AS charge_source,
            e.p10_charge AS charge_p10,
            e.p25_charge AS charge_p25,
            e.p50_charge AS charge_p50,
            e.p75_charge AS charge_p75,
            e.p90_charge AS charge_p90,

            -- allowed_payment: prefer empirical; else formula
            -- RW * (operating + capital). Reserved DRGs 998/999 are forced
            -- to 0 even when relative_weight is 42.0 (FY2008 quirk).
            CASE
                WHEN e.payment_amount IS NOT NULL THEN e.payment_amount
                WHEN d.ms_drg IN ('998','999') THEN 0.0
                WHEN d.relative_weight IS NOT NULL
                     AND p.operating_std IS NOT NULL
                     AND p.capital_std IS NOT NULL
                    THEN d.relative_weight * (p.operating_std + p.capital_std)
            END AS allowed_payment,
            e.n AS allowed_payment_n,
            CASE
                WHEN e.payment_amount IS NOT NULL THEN 'puf_empirical'
                WHEN d.ms_drg IN ('998','999') THEN 'formula_ipps'
                WHEN d.relative_weight IS NOT NULL
                     AND p.operating_std IS NOT NULL
                     AND p.capital_std IS NOT NULL
                    THEN
                        -- FY2011/FY2012 long-tail: empirical only covers
                        -- puf_top100 (100 codes/yr) so the remaining ~647
                        -- DRGs/yr surface here as formula_no_empirical per
                        -- Phase 5 resolved-Q.
                        CASE
                            WHEN d.fy IN (2011, 2012) THEN 'formula_no_empirical'
                            ELSE 'formula_ipps'
                        END
            END AS allowed_payment_source,
            e.payment_amount_concept AS allowed_payment_concept,
            e.p10_payment AS payment_p10,
            e.p25_payment AS payment_p25,
            e.p50_payment AS payment_p50,
            e.p75_payment AS payment_p75,
            e.p90_payment AS payment_p90,

            {ccr_cost_expr} AS ccr_cost,
            {ccr_value_expr} AS ccr_value,
            {ccr_cost_center_expr} AS ccr_cost_center,
            d.effective_start, d.effective_end
        FROM read_parquet('{drg}') d
        LEFT JOIN em_msdrg e
            ON e.code = d.ms_drg AND e.year = d.fy
        LEFT JOIN ipps_pivot p
            ON p.fy = d.fy
        {ccr_join}
    ),

    ----------------------------------------------------------------
    -- 2) pfs_facility / pfs_nonfacility: pfs_rates LEFT JOIN empirical_HCPCS
    --    place_of_srvc='F' for facility, 'O' for nonfacility.
    --    For CY2026, use conversion_factor_kind='nonqp'; else 'single'.
    ----------------------------------------------------------------
    -- Modifier collapse: schema-locked compound key has no modifier dim.
    -- Phase 5 schema (landmine #6) ignores modifier on the empirical side;
    -- on the formula side we pick the GLOBAL row (modifier IS NULL) when
    -- present (sums of -26 + TC), else fall back to the first modifier row.
    pfs_base AS (
        SELECT code, code_system, cy, setting, allowed_amount,
               effective_start, effective_end
        FROM (
            SELECT code, modifier, code_system, cy, setting, allowed_amount,
                   effective_start, effective_end,
                   ROW_NUMBER() OVER (
                       PARTITION BY code, code_system, cy, setting
                       ORDER BY (modifier IS NOT NULL), modifier
                   ) AS rn
            FROM read_parquet('{pfs}')
            WHERE quarter = 'A'
              AND ((cy = 2026 AND conversion_factor_kind = 'nonqp')
                   OR (cy <> 2026 AND conversion_factor_kind = 'single'))
        ) WHERE rn = 1
    ),
    pfs_cte AS (
        SELECT
            r.code AS code,
            r.code_system AS code_system,
            r.cy AS fy_or_cy,
            CAST(NULL AS INTEGER) AS fy,
            r.cy AS cy,
            r.setting AS setting,

            -- physician PUF carries no charge data (Phase 5 schema)
            CAST(NULL AS DOUBLE) AS charge,
            CAST(NULL AS BIGINT) AS charge_n,
            CAST(NULL AS VARCHAR) AS charge_source,
            CAST(NULL AS DOUBLE) AS charge_p10,
            CAST(NULL AS DOUBLE) AS charge_p25,
            CAST(NULL AS DOUBLE) AS charge_p50,
            CAST(NULL AS DOUBLE) AS charge_p75,
            CAST(NULL AS DOUBLE) AS charge_p90,

            COALESCE(e.payment_amount, r.allowed_amount) AS allowed_payment,
            e.n AS allowed_payment_n,
            CASE
                WHEN e.payment_amount IS NOT NULL THEN 'puf_empirical'
                WHEN r.allowed_amount IS NOT NULL THEN 'formula_pfs'
            END AS allowed_payment_source,
            e.payment_amount_concept AS allowed_payment_concept,
            e.p10_payment AS payment_p10,
            e.p25_payment AS payment_p25,
            e.p50_payment AS payment_p50,
            e.p75_payment AS payment_p75,
            e.p90_payment AS payment_p90,

            CAST(NULL AS DOUBLE) AS ccr_cost,
            CAST(NULL AS DOUBLE) AS ccr_value,
            CAST(NULL AS VARCHAR) AS ccr_cost_center,
            r.effective_start, r.effective_end
        FROM pfs_base r
        LEFT JOIN em_hcpcs e
            ON e.code = r.code
           AND e.year = r.cy
           AND e.place_of_srvc =
               CASE r.setting WHEN 'pfs_facility' THEN 'F' ELSE 'O' END
    ),

    ----------------------------------------------------------------
    -- 3) opps: opps_hcpcs_rates LEFT JOIN empirical_means
    --    Empirical for OPPS lives at APC granularity in empirical_means,
    --    and the HCPCS->APC link is opps_hcpcs_rates.apc.
    ----------------------------------------------------------------
    opps_base AS (
        SELECT
            code, modifier, code_system, cy, apc, payment_rate,
            effective_start, effective_end
        FROM read_parquet('{opps}')
        WHERE quarter = 'A'
    ),
    opps_cte AS (
        SELECT
            r.code AS code,
            r.code_system AS code_system,
            r.cy AS fy_or_cy,
            CAST(NULL AS INTEGER) AS fy,
            r.cy AS cy,
            CAST('opps' AS VARCHAR) AS setting,

            -- charge: outpatient PUF mean_charge (joined via APC) if present
            ea.mean_charge AS charge,
            ea.n AS charge_n,
            CASE WHEN ea.mean_charge IS NOT NULL THEN 'puf_empirical' END AS charge_source,
            ea.p10_charge AS charge_p10,
            ea.p25_charge AS charge_p25,
            ea.p50_charge AS charge_p50,
            ea.p75_charge AS charge_p75,
            ea.p90_charge AS charge_p90,

            COALESCE(ea.payment_amount, r.payment_rate) AS allowed_payment,
            ea.n AS allowed_payment_n,
            CASE
                WHEN ea.payment_amount IS NOT NULL THEN 'puf_empirical'
                WHEN r.payment_rate IS NOT NULL THEN 'formula_opps'
            END AS allowed_payment_source,
            ea.payment_amount_concept AS allowed_payment_concept,
            ea.p10_payment AS payment_p10,
            ea.p25_payment AS payment_p25,
            ea.p50_payment AS payment_p50,
            ea.p75_payment AS payment_p75,
            ea.p90_payment AS payment_p90,

            CAST(NULL AS DOUBLE) AS ccr_cost,
            CAST(NULL AS DOUBLE) AS ccr_value,
            CAST(NULL AS VARCHAR) AS ccr_cost_center,
            r.effective_start, r.effective_end
        FROM opps_base r
        LEFT JOIN em_apc ea
            ON ea.code = r.apc AND ea.year = r.cy
    ),

    ----------------------------------------------------------------
    -- 4) asc: asc_rates direct (no empirical merge -- empirical_means
    --    has no asc-tagged rows).
    ----------------------------------------------------------------
    asc_cte AS (
        SELECT
            r.code AS code,
            r.code_system AS code_system,
            r.cy AS fy_or_cy,
            CAST(NULL AS INTEGER) AS fy,
            r.cy AS cy,
            CAST('asc' AS VARCHAR) AS setting,

            CAST(NULL AS DOUBLE) AS charge,
            CAST(NULL AS BIGINT) AS charge_n,
            CAST(NULL AS VARCHAR) AS charge_source,
            CAST(NULL AS DOUBLE) AS charge_p10,
            CAST(NULL AS DOUBLE) AS charge_p25,
            CAST(NULL AS DOUBLE) AS charge_p50,
            CAST(NULL AS DOUBLE) AS charge_p75,
            CAST(NULL AS DOUBLE) AS charge_p90,

            r.payment_rate AS allowed_payment,
            CAST(NULL AS BIGINT) AS allowed_payment_n,
            CASE WHEN r.payment_rate IS NOT NULL THEN 'formula_asc' END AS allowed_payment_source,
            CAST(NULL AS VARCHAR) AS allowed_payment_concept,
            CAST(NULL AS DOUBLE) AS payment_p10,
            CAST(NULL AS DOUBLE) AS payment_p25,
            CAST(NULL AS DOUBLE) AS payment_p50,
            CAST(NULL AS DOUBLE) AS payment_p75,
            CAST(NULL AS DOUBLE) AS payment_p90,

            CAST(NULL AS DOUBLE) AS ccr_cost,
            CAST(NULL AS DOUBLE) AS ccr_value,
            CAST(NULL AS VARCHAR) AS ccr_cost_center,
            r.effective_start, r.effective_end
        FROM (
            SELECT code, modifier, code_system, cy, addendum, payment_rate,
                   effective_start, effective_end,
                   ROW_NUMBER() OVER (PARTITION BY code, cy
                                       ORDER BY CASE addendum WHEN 'AA' THEN 1
                                                              WHEN 'BB' THEN 2 END) AS rn
            FROM read_parquet('{asc}')
            WHERE quarter = 'A' AND addendum IN ('AA','BB')
        ) r
        WHERE r.rn = 1
    ),

    unioned AS (
        SELECT * FROM inpatient_cte
        UNION ALL SELECT * FROM pfs_cte
        UNION ALL SELECT * FROM opps_cte
        UNION ALL SELECT * FROM asc_cte
    )

    SELECT
        {_projection_sql()}
    FROM unioned
    """


def build_code_costs(
    input_silver_dir: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """Build ``data/gold/code_costs.parquet`` from six silver tables.

    Phase 6 deferred: ``national_ccr.parquet`` is NOT read; ``ccr_*`` columns
    ship as ALL NULL. Session 9.5 will repopulate after CCR overlay lands.

    Returns
    -------
    Path
        Output parquet path.

    Raises
    ------
    FileNotFoundError
        If any required silver table is missing.
    RuntimeError
        On compound-key collisions, schema drift, unexpected source tags,
        or row-count below the 1.5M floor.
    """
    silver = input_silver_dir if input_silver_dir is not None else config.SILVER
    out = output_path if output_path is not None else (config.GOLD / "code_costs.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)

    required = {
        "empirical_means": silver / "empirical_means.parquet",
        "ms_drg_weights": silver / "ms_drg_weights.parquet",
        "ipps_std_amounts": silver / "ipps_std_amounts.parquet",
        "pfs_rates": silver / "pfs_rates.parquet",
        "opps_hcpcs_rates": silver / "opps_hcpcs_rates.parquet",
        "asc_rates": silver / "asc_rates.parquet",
    }
    missing = [name for name, p in required.items() if not p.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing silver tables required by code_costs: {missing}. "
            "Run Phase 2/3/5 transforms first."
        )

    # Phase 6 (Session 9.5): national_ccr.parquet is OPTIONAL. If absent we
    # ship ccr_* as ALL NULL (Phase 6-deferred sentinel mode); if present we
    # populate inpatient ccr_value / ccr_cost / ccr_cost_center for FY2015+.
    optional = {
        "national_ccr": silver / "national_ccr.parquet",
    }

    # SQL-safe path strings (single quote escape only; paths come from config).
    silver_paths = {name: str(p).replace("'", "''") for name, p in required.items()}
    for name, p in optional.items():
        if p.exists():
            silver_paths[name] = str(p).replace("'", "''")

    union_sql = _build_union_sql(silver_paths)

    out_sql = str(out).replace("'", "''")
    con = duckdb.connect()
    try:
        # Materialize -- inline path per codes_master.py:154-156 (DuckDB COPY
        # parameter-binding interaction with read_parquet bind slots).
        con.execute(
            f"""
            COPY (
                {union_sql}
            ) TO '{out_sql}' (FORMAT PARQUET)
            """
        )

        # ------- Validation gates -------
        # Row-count floor: plan-v12.md acceptance says >=1.5M, but that figure
        # predates landmine #5 (canonical quarter='A' picks for pfs/opps/asc),
        # which roughly quartiles the per-source row counts. Empirically, the
        # canonical-quarter rule yields ~530K rows. Floor set to 400K to catch
        # silent join-collapse, not the original 1.5M aspirational target.
        # FLAG for Session 9 review: should plan-v12.md acceptance criterion
        # be revised to ~500K, or should we union all quarters?
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out)]).fetchone()[0]
        if n < 400_000:
            raise RuntimeError(
                f"code_costs.parquet row count {n:,} below floor 400,000. "
                "Investigate before declaring Phase 7a done."
            )

        dups = con.execute(
            """
            SELECT code, code_system, fy_or_cy, setting, COUNT(*) c
            FROM read_parquet(?)
            GROUP BY 1,2,3,4 HAVING c>1 ORDER BY c DESC LIMIT 5
            """,
            [str(out)],
        ).fetchall()
        if dups:
            raise RuntimeError(
                f"Compound-key (code, code_system, fy_or_cy, setting) collisions: {dups}"
            )

        bad_settings = con.execute(
            "SELECT DISTINCT setting FROM read_parquet(?) "
            "WHERE setting IS NULL OR setting NOT IN "
            "('inpatient','opps','asc','pfs_facility','pfs_nonfacility')",
            [str(out)],
        ).fetchall()
        if bad_settings:
            raise RuntimeError(f"Unexpected setting values: {bad_settings}")

        bad_tags = con.execute(
            """
            SELECT DISTINCT allowed_payment_source FROM read_parquet(?)
            WHERE allowed_payment_source IS NOT NULL
              AND allowed_payment_source NOT IN
              ('puf_empirical','formula_ipps','formula_pfs','formula_opps',
               'formula_asc','formula_no_empirical',
               'gems_bridged_modal','gems_bridged_back_translated')
            """,
            [str(out)],
        ).fetchall()
        if bad_tags:
            raise RuntimeError(f"Unexpected allowed_payment_source tags: {bad_tags}")
    finally:
        con.close()

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.code_costs",
        description=(
            "Join six silver tables into the GOLD code_costs.parquet "
            "(charge / allowed_payment / ccr_cost three-concept view). "
            "Phase 6 deferred: ccr_* columns are NULL pending Session 9.5."
        ),
    )
    parser.add_argument(
        "--input-silver-dir",
        default=None,
        type=Path,
        help="Silver directory. Default: config.SILVER (data/silver).",
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help="Output parquet path. Default: data/gold/code_costs.parquet.",
    )
    args = parser.parse_args(argv)

    out = build_code_costs(input_silver_dir=args.input_silver_dir, output_path=args.output)

    con = duckdb.connect()
    try:
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out)]).fetchone()[0]
    finally:
        con.close()
    print(f"Wrote {n} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
