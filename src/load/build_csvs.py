"""Phase 8/9 CSV builder for Neo4j 5.x bulk import.

Reads silver+gold parquets, writes 12 node CSVs + 13 relationship CSVs (the
``OBSERVED_AS``, ``MAPS_FROM``, ``MAPS_TO`` types each split into two CSVs by
end-label so each ID-space pairing is homogeneous) to ``data/neo4j_import/``
via DuckDB. Idempotent: re-running overwrites prior CSVs.

Each CSV header conforms to ``neo4j-admin database import full`` conventions
documented in ``docs/phase-8-neo4j-notes.md``:

    * One ID-space per node label (12 spaces).
    * RFC-4180 quoting via ``COPY ... TO ... CSV (HEADER, QUOTE '"')``.
    * Empty cells materialize as blanks; the importer is invoked with
      ``--ignore-empty-strings=true`` so they map to NULL downstream.
    * Date columns serialize ISO ``YYYY-MM-DD`` automatically.
    * Multiline fields work with ``--multiline-fields=true`` on the importer.

CostObservation IDs are sha1-truncated to 16 hex characters via a Python UDF
registered on the DuckDB connection (``sha1_id``) — option (a) from the brief.
GEMMapping IDs are synthetic ``GEM-{row_number}`` over a deterministic ORDER
BY so they remain stable across rebuilds of the frozen FY2018 silver.

Usage::

    python -m src.load.build_csvs                  # build everything
    python -m src.load.build_csvs --label MSDRG    # build one node CSV
    python -m src.load.build_csvs --skip-referential-check
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Callable

import duckdb

from src.config import GOLD, NEO4J_IMPORT, SILVER

log = logging.getLogger("build_csvs")

# ---------------------------------------------------------------------------
# Constants — ID-space spellings, file names, quoted parquet paths
# ---------------------------------------------------------------------------

ID_SPACES: dict[str, str] = {
    "ICDCMCode": "IcdCm-ID",
    "ICDPCSCode": "IcdPcs-ID",
    "HCPCS": "Hcpcs-ID",
    "MSDRG": "MsDrg-ID",
    "APC": "Apc-ID",
    "CostObservation": "Obs-ID",
    "IPPSStandardizedAmount": "IppsStdAmount-ID",
    "OPPSConversionFactor": "OppsCf-ID",
    "PFSConversionFactor": "PfsCf-ID",
    "CCR": "Ccr-ID",
    "CostCenter": "CostCenter-ID",
    "GEMMapping": "Gem-ID",
}

NODE_CSV_NAMES: dict[str, str] = {
    "ICDCMCode": "nodes_icd_cm.csv",
    "ICDPCSCode": "nodes_icd_pcs.csv",
    "HCPCS": "nodes_hcpcs.csv",
    "MSDRG": "nodes_msdrg.csv",
    "APC": "nodes_apc.csv",
    "CostObservation": "nodes_cost_observation.csv",
    "IPPSStandardizedAmount": "nodes_ipps_std_amount.csv",
    "OPPSConversionFactor": "nodes_opps_cf.csv",
    "PFSConversionFactor": "nodes_pfs_cf.csv",
    "CCR": "nodes_ccr.csv",
    "CostCenter": "nodes_cost_center.csv",
    "GEMMapping": "nodes_gem_mapping.csv",
}

# 13 rel CSVs — OBSERVED_AS, MAPS_FROM, MAPS_TO each split by end-label.
REL_CSV_NAMES: dict[str, str] = {
    "CAN_ANCHOR": "rels_can_anchor.csv",
    "IS_MODAL_FOR": "rels_is_modal_for.csv",
    "OBSERVED_AS_MSDRG": "rels_observed_as_msdrg.csv",
    "OBSERVED_AS_HCPCS": "rels_observed_as_hcpcs.csv",
    "MAPPED_TO_APC": "rels_mapped_to_apc.csv",
    "HAS_STD_AMOUNT": "rels_has_std_amount.csv",
    "USES_CCR": "rels_uses_ccr.csv",
    "IN_COST_CENTER": "rels_in_cost_center.csv",
    "MAPS_FROM_ICD_CM": "rels_maps_from_icd_cm.csv",
    "MAPS_FROM_ICD_PCS": "rels_maps_from_icd_pcs.csv",
    "MAPS_TO_ICD_CM": "rels_maps_to_icd_cm.csv",
    "MAPS_TO_ICD_PCS": "rels_maps_to_icd_pcs.csv",
    "IS_REIMBURSEMENT_OF": "rels_is_reimbursement_of.csv",
}

# Quoted paths for embedding in DuckDB SQL.
P_CODES = f"'{(SILVER / 'codes_master.parquet').as_posix()}'"
P_GEMS = f"'{(SILVER / 'gems_2018.parquet').as_posix()}'"
P_MSDRG = f"'{(SILVER / 'ms_drg_weights.parquet').as_posix()}'"
P_APC = f"'{(SILVER / 'opps_apc_rates.parquet').as_posix()}'"
P_OPPS_HCPCS = f"'{(SILVER / 'opps_hcpcs_rates.parquet').as_posix()}'"
P_PFS = f"'{(SILVER / 'pfs_rates.parquet').as_posix()}'"
P_NCCR = f"'{(SILVER / 'national_ccr.parquet').as_posix()}'"
P_IPPS_STD = f"'{(SILVER / 'ipps_std_amounts.parquet').as_posix()}'"
P_COSTS = f"'{(GOLD / 'code_costs.parquet').as_posix()}'"
P_MODAL = f"'{(GOLD / 'icd_principal_drg_modal.parquet').as_posix()}'"
P_REACH = f"'{(GOLD / 'icd_principal_drg_reachability.parquet').as_posix()}'"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha1_id(s: str) -> str:
    """Sha1-truncated to 16 hex chars — used for CostObservation IDs."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def _icd_system_short_sql(col: str) -> str:
    """SQL fragment that strips the 'ICD-' prefix from a code_system column.

    e.g. ``ICD-10-CM`` -> ``10-CM``, ``ICD-9-Vol3`` -> ``9-Vol3``.
    """
    return f"REPLACE({col}, 'ICD-', '')"


def _slugify_sql(col: str) -> str:
    """SQL fragment that lowercases and replaces spaces with underscores."""
    return f"REGEXP_REPLACE(LOWER({col}), ' +', '_', 'g')"


def _copy_to_csv(con: duckdb.DuckDBPyConnection, select_sql: str, out_path: Path) -> int:
    """Execute COPY (select_sql) TO out_path with HEADER, QUOTE '"'.

    Returns the number of data rows written (excluding header).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({select_sql}) TO '{out_path.as_posix()}' "
        "(HEADER, FORMAT CSV, QUOTE '\"', ESCAPE '\"')"
    )
    # Count rows post-write (DuckDB COPY does not return count).
    [(row_count,)] = con.execute(
        f"SELECT count(*) FROM read_csv_auto('{out_path.as_posix()}', header=true)"
    ).fetchall()
    return int(row_count)


def _timed(name: str, fn: Callable[[], int]) -> int:
    t0 = time.perf_counter()
    n = fn()
    dt = time.perf_counter() - t0
    log.info("%s: %d rows in %.2fs", name, n, dt)
    return n


# ---------------------------------------------------------------------------
# Node builders — 12 CSVs
# ---------------------------------------------------------------------------


def build_nodes_icd_cm(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["ICDCMCode"]
    select_sql = f"""
    SELECT
      'ICDCM-' || {_icd_system_short_sql("code_system")} || '-' || code AS "id:ID(IcdCm-ID)",
      'ICDCMCode' AS ":LABEL",
      code,
      code_system,
      ANY_VALUE(description ORDER BY fy_or_cy DESC) AS description,
      ANY_VALUE(short_description ORDER BY fy_or_cy DESC) AS short_description,
      ANY_VALUE(chapter ORDER BY fy_or_cy DESC) AS chapter,
      BOOL_OR(mcc_flag) AS "mcc_flag:boolean",
      BOOL_OR(cc_flag) AS "cc_flag:boolean",
      BOOL_OR(poa_exempt) AS "poa_exempt:boolean",
      MIN(effective_start) AS "effective_start:date",
      MAX(effective_end) AS "effective_end:date",
      MAX(fy_or_cy) AS "latest_fy:int"
    FROM {P_CODES}
    WHERE code_system IN ('ICD-10-CM', 'ICD-9-CM')
    GROUP BY code, code_system
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_icd_pcs(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["ICDPCSCode"]
    select_sql = f"""
    SELECT
      'ICDPCS-' || {_icd_system_short_sql("code_system")} || '-' || code AS "id:ID(IcdPcs-ID)",
      'ICDPCSCode' AS ":LABEL",
      code,
      code_system,
      ANY_VALUE(description ORDER BY fy_or_cy DESC) AS description,
      ANY_VALUE(short_description ORDER BY fy_or_cy DESC) AS short_description,
      BOOL_OR(or_procedure_flag) AS "or_procedure_flag:boolean",
      MIN(effective_start) AS "effective_start:date",
      MAX(effective_end) AS "effective_end:date",
      MAX(fy_or_cy) AS "latest_fy:int"
    FROM {P_CODES}
    WHERE code_system IN ('ICD-10-PCS', 'ICD-9-Vol3')
    GROUP BY code, code_system
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_hcpcs(con: duckdb.DuckDBPyConnection) -> int:
    """Union HCPCS-eligible codes across silver + gold; verify no collisions.

    A code's ``code_type`` is the single ``code_system`` it appears under.
    A collision (same ``code`` under two ``code_system`` values) raises before
    write — per plan-v13 risk register row 2.
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["HCPCS"]

    # Pre-write collision check: same code under multiple code_systems.
    collisions = con.execute(
        f"""
        WITH all_hcpcs AS (
          SELECT code, code_system FROM {P_CODES} WHERE code_system='HCPCS-II'
          UNION
          SELECT code, code_system FROM {P_PFS}
          UNION
          SELECT code, code_system FROM {P_OPPS_HCPCS}
          UNION
          SELECT code, code_system FROM {P_COSTS}
            WHERE code_system IN ('CPT', 'HCPCS-II')
        )
        SELECT code, count(DISTINCT code_system) AS k
        FROM all_hcpcs
        GROUP BY 1 HAVING count(DISTINCT code_system) > 1
        """
    ).fetchall()
    if collisions:
        sample = collisions[:10]
        raise RuntimeError(
            f"HCPCS code-collision: {len(collisions)} codes appear under multiple "
            f"code_systems (CPT vs HCPCS-II). Sample: {sample}"
        )

    # Build a per-source descriptor + year ladder, pick the latest descriptor
    # available across all sources, with HCPCS-II preferring codes_master and
    # CPT preferring opps_hcpcs_rates.short_descriptor (AMA strips long-form
    # CPT descriptors from codes_master).
    select_sql = f"""
    WITH base AS (
      -- codes_master (HCPCS-II only — CPT not in codes_master)
      SELECT code, code_system, fy_or_cy AS year,
             description, short_description AS short_descriptor
      FROM {P_CODES}
      WHERE code_system = 'HCPCS-II'
      UNION ALL
      -- opps_hcpcs_rates: source for CPT short descriptors
      SELECT code, code_system, cy AS year,
             NULL AS description, short_descriptor
      FROM {P_OPPS_HCPCS}
      UNION ALL
      -- pfs_rates: codes appearing in PFS
      SELECT code, code_system, cy AS year,
             NULL AS description, NULL AS short_descriptor
      FROM {P_PFS}
      UNION ALL
      -- code_costs: catch any CPT/HCPCS-II not surfaced above
      SELECT code, code_system, fy_or_cy AS year,
             NULL AS description, NULL AS short_descriptor
      FROM {P_COSTS}
      WHERE code_system IN ('CPT', 'HCPCS-II')
    ),
    by_code AS (
      SELECT
        code,
        ANY_VALUE(code_system) AS code_system,
        -- descriptor preference: latest non-null description, else latest short_descriptor
        ANY_VALUE(description ORDER BY year DESC) FILTER (WHERE description IS NOT NULL)
          AS description_latest,
        ANY_VALUE(short_descriptor ORDER BY year DESC) FILTER (WHERE short_descriptor IS NOT NULL)
          AS short_descriptor_latest,
        MAX(year) AS latest_year
      FROM base
      GROUP BY code
    )
    SELECT
      'HCPCS-' || code AS "id:ID(Hcpcs-ID)",
      'HCPCS' AS ":LABEL",
      code,
      CASE code_system WHEN 'HCPCS-II' THEN 'HCPCS_II' WHEN 'CPT' THEN 'CPT' ELSE code_system END
        AS code_type,
      COALESCE(description_latest, short_descriptor_latest) AS descriptor,
      short_descriptor_latest AS short_descriptor,
      latest_year AS "latest_year:int"
    FROM by_code
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_msdrg(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["MSDRG"]
    select_sql = f"""
    SELECT
      'MSDRG-' || ms_drg || '-' || fy AS "id:ID(MsDrg-ID)",
      'MSDRG' AS ":LABEL",
      ms_drg AS code,
      fy AS "fy:int",
      title,
      mdc,
      drg_type,
      severity,
      relative_weight AS "relative_weight:double",
      gmlos AS "gmlos:double",
      amlos AS "amlos:double",
      effective_start AS "effective_start:date",
      effective_end AS "effective_end:date"
    FROM {P_MSDRG}
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_apc(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["APC"]
    select_sql = f"""
    SELECT
      'APC-' || apc || '-' || cy || '-' || quarter AS "id:ID(Apc-ID)",
      'APC' AS ":LABEL",
      apc AS code,
      cy AS "cy:int",
      quarter,
      status_indicator,
      apc_title,
      relative_weight AS "relative_weight:double",
      payment_rate AS "payment_rate:double",
      national_unadjusted_copayment AS "national_unadjusted_copayment:double",
      minimum_unadjusted_copayment AS "minimum_unadjusted_copayment:double",
      effective_start AS "effective_start:date",
      effective_end AS "effective_end:date"
    FROM {P_APC}
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_ipps_std_amount(con: duckdb.DuckDBPyConnection) -> int:
    """Pivot long-format silver to wide. One row per fy, 5 columns.

    Silver carries (line_item, category) tuples per fy. We keep the
    'national' rate-period (most generally applicable) for these columns:

    * operating: ``operating_full_update``
    * capital: ``capital_federal_rate``
    * labor_share: ``labor_share``
    * nonlabor_share: ``nonlabor_share``
    * ltch_standard_rate: ``ltch_standard_rate``

    'outlier_threshold' is not present in silver (plan-v13 prose only) — skip.
    Rural / Puerto Rico variants are dropped at this layer; they can be
    revisited if downstream queries surface a need.
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["IPPSStandardizedAmount"]
    select_sql = f"""
    WITH wide AS (
      SELECT
        fy,
        MAX(value) FILTER (WHERE line_item='operating_full_update' AND category='national')
          AS operating,
        MAX(value) FILTER (WHERE line_item='capital_federal_rate' AND category='national')
          AS capital,
        MAX(value) FILTER (WHERE line_item='labor_share' AND category='national')
          AS labor_share,
        MAX(value) FILTER (WHERE line_item='nonlabor_share' AND category='national')
          AS nonlabor_share,
        MAX(value) FILTER (WHERE line_item='ltch_standard_rate' AND category='national')
          AS ltch_standard_rate,
        MIN(effective_start) AS effective_start,
        MAX(effective_end) AS effective_end
      FROM {P_IPPS_STD}
      GROUP BY fy
    )
    SELECT
      'IPPSSTD-' || fy AS "id:ID(IppsStdAmount-ID)",
      'IPPSStandardizedAmount' AS ":LABEL",
      fy AS "fy:int",
      operating AS "operating:double",
      capital AS "capital:double",
      labor_share AS "labor_share:double",
      nonlabor_share AS "nonlabor_share:double",
      ltch_standard_rate AS "ltch_standard_rate:double",
      effective_start AS "effective_start:date",
      effective_end AS "effective_end:date"
    FROM wide
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_opps_cf(con: duckdb.DuckDBPyConnection) -> int:
    """Distinct (cy, quarter) from opps_apc_rates.

    Silver does not carry the OPPS conversion factor as a column, so
    ``conversion_factor`` ships as NULL. CF nodes are reference-only this
    session per plan-v13 Step 3 ("conversion-factor edges into rate
    calculations" listed as out of scope).
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["OPPSConversionFactor"]
    select_sql = f"""
    SELECT
      'OPPSCF-' || cy || '-' || quarter AS "id:ID(OppsCf-ID)",
      'OPPSConversionFactor' AS ":LABEL",
      cy AS "cy:int",
      quarter,
      CAST(NULL AS DOUBLE) AS "conversion_factor:double",
      MIN(effective_start) AS "effective_start:date",
      MAX(effective_end) AS "effective_end:date"
    FROM {P_APC}
    GROUP BY cy, quarter
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_pfs_cf(con: duckdb.DuckDBPyConnection) -> int:
    """Distinct (cy, kind) from pfs_rates. ``kind`` is ``conversion_factor_kind``.

    CY2017-2025 ships ``single``; CY2026 ships both ``qp`` and ``nonqp``
    (the QP / non-QP split is the user-locked decision in CLAUDE.md).
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["PFSConversionFactor"]
    select_sql = f"""
    SELECT
      'PFSCF-' || cy || '-' || conversion_factor_kind AS "id:ID(PfsCf-ID)",
      'PFSConversionFactor' AS ":LABEL",
      cy AS "cy:int",
      conversion_factor_kind AS kind,
      ANY_VALUE(conversion_factor) AS "conversion_factor:double",
      MIN(effective_start) AS "effective_start:date",
      MAX(effective_end) AS "effective_end:date"
    FROM {P_PFS}
    WHERE conversion_factor_kind IS NOT NULL
    GROUP BY cy, conversion_factor_kind
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_cost_center(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["CostCenter"]
    select_sql = f"""
    SELECT
      'COSTCTR-' || {_slugify_sql("cost_center_group")} AS "id:ID(CostCenter-ID)",
      'CostCenter' AS ":LABEL",
      cost_center_group AS name
    FROM (
      SELECT DISTINCT cost_center_group FROM {P_NCCR}
    )
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_ccr(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / NODE_CSV_NAMES["CCR"]
    select_sql = f"""
    SELECT
      'CCR-' || fy || '-' || {_slugify_sql("cost_center_group")} AS "id:ID(Ccr-ID)",
      'CCR' AS ":LABEL",
      fy AS "fy:int",
      cost_center_group,
      n_providers AS "n_providers:int",
      sum_cost AS "sum_cost:double",
      sum_charges AS "sum_charges:double",
      ccr_value AS "ccr_value:double",
      source,
      effective_start AS "effective_start:date",
      effective_end AS "effective_end:date"
    FROM {P_NCCR}
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_gem_mapping(con: duckdb.DuckDBPyConnection) -> int:
    """Synthetic ``GEM-{row_number}`` IDs.

    Stable across rebuilds *as long as the silver gems_2018.parquet file is
    unchanged*: ORDER BY (source_system, source_code, target_code,
    is_reimbursement) is a total ordering on the GEMs row set. GEMs is
    frozen FY2018 (CLAUDE.md schema lock), so the IDs are effectively stable
    in practice. If silver is ever re-extracted, GEM-edge IDs may shift.
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["GEMMapping"]
    select_sql = f"""
    SELECT
      'GEM-' || ROW_NUMBER() OVER (
        ORDER BY source_system, source_code, target_code,
                 is_reimbursement, scenario, choice_list
      ) AS "id:ID(Gem-ID)",
      'GEMMapping' AS ":LABEL",
      source_code,
      source_system,
      target_code,
      target_system,
      approximate_flag AS "approximate_flag:boolean",
      no_map_flag AS "no_map_flag:boolean",
      combination_flag AS "combination_flag:boolean",
      scenario AS "scenario:int",
      choice_list AS "choice_list:int",
      is_reimbursement AS "is_reimbursement:boolean"
    FROM {P_GEMS}
    """
    return _copy_to_csv(con, select_sql, out)


def build_nodes_cost_observation(con: duckdb.DuckDBPyConnection) -> int:
    """Emit one node per (cost_costs row × concept) where value IS NOT NULL.

    OBS IDs are sha1[:16] over ``code|code_system|year|setting|concept``,
    computed via the ``sha1_id`` UDF registered in ``main()``. We assert
    DISTINCT on the synthesized id and fail loudly on collisions (per the
    discovery brief, ~10⁻¹³ over ~375k rows).
    """
    out = NEO4J_IMPORT / NODE_CSV_NAMES["CostObservation"]
    # The three-concept materialization unions three SELECTs over code_costs.
    # ccr_cost rows hardcode source='hcris_v_2010' per Step 4 spec.
    select_sql = f"""
    WITH all_obs AS (
      -- charge
      SELECT
        'OBS-' || sha1_id(code || '|' || code_system || '|'
                          || fy_or_cy || '|' || setting || '|charge') AS id,
        charge AS value,
        'charge' AS concept,
        setting,
        fy_or_cy AS year,
        charge_source AS source,
        charge_n AS n,
        charge_p10 AS p10, charge_p25 AS p25, charge_p50 AS p50,
        charge_p75 AS p75, charge_p90 AS p90,
        CAST(NULL AS DOUBLE) AS ccr_value,
        CAST(NULL AS VARCHAR) AS ccr_cost_center,
        CAST(NULL AS VARCHAR) AS allowed_payment_concept
      FROM {P_COSTS}
      WHERE charge IS NOT NULL
      UNION ALL
      -- allowed_payment
      SELECT
        'OBS-' || sha1_id(code || '|' || code_system || '|'
                          || fy_or_cy || '|' || setting || '|allowed_payment') AS id,
        allowed_payment AS value,
        'allowed_payment' AS concept,
        setting,
        fy_or_cy AS year,
        allowed_payment_source AS source,
        allowed_payment_n AS n,
        payment_p10 AS p10, payment_p25 AS p25, payment_p50 AS p50,
        payment_p75 AS p75, payment_p90 AS p90,
        CAST(NULL AS DOUBLE) AS ccr_value,
        CAST(NULL AS VARCHAR) AS ccr_cost_center,
        allowed_payment_concept
      FROM {P_COSTS}
      WHERE allowed_payment IS NOT NULL
      UNION ALL
      -- ccr_cost
      SELECT
        'OBS-' || sha1_id(code || '|' || code_system || '|'
                          || fy_or_cy || '|' || setting || '|ccr_cost') AS id,
        ccr_cost AS value,
        'ccr_cost' AS concept,
        setting,
        fy_or_cy AS year,
        'hcris_v_2010' AS source,
        CAST(NULL AS BIGINT) AS n,
        CAST(NULL AS DOUBLE) AS p10, CAST(NULL AS DOUBLE) AS p25,
        CAST(NULL AS DOUBLE) AS p50, CAST(NULL AS DOUBLE) AS p75,
        CAST(NULL AS DOUBLE) AS p90,
        ccr_value,
        ccr_cost_center,
        CAST(NULL AS VARCHAR) AS allowed_payment_concept
      FROM {P_COSTS}
      WHERE ccr_cost IS NOT NULL
    )
    SELECT
      id AS "id:ID(Obs-ID)",
      'CostObservation' AS ":LABEL",
      -- IF(isnan(...)) coerces pandas-NaN floats (originating in the PUF
      -- source parquets) to SQL NULL so neo4j-admin sees an empty cell
      -- rather than the literal string "nan", which it rejects with
      -- 'Not a number: "nan"' when --ignore-empty-strings=true is set.
      IF(isnan(value), NULL, value) AS "value:double",
      concept,
      setting,
      year AS "year:int",
      source,
      n AS "n:long",
      IF(isnan(p10), NULL, p10) AS "p10:double",
      IF(isnan(p25), NULL, p25) AS "p25:double",
      IF(isnan(p50), NULL, p50) AS "p50:double",
      IF(isnan(p75), NULL, p75) AS "p75:double",
      IF(isnan(p90), NULL, p90) AS "p90:double",
      IF(isnan(ccr_value), NULL, ccr_value) AS "ccr_value:double",
      ccr_cost_center,
      allowed_payment_concept
    FROM all_obs
    """
    n = _copy_to_csv(con, select_sql, out)

    # Collision guard.
    [(distinct_ids,)] = con.execute(
        f"SELECT count(DISTINCT \"id:ID(Obs-ID)\") "
        f"FROM read_csv_auto('{out.as_posix()}', header=true)"
    ).fetchall()
    if int(distinct_ids) != n:
        raise RuntimeError(
            f"CostObservation sha1[:16] id collision: {n} rows, "
            f"{distinct_ids} distinct ids"
        )
    return n


# ---------------------------------------------------------------------------
# Relationship builders — 13 CSVs (3 split rels)
# ---------------------------------------------------------------------------


def build_rels_can_anchor(con: duckdb.DuckDBPyConnection) -> int:
    """ICD-CM -> MS-DRG reachability edges.

    Inner-joins against ``ms_drg_weights`` to drop drgpy "ungroupable"
    sentinels (ms_drg='000') and any other rows whose target MS-DRG node
    doesn't exist for that FY. ~31k orphan-end rows on the FY2019-FY2022
    drgpy backfill — see plans/Session 9 flags.md for context.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["CAN_ANCHOR"]
    select_sql = f"""
    SELECT
      'ICDCM-10-CM-' || r.icd_cm AS ":START_ID(IcdCm-ID)",
      'MSDRG-' || r.ms_drg || '-' || r.fy AS ":END_ID(MsDrg-ID)",
      'CAN_ANCHOR' AS ":TYPE",
      r.fy AS "fy:int",
      r.source,
      r.permutations_hit AS "permutations_hit:int"
    FROM {P_REACH} r
    JOIN {P_MSDRG} m ON r.ms_drg = m.ms_drg AND r.fy = m.fy
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_is_modal_for(con: duckdb.DuckDBPyConnection) -> int:
    """ICD-CM -> modal MS-DRG. Same inner-join filter as CAN_ANCHOR."""
    out = NEO4J_IMPORT / REL_CSV_NAMES["IS_MODAL_FOR"]
    select_sql = f"""
    SELECT
      'ICDCM-10-CM-' || mo.icd_cm AS ":START_ID(IcdCm-ID)",
      'MSDRG-' || mo.ms_drg || '-' || mo.fy AS ":END_ID(MsDrg-ID)",
      'IS_MODAL_FOR' AS ":TYPE",
      mo.fy AS "fy:int",
      mo.source,
      mo.modal_permutation,
      mo.permutations_total AS "permutations_total:int"
    FROM {P_MODAL} mo
    JOIN {P_MSDRG} m ON mo.ms_drg = m.ms_drg AND mo.fy = m.fy
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_observed_as_msdrg(con: duckdb.DuckDBPyConnection) -> int:
    """Inpatient cost-obs edges: MSDRG -> CostObservation.

    Same sha1 logic as ``build_nodes_cost_observation`` keeps OBS IDs aligned.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["OBSERVED_AS_MSDRG"]
    select_sql = f"""
    WITH src AS (
      SELECT 'charge' AS concept, code, code_system, fy_or_cy, setting, fy, charge AS v
      FROM {P_COSTS} WHERE setting='inpatient' AND charge IS NOT NULL
      UNION ALL
      SELECT 'allowed_payment', code, code_system, fy_or_cy, setting, fy, allowed_payment
      FROM {P_COSTS} WHERE setting='inpatient' AND allowed_payment IS NOT NULL
      UNION ALL
      SELECT 'ccr_cost', code, code_system, fy_or_cy, setting, fy, ccr_cost
      FROM {P_COSTS} WHERE setting='inpatient' AND ccr_cost IS NOT NULL
    )
    SELECT
      'MSDRG-' || code || '-' || fy AS ":START_ID(MsDrg-ID)",
      'OBS-' || sha1_id(code || '|' || code_system || '|' || fy_or_cy
                        || '|' || setting || '|' || concept) AS ":END_ID(Obs-ID)",
      'OBSERVED_AS' AS ":TYPE",
      setting,
      fy_or_cy AS "year:int",
      concept
    FROM src
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_observed_as_hcpcs(con: duckdb.DuckDBPyConnection) -> int:
    """Non-inpatient cost-obs edges: HCPCS -> CostObservation."""
    out = NEO4J_IMPORT / REL_CSV_NAMES["OBSERVED_AS_HCPCS"]
    select_sql = f"""
    WITH src AS (
      SELECT 'charge' AS concept, code, code_system, fy_or_cy, setting, charge AS v
      FROM {P_COSTS} WHERE setting <> 'inpatient' AND charge IS NOT NULL
      UNION ALL
      SELECT 'allowed_payment', code, code_system, fy_or_cy, setting, allowed_payment
      FROM {P_COSTS} WHERE setting <> 'inpatient' AND allowed_payment IS NOT NULL
      UNION ALL
      SELECT 'ccr_cost', code, code_system, fy_or_cy, setting, ccr_cost
      FROM {P_COSTS} WHERE setting <> 'inpatient' AND ccr_cost IS NOT NULL
    )
    SELECT
      'HCPCS-' || code AS ":START_ID(Hcpcs-ID)",
      'OBS-' || sha1_id(code || '|' || code_system || '|' || fy_or_cy
                        || '|' || setting || '|' || concept) AS ":END_ID(Obs-ID)",
      'OBSERVED_AS' AS ":TYPE",
      setting,
      fy_or_cy AS "year:int",
      concept
    FROM src
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_mapped_to_apc(con: duckdb.DuckDBPyConnection) -> int:
    """HCPCS -> APC for OPPS HCPCS-with-APC rows.

    Inner-joins against ``opps_apc_rates`` to drop ~12k rows pointing to
    (cy, quarter) cells that exist in opps_hcpcs_rates but not in
    opps_apc_rates (silver gaps for CY2016 Q1, CY2020-2021, CY2022 Q2-Q4,
    CY2024 Q2). Also filters NULL apc rows (~522k OPPS HCPCS rows have no
    APC mapping).
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["MAPPED_TO_APC"]
    select_sql = f"""
    SELECT
      'HCPCS-' || h.code AS ":START_ID(Hcpcs-ID)",
      'APC-' || h.apc || '-' || h.cy || '-' || h.quarter AS ":END_ID(Apc-ID)",
      'MAPPED_TO_APC' AS ":TYPE",
      h.cy AS "cy:int",
      h.quarter,
      h.status_indicator,
      h.relative_weight AS "relative_weight:double",
      h.payment_rate AS "payment_rate:double"
    FROM {P_OPPS_HCPCS} h
    JOIN {P_APC} a
      ON h.apc = a.apc AND h.cy = a.cy AND h.quarter = a.quarter
    WHERE h.apc IS NOT NULL
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_has_std_amount(con: duckdb.DuckDBPyConnection) -> int:
    """One edge per (ms_drg, fy) row in silver, only if an IPPS std node exists.

    Silver IPPS std spans FY2008-FY2026; ms_drg_weights spans FY2008-FY2026.
    Inner join on fy guarantees referential integrity even if the FY ranges
    diverge in the future.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["HAS_STD_AMOUNT"]
    select_sql = f"""
    SELECT
      'MSDRG-' || m.ms_drg || '-' || m.fy AS ":START_ID(MsDrg-ID)",
      'IPPSSTD-' || m.fy AS ":END_ID(IppsStdAmount-ID)",
      'HAS_STD_AMOUNT' AS ":TYPE",
      m.fy AS "fy:int"
    FROM {P_MSDRG} m
    WHERE EXISTS (SELECT 1 FROM {P_IPPS_STD} i WHERE i.fy = m.fy)
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_uses_ccr(con: duckdb.DuckDBPyConnection) -> int:
    """For every concept='ccr_cost' obs, edge to the matching CCR node.

    CCR id = ``CCR-{fy}-{slug(cost_center_group)}`` where
    ``cost_center_group`` is the obs row's ``ccr_cost_center`` column.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["USES_CCR"]
    select_sql = f"""
    SELECT
      'OBS-' || sha1_id(code || '|' || code_system || '|' || fy_or_cy
                        || '|' || setting || '|ccr_cost') AS ":START_ID(Obs-ID)",
      'CCR-' || fy_or_cy || '-' || {_slugify_sql("ccr_cost_center")}
        AS ":END_ID(Ccr-ID)",
      'USES_CCR' AS ":TYPE"
    FROM {P_COSTS}
    WHERE ccr_cost IS NOT NULL AND ccr_cost_center IS NOT NULL
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_in_cost_center(con: duckdb.DuckDBPyConnection) -> int:
    out = NEO4J_IMPORT / REL_CSV_NAMES["IN_COST_CENTER"]
    select_sql = f"""
    SELECT
      'CCR-' || fy || '-' || {_slugify_sql("cost_center_group")}
        AS ":START_ID(Ccr-ID)",
      'COSTCTR-' || {_slugify_sql("cost_center_group")} AS ":END_ID(CostCenter-ID)",
      'IN_COST_CENTER' AS ":TYPE"
    FROM {P_NCCR}
    """
    return _copy_to_csv(con, select_sql, out)


# --- GEMs MAPS_FROM / MAPS_TO splits ---------------------------------------
#
# GEM nodes get synthetic IDs `GEM-{row_number}` over a deterministic ordering.
# To wire MAPS_FROM/MAPS_TO edges we reproduce that exact ordering inside a
# CTE, so the same row gets the same id in both the node CSV and the rel CSV.

_GEMS_WITH_IDS_CTE = f"""
WITH gems_ids AS (
  SELECT
    'GEM-' || ROW_NUMBER() OVER (
      ORDER BY source_system, source_code, target_code,
               is_reimbursement, scenario, choice_list
    ) AS gem_id,
    *
  FROM {P_GEMS}
)
"""


def build_rels_maps_from_icd_cm(con: duckdb.DuckDBPyConnection) -> int:
    """GEMMapping -> source ICDCMCode.

    Inner-joins on codes_master (semi-join) to drop ~1 row pointing to
    NULL/retired codes that never made it into codes_master.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["MAPS_FROM_ICD_CM"]
    select_sql = f"""
    {_GEMS_WITH_IDS_CTE}
    SELECT
      g.gem_id AS ":START_ID(Gem-ID)",
      'ICDCM-' || {_icd_system_short_sql("g.source_system")} || '-' || g.source_code
        AS ":END_ID(IcdCm-ID)",
      'MAPS_FROM' AS ":TYPE"
    FROM gems_ids g
    WHERE g.source_system IN ('ICD-9-CM', 'ICD-10-CM')
      AND EXISTS (
        SELECT 1 FROM {P_CODES} c
        WHERE c.code = g.source_code AND c.code_system = g.source_system
      )
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_maps_from_icd_pcs(con: duckdb.DuckDBPyConnection) -> int:
    """GEMMapping -> source ICDPCSCode."""
    out = NEO4J_IMPORT / REL_CSV_NAMES["MAPS_FROM_ICD_PCS"]
    select_sql = f"""
    {_GEMS_WITH_IDS_CTE}
    SELECT
      g.gem_id AS ":START_ID(Gem-ID)",
      'ICDPCS-' || {_icd_system_short_sql("g.source_system")} || '-' || g.source_code
        AS ":END_ID(IcdPcs-ID)",
      'MAPS_FROM' AS ":TYPE"
    FROM gems_ids g
    WHERE g.source_system IN ('ICD-9-Vol3', 'ICD-10-PCS')
      AND EXISTS (
        SELECT 1 FROM {P_CODES} c
        WHERE c.code = g.source_code AND c.code_system = g.source_system
      )
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_maps_to_icd_cm(con: duckdb.DuckDBPyConnection) -> int:
    """GEMMapping -> target ICDCMCode.

    Drops ~1228 rows whose target_code is NULL (a 'no_map' GEM convention)
    or pointed at a retired code not in codes_master.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["MAPS_TO_ICD_CM"]
    select_sql = f"""
    {_GEMS_WITH_IDS_CTE}
    SELECT
      g.gem_id AS ":START_ID(Gem-ID)",
      'ICDCM-' || {_icd_system_short_sql("g.target_system")} || '-' || g.target_code
        AS ":END_ID(IcdCm-ID)",
      'MAPS_TO' AS ":TYPE"
    FROM gems_ids g
    WHERE g.target_system IN ('ICD-9-CM', 'ICD-10-CM')
      AND g.target_code IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM {P_CODES} c
        WHERE c.code = g.target_code AND c.code_system = g.target_system
      )
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_maps_to_icd_pcs(con: duckdb.DuckDBPyConnection) -> int:
    """GEMMapping -> target ICDPCSCode."""
    out = NEO4J_IMPORT / REL_CSV_NAMES["MAPS_TO_ICD_PCS"]
    select_sql = f"""
    {_GEMS_WITH_IDS_CTE}
    SELECT
      g.gem_id AS ":START_ID(Gem-ID)",
      'ICDPCS-' || {_icd_system_short_sql("g.target_system")} || '-' || g.target_code
        AS ":END_ID(IcdPcs-ID)",
      'MAPS_TO' AS ":TYPE"
    FROM gems_ids g
    WHERE g.target_system IN ('ICD-9-Vol3', 'ICD-10-PCS')
      AND g.target_code IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM {P_CODES} c
        WHERE c.code = g.target_code AND c.code_system = g.target_system
      )
    """
    return _copy_to_csv(con, select_sql, out)


def build_rels_is_reimbursement_of(con: duckdb.DuckDBPyConnection) -> int:
    """Self-join GEMs reverse-direction rows to their forward counterparts.

    Pair criterion: R (is_reimbursement=True) and F (is_reimbursement=False)
    where R.source_code=F.target_code AND R.target_code=F.source_code AND
    the system pair is swapped. Drop rows with no forward counterpart.
    """
    out = NEO4J_IMPORT / REL_CSV_NAMES["IS_REIMBURSEMENT_OF"]
    select_sql = f"""
    {_GEMS_WITH_IDS_CTE}
    SELECT
      r.gem_id AS ":START_ID(Gem-ID)",
      f.gem_id AS ":END_ID(Gem-ID)",
      'IS_REIMBURSEMENT_OF' AS ":TYPE"
    FROM gems_ids r
    JOIN gems_ids f
      ON r.is_reimbursement = TRUE
     AND f.is_reimbursement = FALSE
     AND r.source_code = f.target_code
     AND r.target_code = f.source_code
     AND r.source_system = f.target_system
     AND r.target_system = f.source_system
    """
    return _copy_to_csv(con, select_sql, out)


# ---------------------------------------------------------------------------
# Referential-integrity check
# ---------------------------------------------------------------------------


def referential_integrity_check(con: duckdb.DuckDBPyConnection) -> None:
    """For each rel CSV, verify START_ID and END_ID resolve to a node CSV.

    Raises on any orphan; logs a count of ok joins per rel CSV.
    """
    pairs: list[tuple[str, str, str]] = [
        # (rel_csv, start_node_csv, end_node_csv)
        ("CAN_ANCHOR", "ICDCMCode", "MSDRG"),
        ("IS_MODAL_FOR", "ICDCMCode", "MSDRG"),
        ("OBSERVED_AS_MSDRG", "MSDRG", "CostObservation"),
        ("OBSERVED_AS_HCPCS", "HCPCS", "CostObservation"),
        ("MAPPED_TO_APC", "HCPCS", "APC"),
        ("HAS_STD_AMOUNT", "MSDRG", "IPPSStandardizedAmount"),
        ("USES_CCR", "CostObservation", "CCR"),
        ("IN_COST_CENTER", "CCR", "CostCenter"),
        ("MAPS_FROM_ICD_CM", "GEMMapping", "ICDCMCode"),
        ("MAPS_FROM_ICD_PCS", "GEMMapping", "ICDPCSCode"),
        ("MAPS_TO_ICD_CM", "GEMMapping", "ICDCMCode"),
        ("MAPS_TO_ICD_PCS", "GEMMapping", "ICDPCSCode"),
        ("IS_REIMBURSEMENT_OF", "GEMMapping", "GEMMapping"),
    ]
    errors: list[str] = []
    for rel_key, start_label, end_label in pairs:
        rel_path = NEO4J_IMPORT / REL_CSV_NAMES[rel_key]
        start_path = NEO4J_IMPORT / NODE_CSV_NAMES[start_label]
        end_path = NEO4J_IMPORT / NODE_CSV_NAMES[end_label]
        start_id_col = f":START_ID({ID_SPACES[start_label]})"
        end_id_col = f":END_ID({ID_SPACES[end_label]})"
        start_node_id_col = f"id:ID({ID_SPACES[start_label]})"
        end_node_id_col = f"id:ID({ID_SPACES[end_label]})"

        # Use sniffed read with all columns as VARCHAR so DuckDB doesn't
        # try to coerce the typed columns (which break on the ":int" suffix
        # in headers when read back).
        rel_q = (
            f"read_csv_auto('{rel_path.as_posix()}', header=true, all_varchar=true)"
        )
        start_q = (
            f"read_csv_auto('{start_path.as_posix()}', header=true, all_varchar=true)"
        )
        end_q = (
            f"read_csv_auto('{end_path.as_posix()}', header=true, all_varchar=true)"
        )

        [(orphan_starts,)] = con.execute(
            f"""
            SELECT count(*) FROM {rel_q} r
            LEFT JOIN {start_q} s ON r."{start_id_col}" = s."{start_node_id_col}"
            WHERE s."{start_node_id_col}" IS NULL
            """
        ).fetchall()
        [(orphan_ends,)] = con.execute(
            f"""
            SELECT count(*) FROM {rel_q} r
            LEFT JOIN {end_q} e ON r."{end_id_col}" = e."{end_node_id_col}"
            WHERE e."{end_node_id_col}" IS NULL
            """
        ).fetchall()
        if orphan_starts or orphan_ends:
            errors.append(
                f"{rel_key}: {orphan_starts} orphan starts, {orphan_ends} orphan ends"
            )
        else:
            log.info("ref-integrity OK: %s", rel_key)
    if errors:
        for e in errors:
            log.error("ref-integrity FAIL: %s", e)
        raise RuntimeError(
            f"Referential integrity failed in {len(errors)} rel CSVs: " + "; ".join(errors)
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

ALL_BUILDERS: dict[str, Callable[[duckdb.DuckDBPyConnection], int]] = {
    # Nodes
    "ICDCMCode": build_nodes_icd_cm,
    "ICDPCSCode": build_nodes_icd_pcs,
    "HCPCS": build_nodes_hcpcs,
    "MSDRG": build_nodes_msdrg,
    "APC": build_nodes_apc,
    "IPPSStandardizedAmount": build_nodes_ipps_std_amount,
    "OPPSConversionFactor": build_nodes_opps_cf,
    "PFSConversionFactor": build_nodes_pfs_cf,
    "CostCenter": build_nodes_cost_center,
    "CCR": build_nodes_ccr,
    "GEMMapping": build_nodes_gem_mapping,
    "CostObservation": build_nodes_cost_observation,
    # Rels — keys mirror REL_CSV_NAMES keys.
    "CAN_ANCHOR": build_rels_can_anchor,
    "IS_MODAL_FOR": build_rels_is_modal_for,
    "OBSERVED_AS_MSDRG": build_rels_observed_as_msdrg,
    "OBSERVED_AS_HCPCS": build_rels_observed_as_hcpcs,
    "MAPPED_TO_APC": build_rels_mapped_to_apc,
    "HAS_STD_AMOUNT": build_rels_has_std_amount,
    "USES_CCR": build_rels_uses_ccr,
    "IN_COST_CENTER": build_rels_in_cost_center,
    "MAPS_FROM_ICD_CM": build_rels_maps_from_icd_cm,
    "MAPS_FROM_ICD_PCS": build_rels_maps_from_icd_pcs,
    "MAPS_TO_ICD_CM": build_rels_maps_to_icd_cm,
    "MAPS_TO_ICD_PCS": build_rels_maps_to_icd_pcs,
    "IS_REIMBURSEMENT_OF": build_rels_is_reimbursement_of,
}


def _register_udfs(con: duckdb.DuckDBPyConnection) -> None:
    """Register the sha1[:16] UDF used by CostObservation + OBSERVED_AS rels."""
    con.create_function("sha1_id", _sha1_id, ["VARCHAR"], "VARCHAR")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--label",
        help="Build a single CSV by builder key (e.g. MSDRG, CAN_ANCHOR). "
        "Default: build all.",
    )
    parser.add_argument(
        "--skip-referential-check",
        action="store_true",
        help="Skip the post-build referential-integrity check.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    NEO4J_IMPORT.mkdir(parents=True, exist_ok=True)

    t_start = time.perf_counter()
    con = duckdb.connect()
    _register_udfs(con)

    counts: dict[str, int] = {}
    if args.label:
        if args.label not in ALL_BUILDERS:
            raise SystemExit(
                f"Unknown label/rel '{args.label}'. "
                f"Valid keys: {sorted(ALL_BUILDERS)}"
            )
        counts[args.label] = _timed(args.label, lambda: ALL_BUILDERS[args.label](con))
    else:
        for key, fn in ALL_BUILDERS.items():
            counts[key] = _timed(key, lambda fn=fn: fn(con))

        if not args.skip_referential_check:
            log.info("running referential-integrity check across all rel CSVs...")
            t_ref = time.perf_counter()
            referential_integrity_check(con)
            log.info("referential-integrity OK in %.2fs", time.perf_counter() - t_ref)

    # Build summary maps {filename: row_count}; downstream importer reads it.
    name_map = {**NODE_CSV_NAMES, **REL_CSV_NAMES}
    summary = {name_map[k]: v for k, v in counts.items() if k in name_map}
    summary_path = NEO4J_IMPORT / "_build_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    elapsed = time.perf_counter() - t_start
    log.info("build_csvs done: %d CSVs, %.2fs total", len(counts), elapsed)
    log.info("summary: %s", summary_path)


if __name__ == "__main__":
    main()
