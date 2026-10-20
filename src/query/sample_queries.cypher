// =============================================================================
// sample_queries.cypher — 8 named, parameterized Cypher queries for the
// KG-COST graph. Parsed at runtime by ``src/query/_bolt.py:read_query`` via
// the header-comment convention:
//
//   // === Q{N}_<snake_name> ===
//   // :param <name> => <python literal default>
//   <cypher body, terminated by ';'>
//
// Defaults are valid Python literals — strings quoted with ``'...'``, ints
// unquoted — coerced via ``ast.literal_eval``. The query body ends at the
// next ``// ===`` marker or EOF; every body MUST end with a semicolon.
//
// Style mirrors ``src/load/post_load.cypher``: stripped-decimal ICD codes,
// explicit year filters on every relationship + observation node, OPTIONAL
// MATCH for the cost layer so missing-cost rows do not silently drop the
// upstream diagnostic node, and every RETURN column aliased with ``AS`` so
// caller dict keys are clean (no ``obs.value`` keys).
// =============================================================================


// === Q1_icd_modal_cost ===
// :param icd_code => 'I214'
// :param fy => 2024
MATCH (icd:ICDCMCode {code: $icd_code, code_system: 'ICD-10-CM'})-[r:IS_MODAL_FOR]->(drg:MSDRG)
WHERE r.fy = $fy AND drg.fy = $fy
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'allowed_payment', year: $fy})
RETURN drg.code AS msdrg,
       drg.title AS title,
       obs.value AS allowed,
       obs.p10 AS p10,
       obs.p90 AS p90,
       obs.n AS n;


// === Q2_cpt_pfs_facility ===
// :param cpt => '27447'
// :param cy => 2024
MATCH (h:HCPCS {code: $cpt})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'pfs_facility'
  AND obs.concept = 'allowed_payment'
  AND obs.year = $cy
RETURN h.code AS cpt,
       h.descriptor AS descriptor,
       obs.value AS allowed,
       obs.source AS source;


// === Q3_hcpcs_apc_drilldown ===
// :param hcpcs => '27447'
// :param cy => 2024
MATCH (h:HCPCS {code: $hcpcs})-[r:MAPPED_TO_APC]->(apc:APC)
WHERE r.cy = $cy AND apc.cy = $cy
RETURN DISTINCT h.code AS hcpcs,
       apc.code AS apc,
       apc.apc_title AS apc_title,
       apc.payment_rate AS payment_rate
ORDER BY apc, payment_rate DESC;


// === Q4_icd_can_anchor_all ===
// :param icd_code => 'I214'
// :param fy => 2024
MATCH (icd:ICDCMCode {code: $icd_code, code_system: 'ICD-10-CM'})-[r:CAN_ANCHOR]->(drg:MSDRG)
WHERE r.fy = $fy AND drg.fy = $fy
RETURN DISTINCT drg.code AS msdrg,
       drg.title AS title
ORDER BY msdrg
LIMIT 50;


// === Q5_gems_forward ===
// :param icd9 => '41041'
MATCH (src:ICDCMCode {code: $icd9, code_system: 'ICD-9-CM'})<-[:MAPS_FROM]-(gem:GEMMapping)-[:MAPS_TO]->(tgt:ICDCMCode)
WHERE gem.is_reimbursement = false
  AND tgt.code_system = 'ICD-10-CM'
RETURN tgt.code AS target,
       tgt.description AS description,
       gem.approximate_flag AS approximate_flag,
       gem.no_map_flag AS no_map_flag,
       gem.combination_flag AS combination_flag,
       gem.scenario AS scenario,
       gem.choice_list AS choice_list
LIMIT 25;


// === Q6_drg_reverse_icds ===
// :param drg => '872'
// :param fy => 2024
MATCH (drg:MSDRG {code: $drg, fy: $fy})<-[r:CAN_ANCHOR]-(icd:ICDCMCode)
WHERE r.fy = $fy
RETURN icd.code AS icd_code,
       icd.code_system AS code_system,
       icd.description AS description
ORDER BY icd_code
LIMIT 50;


// === Q7_ccr_adjusted_cost ===
// :param icd_code => 'I214'
// :param fy => 2024
MATCH (icd:ICDCMCode {code: $icd_code, code_system: 'ICD-10-CM'})-[r:IS_MODAL_FOR]->(drg:MSDRG)
WHERE r.fy = $fy AND drg.fy = $fy
MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'ccr_cost', year: $fy})
OPTIONAL MATCH (obs)-[:USES_CCR]->(ccr:CCR)
RETURN drg.code AS msdrg,
       drg.title AS title,
       obs.value AS ccr_cost,
       obs.ccr_value AS ccr_value,
       obs.ccr_cost_center AS cost_center;


// === Q8_cost_distribution_by_year ===
// :param code => '27447'
// :param setting => 'pfs_facility'
// :param concept => 'allowed_payment'
// :param fy_min => 2019
// :param fy_max => 2026
MATCH (n)-[:OBSERVED_AS]->(obs:CostObservation)
WHERE n.code = $code
  AND (n:HCPCS OR n:MSDRG)
  AND obs.setting = $setting
  AND obs.concept = $concept
  AND obs.year >= $fy_min
  AND obs.year <= $fy_max
RETURN obs.year AS year,
       obs.value AS allowed,
       obs.p10 AS p10,
       obs.p90 AS p90,
       obs.n AS n
ORDER BY year;
