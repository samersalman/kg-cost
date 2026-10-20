// =============================================================================
// post_load.cypher — runs AFTER `neo4j-admin database import full` AND after
// `cypher-shell -f /cypher/schema.cypher` has applied the constraints.
// =============================================================================
//
// Runs sanity counts and four sample queries that any load failure would
// surface. Invoked from neo4j_import.sh as the second of two cypher-shell -f
// calls (schema.cypher first, then this file). Splitting avoids relying on
// cypher-shell's `:source` directive, which has ambiguous support in -f mode.
//
// File is mounted read-only at /cypher/post_load.cypher inside the container.
// =============================================================================

// ------------------------------------------------------------------
// 1. Wait for index population to complete before running counts.
//    Neo4j 5.x: CALL db.awaitIndexes(timeoutSeconds) — defaults to 300.
// ------------------------------------------------------------------
CALL db.awaitIndexes(300);

// ------------------------------------------------------------------
// 2. Sanity: counts by node label
// ------------------------------------------------------------------
CALL db.labels() YIELD label
CALL apoc.cypher.run('MATCH (n:`'+label+'`) RETURN count(n) AS c', {}) YIELD value
RETURN label, value.c AS count
ORDER BY count DESC;

// ------------------------------------------------------------------
// 3. Sanity: counts by relationship type
// ------------------------------------------------------------------
CALL db.relationshipTypes() YIELD relationshipType
CALL apoc.cypher.run('MATCH ()-[r:`'+relationshipType+'`]->() RETURN count(r) AS c', {}) YIELD value
RETURN relationshipType, value.c AS count
ORDER BY count DESC;

// ------------------------------------------------------------------
// 4. Sample queries (sanity-check that key edges actually wire up)
// ------------------------------------------------------------------

// Q1: ICD-10-CM I21.4 (acute MI subendocardial) modal MS-DRG, FY2024,
//     pulling allowed-payment distribution for that DRG.
MATCH (icd:ICDCMCode {code: 'I214', code_system: 'ICD-10-CM'})-[r:IS_MODAL_FOR]->(drg:MSDRG)
WHERE r.fy = 2024 AND drg.fy = 2024
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'allowed_payment', year: 2024})
RETURN drg.code AS msdrg, drg.title, obs.value AS allowed, obs.p10, obs.p90, obs.n
LIMIT 5;

// Q2: CPT 27447 (total knee arthroplasty), CY2024 PFS facility allowed.
MATCH (h:HCPCS {code: '27447'})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'pfs_facility' AND obs.concept = 'allowed_payment' AND obs.year = 2024
RETURN h.code, h.descriptor, obs.value, obs.source
LIMIT 5;

// Q3: All MS-DRGs reachable from any acute-MI ICD-10 code (I21*) in FY2024.
MATCH (icd:ICDCMCode)-[r:CAN_ANCHOR]->(drg:MSDRG)
WHERE icd.code STARTS WITH 'I21'
  AND r.fy = 2024
  AND drg.fy = 2024
RETURN DISTINCT drg.code, drg.title
ORDER BY drg.code
LIMIT 25;

// Q4: GEMs lookup — what does ICD-9-CM 410.41 (no decimal: '41041') map to in
//     ICD-10-CM (forward direction)?
MATCH (src:ICDCMCode {code: '41041', code_system: 'ICD-9-CM'})<-[:MAPS_FROM]-(gem:GEMMapping)-[:MAPS_TO]->(tgt:ICDCMCode)
WHERE gem.is_reimbursement = false AND tgt.code_system = 'ICD-10-CM'
RETURN tgt.code, tgt.description, gem.approximate_flag, gem.no_map_flag
LIMIT 10;

// post_load.cypher applied successfully
