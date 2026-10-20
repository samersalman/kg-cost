// =============================================================================
// schema.cypher — KG-COST Neo4j schema (constraints + indexes)
// =============================================================================
//
// Purpose:
//   Declares all unique-key constraints (one per node label) and lookup
//   indexes for the KG-COST Medicare Cost Knowledge Graph. Constraints are
//   idempotent (IF NOT EXISTS) so the script is safe to re-run.
//
// When invoked:
//   AFTER `neo4j-admin database import` — bulk import bypasses constraints,
//   so the schema must be applied post-load. Entry point is
//   `src/load/post_load.cypher`, which sources this file.
//
// Syntax:
//   Neo4j 5.x Cypher only. Uses the modern
//     `CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:Label) REQUIRE ... IS UNIQUE`
//   and
//     `CREATE INDEX <name> IF NOT EXISTS FOR (n:Label) ON (n.prop)`
//   forms; legacy `ON (...) ASSERT ...` syntax is NOT supported here.
//
// Coverage:
//   12 node-label constraints (one per :Label in the schema; the plan's
//   "11 total" wording undercounts by one — see return summary).
//   10 relationship types are declared in plan-v13 Step 3 but Neo4j 5.x
//   Community does not support relationship-uniqueness constraints, so
//   relationship integrity is enforced by the CSV builder (Step 4) instead.
//
//   Lookup indexes cover the highest-traffic Phase 10 query columns:
//   CostObservation.year, CostObservation.(concept, year), MSDRG.code,
//   HCPCS.descriptor, ICDCMCode.chapter — plus four code/setting helpers.
// =============================================================================

// === Code masters ===

CREATE CONSTRAINT icd_cm_pk IF NOT EXISTS
  FOR (n:ICDCMCode) REQUIRE (n.code, n.code_system) IS UNIQUE;

CREATE CONSTRAINT icd_pcs_pk IF NOT EXISTS
  FOR (n:ICDPCSCode) REQUIRE (n.code, n.code_system) IS UNIQUE;

CREATE CONSTRAINT hcpcs_pk IF NOT EXISTS
  FOR (n:HCPCS) REQUIRE n.code IS UNIQUE;

// === Year-stamped clinical aggregates ===

CREATE CONSTRAINT msdrg_pk IF NOT EXISTS
  FOR (n:MSDRG) REQUIRE (n.code, n.fy) IS UNIQUE;

CREATE CONSTRAINT apc_pk IF NOT EXISTS
  FOR (n:APC) REQUIRE (n.code, n.cy, n.quarter) IS UNIQUE;

// === Cost / standardization ===

CREATE CONSTRAINT cost_obs_pk IF NOT EXISTS
  FOR (n:CostObservation) REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT ipps_std_pk IF NOT EXISTS
  FOR (n:IPPSStandardizedAmount) REQUIRE n.fy IS UNIQUE;

CREATE CONSTRAINT opps_cf_pk IF NOT EXISTS
  FOR (n:OPPSConversionFactor) REQUIRE (n.cy, n.quarter) IS UNIQUE;

CREATE CONSTRAINT pfs_cf_pk IF NOT EXISTS
  FOR (n:PFSConversionFactor) REQUIRE (n.cy, n.kind) IS UNIQUE;

CREATE CONSTRAINT ccr_pk IF NOT EXISTS
  FOR (n:CCR) REQUIRE (n.fy, n.cost_center_group) IS UNIQUE;

CREATE CONSTRAINT cost_center_pk IF NOT EXISTS
  FOR (n:CostCenter) REQUIRE n.name IS UNIQUE;

// === GEMs ===

CREATE CONSTRAINT gem_pk IF NOT EXISTS
  FOR (n:GEMMapping) REQUIRE n.id IS UNIQUE;

// === Lookup indexes ===

CREATE INDEX cost_obs_year IF NOT EXISTS
  FOR (n:CostObservation) ON (n.year);

CREATE INDEX cost_obs_concept_year IF NOT EXISTS
  FOR (n:CostObservation) ON (n.concept, n.year);

CREATE INDEX cost_obs_setting IF NOT EXISTS
  FOR (n:CostObservation) ON (n.setting);

CREATE INDEX msdrg_code IF NOT EXISTS
  FOR (n:MSDRG) ON (n.code);

CREATE INDEX hcpcs_descriptor IF NOT EXISTS
  FOR (n:HCPCS) ON (n.descriptor);

CREATE INDEX icd_cm_chapter IF NOT EXISTS
  FOR (n:ICDCMCode) ON (n.chapter);

CREATE INDEX icd_cm_code IF NOT EXISTS
  FOR (n:ICDCMCode) ON (n.code);

CREATE INDEX icd_pcs_code IF NOT EXISTS
  FOR (n:ICDPCSCode) ON (n.code);

CREATE INDEX apc_code IF NOT EXISTS
  FOR (n:APC) ON (n.code);

// schema.cypher applied successfully
