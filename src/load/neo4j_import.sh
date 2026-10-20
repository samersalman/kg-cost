#!/usr/bin/env bash
# neo4j_import.sh — Phase 9 bulk import for KG-COST
#
# Pre-conditions:
#   - data/neo4j_import/ contains all node and relationship CSVs (output of
#     src/load/build_csvs.py)
#   - Docker daemon is running, neo4j/docker-compose.yml is valid
#   - First-run: container has been started at least once so /data is initialized
#
# Steps:
#   1. Stop the running Neo4j container (bulk import requires offline DB)
#   2. Run `neo4j-admin database import full neo4j` against /var/lib/neo4j/import/*.csv
#   3. Restart Neo4j and wait for bolt to bind
#   4. Apply post-load Cypher (constraints + sanity counts)
#
# Idempotency: --overwrite-destination=true makes step 2 wipe and rebuild the
# 'neo4j' database from scratch each invocation.
#
# Exits 0 on success. Exits non-zero on any step failure (set -euo pipefail).

set -euo pipefail

# Load env vars from repo-root .env if present (best-effort; export each KEY=VAL
# line that does not start with #).
_DOTENV="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.env"
if [[ -f "$_DOTENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$_DOTENV"
  set +a
fi

# Required for cypher-shell phases of the import. Defaults match the
# docker-compose stack so behavior is unchanged when .env is absent.
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-kgcostlocal}"

# Resolve script-relative project root so this works from any CWD
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/neo4j/docker-compose.yml"
IMPORT_DIR="$ROOT/data/neo4j_import"

cd "$ROOT"

# Sanity: refuse to proceed if no CSVs exist
if ! ls "$IMPORT_DIR"/nodes_*.csv >/dev/null 2>&1; then
  echo "[neo4j_import] FAIL: no node CSVs found in $IMPORT_DIR. Run build_csvs.py first." >&2
  exit 2
fi

echo "[neo4j_import] Stopping Neo4j (bulk import requires offline DB)..."
docker compose -f "$COMPOSE_FILE" stop neo4j 2>/dev/null || true

echo "[neo4j_import] Running neo4j-admin database import full..."
docker compose -f "$COMPOSE_FILE" run --rm --no-deps neo4j \
  neo4j-admin database import full neo4j \
    --overwrite-destination=true \
    --multiline-fields=true \
    --ignore-empty-strings=true \
    --skip-bad-relationships=false \
    --skip-duplicate-nodes=false \
    --nodes=ICDCMCode=/var/lib/neo4j/import/nodes_icd_cm.csv \
    --nodes=ICDPCSCode=/var/lib/neo4j/import/nodes_icd_pcs.csv \
    --nodes=HCPCS=/var/lib/neo4j/import/nodes_hcpcs.csv \
    --nodes=MSDRG=/var/lib/neo4j/import/nodes_msdrg.csv \
    --nodes=APC=/var/lib/neo4j/import/nodes_apc.csv \
    --nodes=IPPSStandardizedAmount=/var/lib/neo4j/import/nodes_ipps_std_amount.csv \
    --nodes=OPPSConversionFactor=/var/lib/neo4j/import/nodes_opps_cf.csv \
    --nodes=PFSConversionFactor=/var/lib/neo4j/import/nodes_pfs_cf.csv \
    --nodes=CostCenter=/var/lib/neo4j/import/nodes_cost_center.csv \
    --nodes=CCR=/var/lib/neo4j/import/nodes_ccr.csv \
    --nodes=GEMMapping=/var/lib/neo4j/import/nodes_gem_mapping.csv \
    --nodes=CostObservation=/var/lib/neo4j/import/nodes_cost_observation.csv \
    --relationships=CAN_ANCHOR=/var/lib/neo4j/import/rels_can_anchor.csv \
    --relationships=IS_MODAL_FOR=/var/lib/neo4j/import/rels_is_modal_for.csv \
    --relationships=OBSERVED_AS=/var/lib/neo4j/import/rels_observed_as_msdrg.csv \
    --relationships=OBSERVED_AS=/var/lib/neo4j/import/rels_observed_as_hcpcs.csv \
    --relationships=MAPPED_TO_APC=/var/lib/neo4j/import/rels_mapped_to_apc.csv \
    --relationships=HAS_STD_AMOUNT=/var/lib/neo4j/import/rels_has_std_amount.csv \
    --relationships=USES_CCR=/var/lib/neo4j/import/rels_uses_ccr.csv \
    --relationships=IN_COST_CENTER=/var/lib/neo4j/import/rels_in_cost_center.csv \
    --relationships=MAPS_FROM=/var/lib/neo4j/import/rels_maps_from_icd_cm.csv \
    --relationships=MAPS_FROM=/var/lib/neo4j/import/rels_maps_from_icd_pcs.csv \
    --relationships=MAPS_TO=/var/lib/neo4j/import/rels_maps_to_icd_cm.csv \
    --relationships=MAPS_TO=/var/lib/neo4j/import/rels_maps_to_icd_pcs.csv \
    --relationships=IS_REIMBURSEMENT_OF=/var/lib/neo4j/import/rels_is_reimbursement_of.csv

echo "[neo4j_import] Import complete. Restarting Neo4j..."
docker compose -f "$COMPOSE_FILE" up -d neo4j

# Wait for bolt to bind (cypher-shell ping)
echo "[neo4j_import] Waiting for bolt to bind..."
for i in {1..30}; do
  if docker compose -f "$COMPOSE_FILE" exec -T neo4j \
       cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" 'RETURN 1' >/dev/null 2>&1; then
    echo "[neo4j_import] Bolt is up after ${i} attempts."
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "[neo4j_import] FAIL: bolt did not bind within 5 minutes." >&2
    exit 3
  fi
  sleep 10
done

echo "[neo4j_import] Applying schema (constraints + indexes)..."
docker compose -f "$COMPOSE_FILE" exec -T neo4j \
  cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" -f /cypher/schema.cypher

echo "[neo4j_import] Running post-load sanity queries..."
# Run schema.cypher first so post_load.cypher does not need a :source
# directive (which has ambiguous support in cypher-shell -f mode).
docker compose -f "$COMPOSE_FILE" exec -T neo4j \
  cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" -f /cypher/post_load.cypher

echo "[neo4j_import] DONE."
