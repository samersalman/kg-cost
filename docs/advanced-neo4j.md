# Advanced: Neo4j cheat-sheet

Focused subset of `RUNBOOK.md` for users who want to run the KG-COST graph in Neo4j. **RUNBOOK.md is authoritative** for the full operational playbook (import modes, env switches, troubleshooting, mode-switch recipes). This file is the 5-minute orientation.

## Why use Neo4j

For 95% of cost lookups you do **not** need Neo4j. The parquet backend (`data/gold/*.parquet`) returns identical values for single-code (`src.query.lookup`) and cohort-level (`src.query.cohort_annotator --source parquet`) queries. Reach for Neo4j only when you need custom multi-hop graph traversals — e.g. exploring ICD → DRG → CCR provider geometry, walking GEMs forward/backward bridges, or running ad-hoc Cypher against `CostObservation` provenance.

Bolt-vs-parquet numeric parity is asserted at 0.0% drift across 7 numeric columns × 30 fixture rows (see `tests/test_cohort_annotator.py`).

## Starting the graph

Two supported modes (selected via `KG_COST_IMPORT_MODE` in `.env`):

### Docker (default)

```bash
# Start: Neo4j 5.20-community + APOC, local-only password kgcostlocal
docker compose -f neo4j/docker-compose.yml up -d
# Logs
docker compose -f neo4j/docker-compose.yml logs -f neo4j
# Stop (keeps data volume)
docker compose -f neo4j/docker-compose.yml stop neo4j
# Drop graph + restart fresh
docker compose -f neo4j/docker-compose.yml down -v && \
  docker compose -f neo4j/docker-compose.yml up -d
```

Bolt: `bolt://localhost:7687` — auth `neo4j/kgcostlocal`. Browser: `http://localhost:7474`.

`.env`:
```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=kgcostlocal
NEO4J_DATABASE=neo4j
KG_COST_IMPORT_MODE=docker
KG_COST_RUN_NEO4J=1   # opt the live-Neo4j smoke tests into pytest
```

### Local-admin (Neo4j Desktop, no Docker)

Point `neo4j-admin` at a local Neo4j Desktop 2026.04.x install. See RUNBOOK.md §"Switching Neo4j targets" for the full setup recipe (UUID discovery, version probe, APOC verification).

`.env`:
```bash
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<your-neo4j-password>
NEO4J_DATABASE=neo4j
KG_COST_IMPORT_MODE=local-admin
KG_COST_NEO4J_HOME=<path-to-neo4j-dbms-directory>/dbms-<UUID>
KG_COST_RUN_NEO4J=1
```

## Loading data

Single orchestrator handles build → import → schema → smoke for both modes:

```bash
# Build the 12 node CSVs + 13 rel CSVs (DuckDB, ~4 sec)
python3 -m src.load.build_csvs

# Full end-to-end import (probes backend, builds-if-stale, neo4j-admin import,
# applies schema + post-load Cypher, runs 4 smoke queries, verifies ±5%)
python3 -m src.load.run_import
```

Useful flags: `--check-only` (probe only), `--skip-build` (reuse CSVs), `--skip-import` (re-run queries only).

Outputs land at `data/neo4j_import/_load_summary.json` + `_smoke_results.json`.

Load order (handled by `src/load/neo4j_import.sh` + `src/load/run_import.py`):

1. `build_csvs.py` writes nodes and relationships under `data/neo4j_import/`.
2. `neo4j-admin database import full` ingests CSVs offline.
3. `cypher-shell -f /cypher/schema.cypher` declares 12 unique constraints + 9 indexes.
4. `cypher-shell -f /cypher/post_load.cypher` applies post-load fixups.
5. Sample-query smoke verifies live counts within ±5% of `_build_summary.json`.

Manual schema apply (after `docker compose up -d`):
```bash
docker compose -f neo4j/docker-compose.yml exec -T neo4j \
  cypher-shell -u neo4j -p kgcostlocal -f /cypher/schema.cypher
docker compose -f neo4j/docker-compose.yml exec -T neo4j \
  cypher-shell -u neo4j -p kgcostlocal -f /cypher/post_load.cypher
```

## Example Cypher queries

The canonical eight live in `src/query/sample_queries.cypher` and are runnable from Python via `src.query._bolt`. Five most useful:

**Q1 — ICD modal DRG cost (year-stamped):**
```cypher
MATCH (icd:ICDCMCode {code: 'I214', code_system: 'ICD-10-CM'})-[r:IS_MODAL_FOR]->(drg:MSDRG)
WHERE r.fy = 2024 AND drg.fy = 2024
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'allowed_payment', year: 2024})
RETURN drg.code AS msdrg, drg.title, obs.value AS allowed, obs.n AS n;
```

**Q2 — CPT PFS facility allowed payment:**
```cypher
MATCH (h:HCPCS {code: '27447'})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'pfs_facility' AND obs.concept = 'allowed_payment' AND obs.year = 2024
RETURN h.code AS cpt, h.descriptor, obs.value AS allowed, obs.source;
```

**Q3 — HCPCS to APC drilldown:**
```cypher
MATCH (h:HCPCS {code: '27447'})-[r:MAPPED_TO_APC]->(apc:APC)
WHERE r.cy = 2024 AND apc.cy = 2024
RETURN DISTINCT h.code AS hcpcs, apc.code AS apc, apc.apc_title, apc.payment_rate
ORDER BY apc, payment_rate DESC;
```

**Q4 — All DRGs an ICD principal can anchor:**
```cypher
MATCH (icd:ICDCMCode {code: 'I214', code_system: 'ICD-10-CM'})-[r:CAN_ANCHOR]->(drg:MSDRG)
WHERE r.fy = 2024 AND drg.fy = 2024
RETURN DISTINCT drg.code AS msdrg, drg.title ORDER BY msdrg LIMIT 50;
```

**Q5 — GEMs forward bridge (ICD-9 → ICD-10):**
```cypher
MATCH (src:ICDCMCode {code: '41041', code_system: 'ICD-9-CM'})
       <-[:MAPS_FROM]-(gem:GEMMapping)-[:MAPS_TO]->(tgt:ICDCMCode)
WHERE gem.is_reimbursement = false AND tgt.code_system = 'ICD-10-CM'
RETURN tgt.code AS target, tgt.description, gem.approximate_flag, gem.no_map_flag;
```

Python invocation:
```python
from src.query._bolt import get_driver, run_query, list_queries
with get_driver() as drv:
    rows = run_query(drv, "Q1_icd_modal_cost", icd_code="I214", fy=2024)
```

## Troubleshooting

- **`run_import.py` exits 0 with a resume message:** Docker daemon is unreachable. macOS: `open -a Docker` then re-run. Linux: `sudo systemctl start docker` (or `colima start`) then re-run.
- **Live-bolt tests skip:** set `KG_COST_RUN_NEO4J=1` and ensure either `docker info` works (docker mode) or `KG_COST_NEO4J_HOME` is set + valid (local-admin mode).
- **Java version conflict (local-admin mode):** Neo4j Desktop ships Zulu 21; the Phase 4 grouper runs on Java 17. `_run_local_admin_import` isolates `JAVA_HOME`/`PATH` to the subprocess `env=` dict so the shell `java` stays at 17. No action required.
- **NaN in CSV imports:** fixed in Session 10.5 — `build_nodes_cost_observation` wraps DOUBLE projections in `IF(isnan(x), NULL, x)`. If you hit it on a fresh build, rebuild CSVs (`python3 -m src.load.build_csvs`).
- **Bulk import OOM:** raise `dbms.memory.heap.max_size` in `neo4j/docker-compose.yml`.

For everything else (mode switching mid-session, resume semantics, `--overwrite-destination` behavior, `KG_COST_NEO4J_HOME` discovery on different Desktop layouts), see `RUNBOOK.md` §"Switching Neo4j targets" and §"Phase 8/9 — Neo4j schema + bulk import".
