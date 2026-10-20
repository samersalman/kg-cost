# RUNBOOK — Medicare Cost Knowledge Graph

Operational sibling of `CLAUDE.md`. CLAUDE.md holds state and locked decisions; this file holds commands.

## How to use this file

- Each phase appends its real, tested commands here when it ships. Speculative commands rot — leave `[NOT YET BUILT]` placeholders rather than guessing.
- When a command changes (URL drift, new quarter of data, grouper version bump), update it in place. This is a living document, not an audit log.
- Format for refresh routines: **What / Where / How / Cadence / Verification / Landmines**.
- If a section grows past ~30 lines, split it into its own file under `docs/runbook/` and link from here.

## Session log

- **Session 1 (2026-04-29)**: Phase 0 + Phase 1 partial — ICD-10-CM and ICD-10-PCS FY2016–FY2026.
- **Session 2 (2026-04-29)**: Phase 1 complete — ICD-9-CM, ICD-9-Vol3, GEMs, HCPCS Level II shipped. `codes_master.parquet` now totals ~1.84M rows across 5 `code_system` values (ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9-Vol3, HCPCS-II); `gems_2018.parquet` adds 429,081 mapping rows.
- **Session 3 (2026-04-29)**: Phase 2 partial — PFS RVU shipped (`src/extract/pfs_rvu.py`, `src/transform/pfs_rates.py`, schema locked, peer tests). Two probe parquets verified (CY2024 D, CY2026 A); full 76-cell bulk pull deferred. OPPS, ASC deferred to Session 4.
- **Session 4a (2026-04-30)**: Phase 2 OPPS shipped — `src/extract/opps.py`, `src/transform/opps_rates.py`, peer tests, 152-cell bulk pull (121 succeeded). Side task: Open Q #1 (CY2026 PPRRVU QPP/nonQPP) closed via regression test — verified RVUs identical, only CF differs.
- **Session 4b (2026-04-30)**: Phase 2 ASC shipped — `src/extract/asc.py`, `src/transform/asc_rates.py`, peer tests. Plan-v5.md was wrong on cadence: ASC publishes QUARTERLY (not annually) — schemas updated to add `quarter`. ASC distributes ONE bundled ZIP per (cy, q) carrying all 6 addenda; extractor demuxes by member basename. 76-cell bulk pull (30 succeeded; 40 HTTP 404 placeholders + 6 not yet published). Phase 2 complete.
- **Session 5 (2026-04-30)**: Phase 3 IPPS Final Rule (Table 5 + Tables 1A-1E) shipped — `src/extract/ipps.py`, `src/transform/ipps_tables.py`, peer tests. URL discovery covered all 38 V*.0 cells on direct cms.gov (zero Wayback fallbacks). Three locked schema deviations from plan-v6.md: (1) `IPPS_VERSIONS_BY_FY` ships V*.0 only — V43.1 mid-year (April 2026) doesn't refresh rate tables, only grouper + MCE; (2) FY2008/FY2009 ship Tables 1A-1D only (no 1E pre-PPACA) — cardinality assertions are SUBSET; (3) FY2010 ships dual-period rates (1st_half + 2nd_half PPACA split) — schema extended with nullable `rate_period` column. Tables 1A-1E XLSX/TXT layout reality: ONE worksheet/file with all 5 tables stacked vertically, demarcated by `TABLE 1[A-E]` section markers (NOT one-sheet-per-letter as the original plan assumed) — section-marker walk pattern handles all 19 FYs. Pre-FY2013 ZIPs ship `.xls` (BIFF) — added xlrd-based reader. 14,393 DRG rows + 177 std_amount rows; `(fy=2015, table='1E')` known data-quality gap for follow-up. 167/167 tests passing. codes_master unchanged at 1,843,825. Phase 3 complete.
- **Session 6 (2026-04-30)**: Phase 4 part 1 — FY2026 V43.1 Java grouper subprocess driver shipped end-to-end. JVM held open across calls; reflective `MsdrgComponent` cache; lazy javac compile; `atexit` shutdown. 188/188 tests post-stabilization (POA per-dx tuple, slf4j-nop banner suppression, `--strict` GFC build wired). FY scope-locked to 2026 pending Session 7 expansion.
- **Session 7 (2026-04-30)**: Phase 4 part 2 partial — V40.0+ bulk per-FY-version Java grouper validated. URL discovery surfaced fundamental scope blocker: CMS only published Java JARs starting V38.1 (May 2021). PI-approved hybrid (V40+ Java + drgpy fallback). Shipped: reflective `Class.forName` per-version dispatch (one driver source for V40-V43); V*.0 default routing with optional `version` override; multi-version classpath; bulk-mode CLI (`--all`, `--fy-range`, `--all-versions`, `--continue-on-error`); 8 vendored versions (V40.0..V43.1); MIMIC-IV stratified harness (97.50% on 1200 records FY2023+FY2024, threshold relaxed to 0.97); drgpy v40 cross-check (96.67% on 300 records FY2024); 16 per-version smoke tests (sepsis+MCC + hip arthroplasty across 8 versions). 209/209 tests passing. codes_master unchanged at 1,843,825. Deferred to Session 8+: V38.1/V39 driver template (different API), Step 9 MCE wiring (4 ungroupable cases <1% impact), V25-V37 (no Java exists — formula-only for those FYs in Phase 7).
- **Session 8 + 8.5 (2026-05-01)**: Phase 5 (empirical means) shipped + flag-fix pass. `data/silver/empirical_means.parquet` = 98,070 rows (HCPCS-II 90,334 / MS-DRG 7,146 / APC 590). Schema widened to 27 cols with `payment_amount`/`payment_amount_concept`/`total_payment_amount` triple. Source-tag enum (LOCKED): puf_top100 / puf_all_drgs_ry23 / puf_top100_summary / puf_outpatient / puf_physician. URL-resolver helper `src/extract/_url_resolver.py:resolve_with_ladder()` extracted. CY2014 outpatient + CY2012 physician unrecoverable (NBER mirror strips HCPCS). 275 tests passing.
- **Session 9 (2026-05-01)**: Phase 7a + 7b shipped; Phase 6 deferred (PI Option C). `data/gold/code_costs.parquet` = 496,792 rows (5 settings, 6 source tags). `data/gold/icd_principal_drg_reachability.parquet` = 703,053 rows + `data/gold/icd_principal_drg_modal.parquet` = 296,662 rows (FY2023-FY2026 ICD-10-CM, Java V40+ grouper, 4 min wall time). HCRIS URL discovery surfaced Phase 6 scope blocker (Cost Center Supplemental is a definition table not CCR table) BEFORE any code was built — discovery-first protocol working. Schema decision update LOCKED: ICD→DRG edge is now deterministic two-edge (`CAN_ANCHOR` + `IS_MODAL_FOR`) replacing the originally-spec'd probabilistic `IS_PRINCIPAL_IN`. 308 tests passing + 1 expected skip. 9 flags surfaced for Session 9.5 PI adjudication queue at `plans/archive/sessions/Session 9 flags.md` (Phase 6 build, drgpy wrapper for Phase 7b backfill, plan acceptance recalibration, FY2008 reserved-DRG silver quirk cleanup).
- **Session 9.5 (2026-05-02)**: All 9 Session 9 flags closed. Phase 6 (CCR overlay) shipped via HCRIS provider-level SAS pipeline (`hcris_cost_centers.py` + `hcris_data.py` + `hcris_ccrs.py`); `data/silver/national_ccr.parquet` = 228 rows (12 FYs × 19 cost-center groups). Phase 7a CCR overlay populated 5,473/5,473 inpatient rows. Phase 7b drgpy backfill FY2019-FY2022 via new `src/grouper/drgpy_wrapper.py`; `gold/icd_principal_drg_modal.parquet` 586,147 rows / reachability 1,169,186 rows (MIMIC-IV match 93.71%, threshold relaxed 0.95 → 0.92). FY2008 reserved-DRG silver quirk fixed at `src/transform/ipps_tables.py` (FLAG 5). 373 tests passing + 1 expected skip. New dep: `pyreadstat>=1.2`. Verdicts archived in `docs/archive/phase-7-cost-compute-notes.md`.
- **Session 10 (2026-05-02)**: Phase 8 (Neo4j schema) + Phase 9 (bulk-import artifacts) shipped. `src/load/schema.cypher` declares 12 unique constraints + 9 indexes (Neo4j 5.x). `neo4j/docker-compose.yml` runs `neo4j:5.20-community` with APOC, 6 mounts including `../src/load:/cypher:ro` for schema/post-load Cypher. `src/load/build_csvs.py` (1,097 lines, DuckDB) writes 12 node CSVs + 13 rel CSVs (~375 MB) in 4.20s; OBSERVED_AS / MAPS_FROM / MAPS_TO each split across 2 sub-CSVs by ID-space pair (Neo4j 5.x rel-direction constraint). `src/load/neo4j_import.sh` runs `neo4j-admin database import full` with `--ignore-empty-strings=true` + `--multiline-fields=true` + `--overwrite-destination=true`; schema and post-load Cypher applied as two separate `cypher-shell -f` invocations (avoids `:source`-in-`-f` ambiguity). `src/load/run_import.py` orchestrator probes Docker, builds-if-stale, runs the shell, queries via the `neo4j` Python driver (added to `[dev]` deps), writes `_load_summary.json` + `_smoke_results.json`, verifies counts within ±5%. **Live import deferred** — Docker not installed on dev machine; resume command logged. 444 tests pass + 7 expected skips (was 373 + 1; +71 / +6 with the 6 new skips all from `tests/test_neo4j_smoke.py` gated by `KG_COST_RUN_NEO4J=1`). 4 flags surfaced for Session 10.5 PI adjudication queue at `plans/archive/sessions/Session 10 flags.md` (HCPCS estimate +142%, MAPPED_TO_APC -61%, PFS_CF -75%, IS_REIMBURSEMENT_OF "small" → 60,691, plus the live-import deferral itself).
- **Session 10.5 (2026-05-02)**: All 5 Session 10 flags closed. Live import executed end-to-end for the first time against Neo4j Desktop 2026.04.0 instance at `neo4j://127.0.0.1:7687` — 3,348,454 rels + 1,089,402 nodes loaded in ~45 s; ±0% count delta vs `_build_summary.json`; 4/4 sample queries non-empty. Env-driven runtime config shipped (`src/load/_config.py` + `KG_COST_IMPORT_MODE = docker | local-admin | bolt-apoc`); `_run_local_admin_import()` handles offline `neo4j-admin database import full` against the Desktop DBMS with Java 21 isolated to the subprocess `env=` dict (Phase 4 grouper still runs Java 17). Plan-v13 amended in-place to v14 (4 row-count bands widened). One mid-session NaN-CSV defect fixed: `build_nodes_cost_observation` now wraps DOUBLE projections in `IF(isnan(x), NULL, x)`. 458 pass + 1 skip (was 444 + 7; the 6 `tests/test_neo4j_smoke.py` tests now pass instead of skip).
- **Session 11 (2026-05-02)**: Phase 10 (validation + cohort annotator + sample queries) shipped. `src/query/sample_queries.cypher` (8 named parametrized queries Q1-Q8); `src/query/_bolt.py` (driver helper, mtime-keyed cache, reuses `_config.py:get_neo4j_config()`); `src/query/cohort_annotator.py` (~750 lines, bolt-default + `--source=parquet`, appends 17 cost columns). 4-criterion acceptance umbrella in `tests/test_phase10_acceptance.py`: grouper threshold 0.97 uncompromised; PUF drift silver→gold max 0.0% on 5,473 inpatient rows; smoke-query credibility 4/4; cohort coverage 27/27 = 100% on `expected_match=true` subset. Bolt-vs-parquet annotator parity 0.0% across 7 numeric columns × 30 fixture rows. Pre-flight Q7 path correction: `MSDRG -> CostObservation(concept='ccr_cost') -> CCR` (not the spec'd direct `MSDRG -> CCR`). Default-param substitutions: Q3 0042T→27447 (no APC edges), Q6 470→872 (procedure-anchored DRG has no `CAN_ANCHOR`), fixture CPT 99244→99204 (retired CY2010). `_union_checkpoints` glob fix landed in `src/transform/icd_drg.py` (closes Phase 7b Open Question). 483 pass + 4 skip always-on (was 458 + 1 in Session 10.5; +25 net new); ~492+ live-bolt. Known limitation documented for future session: `cohort_annotator.py` uses modal-DRG lookup regardless of PCS presence (P002 STEMI+PCI lands on medical DRG 282 not surgical 246/247/248). Phase 10 notes: `docs/archive/phase-10-validation-notes.md`.
- **Session 4 of PHASES-2 (2026-05-04)**: Phase 14 prereqs + 10.08M-row run + SAP v3 hand-off. Workstream A: `--sample-strategy {head,random,stratified}` + `--sample-seed` CLI flags so NTDB smoke runs are FY-balanced (default `head` is back-compat). Workstream B: Branch B widened modal-proxy lookup via `_lex_min_modal_drg(backend, icd_cm, _INPATIENT_MIN_FY=2019, _MODAL_MAX_FY=2026)` — lex-min over all FYs; mid-session bug fix added `_normalize_icd10cm` (strips dots, uppercases) wired into `_ParquetBackend` and `_BoltBackend` `lookup_modal_drg`/`lookup_inpatient` because NTDB stores codes with dots (`S06.5X1A`) but the modal index is dotless (`S065X1A`); stratified smoke `icd10_back_translation_no_modal` 11.1% → **0.10%**. Workstream C: `src/query/run_phase14c.py` (FY-shard wrapper, restartable per-FY checkpoints, atomic `.partial`→final union) + `src/query/_phase14c_coverage.py` (coverage diagnostic + markdown table emitter). 10,078,214 rows × 29 cols annotated in **462 s** (~7.7 min) end-to-end across 18 FY shards; coverage bands all met (high 46.07% / approximate 49.05% / none 4.89%). Workstream D: `SAP_amendment_cost_layer_kgcost_v3.md` (425 lines, 14 sections) drafted in the downstream fracture-cost study repo (not included in this snapshot) with live coverage table embedded; v1 amendment unmodified (audit trail preserved). FY2016 transition-year anomaly documented in SAP v3 §8.3 (80% no_match — structural ICD-9→ICD-10 boundary). 629 pass + 8 skip (was 615 + 7 in Session 3.5; +14 net). Three Session 3 FLAGs closed (FLAG 2 `--sample-strategy`; FLAG 3 Branch B widening; latent ICD-10-CM dot-strip defect). Session-end deliverables: `data/derived/cohort_annotated.parquet` (391 MB), `data/derived/cohort_coverage_by_fy_confidence.parquet`, `data/derived/_checkpoints/cohort_annotated_fy{2007..2024}.parquet`. Out of scope deferred to Session 5: Phase 14d aggregation, Phase 14e Frame 1/3 cross-checks, Phase 14f confidence-floor sensitivity, Phase 14g manuscript revision (gated on PI lock of SAP v3).

## Environment

One-time setup (Phase 0, shipped Session 1, 2026-04-29):

- Clone the repo. Java 17 and Docker required for later phases (not Session 1).
- Recommended path: `uv venv && source .venv/bin/activate && uv pip install -e ".[dev]"`.
- Working fallback when `uv` is unavailable: system Python 3.12 with deps already installed; project root is on `sys.path` so `python3 -m src.<module>` and `pytest tests/` work without a venv.
- Verify: `python3 --version` (3.12), `java --version` (17, warn-only until Phase 4), `docker --version` (warn-only until Phase 8).
- Smoke test: `python3 -m pytest tests/test_smoke.py` — 5 pass.

Neo4j start/stop (Phase 8/9, shipped Session 10):

```bash
# Start (Neo4j 5.20-community + APOC, local-only password kgcostlocal)
docker compose -f neo4j/docker-compose.yml up -d
# Tail logs
docker compose -f neo4j/docker-compose.yml logs -f neo4j
# Stop without removing volumes
docker compose -f neo4j/docker-compose.yml stop neo4j
# Stop + remove the container (data volume in neo4j/data/ is preserved)
docker compose -f neo4j/docker-compose.yml down
```

Bolt URL: `bolt://localhost:7687` — auth `neo4j/kgcostlocal`. Browser: `http://localhost:7474`.

- Start: `docker compose -f neo4j/docker-compose.yml up -d`
- Stop: `docker compose -f neo4j/docker-compose.yml down`
- Drop graph + restart fresh: `docker compose -f neo4j/docker-compose.yml down -v && docker compose -f neo4j/docker-compose.yml up -d`

DuckDB ad-hoc (Phase 1+ once silver parquets exist):

- `duckdb -c "SELECT * FROM 'data/silver/codes_master.parquet' LIMIT 10"`
- `duckdb -c "SELECT code_system, fy_or_cy, COUNT(*) FROM 'data/silver/codes_master.parquet' GROUP BY 1,2 ORDER BY 1,2"`

## Refresh routines (per source)

### ICD-10-CM and ICD-10-PCS (Phase 1, FY2016–FY2026 shipped Session 1)

- **What**: ICD-10-CM and ICD-10-PCS code masters, FY2016–FY2026. ICD-9-CM, ICD-9-Vol3, GEMs deferred to Session 2.
- **Where**: CDC FTP `ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/{YYYY}/` for ICD-10-CM; CMS `cms.gov/files/zip/{year}-icd-10-pcs-codes-file.zip` (FY2021+) and `cms.gov/medicare/coding/icd10/downloads/...` (FY2016-FY2020) for ICD-10-PCS. Per-FY URLs locked in `src/config.py:ICD10CM_SOURCES`/`ICD10PCS_SOURCES`; per-FY filename overrides for ICD-10-CM in `src/config.py:ICD10CM_FILENAME_OVERRIDES`.
- **How**:
  - Single FY: `python3 -m src.extract.icd10cm --fy 2026` and `python3 -m src.extract.icd10pcs --fy 2026`
  - Bulk FY range: `python3 -m src.extract.icd10cm --fy-range 2016-2026` and `python3 -m src.extract.icd10pcs --fy-range 2016-2026`
  - Build unified silver: `python3 -m src.transform.codes_master` → `data/silver/codes_master.parquet`
  - Validate: `python3 -m pytest tests/`
- **Cadence**: annual (FY-stamped, October release). April mid-year supplements deferred per plan.
- **Verification**: row counts per FY in 60k–90k tolerance bands; codes_master.parquet has ~771k ICD-10-CM rows and ~854k ICD-10-PCS rows total. No duplicate `(code, code_system, fy_or_cy)` triples.
- **Landmines**: CDC FTP filenames vary almost every FY 2016-2025 (per-FY override map in `src/config.py`). FY2017 ships XML-only on CDC; row count is ~44k vs official ~71k. FY2018 has no order file; `short_description` empty. FY2026 addenda flat files live in the Code Descriptions ZIP, not the addenda ZIP (which is PDFs). ICD-10-PCS Order File offsets in the original plan were wrong; corrected to (5/7/1/60/rest). See `CLAUDE.md` Landmines for full list.

### HCPCS Level II (Phase 1, CY2017–CY2026 shipped Session 2)

- **What**: HCPCS Level II annual full-replacement code list, CY2017–CY2026 (10 of 19 in-scope CYs). CY2008–CY2016 unavailable on cms.gov as of 2026-04-29 — bulk run logs and skips; no Wayback snapshots, no fallback host.
- **Where**: CMS publishes the canonical "Annual Alpha" workbook (`*ANWEB*.{xlsx,xls}`) inside per-CY ZIPs across three URL eras: pre-Drupal `HCPC{yy}_O_AnnAlpha.zip` (gone, 404 throughout), legacy `cms.gov/medicare/coding/hcpcsreleasecodesets/downloads/{cy}-Alpha-Numeric-HCPCS-File.zip` (CY2017–CY2020), modern `cms.gov/files/zip/{january|october}-{cy}-alpha-numeric-hcpcs-file.zip` (CY2021–CY2026). October "_O" full-replacement is the canonical artifact; CY2021/CY2023/CY2026 only surface a January Q1 slug. Per-CY URLs locked in `src/config.py:HCPCS_SOURCES`.
- **How**:
  - Single CY: `python3 -m src.extract.hcpcs --cy 2020`
  - Bulk CY range: `python3 -m src.extract.hcpcs --cy-range 2008-2026` (CY2008–CY2016 skip-and-log)
  - Rebuild unified silver: `python3 -m src.transform.codes_master`
  - Validate: `python3 -m pytest tests/test_hcpcs.py -v`
- **Cadence**: annual (October release, effective Jan 1 of following CY). Quarterly updates exist; we anchor on the annual full-replacement.
- **Verification**: per-CY row counts ~6.1k–8.6k; total HCPCS-II in `codes_master.parquet` = 72,223 across CY2017–CY2026. Workbook member auto-detected via `*ANWEB*` predicate (excludes Trans/Corrections/Changes/Report).
- **Landmines**: pre-CY2017 unavailable (no working URL anywhere). CY2020 ships `.xls` not `.xlsx` — `xlrd<2` is a hard dependency in `pyproject.toml`. Workbook member names drift across years (`HCPC{cy}_OCT_ANWEB.xlsx`, `HCPC{cy}_JAN_ANWEB_{MMDDYYYY}.xlsx`, `HCPC{cy}_CONTR_ANWEB_disc.xlsx`, etc.); auto-detection beats per-CY override map. CY2020 prepends a 10-row legal disclaimer before the header. Modifier rows (`RECID=7`) filtered. See `CLAUDE.md` Landmines for full list.

### ICD-9-CM (Phase 1, FY2008–FY2015 shipped Session 2)

- **What**: ICD-9-CM diagnosis code master, FY2008–FY2015 (8 FYs). Code set was frozen Oct 1, 2011; FY2014/FY2015 master ZIP is byte-identical (CMS reused v31 artifact for v32).
- **Where**: CMS "ICD-9-CM Diagnosis & Procedure Codes" page (`cms.gov/medicare/coding-billing/icd-10-codes/icd-9-cm-diagnosis-procedure-codes-abbreviated-and-full-code-titles`) — NOT the GEMs archive. Per-FY URLs in `src/config.py:ICD9CM_SOURCES`; FY2010 long-titles supplement in `src/config.py:ICD9CM_FILENAME_OVERRIDES[2010]`. ICD-9-CM and ICD-9-Vol3 share the same per-FY ZIP across all 8 FYs.
- **How**:
  - Single FY: `python3 -m src.extract.icd9cm --fy 2014`
  - Bulk FY range: `python3 -m src.extract.icd9cm --fy-range 2008-2015`
  - Rebuild unified silver: `python3 -m src.transform.codes_master`
  - Validate: `python3 -m pytest tests/test_icd9cm.py -v`
- **Cadence**: annual through FY2015; frozen thereafter (no future updates).
- **Verification**: per-FY row counts ~13.7k–14.6k; total ICD-9-CM in `codes_master.parquet` = 114,708. FY2012–FY2015 share content (post-freeze); the `(code, code_system, fy_or_cy)` triple guard absorbs the duplicates.
- **Landmines**:
  - v25 (FY2008) and v26 (FY2009) ship short titles only — `description` falls back to short title for those FYs.
  - v27 (FY2010) needs a separate `fulltitles` supplement ZIP for long descriptions; URL in `src/config.py:ICD9CM_FILENAME_OVERRIDES[2010]["long_supplement"]`.
  - v28–v32 (FY2011–FY2015) ship paired LONG/SHORT files with inconsistent embedded date suffixes (e.g. `CMS29_DESC_LONG_DX.101111.txt`, `CMS30_DESC_LONG_DX 080612.txt`); member-detection in `src/extract/icd9cm.py` uses substring predicates rather than exact filenames.
  - v26 procedures filename has a double space (`V26  I-9 Procedures.txt`).
  - v32 (FY2015) ZIP is byte-identical to v31 (FY2014) — CMS reused the artifact post-freeze; the codes_master `(code, code_system, fy_or_cy)` triple guard absorbs the duplicates.
  - ICD-9-CM and ICD-9-Vol3 share the same per-FY ZIP across all 8 FYs (`config.ICD9PCS_SOURCES = dict(ICD9CM_SOURCES)`).
  - No machine-readable POA-exempt list, no machine-readable addenda, no chapter info for any ICD-9 FY (`poa_exempt`, `addendum_action`, `chapter` all NULL).
  - See `CLAUDE.md` Landmines for cross-cutting context.

### ICD-9-CM Volume 3 (Phase 1, FY2008–FY2015 shipped Session 2)

- **What**: ICD-9-CM Volume 3 inpatient procedure code master, FY2008–FY2015 (8 FYs). `code_system="ICD-9-Vol3"`. Frozen alongside ICD-9-CM.
- **Where**: same per-FY ZIPs as ICD-9-CM (procedure-side member uses `_SG` / `procedures` / `Procedures` substring). Per-FY URLs in `src/config.py:ICD9PCS_SOURCES` (= `dict(ICD9CM_SOURCES)`).
- **How**:
  - Single FY: `python3 -m src.extract.icd9pcs --fy 2014`
  - Bulk FY range: `python3 -m src.extract.icd9pcs --fy-range 2008-2015`
  - Rebuild unified silver: `python3 -m src.transform.codes_master`
  - Validate: `python3 -m pytest tests/test_icd9pcs.py -v`
- **Cadence**: annual through FY2015; frozen thereafter.
- **Verification**: per-FY row counts ~3.77k–3.88k; total ICD-9-Vol3 in `codes_master.parquet` = 30,808.
- **Landmines**: reuses parsing helpers from `src/extract/icd9cm.py` (`PER_FY_URLS`, `download_bronze`, `extract_rows`, `build_table`); writes to a separate bronze tree (`bronze/icd9pcs/`) for re-pull provenance. v26 procedures filename has a double-space (`V26  I-9 Procedures.txt`). `poa_exempt` always NULL by design (POA is diagnosis-only).

### GEMs (Phase 1, frozen FY2018, shipped Session 2)

- **What**: General Equivalence Mappings between ICD-9 and ICD-10 (bidirectional, CM and PCS) plus FY2016 Reimbursement Mappings. Single-shot, frozen FY2018. Output: `data/silver/gems_2018.parquet` (NOT in `codes_master` — distinct relation schema, used by Phase 8 `(:GEMMapping)` nodes).
- **Where**: 4 separate CMS ZIPs — per-system GEMs CM/PCS plus per-system Reimbursement Mappings DX/PR. RM artifacts last published FY2016 (program ended one year before the FY2018 GEMs freeze). URLs in `src/config.py:GEMS_SOURCES` (keys: `cm | pcs | rm_dx | rm_pr`); ZIP-internal filenames in `src/config.py:GEMS_FILES` (mixed `2018_I9gem.txt` / `gem_i9pcs.txt` / `reimb_map_dx_2016.txt` patterns).
- **How**:
  - Full extraction (no FY argument — frozen): `python3 -m src.extract.gems`
  - Validate: `python3 -m pytest tests/test_gems.py -v`
- **Cadence**: none (frozen FY2018; will not be re-published by CMS).
- **Verification**: 429,081 rows total across 6 mapping files (4 GEMs + 2 RM). Source/target tally: ICD-10-CM→ICD-9-CM 154,448 (81,593 GEMs + 72,855 RM); ICD-10-PCS→ICD-9-Vol3 176,180 (101,025 + 75,155); ICD-9-CM→ICD-10-CM 24,860; ICD-9-Vol3→ICD-10-PCS 73,593. `effective_start=2018-10-01`, `effective_end=NULL`.
- **Landmines**: no single combined "2018 ICD-10-CM and GEMs" ZIP — must pull all 4 ZIPs. RM uses variable-column cluster format (`<icd10_src> <count> <icd9_target_1> [<target_2> ...]`); extractor expands clusters into 1:1 rows. GEMs `no_map_flag=1` rows carry literal target `"NoDx"` / `"NoPCS"`; extractor nulls these. RM rows have NULL for all GEMs flag columns. See `CLAUDE.md` Landmines for full list.

### PFS RVU (Phase 2, partial — schema + probe shipped Session 3, full 76-cell pull deferred)

- **What**: Physician Fee Schedule RVU files, `RVU{YY}{Q}.zip`, all four quarters CY2008–CY2026 (76 (cy, q) cells). Output schema `silver/pfs_rates.parquet` is locked: 18 columns, compound key `(code, modifier, cy, quarter, setting, conversion_factor_kind)`, long-form per setting (one row per `pfs_facility` / `pfs_nonfacility`). CY2026 doubles each row into `qp` + `nonqp` variants per the locked schema decision.
- **Where**: CMS PFS quarterly addenda. Three URL eras (legacy `Downloads/RVU{YY}{Q}.zip` for older CYs, modern `files/zip/rvu{YY}{q}.zip` for recent CYs); 76 cells encoded as canonical-pattern placeholders in `src/config.py:PFS_SOURCES`. Per-CY conversion factors (CY2008–CY2025 single floats, CY2026 `(qp, nonqp)` tuple) in `src/config.py:PFS_CONVERSION_FACTORS`. Inner artifact: `PPRRVU{YY}.csv` (or release-month-suffixed variants like `PPRRVU24_OCT.csv`) located by suffix predicate.
- **How**:
  - Single (cy, quarter): `python3 -m src.extract.pfs_rvu --cy 2024 --quarter D`
  - Single CY × all quarters: `python3 -m src.extract.pfs_rvu --cy 2024 --quarter-range A-D`
  - Single quarter × CY range: `python3 -m src.extract.pfs_rvu --cy-range 2008-2026 --quarter D`
  - Full bulk: `python3 -m src.extract.pfs_rvu --cy-range 2008-2026 --quarter-range A-D` (76 cells; pre-CY2017 likely log+skip)
  - Build unified silver: `python3 -m src.transform.pfs_rates` → `data/silver/pfs_rates.parquet`
  - Validate: `python3 -m pytest tests/test_pfs_rvu.py tests/test_pfs_rates.py -v`
  - Confirm codes_master untouched: `python3 -m src.transform.codes_master` (row count + code_system set unchanged)
- **Cadence**: quarterly (CMS publishes RVU{YY}A by Jan 1, B by Apr 1, C by Jul 1, D by Oct 1). Annual full-replacement is the Q1 (`A`) release; Q2-Q4 are mid-year corrections.
- **Verification**: per-(cy, q) row count ~16k–17k codes × 2 settings = ~32k–34k rows for non-2026 CYs; ~64k–68k for CY2026 (extra ×2 for qp/nonqp). Spot-check CY2024 Q4 99213 (no modifier): `pfs_facility ≈ $64`, `pfs_nonfacility ≈ $89` (within tolerance of CMS-published $57 / $92 — note tolerance band is wide because the config CF is start-of-CY 32.7442, while the Q4 CSV uses the post-March 33.2875). CY2026 doubling: distinct (cy=2026, setting, conversion_factor_kind) bucket counts must be 1:1:1:1 across (pfs_facility, pfs_nonfacility) × (qp, nonqp).
- **Landmines**: PFS quarterly URL slug drift across CMS eras (canonical-pattern placeholders + log+skip on 404). Status-indicator `I` and `N` rows dropped pre-emit. Modifier kept as a separate nullable column (compound key has 6 parts). DuckDB GROUP BY treats NULL modifier values as equal natively (no COALESCE shim). PFS silver lives in subdirectory `silver/per_source/pfs_rvu/` so codes_master's non-recursive glob does not pick it up.
  - PPRRVU CSV inner-name drift (`PPRRVU24_OCT.csv` vs `PPRRVU24.csv`): extractor falls back from exact-name to substring `*PPRRVU*.csv`.
  - PPRRVU CSV uses a multi-row stacked column header (qualifiers like `WORK` / `NON-FAC` / `FACILITY` / `STATUS` / `MP` stacked on row N-1 above leaf names like `RVU` / `PE RVU` / `CODE` on row N). `_assemble_stacked_header` stitches up to 6 rows; single-row pre-modern era handled too.
  - CY2026 ships TWO PPRRVU CSVs (`PPRRVU2026_Jan_QPP.csv` + `PPRRVU2026_Jan_nonQPP.csv`); current extractor reads one CSV and applies both `qp` and `nonqp` CFs. RVUs are identical across the 11,811 common (code, modifier) pairs (defended by `tests/test_pfs_rvu.py::test_cy2026_qpp_nonqpp_rvus_identical`); only the conversion factor differs.
  - Per-quarter CF drift: `config.PFS_CONVERSION_FACTORS[2024] = 32.7442` is the start-of-CY2024 rate; the CY2024 Q4 PPRRVU CSV's own `CONV FACTOR` column shows `33.2875` (post-March 2024 statutory adjustment). Phase 2 uses the config-level scalar; Phase 7 may revisit if cost-observation math is sensitive.
  - CY2018 Q3 PFS ships as `rvu18c1.zip` (NOT `rvu18c.zip`) — single-cell anomaly hardcoded in `src/config.py`.
  - CY2024 Q2 PFS RVU unrecoverable as of 2026-04-30: `https://www.cms.gov/files/zip/rvu24b.zip` returns DMEPOS Q2 2024 content; CMS appears to have published DMEPOS under the PFS-shaped slug. Extractor's `PPRRVU CSV not found in ZIP` guard catches it cleanly. Revisit at Phase 5 if PUF overlay needs CY2024 Q2.
  - CY2026 Q3/Q4 PFS RVU not yet published as of 2026-04-30 (CMS PFS landing page lists only Q1=`rvu26a` and Q2=`rvu26b`). Refresh in Q3/Q4 calendar windows (~Jul 2026 / Oct 2026).
  - `src/extract/pfs_rvu.py:extract_cy_quarter` carries a defensive bronze-filename synthesis fallback that renames downloaded ZIPs to canonical `rvu{YY}{q}.zip` when `Path(url).name` does not begin with `rvu` (defends against literal-slash dated suffixes).
  - See `CLAUDE.md` Landmines for the full list.

### OPPS Addendum A & B (Phase 2, CY2008–CY2026 shipped Session 4a)

- **What**: Hospital Outpatient PPS quarterly Addenda. Addendum A is APC-keyed (APC code, status indicator, weight, national-unadjusted payment rate, copays). Addendum B is HCPCS-keyed with APC FK + payment rate. Two locked silver tables: `opps_apc_rates.parquet` (12 cols, key `(apc, cy, quarter)`) and `opps_hcpcs_rates.parquet` (13 cols, key `(code, modifier, cy, quarter)`). National-unadjusted rates only — wage-index correction is Phase 7.
- **Where**: CMS publishes quarterly Addendum A/B per (CY, quarter) across four URL eras: legacy ad-hoc (CY2008–CY2011, idiosyncratic basenames), legacy hyphenated (CY2012–CY2019, mostly `{Mon}-{CY}-Addendum-{A|B}.zip` on `Hospitaloutpatientpps/{downloads,Downloads}/`), modern partial (CY2020–CY2022, mostly broken on `cms.gov/files/zip/`), modern Drupal (CY2023–CY2026, `cms.gov/files/zip/{month}-{cy}-opps-addendum{-b}.zip` with per-cell drift). 152 cells encoded as canonical-pattern entries in `src/config.py:OPPS_SOURCES`. Inner artifact: a `.csv` (CY2018+) or `.txt` tab-delimited (CY2008–CY2017) member located by predicate (`*addendum*` substring, prefer non-508 variants).
- **How**:
  - Single (cy, quarter, addendum): `python3 -m src.extract.opps --cy 2024 --quarter D --addendum A` (or `B`)
  - Single CY × all quarters × both addenda: `python3 -m src.extract.opps --cy 2024 --quarter-range A-D`
  - Full bulk: `python3 -m src.extract.opps --cy-range 2008-2026 --quarter-range A-D` (152 cells; pre-Aug-2026 the CY2026 Q3-Q4 cells log+skip; CY2020–CY2021 + CY2022 Q2-Q4 also log+skip)
  - Build unified silvers: `python3 -m src.transform.opps_rates` (or `--target apc` / `--target hcpcs`) → `data/silver/opps_apc_rates.parquet` + `data/silver/opps_hcpcs_rates.parquet`
  - Validate: `python3 -m pytest tests/test_opps.py tests/test_opps_rates.py -v`
  - Confirm codes_master untouched: `python3 -m src.transform.codes_master` (row count + code_system set unchanged at 1,843,825)
- **Cadence**: quarterly (CMS publishes Q1 in late November, Q2 in late February, Q3 in late May, Q4 in late August). Addendum updates are typically released ~1 month before the quarter starts. Force-refresh with `--download-date YYYY-MM-DD`.
- **Verification**: 121 of 152 cells succeeded as of 2026-04-30 (Session 4a). `opps_apc_rates.parquet` = 50,253 rows (~400-1100 per CY-quarter pair, varies); `opps_hcpcs_rates.parquet` = 875,109 rows (~6k–16k per CY-quarter pair). Spot-check: `duckdb -c "SELECT * FROM 'data/silver/opps_apc_rates.parquet' WHERE cy=2024 AND apc='5071' LIMIT 5"`. Compound-key uniqueness enforced in build (no duplicate `(apc, cy, quarter)` or `(code, modifier, cy, quarter)`).
- **Landmines**: four URL eras (see CLAUDE.md Phase 2-OPPS landmines). CY2020–CY2021 entirely unrecoverable on cms.gov; CY2022 Q2-Q4 unrecoverable; CY2024 Q2 Addendum A unrecoverable; CY2026 Q3–Q4 not yet released. Pre-CY2018 ZIPs ship tab-delimited `.txt` (with `.xlsx` next to it) instead of `.csv`; the parser auto-detects delimiter by file extension. CY2024 Q1 Addendum B has trailing-dash basename (`january-2024-opps-addendum-b-.zip`); CY2025 Q3 Addendum B uses uppercased "July"; CY2023 Q1 has literal slashes in basename. Defensive `_synthesize_bronze_filename` falls back to canonical name when URL basename is empty / contains `/` / equals `addendum.zip`. OPPS Addendum CSV/TXT uses a SINGLE-row header preceded by 2-4 banner rows (NOT stacked-stitched like PPRRVU). Header normalization MUST collapse double-spaces (`" ".join(str(v).split()).upper()`) — plain `.strip().upper()` silently drops columns. Latin-1 decode fallback is load-bearing for at least one CY (0xa0 non-breaking-space byte). Status indicators `E1`/`E2` dropped pre-emit; `N` (packaged) is RETAINED with null APC and null payment_rate. Within-CSV `(code, modifier)` duplicates dropped first-occurrence-wins with stderr warning.
  - Per-cell URL anomalies (encoded explicitly in `src/config.py:OPPS_SOURCES`, append-only):
    - CY2023 Q1: literal slashes in basename (`january-2023-opps-addendum-updated-01/20/2023.zip`) — `_synthesize_bronze_filename` falls back to canonical name.
    - CY2024 Q1 Addendum B: trailing-dash basename (`january-2024-opps-addendum-b-.zip`) — verified live, retained verbatim.
    - CY2024 Q2 Addendum A: unrecoverable; CMS Drupal wrapper links to generic `/files/zip/addendum.zip` which 404s. Bulk extractor logs+skips. Addendum B Q2 worked.
    - CY2025 Q3 Addendum B: uppercased "July" in basename (`July-2025-opps-addendum-b.zip`) — single-cell case mutation.
    - CY2026 Q3/Q4: not yet released (expected ~Jul 2026 / Oct 2026); placeholders on canonical Q1 slug pattern — refresh URLs in `OPPS_SOURCES` after CMS publishes (basename pattern may drift).
  - See `CLAUDE.md` Landmines for the full list.

### ASC Addenda AA / BB / DD1 / DD2 / EE / FF (Phase 2, CY2013Q4–CY2026 shipped Session 4b)

- **What**: ASC (Ambulatory Surgical Center) Final Rule quarterly addenda. Six addenda per (cy, quarter) cell distributed inside ONE bundled ZIP: AA (Covered Surgical Procedures, HCPCS+rate), BB (Covered Ancillary Services, HCPCS+rate), DD1 (Payment Indicator definitions; small reference table), DD2 (Status Indicator definitions), EE (Excluded Procedures, HCPCS-only), FF (Drug/Device Pass-Through Pricing, HCPCS+rate). FF only appears starting CY2022 — earlier CYs ship 5 addenda. Four locked silver tables: `asc_rates.parquet` (UNION of AA+BB+FF, HCPCS-keyed, 12 cols, key `(code, modifier, cy, quarter, addendum)`), `asc_payment_indicators.parquet` (DD1, 5 cols), `asc_status_indicators.parquet` (DD2, 5 cols), `asc_excluded_procedures.parquet` (EE, 5 cols). All silver tables include a `quarter` column — plan-v5.md's annual-cadence premise was wrong; CMS publishes ASC quarterly, mirroring OPPS.
- **Where**: ASC quarterly addenda live in CMS Drupal `cms.gov/files/zip/` (CY2020+) and legacy `cms.gov/Medicare/Medicare-Fee-for-Service-Payment/ASCPayment/{Downloads,downloads}/` (CY2013Q4-CY2019). Two basename schemes: short `{CY}-{Mon}-ASC-Addenda.zip` and long `{Month}-{CY}-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip`. URLs encoded as 76 unique ZIP keys in `src/config.py:_ASC_ZIP_BY_CY_QUARTER`; `ASC_SOURCES` is the 456-entry exploded-by-addendum view. Inside each ZIP: 6 text/CSV members (one per addendum) plus an `.xlsx` master (ignored). CY2021–CY2024 era ships tab-delimited `.txt` (filenames like `January 2024 ASC AA.txt`); CY2025+ ships comma-delimited `.csv` inside a `508 Version {Month} {CY} ASC Addenda/` subdirectory. Demux: case-insensitive token match on the addendum letter (priority 1: `Addendum {LETTER}` substring; priority 2: `\b(AA|BB|DD1|DD2|EE|FF)\b` immediately before the extension).
- **How**:
  - Single (cy, quarter): `python3 -m src.extract.asc --cy 2024 --quarter A` (downloads ONE ZIP, emits up to 6 parquets)
  - Single (cy, quarter, single addendum): `python3 -m src.extract.asc --cy 2024 --quarter A --addendum AA` (same ZIP, one parquet)
  - Single CY × all quarters × all addenda: `python3 -m src.extract.asc --cy 2024 --quarter-range A-D`
  - Full bulk: `python3 -m src.extract.asc --cy-range 2008-2026 --quarter-range A-D` (76 cells; pre-2013 + CY2020 Q1/Q3/Q4 + CY2025 Q1/Q3/Q4 + CY2026 Q3/Q4 log+skip)
  - Build unified silvers: `python3 -m src.transform.asc_rates` (or `--target {rates,dd1,dd2,ee,all}`)
  - Validate: `python3 -m pytest tests/test_asc.py tests/test_asc_rates.py -v`
  - Confirm codes_master untouched: `python3 -m src.transform.codes_master` (1,843,825 rows, unchanged)
- **Cadence**: quarterly (CMS publishes Q1 in late November preceding the CY, Q2 in late February, Q3 in late May, Q4 in late August), aligned with the OPPS Final Rule + Addenda Updates calendar.
- **Verification**: 30 of 76 (cy, quarter) cells succeeded as of 2026-04-30 (Session 4b); 164 per-source parquets total. Unified silver row counts: `asc_rates` = 238,227 rows; `asc_payment_indicators` = 574 rows; `asc_status_indicators` = 139 rows; `asc_excluded_procedures` = 57,021 rows. Spot-check: `duckdb -c "SELECT addendum, COUNT(*) FROM 'data/silver/asc_rates.parquet' WHERE cy=2024 AND quarter='A' GROUP BY 1"`. Compound-key uniqueness enforced in build (no duplicate `(code, modifier, cy, quarter, addendum)`, `(payment_indicator, cy, quarter)`, `(status_indicator, cy, quarter)`, `(code, cy, quarter)`).
- **Landmines**: ASC publishes QUARTERLY, not annually (plan-v5.md premise was wrong). Bundled ZIP demux is the contract — addendum letter is a member-basename predicate, not a URL key (`ASC_SOURCES` and `_ASC_ZIP_BY_CY_QUARTER` encode this). Mixed `.txt` (legacy/CY2024) vs `.csv` (CY2025+) with auto-detected delimiter. 1-4 banner/disclaimer rows precede column header (variable; scan first 30 rows). CY2025+ `.csv` lives inside a `508 Version .../` subdirectory inside the ZIP. CY2024 Q1 EE filename anomaly: drops the `ASC` token (`January 2024 EE.txt` not `... ASC EE.txt`); demux uses bare-letter fallback. Within-CSV `(code, modifier)` duplicates in BB/FF (J-code drugs) are dropped first-occurrence-wins with stderr warning — required to satisfy `(code, modifier, cy, quarter, addendum)` compound uniqueness. AA and FF share the same HCPCS code list per (cy, q) but differ on rates (FF is the device-offset percentages over the same covered-surgical set); compound key includes `addendum` to keep them distinct in unified silver. FF only appears starting CY2022 — earlier CYs have 5 addenda. Literal-slash dated suffixes in some URLs (CY2024 Q1+Q4, CY2025 Q2 — same hazard as PFS rvu24b and OPPS CY2023 Q1); defensive bronze-filename synthesis fallback in `_synthesize_bronze_filename`. Drupal `-0` suffix redirects (CY2025 Q4, CY2026 Q1) — `requests` follows 301 transparently, no helper needed. Legacy host case sensitivity: `Downloads/` (capital) for most cells, `downloads/` (lowercase) for CY2013 Q4 + CY2018 Q2; encoded explicitly per-cell. CY2008–CY2012 entirely unrecoverable on cms.gov. CY2025 Q1/Q3/Q4 + CY2026 Q3/Q4 not yet released as of 2026-04-30. AMA CPT license click-through wraps Drupal landing pages but underlying ZIP URLs work without cookie. ASC silver lives in `data/silver/per_source/asc/*.parquet` (subdirectory); codes_master's non-recursive glob skips it. See `CLAUDE.md` Phase 2-ASC landmines for the full list.

### IPPS Final Rule Tables 5, 1A–1E (Phase 3, FY2008–FY2026 shipped Session 5)

- **What**: MS-DRG weights (Table 5) + operating/capital standardized amounts and labor/nonlabor shares (Tables 1A–1E), FY2008–FY2026 V*.0. 19 FYs × 2 table_kinds = 38 cells. Two top-level silver tables: `data/silver/ms_drg_weights.parquet` (14,393 rows; key `(ms_drg, fy, version)`) and `data/silver/ipps_std_amounts.parquet` (177 rows; key `(fy, version, "table", line_item, category, rate_period)`). Tables 6P (code-change crosswalks) deferred to Phase 4. V*.1 mid-year refreshes (e.g. V43.1 April 2026) only refresh the grouper + MCE — no rate-table republication; Phase 3 captures V*.0 only.
- **Where**: All 38 V*.0 cells live on direct cms.gov across three URL eras encoded in `src/config.py:_IPPS_TABLE5_BY_FY` / `_IPPS_TABLES1_BY_FY`. **Era A** (Modern Drupal, FY2022/FY2023/FY2024/FY2026): `cms.gov/files/zip/fy{YYYY}-ipps-fr-table-{5|1a-1e}.zip`. **Era A'** (FY-prefix anomaly, FY2021/FY2025): `cms.gov/files/zip/fy-{YYYY}-ipps-{fr|final-rule}-table-{5|1a-1e}.zip`. **Era B** (legacy mixed-case + lowercase host, FY2008–FY2020): `cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/{Downloads,downloads}/...` with severe per-FY basename drift (every URL hard-coded individually). Discovery doc with all 38 verified URLs at `plans/archive/discovery/ipps-url-discovery.md`.
- **How**:
  - Single (FY, version): `python3 -m src.extract.ipps --fy 2026 --version V43.0 --target both`
  - Bulk FY range: `python3 -m src.extract.ipps --fy-range 2008-2026 --target both`
  - One target only: `python3 -m src.extract.ipps --fy 2026 --target table5` or `--target tables1`
  - Strict mode (fail-loud on unknown Tables 1A-1E line-item labels): `python3 -m src.extract.ipps --fy 2026 --strict`
  - Re-pull bronze with fresh date: `python3 -m src.extract.ipps --fy 2026 --download-date 2026-MM-DD`
  - Build unioned silvers: `python3 -m src.transform.ipps_tables` (or `--target weights` / `--target std_amounts`)
  - Validate: `python3 -m pytest tests/test_ipps.py tests/test_ipps_tables.py -v`
  - Re-extracting Table 5 for multi-sheet FYs (2015/16/18/20/21/25) automatically uses `_pick_fr_sheet` (selects FR sheet by name); single-sheet FYs fall back to `wb.active`.
- **Cadence**: annual (October FY-stamped). V*.1 mid-year refresh exists for grouper / MCE only — Phase 4 territory.
- **Verification**: per-FY DRG counts climb 745 (FY2008) → 772 (FY2026); DRG 470 (Major Joint Replacement) RW between 1.9–2.1; codes_master row count UNCHANGED at 1,843,825 post-build (subdirectory isolation). All 167 tests passing.
- **Landmines**: (1) Title banner row 0 substring-matches `"DRG"` inside `"MS-DRGS"` — header detection requires exact `MS-DRG` AND length cap. (2) Tables 1A-1E XLSX is ONE worksheet with all 5 tables stacked (NOT one sheet per letter) — section-marker walk via `TABLE 1[A-E]` regex. (3) FY2018+FY2017+FY2016 ship `Table-1.zip` (singular) carrying bundled `Table 1A - 1E.xlsx`; same Mode A bundled-stacked path. (4) FY2008–FY2012 ship legacy `.xls` (BIFF) — xlrd reader (xlrd<2 dep already in pyproject). (5) Pre-FY2013 ZIPs ship `Description.txt` siblings — filtered as disclaimer member. (6) `_ext_rank` prefers `.xlsx > .txt > .xls > .csv` (NOT `.xls > .txt`) because legacy .xls hides data behind a Description sheet at index 0. (7) FY2010 V27.0 PPACA dual-period emits separate `_1st_half` / `_2nd_half` parquets; `rate_period` non-null only for FY2010. (8) FY2008/FY2009 ship Tables 1A-1D only (no 1E pre-PPACA) — cardinality assertions are SUBSET. (9) FY2015 V32.0 Table 1E LTCH rate (resolved 2026-04-30 by `_pick_bundled_tables1_sheet` selecting the FR sheet on multi-sheet workbooks). (10) DuckDB reserved keyword `table` — every SQL touching `ipps_std_amounts.table` MUST quote as `"table"`. See `CLAUDE.md` Landmines for full Phase 3 list.

### MS-DRG Java grouper JARs (Phase 4)

- **What**: official CMS MS-DRG Java reference implementation. **Sessions 6 + 7 ship V40.0 → V43.1 (FY2023–FY2026, 8 versions)**. Pre-V40 versions either have no Java JAR (V25–V37) or use a different API requiring a separate driver template (V38.1, V39.0/V39.1 — deferred to Session 8+).
- **Where**: `cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software` (URL constants in `src/config.py:GROUPER_SOURCES`; runtime deps in `src/config.py:GROUPER_RUNTIME_DEPS`; deferred V38.1/V39 URLs in `src/config.py:_GROUPER_DEFERRED_SOURCES`).
- **How**:
  - Bulk-pull all V40+ vendored cells: `python3 -m src.extract.grouper --all --strict --continue-on-error`
  - Pull a single cell: `python3 -m src.extract.grouper --fy 2023 --version V40.0`
  - Pull all minor versions for one FY: `python3 -m src.extract.grouper --fy 2024 --all-versions`
  - Pull a contiguous FY range: `python3 -m src.extract.grouper --fy-range 2023-2025`
  - Re-pull with fresh bronze date: append `--download-date 2026-MM-DD`. Bronze write-once per `(fy, version, date)`.
  - Run all grouper smoke tests (V40+ per-version sepsis + hip): `python3 -m pytest tests/test_grouper.py -v` (36 tests)
  - Run MIMIC-IV stratified validation (needs `KG_COST_MIMIC_HOSP_DIR=<path-to-mimic-iv-hosp-dir>`; full 1,200 records, ~3s): `python3 -m pytest tests/test_grouper_validation.py -v -s`
  - Run drgpy v40 cross-check (FY2024 sanity tier): `python3 -m pytest tests/test_drgpy_cross.py -v -s`
  - Iterative dev under MIMIC harness: `MIMIC_VALIDATION_QUICK=1 python3 -m pytest tests/test_grouper_validation.py` (60 records/FY for ~1s wall-time)
  - Programmatic invocation (V*.0 default routing per Session 7 lock):
    ```python
    from src.grouper.grouper import assign, ClaimRecord
    r = ClaimRecord(pdx="I120", sdx=(), pcs=(), sex="M", age=68, discharge_disp="01", los=4)
    result = assign(r, 2026)  # routes to V43.0; pass version="V43.1" to override
    ```
  - Full constructor signature: `assign(ClaimRecord(pdx, sdx, pcs, sex, age, age_days_admit, age_days_discharge, discharge_disp, los, poa=...), fy=2026, version=None)`. Default `version=None` resolves to `GROUPER_VERSIONS_BY_FY[fy][0]` (V*.0). `poa: tuple[str, ...] = ()` is per-dx, positionally aligned with `(pdx,) + sdx`; empty tuple defaults to all-Y. Validation set `{Y, N, U, W, 1, E}`.
- **Cadence**: annual; one JAR per FY of discharge. Mid-year V*.1 refresh (April) ships an updated grouper + MCE without changing the rate tables — Phase 7 cost-compute over Apr 1 - Sep 30 must pass `version="V*.1"`; Oct 1 - Mar 31 uses default V*.0 routing.
- **Verification**:
  - Session 6: 19 hand-curated tests for V43.1 (Pre-MDC heart transplant DRG 002, surgical hip-replacement w/ + w/o MCC 469/470, medical sepsis w/ + w/o MCC 871/872, newborn normal liveborn 795, newborn respiratory distress 790, ECMO Pre-MDC 003, discharge-disp lock, POA wiring, dataclass shape).
  - Session 7: 16 per-version smoke tests (sepsis+MCC + hip across V40-V43 = 8 × 2 cases). 1 new contract test for FY out-of-scope (FY2007 rejected). 1 new contract test for explicit version override validated. 1 new contract test for pre-V38.1 FY rejected (FY2018 → "no Java MS-DRG grouper" error).
  - MIMIC-IV stratified ≥97% match: 1170/1200 = 97.50% on FY2023+FY2024 (just below plan-v9.md aspirational 98%; gap documented in CLAUDE.md Landmines). Test asserts ≥0.97 floor.
  - drgpy v40 vs Java V41.0 cross-check: 290/300 = 96.67% on FY2024 (≥0.96 floor).
  - Total: 209/209 tests pass post-Session-7. codes_master.parquet unchanged at 1,843,825 rows.
- **Landmines** (excerpt — full list in `CLAUDE.md` Landmines section, especially Phase 4 part 2 block):
  - Java 17 required for V40+ (CMS recompiled).
  - The CMS standalone JARs are a **library**, not a runnable JAR — driven via `src/grouper/driver/KGCostGrouperDriver.java` (subprocess stdin/stdout JSON contract; reflective per-version `MsdrgComponent` dispatch via `Class.forName`).
  - Four runtime deps NOT bundled: `gfc-base-api` (built locally from GitHub source via stdlib `javac` fallback), `protobuf-java`, `slf4j-api`, `slf4j-nop`. All vendored under `src/grouper/vendor/`. Single classpath spans all V40+ version dirs.
  - Pre-MDC DRGs report a non-"00" MDC. Filter via `int(ms_drg) <= 18`.
  - `discharge_disp` does NOT alter DRG routing.
  - Newborn fields are load-bearing for MDC-15 routing.
  - **CMS Java grouper era: V38.1+ (May 2021).** Pre-V38.1 versions are NTIS-paid (V25-V34) or Windows-only (V35-V38.0). PI-approved hybrid scope: V40+ Java + drgpy fallback for FY2019-FY2023. Pre-FY2019 cohorts: formula-only Phase 7 path.
  - **Per-version core JAR naming drifts**: V40.0 `msdrg-40.0.0.3.jar` (no prefix); V41 `msdrg-v410-*.jar`; V42 `msdrg-v420-*.jar`; V43.0 `msdrg-v430-43.0.0.2.jar` (NOT `msdrg-core-*` — that rename started ONLY at V43.1); V43.1 `msdrg-core-43.1.0.0.jar`. `_find_core_jar()` heuristic (in `src/extract/grouper.py`) handles all variants.
  - **MIMIC-IV idiosyncrasy**: HIPAA-shifted dates with anchor_year_group ±1.5 yr fuzziness. POA defaults to all-Y (MIMIC has no `icd_code_poa` column). Validation harness restricted to ICD-10 era (real-FY ≥ 2016). ICD-9 era requires drgpy fallback.
  - **drgpy 0.0.6 needs `setuptools<80`** (drgpy 0.0.6 uses `pkg_resources`, which was removed in setuptools 80+). This release pins `drgpy==0.2.1` in `pyproject.toml` `[project.optional-dependencies].dev`; drgpy 0.2.1 does not import `pkg_resources`, so no setuptools cap is pinned.
  - **V42.0 bare-slug URLs** (`standalone-jars.zip`, `mainframe-software.zip`) carry collision risk — any future CMS upload could overwrite. Re-pulls should compare bytes against prior bronze.
  - **MCE wiring deferred** — MIMIC validation surfaced 4 ungroupable DRG cases (308→999, 269→999) but <1% impact; Step 9 MCE integration deferred to Session 8+.

### Empirical PUF means (Phase 5)

Phase 5 ships four extractors that produce per-source weighted-mean and per-provider-percentile parquets, then unifies them into `data/silver/empirical_means.parquet` (27 columns post-Session-8.5 schema rename; compound key `(code, code_system, year, place_of_srvc, source)`). Empirical row counts as locked at end of Session 8.5 (2026-05-01): puf_inpatient 6,846; puf_inpatient_summary 300; puf_outpatient 590; puf_physician 90,334; **`empirical_means.parquet` 98,070**. Source-tag distribution: `puf_top100`=200, `puf_top100_summary`=300, `puf_all_drgs_ry23`=6,646, `puf_outpatient`=590, `puf_physician`=90,334. `source_host` distribution: `cms_gov`=97,680, `nber`=390. codes_master.parquet UNCHANGED at 1,843,825 rows (verified post-bulk).

URL dicts for Phase 5 live module-local at the top of each `src/extract/puf_*.py` (deviation from Phase 1-3 precedent — driven by parallel-execution coordination during code-write). Update there when CMS publishes a new release; year-range constants stay in `src/config.py` (`PUF_INPATIENT_FYS`, `PUF_INPATIENT_SUMMARY_FYS`, `PUF_OUTPATIENT_CYS`, `PUF_PHYSICIAN_CYS`).

**Resolver helper** (Session 8.5): every Phase 5 extractor that touches FY2011-FY2013 inpatient summary or CY2011-CY2014 outpatient routes URL resolution through a 3-tier fallback chain — `cms.gov` (primary) → `nber` (`data.nber.org/providerchargepuf/`) → `wayback` (CDX API) → `None` (bulk runner log+skips). Resolver returns `(url, source_host)` so the extractor can stamp the `source_host` column on every emitted row; `source_host` literal vocabulary is `{"cms_gov", "nber", "wayback"}` (locked — extractor tests assert subset membership).

#### Medicare Inpatient PUF (per-provider, per-DRG)

- **What**: per-MS-DRG national mean charge / payment / allowed + p10..p90 percentiles across providers, FY2011-FY2024 (14 FYs, 14 parquets, 6,846 rows total at end of Session 8.5).
- **Where**: `data.cms.gov/sites/default/files/{YYYY-MM}/MUP_INP_RY{YY}_*.csv` (modern); legacy `cms.gov/.../Medicare-Provider-Charge-Data/Downloads/Inpatient_Data_{YYYY}_CSV.zip` for FY2011-FY2012. URLs hardcoded in `src/extract/puf_inpatient.py:_PUF_INPATIENT_SOURCES` (14/14 verified HTTP 200 on 2026-05-01).
- **How**: `python3 -m src.extract.puf_inpatient --fy 2023` for one FY; `--fy-range 2011-2024` for bulk. Streams CSV via DuckDB `read_csv_auto(ALL_VARCHAR=TRUE)`, groups by `(DRG_Cd, fy)`, weighted means with weight `Tot_Dschrgs`, `quantile_cont` for percentiles. Bronze: `data/bronze/puf_inpatient/fy{fy}/{download_date}/`. Silver: `data/silver/per_source/puf/inpatient/puf_inpatient_fy{fy}.parquet`.
- **Cadence**: CMS publishes annually with ~2-year lag (e.g., FY2024 data released ~mid-2026). Privacy suppression: rows with `n_providers < 11` get NULL percentiles; means + `n` retained.
- **Verification**: per-FY parquet schema match (27 cols post-Session-8.5); MS-DRG codes match `^[0-9]{3}$`; weighted means reconstruct CMS-published national averages within rounding; `ls data/silver/per_source/puf/inpatient/ | wc -l == 14`. FY2011/FY2012 row counts capped at ~100 (top-100 truncation evidence — see top-100 caveat below); FY2013+ ~570 DRGs (full-DRG via RY23 unified release).
- **Top-100 caveat**: **FY2011-FY2012 only top-100 DRGs**; the long-tail ~650 DRGs are unrecoverable from this source. Source tag distinguishes — `puf_top100` for FY2011-FY2012, `puf_all_drgs_ry23` for FY2013+ (renamed Session 8.5 to capture RY23 unified-release methodology; `puf_all_drgs_legacy` namespace reserved). FY2013 was retroactively re-published as FULL-DRG in CMS's RY23 unified release (~570 distinct DRGs in our silver), so FY2013 carries `puf_all_drgs_ry23` despite plan-v11.md's earlier presumption it was still top-100 capped. Phase 7 will add `formula_no_empirical` for the FY2011-FY2012 non-top-100 long-tail with full Impact File / labor-share / capital-rate / wage-index provenance joined from Phase 3 silvers.
- **Landmines**:
  - **CMS source-token rename `MUP_IHP` → `MUP_INP`** between RY23 (2023-05) and RY24 (2024-05). Encoded per-FY in the URL dict.
  - **RY23 is the canonical source for DY13-DY21** (CMS re-published 9 prior data years simultaneously); legacy per-year ZIPs at `cms.gov/.../Inpatient_Data_{YYYY}_CSV.zip` exist for DY13-DY17 but use legacy human-readable column names — prefer RY23 unified release.
  - **Inpatient PUF carries `Avg_Tot_Pymt_Amt` (all-payer total → populates `mean_allowed`) AND `Avg_Mdcr_Pymt_Amt` (Medicare portion → populates `mean_payment`).** Session 8.5 wired `mean_allowed` from `Avg_Tot_Pymt_Amt`; previously NULL. Phase 7 cost-compute can use empirical `mean_allowed` directly; formula `RW × (Std_Amount + Capital_Rate)` is the fallback only when PUF row missing.
  - **DY = calendar/claims year, NOT FY.** This project's config keys map FY values to `DY{fy}` URL slugs (verified: live `MUP_INP_RY26_*_DY24_PrvSvc.CSV` covers FY2024).
  - Column-name drift YoY (e.g. `Avg_Submtd_Cvrd_Chrg` ↔ `average_submitted_covered_charge`); `_resolve_columns` helper handles aliases. Fail loud on missing required columns.

#### Medicare Inpatient PUF — National Summary (cross-validation aggregate)

- **What**: by-DRG aggregate XLSX for FY2011-FY2013 only. Same schema columns as `puf_inpatient` but with `p*_*` and `n_providers` ALL NULL (no per-provider distribution). Source label `puf_top100_summary` so per-provider and summary rows for the same `(DRG, FY)` coexist in the unified table; Phase 7 prefers per-provider, falls back to summary. 100 rows × 3 FYs = 300 rows total (locked, end of Session 8.5).
- **Where**: NBER mirror `data.nber.org/providerchargepuf/{YYYY}/IPPS_NAT_STATE_SUM_XLSX.zip` (FY2011) and `Inpatient_Summary_{YYYY}_XLSX.zip` (FY2012/FY2013). CMS legacy `cms.gov/.../Medicare-Provider-Charge-Data/Downloads/...` retired in the 2025 renormalization. URLs hardcoded in `src/extract/puf_inpatient_summary.py:_PUF_INPATIENT_SUMMARY_SOURCES`. All 3 FYs route via the `cms_gov → nber → wayback` resolver helper; in the locked FY2011-FY2013 baseline all 300 rows carry `source_host = "nber"`.
- **How**: `python3 -m src.extract.puf_inpatient_summary --fy-range 2011-2013`. XLSX read via openpyxl; mirrors `src/extract/ipps.py` header-detection pattern. Bronze: `data/bronze/puf_inpatient_summary/fy{fy}/{download_date}/`. Silver: `data/silver/per_source/puf/inpatient_summary/puf_inpatient_summary_fy{fy}.parquet`.
- **Cadence**: frozen historical (FY2014+ National Summary uses different schema and is not consumed by Phase 5).
- **Verification**: per-FY row count ≥ 80 (relaxed from plan-v10's ≥600 — see Landmine); all `p*_*` and `n_providers` NULL; `mean_allowed` non-null all 3 FYs; `mean_payment` 100% NULL for FY2011 (single-column source). `ls data/silver/per_source/puf/inpatient_summary/ | wc -l == 3`; row-count check `duckdb -c "SELECT COUNT(*) FROM 'data/silver/per_source/puf/inpatient_summary/*.parquet'"` returns 300.
- **NBER fallback recipe**: every FY routes through `_resolve_url(fy, sources)` → tries `cms.gov` first, falls back to `data.nber.org/providerchargepuf/{YYYY}/{basename}` (NBER landing pages: `Inpatient.html` exists ONLY for FY2011; FY2012/FY2013 use `Inpatient2012.html` / `Inpatient2013.html`); third tier is Wayback CDX (`web.archive.org/cdx/search/cdx?url=...`) for any timestamp matching the canonical basename pattern. Returns `(url, source_host)` so each emitted row stamps `source_host ∈ {"cms_gov", "nber", "wayback"}`. Resolver `None` → bulk runner log+skips with stderr warning.
- **Landmines**:
  - **Plan-v10's premise that the National Summary backfills the missing ~650 DRGs is factually wrong.** CMS's FY2011-FY2013 Inpatient National Summary XLSXs are named `Medicare_Charge_Inpatient_DRG100_..._FY{YYYY}.xlsx` and contain only ~100 DRGs (top-100, same scope as per-provider). CMS expanded to all DRGs (`..._DRGALL_..._FY{YYYY}.xlsx`) only starting FY2014. So this extractor ships the cross-validation aggregate but provides NO ~650-DRG backfill — the truncated FY2011-FY2013 long-tail is unrecoverable from this source. Phase 7 cost-compute and Batch 2a row-count expectations must accept this gap.
  - **FY2011 NBER ZIP carries ONLY `Average Total Payments`** (no Medicare-only column). `_REQUIRED_LOGICAL` makes `mean_payment` optional; `mean_allowed` populated from `Average Total Payments`; `mean_payment` is 100% NULL for FY2011 by design. Defended by `tests/test_puf_inpatient_summary.py::test_fy2011_mean_payment_all_null_mean_allowed_populated`.
  - Test threshold relaxed `>=600` → `>=80` with full deviation note in test docstring + module docstring.
  - Log+skip on 404 (returns `Path | None`); mirrors HCPCS pre-2017 / OPPS pre-2017 precedent.

#### Medicare Outpatient PUF (per-provider, per-APC)

- **What**: per-APC national mean charge / payment / allowed + p10..p90 percentiles, CY2011-CY2023 (12 of 13 CYs in scope; CY2014 unrecoverable). 590 rows total across 12 parquets at end of Session 8.5.
- **Where**: `data.cms.gov/sites/default/files/{YYYY-MM}/MUP_OUT_RY{YY}_*.csv` (modern, CY2015-CY2023) / `MUP_OHP_R{YY}_*.csv` (legacy, CY2017-CY2018) / NBER mirror `data.nber.org/providerchargepuf/{YYYY}/Medicare_Provider_Charge_Outpatient_APC30_CY{YYYY}.{xlsx,csv}` (CY2011-CY2013). CY2014 confirmed unrecoverable on cms.gov + NBER + Wayback as of 2026-05-01. URLs hardcoded in `src/extract/puf_outpatient.py:_PUF_OUTPATIENT_SOURCES`.
- **How**: `python3 -m src.extract.puf_outpatient --cy 2022` / `--cy-range 2011-2023`. DuckDB streams CSV, groups by `(APC_Cd, cy)`, weighted means weighted by `Tot_Srvcs` (CY2015+) or `CAPC_Srvcs` (legacy NBER), `quantile_cont`. Bronze: `data/bronze/puf_outpatient/cy{cy}/{download_date}/` (`period="cy"`). Silver: `data/silver/per_source/puf/outpatient/puf_outpatient_cy{cy}.parquet`.
- **Cadence**: annual, ~2-year lag.
- **Verification**: APC count per CY ∈ [25, 70] (relaxed band — see APC drift caveat below). APC codes match `^[0-9]{4,5}$` post zero-pad. `mean_allowed` non-null for CY2015+ (Outpatient PUF carries `Avg_Mdcr_Alowd_Amt` natively); for CY2011-CY2013 NBER era, `mean_allowed` falls back to equal `mean_payment` (single-column "Average Total Payments" semantics). `ls data/silver/per_source/puf/outpatient/ | wc -l == 12`.
- **NBER fallback recipe**: same 3-tier resolver as Inpatient Summary — `cms.gov → nber → wayback → log+skip`. CY2011-CY2013 stamps `source_host = "nber"`; CY2015+ stamps `source_host = "cms_gov"`. NBER outpatient ZIP basename for those years is `Medicare_Provider_Charge_Outpatient_APC30_CY{YYYY}.xlsx`; CY2013 ships an XLSX (the `_v2` suffix) with 6 banner rows preceding the header — header-detection scans first 30 rows. CY2011/CY2012 ship CSVs.
- **APC drift caveat**: APC coverage band is **not stable** across CYs — 30 APCs in NBER (CY2011-CY2013) → 25-33 APCs in RY19 (CY2015-CY2016) → 58-68 APCs in CMS modern era (CY2017+). The C-APC consolidation post-CY2016 broadens the population; band relaxed to [25, 70] with documented justification. Phase 7 cost-compute should NOT assume APC-population parity across CYs.
- **Landmines**:
  - **`MUP_OHP` → `MUP_OUT` prefix shift between RY20 (DY18) and RY21 (DY19).** Hardcoded per-CY.
  - **UUID-prefixed sub-paths starting RY24 (2024-06 release)**: `/sites/default/files/{YYYY-MM}/{uuid}/MUP_OUT_RY{YY}_*.csv`. Hardcode literals; do NOT pattern-guess.
  - **`R{nn}` → `RY{nn}`** and **`D{nn}` → `DY{nn}`** rename happened RY20→RY21.
  - **CY2014 unrecoverable** on cms.gov + NBER + Wayback as of 2026-05-01 (NBER `/2014/` directory 404s; Wayback CDX returned `[]` for both `MUP_OHP*D14*` and `data.cms.gov/...D14*` patterns). Resolver returns `None` → bulk runner log+skips. Same posture as HCPCS pre-CY2017.
  - **CY2015+ uses CMS modern column names** (`CAPC_Srvcs` not `Tot_Srvcs`; `Avg_Tot_Sbmtd_Chrgs` not `Avg_Submtd_Chrg`); column-alias dict updated. NBER era (CY2011-CY2013) ships no `Avg_Mdcr_Alowd_Amt` column; resolver falls back so `mean_allowed` for those CYs equals `mean_payment`.

#### Medicare Physician & Other Practitioners PUF (per-provider CSV, ~2 GB/CY)

- **What**: per-HCPCS × place_of_srvc national mean charge / payment / allowed / standardized + p10..p90 percentiles, CY2013-CY2023 (11 of 12 CYs in scope; CY2012 unrecoverable). 90,334 rows total across 11 parquets at end of Session 8.5. Only PUF source carrying `mean_payment_stdzd` (geographically standardized payment).
- **Where**: `data.cms.gov/sites/default/files/2025-{04,11}/{UUID}/MUP_PHY_R25_P{04,05,07}_V20_D{YY}_Prov_Svc.csv` (canonical 2026 distribution after the 2025 renormalization; bare CSV, NOT zipped TSV). Legacy `Medicare_Provider_Util_Payment_PUF_CY{YYYY}.zip` and earlier `MUP_PHY_R{YY}_P05_V10_*.zip` URLs all retired. URLs in `src/extract/puf_physician.py:_PUF_PHYSICIAN_SOURCES`.
- **How**: `python3 -m src.extract.puf_physician --cy 2022` / `--cy-range 2013-2023`. **Heaviest extractor**: streams ~2 GB CSV (DuckDB `SET memory_limit='8GB'`), groups by `(HCPCS_Cd, Place_Of_Srvc, cy)`, weighted means weighted by `Tot_Srvcs`. `_detect_delimiter()` auto-routes by file suffix (`.csv` → comma; `.tsv` → tab — the latter retained for legacy bronze re-pulls). Bronze: `data/bronze/puf_physician/cy{cy}/{download_date}/{file}.csv` (write-once; ~22 GB across 11 CYs). Silver: `data/silver/per_source/puf/physician/puf_physician_cy{cy}.parquet` (already-aggregated; per-provider rows do NOT persist to silver).
- **Cadence**: annual, ~2-year lag (April-May release each year). Per-CY wall-clock ~30-60 min on the dev machine.
- **Verification**: HCPCS codes match `^[A-V0-9][0-9]{4}$`. `place_of_srvc` strictly in `{"F", "O"}` (NULL is a data error). `mean_payment_stdzd` non-null. Privacy suppression on `n_providers < 11` for percentiles only. `ls data/silver/per_source/puf/physician/ | wc -l == 11`.
- **Landmines**:
  - **2025 renormalization retired all legacy URLs.** `Medicare_Provider_Util_Payment_PUF_CY{YYYY}.zip` and `MUP_PHY_R{YY}_P05_V10_*.zip` no longer resolve. Canonical 2026 distribution is bare CSV under `data.cms.gov/sites/default/files/2025-{04,11}/{UUID}/MUP_PHY_R25_P{04,05,07}_V20_D{YY}_Prov_Svc.csv`. Bronze-layer file extension shifted from `.tsv` to `.csv`.
  - **CY2012 unrecoverable** on cms.gov + Wayback as of 2026-05-01. Canonical "by Provider and Service" dataset starts at CY2013. Phase 5 scope locked to CY2013-CY2023 (11 CYs).
  - **Malformed source CSV lines** — e.g. line 8549478 in CY2013 has 5 columns instead of 28. `read_csv_auto` runs with `ignore_errors=TRUE` to skip them; row count delta vs. canonical is < 0.001%.
  - **Modifier handling: ignored** for Phase 5. Compound key is `(code, code_system, year, place_of_srvc, source)`; `code` carries bare 5-char HCPCS only. Phase 7 will revisit if cohorts need modifier-aware rates.
  - **Pre-stripped privacy suppression**: CMS already removes rows with `Tot_Srvcs<11 AND Tot_Bene<11` from the source CSV. Our `n_providers < 11` mask is additive over CMS's.
  - **DuckDB streaming pattern is the canonical approach** — `read_csv_auto(?, ALL_VARCHAR=TRUE, header=TRUE, ignore_errors=TRUE)` + double-quoted identifier substitution from `_resolve_columns`. Hold-the-2-GB-in-RAM via pandas WILL OOM on the dev machine.

#### Unified empirical means transform

- **What**: unions all 4 PUF subdirs into `data/silver/empirical_means.parquet`. **27-col locked schema** (Session 8.5 added `source_host` + the `payment_amount` / `payment_amount_concept` / `total_payment_amount` triple replacing the legacy `mean_allowed`). Compound-key uniqueness, code_system + source cardinality assertions, row-count parity check, plus Session 8.5 `payment_amount NOT NULL` + concept-enum + total_payment_amount semantic-alias invariants. Locked total: 98,070 rows.
- **How**: `python3 -m src.transform.empirical_means`. Pre-flight: at least one input glob must populate; partial-empty stderr-warns; fully-empty raises `RuntimeError`. Output path inlined into SQL (DuckDB 1.4 single-`?`-binding landmine, Phase 1).
- **Cadence**: re-run after any new per-source parquet lands.
- **Verification**: `python3 -m pytest tests/test_empirical_means.py tests/test_codes_master.py -v` — schema match + no duplicate compound keys + code_system ⊆ {MS-DRG, APC, HCPCS-II} + source ⊆ {puf_top100, puf_top100_summary, puf_all_drgs_ry23, puf_outpatient, puf_physician} + source_host ⊆ {cms_gov, nber, wayback} + scope == "national" + codes_master row count UNCHANGED at 1,843,825. Spot-check: `duckdb -c "SELECT source, COUNT(*) FROM 'data/silver/empirical_means.parquet' GROUP BY 1 ORDER BY 1"` returns puf_all_drgs_ry23=6646, puf_outpatient=590, puf_physician=90334, puf_top100=200, puf_top100_summary=300.
- **Landmines**:
  - **Subdirectory isolation**: PUF parquets live at `silver/per_source/puf/<source>/*.parquet`. DuckDB's non-recursive `silver/per_source/*.parquet` glob in `codes_master` does NOT pick them up — verified by `test_codes_master_unaffected_by_phase5`.
  - **Source label literals are load-bearing**. The transform's source-cardinality assertion fails loud if any extractor regresses to writing the URL string into the `source` column (Batch 2 review caught this in inpatient + outpatient initially; fixed to emit `_BRONZE_SOURCE`). Three-source-tag scheme on inpatient: `puf_top100` (FY2011-FY2012) / `puf_all_drgs_ry23` (FY2013+, renamed Session 8.5; `puf_all_drgs_legacy` reserved) / `puf_top100_summary` (inpatient_summary FY2011-FY2013); Phase 7 will introduce `formula_no_empirical` for the FY2011-FY2012 non-top-100 long-tail.
  - **`source` is in the compound key** so per-provider and summary rows for the same `(DRG, FY)` cell coexist. Phase 7 cost-compute prefers per-provider when both present.
  - **`source_host` literal vocabulary** is `{cms_gov, nber, wayback}`. End-of-Session-8.5 distribution: `cms_gov`=97,680; `nber`=390 (CY2011-CY2013 outpatient + FY2011-FY2013 inpatient summary); `wayback`=0 (no row currently sourced from Wayback in the locked baseline, but the resolver supports it).

### Phase 5 cross-cutting reference (URL ladder, Socrata IDs, schema migration)

Companion to the per-source refresh blocks above. This section consolidates the cross-cutting Session 8.5 contracts: the canonical 3-tier URL resolution ladder, the Socrata dataset IDs that pin replication, the dual-concept payment-column schema migration, and the re-verification cadence anchored on manuscript-supplementary-materials freeze.

### What

- Per-source PUF aggregates (per-DRG inpatient, per-APC outpatient, per-(HCPCS, POS) physician) into national-mean silver.
- 4 extractors + 1 transform; 27-column locked schema; row counts: inpatient 6,846, summary 300, outpatient 590, physician 90,334; `empirical_means.parquet` 98,070.
- New schema columns (Session 8.5): `payment_amount` (NOT NULL float64), `payment_amount_concept` (NOT NULL string enum `{medicare_allowed, total_payment_allpayer}`), `total_payment_amount` (nullable float64; populated when `concept = total_payment_allpayer`). See CLAUDE.md schema decisions for full column definitions; not duplicated here.

### Where (URLs and bronze layout)

- Per-CY/per-FY URL dicts live module-local in each extractor (`_PUF_INPATIENT_SOURCES`, `_PUF_INPATIENT_SUMMARY_SOURCES`, `_PUF_OUTPATIENT_SOURCES`, `_PUF_PHYSICIAN_SOURCES`) — NOT centralized in `src/config.py`.
- Bronze tree: `data/bronze/puf_{inpatient,inpatient_summary,outpatient,physician}/{fy|cy}{YYYY}/{download_date}/`.
- Silver tree: `data/silver/per_source/puf/{inpatient,inpatient_summary,outpatient,physician}/puf_*_{fy|cy}{YYYY}.parquet`.
- Unioned silver: `data/silver/empirical_means.parquet`.

### URL resolution ladder (canonical pattern)

Order of precedence — first truthy hit wins:

1. **cms.gov canonical** — current `data.cms.gov` UUID-prefixed `sites/default/files/{YYYY-MM}/{uuid}/...` paths.
2. **NBER mirror** — `https://data.nber.org/providerchargepuf/{YYYY}/...`. Captures legacy releases CMS retired around 2020.
3. **Wayback CDX** — `web.archive.org/cdx/search/cdx?url=...` (currently no live wayback URLs in `_PUF_*_SOURCES`; reserved for future use).
4. **`None` → log+skip** — caller emits stderr message and the bulk runner continues with the next cell.

Helper: `src/extract/_url_resolver.py:resolve_with_ladder(cms_url, nber_url=None, wayback_url=None) -> tuple[str, str] | None`. Returns `(url, source_host)` where `source_host ∈ {"cms_gov", "nber", "wayback"}` and is stamped onto every emitted silver row.

**Important pattern — NBER is a TIER-1 source, NOT a fallback-of-last-resort.** Sessions 8 and 8.5-first-pass repeatedly declared NBER-mirrored PUFs "unrecoverable" because they only checked cms.gov canonical paths. NBER MUST be probed BEFORE giving up. Documented here so future application to non-PUF extractors (HCPCS pre-CY2017, OPPS/PFS pre-2017) is automatic — apply lazily only when those extractors are next touched (out of Session 8.5 scope).

### Socrata dataset IDs (replication-package metadata, NOT consumed at extract time)

The hardcoded UUID URLs in `_PUF_*_SOURCES` are a frozen snapshot for this analytic build (one-pass paradigm). For replication / re-runs after manuscript freeze, resolve current URLs via the data.cms.gov data.json catalog (`https://data.cms.gov/data.json`) using these stable dataset IDs:

- Inpatient (by Provider and Service): `690ddc6c-2767-4618-b277-420ffb2bf27c`
- Outpatient (by Provider and Service): `ccbc9a44-40d4-46b4-a709-5caa59212e50`
- Physician (by Provider and Service): `92396110-2aed-4d63-a6a2-5d6207d46a29`
- Inpatient Summary (by Geography and Service): `2941ab09-8cee-49d8-9703-f3c5b854e388`

Resolution endpoint (when needed): `https://data.cms.gov/data-api/v1/dataset-resources/{dataset_id}`.

### Refresh routines (operational commands)

Per-FY / per-CY single extract:

```
python3 -m src.extract.puf_inpatient --fy 2024
python3 -m src.extract.puf_inpatient_summary --fy 2013
python3 -m src.extract.puf_outpatient --cy 2023
python3 -m src.extract.puf_physician --cy 2023
```

Bulk re-extract end-to-end (Session 8.5 reference):

```
python3 -m src.extract.puf_inpatient --fy-range 2011-2024
python3 -m src.extract.puf_inpatient_summary --fy-range 2011-2013
python3 -m src.extract.puf_outpatient --cy-range 2011-2023
python3 -m src.extract.puf_physician --cy-range 2012-2023
python3 -m src.transform.empirical_means
```

Force a fresh bronze pull (override write-once cache): append `--download-date YYYY-MM-DD`.

### Cadence

- One-pass paradigm. URLs frozen until manuscript-supplementary-materials freeze.
- At manuscript freeze: re-resolve all 4 PUF URLs via the Socrata dataset IDs above against the current data.cms.gov data.json catalog. If UUIDs have rotated, update `_PUF_*_SOURCES` and re-run the bulk extract.
- Wayback fallback layer remains unwired by default — surface it only if cms.gov + NBER both miss for a previously-working cell.

### `mean_allowed` → `payment_amount` migration note

Any historical script / notebook that reads the Phase 5 silvers and references `mean_allowed` will fail post-Session-8.5. Migration:

1. Rename column references: `mean_allowed` → `payment_amount`.
2. Add concept-filtering: `WHERE payment_amount_concept = 'medicare_allowed'` for Medicare-only allowed analyses, OR `WHERE payment_amount_concept = 'total_payment_allpayer'` for all-payer total analyses, OR pivot to dual-concept analyses by reading both via `payment_amount` + `payment_amount_concept`.
3. For inpatient consumers wanting the all-payer total explicitly, the self-documenting alias column `total_payment_amount` is populated when `concept = total_payment_allpayer` (equal to `payment_amount`), NULL otherwise.

### Verification (Session 8.5 invariants)

After bulk re-extract, confirm:

```
python3 -c "
import duckdb
print('empirical_means:', duckdb.execute(\"SELECT count(*) FROM 'data/silver/empirical_means.parquet'\").fetchone()[0])  # 98070
print('null payment_amount:', duckdb.execute(\"SELECT count(*) FROM 'data/silver/empirical_means.parquet' WHERE payment_amount IS NULL\").fetchone()[0])  # 0
print('null concept:', duckdb.execute(\"SELECT count(*) FROM 'data/silver/empirical_means.parquet' WHERE payment_amount_concept IS NULL\").fetchone()[0])  # 0
"
python3 -m src.transform.codes_master  # codes_master.parquet stays at 1,843,825 rows
python3 -m pytest tests/ -v             # all tests green
```

### CY2012 physician — known unrecoverable

NBER mirror (`https://data.nber.org/providerchargepuf/2012/phys2012.csv.zip`) ships a 303 MB ZIP wrapping a 1.66 GB CSV with 9.15M records — but per NBER's `desc_phys2012.txt`, HCPCS codes and descriptions are explicitly stripped. Without HCPCS the file cannot populate the silver `(HCPCS_Cd, Place_Of_Srvc)` compound key. Wayback Machine has zero archived snapshots of the original cms.gov HCPCS-retained version. Phase 7 cost-compute on CY2012 physician cohorts must fall back to formula-only (PFS RVU × CF). Session 8.5 Step 1 finding (more precise than Session 8's "cms.gov 404s + Wayback empty" reason).

### HCRIS Cost Center Supplemental (Phase 6) — DEFERRED to Session 9.5

- **What**: CMS's 19-cost-center national CCRs (cost-to-charge ratios), used by CMS for MS-DRG relative-weight calibration and by KG-COST for cost concept #3 (`ccr_cost = charge × ccr_value`).
- **Where**: published with each FY's IPPS Final Rule. URL discovery 2026-05-01 found that the file labeled "Cost Center Supplemental Data File" is a definition/dictionary table mapping `cost_center_group → (MedPAR field, UB-04 revenue codes, HCRIS Worksheet C/D-3 cell refs)`, NOT a CCR-value table. Real CCR values must be computed from the separate provider-level HCRIS SAS dataset (`*-hcris-data-file.zip`, ~24 MB per FY × 12 FYs FY2015+).
- **How**: `[DEFERRED to Session 9.5 — PI Option C, 2026-05-01]`. Proposed 4-step path: (1) `src/extract/hcris_cost_centers.py` pulls FY2021+ definition table (XLSX/TXT, 6 cells); (2) `src/extract/hcris_data.py` pulls FY2015+ provider-level SAS dataset (12 cells, needs `pyreadstat` dep); (3) `src/transform/hcris_ccrs.py` joins definition + provider data, computes `ccr_value = sum(cost_cells) / sum(charges_cells)` per (fy, cost_center_group), aggregates to national mean; (4) re-run `src/transform/code_costs.py` to populate the three CCR columns and flip the `test_phase6_deferred_ccr_all_null` sentinel.
- **Cadence**: annual (each FY's IPPS Final Rule publication, ~August preceding FY).
- **Verification**: TBD. Sanity bands (when shipped): `ccr_value` operating-CCR range 0.10-0.60. Spot check Operating Room cost center 0.15-0.30.
- **Landmines**: (a) FY2024 cost-center URL has anomalous slug `fy2024-ipps-fr-hcris-supplemental-data-file.zip` (looks like 23 MB provider data file but is the 16 KB definition table — disambiguate by Content-Length); (b) FY2024 has a `proposed-rule` lookalike URL with WRONG content (returns 200, do not use); (c) FY2008-FY2014 unrecoverable as Final-Rule-package artifact — fall back to NBER HCRIS mirror or CMS HCRIS quarterly cost report PUF; (d) cost-center taxonomy is stable but adds groups by FY (e.g., Cell Therapy Drugs in FY2026) — key on `(fy, cost_center_group)` not just `cost_center_group`. (e) Optional HCUP CCR-NIS overlay if hospital-level CCRs are ever needed later.
- **Discovery artifact**: `plans/archive/discovery/hcris-url-discovery.md` (full URL table FY2008-FY2026 + content sniff of FY2026 sample + paste-ready `_HCRIS_CCR_BY_FY` dict for `src/config.py`).
- **Flags queue**: `plans/archive/sessions/Session 9 flags.md` FLAG 1 (4 sub-flags 1a/1b/1c/1d).

### Phase 7a — Three cost concepts (Session 9, shipped)

- **What**: gold parquet `data/gold/code_costs.parquet` joining 6 silver tables into a unified `(code, code_system, fy_or_cy, setting)` row set carrying three independent cost concepts: `charge`, `allowed_payment`, `ccr_cost`. Each concept tagged with `*_source` and `*_n` so query consumers can filter on confidence. **Three-concept independence rule**: no cross-substitution between concepts (`charge` ships NULL when no PUF, `allowed_payment` ships NULL when no formula AND no empirical, `ccr_cost` ships NULL until Phase 6 ships).
- **Where**: `src/transform/code_costs.py` reads `data/silver/{empirical_means, ms_drg_weights, ipps_std_amounts, pfs_rates, opps_apc_rates, opps_hcpcs_rates, asc_rates}.parquet`. (Does NOT read `national_ccr.parquet` — Phase 6 deferred; `ccr_cost` columns ship NULL.)
- **How**: `python3 -m src.transform.code_costs` → `data/gold/code_costs.parquet` (496,792 rows, ~5.3 MB). Wall time 0.43s. DuckDB UNION ALL of 5 per-setting CTEs.
- **Cadence**: re-run after any silver refresh (especially `empirical_means`, `pfs_rates`, `opps_*_rates`, `asc_rates`, `ms_drg_weights`, `ipps_std_amounts`).
- **Verification**: `python3 -m pytest tests/test_code_costs.py -v` → 12 tests pass. Spot-check via duckdb: `SELECT setting, COUNT(*) FROM 'data/gold/code_costs.parquet' GROUP BY 1`. Median charge/allowed_payment ratio target ≥2.0; observed 6.21x.
- **Landmines**: see `docs/archive/phase-7-cost-compute-notes.md` (10 landmines) — most important: (1) `payment_amount_concept` never NULL when source=puf_empirical; (2) DRGs 998/999 unconditional 0 (override FY2008 silver rw=42.0 quirk); (3) FY2010 default rate_period=2nd_half; (4) CY2026 PFS prefer nonqp; (5) quarter='A' canonical (yields ~497K rows, NOT plan's 1.5M target); (6) PFS modifier collapse to global-first; (7) `formula_no_empirical` carve-out 1,294 rows for FY2011/FY2012 inpatient long-tail; (8) three-concept independence one-directional in current build (no charge-only sources in Phase 5 PUFs).

### Phase 7b — Deterministic ICD→DRG layer (Session 9, shipped)

- **What**: TWO gold parquets — `data/gold/icd_principal_drg_reachability.parquet` (every reachable DRG per `(icd_cm, fy)`, 703,053 rows) and `data/gold/icd_principal_drg_modal.parquet` (one modal DRG per `(icd_cm, fy)`, 296,662 rows). Replaces the originally-spec'd NIS-derived probabilistic edge with a free, deterministic alternative. **Schema decision LOCKED**: graph edges become `CAN_ANCHOR {fy, source}` + `IS_MODAL_FOR {fy, source}` (was `IS_PRINCIPAL_IN {p, n, fy, source}`).
- **Where**: `src/transform/icd_drg.py` reuses `src/grouper/grouper.py:assign(record, fy)` (single persistent JVM, MsdrgComponent cached per version). Reads `data/silver/codes_master.parquet` filtered to `code_system='ICD-10-CM' AND fy_or_cy IN (2023,2024,2025,2026)`.
- **How**: `python3 -m src.transform.icd_drg --fy-range 2023-2026 --output-dir data/gold/`. For each ICD, runs 5 synthetic-claim permutations (baseline / with_mcc / with_cc / with_or_proc / male_baseline; newborn override age=0/sex=F/dispo=03 for P-prefix codes). Per-FY checkpointing under `_checkpoint_*_fy{fy}.parquet`. KeyboardInterrupt flushes partial work. Wall time: 4 min for FY2023-FY2026 (~1.5M grouper calls @ 7,500 calls/s post-warmup; 24× faster than plan's estimate).
- **Cadence**: re-run when grouper JARs change (annually with each new V*.0 release) or codes_master ICD-10-CM expands.
- **Verification**: `python3 -m pytest tests/test_icd_drg.py -v` → 21 pass + 1 expected skip (pre-MDC test skips because pre-MDC DRGs need procedures, not principal dx alone). Hand-curated set: 10 (icd, fy, expected_drg_set, expected_mdc) tuples calibrated against bulk-run truth (4 plan-v12.md guesses corrected). Newborn MDC-15 routing target ≥90%, observed 96.4%. Ungroupable rate target <1% AFTER excluding chapter-XX V/W/X/Y external-cause codes (which CMS MCE rejects as principal by code-set rule); observed 0.0% non-V/W/X/Y, 9.99% raw.
- **Coverage**: FY2023-FY2026 (Java V40+ grouper available). FY2019-FY2022 drgpy fallback DEFERRED to Session 9.5 — no production drgpy wrapper in `src/grouper/` yet, only inside `tests/test_drgpy_cross.py`. Building one is the Session 9.5 work item; afterwards re-run `--fy-range 2019-2026`. Pre-FY2019 ICD-10 (FY2016-FY2018) and ICD-9 era indefinitely deferred — no grouper coverage.
- **Landmines**: (a) Permutation-set design choice (5 perms): baseline = no MCC/no procedure, so most modal DRGs anchor on baseline by 3-vote majority; multi-perm modals only surface for OR-procedure-anchoring or MCC-anchoring codes. (b) Pre-MDC DRGs (001-018) require specific OR procedures to trigger; our single 0DTJ4ZZ (laparoscopic appendectomy) doesn't hit them — `test_premdc_drg_001_to_018_nonzero_mdc` is a defensive guard for future permutation expansions. (c) Grouper returns DRG=999/MDC=00 on `GrouperError` per-perm; the row is emitted so the gap is visible. (d) Chapter XX V/W/X/Y external causes are 100% ungroupable as principal — by ICD-10-CM code-set design, not a defect. Test threshold filters them out.

## Build pipeline (re-run from scratch)

`[NOT YET BUILT — Phases 7–9 will define this]`

Pipeline order: `bronze → silver → gold → neo4j_import → graph`. Document per stage: which scripts, in what order, expected wall-clock, where to resume after a failure.

## Common operations

Per-source refresh routines live in their own sections above. The lookups here are cross-cutting one-liners that the routines reference.

- Re-build all unioned silver tables: `python3 -m src.transform.codes_master && python3 -m src.transform.pfs_rates && python3 -m src.transform.opps_rates && python3 -m src.transform.asc_rates && python3 -m src.transform.ipps_tables`
- Refresh just one quarter of PFS: `python3 -m src.extract.pfs_rvu --cy 2024 --quarter D` then `python3 -m src.transform.pfs_rates`
- Re-pull a single source with a fresh bronze date: `python3 -m src.extract.<source> --fy <YYYY> --download-date 2026-MM-DD` (works for all extractors; bronze layout is `{source}/{period}{year}/{download_date}/`)
- Run the full validation suite: `python3 -m pytest tests/ -v`
- Run validation for one source: `python3 -m pytest tests/test_<source>.py -v`
- DuckDB ad-hoc spot-check on the unioned PFS table: `duckdb -c "SELECT setting, conversion_factor_kind, COUNT(*) FROM 'data/silver/pfs_rates.parquet' WHERE cy = 2024 GROUP BY 1,2 ORDER BY 1,2"`
- `[NOT YET BUILT — Phases 4+]` Re-group a cohort: `python3 -m src.query.cohort_annotator --input <file> --output <file>`
- `[NOT YET BUILT — Phases 8+]` Drop and reimport graph: `docker compose -f neo4j/docker-compose.yml down -v && bash src/load/neo4j_import.sh`

## Troubleshooting

`[Append as discovered — same convention as CLAUDE.md Landmines.]`

- CMS URL redirects / dead links: check archive.org Wayback Machine; CMS reorganizes annually
- Java grouper version mismatch: confirm FY of discharge maps to the right JAR (e.g. discharge 2025-10-01 → FY2026 → V43.1)
- Neo4j memory tuning if bulk import OOMs: raise `dbms.memory.heap.max_size` in `neo4j/docker-compose.yml`

## Source URL registry

`[NOT YET BUILT — populated when src/config.py exists]`

Mirror of the URL constants in `src/config.py`, in human-readable form, so a non-Python collaborator can audit sources without grepping code.

## Session 9.5 commands (2026-05-02)

Phase 6 (HCRIS → national CCR) shipped + Phase 7b drgpy backfill (FY2019-FY2022) shipped. Commands below are operational lookups; full schema/state lives in CLAUDE.md "Current state (end of Session 9.5, 2026-05-02)".

### Phase 6 — HCRIS pipeline

- Extract HCRIS cost-center definition table (FY2021+; ~9 sec wall-clock):
  ```bash
  python3 -m src.extract.hcris_cost_centers --fy-range 2021-2026
  ```
  Output: `data/silver/per_source/hcris/cost_centers/hcris_cost_centers_fy*.parquet` + unioned `data/silver/hcris_cost_centers.parquet` (520 rows, 88 × 6 FYs). FY2024 sub-flag 1b: rejects payloads ≥1MB Content-Length, warns >100KB.
- Extract HCRIS provider-level SAS (FY2015+; ~54 sec wall-clock; needs `pyreadstat>=1.2`, ships as 1.3.4):
  ```bash
  python3 -m src.extract.hcris_data --fy-range 2015-2026
  ```
  Output: `data/silver/per_source/hcris/data/hcris_data_fy{fy}.parquet` (~5,941-6,199 providers × ~290 cols per FY, trimmed from ~5,600-col raw SAS matrix).
- Compute national-mean CCRs:
  ```bash
  python3 -m src.transform.hcris_ccrs
  ```
  Output: `data/silver/national_ccr.parquet` (228 rows = 12 FYs × 19 cost-center groups). Joins definition table + provider data, computes `ccr_value = sum_cost / sum_charges` per (fy, cost_center_group, provider) → national mean. Canonicalizes 5 FY2024+ verbose group names to FY2021-2023 short form.
- Rebuild Phase 7a gold with CCR overlay:
  ```bash
  python3 -m src.transform.code_costs
  ```
  Output: `data/gold/code_costs.parquet` (496,792 rows; 5,473 inpatient rows now carry `ccr_cost = charge × ccr_value`, `ccr_cost_center='Operating Room'` default).

### HCRIS NBER long-form (FY2011–FY2014) — Phase 11 PARTIAL / Session 1 of PHASES-2

> **STATUS — Phase 11 PARTIAL ship (2026-05-02).** Code path is correct in principle for Form 2552-10 (FY2011–FY2014) but `_FORWARD_FILL_BACK_TO_FY = 2015` in `src/transform/hcris_ccrs.py` gates the downstream transform — running this recipe end-to-end TODAY produces FY2011–FY2014 silvers but `national_ccr.parquet` stays at 228 rows because `hcris_ccrs.py` skips fy < 2015. **FY2008–FY2010 (Form 2552-96) is hard-blocked at `hcris_data_nber.extract_fy` and short-circuits with a clear log message** — it requires a worksheet-mapping module that doesn't exist yet (Form 2552-96 has no Worksheet D-3; uses D-0/D-1/D-4 with alpha sub-line indicators). Full backfill deferred to Session 1.5; PI verdict on FY2008–FY2010 strategy gates the next session — see `plans/archive/sessions/Session 1 flags.md` FLAG 5.

**What**: NBER mirror of the HCRIS provider-level cost-report cell matrix. Schema: `(rpt_rec_num, wksht_cd, line_num, clmn_num, itm_val_num)`. Form 2552-96 for FY2008–FY2010 **(currently short-circuited)**; Form 2552-10 for FY2011–FY2014 **(currently functional but gated by hcris_ccrs floor)**.

**Where**:
- `https://data.nber.org/hcris/2552-96/hosp_nmrc2552_96_{2008,2009,2010}_long.csv` (Form 2552-96; **deferred — Session 1.5**)
- `https://data.nber.org/hcris/2552-10/hosp_nmrc2552_10_{2011,2012,2013,2014}_long.csv` (Form 2552-10; **functional but unused until floor lowers**)

Per-FY size: 240 MB (FY2010) – 800 MB (FY2013/2014). HEAD-probed 2026-05-02 in `plans/archive/discovery/hcris-url-discovery.md`.

**How** (current, partial state):
```bash
# Single-FY pull (Form 2552-10 only):
python3 -m src.extract.hcris_data_nber --fy 2014   # works
python3 -m src.extract.hcris_data_nber --fy 2008   # short-circuits with FLAG 5 log

# Routed transparently via the dispatcher:
python3 -m src.extract.hcris_data --fy 2014        # equivalent
```

**How** (Session 1.5 future-state — ALL FYs functional):
```bash
# Bulk pull (~3 GB FY2011-FY2014, or ~4 GB if FY2008-FY2010 strategy lands):
python3 -m src.extract.hcris_data --fy-range 2011-2014   # current usable range
python3 -m src.extract.hcris_data --fy-range 2008-2014   # post-Session-1.5

# Then rebuild downstream:
python3 -m src.transform.hcris_ccrs       # 228 → 304 rows (FY2011-2026) or 361 (FY2008-2026)
python3 -m src.transform.code_costs       # ccr_cost populated for added FYs
```

**Cadence**: One-time. NBER long-form is frozen historical data; no need to re-pull.

**Verification** (future-state — once Session 1.5 lands):
```bash
# Silver row counts (per FY):
python3 -c "
import pyarrow.parquet as pq, glob
for p in sorted(glob.glob('data/silver/per_source/hcris/data/hcris_data_fy*.parquet')):
    print(p.split('/')[-1], pq.ParquetFile(p).metadata.num_rows, 'rows')
"
# Each FY should report ≥ 5,000 provider rows (post-Session-1.5).

# National CCR row count:
python3 -c "
import duckdb
df = duckdb.sql(\"SELECT fy, COUNT(*) AS n FROM 'data/silver/national_ccr.parquet' GROUP BY fy ORDER BY fy\").df()
print(df.to_string())
print('Total:', df['n'].sum())  # current 228; post-Session-1.5: 304 (FY2011+) or 361 (FY2008+)
"

# code_costs ccr_cost backfill (post-Session-1.5):
python3 -c "
import duckdb
df = duckdb.sql(\"\"\"
  SELECT fy, COUNT(*) AS rows, SUM(CASE WHEN ccr_cost IS NOT NULL THEN 1 ELSE 0 END) AS populated
  FROM 'data/gold/code_costs.parquet'
  WHERE setting='inpatient' AND fy BETWEEN 2011 AND 2014
  GROUP BY fy ORDER BY fy
\"\"\").df()
print(df)
"
```

**Storage** (future-state):
```bash
du -sh data/bronze/hcris_data_nber/  # ~3 GB (FY2011-2014) or ~4 GB (FY2008-2014)
du -sh data/silver/per_source/hcris/data/  # ≤ 4.5 GB total (NBER + cms.gov)
```

### Phase 7b — drgpy backfill (FY2019-FY2022)

- Use the unified grouper dispatcher; FY routes to drgpy v36-v39 automatically:
  ```python
  from src.grouper.grouper import assign
  result = assign(record, fy=2020)         # → drgpy v37
  result = assign(record, fy=2024)         # → Java V42.0
  result = assign(record, fy=2023, version="v40")  # lower-case forces drgpy override
  ```
  Direct module: `src/grouper/drgpy_wrapper.py:assign(record, fy, version=None) -> GrouperResult`. FY map FY2019→v36 / FY2020→v37 / FY2021→v38 / FY2022→v39 / FY2023→v40 (override).
- Bulk-run icd_drg end-to-end (drgpy FY2019-2022 + Java FY2023+):
  ```bash
  python3 -m src.transform.icd_drg --fy-range 2019-2026
  ```
  **Caveat**: the script's `_union_checkpoints` step only unions the FYs run during the current invocation. If FY2019-FY2026 are run across multiple invocations, the final parquet ends up holding only the most-recent batch. To re-union ALL 8 FY checkpoints into the canonical gold parquets:
  ```bash
  python3 -c "
  import duckdb
  con = duckdb.connect()
  con.execute(\"COPY (SELECT * FROM read_parquet('data/gold/_checkpoint_modal_fy*.parquet', union_by_name=true) ORDER BY fy, icd_cm) TO 'data/gold/icd_principal_drg_modal.parquet' (FORMAT PARQUET)\")
  con.execute(\"COPY (SELECT * FROM read_parquet('data/gold/_checkpoint_reachability_fy*.parquet', union_by_name=true) ORDER BY fy, icd_cm, ms_drg) TO 'data/gold/icd_principal_drg_reachability.parquet' (FORMAT PARQUET)\")
  "
  ```
  All 8 FY checkpoints must exist at `data/gold/_checkpoint_{modal,reachability}_fy{2019..2026}.parquet`. Expected output: modal 586,147 rows; reachability 1,169,186 rows.

### FY2008 reserved-DRG silver re-run (FLAG 5)

- Projection-layer fix lives in `src/transform/ipps_tables.py` (sets `relative_weight=0.0` for reserved DRGs 998/999). Re-run after pulling the fix:
  ```bash
  python3 -m src.transform.ipps_tables
  ```
  Defense-in-depth override is also retained in `code_costs.py`.

### Phase 8/9 — Neo4j schema + bulk import (shipped Session 10)

- Build the 12 node CSVs + 13 rel CSVs to `data/neo4j_import/` (DuckDB, idempotent, ~4 sec end-to-end):
  ```bash
  python3 -m src.load.build_csvs
  # Inspect counts: cat data/neo4j_import/_build_summary.json
  # Single-CSV mode for iteration: python3 -m src.load.build_csvs --label HCPCS
  # Skip pre-write referential check: --skip-referential-check
  ```
- Run the full import end-to-end (probes Docker, builds-if-stale, stops Neo4j, `neo4j-admin database import full`, restarts, applies schema + post-load Cypher, queries 4 sample queries, verifies counts within ±5%):
  ```bash
  python3 -m src.load.run_import
  # Probe-only (does not touch Neo4j):    --check-only
  # Reuse existing CSVs:                   --skip-build
  # Re-run only the live-graph queries:    --skip-import
  ```
  Outputs:
  - `data/neo4j_import/_load_summary.json` — node + rel counts by label / type
  - `data/neo4j_import/_smoke_results.json` — 4 sample-query result rows
- Run only the import shell (assumes CSVs are already built; useful for ad-hoc reloads):
  ```bash
  bash src/load/neo4j_import.sh
  ```
- Apply schema + post-load Cypher manually (after `docker compose up -d`):
  ```bash
  docker compose -f neo4j/docker-compose.yml exec -T neo4j \
    cypher-shell -u neo4j -p kgcostlocal -f /cypher/schema.cypher
  docker compose -f neo4j/docker-compose.yml exec -T neo4j \
    cypher-shell -u neo4j -p kgcostlocal -f /cypher/post_load.cypher
  ```
  Both files are mounted at `/cypher/` inside the container via the `../src/load:/cypher:ro` bind.
- Interactive cypher-shell exploration:
  ```bash
  docker compose -f neo4j/docker-compose.yml exec neo4j \
    cypher-shell -u neo4j -p kgcostlocal
  # Then in the prompt:
  #   MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n ORDER BY n DESC;
  #   MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC;
  ```
- Run the live-Neo4j smoke test (opt-in, requires Docker running):
  ```bash
  KG_COST_RUN_NEO4J=1 python3 -m pytest tests/test_neo4j_smoke.py -v
  ```
  Wall-time budget: ~5 min. Tears down container at end (data volume in `neo4j/data/` preserved).

### Phase 8/9 — Resume after a docker-down skip

`run_import.py` exits 0 with a one-line resume command if Docker is unreachable. To resume on macOS:
```bash
open -a Docker  # wait for the daemon to bind
python -m src.load.run_import
```
On Linux: `sudo systemctl start docker` (or `colima start`), then re-run `run_import`.

## Switching Neo4j targets

Two import modes ship as of Session 10.5 (selected via `KG_COST_IMPORT_MODE` in `.env` or shell env):

- **`docker`** (default) — uses `neo4j/docker-compose.yml` (image `neo4j:5.20-community`, APOC auto-installed). Bolt at `bolt://localhost:7687`, default password `kgcostlocal`. `src/load/neo4j_import.sh` handles the offline import inside the container.
- **`local-admin`** — points `neo4j-admin` at a local Neo4j Desktop / standalone install at `KG_COST_NEO4J_HOME`. Runs offline (DBMS stopped, import, DBMS restarted). No Docker required.

### `.env` snippets

`docker` mode (default — works as-is with docker-compose stack):

```bash
# .env (docker mode)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=kgcostlocal
NEO4J_DATABASE=neo4j
KG_COST_IMPORT_MODE=docker
# KG_COST_NEO4J_HOME unset
KG_COST_RUN_NEO4J=1   # opt the live-Neo4j smoke tests into pytest
```

`local-admin` mode (Neo4j Desktop 2026.04.0):

```bash
# .env (local-admin mode)
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<your-neo4j-password>
NEO4J_DATABASE=neo4j
KG_COST_IMPORT_MODE=local-admin
KG_COST_NEO4J_HOME=<path-to-neo4j-dbms-directory>/dbms-<UUID>
KG_COST_RUN_NEO4J=1
```

### Setup recipe — `local-admin` mode (Neo4j Desktop)

Run these once when first targeting a Desktop instance, before `python3 -m src.load.run_import`:

```bash
# 1. Find the Desktop DBMS directory matching the instance UUID (UI shows it as "DBMS ID")
find "$HOME/Library/Application Support/Neo4j Desktop" \
     -type d -name "dbms-*" 2>/dev/null
find "$HOME/Library/Application Support/com.Neo4j.Relate" \
     -type d -name "dbms-*" 2>/dev/null
# Newer Desktop layouts also use ~/Library/Application Support/com.neo4j.relate-desktop/

# 2. Confirm neo4j-admin works (expect a 2026.04.x version string)
"$KG_COST_NEO4J_HOME/bin/neo4j-admin" --version

# 3. Confirm import dir is writable + APOC plugin is present
ls -la "$KG_COST_NEO4J_HOME/import/"
ls -la "$KG_COST_NEO4J_HOME/plugins/" | grep -i apoc

# 4. Confirm DBMS state controls work
"$KG_COST_NEO4J_HOME/bin/neo4j" status
```

If `neo4j-admin --version` does not return a 2026.04.x string, abort and escalate before writing data — the JAR layout / API may differ.

### Operational notes

- **Java 21 isolation** — Neo4j Desktop ships a Zulu 21 JDK in its `Cache/runtime/zulu21*-jdk*` dir. `_run_local_admin_import` resolves it automatically via glob and exports `JAVA_HOME`/`PATH` only into the subprocess `env=` dict (never `os.environ`), so the project's default Java 17 (used by Phase 4 grouper) remains the shell's `java`. No interaction between modes.
- **Live-Neo4j smoke tests** — `KG_COST_RUN_NEO4J=1` (set in `.env` or shell env) opts `tests/test_neo4j_smoke.py` into pytest. Without it, those tests skip. Gating also requires either `docker info` reachable (docker mode) OR `KG_COST_NEO4J_HOME` set + valid (local-admin mode).
- **Resume command** (either mode) — `python3 -m src.load.run_import`. Idempotent: detects stale CSVs, rebuilds if needed, dispatches to the right `_run_*_import()` function based on `KG_COST_IMPORT_MODE`. Writes `data/neo4j_import/_load_summary.json` + `_smoke_results.json`.
- **Switching modes mid-session** — Just edit `.env` and re-run `python3 -m src.load.run_import`. The orchestrator does not assume any prior state in the target DBMS; `--overwrite-destination=true` clears it on every import.

## Phase 10 cohort annotation

### Annotator CLI

**Parquet mode** (no Neo4j required; reads `data/gold/code_costs.parquet` + `data/gold/icd_principal_drg_modal.parquet` directly):

```bash
python3 -m src.query.cohort_annotator \
    --input tests/fixtures/sample_cohort.csv \
    --output /tmp/sample_cohort_with_costs.csv \
    --source parquet
```

**Bolt mode** (Neo4j required; uses `.env` config via `src/load/_config.py:get_neo4j_config()`):

```bash
python3 -m src.query.cohort_annotator \
    --input tests/fixtures/sample_cohort.csv \
    --output /tmp/sample_cohort_with_costs_bolt.csv \
    --source bolt
```

Both modes append the same 17 cost columns; bolt-vs-parquet numeric parity is asserted at 0.0% drift across 7 numeric columns × 30 fixture rows.

### Confidence floor + scope (Session 2 of PHASES-2)

The annotator supports two new flags introduced in Session 2: `--confidence-floor {none,approximate,high}` (default `none`) and `--scope {ip,op,all}` (default `ip`). The floor controls which inpatient routing paths are accepted; the scope controls which output columns are nulled/zeroed on rejected rows.

```bash
# Default: floor=none, scope=ip — passes everything through.
python3 -m src.query.cohort_annotator \
    --input tests/fixtures/sample_cohort.csv \
    --output /tmp/annotated.csv \
    --source parquet
# rows=32 ok=30 no_match=2 coverage=0.938

# Strict sensitivity: reject all bridged rows (only native FY>=2019 ICD-10 routing kept).
python3 -m src.query.cohort_annotator \
    --input tests/fixtures/sample_cohort.csv \
    --output /tmp/annotated_strict.csv \
    --source parquet \
    --confidence-floor high
# Rejected rows: P009/P010 (ICD-9 GEMs-bridged), P032 (FY2017 back-translated).
# Rejected -> annotation_status='no_match', notes contain 'confidence_floor_exceeded',
# inpatient_drg/title/cost/etc. cols all nulled, inpatient_confidence reset to 'none'.

# Strict + scope=all: also zero the 4 outpatient/CPT totals on rejected rows.
python3 -m src.query.cohort_annotator \
    --input tests/fixtures/sample_cohort.csv \
    --output /tmp/annotated_strict_all.csv \
    --source parquet \
    --confidence-floor high --scope all
# Same rejections as above, but outpatient totals also zeroed for the 3 rejected rows.
```

**`--confidence-floor` quick reference**:

| Floor       | Rows kept                                                                              |
|-------------|----------------------------------------------------------------------------------------|
| none (default) | All inpatient rows, regardless of how DRG was resolved.                              |
| approximate | Native FY>=2019 ICD-10 routing OR GEMs-bridged routing. Rejects unresolved rows only. |
| high        | Native FY>=2019 ICD-10 routing only. Rejects all GEMs-bridged + back-translated rows. |

**`--scope` quick reference**:

| Scope     | Effect on rejected rows                                                                 |
|-----------|-----------------------------------------------------------------------------------------|
| ip (default) | Null the 9 inpatient cost cols + reset inpatient_confidence='none'.                |
| all       | Same as 'ip' + zero the 4 outpatient/CPT total cols.                                    |
| op        | Reserved for Phase 8+ outpatient_confidence scoring; currently no-op.                   |

### Known limitations

- **GEMs version: 2018 final, forward direction only.** ICD-9-CM -> ICD-10-CM mappings are derived from `data/silver/gems_2018.parquet` filtered to `is_reimbursement=false`. Reverse direction (ICD-10 -> ICD-9) is not used by the annotator. Round-trip-recovery validation deferred to Phase 14 NTDB integration.
- **Trauma-specific overrides ship empty.** `src/query/gems_overrides.yaml` is a stub; manual curation is required before annotating trauma cohorts.
- **`outpatient_confidence` is a reserved schema column.** Always `None` in Phase 7. Phase 8+ work will populate based on HCPCS reorganization, APC reassignment, PFS site-of-service ambiguity, and modifier handling.
- **CPT-only rows are not subject to the confidence floor.** A row without `dx_principal` is never rejected by `--confidence-floor` even though its default `inpatient_confidence='none'` would otherwise trigger rejection. This is intentional: the structural `None` means "no inpatient routing requested", not "inpatient routing failed".

### Sample-query invocation

```python
from src.query._bolt import get_driver, run_query, list_queries
print(list_queries())  # 8 names: Q1_icd_modal_cost, ..., Q8_cost_distribution_by_year
with get_driver() as drv:
    rows = run_query(drv, "Q1_icd_modal_cost", icd_code="I214", fy=2024)
    print(rows)
```

`run_query()` honors the `// :param x => default` defaults inline in `src/query/sample_queries.cypher`, so any parameter you don't override falls back to the committed default. `list_queries()` is mtime-keyed: edits to `sample_queries.cypher` invalidate the cache without a process restart.

### Fixture

- `tests/fixtures/sample_cohort.csv` — 30-row hand-curated cohort. Per-row notes in `tests/fixtures/sample_cohort_README.md`.
- 27 `expected_match=true` rows (the test denominator for the cohort-coverage criterion in `tests/test_phase10_acceptance.py`).
- 3 `expected_match=false` rows: P009 / P010 are ICD-9-era (out of FY2016+ ICD-10 scope); P030 is bogus by design.

### Test entry points

```bash
# Always-on (no Neo4j required)
python3 -m pytest tests/test_phase10_acceptance.py -v
python3 -m pytest tests/test_sample_queries.py -k "not credible_values" -v
python3 -m pytest tests/test_cohort_annotator.py -k "not bolt" -v

# Live-bolt (8 sample-query tests + 3 annotator tests un-skip)
KG_COST_RUN_NEO4J=1 python3 -m pytest tests/test_sample_queries.py tests/test_cohort_annotator.py -v
```

Always-on baseline: 483 pass + 4 skip. Live-bolt: ~492+ pass.

### NTDB input-format (Session 3, 2026-05-03)

`cohort_annotator` now accepts `--input-format ntdb` to read NTDB `annual_cohort.parquet` directly (instead of CSV), via `src.query._ntdb_adapter`. Procedure attachment for FY2019-FY2024 ICD-10-PCS rows is automatic when the NTDB DuckDB is reachable (set `KG_COST_NTDB_DUCKDB` or pass `--ntdb-duckdb`; there is no default path, because the NTDB is restricted data and is not included in this repo). An unreachable DuckDB soft-fails to `pcs=''`.

**Smoke command** (1k-row sample):

```bash
mkdir -p data/derived
python3 -m src.query.cohort_annotator \
    --input-format ntdb \
    --input "<path-to-annual_cohort.parquet>" \
    --output data/derived/ntdb_smoke_1k.parquet \
    --source parquet \
    --sample-rows 1000
```

**Stratified smoke** (Session 4 of PHASES-2 — `--sample-strategy {head, random, stratified}` + `--sample-seed`; default `head` is back-compat). Stratified samples ≥1 row per FY against the FY-sorted source so smoke runs are FY-balanced:

```bash
python3 -m src.query.cohort_annotator \
    --input-format ntdb \
    --input "<path-to-annual_cohort.parquet>" \
    --output data/derived/ntdb_smoke_1k_stratified.parquet \
    --source parquet \
    --sample-rows 1008 \
    --sample-strategy stratified \
    --sample-seed 0
# Verify: icd10_back_translation_no_modal count <= 25 (Workstream B acceptance gate)
python3 -c "
import pandas as pd
df = pd.read_parquet('data/derived/ntdb_smoke_1k_stratified.parquet')
n = df['annotation_notes'].str.contains('icd10_back_translation_no_modal', na=False).sum()
print(f'icd10_back_translation_no_modal: {n} / {len(df)} = {n/len(df)*100:.2f}%')
"
```

**Output suffix dispatch**: `.parquet` / `.pq` -> `to_parquet`; anything else -> `to_csv`. Detected from `args.output.suffix.lower()`.

**Env vars / overrides**:

- `KG_COST_NTDB_DUCKDB`: path to the NTDB DuckDB used for procedure attachment. There is no default (the NTDB is restricted data and is not included in this repo). Resolution order: `--ntdb-duckdb` CLI arg -> env var. If neither is set and the cohort has FY2019-FY2024 rows with procedure attachment on, the adapter raises `RuntimeError`.
- `KG_COST_RUN_NTDB=1`: gates the live-data smoke test in `tests/test_ntdb_adapter.py::TestLoadNtdbCohortLive`.
- `KG_COST_NTDB_COHORT_PARQUET`: path to your own NTDB `annual_cohort.parquet`, read by the live tests in `tests/test_ntdb_adapter.py` and `tests/test_run_phase14c.py`. Unset means those tests skip.
- `KG_COST_MIMIC_HOSP_DIR`: path to the MIMIC-IV `hosp` directory (`drgcodes.csv.gz`, `diagnoses_icd.csv.gz`, and the other files listed in `tests/test_grouper_validation.py`). Unset means the MIMIC-IV validation tests skip.

**Procedure-attachment scope** (locked decision):

| FY range  | Source                                          | Format                            | Routing benefit                           | Attach? |
|-----------|-------------------------------------------------|-----------------------------------|-------------------------------------------|---------|
| 2007-2011 | `PUF_PCODE.csv` (ICD-9)                         | Per-year CSVs joined on `INC_KEY` | None (Branch D ignores PCS)               | No      |
| 2012-2016 | None recoverable as relational                  | Compiled CSVs ~99% null           | None (no data)                            | No      |
| 2017-2018 | DuckDB `v_icd_procedure_2017_2022` (ICD-10)     | Joined on `inc_key`               | None (Branch B back-translates)           | No      |
| 2019-2024 | DuckDB `v_icd_procedure_2019_2024` (ICD-10)     | Joined on `inc_key`               | High (Branch A grouper.assign uses PCS)   | Yes     |

**Soft-fail behavior**: if a DuckDB path is configured but unreachable (file moved, permissions, etc.) the adapter ships rows with `pcs=''` for FY2019-FY2024 and emits `warnings.warn("NTDB procedure attachment failed (...)")` to stderr. The smoke run still produces a valid output parquet; downstream Branch A routing falls back to modal lookup.

**Reproducibility checks** (run after smoke completes):

```bash
python3 -c "
import pandas as pd
df = pd.read_parquet('data/derived/ntdb_smoke_1k.parquet')
print(f'rows: {len(df)}')
print('confidence dist:'); print(df.inpatient_confidence.value_counts(dropna=False))
print('annotation_status:'); print(df.annotation_status.value_counts())
print('coverage by FY:')
print(df.groupby('fy').inpatient_confidence.value_counts(normalize=True).unstack(fill_value=0).round(2))
"
```

**Out of scope (deferred to Session 5 of PHASES-2 — gated on PI lock of SAP v3)**:

- Phase 14d — `v2/code/01_aggregate_kgcost_costs.py` (year × 15-category aggregation script).
- Phase 14e — Frame 1 counterfactual + Frame 3 LOS × per-diem rebind to KG-COST.
- Phase 14f — `--confidence-floor high` sensitivity re-run + side-by-side supplement table.
- Phase 14g — `manuscript.docx` Methods + Results revision; figure regeneration.

### Phase 14c full 10.08M-row run (Session 4 of PHASES-2, shipped 2026-05-04)

`src/query/run_phase14c.py` is an FY-shard wrapper around `cohort_annotator` for the downstream fracture-cost study's primary cost-engine output. Per-FY checkpoints under `data/derived/_checkpoints/` are restart-safe (skip-on-exists); final union uses an atomic `.partial`→final `os.replace` so an interrupted union cannot corrupt the final file.

**Run command** (`--source-parquet` is required: the path to your own NTDB `annual_cohort.parquet`):

```bash
mkdir -p data/derived data/derived/_checkpoints logs
python3 -m src.query.run_phase14c --source-parquet "<path-to-annual_cohort.parquet>"
# Optional flags (defaults shown):
#   --output-dir data/derived
#   --checkpoint-dir data/derived/_checkpoints
#   --fy-range 2007-2024
#   --no-attach-procedures   # skip DuckDB procedure attachment for FY2019-FY2024
```

Wall-time on a Mac M-series with the parquet backend: **~462 s** (~7.7 min) end-to-end across 18 FY shards. Per-FY shard elapsed: 6.8 s (FY2007 / 267k rows) → 67.7 s (FY2024 / 870k rows). Final output: `data/derived/cohort_annotated.parquet` (391 MB; 10,078,214 rows × 29 cols).

**Restart drill** (verify idempotence; the second invocation should `[skip]` every shard and re-emit the union):

```bash
rm data/derived/cohort_annotated.parquet
python3 -m src.query.run_phase14c --source-parquet "<path-to-annual_cohort.parquet>"
# Expect 18× "[skip] fy=YYYY checkpoint exists" + a fresh union write.
```

**Coverage diagnostic** (consumed by SAP v3 §8):

```bash
python3 -m src.query._phase14c_coverage > /tmp/coverage_table.md
# Writes data/derived/cohort_coverage_by_fy_confidence.parquet
# and prints a markdown table to stdout (capture for SAP-amendment embedding).
```

Post-run aggregate confidence distribution (locked Session 4 of PHASES-2 acceptance bands):

| `inpatient_confidence` | n | % of total | band |
|---|---|---|---|
| `high` | 4,642,565 | 46.07% | ≥ 25% ✓ |
| `approximate` | 4,943,222 | 49.05% | ≥ 30% ✓ |
| `none` | 492,427 | 4.89% | ≤ 35% ✓ |

**Per-FY anomaly**: FY2016 carries 80% `no_match` (414,655 / 518,386 rows). Structural — federal ICD-9 → ICD-10-CM transition year (CMS adoption October 1, 2015); ICD-10-coded FY2016 admissions hit Branch C (`fy < 2017` ICD-10-CM input → `fy{fy}_pre_icd10_era`). Documented in SAP v3 §8.3.

**Requirement**: NTDB DuckDB at the `KG_COST_NTDB_DUCKDB` env var (or `--ntdb-duckdb`; there is no default, because the NTDB is restricted data and is not included in this repo) for FY2019-FY2024 procedure attachment. Pass `--no-attach-procedures` to skip it; downstream Branch A still routes via modal lookup, just without surgical-DRG specificity.
