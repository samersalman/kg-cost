# sample_cohort.csv — fixture for cohort_annotator (Phase 10 + PHASES-2 Session 2)

Hand-curated 32-row synthetic cohort exercising every graph path the
`src/query/cohort_annotator` (Step 4) is expected to hit:

- **Inpatient routing** (dx + pcs + fy ≥ 2019) via two-edge ICD→DRG
  (`CAN_ANCHOR` + `IS_MODAL_FOR`) and the Java/drgpy hybrid grouper.
  (Branch A — `inpatient_confidence='high'`.)
- **GEMs forward bridge** (ICD-9 → ICD-10 → modal DRG → cost at native
  FY). Branch D — `inpatient_confidence='approximate'`.
- **Back-translation** (FY2017–FY2018 ICD-10 → FY2019 modal → FY-native
  cost). Branch B — `inpatient_confidence='approximate'`.
- **Professional fees** (PFS facility + non-facility) via CPT lists.
- **Outpatient APC** drilldown via OPPS-bound HCPCS.
- **ASC** routing for ambulatory surgery codes.
- **No-match paths** — bogus ICD (`X9999`) and a V-code with no GEMs
  forward target, so the annotator's `gems_no_target` and
  `dx_not_in_graph` branches are exercised.

The CSV is canonical; this README is descriptive.

## Coverage math

- 32 total rows.
- 30 rows with `expected_match=true` → denominator for the
  `cohort_annotator` ≥0.95 acceptance test (≥29 of 30 must annotate to
  ≥1 cost). PHASES-2.md (Session 2) bumped the denominator from 27 → 29
  by flipping P009/P010 from `false` to `true` once GEMs Branch D
  shipped; PHASES-2.md plan-v line 24 wrote "29 true / 3 false" but the
  actual math (with P031 false + P032 true added) is **30 true / 2
  false** — fixture follows the mechanical spec, not the line-24 prose.
- 2 rows with `expected_match=false` (P030 X9999, P031 V9988) —
  excluded from the denominator. Annotator must tag them `no_match`
  (P030: `dx_not_in_graph`; P031: `gems_no_target`).

## Substitutions vs the Step 5 starting matrix

| Original | Substitute | Why |
|---|---|---|
| CPT `99244` (P017, P026) | CPT `99204` | `99244` (office consultation, comprehensive) was retired by CMS for Medicare claims in CY2010 and has no `:CostObservation` in the graph for FY2024. Substituted with `99204` (new patient outpatient E/M, level 4) — equivalent comprehensive new-visit semantics, present in PFS facility/non-facility for FY2024. |

No other substitutions made. All other CPT/HCPCS codes from the matrix
were live-graph-verified at FY2024 against the Neo4j Desktop instance
before the CSV was finalized. ICD-10-CM modal-DRG lookups verified for
I214/I213/M1711/A419/J189/M1611/I5023 at FY2024 and I214 at FY2019,
FY2020, FY2022, FY2026 (all four ground to DRG 282).

## Per-row map

| Row | dx_principal | pcs | cpts | fy | Expected match path | Source-of-truth |
|----|----|----|----|----|----|----|
| P001 | I214 (NSTEMI) | — | — | 2024 | inpatient routing — modal DRG 282 (no PCS, medical anchor) | live: `MATCH (i:ICDCMCode {code:'I214'})-[:IS_MODAL_FOR {fy:2024}]->(d:MSDRG)` |
| P002 | I213 (STEMI) | 02703DZ (LAD PCI w/ DES) | — | 2024 | inpatient → DRG (PCI procedural anchor) | grouper |
| P003 | M1711 (R knee primary OA) | 0SRC07Z (R knee replacement) | 27447 | 2024 | inpatient (DRG 469/470) + PFS facility for 27447 | grouper + HCPCS path |
| P004 | A419 (sepsis, organism unspec) | 02HV33Z (central line) | — | 2024 | inpatient — modal 872 | grouper |
| P005 | J189 (pneumonia unspec) | — | — | 2024 | inpatient — modal 195 | live: J189→195 |
| P006 | M1611 (R hip primary OA) | 0SR9019 (R hip replacement, ceramic) | — | 2024 | inpatient → DRG (THA procedural anchor) | grouper |
| P007 | I214 | — | — | 2020 | drgpy v37 fallback → modal 282 | live: I214 FY2020→282 |
| P008 | I5023 (acute on chronic systolic HF) | — | — | 2022 | drgpy v39 fallback → modal 293 | live |
| P009 | 41041 (ICD-9 acute MI subendocard) | 8154 (ICD-9 PCI) | — | 2014 | **GEMs Branch D** — 41041 → I2119 (Tier 1) → modal DRG 282 (FY2019) → cost at FY2014. confidence=approximate; source=`gems_bridged_modal`. | gems_2018 + modal index |
| P010 | 486 (ICD-9 pneumonia) | — | — | 2014 | **GEMs Branch D** — 486 → J189 (Tier 1) → modal DRG 195 (FY2019) → cost at FY2014. confidence=approximate; source=`gems_bridged_modal`. | gems_2018 + modal index |
| P011 | — | — | 80053;85025;82947 | 2024 | PFS-nonfacility sum (lab panel) | live: all three present |
| P012 | — | — | 71045;72100 | 2024 | PFS + OPPS (CXR + LS spine) | live |
| P013 | — | — | 99213 | 2024 | PFS facility/non-facility | live |
| P014 | — | — | 90686 | 2024 | PFS (flu vaccine) | live |
| P015 | — | — | 97597 | 2024 | PFS + OPPS (wound debridement) | live |
| P016 | — | — | 27447;99231;99238 | 2024 | PFS facility (TKA + post-op rounds + d/c) | live |
| P017 | — | — | 99204;93000;93306 | 2024 | PFS (cardiology consult + EKG + echo) | live (note 99204 substitution) |
| P018 | — | — | 00400;01400 | 2024 | PFS (anesthesia codes) | live |
| P019 | — | — | 99284;71046 | 2024 | PFS + OPPS (ED visit + CXR 2-view) | live |
| P020 | — | — | 93306;93320 | 2024 | PFS (TTE complete + Doppler) | live |
| P021 | — | — | 66984 | 2024 | OPPS APC + ASC (cataract surgery) | live: all 4 settings |
| P022 | — | — | 45378 | 2024 | OPPS + ASC (colonoscopy diagnostic) | live |
| P023 | — | — | 43239 | 2024 | OPPS + ASC (EGD with biopsy) | live |
| P024 | — | — | 66984 | 2024 | dup of P021 — same code, exercises ASC routing path | live |
| P025 | — | — | 64721 | 2024 | OPPS + ASC (carpal tunnel release) | live |
| P026 | J189 | — | 99204 | 2024 | inpatient (DRG 195) + PFS (consult) | mixed routing |
| P027 | M1711 | 0SRC07Z | 97110 | 2024 | inpatient (DRG 470) + PFS PT follow-up | mixed routing |
| P028 | I214 | — | — | 2019 | drgpy v36 fallback → modal 282 | live: FY2019 verified |
| P029 | I214 | — | — | 2026 | Java grouper modern → modal 282 | live: FY2026 verified |
| P030 | X9999 (bogus) | — | — | 2024 | **no_match** — code not in graph | expected_match=false |
| P031 | V9988 (V-code, NOT in ICD-9-CM / GEMs) | — | — | 2014 | **no_match** — `gems_no_target` (zero-row in GEMs forward index). Branch D fail-fast path; confidence stays `none`. | expected_match=false |
| P032 | I213 (STEMI, FY2017) | — | — | 2017 | **Branch B** — I213 → modal DRG 282 (looked up at FY2019) → cost at FY2017. confidence=approximate; source=`gems_bridged_back_translated`. | modal index FY2019 + code_costs FY2017 |

## Verification snapshot (live graph, 2026-05-02)

Sampled against Neo4j Desktop 2026.04.0 instance
`dbms-<UUID>` at `neo4j://127.0.0.1:7687`:

- `CPT 27447 → :CostObservation` — pfs_facility + pfs_nonfacility +
  opps + asc all present FY2024.
- `I214 → IS_MODAL_FOR{fy:2024} → :MSDRG{code:'282'}` — confirmed
  (matches Session 11 preflight).
- `I214 FY2019/2020/2022/2026 → DRG 282` — drgpy + Java fallbacks all
  resolve.
- `CPT 99213 → :CostObservation` FY2024 — both PFS settings present.
- `CPT 99244 → :CostObservation` FY2024 — **empty** (drove the
  substitution above).

## Notes for downstream tasks

- Task #4 (`cohort_annotator`): the `99244 → 99204` substitution is
  documented above. No other CPT lookups will silently fail in this
  fixture given live-graph verification at fixture-author time.
  Post-PHASES-2 Session 2: ICD-9 rows (P009, P010) now route through
  GEMs Branch D and resolve to `inpatient_confidence='approximate'`
  with `inpatient_allowed_source='gems_bridged_modal'`.
- Task #6/#8 (`test_cohort_annotator`): the test asserts
  `coverage ≥ 0.95` over the 30-row `expected_match=true` denominator,
  and that exactly 2 rows are tagged `no_match` (P030, P031). The
  `test_inpatient_routing_for_I214_lands_on_DRG_282` parametrized check
  can use P001 (FY2024), P007 (FY2020), P028 (FY2019), or P029 (FY2026)
  as inputs. P031's V-code (`V9988`) is intentionally synthetic — it's
  not a real ICD-9-CM code, which guarantees `lookup_gems_targets`
  returns `[]` and the annotator emits `gems_no_target`.
