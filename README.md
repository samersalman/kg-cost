# KG-COST

KG-COST is a Python pipeline that assigns an MS-DRG to ICD-10-CM/PCS-coded encounters and looks up the payment amount that CMS public files give for that DRG in the encounter's fiscal year. It also looks up payment amounts for CPT and HCPCS codes (physician fee schedule, hospital outpatient and ambulatory surgical center files). Each result carries a confidence tier (`high`, `approximate` or `none`) that records how the DRG was obtained.

The repository holds the extraction, transformation, grouper and query code, three derived lookup tables, and an optional Neo4j graph layer. It contains no patient data.

## What the output is

- **Nominal dollars from CMS public files.** Amounts are in the dollars of the fiscal year (inpatient) or calendar year (other settings) they belong to. Nothing is adjusted for inflation here; any CPI adjustment is done in the study code that calls KG-COST.
- **Not charges and not hospital costs.** The inpatient `charge` and `ccr_cost` columns are present in the data but are separate from the payment amount and should not be read as payments.
- **Inpatient amounts come from two kinds of source, and they are different quantities.** Where the Medicare Inpatient public use file covers a DRG (mostly FY2013 to FY2024), the amount is that file's Average Total Payments field (`Avg_Tot_Pymt_Amt`). In CMS's data dictionary (Medicare Inpatient Hospitals by Provider and Service, RY22) this is the average payment for the DRG including the MS-DRG amount and the teaching, disproportionate share, capital and outlier payments, plus the patient's co-payment and deductible and any additional third-party payments for coordination of benefits. It is therefore a Medicare-allowed style amount. It is not the Medicare-only payment, which CMS lists separately as Average Medicare Payments, and it is not a sum of what every payer paid. The code stores it with `allowed_payment_concept = total_payment_allpayer`; that tag is the code's label for this field, not a claim about payers. Otherwise the amount comes from the IPPS formula (`formula_ipps`, or `formula_no_empirical` for the DRGs the public use file lacks in FY2011 and FY2012): the relative weight multiplied by the national operating and capital base amounts for the year. That formula has no wage-index, teaching, disproportionate-share or outlier adjustment, and the code adds no patient cost sharing or third-party amount to it. For MS-DRG 470 the tables hold 17,318 in FY2024 (public use file) and 13,456 in FY2025 (formula); do not read that step as a payment trend. Always check `inpatient_allowed_source` before comparing years. Details are in `data/README.md`.
- **A modal DRG is a proxy.** When an encounter has no procedure codes, the DRG is the one the grouper most often returns for that principal diagnosis on synthetic claims, not the DRG of the real encounter.

**Amount by series.** Inpatient: the public use file's Average Total Payments (a discharge-weighted mean across providers) for FY2011 to FY2024 where the file covers the DRG, otherwise the IPPS formula above (always for FY2008 to FY2010 and FY2025 to FY2026). Physician fee schedule: for CY2017 to CY2023 where the Physician and Other Practitioners public use file has the code, the service-weighted mean of its Average Medicare Allowed Amount, otherwise an allowed amount computed from the fee-schedule RVUs and conversion factor. Hospital outpatient: where the Outpatient public use file has the code's APC, the APC mean, copied to every code in that APC (Average Medicare Allowed Amount for CY2015 to CY2022; Average Total Payments, tagged `total_payment_allpayer`, for CY2011 to CY2013), otherwise the OPPS Addendum B payment rate. Ambulatory surgical center: the ASC Addendum AA or BB payment rate. Counts, years and columns are in `data/README.md`.

## Quickstart

Python 3.12 is required (`pyproject.toml` declares `>=3.12,<3.13`).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Look up payment amounts without any further setup

The three tables in `data/gold/` can be read directly. This query maps an ICD-10-CM principal diagnosis (no decimal point) to its modal DRG and the FY2024 amount for that DRG:

```python
import duckdb

duckdb.sql("""
    SELECT m.icd_cm, m.fy, m.ms_drg, c.allowed_payment, c.allowed_payment_source
    FROM 'data/gold/icd_principal_drg_modal.parquet' AS m
    JOIN 'data/gold/code_costs.parquet' AS c
      ON c.code = m.ms_drg AND c.fy_or_cy = m.fy AND c.setting = 'inpatient'
    WHERE m.icd_cm = 'S72001A' AND m.fy = 2024
""").show()
```

Run it from the repository root. It returns MS-DRG 536 with an amount of about 7,596 from `puf_empirical`. Column definitions are in `data/README.md`.

### Annotate a cohort

`annotate_cohort` reads the gold tables and also `data/silver/gems_2018.parquet`, the CMS General Equivalence Mappings (ICD-9-CM to ICD-10-CM, frozen FY2018 release). That file is not shipped. Build it once; this downloads four small archives from CMS:

```bash
python -m src.extract.gems
```

Then annotate the 32-row synthetic cohort in `tests/fixtures/sample_cohort.csv`:

```python
import pandas as pd
from src.query.cohort_annotator import annotate_cohort

cohort = pd.read_csv("tests/fixtures/sample_cohort.csv")
result = annotate_cohort(cohort, source="parquet", default_year=2024)

print(result.n_rows, result.n_ok, result.n_no_match, round(result.coverage, 3))
print(result.df[["patient_id", "dx_principal", "fy", "inpatient_drg",
                 "inpatient_allowed_mean", "inpatient_confidence",
                 "annotation_status"]].head())
```

Pass `source="parquet"`. The default is `source="bolt"`, which needs a running Neo4j graph (see below). Without the Java grouper, this run reports 32 rows, 30 `ok` and 2 `no_match`; the two `no_match` rows are deliberately invalid test codes. The same run from the command line:

```bash
python -m src.query.cohort_annotator --input tests/fixtures/sample_cohort.csv --output sample_cohort_with_costs.csv --source parquet
```

Signature: `annotate_cohort(cohort_df, source="bolt", default_year=2024, fy_column="fy", partial_output=None, confidence_floor="none", scope="ip")`. It returns an `AnnotationResult` with `df`, `coverage`, `n_rows`, `n_ok`, `n_partial`, `n_no_match`, `unmatched_rows` and `mode`. `confidence_floor` (`"none"`, `"approximate"` or `"high"`) rejects inpatient rows below that tier: their inpatient columns are blanked and `annotation_status` becomes `no_match`.

Input columns the annotator reads (others are passed through unchanged):

| Column | Content |
|---|---|
| `dx_principal` | Principal diagnosis. Dots are removed. |
| `dx_principal_system` | `ICD-10-CM` or `ICD-9-CM`. Blank is treated as ICD-10-CM. |
| `dx_secondary` | Secondary diagnoses separated by `;`. Used by the grouper only. |
| `pcs` | ICD-10-PCS procedure codes (7 characters) separated by `;`. |
| `cpts` | CPT or HCPCS codes separated by `;`. |
| `sex`, `age`, `discharge_disp` | `sex` is 1 (male) or 2 (female); `discharge_disp` is the 2-digit UB-04 code, default `01`. Used by the grouper only. |
| `fy` | Fiscal year of discharge. The column name is set with `fy_column`; `default_year` fills blanks. |

**Input contract for the multi-code columns.** `dx_secondary`, `pcs` and `cpts` each hold all of an encounter's codes in one cell, separated by `;` and by nothing else. A pipe (`|`), a comma or a space is not a delimiter: a string joined any other way is read as one code (see Known issues). Write every code in upper case without a decimal point, for example `S72001A` and `0QS734Z`. The grouper and the modal-DRG lookup strip dots and upper-case ICD-10-CM and ICD-10-PCS codes, but CPT and HCPCS codes in `cpts` are matched exactly as written.

The 19 appended columns are `inpatient_drg`, `inpatient_drg_title`, `inpatient_allowed_mean`, `inpatient_allowed_p10`, `inpatient_allowed_p90`, `inpatient_allowed_n`, `inpatient_allowed_source`, `inpatient_confidence`, `inpatient_charge_mean`, `inpatient_ccr_cost`, `pfs_facility_allowed_total`, `pfs_nonfacility_allowed_total`, `outpatient_apc_allowed_total`, `asc_allowed_total`, `outpatient_confidence`, `n_cpts_matched`, `n_cpts_missing`, `annotation_status` (`ok`, `partial` or `no_match`) and `annotation_notes`. `inpatient_drg_title` is empty here because the title table is not shipped, and `outpatient_confidence` is a reserved column that is always empty.

A single code can be looked up with `from src.query import get_cost`, for example `get_cost(code="470", code_type="drg", fy=2024)`, or on the command line with `python -m src.query.lookup --code 470 --type drg --fy 2024`. It uses the same backend, so it also needs `data/silver/gems_2018.parquet`.

## How the DRG is chosen, by fiscal year

| Case | How the DRG is obtained | Confidence tier |
|---|---|---|
| ICD-10-CM, FY2023 to FY2026 | If the row has procedure codes, the official CMS MS-DRG Java grouper (V40 to V43; the `.0` version of the year by default) through a small Java driver, `src/grouper/driver/KGCostGrouperDriver.java`. Needs JDK 17 or newer and the CMS JARs, which are not shipped. | `high` |
| ICD-10-CM, FY2019 to FY2022 | If the row has procedure codes, the `drgpy` package (pure Python, grouper versions v36 to v39), which the dev extra installs, pinned to 0.2.1 because it gave the same MS-DRG as the CMS Java grouper (V40.0) for 95.8% to 96.6% of 4,090 FY2023 NTDB trauma encounters with procedure codes, against 82.6% to 85.7% for drgpy 0.0.6 (141 to 170 of the 4,090 still differ under 0.2.1). Each range spans two ways of choosing the principal diagnosis, which the NTDB public use file does not record: the alphabetically first recorded code (96.6% and 82.6%) and a random choice among the recorded codes (95.8% and 85.7%). The NTDB data are not included here. FY2023 is the nearest year in which both engines can group the same encounters; FY2019 to FY2022 themselves cannot be checked against the Java grouper, because the CMS Java driver in this repository supports V40 and later only. | `high` |
| ICD-10-CM, FY2019 or later, no procedure codes, or the grouper could not run | The modal DRG for the principal diagnosis from `icd_principal_drg_modal.parquet`. | `high` |
| ICD-10-CM, FY2017 and FY2018 | No grouper. The modal DRG (smallest DRG number among that code's modal DRGs in FY2019 to FY2026), with the payment looked up in the encounter's own fiscal year. `inpatient_allowed_source` is `gems_bridged_back_translated`. | `approximate` |
| ICD-9-CM, any year | The CMS General Equivalence Mappings give an ICD-10-CM code, then the modal DRG at the later of the encounter year and FY2019, with the payment looked up at the later of the encounter year and FY2008. `inpatient_allowed_source` is `gems_bridged_modal`. | `approximate` |
| ICD-10-CM before FY2017, no DRG found, or CPT-only rows | No DRG is assigned. | `none` |

Two things to know when reading results:

- `high` covers both a DRG assigned by a grouper and a modal DRG. Rows that a grouper handled have `drg_via_grouper:<DRG>` in `annotation_notes`.
- **If the grouper cannot run** (no JDK 17, no JARs, or `drgpy` missing), `annotate_cohort` does not raise. It falls back to the modal DRG and adds no note. For example, the sample row P003 (knee replacement procedure code) comes back as DRG 554 and `high` without the grouper, not a joint replacement DRG. Check `annotation_notes` before relying on procedure-based DRGs.

To get the official grouper, see `src/grouper/jars/README.md`.

### Known issues in v0.1.0

- **Separate several codes with `;` only.** The annotator splits `dx_secondary`, `pcs` and `cpts` on a semicolon, strips spaces and drops empty items (`_split_codes` in `src/query/cohort_annotator.py`). A multi-code string joined any other way, for example `0QS734Z|0W9G30Z`, is read as one code of that whole length. For FY2023 to FY2026 the Java grouper rejects a procedure code that is not 7 characters; the annotator catches the error and uses the modal DRG, with no `drg_via_grouper` note. For FY2019 to FY2022, `src/grouper/drgpy_wrapper.py` keeps only procedure codes of exactly 7 characters and drops the others without an error or a note, so the DRG is assigned without them, and the row can still carry a `drg_via_grouper:<DRG>` note.
- **`get_cost` raises `TypeError` for many CPT and HCPCS codes.** In `_resolve_cpt_or_hcpcs` in `src/query/lookup.py`, `int()` is applied to `allowed_payment_n` after a check that catches `None` and float `NaN` but not the pandas `NA` that a missing count is loaded as. For example, `get_cost(code="00100", code_type="cpt", fy=2024)` raises `TypeError`. In `data/gold/code_costs.parquet`, 278,783 of the 482,399 rows in the four CPT and HCPCS settings (physician fee schedule facility and non-facility, hospital outpatient, ambulatory surgical center) have a payment amount and no count. `python -m src.query.lookup` with `--type cpt` or `--type hcpcs` calls the same function. The DRG branch of `get_cost` is separate code and does not do this, and `annotate_cohort` is not affected.
- **An unset NTDB DuckDB path raises `RuntimeError`.** If neither `--ntdb-duckdb` nor `KG_COST_NTDB_DUCKDB` is set, `src.query.run_phase14c` and `python -m src.query.cohort_annotator --input-format ntdb` raise `RuntimeError` when FY2019 to FY2024 procedures have to be attached. `annotate_cohort` called on a DataFrame is not affected.
- **The shipped FY2019 to FY2022 rows of the two ICD-to-DRG tables match drgpy 0.0.6, not the pinned 0.2.1.** A full rebuild of those four fiscal years with drgpy 0.0.6 reproduces every row of `icd_principal_drg_modal.parquet` and `icd_principal_drg_reachability.parquet` (289,485 principal diagnosis and year pairs). A rebuild with drgpy 0.2.1 gives the same modal DRG for all but 28 principal diagnoses in each fiscal year (112 of 289,485 rows; all are S30, S31 or S38 injury codes, for which 0.0.6 gives DRG 965 and 0.2.1 gives the placeholder `000`, described in `data/README.md`). It gives a different set of reachable DRGs for 55,807 to 56,336 principal diagnoses per fiscal year (about 77%); in every year this is chiefly because the claim with an operating-room procedure now reaches DRG 983 (55,117 to 55,631 of the differing codes). The tables were not rebuilt for this release. `annotate_cohort` groups a real FY2019 to FY2022 encounter that has procedure codes with the installed drgpy, and it reads the modal table only as a fallback.

## The gold tables

`data/README.md` documents the three tables, with columns, types, coverage, sources, build scripts and caveats:

- `data/gold/code_costs.parquet` (496,792 rows): payment amounts by code, year and setting (MS-DRG inpatient, physician fee schedule facility and non-facility, hospital outpatient, ambulatory surgical center).
- `data/gold/icd_principal_drg_modal.parquet` (586,147 rows): the modal DRG for each ICD-10-CM principal diagnosis, FY2019 to FY2026.
- `data/gold/icd_principal_drg_reachability.parquet` (1,169,186 rows): every DRG each principal diagnosis can reach, FY2019 to FY2026.

## Repository layout

| Path | Contents |
|---|---|
| `src/extract/` | Downloaders that pull CMS and CDC public files into `data/bronze/` and write per-source tables to `data/silver/`. |
| `src/transform/` | Builds the unified silver tables and the three gold tables. |
| `src/grouper/` | The grouper wrapper (`grouper.py`, `drgpy_wrapper.py`) and the Java driver. |
| `src/query/` | `cohort_annotator.py`, `lookup.py`, and the optional NTDB hooks. |
| `src/load/` | CSV builder and import scripts for the optional Neo4j graph. |
| `neo4j/` | Docker Compose file and configuration for the graph. |
| `tests/` | Test suite and synthetic fixtures. |
| `RUNBOOK.md` | The author's development log with commands for each extractor and transform. It is not a quickstart and refers to some planning documents that are not in this repository. |
| `CHANGES_FROM_PROJECT.md` | What differs between this snapshot and the author's working project. |

Rebuilding the gold tables means running the extractors, which download many CMS files (some public use files are about 2 GB per year), and then the transforms. CMS file locations change over time, so a rebuild may not reproduce the shipped tables exactly.

## Optional: Neo4j graph

The cost tables can also be loaded into a Neo4j graph, which is useful only for multi-hop graph queries. The parquet backend returns the same values for the lookups above. The graph is not built by anything in the quickstart. It needs the intermediate silver tables, so you must first run the extractors and transforms, then `python -m src.load.run_import`, which builds the import CSVs (`src/load/build_csvs.py`) when needed and runs the bulk import.

`neo4j/docker-compose.yml` starts `neo4j:5.20-community` with the APOC plugin and the **local-only default password `kgcostlocal`**. The compose file publishes ports 7474 and 7687 without restricting them to the local machine. Use it only on a machine that is not reachable from a network. For any other use, change `NEO4J_AUTH` and the password in the healthcheck in `neo4j/docker-compose.yml`, and set the matching `NEO4J_PASSWORD` in `.env` (copy `.env.example`). `docs/advanced-neo4j.md` has a short orientation.

## Optional: restricted-data hooks (not included, not required)

Some code reads restricted-access datasets that are not part of this repository and are not needed for anything above:

- **NTDB** (National Trauma Data Bank). `src/query/_ntdb_adapter.py` maps an NTDB cohort file onto the annotator's input columns, and `src/query/run_phase14c.py` annotates it fiscal year by fiscal year. `--source-parquet` is a required argument of `run_phase14c.py`. Attaching ICD-10-PCS procedures for FY2019 to FY2024 needs your own NTDB DuckDB file, given with `--ntdb-duckdb` or the environment variable `KG_COST_NTDB_DUCKDB`; there is no default. `run_phase14c.py` also accepts `--no-attach-procedures`.
- **MIMIC-IV.** Two validation tests (`tests/test_grouper_validation.py`, `tests/test_drgpy_cross.py`) read a MIMIC-IV `hosp` directory named by the environment variable `KG_COST_MIMIC_HOSP_DIR`. They skip when it is not set.

## Tests

```bash
python -m pytest tests -rs
```

`-rs` prints the reason for every skip. Tests that read intermediate files in `data/bronze/` or `data/silver/` skip with a message naming the missing file. Others are switched on by environment variables: `KG_COST_RUN_NEO4J=1` (live Neo4j, which also needs a reachable instance), `KG_COST_RUN_NTDB=1` with `KG_COST_NTDB_COHORT_PARQUET` (live NTDB run), and `KG_COST_MIMIC_HOSP_DIR`. Grouper tests skip when `src/grouper/jars/V43_1/msdrg-core-43.1.0.0.jar` is missing, and `drgpy` tests skip when `drgpy` is not importable.

Two exceptions to the skip behavior:

- Most tests in `tests/test_cohort_annotator.py` need `data/silver/gems_2018.parquet` and error without it. Run `python -m src.extract.gems` first.
- Two tests in that file (the sample rows P002 and P003) assert DRGs that only the grouper produces. They fail without JDK 17 and the CMS JARs.

## Data and privacy

No patient data, NTDB data or MIMIC-IV data is included. `tests/fixtures/sample_cohort.csv` is a hand-written synthetic cohort, and `tests/fixtures/ntdb_synthetic_100.parquet` is generated by `tests/_build_ntdb_synthetic.py`. The gold tables contain codes, years, settings and summary statistics only. See `NOTICE` for third-party material: CMS and CDC public data, the CPT trademark, and the CMS grouper and other dependencies that are not redistributed.

## Citation

See `CITATION.cff`, or use GitHub's "Cite this repository" button. A Zenodo DOI will be added here after the first release.

Samer G. Salman, Baylor College of Medicine. KG-COST: an open-source pipeline that assigns MS-DRGs to ICD-10-CM/PCS-coded encounters and estimates Medicare-allowed payments. Version 0.1.0. https://github.com/samersalman/kg-cost

## License

MIT. See `LICENSE`. The one exception is the Java driver `src/grouper/driver/KGCostGrouperDriver.java`, whose header carries the SPDX identifier CC0-1.0 (see `NOTICE`). Third-party notices are in `NOTICE`.

## Contact

Open an issue at https://github.com/samersalman/kg-cost/issues.
