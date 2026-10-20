# Gold tables

This folder holds the three derived lookup tables that the query code reads. They are Parquet files, built by the author from public CMS files and from the MS-DRG grouper. Nothing else under `data/` is tracked: the intermediate layers (`data/bronze`, `data/silver`, `data/neo4j_import`) are rebuilt locally and are listed in `.gitignore`.

**No patient data.** Every column in all three tables is a code, a year, a setting, a summary statistic computed across providers or discharges, or a provenance tag. There are no patient, provider, hospital or free-text fields.

| File | Rows | Columns | Size (bytes) | File modified | SHA-256 |
|---|---|---|---|---|---|
| `gold/code_costs.parquet` | 496,792 | 28 | 5,600,838 | 2026-05-02 | `3c13521fda47f51124a8766959ee3c511a17982833a76b3102a07324bbd36b63` |
| `gold/icd_principal_drg_modal.parquet` | 586,147 | 7 | 2,474,321 | 2026-05-05 | `264f1293aa71e788e486f1e9f273c3dcab4844b1788b4db432072c84fc405501` |
| `gold/icd_principal_drg_reachability.parquet` | 1,169,186 | 6 | 3,345,601 | 2026-05-05 | `32091ba05879344e2ca15e841e11d37a1d1994b986f1d57dfda8f650f480c2fe` |

"File modified" is the modification date of the file in the author's working copy (`ls -l`), used here as the build date. Row counts and column types below were read from the Parquet files themselves.

Terms: FY is the federal fiscal year (October 1 to September 30). CY is the calendar year. All dollar amounts are nominal dollars of the year shown. Nothing is adjusted for inflation.

## Reading the tables

```python
import duckdb

duckdb.sql("""
    SELECT code AS ms_drg, fy_or_cy AS fiscal_year, allowed_payment,
           allowed_payment_n, allowed_payment_source, allowed_payment_concept
    FROM 'data/gold/code_costs.parquet'
    WHERE setting = 'inpatient' AND code = '470' AND fy_or_cy BETWEEN 2022 AND 2026
    ORDER BY fy_or_cy
""").show()
```

Run from the repository root. For MS-DRG 470 this returns amounts from the Medicare Inpatient PUF for FY2022 to FY2024 (`puf_empirical`) and IPPS formula amounts for FY2025 and FY2026 (`formula_ipps`). Those two kinds of amount are not the same quantity; see the caveats below.

## code_costs.parquet

One row per code, year and setting. The key `(code, code_system, fy_or_cy, setting)` is unique (checked by the build script).

Built by `src/transform/code_costs.py` (`python -m src.transform.code_costs`), which joins six intermediate tables in `data/silver/` (and, when present, a national cost-to-charge table). The intermediate tables are not shipped, so the file cannot be regenerated from this repository without re-downloading the CMS source files. CMS file locations change over time, so a rebuild may not reproduce this file exactly.

### Coverage

| `setting` | `code_system` | Years in the file | Rows | Distinct codes | CMS source |
|---|---|---|---|---|---|
| `inpatient` | MS-DRG | FY2008 to FY2026 | 14,393 | 829 | IPPS Final Rule Table 5 (relative weights) and Tables 1A to 1E (national standardized amounts); Medicare Inpatient Hospitals public use file (PUF); HCRIS cost reports for the cost-to-charge ratio |
| `pfs_facility`, `pfs_nonfacility` | CPT, HCPCS-II | CY2017 to CY2026 | 105,276 each | 15,482 | Medicare Physician Fee Schedule RVU files; Medicare Physician and Other Practitioners PUF |
| `opps` | CPT, HCPCS-II | CY2008 to CY2026 | 229,712 | 19,854 | OPPS Addendum B (payment rates by code); Medicare Outpatient Hospitals PUF |
| `asc` | CPT, HCPCS-II | CY2018 to CY2026 | 42,135 | 7,488 | ASC Addenda AA and BB |

By code system: 326,625 CPT rows, 155,774 HCPCS-II rows and 14,393 MS-DRG rows. The extractor modules are in `src/extract/` (`ipps.py`, `puf_inpatient.py`, `puf_inpatient_summary.py`, `pfs_rvu.py`, `puf_physician.py`, `opps.py`, `puf_outpatient.py`, `asc.py`, `hcris_data.py`, `hcris_data_nber.py`). Where CMS no longer hosts a public use file for a year, the extractors can fall back to the NBER mirror or a Wayback copy (`src/extract/_url_resolver.py`).

For PFS, OPPS and ASC the table uses the first quarterly release ("quarter A") of each calendar year, so mid-year rate updates are not in the table. Inpatient rows are per fiscal year.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `code` | string | MS-DRG (3 characters, zero padded) or CPT or HCPCS Level II code (5 characters). |
| `code_system` | string | `MS-DRG`, `CPT` or `HCPCS-II`. |
| `fy_or_cy` | int32 | The year used for lookups: fiscal year for inpatient rows, calendar year for all other settings. |
| `fy` | int32 | Fiscal year. Filled for inpatient rows only. |
| `cy` | int32 | Calendar year. Filled for non-inpatient rows only. |
| `setting` | string | `inpatient`, `opps`, `asc`, `pfs_facility` or `pfs_nonfacility`. |
| `charge` | double | Mean submitted (covered) charge from the PUF. This is a hospital charge, not a payment and not a cost. Filled on 6,816 inpatient rows (FY2011 to FY2024) and 13,804 OPPS rows; null elsewhere. |
| `charge_n` | int64 | Number of discharges or services behind `charge`. |
| `charge_source` | string | `puf_empirical` when `charge` is filled, otherwise null. |
| `charge_p10`, `charge_p25`, `charge_p50`, `charge_p75`, `charge_p90` | double | Percentiles of the per-provider average charge. Null when `charge` is null or the extractor masked them. |
| `allowed_payment` | double | The payment amount for the code, year and setting. What it means depends on `allowed_payment_source` and `allowed_payment_concept`; see below. Null on 147,497 rows where neither the PUF nor the rate file gave an amount for that code and year (136,955 OPPS rows and 10,542 ASC rows). |
| `allowed_payment_n` | int64 | Discharges (inpatient) or services (physician, outpatient) behind an empirical mean. Null on formula rows. |
| `allowed_payment_source` | string | How `allowed_payment` was obtained. See the next table. Null when `allowed_payment` is null. |
| `allowed_payment_concept` | string | `total_payment_allpayer` or `medicare_allowed` on empirical rows, null on formula rows. `total_payment_allpayer` is the code's label for the PUF Average Total Payments field, not a claim about payers. See below. |
| `payment_p10`, `payment_p25`, `payment_p50`, `payment_p75`, `payment_p90` | double | Percentiles across providers of the per-provider average payment. Present on empirical rows only, and null where the source percentiles were suppressed or the extractor masked them (for example inpatient DRGs with fewer than 11 providers). `payment_p10` is filled on 45,383 of the 62,935 empirical rows (all inpatient and OPPS empirical rows, and 10,837 of 20,386 PFS facility and 13,926 of 21,929 PFS non-facility empirical rows). |
| `ccr_cost` | double | `charge` multiplied by `ccr_value`. An estimate of hospital cost, not a payment. Filled on the 6,816 inpatient rows that have a charge. |
| `ccr_value` | double | National mean cost-to-charge ratio (HCRIS) for the cost-center group named in `ccr_cost_center`. Filled on every inpatient row, null elsewhere. |
| `ccr_cost_center` | string | `Operating Room` on inpatient rows (a single default cost center, not specific to the DRG), null elsewhere. |
| `effective_start`, `effective_end` | date32 | Validity window of the source release: October 1 to September 30 for inpatient rows, January 1 to March 31 of the calendar year (quarter A) for the other settings. |

### How `allowed_payment` was obtained

| `setting` | `allowed_payment_source` | Rows | What the amount is |
|---|---|---|---|
| `inpatient` | `puf_empirical` | 6,816 | Discharge-weighted mean of the PUF field `Avg_Tot_Pymt_Amt` (Average Total Payments) across providers, with `allowed_payment_concept = total_payment_allpayer`. CMS's data dictionary for the file (Medicare Inpatient Hospitals by Provider and Service, RY22) defines the field as the average total payments to all providers for the DRG including the MS-DRG amount, teaching, disproportionate share, capital and outlier payments for all cases, plus the co-payment and deductible amounts that the patient is responsible for and any additional payments by third parties for coordination of benefits. It is therefore a Medicare-allowed style amount, not the Medicare-only payment. The tag `total_payment_allpayer` is the code's label for this field, not a claim about payers. The Medicare-only field (`Avg_Mdcr_Pymt_Amt`, which the same dictionary says excludes beneficiary co-payments and deductibles and any additional third-party payments for coordination of benefits) stays in the intermediate layer, not in this table. FY2011 to FY2024. |
| `inpatient` | `formula_ipps` | 6,283 | IPPS relative weight multiplied by the sum of the national operating standardized amount and the national capital federal rate for that fiscal year. National values only; no hospital-specific adjustments. All of FY2008 to FY2010 and FY2025 to FY2026, plus DRGs missing from the PUF in other years. |
| `inpatient` | `formula_no_empirical` | 1,294 | The same formula, used for the DRGs that the PUF does not cover in FY2011 and FY2012 (the PUF is limited to 100 DRGs in those two years). |
| `pfs_facility`, `pfs_nonfacility` | `puf_empirical` | 20,386 and 21,929 | Service-weighted mean of the PUF field `Avg_Mdcr_Alowd_Amt` (`medicare_allowed`), CY2017 to CY2023. |
| `pfs_facility`, `pfs_nonfacility` | `formula_pfs` | 84,890 and 83,347 | Allowed amount computed from the fee-schedule RVUs and conversion factor. |
| `opps` | `puf_empirical` | 12,429 and 1,375 | APC-level mean from the Outpatient PUF, copied to every code in that APC. `medicare_allowed` (field `Avg_Mdcr_Alowd_Amt`) for CY2015 to CY2022 (12,429 rows); `total_payment_allpayer` for CY2011 to CY2013 (1,375 rows), the code's label for the Average Total Payments field, which is the only payment field the NBER-mirrored files for those years publish. |
| `opps` | `formula_opps` | 78,953 | OPPS Addendum B payment rate. |
| `asc` | `formula_asc` | 31,593 | ASC Addendum AA or BB payment rate. |

### Caveats

- **Nominal dollars.** Nothing is inflation adjusted. Any price-year adjustment belongs in the calling study.
- **Not one consistent series.** The amount for a given code can change in kind from one year to the next. For MS-DRG 470 the file holds 17,318 in FY2024 (`puf_empirical`, the PUF Average Total Payments field) and 13,456 in FY2025 (`formula_ipps`). A step like this reflects the two definitions as well as any real change, so do not read it as a payment trend. Filter on or stratify by `allowed_payment_source` and `allowed_payment_concept` before comparing years.
- **Not a charge and not a cost.** `charge` is what hospitals billed. `ccr_cost` is a rough charge-based estimate using one national ratio.
- **Inpatient formula rows are national base amounts.** They are not hospital-specific payments and do not include wage-index, teaching, disproportionate-share or outlier adjustments. The code adds no patient cost sharing or third-party amount to them, so they are not the same quantity as the PUF Average Total Payments.
- **OPPS PUF means are shared across an APC.** Every code that maps to an APC receives the same empirical mean.
- **CPT numbers only.** CPT is a registered trademark of the American Medical Association. The table holds code numbers and no code descriptors. See `NOTICE`.
- **Inpatient DRG coverage varies by year.** There are 745 to 773 MS-DRG rows per year.

## icd_principal_drg_modal.parquet

For each ICD-10-CM code and fiscal year, the most frequent MS-DRG that the grouper returns when that code is the principal diagnosis of a synthetic claim. It is the lookup the cohort annotator uses when an encounter has no procedure codes or the grouper cannot be used, and the back-translation proxy for FY2017 and FY2018.

Built by `src/transform/icd_drg.py` (`python -m src.transform.icd_drg --fy-range 2019-2026`). It reads the list of ICD-10-CM codes for each year from `data/silver/codes_master.parquet` (CDC/NCHS code files, not shipped) and runs the grouper five times per code and year on synthetic claims. The claim variants are labelled `baseline`, `with_mcc`, `with_cc`, `with_or_proc` and `male_baseline` in the code; they differ in secondary diagnosis, procedure code and sex, and use age 65 and discharge status 01 (perinatal codes get a newborn override). The modal DRG is the most frequent result, with ties broken by the lowest DRG number. FY2019 to FY2022 are grouped with the `drgpy` package; FY2023 to FY2026 with the CMS Java grouper (the default `.0` version for each year, V40.0 to V43.0, as set in `src/config.py`). The shipped FY2019 to FY2022 rows of both tables match a build with drgpy 0.0.6 (a full rebuild with 0.0.6 reproduces every row; which version built them was not established); the release pins drgpy 0.2.1, with which a rebuild changes the modal DRG of 28 principal diagnoses per fiscal year (S30, S31 and S38 codes: 965 becomes the placeholder 000) and the reachable-DRG set for about 77% of codes (see Known issues in the top-level README).

The key `(icd_cm, fy)` is unique. There are 71,932 to 74,719 rows per fiscal year, FY2019 to FY2026.

| Column | Type | Meaning |
|---|---|---|
| `icd_cm` | string | ICD-10-CM code without a decimal point, 3 to 7 characters. Category-level codes are included. |
| `fy` | int32 | Fiscal year, 2019 to 2026. |
| `ms_drg` | string | The modal MS-DRG, 3 characters. |
| `mdc` | string | Major diagnostic category, 2 characters. |
| `source` | string | `grouper_modal_default_2026-05` on every row. |
| `modal_permutation` | string | Label of the first claim variant that produced the modal DRG. `baseline` on every row of this file. |
| `permutations_total` | int8 | Number of claim variants run: 5 on every row. |

### Caveats

- **A modal DRG is a proxy, not an encounter DRG.** It reflects synthetic claims built around one principal diagnosis. A real encounter with other diagnoses, procedures, age, sex or discharge status can group to a different DRG.
- **Placeholder DRGs are present.** `000` is the sentinel for a claim the grouper path could not group: the `drgpy` path returns it for codes it does not recognize, and the build script also writes it when a grouper call raises an error (FY2019 to FY2022: 7,227 to 7,416 codes per year in the shipped table; that table matches a build with drgpy 0.0.6, a full rebuild with 0.0.6 reproduces every row, and which version built it was not established; a rebuild with drgpy 0.2.1 changes 28 of the modal rows per year, see above). `999` is the CMS ungroupable DRG (FY2023 to FY2026: 7,307 to 7,461 codes per year). `998` is a reserved DRG (278 or 279 codes per year). In the shipped FY2019 table the `000` codes are mostly in chapters V, W, X and Y (6,865 of 7,227), which are chapter 20 external-cause codes that do not normally serve as a principal diagnosis. `code_costs.parquet` has no `000` row. It carries 0.0 for `998` and `999` in every year except FY2020, where DRG 999 has an empirical mean of 1,408.0 from 15 discharges. Exclude these three DRGs before using the amounts.
- **No FY2017 or FY2018 modal rows.** There is no grouper output for those years; the annotator back-translates through FY2019 to FY2026 (see the top-level README).

## icd_principal_drg_reachability.parquet

For each ICD-10-CM code and fiscal year, every MS-DRG that any of the five synthetic claim variants reached, and how many variants reached it. It shows which DRGs a principal diagnosis can anchor. Built by the same script as the modal table, in the same run.

The key `(icd_cm, fy, ms_drg)` is unique. There are 115,792 to 177,112 rows per fiscal year, FY2019 to FY2026.

| Column | Type | Meaning |
|---|---|---|
| `icd_cm` | string | ICD-10-CM code without a decimal point. |
| `fy` | int32 | Fiscal year, 2019 to 2026. |
| `ms_drg` | string | An MS-DRG reached by at least one claim variant. Includes the placeholders `000`, `998` and `999` described above. |
| `mdc` | string | Major diagnostic category, 2 characters. |
| `source` | string | `grouper_enumeration_v9_2026-05` on every row. |
| `permutations_hit` | int8 | Number of the five claim variants that reached this DRG, 1 to 5. |

The same caveats apply: these are results for synthetic claims, not for real encounters.
