"""HCRIS Cost Center Supplemental Data File extractor (FY2008-FY2010 + FY2021+).

Extracts the small (~15-21 KB) cost-center supplemental ZIP for each FY in
[2021, 2026] and emits a single unioned silver parquet. The artifact is a
**definition / dictionary table** — it lists the 19 cost-center groups (e.g.
"Routine Days", "Operating Room") and maps each to:

  * the MedPAR Charge Field rolls-up (e.g. "Private Room Charges")
  * the UB-04 Revenue Codes that populate that MedPAR charge field
  * the cost-report Worksheet C / D-3 cell references (e.g. ``C_1_C5_30`` =
    Worksheet C, Part 1, Column 5, Line 30) used to compute CCRs from the
    provider-level HCRIS SAS dataset.

**Form 2552-96 vs Form 2552-10** — CMS published the cost-center supplemental
table only starting FY2021 (under Form 2552-10). For the FY2008-FY2010 era
(Form 2552-96), no equivalent CMS supplemental ever existed; the cost-center
mapping has to be hand-extracted from the CMS HCRIS Specifications for HCFA
2552-96 documentation (line numbers in Worksheet C Part 1 / Worksheet A) and
constant-folded into :func:`_extract_2552_96_definition`. See
``plans/hcris-url-discovery.md`` "Session 1.5 Form 2552-96 discovery"
section for the full URL ladder + verdict.

The 19 canonical cost-center group taxonomy is preserved across both eras
(Routine Days, Operating Room, Anesthesia, Labor & Delivery, etc.). Only the
cell-ref triples change to point at Form 2552-96 worksheet lines:

  * Form 2552-10 Worksheet C Part 1 line 30 (Routine) -> Form 2552-96 line 25
  * Form 2552-10 Worksheet C Part 1 line 50 (OR)      -> Form 2552-96 line 37
  * Form 2552-10 Worksheet D-3 (Medicare charges)     -> Form 2552-96 W/S D-4
  * etc. (full crosswalk in :func:`_extract_2552_96_definition`)

Output schema (one row per ``(fy, cost_center_group, line_description)``):

    fy: int32
    cost_center_group: str
    medpar_charge_field: str | None
    revenue_codes: str | None
    cost_report_line_description: str | None
    hcris_cost_cell_ref: str | None      -- e.g. C_1_C5_30 (or C_1_C5_25 for FY2008-FY2010)
    hcris_charges_cell_ref: str | None   -- e.g. C_1_C6_30 (may be 6&7)
    hcris_medicare_charges_cell_ref: str | None  -- e.g. D3_HOS_C2_30 / D4_HOS_C2_25
    source: str   -- always "hcris_cost_centers"
    effective_start: date32
    effective_end: date32

URL discovery: see ``plans/hcris-url-discovery.md`` for the full per-FY URL
manifest, the FY2024 anomalous-slug landmine, and the Session 1.5 Form
2552-96 constant-fold discovery.

CLI:

    python -m src.extract.hcris_cost_centers --fy 2008
    python -m src.extract.hcris_cost_centers --fy 2026
    python -m src.extract.hcris_cost_centers --fy-range 2021-2026
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from openpyxl import load_workbook

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "hcris_cost_centers"
_SOURCE_TAG: str = "hcris_cost_centers"

# FY2008-FY2010 cost-center definitions are constant-folded from the CMS HCRIS
# Specifications for HCFA 2552-96 (Worksheet line-number documentation). No
# CMS XLSX supplemental was ever published for the 2552-96 era — the
# extractor short-circuits the URL fetch + XLSX parse for these FYs and
# delegates to :func:`_extract_2552_96_definition` instead. See
# ``plans/hcris-url-discovery.md`` "Session 1.5 Form 2552-96 discovery".
_FORM_2552_96_FYS: frozenset[int] = frozenset({2008, 2009, 2010})

# Per-FY URLs verified 2026-05-01 (see plans/hcris-url-discovery.md).
# FY2024 uses the anomalous `hcris-supplemental-data-file` slug — the unpacked
# content IS the cost-center definition table (~16 KB), NOT the 23 MB
# provider-level data file. Distinguished at sniff time by Content-Length.
# FY2008-FY2010 deliberately absent — these FYs use Form 2552-96 and route
# through :func:`_extract_2552_96_definition` instead of any URL fetch.
_HCRIS_COST_CENTER_URLS: dict[int, str] = {
    2026: "https://www.cms.gov/files/zip/fy2026-ipps-fr-cost-center-supplemental-data-file.zip",
    2025: "https://www.cms.gov/files/zip/fy-2025-ipps-final-cost-center-hcris-lines-supplemental-data-file.zip",
    2024: "https://www.cms.gov/files/zip/fy2024-ipps-fr-hcris-supplemental-data-file.zip",
    2023: "https://www.cms.gov/files/zip/fy2023-ipps-fr-cost-center-supplemental-data-file.zip",
    2022: "https://www.cms.gov/files/zip/fy-2022-ipps-fr-cost-center-hcris-lines-supplemental-data-file.zip",
    2021: "https://www.cms.gov/files/zip/fy-2021-ipps-fr-cost-center-hcris-lines-supplemental-data-file.zip",
}

# FY2024 sub-flag 1b: avoid the proposed-rule lookalike URL which returns
# different content (FY2024 Proposed Rule supplemental info, not the FR
# cost-center definition).
_FY2024_BAD_LOOKALIKE: str = (
    "https://www.cms.gov/files/zip/fy2024-cost-center-hcris-lines-supplemental-data-file.zip"
)

# Sub-flag 1b: definition tables are < 100 KB, provider-level HCRIS data files
# are > 1 MB. Used as a sanity sniff before parsing.
_DEFINITION_MAX_BYTES: int = 100_000
_PROVIDER_DATA_MIN_BYTES: int = 1_000_000

_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("fy", pa.int32(), nullable=False),
        pa.field("cost_center_group", pa.string(), nullable=False),
        pa.field("medpar_charge_field", pa.string(), nullable=True),
        pa.field("revenue_codes", pa.string(), nullable=True),
        pa.field("cost_report_line_description", pa.string(), nullable=True),
        pa.field("hcris_cost_cell_ref", pa.string(), nullable=True),
        pa.field("hcris_charges_cell_ref", pa.string(), nullable=True),
        pa.field("hcris_medicare_charges_cell_ref", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
    ]
)


def _norm(v: object) -> str | None:
    """Trim and collapse whitespace; return None for empty / sentinel cells."""
    if v is None:
        return None
    s = " ".join(str(v).split())
    if not s or s in {"-", "—"}:
        return None
    return s


def _find_xlsx(zip_path: Path) -> str:
    """Return the .xlsx member name from a HCRIS cost-center ZIP."""
    with zipfile.ZipFile(zip_path) as zf:
        for n in zf.namelist():
            if n.lower().endswith(".xlsx"):
                return n
    raise RuntimeError(f"No .xlsx member in {zip_path}")


def _parse_xlsx(zip_path: Path, fy: int) -> list[dict]:
    """Parse the cost-center supplemental XLSX into normalized rows.

    Layout: column A carries the cost-center group (sparse — repeats only on
    transition rows due to merged cells in the original CMS workbook). Forward-
    fill on group name. Each non-empty row carries some subset of (MedPAR
    charge field, revenue codes, cost-report line description, HCRIS cost
    cell ref, HCRIS charges cell ref, HCRIS Medicare charges cell ref).

    Parser is permissive: it walks every data row and emits a record whenever
    EITHER the cost-report line description OR any HCRIS cell ref is present.
    Rows with neither (pure separator / banner rows) are skipped.
    """
    xlsx_member = _find_xlsx(zip_path)
    with zipfile.ZipFile(zip_path) as zf, zf.open(xlsx_member) as fh:
        wb = load_workbook(fh, read_only=True, data_only=True)
        ws = wb.active
        all_rows = [list(r) for r in ws.iter_rows(values_only=True)]
        wb.close()

    # Skip rows until we find the header row whose first cell starts with
    # "Cost Center Group". Header may span 1 or 2 rows depending on FY.
    data_start = 0
    for i, row in enumerate(all_rows[:10]):
        first = _norm(row[0]) if row else None
        if first and first.lower().startswith("cost center group"):
            data_start = i + 1
            break

    out: list[dict] = []
    current_group: str | None = None

    for raw in all_rows[data_start:]:
        if not raw:
            continue
        # Pad to 8 columns (the canonical schema width).
        cells: list[object] = list(raw) + [None] * (8 - len(raw))
        col_a = _norm(cells[0])
        col_b = _norm(cells[1])
        col_c = _norm(cells[2])
        # cells[3] is the blank separator column D.
        col_e = _norm(cells[4])
        col_f = _norm(cells[5])
        col_g = _norm(cells[6])
        col_h = _norm(cells[7])

        if col_a:
            current_group = col_a

        # Skip rows that have no actionable content (pure spacer rows).
        if all(v is None for v in (col_b, col_c, col_e, col_f, col_g, col_h)):
            continue

        if current_group is None:
            # Header drift: shouldn't normally happen if the picker found the
            # header row, but skip defensively rather than emit a NULL group.
            continue

        out.append(
            {
                "fy": fy,
                "cost_center_group": current_group,
                "medpar_charge_field": col_b,
                "revenue_codes": col_c,
                "cost_report_line_description": col_e,
                "hcris_cost_cell_ref": col_f,
                "hcris_charges_cell_ref": col_g,
                "hcris_medicare_charges_cell_ref": col_h,
            }
        )

    return out


# Form 2552-96 (FY2008-FY2010) cost-center definition rows. Hand-extracted
# 2026-05-02 from the CMS HCRIS Specifications for HCFA 2552-96 worksheet
# line/column documentation (see ``plans/hcris-url-discovery.md`` "Session 1.5
# Form 2552-96 discovery"). The 19 canonical cost-center groups (per the
# FY2021+ taxonomy) map onto the Form 2552-96 line numbers below.
#
# Worksheet topology mapping:
#   * Worksheet C Part 1 cost (col 5)    -> ``C_1_C5_{line}``
#   * Worksheet C Part 1 charges (col 6) -> ``C_1_C6_{line}``  (inpatient)
#   * Worksheet C Part 1 charges (col 7) -> ``C_1_C7_{line}``  (outpatient)
#   * Worksheet D-4 (col 2)              -> ``D4_HOS_C2_{line}`` (Medicare charges)
#
# Form 2552-96 LINE NUMBERS for the 19 canonical groups (vs Form 2552-10):
#
#   Group                               2552-96 line   2552-10 line
#   Routine Days (Adults & Pediatrics)  25             30
#   Intensive Days (ICU)                26             31
#   Intensive Days (CCU)                27             32
#   Intensive Days (Burn)               28             33
#   Intensive Days (Surgical)           29             34
#   Intensive Days (Other Special)      30             35
#   Operating Room                      37             50
#   Recovery Room                       38             51
#   Labor & Delivery                    39             52
#   Anesthesia                          40             53
#   Radiology Diagnostic                41             54
#   Radiology Therapeutic               42             55
#   Radioisotope                        43             56
#   Laboratory                          44             60
#   PBP Clinic Lab                      45             61
#   Whole Blood / Packed Red Cells      46             62
#   Blood Storing                       47             63
#   IV Therapy                          48             64
#   Respiratory (Inhalation) Therapy    49             65
#   Physical Therapy                    50             66
#   Occupational Therapy                51             67
#   Speech Pathology                    52             68
#   Electrocardiology (Cardiology)      53             69
#   Electroencephalography              54             70
#   Medical Supplies                    55             71
#   Implant Devices                     5530           72
#   Drugs Charged to Patients           56             73
#   Renal Dialysis (Other Services)     57             74
#   ASC (Non-Distinct Part)             58             75
#   Other Ancillary                     59             76
#   FQHC                                63.60-63.84    89
#   Rural Health Clinic                 63.50-63.59    88
#   Clinic                              60             90
#   Emergency                           61             91
#   Observation Beds                    62             92.01
#   Home Program Dialysis               64             94
#   Ambulance Services                  65             95
#   DME-Rented                          66             96
#   DME-Sold                            67             97
#
# CT Scan and MRI in Form 2552-96 are subcategories of Worksheet A line 41
# (Radiology Diagnostic; HCRIS line codes 3230-3239 for CAT Scan, 3430-3439
# for MRI). Since Form 2552-96 does not give CT/MRI dedicated worksheet C
# line numbers, the corresponding cost-center groups are folded into the
# parent "Radiology" group below; CT and MRI as standalone groups don't
# have stand-alone cell refs in 2552-96 era and so carry NULLs.
#
# Cardiac Catheterization in Form 2552-96 is HCRIS code 3120-3129, a
# subcategory of line 53 (Electrocardiology); it likewise lacks a dedicated
# Worksheet C line number, so its cell refs are NULL in 2552-96 era and the
# Cardiology group carries the line-53 refs.

_FORM_2552_96_DEF_ROWS: tuple[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None], ...] = (
    # (cost_center_group, medpar_charge_field, revenue_codes,
    #  cost_report_line_description,
    #  hcris_cost_cell_ref, hcris_charges_cell_ref, hcris_medicare_charges_cell_ref)
    # --- Routine ---
    ("Routine Days", "Private Room Charges", "011X and 014X",
     "Adults & Pediatrics (General Routine Care)",
     "C_1_C5_25", "C_1_C6_25", "D4_HOS_C2_25"),
    # --- Intensive Days (ICU family rolls up to a single group) ---
    ("Intensive Days", "ICU Charges", "020X",
     "Intensive Care Unit",
     "C_1_C5_26", "C_1_C6_26", "D4_HOS_C2_26"),
    ("Intensive Days", "CCU Charges", "0210-0214",
     "Coronary Care Unit",
     "C_1_C5_27", "C_1_C6_27", "D4_HOS_C2_27"),
    ("Intensive Days", "Burn ICU Charges", "0207",
     "Burn Intensive Care Unit",
     "C_1_C5_28", "C_1_C6_28", "D4_HOS_C2_28"),
    ("Intensive Days", "Surgical ICU Charges", "0201",
     "Surgical Intensive Care Unit",
     "C_1_C5_29", "C_1_C6_29", "D4_HOS_C2_29"),
    ("Intensive Days", "Other Special Care Unit Charges", "0209",
     "Other Special Care Unit",
     "C_1_C5_30", "C_1_C6_30", "D4_HOS_C2_30"),
    # --- Operating Room (group includes Recovery Room and L&D per FY2021+ canonicalization) ---
    ("Operating Room", "OR Charges", "036X",
     "Operating Room",
     "C_1_C5_37", "C_1_C6_37", "D4_HOS_C2_37"),
    ("Operating Room", "Recovery Room Charges", "0710",
     "Recovery Room",
     "C_1_C5_38", "C_1_C6_38", "D4_HOS_C2_38"),
    # --- Labor & Delivery ---
    ("Labor & Delivery", "Labor Room/Delivery Room Charges", "072X",
     "Delivery Room and Labor Room",
     "C_1_C5_39", "C_1_C6_39", "D4_HOS_C2_39"),
    # --- Anesthesia ---
    ("Anesthesia", "Anesthesia Charges", "037X",
     "Anesthesiology",
     "C_1_C5_40", "C_1_C6_40", "D4_HOS_C2_40"),
    # --- Radiology (Diagnostic + Therapeutic + Radioisotope; CT/MRI are subcategories) ---
    ("Radiology", "Radiology Diagnostic Charges", "032X, 040X",
     "Radiology - Diagnostic",
     "C_1_C5_41", "C_1_C6_41", "D4_HOS_C2_41"),
    ("Radiology", "Radiology Therapeutic Charges", "033X",
     "Radiology - Therapeutic",
     "C_1_C5_42", "C_1_C6_42", "D4_HOS_C2_42"),
    ("Radiology", "Radioisotope Charges", "034X, 035X",
     "Radioisotope",
     "C_1_C5_43", "C_1_C6_43", "D4_HOS_C2_43"),
    # --- Computed Tomography (CT) Scan — subcat of line 41 (Radiology Diagnostic);
    #     no standalone W/S C line in 2552-96 era. Charges-only proxy via 035X. ---
    ("Computed Tomography (CT) Scan",
     "CT Scan Charges (subcategory of Radiology Diagnostic)",
     "035X",
     "CAT Scan (HCRIS code 3230-3239 — subcategory of Worksheet C line 41)",
     None, "C_1_C7_41", None),
    # --- Magnetic Resonance Imaging (MRI) — subcat of line 41; same caveat ---
    ("Magnetic Resonance Imaging (MRI)",
     "MRI Charges (subcategory of Radiology Diagnostic)",
     "061X",
     "MRI (HCRIS code 3430-3439 — subcategory of Worksheet C line 41)",
     None, None, None),
    # --- Cardiology (Electrocardiology + Cardiac Cath subcategory) ---
    ("Cardiology", "Electrocardiology / Cardiology Charges", "048X, 0730-0739",
     "Electrocardiology",
     "C_1_C5_53", "C_1_C6_53", "D4_HOS_C2_53"),
    # --- Cardiac Catheterization — subcat of line 53 (Electrocardiology) in 2552-96.
    #     HCRIS code 3120-3129; no standalone Worksheet C line number. ---
    ("Cardiac Catheterization",
     "Cardiac Catheterization Charges (subcategory of Cardiology)",
     "048X",
     "Cardiac Catheterization Laboratory (HCRIS code 3120-3129 — subcategory of Worksheet C line 53)",
     None, None, None),
    # --- Inhalation Therapy (Respiratory) ---
    ("Inhalation Therapy", "Respiratory Therapy Charges", "041X, 0460",
     "Respiratory Therapy",
     "C_1_C5_49", "C_1_C6_49", "D4_HOS_C2_49"),
    # --- Therapy Services (Physical, Occupational, Speech) ---
    ("Therapy Services", "Physical Therapy Charges", "042X",
     "Physical Therapy",
     "C_1_C5_50", "C_1_C6_50", "D4_HOS_C2_50"),
    ("Therapy Services", "Occupational Therapy Charges", "043X",
     "Occupational Therapy",
     "C_1_C5_51", "C_1_C6_51", "D4_HOS_C2_51"),
    ("Therapy Services", "Speech Pathology Charges", "044X",
     "Speech Pathology",
     "C_1_C5_52", "C_1_C6_52", "D4_HOS_C2_52"),
    # --- Laboratory (Lab + PBP Clinic Lab + Electroencephalography) ---
    ("Laboratory", "Laboratory Charges", "030X, 031X",
     "Laboratory",
     "C_1_C5_44", "C_1_C6_44", "D4_HOS_C2_44"),
    ("Laboratory", "PBP Clinic Lab Charges", "030X",
     "PBP Clinical Lab Service Program Only",
     "C_1_C5_45", "C_1_C6_45", "D4_HOS_C2_45"),
    ("Laboratory", "EEG Charges", "074X",
     "Electroencephalography",
     "C_1_C5_54", "C_1_C6_54", "D4_HOS_C2_54"),
    # --- Blood and Blood Products ---
    ("Blood and Blood Products", "Blood Charges", "038X, 0390-0392",
     "Whole Blood & Packed Red Blood Cells",
     "C_1_C5_46", "C_1_C6_46", "D4_HOS_C2_46"),
    ("Blood and Blood Products", "Blood Administration Charges", "0390-0399",
     "Blood Storing, Processing, & Transfusing",
     "C_1_C5_47", "C_1_C6_47", "D4_HOS_C2_47"),
    # --- Drugs (IV Therapy + Drugs Charged to Patients) ---
    ("Drugs", "IV Therapy Charges", "026X",
     "Intravenous Therapy",
     "C_1_C5_48", "C_1_C6_48", "D4_HOS_C2_48"),
    ("Drugs", "Pharmacy Charges", "025X, 063X",
     "Drugs Charged To Patient",
     "C_1_C5_56", "C_1_C6_56", "D4_HOS_C2_56"),
    # --- Supplies and Equipment (Medical Supplies + DME) ---
    ("Supplies and Equipment", "Medical/Surgical Supplies Charges", "027X",
     "Medical Supplies Charged to Patients",
     "C_1_C5_55", "C_1_C6_55", "D4_HOS_C2_55"),
    ("Supplies and Equipment", "DME-Rented Charges", "0291",
     "Durable Medical Equipment -- Rented",
     "C_1_C5_66", "C_1_C6_66", "D4_HOS_C2_66"),
    ("Supplies and Equipment", "DME-Sold Charges", "0292",
     "Durable Medical Equipment -- Sold",
     "C_1_C5_67", "C_1_C6_67", "D4_HOS_C2_67"),
    # --- Implantable Devices (line 5530 in Form 2552-96 — note non-integer line) ---
    ("Implantable Devices", "Implant Charges", "0276, 0278",
     "Implant Devices Charged to Patients",
     "C_1_C5_5530", "C_1_C6_5530", "D4_HOS_C2_5530"),
    # --- Emergency Room ---
    ("Emergency Room", "Emergency Room Charges", "045X",
     "Emergency",
     "C_1_C5_61", "C_1_C6_61", "D4_HOS_C2_61"),
    # --- Other Services (rolls up Renal Dialysis, ASC Non-Distinct, Other Ancillary,
    #     Clinic, Observation Beds, Home Dialysis, Ambulance, FQHC, RHC) ---
    ("Other Services", "Renal Dialysis Charges", "080X-0850",
     "Renal Dialysis",
     "C_1_C5_57", "C_1_C6_57", "D4_HOS_C2_57"),
    ("Other Services", "ASC Charges", "049X",
     "ASC (Non-Distinct Part)",
     "C_1_C5_58", "C_1_C6_58", "D4_HOS_C2_58"),
    ("Other Services", "Other Ancillary Charges", "079X",
     "Other Ancillary Cost Centers",
     "C_1_C5_59", "C_1_C6_59", "D4_HOS_C2_59"),
    ("Other Services", "Clinic Charges", "051X",
     "Clinic",
     "C_1_C5_60", "C_1_C6_60", "D4_HOS_C2_60"),
    ("Other Services", "Observation Bed Charges", "0762",
     "Observation Beds (Non-Distinct Part)",
     "C_1_C5_62", "C_1_C6_62", "D4_HOS_C2_62"),
    ("Other Services", "Home Dialysis Charges", "082X",
     "Home Program Dialysis",
     "C_1_C5_64", "C_1_C6_64", "D4_HOS_C2_64"),
    ("Other Services", "Ambulance Charges", "054X",
     "Ambulance Services",
     "C_1_C5_65", "C_1_C6_65", "D4_HOS_C2_65"),
    # FQHC and RHC use sub-line indicators in 2552-96 (63.50-63.59 for RHC,
    # 63.60-63.84 for FQHC). Since the existing FY2021+ silver carries them as
    # generic "Other Services" with line 88/89 refs, we mirror that and
    # constant-fold the canonical sub-line "63.50" / "63.60" so the cell-name
    # tokens remain pure-numeric and downstream (_CELL_TOKEN_RE in
    # hcris_ccrs.py) accepts them under the dot-suffix subline rule.
    ("Other Services", "Rural Health Clinic Charges", "052X",
     "Rural Health Clinic",
     "C_1_C5_63.50", "C_1_C6_63.50", "D4_HOS_C2_63.50"),
    ("Other Services", "FQHC Charges", "0521-0529",
     "Federally Qualified Health Center",
     "C_1_C5_63.60", "C_1_C6_63.60", "D4_HOS_C2_63.60"),
)


def _extract_2552_96_definition(fy: int) -> list[dict]:
    """Form 2552-96 (FY2008-FY2010) cost-center definitions.

    Hand-extracted from the CMS HCRIS Specifications for HCFA 2552-96
    worksheet line/column documentation (see ``plans/hcris-url-discovery.md``
    "Session 1.5 Form 2552-96 discovery"). Returns rows in the same shape
    as :func:`_parse_xlsx` so :func:`_rows_to_table` can consume them
    unchanged.

    Form 2552-96 has NO Worksheet D-3; Medicare hospital ancillary cost &
    charges live on Worksheet D-4 (``D40A180``) instead. Worksheet C Part 1
    layout is preserved across the 2552-96 → 2552-10 transition (col 5 =
    cost, col 6 = inpatient charges, col 7 = outpatient charges) but the
    line numbers shifted (e.g. Routine Days line 25 → 30; OR line 37 → 50).

    The 19 canonical cost-center groups are preserved across both eras.
    Two groups (Computed Tomography (CT) Scan, Magnetic Resonance Imaging
    (MRI), Cardiac Catheterization) are subcategories of parent worksheet
    lines in 2552-96 — they emit rows with ``hcris_cost_cell_ref = NULL``
    so the parent group's CCR is used in CCR computation. CT also has a
    charges-only proxy via outpatient charges col 7 line 41.

    Args:
        fy: Fiscal year (must be 2008, 2009, or 2010).

    Returns:
        List of dicts, one per (cost_center_group, line_description) row.
    """
    if fy not in _FORM_2552_96_FYS:
        raise ValueError(
            f"_extract_2552_96_definition only handles FY{sorted(_FORM_2552_96_FYS)}; "
            f"got fy={fy}"
        )
    out: list[dict] = []
    for (
        group,
        medpar,
        revcodes,
        line_desc,
        cost_ref,
        chrg_ref,
        med_chrg_ref,
    ) in _FORM_2552_96_DEF_ROWS:
        out.append(
            {
                "fy": fy,
                "cost_center_group": group,
                "medpar_charge_field": medpar,
                "revenue_codes": revcodes,
                "cost_report_line_description": line_desc,
                "hcris_cost_cell_ref": cost_ref,
                "hcris_charges_cell_ref": chrg_ref,
                "hcris_medicare_charges_cell_ref": med_chrg_ref,
            }
        )
    return out


def _rows_to_table(rows: list[dict], fy: int) -> pa.Table:
    eff_start, eff_end = config.fy_interval(fy)
    n = len(rows)
    return pa.Table.from_arrays(
        [
            pa.array([r["fy"] for r in rows], type=pa.int32()),
            pa.array([r["cost_center_group"] for r in rows], type=pa.string()),
            pa.array([r["medpar_charge_field"] for r in rows], type=pa.string()),
            pa.array([r["revenue_codes"] for r in rows], type=pa.string()),
            pa.array([r["cost_report_line_description"] for r in rows], type=pa.string()),
            pa.array([r["hcris_cost_cell_ref"] for r in rows], type=pa.string()),
            pa.array([r["hcris_charges_cell_ref"] for r in rows], type=pa.string()),
            pa.array([r["hcris_medicare_charges_cell_ref"] for r in rows], type=pa.string()),
            pa.array([_SOURCE_TAG] * n, type=pa.string()),
            pa.array([eff_start] * n, type=pa.date32()),
            pa.array([eff_end] * n, type=pa.date32()),
        ],
        schema=_PARQUET_SCHEMA,
    )


def extract_fy(
    fy: int,
    *,
    download_date: str | None = None,
    session: requests.Session | None = None,
) -> Path | None:
    """Download + parse the cost-center supplemental for a single FY.

    Returns the path to a per-FY parquet under ``data/silver/per_source/``,
    or ``None`` if the URL is missing / 404s.

    For FY2008-FY2010 (Form 2552-96), the URL fetch and XLSX parse are
    bypassed entirely; rows are constant-folded from
    :func:`_extract_2552_96_definition`. No CMS supplemental table was ever
    published for the 2552-96 era, so the constant-fold is the canonical
    source.
    """
    # Form 2552-96 era — no URL fetch, no XLSX parse. Constant-fold from
    # the hand-extracted CMS HCRIS Specifications mapping in this module.
    if fy in _FORM_2552_96_FYS:
        rows = _extract_2552_96_definition(fy)
        if not rows:
            raise RuntimeError(
                f"FY{fy} _extract_2552_96_definition yielded zero rows"
            )
        table = _rows_to_table(rows, fy)
        out = (
            config.SILVER_PER_SOURCE
            / "hcris"
            / "cost_centers"
            / f"hcris_cost_centers_fy{fy}.parquet"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out, compression="zstd")
        print(
            f"[hcris_cost_centers] FY{fy} (Form 2552-96 constant-fold): "
            f"{len(rows)} rows -> {out}",
            file=sys.stderr,
        )
        return out

    url = _HCRIS_COST_CENTER_URLS.get(fy)
    if not url:
        print(
            f"[hcris_cost_centers] FY{fy} not in URL manifest "
            "(only FY2008-FY2010 [Form 2552-96 constant-fold] and "
            "FY2021-FY2026 [Form 2552-10 XLSX] supported); skipping",
            file=sys.stderr,
        )
        return None
    if url == _FY2024_BAD_LOOKALIKE:  # belt-and-suspenders
        raise RuntimeError(f"FY2024 lookalike URL routed in error: {url}")

    sess = session or _http.make_session()
    try:
        bronze = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=fy,
            filename=Path(url).name,
            download_date=download_date,
            session=sess,
        )
    except requests.HTTPError as exc:
        print(
            f"[hcris_cost_centers] HTTPError for FY{fy}: {exc}; skipping",
            file=sys.stderr,
        )
        return None

    # Sub-flag 1b sniff: definition tables < 100 KB. If the file is much
    # bigger than that, the URL has drifted to point at the provider-level
    # SAS dataset instead. Refuse to parse rather than silently corrupt.
    sz = bronze.stat().st_size
    if sz >= _PROVIDER_DATA_MIN_BYTES:
        raise RuntimeError(
            f"FY{fy} bronze file {bronze.name} is {sz:,} bytes (>= "
            f"{_PROVIDER_DATA_MIN_BYTES:,}); looks like the provider-level "
            "HCRIS data file, not the cost-center definition table. URL drift?"
        )
    if sz > _DEFINITION_MAX_BYTES:
        # Soft warn but proceed — content sniff below will catch genuine drift.
        print(
            f"[hcris_cost_centers] FY{fy} {bronze.name} = {sz:,} bytes; "
            f"larger than typical definition file (<{_DEFINITION_MAX_BYTES:,})",
            file=sys.stderr,
        )

    rows = _parse_xlsx(bronze, fy)
    if not rows:
        raise RuntimeError(f"FY{fy} XLSX yielded zero rows from {bronze}")
    table = _rows_to_table(rows, fy)

    # Per-FY parquet under per_source/hcris/cost_centers/. Subdir layout
    # mirrors the Phase 5 PUF convention so the codes_master non-recursive
    # glob (`per_source/*.parquet`) does not pick HCRIS files up.
    out = (
        config.SILVER_PER_SOURCE
        / "hcris"
        / "cost_centers"
        / f"hcris_cost_centers_fy{fy}.parquet"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[hcris_cost_centers] FY{fy}: {len(rows)} rows -> {out}",
        file=sys.stderr,
    )
    return out


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    download_date: str | None = None,
) -> list[Path]:
    sess = _http.make_session()
    out: list[Path] = []
    for fy in range(start_fy, end_fy + 1):
        try:
            p = extract_fy(fy, download_date=download_date, session=sess)
            if p is not None:
                out.append(p)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[hcris_cost_centers] FY{fy} failed: {exc}; continuing",
                file=sys.stderr,
            )
    return out


def build_unioned_silver(out_path: Path | None = None) -> Path:
    """Union all per-FY parquets into ``data/silver/hcris_cost_centers.parquet``.

    Driven by ``data/silver/per_source/hcris_cost_centers_fy*.parquet``; if
    none exist, raises FileNotFoundError.
    """
    out = out_path or (config.SILVER / "hcris_cost_centers.parquet")
    src_dir = config.SILVER_PER_SOURCE / "hcris" / "cost_centers"
    paths = sorted(src_dir.glob("hcris_cost_centers_fy*.parquet"))
    if not paths:
        raise FileNotFoundError(
            f"No per-FY hcris_cost_centers parquets found under {src_dir}; "
            "run `python -m src.extract.hcris_cost_centers --fy-range 2008-2026` "
            "first (FY2008-FY2010 are constant-folded from Form 2552-96; "
            "FY2021-FY2026 fetch the CMS XLSX supplemental)."
        )
    tables = [pq.read_table(p) for p in paths]
    unioned = pa.concat_tables(tables)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(unioned, out, compression="zstd")
    print(
        f"[hcris_cost_centers] unioned {len(paths)} FY parquets -> {out} "
        f"({unioned.num_rows} rows)",
        file=sys.stderr,
    )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.hcris_cost_centers",
        description=(
            "Build the HCRIS cost-center definition table (19 cost-center groups "
            "mapped to MedPAR/UB-04/HCRIS cell references) for FY2008-FY2010 "
            "(Form 2552-96 constant-fold) and FY2021-FY2026 (Form 2552-10 XLSX "
            "fetched from cms.gov)."
        ),
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fy", type=int)
    grp.add_argument("--fy-range", type=str, metavar="START-END")
    grp.add_argument(
        "--build-silver",
        action="store_true",
        help="Union the per-FY parquets into data/silver/hcris_cost_centers.parquet.",
    )
    parser.add_argument("--download-date", default=None)
    args = parser.parse_args(argv)

    if args.build_silver:
        build_unioned_silver()
        return 0
    if args.fy is not None:
        extract_fy(args.fy, download_date=args.download_date)
    else:
        try:
            a, b = args.fy_range.split("-", 1)
            start, end = int(a), int(b)
        except ValueError as exc:
            raise SystemExit(
                f"--fy-range must be START-END, got {args.fy_range!r}"
            ) from exc
        extract_fy_range(start, end, download_date=args.download_date)
        # Auto-build the unioned silver after a range pull.
        try:
            build_unioned_silver()
        except FileNotFoundError as exc:
            print(f"[hcris_cost_centers] {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
