"""Project paths, year tiers, and per-FY source URL manifests."""

from datetime import date
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[1]
DATA: Path = ROOT / "data"
BRONZE: Path = DATA / "bronze"
SILVER: Path = DATA / "silver"
GOLD: Path = DATA / "gold"
NEO4J_IMPORT: Path = DATA / "neo4j_import"
SILVER_PER_SOURCE: Path = SILVER / "per_source"

TIER_A_FYS: list[int] = list(range(2016, 2027))
TIER_B_FYS: list[int] = list(range(2011, 2016))
TIER_C_FYS: list[int] = list(range(2008, 2011))

ICD10CM_SOURCES: dict[int, str] = {
    fy: f"https://ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/{fy}/"
    for fy in TIER_A_FYS
}

ICD10CM_FILENAME_PATTERNS: dict[str, str] = {
    "table_and_index": "icd10cm-table-and-index-{fy}.zip",
    "code_descriptions": "icd10cm-Code-Descriptions-{fy}.zip",
    "addenda": "icd10cm-addenda-{fy}.zip",
    "poa_exempt": "POAexemptCodesFY{yy}.zip",
}

# Per-FY filename overrides for the ICD-10-CM CDC FTP. CDC has used a different
# naming convention almost every year between FY2016 and FY2025; the canonical
# patterns above only line up cleanly with FY2026's modern scheme. Each entry is
# a partial override: keys present here win over the canonical pattern, keys
# absent fall back to the canonical pattern.
#
# Filenames here are RAW (not URL-encoded) — extractors URL-encode at compose
# time. None means "no such artifact for this FY" — extractors must adapt the
# parser path (e.g. FY2016 has no addenda; FY2017 has no flat-file code list).
#
# A few FYs (FY2019, FY2020) ship the artifacts as four LOOSE files at the
# directory level (no ZIP wrapper). Those are encoded as a list of filenames
# under the "table_and_index" key; the extractor recognizes the list and treats
# the FY as "loose-files" mode (downloads each file separately into bronze and
# parses them in place rather than via zipfile).
ICD10CM_FILENAME_OVERRIDES: dict[int, dict[str, object]] = {
    2016: {
        "table_and_index": "ICD10CM_FY2016_ Full_ XML.ZIP",
        "code_descriptions": "ICD10CM_FY2016_code_descriptions.zip",
        # Launch year — no addenda artifact published.
        "addenda": None,
        "poa_exempt": "POAexemptCodes2016.zip",
    },
    2017: {
        "table_and_index": "ICD10CM_FY2017_Full_XML.zip",
        # No flat-file code list shipped — codes are derived from tabular XML.
        "code_descriptions": None,
        # Only PDFs published — no machine-readable addenda.
        "addenda": None,
        "poa_exempt": "POAexemptCodes2017.zip",
    },
    2018: {
        "table_and_index": "ICD-10-CM-Codes-Tables-and-Index-2018.zip",
        "code_descriptions": "2018-ICD-10-CM-Codes-File.zip",
        "addenda": "2018-ICD-10-Addendum.zip",
        "poa_exempt": "POAexemptAddCodes2018.zip",
    },
    2019: {
        # Loose files — list rather than scalar tells the extractor to use
        # loose-files mode. Order: tabular XML, codes flat, order flat, addenda.
        "table_and_index": [
            "icd10cm_tabular_2019.xml",
            "icd10cm_codes_2019.txt",
            "icd10cm_order_2019.txt",
            "icd10cm_codes_addenda_2019.txt",
        ],
        # Loose-mode FYs encode all four loose files under table_and_index;
        # the other keys are unused.
        "code_descriptions": "__LOOSE__",
        "addenda": "__LOOSE__",
        "poa_exempt": "POAexemptCodes2019.zip",
    },
    2020: {
        "table_and_index": [
            "icd10cm_tabular_2020.xml",
            "icd10cm_codes_2020.txt",
            "icd10cm_order_2020.txt",
            "icd10cm_codes_addenda_2020.txt",
        ],
        "code_descriptions": "__LOOSE__",
        "addenda": "__LOOSE__",
        "poa_exempt": "POAexemptCodes2020.zip",
    },
    2021: {
        "table_and_index": "icd10cm-Full-Tab-Index Table-Jan-2021.zip",
        "code_descriptions": "icd10cm-codes-order-Jan-2021.zip",
        "addenda": "icd10cm-addenda-files-Jan-2021.zip",
        # Pre-FY22 POA naming convention (4-digit year, no FY prefix).
        "poa_exempt": "POAexemptCodes2021.zip",
    },
    2022: {
        "table_and_index": "Table and Index zip.zip",
        "code_descriptions": "Code Descriptions zip.zip",
        "addenda": "Addendum zip.zip",
        # FY22 is where CDC switched to the modern POAexemptCodesFY{yy}.zip name.
        "poa_exempt": "POAexemptCodesFY22.zip",
    },
    2023: {
        "table_and_index": "icd10cm-Tabular-Index-2023.zip",
        # Note: filename starts with "icd10-" not "icd10cm-".
        "code_descriptions": "icd10-Order-CodeFiles2023.zip",
        "addenda": "icd10cm-addendum-2023.zip",
        "poa_exempt": "POAexemptCodesFY23.zip",
    },
    2024: {
        "table_and_index": "icd10cm-Table and Index-2024.zip",
        "code_descriptions": "icd10cm-CodesDescriptions-2024.zip",
        "addenda": "icd10cm-addendum-2024.zip",
        "poa_exempt": "POAexemptCodesFY24.zip",
    },
    2025: {
        "table_and_index": "icd10cm-table-index-2025.zip",
        "code_descriptions": "ICD10-CM Code Descriptions 2025.zip",
        "addenda": "ICD10cm-addendum-2025.zip",
        "poa_exempt": "POAexemptCodesFY25.zip",
    },
    # FY2026 has no entry — the canonical patterns + space-fallback in
    # icd10cm.py already handle it.
}

# ICD-10-PCS per-FY URL manifest is hand-maintained because CMS reorganized the
# archive page in 2024 and individual FY URLs drift. Each value is the full URL
# to the per-FY ZIP (a *single* ZIP per FY — unlike ICD-10-CM, which uses a
# directory base). Sources verified 2026-04-29 against:
#   - https://www.cms.gov/medicare/coding-billing/icd-10-codes (live FY page)
#   - https://www.cms.gov/medicare/coding-billing/icd-10-codes/icd-10-cm-icd-10-pcs-gem-archive
# FY2018-FY2026 ZIPs ship `icd10pcs_codes_{fy}.txt` (code + long title only) and
# `codes_addenda_{fy}.txt`. FY2016-FY2017 ZIPs ship the full fixed-width Order
# File `icd10pcs_order_{fy}.txt` (which carries short titles + an inline action
# flag) — see src.extract.icd10pcs for the parser branch.
ICD10PCS_SOURCES: dict[int, str] = {
    2026: "https://www.cms.gov/files/zip/2026-icd-10-pcs-codes-file.zip",
    2025: "https://www.cms.gov/files/zip/2025-icd-10-pcs-codes-file.zip",
    2024: "https://www.cms.gov/files/zip/2024-icd-10-pcs-codes-file.zip",
    2023: "https://www.cms.gov/files/zip/2023-icd-10-pcs-codes-file.zip",
    2022: "https://www.cms.gov/files/zip/2022-icd-10-pcs-codes-file.zip",
    2021: "https://www.cms.gov/files/zip/2021-icd-10-pcs-codes-file.zip",
    2020: "https://www.cms.gov/medicare/coding/icd10/downloads/2020-icd-10-pcs-codes.zip",
    2019: "https://www.cms.gov/medicare/coding/icd10/downloads/2019-icd-10-pcs-codes-file.zip",
    2018: "https://www.cms.gov/medicare/coding/icd10/downloads/2018-icd-10-pcs-codes-file.zip",
    2017: "https://www.cms.gov/medicare/coding/icd10/downloads/2017-pcs-long-abbrev-titles.zip",
    2016: "https://www.cms.gov/medicare/coding/icd10/downloads/2016-pcs-long-abbrev-titles.zip",
}

BRONZE_DATE_FMT: str = "%Y-%m-%d"


def fy_interval(fy: int) -> tuple[date, date]:
    return (date(fy - 1, 10, 1), date(fy, 9, 30))


def cy_interval(cy: int) -> tuple[date, date]:
    """Return the calendar-year interval (Jan 1 .. Dec 31) for `cy`.

    Used by HCPCS Level II and other CY-keyed sources whose effective_start /
    effective_end span the calendar year rather than the federal fiscal year.
    """
    return (date(cy, 1, 1), date(cy, 12, 31))


def bronze_dir(
    source: str,
    fy_or_cy: int,
    download_date: str | None = None,
    *,
    period: str = "fy",
) -> Path:
    """Compose the bronze directory for a download.

    Layout: ``BRONZE / {source} / {period}{fy_or_cy} / {download_date}``.

    Parameters
    ----------
    source:
        Bronze subdirectory name (e.g., ``"icd10cm"``, ``"hcpcs"``, ``"gems"``).
    fy_or_cy:
        Year integer used for the period subdirectory.
    download_date:
        Optional ``YYYY-MM-DD`` override; defaults to today.
    period:
        Period prefix — ``"fy"`` (default) for fiscal-year sources or ``"cy"``
        for calendar-year sources (HCPCS Level II).
    """
    dl = download_date or date.today().strftime(BRONZE_DATE_FMT)
    return BRONZE / source / f"{period}{fy_or_cy}" / dl


# ---------------------------------------------------------------------------
# Session 2 sources: ICD-9-CM, ICD-9-Vol3, GEMs, HCPCS Level II
# ---------------------------------------------------------------------------
#
# Per Phase 1 plan (plans/plan-v2.md), these legacy / adjunct code systems are
# extracted in Session 2 to round out Tier B/C work (FY2008-FY2015) and to
# cover CPT-adjacent outpatient codes across all years.
#
# URL discovery: Each per-FY/CY URL dict starts with the empirical
# best-known URL pattern. Extractors are responsible for runtime
# fallback (HEAD probe, hyphen<->space variant, etc.) and for surfacing
# discovered overrides in landmines.


# Tier B/C (ICD-9 era): FY2008-FY2015 inclusive. ICD-9-CM was frozen for the
# ICD-10 transition starting Oct 1, 2011; FY2012-FY2015 carry duplicate codes
# across years which the codes_master union dedupes on the
# (code, code_system, fy_or_cy) triple guard.
ICD9_TIER_BC_FYS: list[int] = list(range(2008, 2016))

# HCPCS Level II is calendar-year keyed (effective Jan 1). October "_O"
# release is the annual full-replacement file effective the following Jan 1.
HCPCS_CYS: list[int] = list(range(2008, 2027))


# CMS "ICD-9-CM Diagnosis & Procedure Codes" page — lists the per-FY
# code-titles ZIPs for ICD-9-CM (diagnoses) and ICD-9-CM Volume 3 (procedures).
# URL verified 2026-04-29 by Phase B3.
#
# NOTE: this is NOT the GEMs archive page. The GEMs archive lives at
# `.../icd-10-cm-icd-10-pcs-gem-archive` and is hard-coded into the GEMs
# extractor's URL set below; ICD-9 code titles do not live there.
ICD9_ARCHIVE_PAGE_URL: str = (
    "https://www.cms.gov/medicare/coding-billing/icd-10-codes/"
    "icd-9-cm-diagnosis-procedure-codes-abbreviated-and-full-code-titles"
)

# ICD-9-CM per-FY ZIP URLs (FY2008-FY2015). Verified 2026-04-29 by Phase B3.
# Each value is the URL of the per-FY "Full and Abbreviated Code Titles" ZIP
# from CMS. The FY2008-FY2010 ZIPs ship short titles only; FY2011-FY2015 ship
# both long and short titles. ICD-9-Vol3 (procedures) ships in the SAME
# per-FY ZIP — see ICD9PCS_SOURCES below.
ICD9CM_SOURCES: dict[int, str] = {
    2008: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/v25_icd9.zip",
    2009: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/v26_icd9.zip",
    2010: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/v27_icd9.zip",
    2011: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/cmsv28_master_descriptions.zip",
    2012: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/cmsv29_master_descriptions.zip",
    2013: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/cmsv30_master_descriptions.zip",
    2014: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/cmsv31-master-descriptions.zip",
    # CMS reused the v31 artifact byte-for-byte for v32. FY15 carried the same
    # content as FY14 plus the New Tech codes added through the freeze
    # exception process (the New Tech codes themselves are PDF addenda only).
    2015: "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/icd-9-cm-v32-master-descriptions.zip",
}

# ICD-9-CM per-FY filename overrides (analog to ICD10CM_FILENAME_OVERRIDES).
# FY2010 needs a supplemental long-titles ZIP because the v27 primary ZIP
# only ships short titles — encoded here as `long_supplement` so extractors
# can pick it up alongside the primary download.
ICD9CM_FILENAME_OVERRIDES: dict[int, dict[str, object]] = {
    2010: {
        "long_supplement": (
            "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/"
            "downloads/fy2010diagnosis-procedurecodesfulltitles.zip"
        ),
    },
}

# ICD-9-Vol3 (procedure codes) per-FY ZIP URLs. ICD-9-Vol3 always co-ships in
# the SAME per-FY ZIP as ICD-9-CM diagnoses — there is no separate procedures
# ZIP across FY2008-FY2015. We keep the dict separate so extractors can look
# up by name without coupling to ICD9CM_SOURCES, but the URL set is identical.
ICD9PCS_SOURCES: dict[int, str] = dict(ICD9CM_SOURCES)


# GEMs (frozen FY2018) URLs. CMS does NOT publish a single combined
# `2018-icd-10-cm-and-gems.zip` — that filename 404s. The actual archive
# splits the artifacts into FOUR ZIPs: per-system GEMs (CM and PCS) plus
# per-system Reimbursement Mappings (DX and PR, last published FY2016). All
# four URLs verified 2026-04-29 against the GEMs archive page
# `https://www.cms.gov/medicare/coding-billing/icd-10-codes/icd-10-cm-icd-10-pcs-gem-archive`.
#
# Logical keys: cm | pcs | rm_dx | rm_pr.
GEMS_SOURCES: dict[str, str] = {
    "cm": (
        "https://www.cms.gov/medicare/coding/icd10/downloads/"
        "2018-icd-10-cm-general-equivalence-mappings.zip"
    ),
    "pcs": (
        "https://www.cms.gov/medicare/coding/icd10/downloads/"
        "2018-icd-10-pcs-general-equivalence-mappings.zip"
    ),
    "rm_dx": (
        "https://www.cms.gov/medicare/coding/icd10/downloads/"
        "reimbursement_mapping_dx_2016.zip"
    ),
    "rm_pr": (
        "https://www.cms.gov/medicare/coding/icd10/downloads/"
        "reimbursement_mapping_pr_2016.zip"
    ),
}

# Logical (zip_key, member_kind) -> in-ZIP filename. The four keys map onto
# `GEMS_SOURCES` above. ZIP-internal filenames verified 2026-04-29.
#
# member_kind values:
#   "i9_to_i10": forward GEMs (ICD-9 -> ICD-10)
#   "i10_to_i9": backward GEMs (ICD-10 -> ICD-9)
#   "rm":       reimbursement mapping (ICD-10 -> ICD-9, cluster format)
GEMS_FILES: dict[tuple[str, str], str] = {
    ("cm", "i9_to_i10"): "2018_I9gem.txt",
    ("cm", "i10_to_i9"): "2018_I10gem.txt",
    ("pcs", "i9_to_i10"): "gem_i9pcs.txt",
    ("pcs", "i10_to_i9"): "gem_pcsi9.txt",
    ("rm_dx", "rm"): "reimb_map_dx_2016.txt",
    ("rm_pr", "rm"): "reimb_map_pr_2016.txt",
}


# HCPCS Level II per-CY ZIP URLs. CMS naming has drifted across three eras:
#   - CY2008-CY2016: pre-Drupal `HCPC{yy}_O_AnnAlpha.zip` is universally 404
#     on cms.gov as of 2026-04-29 with no Wayback snapshots either. We keep
#     the canonical-pattern entry so HCPCS_CYS stays consistent; the
#     extractor will request the URL, get a 404, and the bulk runner logs +
#     skips per CY.
#   - CY2017-CY2020: legacy `cms.gov/medicare/coding/hcpcsreleasecodesets/
#     downloads/` path with `{cy}-Alpha-Numeric-HCPCS-File.zip` filename.
#   - CY2021-CY2026: modern `cms.gov/files/zip/` slug with
#     `{month}-{cy}-alpha-numeric-hcpcs-file.zip`. Some CYs only surface a
#     January Q1 update under this slug (CY2021/CY2023/CY2026); the rest use
#     the October annual full-replacement.
#
# All non-pre-2017 URLs verified 2026-04-29 by Phase B2.
HCPCS_BASE_URL: str = "https://www.cms.gov/files/zip/"
_HCPCS_LEGACY_BASE: str = (
    "https://www.cms.gov/medicare/coding/hcpcsreleasecodesets/downloads/"
)
HCPCS_SOURCES: dict[int, str] = {
    # ---- CY2008-CY2016: UNAVAILABLE on cms.gov (canonical 404 placeholder). ----
    # Extractor will request these, hit 404, and the bulk runner logs+skips.
    # Kept here to keep HCPCS_CYS consistent and to leave a forward-compat
    # anchor if a mirror is ever discovered.
    2008: f"{HCPCS_BASE_URL}2008-alpha-numeric-hcpcs-file.zip",
    2009: f"{HCPCS_BASE_URL}2009-alpha-numeric-hcpcs-file.zip",
    2010: f"{HCPCS_BASE_URL}2010-alpha-numeric-hcpcs-file.zip",
    2011: f"{HCPCS_BASE_URL}2011-alpha-numeric-hcpcs-file.zip",
    2012: f"{HCPCS_BASE_URL}2012-alpha-numeric-hcpcs-file.zip",
    2013: f"{HCPCS_BASE_URL}2013-alpha-numeric-hcpcs-file.zip",
    2014: f"{HCPCS_BASE_URL}2014-alpha-numeric-hcpcs-file.zip",
    2015: f"{HCPCS_BASE_URL}2015-alpha-numeric-hcpcs-file.zip",
    2016: f"{HCPCS_BASE_URL}2016-alpha-numeric-hcpcs-file.zip",
    # ---- CY2017-CY2020: legacy `Downloads/` host. ----
    2017: f"{_HCPCS_LEGACY_BASE}2017-Alpha-Numeric-HCPCS-File.zip",
    2018: f"{_HCPCS_LEGACY_BASE}2018-Alpha-Numeric-HCPCS-File.zip",
    2019: f"{_HCPCS_LEGACY_BASE}2019-Alpha-Numeric-HCPCS-File.zip",
    2020: f"{_HCPCS_LEGACY_BASE}2020-Alpha-Numeric-HCPCS-File.zip",
    # ---- CY2021-CY2026: modern slug. ----
    # CY2021/CY2023/CY2026 use the January Q1 update; the October full-
    # replacement slug isn't surfaced for those years. Others use October.
    2021: f"{HCPCS_BASE_URL}january-2021-alpha-numeric-hcpcs-file.zip",
    2022: f"{HCPCS_BASE_URL}october-2022-alpha-numeric-hcpcs-file.zip",
    2023: f"{HCPCS_BASE_URL}january-2023-alpha-numeric-hcpcs-file.zip",
    2024: f"{HCPCS_BASE_URL}october-2024-alpha-numeric-hcpcs-file.zip",
    2025: f"{HCPCS_BASE_URL}october-2025-alpha-numeric-hcpcs-file.zip",
    2026: f"{HCPCS_BASE_URL}january-2026-alpha-numeric-hcpcs-file.zip",
}

# HCPCS per-CY filename overrides. Same shape as ICD10CM_FILENAME_OVERRIDES:
# keys present here win over the canonical pattern. None discovered to date —
# the extractor's `_find_workbook_member` heuristic auto-detects the ANWEB
# workbook member inside each ZIP, so no per-CY overrides are needed even
# though CMS has drifted ZIP-internal naming several times.
HCPCS_FILENAME_OVERRIDES: dict[int, dict[str, object]] = {}


# ---------------------------------------------------------------------------
# Session 3 / Phase 2 sources: PFS RVU (physician fee schedule)
# ---------------------------------------------------------------------------
#
# Per Phase 2 plan (plans/plan-v3.md), the PFS RVU quarterly releases are the
# first fee schedule we ship. Each per-(cy, quarter) ZIP carries the PPRRVU
# CSV — the locked-schema layer is national-mean, so locality / GPCI files
# inside the same ZIP are deferred. The extractor downloads, parses, and
# emits long-form rows (one per setting) into
# `silver/per_source/pfs_rvu/pfs_rates_cy{cy}_{q}.parquet`. The unifier in
# `src/transform/pfs_rates.py` will union these into `silver/pfs_rates.parquet`.
#
# Coverage: CY2008-CY2026 × four quarters = 76 cells. URL discovery follows
# the HCPCS precedent — we encode canonical-pattern placeholders for every
# (cy, quarter) cell, and the extractor logs+skips on 404. Pre-2017 quarters
# may partially or fully be unavailable on cms.gov today; we keep the
# placeholders so any later URL-overrides are append-only edits, not
# pattern-rewrites.
#
# Quarter encoding mirrors CMS's letter scheme:
#   A = Q1 (Jan 1 - Mar 31)
#   B = Q2 (Apr 1 - Jun 30)
#   C = Q3 (Jul 1 - Sep 30)
#   D = Q4 (Oct 1 - Dec 31)


PFS_CYS: list[int] = list(range(2008, 2027))

PFS_QUARTERS: list[str] = ["A", "B", "C", "D"]


# PFS quarterly RVU per-(cy, quarter) URL manifest. CMS has used (at least)
# three URL eras for these files; we encode canonical-pattern placeholders
# for every (cy, quarter) cell:
#
#   - CY2008-CY2020 (legacy `Downloads/` host on the medicare-fee-for-service
#     subtree):
#       https://www.cms.gov/medicare/medicare-fee-for-service-payment/
#         PhysicianFeeSched/PFS-Federal-Regulation-Notices/Downloads/
#         RVU{YY}{Q}.zip
#     where `YY = cy % 100` (2-digit, zero-padded) and `Q` is the quarter
#     letter (A/B/C/D). Example: CY2018 Q3 = `RVU18C.zip`.
#
#   - CY2021-CY2026 (modern Drupal `files/zip/` slug):
#       https://www.cms.gov/files/zip/rvu{YY}{q-lower}.zip
#     The actually-published filenames sometimes carry an `-updated-{MM}-{YYYY}`
#     date suffix, but the suffix is unstable and CMS often republishes under
#     a different date. The plain `rvu{YY}{q}.zip` form is the canonical
#     placeholder; if it 404s we fall back per the HCPCS precedent.
#
# All entries are hard-coded explicitly so any later URL-overrides are
# append-only edits to this dict, not pattern rewrites — mirrors the
# HCPCS_SOURCES style. Extractor will request these and log+skip on 404.
_PFS_LEGACY_BASE: str = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment/"
    "PhysicianFeeSched/PFS-Federal-Regulation-Notices/Downloads/"
)
_PFS_MODERN_BASE: str = "https://www.cms.gov/files/zip/"
# CY2017-CY2019 quarterly RVU ZIPs live on the lowercase
# `physicianfeesched/downloads/` slug (no `PFS-Federal-Regulation-Notices`
# segment), with lowercase filenames `rvu{YY}{q-lower}.zip`. This is a
# distinct era from the original `_PFS_LEGACY_BASE` (CY2008-CY2016) and the
# `_PFS_MODERN_BASE` (CY2021+). Verified via curl on 2026-04-30 — see
# CLAUDE.md Phase 2 landmine "PFS quarterly URL slug drift across three
# CMS eras".
_PFS_MID_BASE: str = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment/"
    "physicianfeesched/downloads/"
)
PFS_SOURCES: dict[tuple[int, str], str] = {
    # ---- CY2008-CY2020: legacy `Downloads/` host. ----
    (2008, "A"): f"{_PFS_LEGACY_BASE}RVU08A.zip",
    (2008, "B"): f"{_PFS_LEGACY_BASE}RVU08B.zip",
    (2008, "C"): f"{_PFS_LEGACY_BASE}RVU08C.zip",
    (2008, "D"): f"{_PFS_LEGACY_BASE}RVU08D.zip",
    (2009, "A"): f"{_PFS_LEGACY_BASE}RVU09A.zip",
    (2009, "B"): f"{_PFS_LEGACY_BASE}RVU09B.zip",
    (2009, "C"): f"{_PFS_LEGACY_BASE}RVU09C.zip",
    (2009, "D"): f"{_PFS_LEGACY_BASE}RVU09D.zip",
    (2010, "A"): f"{_PFS_LEGACY_BASE}RVU10A.zip",
    (2010, "B"): f"{_PFS_LEGACY_BASE}RVU10B.zip",
    (2010, "C"): f"{_PFS_LEGACY_BASE}RVU10C.zip",
    (2010, "D"): f"{_PFS_LEGACY_BASE}RVU10D.zip",
    (2011, "A"): f"{_PFS_LEGACY_BASE}RVU11A.zip",
    (2011, "B"): f"{_PFS_LEGACY_BASE}RVU11B.zip",
    (2011, "C"): f"{_PFS_LEGACY_BASE}RVU11C.zip",
    (2011, "D"): f"{_PFS_LEGACY_BASE}RVU11D.zip",
    (2012, "A"): f"{_PFS_LEGACY_BASE}RVU12A.zip",
    (2012, "B"): f"{_PFS_LEGACY_BASE}RVU12B.zip",
    (2012, "C"): f"{_PFS_LEGACY_BASE}RVU12C.zip",
    (2012, "D"): f"{_PFS_LEGACY_BASE}RVU12D.zip",
    (2013, "A"): f"{_PFS_LEGACY_BASE}RVU13A.zip",
    (2013, "B"): f"{_PFS_LEGACY_BASE}RVU13B.zip",
    (2013, "C"): f"{_PFS_LEGACY_BASE}RVU13C.zip",
    (2013, "D"): f"{_PFS_LEGACY_BASE}RVU13D.zip",
    (2014, "A"): f"{_PFS_LEGACY_BASE}RVU14A.zip",
    (2014, "B"): f"{_PFS_LEGACY_BASE}RVU14B.zip",
    (2014, "C"): f"{_PFS_LEGACY_BASE}RVU14C.zip",
    (2014, "D"): f"{_PFS_LEGACY_BASE}RVU14D.zip",
    (2015, "A"): f"{_PFS_LEGACY_BASE}RVU15A.zip",
    (2015, "B"): f"{_PFS_LEGACY_BASE}RVU15B.zip",
    (2015, "C"): f"{_PFS_LEGACY_BASE}RVU15C.zip",
    (2015, "D"): f"{_PFS_LEGACY_BASE}RVU15D.zip",
    (2016, "A"): f"{_PFS_LEGACY_BASE}RVU16A.zip",
    (2016, "B"): f"{_PFS_LEGACY_BASE}RVU16B.zip",
    (2016, "C"): f"{_PFS_LEGACY_BASE}RVU16C.zip",
    (2016, "D"): f"{_PFS_LEGACY_BASE}RVU16D.zip",
    (2017, "A"): f"{_PFS_MID_BASE}rvu17a.zip",
    (2017, "B"): f"{_PFS_MID_BASE}rvu17b.zip",
    (2017, "C"): f"{_PFS_MID_BASE}rvu17c.zip",
    (2017, "D"): f"{_PFS_MID_BASE}rvu17d.zip",
    (2018, "A"): f"{_PFS_MID_BASE}rvu18a.zip",
    (2018, "B"): f"{_PFS_MID_BASE}rvu18b.zip",
    # CY2018 Q3 ships as `rvu18c1.zip` (NOT `rvu18c.zip`) — CMS-side rename.
    (2018, "C"): f"{_PFS_MID_BASE}rvu18c1.zip",
    (2018, "D"): f"{_PFS_MID_BASE}rvu18d.zip",
    (2019, "A"): f"{_PFS_MID_BASE}rvu19a.zip",
    (2019, "B"): f"{_PFS_MID_BASE}rvu19b.zip",
    (2019, "C"): f"{_PFS_MID_BASE}rvu19c.zip",
    (2019, "D"): f"{_PFS_MID_BASE}rvu19d.zip",
    # CY2020 A-D unresolved 2026-04-30 — CMS landing page HTML emits corrupted
    # hrefs, no working URL surfaced across naming variants. Placeholders retained
    # on legacy host; bulk extractor logs+skips on 404.
    (2020, "A"): f"{_PFS_LEGACY_BASE}RVU20A.zip",
    (2020, "B"): f"{_PFS_LEGACY_BASE}RVU20B.zip",
    (2020, "C"): f"{_PFS_LEGACY_BASE}RVU20C.zip",
    (2020, "D"): f"{_PFS_LEGACY_BASE}RVU20D.zip",
    # ---- CY2021-CY2026: modern Drupal `files/zip/` slug. ----
    # CY2021 Q1 unresolved 2026-04-30 — placeholder 404s, no working URL found.
    (2021, "A"): f"{_PFS_MODERN_BASE}rvu21a.zip",
    (2021, "B"): f"{_PFS_MODERN_BASE}rvu21b.zip",
    (2021, "C"): f"{_PFS_MODERN_BASE}rvu21c.zip",
    (2021, "D"): f"{_PFS_MODERN_BASE}rvu21d.zip",
    (2022, "A"): f"{_PFS_MODERN_BASE}rvu22a.zip",
    (2022, "B"): f"{_PFS_MODERN_BASE}rvu22b.zip",
    (2022, "C"): f"{_PFS_MODERN_BASE}rvu22c.zip",
    (2022, "D"): f"{_PFS_MODERN_BASE}rvu22d.zip",
    (2023, "A"): f"{_PFS_MODERN_BASE}rvu23a.zip",
    (2023, "B"): f"{_PFS_MODERN_BASE}rvu23b.zip",
    (2023, "C"): f"{_PFS_MODERN_BASE}rvu23c.zip",
    (2023, "D"): f"{_PFS_MODERN_BASE}rvu23d.zip",
    (2024, "A"): f"{_PFS_MODERN_BASE}rvu24a.zip",
    # CY2024 Q2 PFS RVU release is unavailable on cms.gov as of 2026-04-30:
    # `rvu24b.zip` (and the literal-slash variant `rvu24b-updated-03/18/2024.zip`)
    # both return 200 but ship DMEPOS-Q2-2024 content, not PPRRVU. No working
    # PFS RVU URL surfaced across naming variants. Placeholder retained; the
    # bulk extractor logs+skips on a 404 (here it would log+skip on a parse
    # failure since the ZIP is structurally valid but lacks a PPRRVU member).
    (2024, "B"): f"{_PFS_MODERN_BASE}rvu24b.zip",
    (2024, "C"): f"{_PFS_MODERN_BASE}rvu24c.zip",
    (2024, "D"): f"{_PFS_MODERN_BASE}rvu24d.zip",
    (2025, "A"): f"{_PFS_MODERN_BASE}rvu25a.zip",
    (2025, "B"): f"{_PFS_MODERN_BASE}rvu25b.zip",
    (2025, "C"): f"{_PFS_MODERN_BASE}rvu25c.zip",
    (2025, "D"): f"{_PFS_MODERN_BASE}rvu25d.zip",
    (2026, "A"): f"{_PFS_MODERN_BASE}rvu26a.zip",
    (2026, "B"): f"{_PFS_MODERN_BASE}rvu26b.zip",
    # CY2026 Q3 / Q4 unresolved 2026-04-30 — not yet published
    # (Q3 ~ July 2026, Q4 ~ October 2026). Placeholders retained.
    (2026, "C"): f"{_PFS_MODERN_BASE}rvu26c.zip",
    (2026, "D"): f"{_PFS_MODERN_BASE}rvu26d.zip",
}


# PFS conversion factors per CY. The conversion factor is the dollar
# multiplier applied to total RVUs to compute the national-mean allowed
# amount: `allowed_amount = total_rvu * conversion_factor` (with GPCI=1.0).
#
# Shape:
#   - CY2008-CY2025: a single float (`conversion_factor_kind = "single"` in
#     silver). Mid-year statutory adjustments (e.g. CY2015's MACRA cliff)
#     are collapsed to the post-adjustment value; documented inline.
#   - CY2026: a tuple `(qp_value, nonqp_value)` capturing the QP / non-QP
#     split introduced by the CY2026 PFS Final Rule. Every CY2026 silver
#     row doubles — one with `conversion_factor_kind="qp"`, one with
#     `="nonqp"`.
#
# Values are sourced from CMS PFS Final Rule fact sheets (citation per
# entry). These are best-known approximations to anchor downstream
# allowed-amount math; a future task is to verify each value against
# CMS-published sample allowed amounts.
PFS_CONVERSION_FACTORS: dict[int, float | tuple[float, float]] = {
    2008: 38.0870,  # MIPPA freeze level
    2009: 36.0666,
    2010: 36.8729,
    2011: 33.9764,
    2012: 34.0376,
    2013: 34.0230,
    2014: 35.8228,
    2015: 35.7547,  # Apr 1+ post-MACRA; pre-MACRA H1 was 35.8013 — use post
    2016: 35.8043,
    2017: 35.8887,
    2018: 35.9996,
    2019: 36.0391,
    2020: 36.0896,
    2021: 34.8931,
    2022: 34.6062,
    2023: 33.8872,
    2024: 32.7442,
    2025: 32.3465,
    2026: (33.5675, 33.4209),  # (qp, nonqp) per CY2026 PFS Final Rule
}


def pfs_quarter_interval(cy: int, quarter: str) -> tuple[date, date]:
    """Return the effective_start/effective_end for a PFS RVU quarter.

    Quarters: A = Jan 1 – Mar 31; B = Apr 1 – Jun 30; C = Jul 1 – Sep 30;
    D = Oct 1 – Dec 31. Each PFS RVU{YY}{Q}.zip is the snapshot of allowed
    amounts effective during that quarter.
    """
    if quarter not in PFS_QUARTERS:
        raise ValueError(
            f"Unknown PFS quarter {quarter!r}; expected one of {PFS_QUARTERS}"
        )
    if quarter == "A":
        return (date(cy, 1, 1), date(cy, 3, 31))
    if quarter == "B":
        return (date(cy, 4, 1), date(cy, 6, 30))
    if quarter == "C":
        return (date(cy, 7, 1), date(cy, 9, 30))
    # quarter == "D"
    return (date(cy, 10, 1), date(cy, 12, 31))


# ---------------------------------------------------------------------------
# Session 4 / Phase 2: OPPS (Hospital Outpatient PPS) Addendum A + Addendum B
# ---------------------------------------------------------------------------
#
# OPPS publishes quarterly Addendum A (APC-keyed payment rates: APC code,
# status indicator, weight, payment rate, copays) and Addendum B (HCPCS-keyed,
# APC FK, payment rate). One ZIP per (cy, quarter, addendum_letter) cell. ZIPs
# bundle the 508 CSV plus an XLSX. CSV layout is single-row header with 1-4
# leading banner / disclaimer rows above (NOT the multi-row stacked-header
# scheme PFS PPRRVU uses) — extractor will skip-leading-banner rather than
# stitch.
#
# URL discovery (verified 2026-04-30 via per-CMS-wrapper-page crawl + curl
# probes) reveals **four** confirmed URL eras with no clean canonical pattern
# at the basename level — every cell carries an idiosyncratic filename. The
# eras differ on host slug, but within each era the basename mutates per cell
# (mixed-case, suffixed dates, "Web" inserts, Q1 vs January spelling, etc.).
# Mirroring the HCPCS / PFS precedent we hard-code every cell explicitly so
# any later URL-overrides are append-only edits to OPPS_SOURCES.
#
# **Era 1 — CY2008-CY2011 (legacy, ad-hoc filenames):**
#   Base = `cms.gov/Medicare/Medicare-Fee-for-Service-Payment/Hospitaloutpatientpps/downloads/`
#   (case is INCONSISTENT — sometimes `HospitalOutpatientPPS`, sometimes
#   `Hospitaloutpatientpps`; sometimes `Downloads/`, sometimes `downloads/`).
#   Filenames are completely unstandardized: `2008addajan.zip`,
#   `Jan_2009_Add_A.zip`, `2010October_AddA.zip`, `OctAddA09302008.zip`,
#   `2009_April_Addendum_A_040109.zip`, `Apr_2011_Add_A.zip`. We therefore
#   inline the *full URL* per cell rather than build it from a base + pattern.
#
# **Era 2 — CY2012-CY2019 (legacy, hyphen filenames):**
#   Base = same legacy host as Era 1; basename mostly settles on
#   `{CY}-{Mon}-Addendum-{A|B}{-File}.zip` (with `Mon ∈ {Jan,April,July,Oct}`)
#   but with significant per-cell drift: `2012-July-Add-A.zip` vs.
#   `2014-Jan-Addendum-A-File.zip` vs. `2018-July-Addendum-A.zip`. CY2012 Q4
#   uses a unique "Web" prefix scheme (`2012October-AddendumA.zip`); CY2013
#   and CY2014 Q4 use a "Web Addendum" scheme
#   (`January-2013-Web-Addendum-A.zip`, `october-2014-web-addendum-a-10-01-14.zip`).
#
# **Era 3 — CY2020-CY2022 partial (modern Drupal `files/zip/`):**
#   Base = `cms.gov/files/zip/`. CY2020-CY2021 are **completely unrecoverable**
#   on cms.gov as of 2026-04-30 — wrapper pages exist but contain no working
#   download links (CMS appears to have retired these). CY2022 Q1 works on
#   the modern slug; CY2022 Q2-Q4 do not. CY2023+ all on this host.
#
# **Era 4 — CY2023-CY2026 (modern Drupal `files/zip/`, idiosyncratic basenames):**
#   Base = `cms.gov/files/zip/`. Filename varies per cell:
#     - CY2023 Q1: `january-2023-opps-addendum{-b}-updated-01/20/2023.zip`
#       (NB: literal slashes `/` in the basename — handled by extractor's
#       defensive bronze-filename synthesis fallback, mirroring the PFS
#       `rvu24b-updated-03/18/2024.zip` precedent).
#     - CY2023 Q2: `april-2023-opps-addendum{-updated|-b-updated}.zip`
#     - CY2023 Q3-Q4 / CY2024: mix of `{month}-{cy}-opps-addendum{-b}.zip`,
#       `{month}-{cy}-addendum{-b}-{date}.zip`, `october-2024-web-addendum{-b}.zip`,
#       and the bare `addendum.zip` placeholder for CY2024 Q2 Addendum A
#       (which CMS's wrapper page links to but the URL itself 404s).
#     - CY2025: mostly `{month}-{cy}-opps-addendum{-b}.zip`; April's "B"
#       capitalizes (`July-2025-opps-addendum-b.zip`); October drops the
#       `-opps-` infix (`october-2025-addendum{-b}.zip`).
#     - CY2026 Q1: `january-2026-opps-addendum{-b}.zip`. Q2: switches to the
#       `cy-{cy}-{month}-opps-addendum{-b}.zip` pattern. Q3+Q4 not yet
#       published as of 2026-04-30.
#
# Coverage summary: 19 CYs × 4 quarters × 2 addenda = 152 cells. Verified live
# (HTTP 200) on 2026-04-30: 100. Permanently 404 / unrecoverable: 52 (mostly
# CY2020-CY2021 16 cells; CY2022 Q2-Q4 6 cells; CY2026 Q3-Q4 4 cells; misc.
# scattered: CY2008 Q3 Addendum A — unverified; CY2024 Q2 Addendum A — bare
# `addendum.zip` 404; CY2025 Q3-Q4 Addendum-Q etc are out of scope; etc.).
# Pre-2008 (CY2007 wrappers we surfaced) is out of project scope.
#
# Quarter encoding mirrors the OPPS calendar (same as PFS):
#   A = Q1 (Jan 1 - Mar 31, "January" release)
#   B = Q2 (Apr 1 - Jun 30, "April" release)
#   C = Q3 (Jul 1 - Sep 30, "July" release)
#   D = Q4 (Oct 1 - Dec 31, "October" release)
#
# Addendum letter is unrelated to the quarter letter:
#   "A" = Addendum A (APC-keyed payment table)
#   "B" = Addendum B (HCPCS-keyed payment table)


OPPS_CYS: list[int] = list(range(2008, 2027))

OPPS_QUARTERS: list[str] = ["A", "B", "C", "D"]

OPPS_ADDENDA: tuple[str, ...] = ("A", "B")


_OPPS_LEGACY_BASE_LOWER: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "Hospitaloutpatientpps/downloads/"
)
_OPPS_LEGACY_BASE_UPPER: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "Hospitaloutpatientpps/Downloads/"
)
# A handful of CY2008-CY2010 cells live under the all-lowercase host
# `cms.gov/medicare/...` (vs the mixed-case `cms.gov/Medicare/...` that
# dominates the era). Verified live on 2026-04-30.
_OPPS_LEGACY_BASE_ALL_LOWER: str = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment/"
    "hospitaloutpatientpps/downloads/"
)
# Era 2's CY2008-CY2009 mixed-case fallback (HospitalOutpatientPPS/Downloads).
_OPPS_LEGACY_BASE_MIXED: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "HospitalOutpatientPPS/Downloads/"
)
_OPPS_MODERN_BASE: str = "https://www.cms.gov/files/zip/"


# Comprehensive (cy, quarter, addendum) → URL manifest. 152 cells. Cells whose
# canonical URL is known to 404 retain a placeholder URL (canonical pattern,
# clearly commented) so the bulk extractor can log+skip cleanly.
OPPS_SOURCES: dict[tuple[int, str, str], str] = {
    # ---- CY2008: legacy ad-hoc filenames (mostly verified). ----
    (2008, "A", "A"): f"{_OPPS_LEGACY_BASE_ALL_LOWER}2008addajan.zip",
    (2008, "A", "B"): f"{_OPPS_LEGACY_BASE_ALL_LOWER}2008addbjan.zip",
    (2008, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2008April_AddA.zip",
    (2008, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2008April_AddB.zip",
    (2008, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2008July_AddA.zip",
    (2008, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2008July_AddB.zip",
    # CY2008 Q4: filenames are CamelCase, no period in the date. Both verified.
    (2008, "D", "A"): f"{_OPPS_LEGACY_BASE_MIXED}OctAddA09302008.zip",
    (2008, "D", "B"): f"{_OPPS_LEGACY_BASE_MIXED}OctAddB09302008.zip",
    # ---- CY2009: legacy ad-hoc, all verified. ----
    (2009, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2009_Add_A.zip",
    (2009, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2009_Add_B.zip",
    (2009, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2009_April_Addendum_A_040109.zip",
    (2009, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2009_April_Addendum_B_040109.zip",
    # CY2009 Q3: "WebOPPS" prefix, "OCEAdd" infix — single-cell anomaly.
    (2009, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}WebOPPS_2009JulyOCEAdd_A.zip",
    (2009, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}WebOPPS_2009JulyOCEAdd_B.zip",
    (2009, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2009_Oct_Addendum_A_092209.zip",
    (2009, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2009_Oct_Addendum_B_092209.zip",
    # ---- CY2010: legacy ad-hoc. Q2 uses CMS's "Revised...05-17-10" rebake. ----
    (2010, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2010_Add_A.zip",
    (2010, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2010_Add_B.zip",
    (2010, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Revised2010WebApril_AddA_05-17-10.zip",
    (2010, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Revised2010WebApril_AddB_05-17-10.zip",
    (2010, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Jul_2010_Add_A.zip",
    (2010, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Jul_2010_Add_B.zip",
    (2010, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2010October_AddA.zip",
    (2010, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2010October_AddB.zip",
    # ---- CY2011: legacy ad-hoc, all verified. ----
    (2011, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2011_Add_A.zip",
    (2011, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2011_Add_B.zip",
    (2011, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Apr_2011_Add_A.zip",
    (2011, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Apr_2011_Add_B.zip",
    (2011, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}July_2011_Add_A.zip",
    (2011, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}July_2011_Add_B.zip",
    (2011, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Oct_2011_Add_A.zip",
    (2011, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Oct_2011_Add_B.zip",
    # ---- CY2012: transition to hyphenated naming. ----
    (2012, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2012_Add_A.zip",
    (2012, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}Jan_2012_Add_B.zip",
    (2012, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}April-2012-Add-A.zip",
    (2012, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}April-2012-Add-B.zip",
    (2012, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2012-July-Add-A.zip",
    (2012, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2012-July-Add-B.zip",
    # CY2012 Q4: anomalous "AddendumA" / "AddendumB" suffix style, no hyphen.
    (2012, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2012October-AddendumA.zip",
    (2012, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2012October-AddendumB.zip",
    # ---- CY2013: "Web Addendum" naming. Q2 carries date suffix `-040113`. ----
    (2013, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}January-2013-Web-Addendum-A.zip",
    (2013, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}January-2013-Web-Addendum-B.zip",
    (2013, "B", "A"): f"{_OPPS_LEGACY_BASE_MIXED}April-2013-Web-Addendum-A-040113.zip",
    (2013, "B", "B"): f"{_OPPS_LEGACY_BASE_MIXED}April-2013-Web-Addendum-B-040113.zip",
    (2013, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2013-July-Addendum-A-File.zip",
    (2013, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2013-July-Addendum-B-File.zip",
    (2013, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}October-2013-Web-Addendum-A.zip",
    (2013, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}October-2013-Web-Addendum-B.zip",
    # ---- CY2014: hyphenated. Q4 uses lowercase "Web Addendum" + date. ----
    (2014, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2014-Jan-Addendum-A-File.zip",
    (2014, "A", "B"): f"{_OPPS_LEGACY_BASE_MIXED}2014-Jan-Addendum-B-File.zip",
    (2014, "B", "A"): f"{_OPPS_LEGACY_BASE_MIXED}2014-April-Addendum-A-File.zip",
    (2014, "B", "B"): f"{_OPPS_LEGACY_BASE_MIXED}2014-April-Addendum-B-File.zip",
    (2014, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2014-July-Addendum-A-File.zip",
    (2014, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2014-July-Addendum-B-File.zip",
    # CY2014 Q4: only-lowercase host AND lowercase basename with date suffix.
    (2014, "D", "A"): f"{_OPPS_LEGACY_BASE_ALL_LOWER}october-2014-web-addendum-a-10-01-14.zip",
    (2014, "D", "B"): f"{_OPPS_LEGACY_BASE_ALL_LOWER}october-2014-web-addendum-b.zip",
    # ---- CY2015: hyphenated, "-File" suffix throughout. ----
    (2015, "A", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2015-Jan-Addendum-A-File.zip",
    (2015, "A", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2015-Jan-Addendum-B-File.zip",
    (2015, "B", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2015-April-Addendum-A-File.zip",
    (2015, "B", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2015-April-Addendum-B-File.zip",
    (2015, "C", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2015-July-Addendum-A-File.zip",
    (2015, "C", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2015-July-Addendum-B-File.zip",
    (2015, "D", "A"): f"{_OPPS_LEGACY_BASE_LOWER}2015-Oct-Addendum-A-File.zip",
    (2015, "D", "B"): f"{_OPPS_LEGACY_BASE_LOWER}2015-Oct-Addendum-B-File.zip",
    # ---- CY2016: hyphenated. Q1+Q2 keep "-File"; Q3+Q4 drop it. ----
    (2016, "A", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2016-Jan-Addendum-A-File.zip",
    (2016, "A", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2016-Jan-Addendum-B-File.zip",
    (2016, "B", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2016-April-Addendum-A-File.zip",
    (2016, "B", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2016-April-Addendum-B-File.zip",
    (2016, "C", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2016-July-Addendum-A.zip",
    (2016, "C", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2016-July-Addendum-B.zip",
    (2016, "D", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2016-Oct-Addendum-A.zip",
    (2016, "D", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2016-Oct-Addendum-B.zip",
    # ---- CY2017: full month spelling, no `-File` suffix. ----
    (2017, "A", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2017-January-Addendum-A.zip",
    (2017, "A", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2017-January-Addendum-B.zip",
    (2017, "B", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2017-April-Addendum-A.zip",
    (2017, "B", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2017-April-Addendum-B.zip",
    (2017, "C", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2017-July-Addendum-A.zip",
    (2017, "C", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2017-July-Addendum-B.zip",
    (2017, "D", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2017-October-Addendum-A.zip",
    (2017, "D", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2017-October-Addendum-B.zip",
    # ---- CY2018: full month spelling. Q4 reverts to "Oct" abbreviation. ----
    (2018, "A", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2018-January-Addendum-A.zip",
    (2018, "A", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2018-January-Addendum-B.zip",
    (2018, "B", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2018-April-Addendum-A.zip",
    (2018, "B", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2018-April-Addendum-B.zip",
    (2018, "C", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2018-July-Addendum-A.zip",
    (2018, "C", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2018-July-Addendum-B.zip",
    (2018, "D", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2018-Oct-Addendum-A.zip",
    (2018, "D", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2018-Oct-Addendum-B.zip",
    # ---- CY2019: full month spelling. Q4 reverts to "Oct"; corrected
    # variants exist (`-Correction.zip`) but we use the canonical first-issue. ----
    (2019, "A", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2019-January-Addendum-A.zip",
    (2019, "A", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2019-January-Addendum-B.zip",
    (2019, "B", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2019-April-Addendum-A.zip",
    (2019, "B", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2019-April-Addendum-B.zip",
    (2019, "C", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2019-July-Addendum-A.zip",
    (2019, "C", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2019-July-Addendum-B.zip",
    (2019, "D", "A"): f"{_OPPS_LEGACY_BASE_UPPER}2019-Oct-Addendum-A.zip",
    (2019, "D", "B"): f"{_OPPS_LEGACY_BASE_UPPER}2019-Oct-Addendum-B.zip",
    # ---- CY2020-CY2021: UNRECOVERABLE on cms.gov as of 2026-04-30. ----
    # Wrapper sub-pages exist (mashed-path scheme) but contain no working
    # download links — CMS appears to have actively retired these. Canonical
    # placeholders retained on the modern slug; bulk extractor will log+skip
    # 404. Mirror the PFS CY2020 A-D and HCPCS CY2008-CY2016 posture.
    (2020, "A", "A"): f"{_OPPS_MODERN_BASE}january-2020-opps-addendum.zip",
    (2020, "A", "B"): f"{_OPPS_MODERN_BASE}january-2020-opps-addendum-b.zip",
    (2020, "B", "A"): f"{_OPPS_MODERN_BASE}april-2020-opps-addendum.zip",
    (2020, "B", "B"): f"{_OPPS_MODERN_BASE}april-2020-opps-addendum-b.zip",
    (2020, "C", "A"): f"{_OPPS_MODERN_BASE}july-2020-opps-addendum.zip",
    (2020, "C", "B"): f"{_OPPS_MODERN_BASE}july-2020-opps-addendum-b.zip",
    (2020, "D", "A"): f"{_OPPS_MODERN_BASE}october-2020-opps-addendum.zip",
    (2020, "D", "B"): f"{_OPPS_MODERN_BASE}october-2020-opps-addendum-b.zip",
    (2021, "A", "A"): f"{_OPPS_MODERN_BASE}january-2021-opps-addendum.zip",
    (2021, "A", "B"): f"{_OPPS_MODERN_BASE}january-2021-opps-addendum-b.zip",
    (2021, "B", "A"): f"{_OPPS_MODERN_BASE}april-2021-opps-addendum.zip",
    (2021, "B", "B"): f"{_OPPS_MODERN_BASE}april-2021-opps-addendum-b.zip",
    (2021, "C", "A"): f"{_OPPS_MODERN_BASE}july-2021-opps-addendum.zip",
    (2021, "C", "B"): f"{_OPPS_MODERN_BASE}july-2021-opps-addendum-b.zip",
    (2021, "D", "A"): f"{_OPPS_MODERN_BASE}october-2021-opps-addendum.zip",
    (2021, "D", "B"): f"{_OPPS_MODERN_BASE}october-2021-opps-addendum-b.zip",
    # ---- CY2022: only Q1 verified live. Q2-Q4 placeholders 404. ----
    (2022, "A", "A"): f"{_OPPS_MODERN_BASE}january-2022-opps-addendum.zip",
    (2022, "A", "B"): f"{_OPPS_MODERN_BASE}january-2022-opps-addendum-b.zip",
    (2022, "B", "A"): f"{_OPPS_MODERN_BASE}april-2022-opps-addendum.zip",
    (2022, "B", "B"): f"{_OPPS_MODERN_BASE}april-2022-opps-addendum-b.zip",
    (2022, "C", "A"): f"{_OPPS_MODERN_BASE}july-2022-opps-addendum.zip",
    (2022, "C", "B"): f"{_OPPS_MODERN_BASE}july-2022-opps-addendum-b.zip",
    (2022, "D", "A"): f"{_OPPS_MODERN_BASE}october-2022-opps-addendum.zip",
    (2022, "D", "B"): f"{_OPPS_MODERN_BASE}october-2022-opps-addendum-b.zip",
    # ---- CY2023: modern slug. Q1+Q2 carry "-updated" / date suffixes. ----
    # CY2023 Q1: literal-slash dates `01/20/2023` in the basename — the
    # extractor's defensive bronze-filename synthesis fallback (mirroring the
    # PFS rvu24b case) handles these.
    (2023, "A", "A"): f"{_OPPS_MODERN_BASE}january-2023-opps-addendum-updated-01/20/2023.zip",
    (2023, "A", "B"): f"{_OPPS_MODERN_BASE}january-2023-opps-addendum-b-updated-01/20/2023.zip",
    (2023, "B", "A"): f"{_OPPS_MODERN_BASE}april-2023-opps-addendum-updated.zip",
    (2023, "B", "B"): f"{_OPPS_MODERN_BASE}april-2023-opps-addendum-b-updated.zip",
    (2023, "C", "A"): f"{_OPPS_MODERN_BASE}july-2023-opps-addendum.zip",
    (2023, "C", "B"): f"{_OPPS_MODERN_BASE}july-2023-opps-addendum-b.zip",
    (2023, "D", "A"): f"{_OPPS_MODERN_BASE}october-2023-opps-addendum.zip",
    (2023, "D", "B"): f"{_OPPS_MODERN_BASE}october-2023-opps-addendum-b.zip",
    # ---- CY2024: modern slug. Naming drifts every quarter. ----
    (2024, "A", "A"): f"{_OPPS_MODERN_BASE}january-2024-opps-addendum.zip",
    # CY2024 Q1 Addendum B: trailing dash before `.zip` is intentional —
    # CMS-side basename quirk (verified 200 on 2026-04-30).
    (2024, "A", "B"): f"{_OPPS_MODERN_BASE}january-2024-opps-addendum-b-.zip",
    # CY2024 Q2 Addendum A: CMS wrapper page links to the literal generic
    # `addendum.zip` slug, which itself 404s on cms.gov. The cell is
    # unrecoverable; placeholder retained to keep the dict shape consistent.
    (2024, "B", "A"): f"{_OPPS_MODERN_BASE}addendum.zip",
    (2024, "B", "B"): f"{_OPPS_MODERN_BASE}april-2024-addendum-b.zip",
    # CY2024 Q3: dropped `-opps-` infix; basename carries the `-080524` date
    # (release date of CMS rebake).
    (2024, "C", "A"): f"{_OPPS_MODERN_BASE}july-2024-addendum-080524.zip",
    (2024, "C", "B"): f"{_OPPS_MODERN_BASE}july-2024-addendum-b-080524.zip",
    # CY2024 Q4: "web addendum" infix, no `-opps-`.
    (2024, "D", "A"): f"{_OPPS_MODERN_BASE}october-2024-web-addendum.zip",
    (2024, "D", "B"): f"{_OPPS_MODERN_BASE}october-2024-web-addendum-b.zip",
    # ---- CY2025: modern slug. Q1+Q2+Q3 use `-opps-`; Q4 drops it. ----
    (2025, "A", "A"): f"{_OPPS_MODERN_BASE}january-2025-opps-addendum.zip",
    (2025, "A", "B"): f"{_OPPS_MODERN_BASE}january-2025-opps-addendum-b.zip",
    (2025, "B", "A"): f"{_OPPS_MODERN_BASE}april-2025-opps-addendum.zip",
    (2025, "B", "B"): f"{_OPPS_MODERN_BASE}april-2025-opps-addendum-b.zip",
    (2025, "C", "A"): f"{_OPPS_MODERN_BASE}july-2025-opps-addendum.zip",
    # CY2025 Q3 Addendum B: CMS uppercased "July" in the basename — single-
    # cell case mutation. Verified 200 on 2026-04-30.
    (2025, "C", "B"): f"{_OPPS_MODERN_BASE}July-2025-opps-addendum-b.zip",
    (2025, "D", "A"): f"{_OPPS_MODERN_BASE}october-2025-addendum.zip",
    (2025, "D", "B"): f"{_OPPS_MODERN_BASE}october-2025-addendum-b.zip",
    # ---- CY2026: Q1 modern canonical; Q2 switches to `cy-{cy}-...` prefix. ----
    (2026, "A", "A"): f"{_OPPS_MODERN_BASE}january-2026-opps-addendum.zip",
    (2026, "A", "B"): f"{_OPPS_MODERN_BASE}january-2026-opps-addendum-b.zip",
    (2026, "B", "A"): f"{_OPPS_MODERN_BASE}cy-2026-april-opps-addendum.zip",
    (2026, "B", "B"): f"{_OPPS_MODERN_BASE}cy-2026-april-opps-addendum-b.zip",
    # CY2026 Q3+Q4 not yet published as of 2026-04-30 (Q3 ~ July 2026,
    # Q4 ~ October 2026). Placeholders retained on the canonical Q1 slug
    # pattern — refresh when CMS publishes.
    (2026, "C", "A"): f"{_OPPS_MODERN_BASE}july-2026-opps-addendum.zip",
    (2026, "C", "B"): f"{_OPPS_MODERN_BASE}july-2026-opps-addendum-b.zip",
    (2026, "D", "A"): f"{_OPPS_MODERN_BASE}october-2026-opps-addendum.zip",
    (2026, "D", "B"): f"{_OPPS_MODERN_BASE}october-2026-opps-addendum-b.zip",
}


def opps_quarter_interval(cy: int, quarter: str) -> tuple[date, date]:
    """Return effective_start / effective_end for an OPPS quarter.

    OPPS quarters align with PFS quarters (A=Q1, B=Q2, C=Q3, D=Q4) on the
    federal calendar year. Delegates to :func:`pfs_quarter_interval` rather
    than duplicating the date math.
    """
    return pfs_quarter_interval(cy, quarter)


# ---------------------------------------------------------------------------
# Session 4b / Phase 2: ASC (Ambulatory Surgical Center) Addenda
# ---------------------------------------------------------------------------
#
# ASC publishes **quarterly** addenda updates (NOT annually as the upstream
# plan-v5 prompt assumed). Each release ships ONE ZIP per (cy, quarter) cell
# containing all six addenda as separate internal files (`.txt` / `.csv`):
#
#   AA  -- ASC Covered Surgical Procedures + Payment Rates (HCPCS-keyed; PI,
#          payment weight, payment rate). Largest table (~6-8k rows).
#   BB  -- ASC Covered Ancillary Services + Payment Rates (HCPCS-keyed; PI,
#          comment indicator, drug pass-through expiration, weight, rate).
#   DD1 -- Payment Indicator Definitions (lookup; ~10-30 rows).
#   DD2 -- Comment / Status Indicator Definitions (lookup; ~10-30 rows).
#   EE  -- Final ASC Excluded Procedures (HCPCS-only; ~50-200 rows).
#   FF  -- ASC Drugs/Devices Pass-Through Pricing (HCPCS-keyed; subset of BB).
#
# Because all six addenda live in a SINGLE ZIP per (cy, quarter), the URL
# manifest is keyed by `(cy, quarter, addendum)` with the same URL repeated
# six times per (cy, quarter) — mirrors the OPPS_SOURCES shape and lets the
# extractor demux by member basename. The addendum letter appears in the
# member basename (modern era CSVs: `... - Addendum AA....csv`; older `.txt`:
# `January 2024 ASC AA.txt`; CY2021-CY2022 `.txt`: `508-compliant-version-of-
# CY 2021 ASC AA.txt`). Extractor predicate: case-insensitive match on the
# addendum tag (`AA`/`BB`/`DD1`/`DD2`/`EE`/`FF`) as a token boundary inside
# the member basename.
#
# Quarter encoding mirrors OPPS / PFS:
#   A = Q1 (Jan 1 - Mar 31, "January" release)
#   B = Q2 (Apr 1 - Jun 30, "April" release)
#   C = Q3 (Jul 1 - Sep 30, "July" release)
#   D = Q4 (Oct 1 - Dec 31, "October" release)
#
# URL discovery (verified 2026-04-30 via curl probes against the canonical
# ASC Payment Rates Addenda page) reveals **two** confirmed hosts:
#
# **Era 1 — CY2017 Q4 + CY2018 + CY2019 (legacy mixed-case host):**
#   Base = `cms.gov/Medicare/Medicare-Fee-for-Service-Payment/ASCPayment/Downloads/`
#   Two basename schemes coexist:
#     (a) `{CY}-{Mon}-ASC-Addenda.zip` (e.g., `2018-Jan-ASC-Addenda.zip`,
#         `2019-Oct-ASC-Addenda.zip`) — used for most cells in this era.
#     (b) `{Month}-{CY}-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip` (e.g.,
#         `April-2018-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip`,
#         `October-2013-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip`) —
#         used for CY2013 Q4 (only surviving pre-2017 cell) and CY2018 Q2.
#   Lowercase-host fallback `.../ascpayment/downloads/` also works for many
#   cells (CMS serves both case-folded variants).
#
# **Era 2 — CY2020-CY2026 (modern Drupal `files/zip/` slug):**
#   Base = `cms.gov/files/zip/`. Filenames drift per quarter:
#     - CY2020 Q1: `January-2020-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip`
#       (mixed case, capital "January").
#     - CY2020 Q2-Q4: `{month}-2020-asc-approved-hcpcs-code-and-payment-rates`
#       all lowercase. Q2 carries `-updated-04092020` suffix.
#     - CY2021-CY2022: BUNDLED short form `{CY}-{Month}-ASC-Addenda.zip`
#       (e.g., `2021-January-ASC-Addenda.zip`, `2022-january-asc-addenda.zip`,
#       `2021-Oct-ASC-Addenda.zip` — Oct uses 3-letter abbreviation; July
#       lowercased in CY2022 Q3 only). CY2022 Q3 onward: filename pattern
#       reverts to `{month}-{cy}-asc-approved-hcpcs-code-and-payment-rates`.
#     - CY2023: `{month}-2023-asc-approved-hcpcs-code-and-payment-rates.zip`.
#     - CY2024: same; Q1 carries `-updated-02/15/2024` literal-slash suffix
#       (mirrors the OPPS CY2023 Q1 + PFS rvu24b literal-slash pattern); Q4
#       carries `-updated-09/24/2024`. Plus a one-off `march-9-2024-...zip`
#       between Q1 and Q2 — out of scope (interim correction, not a quarter
#       boundary release).
#     - CY2025: Q1 and Q2 use the long pattern; Q2 carries `-updated-04/07/
#       2025` literal-slash suffix; Q3 collapses to `july-2025-asc-addenda.
#       zip` (BUNDLED short form, similar to CY2021-CY2022 era); Q4 returns
#       to long pattern but drops `-and-` connector
#       (`october-2025-asc-approved-hcpcs-code-payment-rates.zip-0` — Drupal
#       uniqueness suffix; redirects to canonical).
#     - CY2026: Q1 + Q2 use the `{month}-2026-asc-approved-hcpcs-code-
#       payment-rates.zip` pattern (drops `-and-` connector permanently). Q1
#       URL has `-0` Drupal suffix that 301s to a real
#       `-updated-02-11-2026.zip` filename. Q3 (Jul 2026) and Q4 (Oct 2026)
#       not yet published as of 2026-04-30.
#
# **Coverage summary:** 19 CYs × 4 quarters × 6 addenda = 456 URL entries.
# Verified live (HTTP 200 or 301→200) on 2026-04-30: 28 (cy, quarter) cells
# × 6 = 168 entries. Permanently 404 / unrecoverable on cms.gov: 48 cells
# × 6 = 288 entries (CY2008-CY2012 = 20 cells; CY2013 Q1-Q3 = 3 cells;
# CY2014-CY2016 = 12 cells; CY2017 Q1-Q3 = 3 cells; CY2026 Q3-Q4 = 2 cells —
# Q3/Q4 are future-carryover, not yet published; plus CY2024 March-9 special
# revision is out of scope). Pre-2017 cells follow the same posture as HCPCS
# pre-CY2017 + PFS pre-CY2017 + OPPS CY2020-CY2021: log+skip in the bulk
# extractor with canonical-pattern placeholders retained.
#
# **AMA license redirect:** the canonical addenda landing page wraps every
# ZIP link in `/apps/ama/license.asp?file=...` (forces a CPT-license click-
# through for human visitors), but the underlying `/files/zip/` and legacy-
# host URLs serve directly via curl with no license challenge. ASC config
# stores the direct URLs.

ASC_CYS: list[int] = list(range(2008, 2027))

ASC_QUARTERS: list[str] = ["A", "B", "C", "D"]

ASC_ADDENDA: tuple[str, ...] = ("AA", "BB", "DD1", "DD2", "EE", "FF")


_ASC_LEGACY_BASE: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "ASCPayment/Downloads/"
)
# Lowercase-`downloads/` variant of the legacy host. Both case-folded variants
# resolve to the same content for most cells; we prefer the canonical mixed-
# case form unless a specific cell only resolves on the lowercase variant.
_ASC_LEGACY_BASE_LOWER: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "ASCPayment/downloads/"
)
_ASC_MODERN_BASE: str = "https://www.cms.gov/files/zip/"


# Per-(cy, quarter) URL — single ZIP bundles all 6 addenda. The full
# `(cy, quarter, addendum)` manifest is built from this by exploding each
# entry six times (one row per addendum letter, same URL).
_ASC_ZIP_BY_CY_QUARTER: dict[tuple[int, str], str] = {
    # ---- CY2008-CY2012: legacy host, all 404 on every probed pattern. ----
    # Canonical-pattern placeholders retained on the legacy host so the bulk
    # extractor logs+skips cleanly. Same posture as HCPCS pre-CY2017.
    (2008, "A"): f"{_ASC_LEGACY_BASE}2008-Jan-ASC-Addenda.zip",
    (2008, "B"): f"{_ASC_LEGACY_BASE}2008-Apr-ASC-Addenda.zip",
    (2008, "C"): f"{_ASC_LEGACY_BASE}2008-Jul-ASC-Addenda.zip",
    (2008, "D"): f"{_ASC_LEGACY_BASE}2008-Oct-ASC-Addenda.zip",
    (2009, "A"): f"{_ASC_LEGACY_BASE}2009-Jan-ASC-Addenda.zip",
    (2009, "B"): f"{_ASC_LEGACY_BASE}2009-Apr-ASC-Addenda.zip",
    (2009, "C"): f"{_ASC_LEGACY_BASE}2009-Jul-ASC-Addenda.zip",
    (2009, "D"): f"{_ASC_LEGACY_BASE}2009-Oct-ASC-Addenda.zip",
    (2010, "A"): f"{_ASC_LEGACY_BASE}2010-Jan-ASC-Addenda.zip",
    (2010, "B"): f"{_ASC_LEGACY_BASE}2010-Apr-ASC-Addenda.zip",
    (2010, "C"): f"{_ASC_LEGACY_BASE}2010-Jul-ASC-Addenda.zip",
    (2010, "D"): f"{_ASC_LEGACY_BASE}2010-Oct-ASC-Addenda.zip",
    (2011, "A"): f"{_ASC_LEGACY_BASE}2011-Jan-ASC-Addenda.zip",
    (2011, "B"): f"{_ASC_LEGACY_BASE}2011-Apr-ASC-Addenda.zip",
    (2011, "C"): f"{_ASC_LEGACY_BASE}2011-Jul-ASC-Addenda.zip",
    (2011, "D"): f"{_ASC_LEGACY_BASE}2011-Oct-ASC-Addenda.zip",
    (2012, "A"): f"{_ASC_LEGACY_BASE}2012-Jan-ASC-Addenda.zip",
    (2012, "B"): f"{_ASC_LEGACY_BASE}2012-Apr-ASC-Addenda.zip",
    (2012, "C"): f"{_ASC_LEGACY_BASE}2012-Jul-ASC-Addenda.zip",
    (2012, "D"): f"{_ASC_LEGACY_BASE}2012-Oct-ASC-Addenda.zip",
    # ---- CY2013: Q4 only verified live on legacy host (Approved-HCPCS pattern).
    (2013, "A"): f"{_ASC_LEGACY_BASE}2013-Jan-ASC-Addenda.zip",
    (2013, "B"): f"{_ASC_LEGACY_BASE}2013-Apr-ASC-Addenda.zip",
    (2013, "C"): f"{_ASC_LEGACY_BASE}2013-Jul-ASC-Addenda.zip",
    (2013, "D"): f"{_ASC_LEGACY_BASE_LOWER}October-2013-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip",
    # ---- CY2014-CY2016: legacy host, all 404. Placeholders retained. ----
    (2014, "A"): f"{_ASC_LEGACY_BASE}2014-Jan-ASC-Addenda.zip",
    (2014, "B"): f"{_ASC_LEGACY_BASE}2014-Apr-ASC-Addenda.zip",
    (2014, "C"): f"{_ASC_LEGACY_BASE}2014-Jul-ASC-Addenda.zip",
    (2014, "D"): f"{_ASC_LEGACY_BASE}2014-Oct-ASC-Addenda.zip",
    (2015, "A"): f"{_ASC_LEGACY_BASE}2015-Jan-ASC-Addenda.zip",
    (2015, "B"): f"{_ASC_LEGACY_BASE}2015-Apr-ASC-Addenda.zip",
    (2015, "C"): f"{_ASC_LEGACY_BASE}2015-Jul-ASC-Addenda.zip",
    (2015, "D"): f"{_ASC_LEGACY_BASE}2015-Oct-ASC-Addenda.zip",
    (2016, "A"): f"{_ASC_LEGACY_BASE}2016-Jan-ASC-Addenda.zip",
    (2016, "B"): f"{_ASC_LEGACY_BASE}2016-Apr-ASC-Addenda.zip",
    (2016, "C"): f"{_ASC_LEGACY_BASE}2016-Jul-ASC-Addenda.zip",
    (2016, "D"): f"{_ASC_LEGACY_BASE}2016-Oct-ASC-Addenda.zip",
    # ---- CY2017: only Q4 verified live (short-form). Q1-Q3 placeholders. ----
    (2017, "A"): f"{_ASC_LEGACY_BASE}2017-Jan-ASC-Addenda.zip",
    (2017, "B"): f"{_ASC_LEGACY_BASE}2017-Apr-ASC-Addenda.zip",
    (2017, "C"): f"{_ASC_LEGACY_BASE}2017-Jul-ASC-Addenda.zip",
    (2017, "D"): f"{_ASC_LEGACY_BASE}2017-Oct-ASC-Addenda.zip",
    # ---- CY2018: short-form for Q1/Q3/Q4; long "Approved-HCPCS" form for Q2.
    (2018, "A"): f"{_ASC_LEGACY_BASE}2018-Jan-ASC-Addenda.zip",
    (2018, "B"): f"{_ASC_LEGACY_BASE}April-2018-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip",
    (2018, "C"): f"{_ASC_LEGACY_BASE}2018-Jul-ASC-Addenda.zip",
    (2018, "D"): f"{_ASC_LEGACY_BASE}2018-Oct-ASC-Addenda.zip",
    # ---- CY2019: short-form throughout. All four quarters verified live. ----
    (2019, "A"): f"{_ASC_LEGACY_BASE}2019-Jan-ASC-Addenda.zip",
    (2019, "B"): f"{_ASC_LEGACY_BASE}2019-Apr-ASC-Addenda.zip",
    (2019, "C"): f"{_ASC_LEGACY_BASE}2019-Jul-ASC-Addenda.zip",
    (2019, "D"): f"{_ASC_LEGACY_BASE}2019-Oct-ASC-Addenda.zip",
    # ---- CY2020: modern host. Q1 mixed case "January"; Q2-Q4 lowercase.
    # Q2 carries `-updated-04092020` suffix (revision in place).
    (2020, "A"): f"{_ASC_MODERN_BASE}January-2020-ASC-Approved-HCPCS-Code-and-Payment-Rates.zip",
    (2020, "B"): f"{_ASC_MODERN_BASE}april-2020-asc-approved-hcpcs-code-and-payment-rates-updated-04092020.zip",
    (2020, "C"): f"{_ASC_MODERN_BASE}july-2020-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2020, "D"): f"{_ASC_MODERN_BASE}october-2020-asc-approved-hcpcs-code-and-payment-rates.zip",
    # ---- CY2021: BUNDLED short-form `{CY}-{Mon}-ASC-Addenda.zip` (Mon mixes
    # full names "January"/"April"/"July" with abbreviated "Oct"). All verified.
    (2021, "A"): f"{_ASC_MODERN_BASE}2021-January-ASC-Addenda.zip",
    (2021, "B"): f"{_ASC_MODERN_BASE}2021-April-ASC-Addenda.zip",
    (2021, "C"): f"{_ASC_MODERN_BASE}2021-July-ASC-Addenda.zip",
    (2021, "D"): f"{_ASC_MODERN_BASE}2021-Oct-ASC-Addenda.zip",
    # ---- CY2022: BUNDLED short-form for Q1/Q2 (Q3 lowercased "july"); Q4
    # switches back to the long "approved-hcpcs" pattern with `-updated-...`.
    (2022, "A"): f"{_ASC_MODERN_BASE}2022-January-ASC-Addenda.zip",
    (2022, "B"): f"{_ASC_MODERN_BASE}2022-April-ASC-Addenda.zip",
    (2022, "C"): f"{_ASC_MODERN_BASE}2022-july-asc-addenda.zip",
    (2022, "D"): f"{_ASC_MODERN_BASE}october-2022-asc-approved-hcpcs-code-and-payment-rates-updated-10182022.zip",
    # ---- CY2023: long `{month}-{cy}-asc-approved-hcpcs-...` throughout. ----
    (2023, "A"): f"{_ASC_MODERN_BASE}january-2023-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2023, "B"): f"{_ASC_MODERN_BASE}april-2023-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2023, "C"): f"{_ASC_MODERN_BASE}july-2023-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2023, "D"): f"{_ASC_MODERN_BASE}october-2023-asc-approved-hcpcs-code-and-payment-rates.zip",
    # ---- CY2024: long pattern. Q1 + Q4 carry literal-slash dated suffixes
    # (the `01/20/2024` / `09/24/2024` patterns mirror PFS rvu24b + OPPS
    # CY2023 Q1). Bronze-filename synthesis fallback handles them.
    (2024, "A"): f"{_ASC_MODERN_BASE}january-2024-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2024, "B"): f"{_ASC_MODERN_BASE}april-2024-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2024, "C"): f"{_ASC_MODERN_BASE}july-2024-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2024, "D"): f"{_ASC_MODERN_BASE}october-2024-asc-approved-hcpcs-code-and-payment-rates-updated-09/24/2024.zip",
    # ---- CY2025: Q2 has literal-slash date `04/07/2025`; Q3 collapses to
    # the bundled short form `july-2025-asc-addenda.zip` (CMS reused the
    # CY2021-CY2022 short-form pattern for this single cell); Q4 drops
    # `-and-` connector and adds Drupal `-0` uniqueness suffix.
    (2025, "A"): f"{_ASC_MODERN_BASE}january-2025-asc-approved-hcpcs-code-and-payment-rates.zip",
    (2025, "B"): f"{_ASC_MODERN_BASE}april-2025-asc-approved-hcpcs-code-and-payment-rates-updated-04/07/2025.zip",
    (2025, "C"): f"{_ASC_MODERN_BASE}july-2025-asc-addenda.zip",
    (2025, "D"): f"{_ASC_MODERN_BASE}october-2025-asc-approved-hcpcs-code-payment-rates.zip-0",
    # ---- CY2026: Q1 has Drupal `-0` suffix that 301s to
    # `-updated-02-11-2026.zip`; Q2 uses canonical short. Q3+Q4 not yet
    # published as of 2026-04-30 (release windows: Q3 ~ July 2026; Q4 ~
    # October 2026). Placeholders retained on the canonical Q1/Q2 pattern.
    (2026, "A"): f"{_ASC_MODERN_BASE}january-2026-asc-approved-hcpcs-code-payment-rates.zip-0",
    (2026, "B"): f"{_ASC_MODERN_BASE}april-2026-asc-approved-hcpcs-code-payment-rates.zip",
    (2026, "C"): f"{_ASC_MODERN_BASE}july-2026-asc-approved-hcpcs-code-payment-rates.zip",
    (2026, "D"): f"{_ASC_MODERN_BASE}october-2026-asc-approved-hcpcs-code-payment-rates.zip",
}


# Comprehensive (cy, quarter, addendum) → URL manifest. 19 × 4 × 6 = 456
# entries. Each (cy, quarter) cell maps to ONE underlying ZIP; the six
# addendum keys for that cell all point to the same URL. The extractor
# downloads the ZIP once (idempotent bronze) and demuxes by member basename.
ASC_SOURCES: dict[tuple[int, str, str], str] = {
    (cy, q, addendum): url
    for (cy, q), url in _ASC_ZIP_BY_CY_QUARTER.items()
    for addendum in ASC_ADDENDA
}


def asc_quarter_interval(cy: int, quarter: str) -> tuple[date, date]:
    """Return effective_start / effective_end for an ASC quarter.

    ASC quarters align with OPPS / PFS quarters (A=Q1, B=Q2, C=Q3, D=Q4) on
    the federal calendar year. Delegates to :func:`pfs_quarter_interval`.
    """
    return pfs_quarter_interval(cy, quarter)


# ---------------------------------------------------------------------------
# Session 5 / Phase 3: IPPS Final Rule (Table 5 + Tables 1A-1E)
# ---------------------------------------------------------------------------
#
# IPPS Final Rule shipping calendar: each federal fiscal year (Oct 1 - Sep 30)
# CMS publishes ONE Final Rule with rate-table artifacts. Phase 3 captures two
# of those artifacts at FY-granularity, FY2008-FY2026:
#
#   Table 5  -- one row per (MS-DRG, FY) carrying relative weight, GMLOS, AMLOS,
#               MDC, type, severity. Single ZIP per FY containing one main XLSX
#               (the .txt sibling is the same data tab-delimited). ~755 DRGs/FY.
#   Tables   -- "Tables 1A through 1E": operating + capital standardized amounts,
#   1A-1E      labor/nonlabor shares, capital federal rate, outlier thresholds,
#               wage-index anchors, COLA factors. Long-form: each scalar value
#               becomes one silver row keyed by (fy, version, table, line_item,
#               category, rate_period). Single ZIP per FY containing 5 sub-tables
#               (XLSX or .txt members named `Table 1A...`, `Table 1B...`, etc.).
#
# Phase 4 (Java grouper) will additionally consume V*.1 mid-year refresh
# artifacts (Definitions Manual + MCE + grouper jars) which ship under
# `cms.gov/medicare/payment/.../ms-drg-classifications-and-software`. Phase 3
# does NOT capture V*.1 because CMS practice is to refresh ONLY the grouper /
# code-edits in `.1` mid-year releases - the rate tables (Table 5, Tables
# 1A-1E) stay V*.0 throughout the FY. Verified 2026-04-30 against FY2026
# V43.1 (April 1, 2026) which shipped only 4 artifacts (definitions manual,
# code edits, mainframe grouper, standalone jars) and NO refreshed Table 5
# or Tables 1A-1E. The `version` column on both Phase 3 silver tables retains
# room for V*.1 if CMS ever changes that practice in a future FY.
#
# URL discovery (verified 2026-04-30 by 3 parallel research agents covering
# FY2008-FY2012, FY2013-FY2017, FY2018-FY2026): all 38 V*.0 cells live on
# direct cms.gov, HTTP 200 + `Content-Type: application/zip`. ZERO Wayback
# fallbacks needed - PI directive (URL gaps are blockers) is satisfied
# without resorting to archive.org.
#
# **Three URL eras across FY2008-FY2026** (mirrors OPPS/PFS multi-era pattern):
#
#   Era A  -- Modern Drupal (FY2022, FY2023, FY2024, FY2026):
#             `cms.gov/files/zip/fy{YYYY}-ipps-fr-table-{N}.zip`.
#   Era A' -- Hyphen + `final-rule` slug (FY2021, FY2025):
#             `cms.gov/files/zip/fy-{YYYY}-ipps-{fr|final-rule}-table-{N}.zip`.
#             Single-cell anomalies; FY2021 tables1 carries Drupal 301 redirect
#             to a long-basename canonical (`table-1a-1e-operating-and-
#             capital-national-standardized-amounts-fy-2021-final-rule-and-
#             correction.zip`); the canonical short URL we encode also
#             returns 200 directly.
#   Era B  -- Legacy mixed-case + lowercase host (FY2008-FY2020):
#             `cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/
#             {Downloads,downloads}/...` with severe per-FY basename drift.
#             FY2018+FY2019 carry CMS docket prefix (`CMS-1677`/`CMS-1694`);
#             FY2020 dropped the docket; FY2017+FY2016 use lowercase host with
#             `cms-1655`/`cms-1632`; FY2015 hyphen-4digit; FY2013+FY2014
#             underscore-2digit; FY2008-FY2012 have idiosyncratic per-FY slugs
#             (`fy_08_fr_table_5`, `fy09_ipps_-fr_table_5` with literal `_-`
#             stray, `fy_2009_final_table_1a_1d`, `fy2010_table_1a-1e_1st_2nd_
#             half_rates`, `fy_11_final_table_1a_through_1e`, etc.).
#
# **Schema deviations from plan-v6.md** (logged in `plans/ipps-url-discovery.md`
# and CLAUDE.md landmines, surfaced to PI):
#
#   1. `IPPS_VERSIONS_BY_FY` is V*.0-only for Phase 3 (V*.1 doesn't ship rates).
#   2. FY2008 + FY2009 ship Tables 1A-1D only (no 1E -- Table 1E was added
#      in FY2010 to expose the Affordable Care Act adjustment). Cardinality
#      assertion in `src/transform/ipps_tables.py` is subset, not equality.
#   3. FY2010 ships dual rate periods (1st_half pre-PPACA, 2nd_half post-PPACA)
#      inside a single ZIP. Locked `ipps_std_amounts` schema extends with
#      one nullable column `rate_period` ∈ {"1st_half", "2nd_half", null}.
#      NULL for all FYs except FY2010. Compound key extends to
#      `(fy, version, table, line_item, category, rate_period)`.

IPPS_FYS: list[int] = list(range(2008, 2027))

# Versions per FY. V*.0 = October FY-start (canonical rate version). V*.1 =
# April mid-year refresh, but per Deviation 1 above it never ships refreshed
# Table 5 or Tables 1A-1E -- so Phase 3 only encodes V*.0. Phase 4 will add
# V*.1 entries here when the grouper layer needs them.
IPPS_VERSIONS_BY_FY: dict[int, tuple[str, ...]] = {
    fy: (f"V{fy - 1983}.0",) for fy in IPPS_FYS
    # V25.0 (FY2008) ... V43.0 (FY2026)
}


_IPPS_LEGACY_BASE_LOWER: str = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment/"
    "acuteinpatientpps/downloads/"
)
_IPPS_LEGACY_BASE_MIXED: str = (
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/"
    "AcuteInpatientPPS/Downloads/"
)
_IPPS_MODERN_BASE: str = "https://www.cms.gov/files/zip/"


# Per-(fy, version) Table 5 ZIP URLs. Every URL hard-coded explicitly because
# basename drift across FY2008-FY2026 has no derivable template (verified
# 2026-04-30 against the CMS Acute Inpatient Files-for-Download landing
# pages). All URLs return HTTP 200 with Content-Type: application/zip on the
# discovery date.
_IPPS_TABLE5_BY_FY: dict[tuple[int, str], str] = {
    # ---- Era A: Modern Drupal (FY2022, FY2023, FY2024, FY2026) ----
    (2026, "V43.0"): f"{_IPPS_MODERN_BASE}fy2026-ipps-fr-table-5.zip",
    (2024, "V41.0"): f"{_IPPS_MODERN_BASE}fy2024-ipps-fr-table-5.zip",
    (2023, "V40.0"): f"{_IPPS_MODERN_BASE}fy2023-ipps-fr-table-5.zip",
    # FY2022 has long descriptive basename - encode verbatim.
    (2022, "V39.0"): (
        f"{_IPPS_MODERN_BASE}"
        "fy2022-ipps-fr-table-5-fy-2022-ms-drgs-relative-weighting-factors-"
        "and-geometric-and-arithmetic-mean.zip"
    ),
    # ---- Era A': fy- prefix anomalies (FY2021, FY2025) ----
    (2025, "V42.0"): f"{_IPPS_MODERN_BASE}fy-2025-ipps-final-rule-table-5.zip",
    (2021, "V38.0"): f"{_IPPS_MODERN_BASE}fy-2021-ipps-fr-table-5.zip",
    # ---- Era B: legacy mixed-case host (FY2018-FY2020) ----
    (2020, "V37.0"): f"{_IPPS_LEGACY_BASE_MIXED}FY2020-FR-Table-5.zip",
    (2019, "V36.0"): f"{_IPPS_LEGACY_BASE_MIXED}FY2019-CMS-1694-FR-Table-5.zip",
    (2018, "V35.0"): f"{_IPPS_LEGACY_BASE_MIXED}FY2018-CMS-1677-FR-Table-5.zip",
    # ---- Era B: legacy lowercase host (FY2008-FY2017) ----
    (2017, "V34.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2017-cms-1655-fr-table-5.zip",
    (2016, "V33.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2016-cms-1632-fr-table-5.zip",
    (2015, "V32.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2015-fr-table-5.zip",
    (2014, "V31.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_14_fr_table_5.zip",
    (2013, "V30.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_13_fr_table_5.zip",
    (2012, "V29.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_12_fr_table_5.zip",
    (2011, "V28.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_2011_fr_table_5.zip",
    (2010, "V27.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_2010_fr_table_5.zip",
    # FY2009 has stray `_-` between `ipps` and `fr` - encode verbatim.
    (2009, "V26.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy09_ipps_-fr_table_5.zip",
    (2008, "V25.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_08_fr_table_5.zip",
}


# Per-(fy, version) Tables 1A-1E ZIP URLs. As with Table 5, every URL is
# hard-coded -- basename drift has no derivable template. NB: FY2008 and
# FY2009 ZIPs ship Tables 1A-1D only (Table 1E was added in FY2010 for
# PPACA). The extractor's per-table member-detection handles the absence
# of Table 1E gracefully; the transform's cardinality assertion is subset
# `observed ⊆ {"1A","1B","1C","1D","1E"}`, not equality.
_IPPS_TABLES1_BY_FY: dict[tuple[int, str], str] = {
    # ---- Era A: Modern Drupal ----
    (2026, "V43.0"): f"{_IPPS_MODERN_BASE}fy2026-ipps-fr-table-1a-1e.zip",
    (2024, "V41.0"): f"{_IPPS_MODERN_BASE}fy2024-ipps-fr-table-1a-1e.zip",
    (2023, "V40.0"): f"{_IPPS_MODERN_BASE}fy2023-ipps-fr-table-1a-1e.zip",
    (2022, "V39.0"): f"{_IPPS_MODERN_BASE}fy2022-ipps-fr-table-1a-1e.zip",
    # ---- Era A': fy- prefix anomalies ----
    (2025, "V42.0"): (
        f"{_IPPS_MODERN_BASE}fy-2025-ipps-final-rule-table-1a-1e.zip"
    ),
    # FY2021 short URL returns 200 directly; CMS also serves a Drupal 301
    # to a long-basename canonical (`table-1a-1e-operating-and-capital-
    # national-standardized-amounts-fy-2021-final-rule-and-correction.zip`).
    # We encode the short form -- Drupal redirects work transparently
    # through `_http.download()`'s session.
    (2021, "V38.0"): f"{_IPPS_MODERN_BASE}fy-2021-ipps-fr-table-1a-1e.zip",
    # ---- Era B: legacy mixed-case host ----
    (2020, "V37.0"): f"{_IPPS_LEGACY_BASE_MIXED}FY2020-FR-Table-1A-1E.zip",
    # FY2019 ships PLURAL `Tables-1A-1E.zip` (singular variant 404s).
    (2019, "V36.0"): (
        f"{_IPPS_LEGACY_BASE_MIXED}FY2019-CMS-1694-FR-Tables-1A-1E.zip"
    ),
    # FY2018 ships SINGULAR `Table-1.zip` whose internal member is
    # `CMS_1677_FR_Table 1A - 1E.xlsx`. Same packaging trick as FY2017+FY2016.
    (2018, "V35.0"): f"{_IPPS_LEGACY_BASE_MIXED}FY2018-CMS-1677-FR-Table-1.zip",
    # ---- Era B: legacy lowercase host ----
    # FY2017+FY2016 ZIPs are basename `*-table-1.zip` (singular) but contain
    # the full Tables 1A-1E txt + xlsx members inside. FY2016 additionally
    # carries CN (Correction Notice), CCC, and CAA (Consolidated
    # Appropriations Act) variants alongside the FR -- extractor selects FR
    # by default and emits a stderr warning if other variants are present.
    (2017, "V34.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2017-cms-1655-fr-table-1.zip",
    (2016, "V33.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2016-cms-1632-fr-table-1.zip",
    (2015, "V32.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy2015-fr-table-1a-1e.zip",
    (2014, "V31.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_14_fr_table_1a-1e.zip",
    (2013, "V30.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_13_fr_table_1a-1e.zip",
    (2012, "V29.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_2012_fr_table_1a-1e.zip",
    # FY2011 uses the `1a_through_1e` slug variant.
    (2011, "V28.0"): (
        f"{_IPPS_LEGACY_BASE_LOWER}fy_11_final_table_1a_through_1e.zip"
    ),
    # FY2010 carries dual rate periods (1st_half + 2nd_half PPACA split)
    # inside ONE ZIP - extractor demuxes via member basename.
    (2010, "V27.0"): (
        f"{_IPPS_LEGACY_BASE_LOWER}fy2010_table_1a-1e_1st_2nd_half_rates.zip"
    ),
    # FY2008 + FY2009 ship Tables 1A-1D only (no 1E pre-PPACA).
    (2009, "V26.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_2009_final_table_1a_1d.zip",
    (2008, "V25.0"): f"{_IPPS_LEGACY_BASE_LOWER}fy_2008_fr_ipps_table1a-1d.zip",
}


# Comprehensive (fy, version, table_kind) -> URL manifest. 19 FYs × 1 version
# × 2 table_kinds = 38 entries. Each entry is consumed by the bulk extractor;
# the per-FY-version dicts above are the natural maintenance keys.
IPPS_SOURCES: dict[tuple[int, str, str], str] = {
    **{(fy, ver, "table5"): url for (fy, ver), url in _IPPS_TABLE5_BY_FY.items()},
    **{(fy, ver, "tables1"): url for (fy, ver), url in _IPPS_TABLES1_BY_FY.items()},
}


def ipps_fy_interval(fy: int, version: str) -> tuple[date, date]:
    """Return (effective_start, effective_end) for an IPPS FY+version cell.

    V*.0 (October FY-start): (Oct 1 fy-1, Sep 30 fy). Reserves space for
    a hypothetical V*.1 republication of rate tables: V*.1 would map to
    (Apr 1 fy, Sep 30 fy) but per Phase 3 Deviation 1 no such cell ships
    today.

    Special case: FY2010 1st_half / 2nd_half rate periods are NOT encoded
    here -- the extractor computes them inline as (Oct 1 2009, Mar 31 2010)
    / (Apr 1 2010, Sep 30 2010) when emitting the rate_period rows. This
    helper still returns the FY-wide interval for FY2010 V27.0 and is
    used by Table 5 (which has no rate_period split) regardless.

    Raises
    ------
    KeyError
        If `(fy, version)` is not a known cell.
    """
    if fy not in IPPS_FYS:
        raise KeyError(
            f"ipps fy{fy}: not in IPPS_FYS "
            f"(known: {min(IPPS_FYS)}-{max(IPPS_FYS)})"
        )
    versions = IPPS_VERSIONS_BY_FY.get(fy, ())
    if version not in versions:
        raise KeyError(
            f"ipps fy{fy} version{version!r}: not in "
            f"IPPS_VERSIONS_BY_FY[{fy}] = {versions}"
        )
    if version.endswith(".0"):
        return (date(fy - 1, 10, 1), date(fy, 9, 30))
    if version.endswith(".1"):
        return (date(fy, 4, 1), date(fy, 9, 30))
    raise ValueError(  # pragma: no cover -- guarded above
        f"ipps fy{fy} version{version!r}: unexpected suffix"
    )


# ---------------------------------------------------------------------------
# Phase 4 source: MS-DRG Java grouper (Session 6 -- V43.1 only; Session 7 fills
# FY2008-FY2025). Discovery artifact: plans/grouper-url-discovery.md.
# ---------------------------------------------------------------------------

GROUPER_HOME_PAGE_URL: str = (
    "https://www.cms.gov/medicare/payment/prospective-payment-systems/"
    "acute-inpatient-pps/ms-drg-classifications-and-software"
)

# Per-FY grouper version manifest. V*.0 = October FY-start; V*.1 = April mid-year
# refresh (which DOES refresh the grouper, unlike rate tables -- see CLAUDE.md
# Phase 3 landmine on V*.1 cadence).
#
# Phase 4 part 2 scope reduction (Session 7, 2026-04-30, PI-approved):
#   - V25.0-V34.0 (FY2008-FY2017): NTIS-paid distribution only, never on
#     public CMS HTTP. No Java grouper available -> empty tuple.
#   - V35.0-V38.0 (FY2018-FY2021 V*.0): CMS published Windows .exe + mainframe
#     COBOL only. Java grouper never built -> empty tuple.
#   - V38.1 / V39.0 / V39.1 (FY2021 mid - FY2022): Java JARs DO exist on
#     cms.gov, but inner JAR shape differs significantly (V38.1 flat namespace,
#     V39 model-v1 not model-v2). The current KGCostGrouperDriver.java imports
#     `gov.agency.msdrg.model.v2.*` statically, so V38.1/V39 require a
#     separate driver template. Deferred to Session 8+; URLs preserved in
#     `_GROUPER_DEFERRED_SOURCES` for traceability.
#   - V40.0+ (FY2023+): model-v2 stable; current driver works; in scope.
#   - drgpy (FY2019-FY2023, v36-v40) covers FY2019-FY2022 as a Python
#     fallback for cohorts in those years (no Java needed).
#
# See `plans/grouper-url-discovery.md` Section 7 + memory file
# `project_phase4_scope_reduction.md`.
GROUPER_VERSIONS_BY_FY: dict[int, tuple[str, ...]] = {
    2008: (), 2009: (), 2010: (), 2011: (), 2012: (), 2013: (),
    2014: (), 2015: (), 2016: (), 2017: (),  # NTIS-only era
    2018: (), 2019: (), 2020: (), 2021: (), 2022: (),  # Windows/.exe or model-v1; deferred
    2023: ("V40.0", "V40.1"),
    2024: ("V41.0", "V41.1"),
    2025: ("V42.0", "V42.1"),
    2026: ("V43.0", "V43.1"),
}

# Phase 7b Session 9.5 FLAG 3: drgpy fallback coverage map for FY2019-FY2022,
# the no-Java window. drgpy is a pure-Python ICD-10 grouper that supports
# v36..v40. Mapping derived from CMS-version manifest + drgpy 0.0.18
# site-packages/drgpy/data/ inspection (verified 2026-05-02). FY2023 (drgpy
# v40) is intentionally absent here -- the Java V40.0 grouper is preferred
# when vendored. To force drgpy on FY2023 for a cross-check, callers pass
# version="v40" explicitly to ``src.grouper.grouper.assign``.
GROUPER_DRGPY_VERSIONS_BY_FY: dict[int, str] = {
    2019: "v36",
    2020: "v37",
    2021: "v38",
    2022: "v39",
}

# Per (fy, version) -> artifact URLs. All URLs HTTP-200 verified 2026-04-30 on
# direct cms.gov; see plans/grouper-url-discovery.md Section 7 for the full
# audit trail (per-cell anomalies, demux quirks, slug irregularities).
GROUPER_SOURCES: dict[tuple[int, str], dict[str, str]] = {
    # FY2023 V40 -- first stable model-v2 era. Slug: `java-source-code-v40`
    # (V40.0) and `java-source-code-v40-1` (V40.1, hyphenated minor).
    (2023, "V40.0"): {
        "java_jar":           "https://www.cms.gov/files/zip/ms-drg-mce-java-source-code-v40.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd10-ms-drg-definitions-manual-text-files-v40.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v40.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-grouper-software-mce-mainframe-v40.zip",
    },
    (2023, "V40.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/ms-drg-mce-java-source-code-v40-1.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v401.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v401.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-grouper-software-mce-mainframe-v40-1.zip",
    },
    # FY2024 V41 -- slug switches to `javasourcecode-binaries`. Inner core JAR
    # renamed `msdrg-v410-41.0.1.0.jar` (entry-point class `v410`).
    (2024, "V41.0"): {
        "java_jar":           "https://www.cms.gov/files/zip/ms-drg-mce-v41-javasourcecode-binaries.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v41.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v41.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-mce-mainframe-software-v41.zip",
    },
    (2024, "V41.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-and-medicare-code-editor-mce-version-v411-java.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v411.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v411.zip",
        # `.zip-8` Drupal redirect suffix is mandatory; bare URL points elsewhere.
        "mainframe":          "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-8",
    },
    # FY2025 V42 -- BARE-SLUG era (collision risk; see Phase 4 landmine).
    # Inner core JAR `msdrg-v420-42.0.3.3.jar` (class `v420`).
    (2025, "V42.0"): {
        # WARNING: bare slug. ANY future CMS upload could overwrite. See landmine.
        "java_jar":           "https://www.cms.gov/files/zip/standalone-jars.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/msdrgv420icd10r0definitionsmanualtext.zip",
        # Note `definitions-` plural (anomalous; other versions use singular).
        "mce":                "https://www.cms.gov/files/zip/definitions-medicare-code-edits-manual-v42.zip",
        "mainframe":          "https://www.cms.gov/files/zip/mainframe-software.zip",
    },
    (2025, "V42.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-and-medicare-code-editor-mce-version-42-java-source.zip",
        # Literal `.1` in slug + Drupal `-0` redirect suffix BOTH required.
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v42.1.zip-0",
        # Literal `.1` in slug (only V42.1 uses this; all others use `-1`/`411`).
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-version-42.1.zip",
        "mainframe":          "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-10",
    },
    # FY2026 V43 -- already vendored in Session 6.
    (2026, "V43.0"): {
        "java_jar":           "https://www.cms.gov/files/zip/ms-drg-mce-v43-standalone-jars.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd10-ms-drg-definitions-manual-files-v43.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v43.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-mce-mainframe-v43.zip",
    },
    (2026, "V43.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/ms-drg-mce-v43-1-standalone-jars.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd10-ms-drg-definitions-manual-files-v43-1.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v43-1.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-mce-mainframe-v43-1.zip",
    },
}

# Java grouper URLs that EXIST on cms.gov but require a separate driver
# template (V38.1: flat namespace, no MsdrgComponent versioned class; V39:
# model-v1 not model-v2). Deferred to Session 8+. Documented here so future
# work can resume without re-running URL discovery.
_GROUPER_DEFERRED_SOURCES: dict[tuple[int, str], dict[str, str]] = {
    (2021, "V38.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce-java.zip",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v381-text.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v381.zip",
        "mainframe":          "https://www.cms.gov/files/zip/ms-drg-mce-v381-mainframe-software.zip",
    },
    (2022, "V39.0"): {
        # 119 MB outer ZIP; inner `MSDRG.V390.Java.P.zip` 4.2 MB. Two-level demux.
        "java_jar":           "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-5",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v39.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v39.zip",
        "mainframe":          "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-4",
    },
    (2022, "V39.1"): {
        "java_jar":           "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-7",
        "definitions_manual": "https://www.cms.gov/files/zip/icd-10-ms-drg-definitions-manual-files-v391.zip",
        "mce":                "https://www.cms.gov/files/zip/definition-medicare-code-edits-v391.zip",
        "mainframe":          "https://www.cms.gov/files/zip/medicare-severity-diagnosis-related-group-ms-drg-grouper-software-and-medicare-code-editor-mce.zip-6",
    },
}

# External runtime dependencies -- NOT bundled in cms.gov standalone JARs ZIP.
# (a) gfc-base-api: Solventum GitHub source ZIP, must be `mvn package`d locally
#     since no prebuilt JAR exists on Maven Central or as a GitHub release asset.
# (b) protobuf-java + (c) slf4j-api: prebuilt JARs on Maven Central.
# (d) slf4j-nop: silences the SLF4J no-binder banner emitted on first JVM call.
GROUPER_RUNTIME_DEPS: dict[str, str] = {
    "gfc_source":   "https://github.com/solventum-oss/GFC-Grouper-Foundation-Classes/archive/refs/tags/v3.4.9.zip",
    "protobuf_jar": "https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.5/protobuf-java-3.25.5.jar",
    "slf4j_jar":    "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
    "slf4j_nop_jar": "https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/1.7.36/slf4j-nop-1.7.36.jar",
}


def grouper_bronze_dir(
    fy: int, version: str, download_date: str | None = None
) -> Path:
    """data/bronze/grouper/fy{fy}_{vshort}/{download_date}/."""
    vshort = version.replace("V", "v").replace(".", "_").lower()  # "V43.1" -> "v43_1"
    dl = download_date or date.today().strftime(BRONZE_DATE_FMT)
    return BRONZE / "grouper" / f"fy{fy}_{vshort}" / dl


def grouper_jar_dir(fy: int, version: str) -> Path:
    """src/grouper/jars/V43_1/ -- extracted JAR working tree (NOT bronze)."""
    vshort_upper = version.replace(".", "_")  # "V43.1" -> "V43_1"
    return ROOT / "src" / "grouper" / "jars" / vshort_upper


def grouper_vendor_dir() -> Path:
    """src/grouper/vendor/ -- vendored runtime dep JARs (gfc, protobuf, slf4j)."""
    return ROOT / "src" / "grouper" / "vendor"


# =============================================================================
# Phase 5 -- Empirical PUF means
# =============================================================================
#
# Year-range constants used by Phase 5 extractors and tests.
# URLs below are populated per-source by Phase 5 implementation agents
# (Batch 2a-2d) via Edit -- each agent owns ONLY its own source dict.

PUF_INPATIENT_FYS: list[int] = list(range(2011, 2025))           # FY2011-FY2024
PUF_INPATIENT_SUMMARY_FYS: list[int] = [2011, 2012, 2013]        # backfill only
PUF_OUTPATIENT_CYS: list[int] = list(range(2011, 2024))          # CY2011-CY2023
PUF_PHYSICIAN_CYS: list[int] = list(range(2012, 2024))           # CY2012-CY2023

_EXPECTED_PUF_YEAR_RANGES: dict[str, tuple[int, int]] = {
    "puf_inpatient":         (2011, 2024),
    "puf_inpatient_summary": (2011, 2013),
    "puf_outpatient":        (2011, 2023),
    "puf_physician":         (2012, 2023),
}

# --- Socrata dataset IDs (replication-package metadata; not consumed at build time) ---
#
# Per Session 8.5 PI verdict on DEV 3 (BLESSED with caveat, one-pass paradigm):
# CMS's data.cms.gov uses content-addressable storage with per-release UUIDs in
# the file path (sites/default/files/{YYYY-MM}/{uuid}/...). The {uuid} changes
# whenever CMS republishes; the {YYYY-MM} marks the release wave. Hardcoded URLs
# in _PUF_*_SOURCES dicts capture a frozen snapshot for this analytic build.
#
# The dataset IDs below are the STABLE replication-package seeds -- they
# survive UUID rotation and wave renormalization. Note that data.cms.gov has
# migrated off the Socrata platform to a custom data portal, so the IDs are
# UUID-form (not the legacy Socrata 8-4 token convention); the resolution
# concept is identical. Resolve via the data.cms.gov dataset-resources API at
# https://data.cms.gov/data-api/v1/dataset-resources/{dataset_id} (or scrape
# the catalog manifest at https://data.cms.gov/data.json) to recover current
# file URLs for re-runs after manuscript freeze.
#
# Dataset IDs verified against https://data.cms.gov/data.json on 2026-05-01:
#   puf_inpatient (Medicare Inpatient Hospitals - by Provider and Service):
#       690ddc6c-2767-4618-b277-420ffb2bf27c
#   puf_outpatient (Medicare Outpatient Hospitals - by Provider and Service):
#       ccbc9a44-40d4-46b4-a709-5caa59212e50
#   puf_physician (Medicare Physician & Other Practitioners - by Provider and Service):
#       92396110-2aed-4d63-a6a2-5d6207d46a29
#   puf_inpatient_summary (Medicare Inpatient Hospitals - by Geography and Service):
#       2941ab09-8cee-49d8-9703-f3c5b854e388
#
# These IDs are NOT consumed at extract time (Phase 5 is one-pass). They are
# captured here so the manuscript replication package can resolve current URLs.

# --- Source URL dicts (module-local in extractors) ---------------------------
#
# Phase 5 deviates from Phase 1-3 precedent: per-source URL dicts live
# module-local at the top of each extractor (``_PUF_<NAME>_SOURCES`` in
# ``src/extract/puf_*.py``) rather than centralized here. Rationale: the
# four extractors were authored in parallel by independent agents who each
# performed URL discovery on data.cms.gov; centralizing would have serialized
# Batch 2 unnecessarily. The year-range constants above remain in config so
# argparse and tests can share them.
