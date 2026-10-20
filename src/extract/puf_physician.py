"""Medicare Physician & Other Practitioners PUF extractor (Phase 5, Batch 2d).

Per CY (CY2012-CY2023):
    1. URL from module-local ``_PUF_PHYSICIAN_SOURCES`` (3 verified seeds:
       CY2012, CY2017, CY2023; remaining 9 CYs are TODO awaiting URL
       discovery on data.cms.gov). Main agent will wire to
       ``config.PUF_PHYSICIAN_SOURCES`` after Batch 2 closes.
    2. Download via ``_http.download_to_bronze`` with ``period="cy"``;
       bronze tree: ``data/bronze/puf_physician/cy{cy}/{download_date}/``.
       If URL is .zip, the inner per-provider TSV is unwrapped to bronze.
    3. Stream the per-provider TSV via DuckDB ``read_csv_auto`` with
       ``ALL_VARCHAR=TRUE``; group by ``(HCPCS_Cd, Place_Of_Srvc)`` and emit
       weighted means + percentile bands. ``SET memory_limit='8GB'`` and
       ``threads=4`` keeps the per-CY aggregation bounded on a 16 GB dev box.
       No per-provider rows ever land in silver.
    4. Apply additive privacy mask: percentile + n_providers columns are
       NULLed when ``n_providers < 11``. Means are NOT masked (CMS already
       suppresses per-provider rows where Tot_Srvcs<11 AND Tot_Bene<11).
    5. Write to ``data/silver/per_source/puf/physician/puf_physician_cy{cy}.parquet``
       with the locked Phase 5 27-column schema (Session 8.5 widening adds
       ``source_host`` between ``source`` and ``effective_start``, and the
       Session 8.5 schema delta drops ``mean_allowed`` in favor of
       ``payment_amount`` / ``payment_amount_concept`` /
       ``total_payment_amount`` per the locked concept-assignment table;
       physician rows always carry concept = ``"medicare_allowed"``).

Modifier handling: **ignored**. The locked schema has no ``modifier`` column;
``code`` carries only the bare 5-char HCPCS. Phase 7 may revisit.

CLI::

    python -m src.extract.puf_physician --cy 2022
    python -m src.extract.puf_physician --cy-range 2012-2023
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http
from src.extract._url_resolver import resolve_with_ladder

_BRONZE_SOURCE: str = "puf_physician"
_CODE_SYSTEM: str = "HCPCS-II"
_SCOPE: str = "national"
_SOURCE_TAG: str = "puf_physician"
_HCPCS_CODE_RE = re.compile(r"^[A-V0-9][0-9]{4}$")
_N_PROVIDERS_SUPPRESS_BELOW: int = 11
_DUCKDB_MEMORY_LIMIT: str = "8GB"
_DUCKDB_THREADS: int = 4

# Per-CY URL dict (module-local; promoted to config.PUF_PHYSICIAN_SOURCES by
# the Batch 2 main agent post-batch). All 11 covered CYs (2013-2023) verified
# 2026-05-01 via the data.cms.gov data.json catalog endpoint
# (https://data.cms.gov/data.json), dataset title ``Medicare Physician &
# Other Practitioners - by Provider and Service``. CMS renormalized the
# legacy ``Medicare_Provider_Util_Payment_PUF_CY{YYYY}.zip`` distribution
# in 2025: every per-CY artifact was republished as a single CSV under a
# UUID-prefixed ``2025-{04,11}/`` path, and the legacy ZIP URLs (Batch-2d
# initial seeds for 2012/2017/2023) now 302-redirect to /not-found.
# Each entry below was HEAD-verified to return HTTP 200 with
# ``content-type: text/csv``.
#
# CY2012 remains unrecoverable as of 2026-05-01. The cms.gov canonical
# "by Provider and Service" dataset starts at CY2013 (renormalization
# shifted naming). The NBER mirror at
# ``https://data.nber.org/providerchargepuf/2012/phys2012.csv.zip``
# (303 MB ZIP, ~9.15M rows) DOES exist but explicitly strips HCPCS codes
# and descriptions per NBER's ``desc_phys2012.txt`` (Session 8.5 Step 1
# finding) -- without HCPCS the file cannot populate our
# ``(HCPCS_Cd, Place_Of_Srvc)`` compound key. Phase 7 cost-compute on
# CY2012 physician cohorts must fall back to formula-only (PFS RVU * CF).
# Wayback Machine has no archived snapshot of the original cms.gov
# HCPCS-retained CY2012 PUF.
_PUF_PHYSICIAN_SOURCES: dict[int, str] = {
    # CY2012: unrecoverable on cms.gov + Wayback as of 2026-05-01
    2013: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "bf4231f9-ec7f-4189-afc3-ba5d53b8bd12/"
        "MUP_PHY_R25_P04_V20_D13_Prov_Svc.csv"
    ),
    2014: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "6700f86d-d2e5-4f2d-9dcb-8c30412768ff/"
        "MUP_PHY_R25_P04_V20_D14_Prov_Svc.csv"
    ),
    2015: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "14954ce3-4c43-43df-97e9-2c0437d7b43c/"
        "MUP_PHY_R25_P04_V20_D15_Prov_Svc.csv"
    ),
    2016: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "426bf97a-4cb8-47ca-9727-a535d9e8c298/"
        "MUP_PHY_R25_P04_V20_D16_Prov_Svc.csv"
    ),
    2017: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "4623fb40-781e-4eef-860e-b851cd5d10ea/"
        "MUP_PHY_R25_P04_V20_D17_Prov_Svc.csv"
    ),
    2018: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "5669eafb-f0b3-4dc5-be6d-abc09b480c2e/"
        "MUP_PHY_R25_P04_V20_D18_Prov_Svc.csv"
    ),
    2019: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "7befba27-752e-47a8-a76c-6c6d4f74f2e3/"
        "MUP_PHY_R25_P04_V20_D19_Prov_Svc.csv"
    ),
    2020: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "d22b18cd-7726-4bf5-8e9c-3e4587c589a1/"
        "MUP_PHY_R25_P05_V20_D20_Prov_Svc.csv"
    ),
    2021: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "bffaf97a-c2ab-4fd7-8718-be90742e3485/"
        "MUP_PHY_R25_P05_V20_D21_Prov_Svc.csv"
    ),
    2022: (
        "https://data.cms.gov/sites/default/files/2025-11/"
        "53fb2bae-4913-48dc-a6d4-d8c025906567/"
        "MUP_PHY_R25_P05_V20_D22_Prov_Svc.csv"
    ),
    2023: (
        "https://data.cms.gov/sites/default/files/2025-04/"
        "e3f823f8-db5b-4cc7-ba04-e7ae92b99757/"
        "MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv"
    ),
}


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip().upper()


_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "hcpcs_cd": ("HCPCS_Cd", "HCPCS", "hcpcs_code", "Hcpcs_Cd"),
    "place_of_srvc": (
        "Place_Of_Srvc", "Place_Of_Service", "place_of_service", "Plc_Of_Srvc",
    ),
    "rndrng_npi": ("Rndrng_NPI", "NPI", "npi", "Rndrng_Npi"),
    "tot_srvcs": (
        "Tot_Srvcs", "Total_Services", "total_services", "line_srvc_cnt",
    ),
    "avg_sbmtd_chrg": (
        "Avg_Sbmtd_Chrg", "Average_Submitted_Charge_Amount",
        "average_submitted_chrg_amt",
    ),
    "avg_mdcr_alowd_amt": (
        "Avg_Mdcr_Alowd_Amt", "Average_Medicare_Allowed_Amount",
        "average_medicare_allowed_amt",
    ),
    "avg_mdcr_pymt_amt": (
        "Avg_Mdcr_Pymt_Amt", "Average_Medicare_Payment_Amount",
        "average_medicare_payment_amt",
    ),
    "avg_mdcr_stdzd_amt": (
        "Avg_Mdcr_Stdzd_Amt", "Average_Medicare_Standardized_Amount",
        "average_medicare_standard_amt",
    ),
}
_REQUIRED_LOGICAL: tuple[str, ...] = tuple(_HEADER_ALIASES.keys())


def _resolve_columns(header: list[str]) -> dict[str, str]:
    """Map logical column names to actual TSV header strings.

    Returns the EXACT header string for each logical name so SQL queries can
    quote it. Raises ``RuntimeError`` if any required column is missing.
    """
    norm_to_actual = {_normalize_header(c): c for c in header}
    out: dict[str, str] = {}
    for logical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            actual = norm_to_actual.get(_normalize_header(alias))
            if actual is not None:
                out[logical] = actual
                break
    missing = [k for k in _REQUIRED_LOGICAL if k not in out]
    if missing:
        raise RuntimeError(
            f"puf_physician: required columns {missing} not found in TSV "
            f"header; saw {sorted(norm_to_actual.keys())[:30]}..."
        )
    return out


def _is_tsv_member(name: str) -> bool:
    bn = Path(name).name.lower()
    if not (bn.endswith(".tsv") or bn.endswith(".txt") or bn.endswith(".csv")):
        return False
    if bn.startswith("readme") or "dictionary" in bn or "metadata" in bn:
        return False
    return True


def _find_tsv_member(zf: zipfile.ZipFile) -> str:
    """Locate the per-provider TSV/CSV inside a CMS Physician PUF ZIP."""
    members = [info for info in zf.infolist() if _is_tsv_member(info.filename)]
    if not members:
        names = [Path(info.filename).name for info in zf.infolist()]
        raise RuntimeError(
            f"puf_physician: no .tsv/.txt/.csv member found in ZIP; "
            f"members were {names}"
        )
    tokens = ("mup_phy", "medicare_provider", "mup_physician", "prov_svc")
    for info in members:
        bn = Path(info.filename).name.lower()
        if any(tok in bn for tok in tokens):
            return info.filename
    members.sort(key=lambda info: info.file_size, reverse=True)
    return members[0].filename


def _extract_zip_member(zf: zipfile.ZipFile, member: str, dest: Path) -> Path:
    """Stream-extract ``member`` from ``zf`` to ``dest`` in 256 KB chunks."""
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zf.open(member, "r") as src, dest.open("wb") as out:
        while True:
            chunk = src.read(256 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return dest


def _stage_tsv(bronze_path: Path) -> Path:
    """Return a Path to a usable .tsv/.txt/.csv; unwrap from .zip if needed.

    The 2026-05-01 data.cms.gov re-publication ships bare ``.csv`` (comma-
    delimited); pre-2026 archived ZIPs ship a ``.tsv``/``.txt`` member.
    """
    suffix = bronze_path.suffix.lower()
    if suffix in (".tsv", ".txt", ".csv"):
        return bronze_path
    if suffix != ".zip":
        raise RuntimeError(
            f"puf_physician: unsupported bronze suffix {suffix!r}"
        )
    with zipfile.ZipFile(bronze_path, "r") as zf:
        member = _find_tsv_member(zf)
        dest = bronze_path.parent / Path(member).name
        return _extract_zip_member(zf, member, dest)


def _detect_delimiter(path: Path) -> str:
    """Return ``\\t`` for .tsv/.txt, ``,`` for .csv. Mirrors OPPS/ASC pattern."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return ","
    return "\t"


def _read_tsv_header(tsv_path: Path) -> list[str]:
    with tsv_path.open("rt", encoding="utf-8", errors="replace") as fh:
        first = fh.readline()
    if not first:
        raise RuntimeError(f"puf_physician: empty TSV at {tsv_path}")
    delim = _detect_delimiter(tsv_path)
    return first.rstrip("\r\n").split(delim)


def _build_aggregation_sql(cols: dict[str, str], *, sep: str) -> str:
    """Construct the per-CY DuckDB aggregation SQL.

    Double-CTE: src (raw read_csv_auto, ALL_VARCHAR) -> typed (cast + filter
    HCPCS regex + filter POS in {F,O}) -> aggregated SELECT grouped on
    (hcpcs, pos). Identifiers double-quoted to survive DuckDB normalization.

    ``sep`` is the field delimiter literal: ``"\\t"`` for legacy ZIP TSV
    members; ``","`` for the post-2026 bare CSV distribution.
    """
    def q(logical: str) -> str:
        return '"' + cols[logical].replace('"', '""') + '"'

    sep_sql = "\\t" if sep == "\t" else sep

    return f"""
        WITH src AS (
            SELECT * FROM read_csv_auto(?, sep='{sep_sql}', ALL_VARCHAR=TRUE,
                                         header=TRUE, ignore_errors=TRUE)
        ),
        typed AS (
            SELECT
                UPPER({q("hcpcs_cd")}) AS hcpcs,
                UPPER({q("place_of_srvc")}) AS pos,
                CAST({q("rndrng_npi")} AS VARCHAR) AS npi,
                TRY_CAST({q("tot_srvcs")} AS DOUBLE) AS tot_srvcs,
                TRY_CAST({q("avg_sbmtd_chrg")} AS DOUBLE) AS avg_sbmtd_chrg,
                TRY_CAST({q("avg_mdcr_alowd_amt")} AS DOUBLE) AS avg_mdcr_alowd_amt,
                TRY_CAST({q("avg_mdcr_pymt_amt")} AS DOUBLE) AS avg_mdcr_pymt_amt,
                TRY_CAST({q("avg_mdcr_stdzd_amt")} AS DOUBLE) AS avg_mdcr_stdzd_amt
            FROM src
            WHERE REGEXP_MATCHES(UPPER({q("hcpcs_cd")}), '^[A-V0-9][0-9]{{4}}$')
              AND UPPER({q("place_of_srvc")}) IN ('F', 'O')
              AND TRY_CAST({q("tot_srvcs")} AS DOUBLE) IS NOT NULL
              AND TRY_CAST({q("tot_srvcs")} AS DOUBLE) > 0
        )
        SELECT
            hcpcs AS code,
            pos AS place_of_srvc,
            CAST(SUM(tot_srvcs) AS BIGINT) AS n,
            SUM(avg_sbmtd_chrg * tot_srvcs) / NULLIF(SUM(tot_srvcs), 0) AS mean_charge,
            SUM(avg_mdcr_alowd_amt * tot_srvcs) / NULLIF(SUM(tot_srvcs), 0) AS payment_amount,
            SUM(avg_mdcr_pymt_amt * tot_srvcs) / NULLIF(SUM(tot_srvcs), 0) AS mean_payment,
            SUM(avg_mdcr_stdzd_amt * tot_srvcs) / NULLIF(SUM(tot_srvcs), 0) AS mean_payment_stdzd,
            QUANTILE_CONT(avg_sbmtd_chrg, 0.10) AS p10_charge,
            QUANTILE_CONT(avg_sbmtd_chrg, 0.25) AS p25_charge,
            QUANTILE_CONT(avg_sbmtd_chrg, 0.50) AS p50_charge,
            QUANTILE_CONT(avg_sbmtd_chrg, 0.75) AS p75_charge,
            QUANTILE_CONT(avg_sbmtd_chrg, 0.90) AS p90_charge,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.10) AS p10_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.25) AS p25_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.50) AS p50_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.75) AS p75_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.90) AS p90_payment,
            CAST(COUNT(DISTINCT npi) AS INTEGER) AS n_providers
        FROM typed
        GROUP BY hcpcs, pos
        ORDER BY hcpcs, pos
    """


def _aggregate_tsv(tsv_path: Path) -> "pa.Table":
    header = _read_tsv_header(tsv_path)
    cols = _resolve_columns(header)
    sep = _detect_delimiter(tsv_path)
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads={_DUCKDB_THREADS}")
        aggregated = con.execute(
            _build_aggregation_sql(cols, sep=sep), [str(tsv_path)]
        ).fetch_arrow_table()
    finally:
        con.close()
    # Schema requires nullable=False on payment_amount (concept = medicare_allowed
    # sourced from Avg_Mdcr_Alowd_Amt). Drop any aggregated row where the
    # weighted-mean payment_amount is null. Mirrors inpatient_summary's
    # post-aggregate filter pattern. Row-drop count logged to stderr.
    n_before = aggregated.num_rows
    aggregated = aggregated.filter(
        pc.is_valid(aggregated.column("payment_amount"))
    )
    n_dropped = n_before - aggregated.num_rows
    if n_dropped > 0:
        print(
            f"[puf_physician] dropped {n_dropped:,} aggregated rows with "
            f"null payment_amount (NOT NULL schema constraint)",
            file=sys.stderr,
        )
    return aggregated


# Locked Phase 5 schema (27 columns; column order load-bearing). Session 8.5
# widening: ``source_host`` inserted between ``source`` and ``effective_start``
# (NOT NULL; values in {"cms_gov", "nber", "wayback"}). Physician PUF emits
# only "cms_gov" -- enforced by the per-cell URL config (no nber_url or
# wayback_url passed to ``resolve_with_ladder``); the dataset has no NBER
# mirror with HCPCS retained (NBER's 2012 mirror strips HCPCS) and no
# usable Wayback fallback. Session 8.5 schema delta also dropped
# ``mean_allowed`` and added ``payment_amount`` / ``payment_amount_concept``
# / ``total_payment_amount`` per the locked concept-assignment table.
# Physician PUF rows always carry concept = "medicare_allowed" sourced
# from Avg_Mdcr_Alowd_Amt; ``total_payment_amount`` is always NULL.
_PARQUET_SCHEMA: pa.Schema = pa.schema([
    pa.field("scope", pa.string(), nullable=False),
    pa.field("code", pa.string(), nullable=False),
    pa.field("code_system", pa.string(), nullable=False),
    pa.field("year", pa.int32(), nullable=False),
    pa.field("place_of_srvc", pa.string(), nullable=True),
    pa.field("n", pa.int64(), nullable=False),
    pa.field("mean_charge", pa.float64(), nullable=True),
    pa.field("mean_payment", pa.float64(), nullable=True),
    pa.field("payment_amount", pa.float64(), nullable=False),
    pa.field("payment_amount_concept", pa.string(), nullable=False),
    pa.field("total_payment_amount", pa.float64(), nullable=True),
    pa.field("mean_payment_stdzd", pa.float64(), nullable=True),
    pa.field("p10_charge", pa.float64(), nullable=True),
    pa.field("p25_charge", pa.float64(), nullable=True),
    pa.field("p50_charge", pa.float64(), nullable=True),
    pa.field("p75_charge", pa.float64(), nullable=True),
    pa.field("p90_charge", pa.float64(), nullable=True),
    pa.field("p10_payment", pa.float64(), nullable=True),
    pa.field("p25_payment", pa.float64(), nullable=True),
    pa.field("p50_payment", pa.float64(), nullable=True),
    pa.field("p75_payment", pa.float64(), nullable=True),
    pa.field("p90_payment", pa.float64(), nullable=True),
    pa.field("n_providers", pa.int32(), nullable=True),
    pa.field("source", pa.string(), nullable=False),
    pa.field("source_host", pa.string(), nullable=False),
    pa.field("effective_start", pa.date32(), nullable=False),
    pa.field("effective_end", pa.date32(), nullable=False),
])

# Physician PUF is exclusively cms.gov-hosted. The NBER mirror for the
# Provider-and-Service distribution exists only for CY2012 and explicitly
# strips HCPCS codes (Session 8.5 Step 1 finding); Wayback has zero
# archived snapshots of the legacy ZIPs (verified 2026-05-01). All emitted
# rows therefore carry ``source_host = "cms_gov"``. Used as a defensive
# default in the URL-resolution path; the actual emit value flows from
# ``resolve_with_ladder``.
_SOURCE_HOST: str = "cms_gov"

# Concept assignment per Session 8.5 plan: Physician PUF (all CYs) sources
# its payment_amount from Avg_Mdcr_Alowd_Amt -> concept "medicare_allowed".
# total_payment_amount is always NULL (no all-payer total in this dataset).
_PAYMENT_CONCEPT: str = "medicare_allowed"

_PERCENTILE_COLS: tuple[str, ...] = (
    "p10_charge", "p25_charge", "p50_charge", "p75_charge", "p90_charge",
    "p10_payment", "p25_payment", "p50_payment", "p75_payment", "p90_payment",
)


def _apply_privacy_mask(aggregated: "pa.Table") -> "pa.Table":
    """NULL out percentile + n_providers columns where n_providers < 11.

    Means are NOT masked (CMS already suppresses per-provider rows where
    both Tot_Srvcs<11 AND Tot_Bene<11). Percentiles are masked because they
    expose the distribution shape.
    """
    n_providers = aggregated.column("n_providers").to_pylist()
    suppress = [
        (v is None) or (v < _N_PROVIDERS_SUPPRESS_BELOW) for v in n_providers
    ]
    new_cols: dict[str, pa.Array] = {}
    for name in aggregated.column_names:
        col = aggregated.column(name)
        if name in _PERCENTILE_COLS:
            vals = col.to_pylist()
            new_cols[name] = pa.array(
                [None if suppress[i] else v for i, v in enumerate(vals)],
                type=pa.float64(),
            )
        elif name == "n_providers":
            vals = col.to_pylist()
            new_cols[name] = pa.array(
                [None if suppress[i] else v for i, v in enumerate(vals)],
                type=pa.int32(),
            )
        else:
            new_cols[name] = col
    return pa.table(new_cols)


def _build_silver_table(
    aggregated: "pa.Table",
    *,
    cy: int,
    cy_start: date,
    cy_end: date,
    source_host: str = _SOURCE_HOST,
) -> "pa.Table":
    """Project aggregated -> locked 27-column schema with constants added.

    ``source_host`` flows from ``resolve_with_ladder`` in ``extract_cy``;
    physician PUF will always receive ``"cms_gov"`` because no NBER or
    Wayback URL is passed to the resolver (NBER's CY2012 mirror strips
    HCPCS per Session 8.5 Step 1 -- see module-level landmine for context).
    The constant default ``_SOURCE_HOST`` is preserved as a defensive
    fallback signature value.
    """
    n = aggregated.num_rows

    def f64(name: str) -> pa.Array:
        return pa.array(aggregated.column(name).to_pylist(), type=pa.float64())

    return pa.Table.from_arrays(
        [
            pa.array([_SCOPE] * n, type=pa.string()),
            pa.array(aggregated.column("code").to_pylist(), type=pa.string()),
            pa.array([_CODE_SYSTEM] * n, type=pa.string()),
            pa.array([cy] * n, type=pa.int32()),
            pa.array(
                aggregated.column("place_of_srvc").to_pylist(),
                type=pa.string(),
            ),
            pa.array(aggregated.column("n").to_pylist(), type=pa.int64()),
            f64("mean_charge"),
            f64("mean_payment"),
            f64("payment_amount"),
            pa.array([_PAYMENT_CONCEPT] * n, type=pa.string()),
            pa.array([None] * n, type=pa.float64()),
            f64("mean_payment_stdzd"),
            f64("p10_charge"),
            f64("p25_charge"),
            f64("p50_charge"),
            f64("p75_charge"),
            f64("p90_charge"),
            f64("p10_payment"),
            f64("p25_payment"),
            f64("p50_payment"),
            f64("p75_payment"),
            f64("p90_payment"),
            pa.array(
                aggregated.column("n_providers").to_pylist(), type=pa.int32()
            ),
            pa.array([_SOURCE_TAG] * n, type=pa.string()),
            pa.array([source_host] * n, type=pa.string()),
            pa.array([cy_start] * n, type=pa.date32()),
            pa.array([cy_end] * n, type=pa.date32()),
        ],
        schema=_PARQUET_SCHEMA,
    )


def _silver_path(cy: int) -> Path:
    return (
        config.SILVER_PER_SOURCE / "puf" / "physician"
        / f"puf_physician_cy{cy}.parquet"
    )


def extract_cy(
    cy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path | None:
    """Extract the Medicare Physician PUF for calendar year ``cy``.

    Raises ``KeyError`` if ``cy`` is not in ``config.PUF_PHYSICIAN_CYS`` or
    if the URL is not yet populated in ``_PUF_PHYSICIAN_SOURCES``.
    Returns ``None`` and prints a stderr notice if the download 404s
    (mirrors HCPCS pre-2017 / OPPS pre-2017 log+skip precedent).
    """
    if cy not in config.PUF_PHYSICIAN_CYS:
        raise KeyError(
            f"puf_physician cy{cy}: not in config.PUF_PHYSICIAN_CYS "
            f"({min(config.PUF_PHYSICIAN_CYS)}-"
            f"{max(config.PUF_PHYSICIAN_CYS)})"
        )

    # Per Session 8.5 Step 1: NBER's CY2012 phys2012.csv.zip strips HCPCS
    # codes per its desc_phys2012.txt and is unusable for our compound key;
    # no other NBER/Wayback fallbacks exist for any CY. Pass nber_url=None
    # and wayback_url=None for every CY so the helper-call shape stays
    # consistent with the other PUF extractors while the per-cell URL
    # config remains the only source of truth on resolvability.
    resolved = resolve_with_ladder(
        cms_url=_PUF_PHYSICIAN_SOURCES.get(cy),
        nber_url=None,  # CY2012 NBER mirror strips HCPCS per Session 8.5 Step 1
        wayback_url=None,
    )
    if resolved is None:
        print(
            f"[puf_physician] cy{cy}: no URL in _PUF_PHYSICIAN_SOURCES; "
            f"skipping (populated CYs: {sorted(_PUF_PHYSICIAN_SOURCES.keys())})",
            file=sys.stderr,
        )
        return None
    url, source_host = resolved

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _silver_path(cy).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"[puf_physician] cy{cy}: URL {url} (source_host={source_host})",
        file=sys.stderr,
    )

    try:
        bronze_path = _http.download_to_bronze(
            url, source=_BRONZE_SOURCE, fy=cy, filename=Path(url).name,
            download_date=download_date, session=sess, period="cy",
        )
    except requests.HTTPError as exc:
        print(
            f"[puf_physician] cy{cy}: HTTP {getattr(exc.response, 'status_code', '?')} "
            f"on {url}; skipping",
            file=sys.stderr,
        )
        return None

    print(
        f"[puf_physician] cy{cy}: bronze at {bronze_path} "
        f"({bronze_path.stat().st_size:,} bytes)", file=sys.stderr,
    )

    tsv_path = _stage_tsv(bronze_path)
    print(
        f"[puf_physician] cy{cy}: staged TSV at {tsv_path} "
        f"({tsv_path.stat().st_size:,} bytes)", file=sys.stderr,
    )

    aggregated = _aggregate_tsv(tsv_path)
    print(
        f"[puf_physician] cy{cy}: aggregated {aggregated.num_rows:,} "
        f"(HCPCS, POS) cells", file=sys.stderr,
    )

    masked = _apply_privacy_mask(aggregated)
    cy_start, cy_end = config.cy_interval(cy)
    table = _build_silver_table(
        masked, cy=cy, cy_start=cy_start, cy_end=cy_end,
        source_host=source_host,
    )

    out_path = out_dir / f"puf_physician_cy{cy}.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[puf_physician] cy{cy}: wrote {table.num_rows:,} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_cy_range(
    start_cy: int,
    end_cy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_cy`` over inclusive ``[start_cy, end_cy]``; log+skip."""
    if end_cy < start_cy:
        raise ValueError(f"end_cy ({end_cy}) must be >= start_cy ({start_cy})")
    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for cy in range(start_cy, end_cy + 1):
        if cy not in config.PUF_PHYSICIAN_CYS:
            print(
                f"[puf_physician] CY{cy}: not in config.PUF_PHYSICIAN_CYS, "
                f"skipping", file=sys.stderr,
            )
            continue
        # Single source of truth on resolvability across extract_cy and
        # extract_cy_range: probe via the same helper extract_cy uses.
        # nber_url and wayback_url are intentionally None for every CY
        # (no usable fallbacks for physician PUF; see Session 8.5 Step 1).
        if resolve_with_ladder(
            cms_url=_PUF_PHYSICIAN_SOURCES.get(cy),
            nber_url=None,
            wayback_url=None,
        ) is None:
            print(
                f"[puf_physician] CY{cy}: no URL in _PUF_PHYSICIAN_SOURCES, "
                f"skipping", file=sys.stderr,
            )
            continue
        try:
            silver_path = extract_cy(
                cy, session=sess, download_date=download_date
            )
            if silver_path is not None:
                out.append(silver_path)
        except Exception as exc:  # noqa: BLE001 -- bulk swallows per-CY
            print(
                f"[puf_physician] CY{cy} FAILED: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((cy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[puf_physician] bulk run: {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for cy, msg in failures:
            print(f"  CY{cy}: {msg}", file=sys.stderr)
    print(
        f"[puf_physician] bulk summary: {len(out)} succeeded, "
        f"{len(failures)} failed", file=sys.stderr,
    )
    return out


def _parse_cy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--cy-range expects 'START-END', got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--cy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.puf_physician",
        description=(
            "Download and stream-aggregate the CMS Medicare Physician & Other "
            "Practitioners PUF (per-provider TSV) for one calendar year or "
            "an inclusive range, writing per-CY pre-aggregated parquet to "
            "data/silver/per_source/puf/physician/."
        ),
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--cy", type=int, help="Single calendar year (e.g., 2022).")
    g.add_argument(
        "--cy-range", type=_parse_cy_range, metavar="START-END",
        help="Inclusive CY range, e.g., '2012-2023'.",
    )
    parser.add_argument(
        "--download-date", default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.cy is not None:
        extract_cy(args.cy, download_date=args.download_date)
    else:
        start_cy, end_cy = args.cy_range
        extract_cy_range(start_cy, end_cy, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
