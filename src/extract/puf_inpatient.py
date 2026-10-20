"""Medicare Inpatient PUF extractor: per-FY CSV -> aggregated per-MS-DRG silver.

Pipeline per FY: read URL from ``_PUF_INPATIENT_SOURCES`` (module-local; main
agent wires ``config.PUF_INPATIENT_SOURCES`` after Batch 2 completes), download
via ``_http.download_to_bronze(period="fy")``, unwrap ZIP if legacy era,
DuckDB-stream the CSV, GROUP BY ``(DRG_Cd, fy)`` with weighted means +
``QUANTILE_CONT`` per-provider percentiles, mask percentiles to NULL when
``n_providers < 11`` (CMS suppression convention), then write per-FY parquet
to ``data/silver/per_source/puf/inpatient/puf_inpatient_fy{fy}.parquet``.

RY/DY/FY mapping: CMS publishes by Release Year (RY) which lags Data Year (DY)
by ~2 years (RY24 = DY22 = FY2022 in our schema). Keys of
``_PUF_INPATIENT_SOURCES`` are FY ints; URL basenames preserve upstream
RY/DY tokens for forensic clarity. The PUF "Data Year" is a calendar/claims
year, but legacy 2011-2017 ZIPs self-label inner CSVs as ``..._FY{YYYY}.csv``,
so the FY=DY identity is consistent across eras for this project's purposes.

Landmines:
- FY2011-FY2013 by-provider files are truncated to top-100 MS-DRGs (~570
  MS-DRGs missing per FY). Batch 2b ``puf_inpatient_summary`` backfills
  aggregate rows. This extractor emits whatever the CSV contains; no silent
  imputation.
- DY2013-DY2021 fetched from the RY23 unified release on data.cms.gov
  (2023-05); CMS re-published 9 prior data years with consistent modern
  column naming (``Rndrng_Prvdr_CCN``, ``DRG_Cd``, ``Avg_Submtd_Cvrd_Chrg``,
  ``Avg_Mdcr_Pymt_Amt``). UUIDs hard-coded per DY (not slug-derivable).
  Source-tag for FY2013-FY2021 is ``puf_all_drgs_ry23`` (renamed from
  ``puf_all_drgs`` per Session 8.5 PI verdict on DEV 1) to capture the
  RY23 retroactive republication methodology; the namespace
  ``puf_all_drgs_legacy`` is reserved for any future fallback to
  original-vintage annual releases.
- Legacy DY2011-DY2012 ZIPs embed DRG_Cd in ``DRG Definition``
  ("039 - EXTRACRANIAL ..."); SQL applies ``REGEXP_EXTRACT('^[0-9]+')``.
- Inpatient PUF carries ``Avg_Tot_Pymt_Amt`` (all-payer total) which
  populates the new ``payment_amount`` column with
  ``payment_amount_concept = 'total_payment_allpayer'`` (also copied into
  the self-documenting ``total_payment_amount`` field per Phase 5
  Session 8.5 schema rename); ``Avg_Mdcr_Pymt_Amt`` (Medicare-only)
  populates ``mean_payment`` as before.

CLI::
    python -m src.extract.puf_inpatient --fy 2023
    python -m src.extract.puf_inpatient --fy-range 2011-2024
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "puf_inpatient"
_SILVER_SUBDIR: tuple[str, str] = ("puf", "inpatient")


def _source_tag_for_fy(fy: int) -> str:
    """Phase 5 Session 8.5 widening: top-100 era vs full-DRG era.

    FY2011-FY2012: legacy CMS per-provider PUF truncated to top-100 MS-DRGs
    (~100 rows). FY2013+: CMS RY23 unified release retroactively re-published
    DY2013 as full-DRG (~570 rows) alongside DY2014-DY2021. Plan-v11.md
    presumed FY2013 was still top-100; empirical bulk run confirms RY23
    upgraded it to all-DRGs.

    Session 8.5 PI verdict on DEV 1 (BLESSED with conditions, 2026-05-01):
    rename FY2013+ tag from ``puf_all_drgs`` to ``puf_all_drgs_ry23`` to
    capture the RY23 retroactive republication methodology
    (https://data.cms.gov/sites/default/files/2023-05/MUP_INP_RY23_20230425_TechSpec_508.pdf).
    The namespace ``puf_all_drgs_legacy`` is reserved for any future
    fallback to the original-vintage annual releases (no rows emitted by
    this session).
    """
    return "puf_top100" if fy in (2011, 2012) else "puf_all_drgs_ry23"

# MS-DRG code (post zero-pad): 3 digits.
_DRG_CODE_RE = re.compile(r"^[0-9]{3}$")

# CMS suppression convention: per-provider quantiles are not disclosable when
# fewer than 11 providers contribute.
_MIN_PROVIDERS_FOR_QUANTILES: int = 11


# Per-FY URL manifest (verified HTTP 200 on 2026-05-01). Three eras:
#   (A) Legacy CMS ZIP (DY2011-DY2012): inner CSV is "...DRG100_FY{YYYY}.csv"
#       -- top-100 MS-DRGs only; backfill aggregate rows via Batch 2b.
#   (B) RY23 unified release (DY2013-DY2021): data.cms.gov 2023-05 with
#       hard-coded per-DY UUIDs and modern column scheme (MUP_IHP_RY23 token).
#   (C) Modern data.cms.gov (DY2022+): MUP_INP_RY{NN}_P03_V10_DY{NN}_PrvSvc.CSV
#       (note: "_INP_" not "_IHP_" as of RY24).
# Discovery: data.cms.gov/data.json catalog + Wayback CDX prefix search on
# data.cms.gov/sites/default/files/2023-05/ + HEAD probe of legacy slugs.
_PUF_INPATIENT_SOURCES: dict[int, str] = {
    # Era A: legacy CMS ZIP (top-100 DRG truncation).
    2011: "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Inpatient_Data_2011_CSV.zip",
    2012: "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Inpatient_Data_2012_CSV.zip",
    # Era B: RY23 unified release; UUIDs hard-coded per DY.
    2013: "https://data.cms.gov/sites/default/files/2023-05/3748379f-baf7-494d-abb7-653fc85176a3/MUP_IHP_RY23_P03_V10_DY13_PRVSVC.CSV",
    2014: "https://data.cms.gov/sites/default/files/2023-05/dd6961c6-0eed-4a33-8527-8f66638819b8/MUP_IHP_RY23_P03_V10_DY14_PRVSVC.CSV",
    2015: "https://data.cms.gov/sites/default/files/2023-05/81895338-4c5c-4ad7-a490-8e5a9a972825/MUP_IHP_RY23_P03_V10_DY15_PRVSVC.CSV",
    2016: "https://data.cms.gov/sites/default/files/2023-05/4f20bce0-607a-46c1-bd0d-78021b0624ec/MUP_IHP_RY23_P03_V10_DY16_PRVSVC.CSV",
    2017: "https://data.cms.gov/sites/default/files/2023-05/ec9287b0-68e6-4818-8c64-3a0c68fcde49/MUP_IHP_RY23_P03_V10_DY17_PRVSVC.CSV",
    2018: "https://data.cms.gov/sites/default/files/2023-05/78d80cf4-fc5b-40db-8e88-ca44bc86102f/MUP_IHP_RY23_P03_V10_DY18_PRVSVC.CSV",
    2019: "https://data.cms.gov/sites/default/files/2023-05/6602e715-b301-4a38-954d-b8d5aec12b87/MUP_IHP_RY23_P03_V10_DY19_PRVSVC.CSV",
    2020: "https://data.cms.gov/sites/default/files/2023-05/e57818f2-318c-4979-a612-c91eba44b011/MUP_IHP_RY23_P03_V10_DY20_PRVSVC.CSV",
    2021: "https://data.cms.gov/sites/default/files/2023-05/a754bf0b-0c51-4daf-876e-272f90a11c05/MUP_IHP_RY23_P03_V10_DY21_PRVSVC.CSV",
    # Era C: modern data.cms.gov releases (DY2022+).
    2022: "https://data.cms.gov/sites/default/files/2024-05/7d1f4bcd-7dd9-4fd1-aa7f-91cd69e452d3/MUP_INP_RY24_P03_V10_DY22_PrvSvc.CSV",
    2023: "https://data.cms.gov/sites/default/files/2025-05/ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV",
    2024: "https://data.cms.gov/sites/default/files/2026-04/828defb5-c9e6-4442-8c1b-f27bc0799daf/MUP_INP_RY26_P03_V10_DY24_PrvSvc.CSV",
}


# Column aliases. Legacy DY2011-DY2012 CSVs use human-readable names
# ("DRG Definition" with embedded "NNN - ..." prefix, "Provider Id",
# "Average Covered Charges"); RY23+ uses terse names ("DRG_Cd",
# "Rndrng_Prvdr_CCN", "Avg_Submtd_Cvrd_Chrg"). The SQL ``REGEXP_EXTRACT('^[0-9]+')``
# shim handles both prefix-embedded and pure-numeric DRG variants.
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "drg_cd": ("DRG_Cd", "drg_cd", "DRG Definition", "DRG_Definition"),
    "provider_id": ("Rndrng_Prvdr_CCN", "Provider_Id", "Provider Id", "provider_id"),
    "tot_dschrgs": ("Tot_Dschrgs", "Total_Discharges", "Total Discharges", "total_discharges"),
    "avg_submtd_cvrd_chrg": ("Avg_Submtd_Cvrd_Chrg", "Average_Covered_Charges", "Average Covered Charges", "average_covered_charges"),
    "avg_mdcr_pymt_amt": ("Avg_Mdcr_Pymt_Amt", "Average_Medicare_Payments", "Average Medicare Payments", "average_medicare_payments"),
    "avg_tot_pymt_amt": ("Avg_Tot_Pymt_Amt", "Average_Total_Payments", "Average Total Payments", "average_total_payments"),
}

_REQUIRED_LOGICAL_COLUMNS: tuple[str, ...] = tuple(_COLUMN_ALIASES.keys())


def _resolve_columns(
    headers: list[str],
    *,
    aliases: dict[str, tuple[str, ...]] = _COLUMN_ALIASES,
    required: tuple[str, ...] = _REQUIRED_LOGICAL_COLUMNS,
) -> dict[str, str]:
    """Map logical column names to the actual CSV header names present.

    Returns ``{logical_name: actual_header_name}``. Raises ``RuntimeError``
    listing every missing required logical name when any has no alias hit.
    Aliases match case-insensitively after stripping whitespace.
    """
    norm_to_actual: dict[str, str] = {}
    for h in headers:
        if h is None:
            continue
        norm_to_actual[str(h).strip().lower()] = h
    out: dict[str, str] = {}
    for logical, alts in aliases.items():
        for alt in alts:
            actual = norm_to_actual.get(alt.strip().lower())
            if actual is not None:
                out[logical] = actual
                break
    missing = [k for k in required if k not in out]
    if missing:
        raise RuntimeError(
            f"puf_inpatient: required columns {missing} not found in CSV header; "
            f"saw {headers[:20]}..."
        )
    return out


# Parquet schema -- locked per Batch 2a spec.
_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
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
    ]
)


def _maybe_extract_zip(bronze_path: Path) -> Path:
    """If `bronze_path` is a ZIP, extract the inner ``*_DRG*.csv`` next to it
    and return that path. Otherwise return `bronze_path` unchanged.

    Idempotent: skips re-extraction when the inner CSV already exists.
    """
    if bronze_path.suffix.lower() != ".zip":
        return bronze_path
    with zipfile.ZipFile(bronze_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(
                f"No .csv member in legacy PUF ZIP {bronze_path}; "
                f"saw {zf.namelist()}"
            )
        # Prefer member containing "DRG" (top-100 file) over methodology.
        members.sort(key=lambda m: ("drg" not in m.lower(), m))
        inner = members[0]
        out = bronze_path.parent / Path(inner).name
        if not out.exists() or out.stat().st_size == 0:
            with zf.open(inner) as src, out.open("wb") as dst:
                while chunk := src.read(64 * 1024):
                    dst.write(chunk)
        return out


def _aggregate_per_drg(csv_path: Path, fy: int) -> "object":
    """Stream the CSV through DuckDB and aggregate per ``(DRG_Cd, fy)``.

    Returns a pandas DataFrame ordered by code with columns:

        code, n, mean_charge, mean_payment, payment_amount,
        p10_charge .. p90_charge, p10_payment .. p90_payment, n_providers

    ``payment_amount`` is the discharge-weighted mean of
    ``Avg_Tot_Pymt_Amt`` (total payment across all payers); the caller
    tags this with ``payment_amount_concept = 'total_payment_allpayer'``
    and copies it into the self-documenting ``total_payment_amount``
    field per Phase 5 Session 8.5 schema rename. ``mean_payment`` is the
    discharge-weighted mean of ``Avg_Mdcr_Pymt_Amt`` (Medicare-only
    portion).
    """
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    # Probe header to resolve column aliases per FY.
    head_rel = con.execute(
        "SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE) LIMIT 0",
        [str(csv_path)],
    )
    headers = [d[0] for d in head_rel.description]
    cols = _resolve_columns(headers)

    drg = cols["drg_cd"]
    pid = cols["provider_id"]
    dis = cols["tot_dschrgs"]
    chrg = cols["avg_submtd_cvrd_chrg"]
    pmt = cols["avg_mdcr_pymt_amt"]
    tot = cols["avg_tot_pymt_amt"]

    sql = f"""
        WITH src AS (
            SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE)
        ),
        typed AS (
            SELECT
                LPAD(REGEXP_EXTRACT(CAST("{drg}" AS VARCHAR), '^([0-9]+)'), 3, '0') AS code,
                CAST("{pid}" AS VARCHAR) AS provider_id,
                TRY_CAST(REPLACE(CAST("{dis}" AS VARCHAR), ',', '') AS DOUBLE) AS tot_dschrgs,
                TRY_CAST(REPLACE(REPLACE(CAST("{chrg}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_submtd_cvrd_chrg,
                TRY_CAST(REPLACE(REPLACE(CAST("{pmt}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_mdcr_pymt_amt,
                TRY_CAST(REPLACE(REPLACE(CAST("{tot}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_tot_pymt_amt
            FROM src
            WHERE REGEXP_MATCHES(CAST("{drg}" AS VARCHAR), '^[0-9]+')
        ),
        typed_pos AS (
            SELECT *
            FROM typed
            WHERE tot_dschrgs IS NOT NULL AND tot_dschrgs > 0
              AND code <> '' AND LENGTH(code) = 3
        )
        SELECT
            code,
            CAST(SUM(tot_dschrgs) AS BIGINT) AS n,
            SUM(avg_submtd_cvrd_chrg * tot_dschrgs)
                / NULLIF(SUM(CASE WHEN avg_submtd_cvrd_chrg IS NOT NULL THEN tot_dschrgs ELSE 0 END), 0) AS mean_charge,
            SUM(avg_mdcr_pymt_amt * tot_dschrgs)
                / NULLIF(SUM(CASE WHEN avg_mdcr_pymt_amt IS NOT NULL THEN tot_dschrgs ELSE 0 END), 0) AS mean_payment,
            SUM(avg_tot_pymt_amt * tot_dschrgs)
                / NULLIF(SUM(CASE WHEN avg_tot_pymt_amt IS NOT NULL THEN tot_dschrgs ELSE 0 END), 0) AS payment_amount,
            QUANTILE_CONT(avg_submtd_cvrd_chrg, 0.10) AS p10_charge,
            QUANTILE_CONT(avg_submtd_cvrd_chrg, 0.25) AS p25_charge,
            QUANTILE_CONT(avg_submtd_cvrd_chrg, 0.50) AS p50_charge,
            QUANTILE_CONT(avg_submtd_cvrd_chrg, 0.75) AS p75_charge,
            QUANTILE_CONT(avg_submtd_cvrd_chrg, 0.90) AS p90_charge,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.10) AS p10_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.25) AS p25_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.50) AS p50_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.75) AS p75_payment,
            QUANTILE_CONT(avg_mdcr_pymt_amt, 0.90) AS p90_payment,
            CAST(COUNT(DISTINCT provider_id) AS INTEGER) AS n_providers
        FROM typed_pos
        GROUP BY code
        ORDER BY code
    """
    df = con.execute(sql, [str(csv_path)]).df()
    con.close()

    # Drop rows whose normalized code doesn't match the post-pad regex.
    df = df[df["code"].str.match(_DRG_CODE_RE.pattern, na=False)].reset_index(drop=True)
    return df


def _mask_quantiles_low_n_providers(df, *, threshold: int = _MIN_PROVIDERS_FOR_QUANTILES):
    """Set all p10..p90 charge/payment quantile columns to NULL when
    ``n_providers < threshold``. Mutates and returns ``df``.
    """
    qcols = [c for c in df.columns if c.startswith(("p10_", "p25_", "p50_", "p75_", "p90_"))]
    mask = df["n_providers"].fillna(0) < threshold
    if mask.any():
        df.loc[mask, qcols] = None
    return df


def _df_to_table(df, *, fy: int, source_url: str) -> pa.Table:
    """Wrap the per-DRG aggregate DataFrame in a pyarrow Table matching the
    locked schema. Adds the constant columns (scope, code_system, year, ...)
    and the per-FY source tag (``puf_top100`` for FY2011-FY2012;
    ``puf_all_drgs_ry23`` for FY2013+). ``mean_payment_stdzd`` is always
    NULL (Inpatient PUF does not publish a standardized-payment column).
    Every row is tagged with ``payment_amount_concept =
    'total_payment_allpayer'`` per the Session 8.5 concept enum, and
    ``total_payment_amount`` is a self-documenting copy of
    ``payment_amount`` for inpatient consumers.
    """
    n_rows = len(df)
    eff_start, eff_end = config.fy_interval(fy)
    source_tag = _source_tag_for_fy(fy)
    # Stage 4 may resolve dynamically (cms_gov / nber / wayback) per fallback
    # tier; for now every cell is sourced from cms.gov per _PUF_INPATIENT_SOURCES.
    source_host = "cms_gov"

    payment_amount_list = df["payment_amount"].tolist()

    arrays = [
        pa.array(["national"] * n_rows, type=pa.string()),
        pa.array(df["code"].tolist(), type=pa.string()),
        pa.array(["MS-DRG"] * n_rows, type=pa.string()),
        pa.array([fy] * n_rows, type=pa.int32()),
        pa.array([None] * n_rows, type=pa.string()),  # place_of_srvc always NULL
        pa.array(df["n"].astype("Int64").tolist(), type=pa.int64()),
        pa.array(df["mean_charge"].tolist(), type=pa.float64()),
        pa.array(df["mean_payment"].tolist(), type=pa.float64()),
        pa.array(payment_amount_list, type=pa.float64()),
        pa.array(["total_payment_allpayer"] * n_rows, type=pa.string()),
        pa.array(payment_amount_list, type=pa.float64()),
        pa.array([None] * n_rows, type=pa.float64()),  # mean_payment_stdzd
        pa.array(df["p10_charge"].tolist(), type=pa.float64()),
        pa.array(df["p25_charge"].tolist(), type=pa.float64()),
        pa.array(df["p50_charge"].tolist(), type=pa.float64()),
        pa.array(df["p75_charge"].tolist(), type=pa.float64()),
        pa.array(df["p90_charge"].tolist(), type=pa.float64()),
        pa.array(df["p10_payment"].tolist(), type=pa.float64()),
        pa.array(df["p25_payment"].tolist(), type=pa.float64()),
        pa.array(df["p50_payment"].tolist(), type=pa.float64()),
        pa.array(df["p75_payment"].tolist(), type=pa.float64()),
        pa.array(df["p90_payment"].tolist(), type=pa.float64()),
        pa.array(df["n_providers"].astype("Int32").tolist(), type=pa.int32()),
        pa.array([source_tag] * n_rows, type=pa.string()),
        pa.array([source_host] * n_rows, type=pa.string()),
        pa.array([eff_start] * n_rows, type=pa.date32()),
        pa.array([eff_end] * n_rows, type=pa.date32()),
    ]
    return pa.Table.from_arrays(arrays, schema=_PARQUET_SCHEMA)


def _silver_path(fy: int) -> Path:
    out = config.SILVER_PER_SOURCE
    for part in _SILVER_SUBDIR:
        out = out / part
    return out / f"puf_inpatient_fy{fy}.parquet"


def extract_fy(
    fy: int,
    *,
    download_date: str | None = None,
    session: requests.Session | None = None,
) -> Path | None:
    """Extract the Inpatient PUF for a single FY -> per-FY parquet.

    Returns the path to the silver parquet on success, or ``None`` if the
    upstream URL 404s (log+skip semantics, mirrors HCPCS / OPPS / Outpatient
    PUF).
    """
    url = _PUF_INPATIENT_SOURCES.get(fy)
    if not url:
        print(f"[puf_inpatient] FY{fy} not in URL dict; skipping", file=sys.stderr)
        return None

    filename = Path(url).name or f"puf_inpatient_fy{fy}.csv"
    sess = session or _http.make_session()
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=fy,
            filename=filename,
            download_date=download_date,
            session=sess,
            period="fy",
        )
    except requests.HTTPError as exc:
        print(
            f"[puf_inpatient] FY{fy}: 404 on {url}; skipping ({exc})",
            file=sys.stderr,
        )
        return None

    csv_path = _maybe_extract_zip(bronze_path)
    print(f"[puf_inpatient] aggregating FY{fy} from {csv_path}", file=sys.stderr)
    df = _aggregate_per_drg(csv_path, fy)

    df = _mask_quantiles_low_n_providers(df)
    table = _df_to_table(df, fy=fy, source_url=url)

    out = _silver_path(fy)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[puf_inpatient] wrote {len(df)} MS-DRG rows to {out}",
        file=sys.stderr,
    )
    return out


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    download_date: str | None = None,
) -> list[Path]:
    """Bulk-extract an FY range with log+skip semantics on per-cell failures."""
    sess = _http.make_session()
    out_paths: list[Path] = []
    for fy in range(start_fy, end_fy + 1):
        try:
            p = extract_fy(fy, download_date=download_date, session=sess)
            if p is not None:
                out_paths.append(p)
        except Exception as exc:
            print(
                f"[puf_inpatient] FY{fy} failed: {exc}; continuing",
                file=sys.stderr,
            )
    return out_paths


def _parse_fy_range(value: str) -> tuple[int, int]:
    try:
        a, b = value.split("-", 1)
        start, end = int(a), int(b)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range must be 'START-END' (e.g. '2011-2024'), got {value!r}"
        ) from exc
    if start > end:
        raise argparse.ArgumentTypeError(
            f"--fy-range start ({start}) must be <= end ({end})"
        )
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.puf_inpatient",
        description=(
            "Download and aggregate the Medicare Inpatient PUF (per-provider, "
            "per-MS-DRG) for one fiscal year or a bulk range, writing per-FY "
            "parquet to data/silver/per_source/puf/inpatient/."
        ),
    )
    fy_group = parser.add_mutually_exclusive_group(required=True)
    fy_group.add_argument(
        "--fy",
        type=int,
        help="Single fiscal year (e.g., 2023).",
    )
    fy_group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        metavar="START-END",
        help="Inclusive FY range, e.g., '2011-2024'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.fy is not None:
        extract_fy(args.fy, download_date=args.download_date)
    else:
        start, end = args.fy_range
        extract_fy_range(start, end, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
