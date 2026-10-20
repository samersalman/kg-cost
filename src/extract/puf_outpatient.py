"""Medicare Outpatient PUF extractor: per-CY CSV/XLSX -> aggregated per-APC silver.

For each calendar year (CY) in scope this module:

    1. Resolves the per-CY URL via ``_resolve_outpatient_url(cy)``, which
       tries (a) the CMS data.cms.gov URL in ``_PUF_OUTPATIENT_SOURCES``,
       (b) the NBER mirror at ``https://data.nber.org/providerchargepuf/``
       for CY2011-CY2013 (CMS retired these from data.cms.gov around 2020),
       (c) Wayback CDX as a tertiary fallback. Returns ``(url, source_host)``
       where ``source_host`` is one of ``cms_gov | nber | wayback``.

    2. Downloads via ``_http.download_to_bronze`` with ``period="cy"``.

    3. Parses the artifact (CSV via DuckDB streaming for CMS-era cells;
       XLSX via openpyxl -> pandas -> DuckDB temp-register for NBER-era
       cells whose bronze file is .xlsx). Normalises APC codes to 4 digits,
       then GROUP BYs ``(APC_Cd, cy)`` to compute weighted means and
       ``QUANTILE_CONT`` percentiles. Per-provider quantiles are masked
       to NULL when ``n_providers < 11`` (CMS suppression convention;
       mirrors Phase 5 plan-v10.md).

    4. Writes a per-CY parquet to
       ``data/silver/per_source/puf/outpatient/puf_outpatient_cy{cy}.parquet``.

CMS publishes this dataset under "Release Year" (RY) labels which lag the
Data Year (DY) by ~2 years, e.g. RY25 = DY23 = CY2023. The keys of
``_PUF_OUTPATIENT_SOURCES`` are calendar years (CY); URLs and bronze
filenames preserve the upstream RY-tagged basenames for forensic clarity.

Outpatient PUF natively carries ``Avg_Mdcr_Alowd_Amt`` for CY2015+ (CMS-era);
the new ``payment_amount`` column is populated directly from that column with
``payment_amount_concept = 'medicare_allowed'`` and ``total_payment_amount``
IS NULL. The earlier CY2011-CY2013 NBER-mirrored files publish ONLY
``Average Total Payments`` (no separate "allowed" amount), so for those
years ``payment_amount`` is the all-payer total with
``payment_amount_concept = 'total_payment_allpayer'`` and
``total_payment_amount = payment_amount`` (self-documenting alias).

Coverage notes (per CMS PUF methodology):

    * Maryland is excluded from every CY because Maryland operates under a
      rate-setting commission outside OPPS.
    * Per-CY APC count drifts: 30 APCs (CY2011-CY2013), 32 APCs (CY2014),
      28 APCs in some later refreshes; ``_APC_COUNT_BAND`` is [25, 40].
    * CY2014 is currently unrecoverable (NBER mirror lacks 2014; Wayback
      CDX search returns no snapshots of the canonical CMS basename as of
      2026-05-01) — bulk runner log+skips that cell.

CLI::

    python -m src.extract.puf_outpatient --cy 2023
    python -m src.extract.puf_outpatient --cy-range 2011-2023
    python -m src.extract.puf_outpatient --cy 2022 --download-date 2026-05-01
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http
from src.extract._url_resolver import VALID_SOURCE_HOSTS, resolve_with_ladder

_BRONZE_SOURCE: str = "puf_outpatient"
_SILVER_SUBDIR: tuple[str, str] = ("puf", "outpatient")

# APC code (post zero-pad): 4 or 5 digits. Mirrors src/extract/opps.py:_APC_CODE_RE.
import re

_APC_CODE_RE = re.compile(r"^[0-9]{4,5}$")

# CMS suppression convention: per-provider quantiles are not disclosable when
# fewer than 11 providers contribute. Plan-v10.md.
_MIN_PROVIDERS_FOR_QUANTILES: int = 11

# Coverage band. APC count drifts by CMS era:
#   * NBER CY2011-CY2013: 30 APCs ("APC30" in basename).
#   * CMS RY19 CY2015-CY2016: 25-33 APCs.
#   * CMS RY19+ CY2017+: 58-68 APCs (CMS broadened coverage).
# Outside [25, 70] -> RuntimeError. plan-v11 documents this as known scope.
_APC_COUNT_BAND: tuple[int, int] = (25, 70)

# Per-CY URLs discovered 2026-05-01 from data.cms.gov/data.json catalog
# (dataset id ccbc9a44-40d4-46b4-a709-5caa59212e50). CY2011-CY2014 are NOT
# present in the catalog as of the discovery date -- historical files appear
# to have been retired around 2020. CY2015..CY2023 verified HTTP 200 via
# curl HEAD on 2026-05-01.
_PUF_OUTPATIENT_SOURCES: dict[int, str] = {
    # CY2011-CY2014: retired from data.cms.gov around 2020 (404). Resolver
    # falls back to NBER mirror (CY2011-CY2013) or Wayback (CY2014).
    # Keys retained as None so resolver knows to try fallbacks.
    2011: None,
    2012: None,
    2013: None,
    2014: None,
    # CY2015-CY2023: verified HTTP 200 on 2026-05-01.
    2015: "https://data.cms.gov/sites/default/files/2020-06/MUP_OHP_R19_P04_V40_D15_Prov_Svc.csv",
    2016: "https://data.cms.gov/sites/default/files/2020-06/MUP_OHP_R19_P04_V40_D16_Prov_Svc.csv",
    2017: "https://data.cms.gov/sites/default/files/2020-06/MUP_OHP_R19_P04_V40_D17_Prov_Svc.csv",
    2018: "https://data.cms.gov/sites/default/files/2020-11/MUP_OHP_R20_P04_V10_D18_Prov_Svc.csv",
    2019: "https://data.cms.gov/sites/default/files/2021-08/MUP_OUT_RY21_P04_V10_DY19_Prov_Svc.csv",
    2020: "https://data.cms.gov/sites/default/files/2024-06/62b3e7e6-674f-4f74-8c52-c1e32d3905eb/MUP_OUT_RY24_P04_V10_DY20_Prov_Svc.csv",
    2021: "https://data.cms.gov/sites/default/files/2024-06/657f3480-6e15-4978-8e4d-78bfa03b7a79/MUP_OUT_RY24_P04_V10_DY21_Prov_Svc.csv",
    2022: "https://data.cms.gov/sites/default/files/2024-06/860428c0-6102-4fff-812d-57c7860613e5/MUP_OUT_RY24_P04_V10_DY22_Prov_Svc.csv",
    2023: "https://data.cms.gov/sites/default/files/2025-08/bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv",
}

# NBER mirror (https://data.nber.org/providerchargepuf/{YYYY}/) for the
# pre-CY2015 era. Verified HTTP 200 via curl HEAD on 2026-05-01.
#
# CY2011 + CY2012 ship as flat CSV; CY2013 ships as XLSX (banner rows then
# header at row 6 zero-indexed). CY2014 has no NBER directory (404 on both
# /2014/ and /2014/Outpatient.html as of 2026-05-01).
_PUF_OUTPATIENT_NBER_SOURCES: dict[int, str] = {
    2011: "https://data.nber.org/providerchargepuf/2011/Medicare_Provider_Charge_Outpatient_APC30_CY2011.csv",
    2012: "https://data.nber.org/providerchargepuf/2012/Medicare_Provider_Charge_Outpatient_APC30_CY2012.csv",
    2013: "https://data.nber.org/providerchargepuf/2013/Medicare_Provider_Charge_Outpatient_APC30_CY2013_v2.xlsx",
}


def _resolve_outpatient_url(cy: int) -> tuple[str, str] | None:
    """Resolve a per-CY URL by trying cms.gov -> NBER -> Wayback.

    Delegates to ``src.extract._url_resolver.resolve_with_ladder`` (Session
    8.5 consolidation): tries (a) the CMS data.cms.gov URL in
    ``_PUF_OUTPATIENT_SOURCES``, (b) the NBER mirror at
    ``https://data.nber.org/providerchargepuf/`` for CY2011-CY2013,
    (c) Wayback CDX as a tertiary fallback. Returns ``(url, source_host)``
    where ``source_host`` is one of ``"cms_gov" | "nber" | "wayback"``.
    Returns ``None`` if every tier misses (caller log+skips).

    No HTTP probing is performed; the NBER fallback layer was verified by
    curl HEAD on 2026-05-01 and CY2014 is documented unrecoverable.
    Wayback is unwired here (CDX search on the canonical CMS basename
    returned [] on 2026-05-01).
    """
    return resolve_with_ladder(
        cms_url=_PUF_OUTPATIENT_SOURCES.get(cy),
        nber_url=_PUF_OUTPATIENT_NBER_SOURCES.get(cy),
        wayback_url=None,  # CY2014 unrecoverable as of 2026-05-01
    )


# ---------------------------------------------------------------------------
# Column-alias resolution (year-over-year drift in CMS PUF column naming)
# ---------------------------------------------------------------------------

# Logical name -> tuple of accepted CSV/XLSX header aliases. CMS renamed
# columns between RY19 (D15-D17) and RY21 (DY19) and again between RY21
# and RY24. NBER-era CY2011-CY2013 headers are space-separated (e.g.
# "Provider Id", "Outpatient Services", "Average Total Payments") with
# occasional double-space artifacts ("Average  Estimated Submitted
# Charges"). Alias resolution collapses double-spaces and matches
# case-insensitively. NBER era has NO allowed-amount column.
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "apc_cd": ("APC_Cd", "APC", "apc"),
    "provider_id": (
        "Rndrng_Prvdr_CCN",
        "Provider_Id",
        "provider_id",
        "Provider Id",
    ),
    "tot_srvcs": (
        "Tot_Srvcs",
        "Total_Services",
        "total_services",
        "Outpatient Services",
        # CY2015+ CMS modern PUF: composite APC services count.
        "CAPC_Srvcs",
        "capc_srvcs",
    ),
    "avg_submtd_chrg": (
        "Avg_Submtd_Chrg",
        "Average_Submitted_Charges",
        "average_submitted_charges",
        "Average Estimated Submitted Charges",
        # CY2015+ CMS modern PUF: total submitted charges.
        "Avg_Tot_Sbmtd_Chrgs",
        "avg_tot_sbmtd_chrgs",
    ),
    "avg_mdcr_alowd_amt": (
        "Avg_Mdcr_Alowd_Amt",
        "Average_Total_Allowed_Amount",
        "average_total_allowed_amount",
    ),
    "avg_mdcr_pymt_amt": (
        "Avg_Mdcr_Pymt_Amt",
        "Average_Medicare_Payment_Amount",
        "average_medicare_payment_amount",
        "Average Total Payments",
    ),
}

# Required for every era. ``avg_mdcr_alowd_amt`` is optional (NBER era
# CY2011-CY2013 doesn't ship it; ``mean_allowed`` falls back to the
# payment-weighted mean for those years).
_REQUIRED_LOGICAL_COLUMNS: tuple[str, ...] = (
    "apc_cd",
    "provider_id",
    "tot_srvcs",
    "avg_submtd_chrg",
    "avg_mdcr_pymt_amt",
)


def _norm_header(h: object) -> str:
    """Lower-case + collapse internal whitespace runs (NBER CY2013 ships
    headers like ``"Average  Estimated Submitted Charges"`` with a double
    space). Mirrors the OPPS extractor's ``_normalize_header`` convention.
    """
    return " ".join(str(h).split()).lower()


def _resolve_columns(
    headers: list[str],
    *,
    aliases: dict[str, tuple[str, ...]] = _COLUMN_ALIASES,
    required: tuple[str, ...] = _REQUIRED_LOGICAL_COLUMNS,
) -> dict[str, str]:
    """Map logical column names to the actual CSV/XLSX header names present.

    Returns ``{logical_name: actual_header_name}``. Raises ``RuntimeError``
    if any ``required`` logical name has no alias hit. Aliases match
    case-insensitively after collapsing internal whitespace runs.
    """
    norm_to_actual: dict[str, str] = {}
    for h in headers:
        if h is None:
            continue
        norm_to_actual[_norm_header(h)] = h
    out: dict[str, str] = {}
    for logical, alts in aliases.items():
        for alt in alts:
            actual = norm_to_actual.get(_norm_header(alt))
            if actual is not None:
                out[logical] = actual
                break
    missing = [k for k in required if k not in out]
    if missing:
        raise RuntimeError(
            f"puf_outpatient: required columns {missing} not found in header; "
            f"saw {headers[:20]}..."
        )
    return out


# ---------------------------------------------------------------------------
# Parquet schema (locked per Batch 2c spec)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Read path: DuckDB streaming aggregate
# ---------------------------------------------------------------------------


def _read_xlsx_to_pandas(xlsx_path: Path):
    """Read an Outpatient PUF XLSX (NBER CY2013 era) and locate the header
    row by scanning the first ~30 rows for a cell whose normalized text is
    in the APC alias set. Returns a pandas DataFrame whose column names are
    the upstream header strings (preserving double-space artifacts so that
    ``_resolve_columns`` collapses them on its own).
    """
    import pandas as pd
    from openpyxl import load_workbook

    apc_aliases_norm = {_norm_header(a) for a in _COLUMN_ALIASES["apc_cd"]}
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active  # NBER files ship a single worksheet; safe for CY2013
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    header_idx: int | None = None
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        first = row[0]
        if first is None:
            continue
        if _norm_header(first) in apc_aliases_norm:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError(
            f"puf_outpatient: could not find APC header row in {xlsx_path.name} "
            f"(scanned {min(30, len(rows))} rows)"
        )
    header = [str(c) if c is not None else "" for c in rows[header_idx]]
    body = [r for r in rows[header_idx + 1 :] if any(c is not None for c in r)]
    # Pad / truncate body rows to header width so DataFrame construction
    # never raises on a ragged trailing row.
    width = len(header)
    body = [list(r[:width]) + [None] * (width - len(r[:width])) for r in body]
    return pd.DataFrame(body, columns=header)


def _aggregate_per_apc(bronze_path: Path, cy: int) -> "duckdb.DuckDBPyRelation":
    """Aggregate per (APC_Cd, cy). Routes by file extension: ``.xlsx`` is
    parsed via openpyxl into pandas then registered into DuckDB; everything
    else goes through ``read_csv_auto``.

    Returns a pandas DataFrame ordered by code with the columns:

        code, n, mean_charge, payment_amount, mean_payment,
        p10_charge .. p90_charge, p10_payment .. p90_payment, n_providers
    """
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    is_xlsx = bronze_path.suffix.lower() == ".xlsx"
    if is_xlsx:
        # XLSX path (NBER CY2013): read via openpyxl, register with DuckDB.
        df_in = _read_xlsx_to_pandas(bronze_path)
        headers = list(df_in.columns)
        # All cells coerced to string so the same TRY_CAST/REGEXP_EXTRACT
        # SQL works uniformly with the CSV path.
        df_in = df_in.astype("string")
        con.register("src_raw", df_in)
        from_clause = "src_raw"
        bind_args: list = []
    else:
        # CSV path (CMS CY2015+, NBER CY2011/CY2012): probe header + stream.
        head_rel = con.execute(
            "SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE) LIMIT 0",
            [str(bronze_path)],
        )
        headers = [d[0] for d in head_rel.description]
        from_clause = "read_csv_auto(?, ALL_VARCHAR=TRUE)"
        bind_args = [str(bronze_path)]

    cols = _resolve_columns(headers)

    apc = cols["apc_cd"]
    pid = cols["provider_id"]
    svc = cols["tot_srvcs"]
    chrg = cols["avg_submtd_chrg"]
    pmt = cols["avg_mdcr_pymt_amt"]
    # Allowed amount is optional (NBER pre-CY2015 era); fall back to the
    # payment column so ``mean_allowed`` is populated rather than NULL.
    alwd = cols.get("avg_mdcr_alowd_amt", pmt)

    # Use double-quotes for column names so CMS-style mixed-case identifiers
    # (e.g. Avg_Mdcr_Pymt_Amt) AND space-separated NBER headers (e.g.
    # "Average Total Payments") bind correctly.
    sql = f"""
        WITH src AS (
            SELECT * FROM {from_clause}
        ),
        typed AS (
            SELECT
                UPPER(LPAD(REGEXP_EXTRACT("{apc}", '^([0-9]+)'), 4, '0')) AS code,
                CAST("{pid}" AS VARCHAR) AS provider_id,
                TRY_CAST(REPLACE(CAST("{svc}" AS VARCHAR), ',', '') AS DOUBLE) AS tot_srvcs,
                TRY_CAST(REPLACE(REPLACE(CAST("{chrg}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_submtd_chrg,
                TRY_CAST(REPLACE(REPLACE(CAST("{alwd}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_mdcr_alowd_amt,
                TRY_CAST(REPLACE(REPLACE(CAST("{pmt}" AS VARCHAR), ',', ''), '$', '') AS DOUBLE) AS avg_mdcr_pymt_amt
            FROM src
            WHERE REGEXP_MATCHES(CAST("{apc}" AS VARCHAR), '^[0-9]+')
        ),
        typed_pos AS (
            SELECT *
            FROM typed
            WHERE tot_srvcs IS NOT NULL AND tot_srvcs > 0
        )
        SELECT
            code,
            CAST(SUM(tot_srvcs) AS BIGINT) AS n,
            SUM(avg_submtd_chrg * tot_srvcs)
                / NULLIF(SUM(CASE WHEN avg_submtd_chrg IS NOT NULL THEN tot_srvcs ELSE 0 END), 0) AS mean_charge,
            SUM(avg_mdcr_alowd_amt * tot_srvcs)
                / NULLIF(SUM(CASE WHEN avg_mdcr_alowd_amt IS NOT NULL THEN tot_srvcs ELSE 0 END), 0) AS payment_amount,
            SUM(avg_mdcr_pymt_amt * tot_srvcs)
                / NULLIF(SUM(CASE WHEN avg_mdcr_pymt_amt IS NOT NULL THEN tot_srvcs ELSE 0 END), 0) AS mean_payment,
            QUANTILE_CONT(avg_submtd_chrg, 0.10) AS p10_charge,
            QUANTILE_CONT(avg_submtd_chrg, 0.25) AS p25_charge,
            QUANTILE_CONT(avg_submtd_chrg, 0.50) AS p50_charge,
            QUANTILE_CONT(avg_submtd_chrg, 0.75) AS p75_charge,
            QUANTILE_CONT(avg_submtd_chrg, 0.90) AS p90_charge,
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
    df = con.execute(sql, bind_args).df()
    con.close()

    # Drop rows whose normalized APC code doesn't match the post-pad regex
    # (footer / malformed). Mirrors src/extract/opps.py convention.
    df = df[df["code"].str.match(_APC_CODE_RE.pattern, na=False)].reset_index(drop=True)
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


# Re-exported from the shared resolver helper (Session 8.5 consolidation).
_VALID_SOURCE_HOSTS: frozenset[str] = VALID_SOURCE_HOSTS


def _df_to_table(
    df,
    *,
    cy: int,
    source_url: str,
    source_host: str,
) -> pa.Table:
    """Wrap the per-APC aggregate DataFrame in a pyarrow Table matching the
    locked schema. Adds the constant columns (scope, code_system, year,
    source, source_host, ...).
    """
    if source_host not in _VALID_SOURCE_HOSTS:
        raise ValueError(
            f"puf_outpatient: source_host {source_host!r} not in "
            f"{sorted(_VALID_SOURCE_HOSTS)}"
        )
    n_rows = len(df)
    eff_start, eff_end = config.cy_interval(cy)

    # Concept assignment (Session 8.5 schema delta):
    #   * cms_gov (CY2015+) -> Avg_Mdcr_Alowd_Amt -> medicare_allowed;
    #     total_payment_amount IS NULL.
    #   * nber (CY2011-CY2013) -> Average Total Payments ->
    #     total_payment_allpayer; total_payment_amount = payment_amount
    #     (self-documenting alias for inpatient consumers).
    #   * wayback -> raise: CY2014 is currently unrecoverable; if a
    #     Wayback snapshot ever resurfaces, we must decide concept based
    #     on the underlying CMS column at that point.
    payment_amount_vals = df["payment_amount"].tolist()
    if source_host == "cms_gov":
        concept = "medicare_allowed"
        total_payment_vals: list = [None] * n_rows
    elif source_host == "nber":
        concept = "total_payment_allpayer"
        total_payment_vals = list(payment_amount_vals)
    else:
        raise RuntimeError(
            f"puf_outpatient: unsupported source_host {source_host!r} for "
            "concept derivation (CY2014 Wayback path is currently unwired; "
            "decide concept based on underlying CMS column when re-resolved)"
        )

    arrays = [
        pa.array(["national"] * n_rows, type=pa.string()),
        pa.array(df["code"].tolist(), type=pa.string()),
        pa.array(["APC"] * n_rows, type=pa.string()),
        pa.array([cy] * n_rows, type=pa.int32()),
        pa.array([None] * n_rows, type=pa.string()),  # place_of_srvc always NULL
        pa.array(df["n"].astype("Int64").tolist(), type=pa.int64()),
        pa.array(df["mean_charge"].tolist(), type=pa.float64()),
        pa.array(df["mean_payment"].tolist(), type=pa.float64()),
        pa.array(payment_amount_vals, type=pa.float64()),
        pa.array([concept] * n_rows, type=pa.string()),
        pa.array(total_payment_vals, type=pa.float64()),
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
        pa.array([_BRONZE_SOURCE] * n_rows, type=pa.string()),
        pa.array([source_host] * n_rows, type=pa.string()),
        pa.array([eff_start] * n_rows, type=pa.date32()),
        pa.array([eff_end] * n_rows, type=pa.date32()),
    ]
    return pa.Table.from_arrays(arrays, schema=_PARQUET_SCHEMA)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _silver_path(cy: int) -> Path:
    out = config.SILVER_PER_SOURCE
    for part in _SILVER_SUBDIR:
        out = out / part
    return out / f"puf_outpatient_cy{cy}.parquet"


def extract_cy(
    cy: int,
    *,
    download_date: str | None = None,
    session: requests.Session | None = None,
) -> Path | None:
    """Extract the Outpatient PUF for a single CY -> per-CY parquet.

    Returns the path to the silver parquet on success, or ``None`` if the
    upstream URL 404s (log+skip semantics, mirrors HCPCS / OPPS).
    """
    resolved = _resolve_outpatient_url(cy)
    if resolved is None:
        print(
            f"[puf_outpatient] CY{cy} unrecoverable on cms.gov / NBER / Wayback; skipping",
            file=sys.stderr,
        )
        return None
    url, source_host = resolved
    filename = Path(url).name or f"puf_outpatient_cy{cy}.csv"
    sess = session or _http.make_session()
    try:
        bronze_path = _http.download_to_bronze(
            url,
            source=_BRONZE_SOURCE,
            fy=cy,
            filename=filename,
            download_date=download_date,
            session=sess,
            period="cy",
        )
    except requests.HTTPError as exc:
        print(
            f"[puf_outpatient] HTTPError for CY{cy} ({url}): {exc}; skipping",
            file=sys.stderr,
        )
        return None

    print(
        f"[puf_outpatient] aggregating CY{cy} from {bronze_path} (source_host={source_host})",
        file=sys.stderr,
    )
    df = _aggregate_per_apc(bronze_path, cy)
    # Drop rows with null payment_amount pre-emit. Schema requires
    # non-null payment_amount (Session 8.5 contract); for CMS-era CY2015+
    # Avg_Mdcr_Alowd_Amt is populated for the vast majority of rows but a
    # small minority can be null. Mirrors the inpatient_summary approach.
    pre_drop = len(df)
    df = df[df["payment_amount"].notna()].reset_index(drop=True)
    dropped = pre_drop - len(df)
    if dropped:
        print(
            f"[puf_outpatient] CY{cy} dropped {dropped} APC rows with null "
            "payment_amount (Session 8.5 non-null contract)",
            file=sys.stderr,
        )

    apc_count = len(df)
    lo, hi = _APC_COUNT_BAND
    if not (lo <= apc_count <= hi):
        raise RuntimeError(
            f"APC count {apc_count} for CY{cy} outside expected band "
            f"[{lo}, {hi}] -- likely PUF schema drift"
        )

    df = _mask_quantiles_low_n_providers(df)
    table = _df_to_table(df, cy=cy, source_url=url, source_host=source_host)

    out = _silver_path(cy)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[puf_outpatient] wrote {len(df)} APC rows to {out}",
        file=sys.stderr,
    )
    return out


def extract_cy_range(
    start_cy: int,
    end_cy: int,
    *,
    download_date: str | None = None,
) -> list[Path]:
    """Bulk-extract a CY range with log+skip semantics on per-cell failures."""
    sess = _http.make_session()
    out_paths: list[Path] = []
    for cy in range(start_cy, end_cy + 1):
        try:
            p = extract_cy(cy, download_date=download_date, session=sess)
            if p is not None:
                out_paths.append(p)
        except Exception as exc:
            print(
                f"[puf_outpatient] CY{cy} failed: {exc}; continuing",
                file=sys.stderr,
            )
    return out_paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_cy_range(value: str) -> tuple[int, int]:
    try:
        a, b = value.split("-", 1)
        start, end = int(a), int(b)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            f"--cy-range must be 'START-END' (e.g. '2015-2023'), got {value!r}"
        ) from exc
    if start > end:
        raise argparse.ArgumentTypeError(
            f"--cy-range start ({start}) must be <= end ({end})"
        )
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.puf_outpatient",
        description=(
            "Download and aggregate the Medicare Outpatient PUF (per-provider, "
            "per-APC) for one calendar year or a bulk range, writing per-CY "
            "parquet to data/silver/per_source/puf/outpatient/."
        ),
    )
    cy_group = parser.add_mutually_exclusive_group(required=True)
    cy_group.add_argument(
        "--cy",
        type=int,
        help="Single calendar year (e.g., 2023).",
    )
    cy_group.add_argument(
        "--cy-range",
        type=_parse_cy_range,
        metavar="START-END",
        help="Inclusive CY range, e.g., '2015-2023'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.cy is not None:
        extract_cy(args.cy, download_date=args.download_date)
    else:
        start, end = args.cy_range
        extract_cy_range(start, end, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
