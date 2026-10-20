"""HCRIS provider-level data extractor (FY2015+).

Downloads the per-FY ``*-hcris-data-file.zip`` from cms.gov (each ~24-26 MB,
12 FYs FY2015-FY2026), unpacks the bundled SAS dataset
(``prds_hosp10_yr{YYYY-2}.sas7bdat``, ~50-65 MB uncompressed), and emits a
trimmed per-FY parquet under
``data/silver/per_source/hcris_data_fy{fy}.parquet``.

The full SAS file has ~5,600 columns (one per Worksheet/Part/Column/Line cell).
We retain only:

  * Identifier columns: ``rpt_rec_num``, ``prvdr_num``, ``rpt_stus_cd``
  * All ``C_1_C5_*`` (cost cells, Worksheet C Part 1 Col 5)
  * All ``C_1_C6_*`` (inpatient charge cells, Col 6)
  * All ``C_1_C7_*`` (total charge cells, Col 7)
  * All ``D3_HOS_C2_*`` (Medicare charge cells, Worksheet D-3)

This still yields ~1,000 columns per FY but is ~25% the size of the full
matrix. Aggregation downstream (``src/transform/hcris_ccrs.py``) reads this
parquet, joins to ``hcris_cost_centers.parquet`` to resolve cell refs, and
computes per-cost-center national CCRs.

FY2008-FY2014 NBER fallback: the NBER mirror carries an equivalent
``hosp_nmrc2552_*_{YYYY}_long.csv`` long-form cell matrix, ~250-800 MB per FY
(~4 GB total). Phase 11 / Session 12 (2026-05-02) ships the pivot in
:mod:`src.extract.hcris_data_nber`. This module's ``extract_fy`` and
``extract_fy_range`` now delegate to that module for any FY ≤ 2014, so callers
can use a single FY-range CLI invocation
(``python -m src.extract.hcris_data --fy-range 2008-2026``) to cover both eras.
See ``plans/hcris-url-discovery.md`` "Session 9.5 NBER pre-check" for the
per-FY size manifest.

CLI:

    python -m src.extract.hcris_data --fy 2026
    python -m src.extract.hcris_data --fy-range 2015-2026
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "hcris_data"
_SOURCE_TAG: str = "hcris_data"

# Per-FY URLs verified 2026-05-01 (see plans/hcris-url-discovery.md).
_HCRIS_DATA_URLS: dict[int, str] = {
    2026: "https://www.cms.gov/files/zip/fy2026-ipps-fr-hcris-data-file.zip",
    2025: "https://www.cms.gov/files/zip/fy-2025-ipps-final-hcris-data-file.zip",
    2024: "https://www.cms.gov/files/zip/fy2024-ipps-fr-hcris-data-file.zip",
    2023: "https://www.cms.gov/files/zip/fy2023-ipps-fr-hcris-data-file.zip",
    2022: "https://www.cms.gov/files/zip/fy-2022-ipps-fr-hcris-data-file.zip",
    2021: "https://www.cms.gov/files/zip/fy-2021-ipps-fr-hcris-data-file.zip",
    2020: "https://www.cms.gov/files/zip/fy-2020-final-rule-hcris-data-file.zip",
    2019: "https://www.cms.gov/files/zip/fy-2019-final-rule-hcris-data-file.zip",
    2018: "https://www.cms.gov/files/zip/fy-2018-final-rule-hcris-data-file.zip",
    2017: "https://www.cms.gov/files/zip/fy-2017-final-rule-hcris-data-file.zip",
    2016: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/downloads/fy2016-cms-1632-fr-hcris.zip",
    2015: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/downloads/fy2015-fr-hcris-data-file.zip",
}

# Sub-flag 1b sniff: provider-level SAS dataset ZIPs are >= 1 MB; the
# similarly-named cost-center supplemental ZIP is < 100 KB. Refuse to parse
# something that looks like the wrong artifact.
_PROVIDER_DATA_MIN_BYTES: int = 1_000_000

# Identifier columns we always keep.
_ID_COLS: tuple[str, ...] = (
    "rpt_rec_num",
    "prvdr_num",
    "rpt_stus_cd",
    "fy_bgn_dt",
    "fy_end_dt",
)

# Cell-column prefixes we retain (drops most of the 5,600-column matrix).
_KEEP_PREFIXES: tuple[str, ...] = (
    "C_1_C5_",   # Worksheet C Part 1 Col 5: cost
    "C_1_C6_",   # Worksheet C Part 1 Col 6: inpatient charges
    "C_1_C7_",   # Worksheet C Part 1 Col 7: total charges
    "D3_HOS_C2_",  # Worksheet D-3 Col 2: Medicare charges
)


def _find_sas7bdat(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        for n in zf.namelist():
            if n.lower().endswith(".sas7bdat"):
                return n
    raise RuntimeError(f"No .sas7bdat member in {zip_path}")


def _unpack_sas(zip_path: Path) -> Path:
    """Unpack the SAS dataset next to the bronze ZIP. Idempotent."""
    member = _find_sas7bdat(zip_path)
    out = zip_path.parent / Path(member).name
    if out.exists() and out.stat().st_size > 0:
        return out
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as fh:
        out.write_bytes(fh.read())
    return out


def _read_trimmed(sas_path: Path) -> pd.DataFrame:
    """Load only the kept columns from the SAS dataset.

    pyreadstat opens the file once to enumerate columns, then re-reads with
    ``usecols=`` to skip the ~4,500 cells we don't need.
    """
    import pyreadstat

    # First pass: column enumeration. metadataonly=True is fast (no data read).
    _, meta = pyreadstat.read_sas7bdat(str(sas_path), metadataonly=True)
    all_cols = list(meta.column_names)
    # Lowercase comparison since column case can drift across SAS releases.
    keep = [
        c
        for c in all_cols
        if c in _ID_COLS or any(c.startswith(p) for p in _KEEP_PREFIXES)
    ]
    if not keep:
        raise RuntimeError(
            f"No keep-prefixed columns found in {sas_path}; sample first 20 "
            f"columns: {all_cols[:20]}"
        )
    df, _ = pyreadstat.read_sas7bdat(str(sas_path), usecols=keep)
    return df


def _df_to_table(df: pd.DataFrame, fy: int, source_url: str) -> pa.Table:
    """Convert the trimmed pandas frame to a parquet-friendly Arrow table.

    Adds the constant columns ``source``, ``fy``, ``effective_start``,
    ``effective_end``. Coerces id columns to string and cell values to
    float64 so types are consistent across FYs.
    """
    eff_start, eff_end = config.fy_interval(fy)

    # Identifier coercion. SAS may store provider numbers as strings or floats
    # depending on release; lock to string.
    id_present = [c for c in _ID_COLS if c in df.columns]
    for c in id_present:
        if df[c].dtype.kind in ("f",):
            df[c] = df[c].astype("Int64").astype("string")
        else:
            df[c] = df[c].astype("string")

    # All other columns -> float64 (NaN for missing).
    cell_cols = [c for c in df.columns if c not in id_present]
    for c in cell_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    n = len(df)
    df = df.assign(
        fy=fy,
        source=_SOURCE_TAG,
        effective_start=eff_start,
        effective_end=eff_end,
    )
    # Reorder: ids, then cells, then trailing constants.
    front = id_present + ["fy", "source", "effective_start", "effective_end"]
    ordered = front + cell_cols
    df = df[ordered]

    # Build the schema explicitly so date columns come through as date32.
    fields = []
    for c in ordered:
        if c in id_present + ["source"]:
            fields.append(pa.field(c, pa.string(), nullable=True))
        elif c == "fy":
            fields.append(pa.field(c, pa.int32(), nullable=False))
        elif c in ("effective_start", "effective_end"):
            fields.append(pa.field(c, pa.date32(), nullable=False))
        else:
            fields.append(pa.field(c, pa.float64(), nullable=True))
    schema = pa.schema(fields)

    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def extract_fy(
    fy: int,
    *,
    download_date: str | None = None,
    session: requests.Session | None = None,
) -> Path | None:
    """Download + parse provider-level HCRIS for a single FY.

    Returns the path to the per-FY parquet, or ``None`` if the URL is missing
    / 404s.

    FY ≤ 2014 delegates to :mod:`src.extract.hcris_data_nber` (NBER long-form
    CSV path); the SAS-form provider-level dataset only exists FY2015+ on
    cms.gov.
    """
    if fy <= 2014:
        from src.extract import hcris_data_nber

        return hcris_data_nber.extract_fy(
            fy, download_date=download_date, session=session
        )

    url = _HCRIS_DATA_URLS.get(fy)
    if not url:
        print(
            f"[hcris_data] FY{fy} not in URL manifest "
            "(provider-level HCRIS only published FY2015+); skipping",
            file=sys.stderr,
        )
        return None

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
            f"[hcris_data] HTTPError for FY{fy}: {exc}; skipping",
            file=sys.stderr,
        )
        return None

    sz = bronze.stat().st_size
    if sz < _PROVIDER_DATA_MIN_BYTES:
        raise RuntimeError(
            f"FY{fy} bronze file {bronze.name} is {sz:,} bytes (< "
            f"{_PROVIDER_DATA_MIN_BYTES:,}); looks like the cost-center "
            "definition file, not the provider-level HCRIS data file. URL drift?"
        )

    sas_path = _unpack_sas(bronze)
    print(
        f"[hcris_data] FY{fy}: parsing {sas_path.name} ({sas_path.stat().st_size:,} bytes)",
        file=sys.stderr,
    )
    df = _read_trimmed(sas_path)
    table = _df_to_table(df, fy, url)

    # Per-FY parquet under per_source/hcris/data/. Subdir layout mirrors the
    # Phase 5 PUF convention so the codes_master non-recursive glob
    # (`per_source/*.parquet`) does not pick HCRIS files up.
    out = config.SILVER_PER_SOURCE / "hcris" / "data" / f"hcris_data_fy{fy}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[hcris_data] FY{fy}: {len(df)} provider rows × {len(df.columns)} cols -> {out}",
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
                f"[hcris_data] FY{fy} failed: {exc}; continuing",
                file=sys.stderr,
            )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.hcris_data",
        description=(
            "Download and parse the IPPS HCRIS provider-level data file "
            "(SAS7BDAT, ~24-26 MB ZIP × 12 FYs FY2015-FY2026)."
        ),
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--fy", type=int)
    grp.add_argument("--fy-range", type=str, metavar="START-END")
    parser.add_argument("--download-date", default=None)
    args = parser.parse_args(argv)

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
