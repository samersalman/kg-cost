"""ICD-9-CM Volume 3 (procedure) extractor: CMS legacy ZIPs -> per-FY parquet.

ICD-9-CM Volume 3 (a.k.a. ICD-9-PCS in older literature, but distinct from
ICD-10-PCS) carries inpatient procedure codes for the FY2008-FY2015 era. CMS
ships the procedure-side flat files inside the *same* per-FY ZIPs as ICD-9-CM
diagnoses; this module reuses the shared helpers in :mod:`src.extract.icd9cm`
to download bronze and parse the procedure-side member out of each ZIP.

Bronze reuse: ``config.ICD9PCS_SOURCES = dict(ICD9CM_SOURCES)``, so the
per-FY ZIPs are byte-identical to those the ICD-9-CM extractor pulls. This
module deliberately does NOT write a separate ``data/bronze/icd9pcs/...``
tree; it calls :func:`src.extract.icd9cm.download_bronze` and reads
straight from the ``data/bronze/icd9cm/fy{fy}/{download_date}/`` paths the
sibling extractor already produces (or produces on demand if not yet
populated). This halves the bronze footprint for FY2008-FY2015.

Per-era file naming inside the per-FY ZIP:

    - FY2008-FY2009 (v25/v26): single ``I9proceduresVnn.txt`` /
      ``Vnn  I-9 Procedures.txt`` (short titles only).
    - FY2010 (v27): ``CMS27_DESC_SHORT_SG.txt`` (short only) plus optional
      supplemental long-titles CSV ``CMS27_DESC_LONG_SHORT_SG_*.csv``.
    - FY2011-FY2015 (v28-v32): paired ``CMSnn_DESC_LONG_SG.txt`` and
      ``CMSnn_DESC_SHORT_SG.txt``.

ICD-9-Vol3 codes are 3-4 numeric characters (no alpha prefix). ``poa_exempt``
is always NULL for Vol3 (POA is a diagnosis-only flag).

CLI::

    python -m src.extract.icd9pcs --fy 2014
    python -m src.extract.icd9pcs --fy-range 2008-2015
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http, icd9cm

_CODE_SYSTEM: str = "ICD-9-Vol3"


def extract_fy(
    fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Extract ICD-9-CM Volume 3 (procedure codes) for one FY -> parquet.

    Delegates bronze acquisition to :func:`src.extract.icd9cm.download_bronze`
    so the per-FY ZIP is fetched (or reused) under
    ``data/bronze/icd9cm/fy{fy}/{download_date}/`` rather than duplicated
    into a parallel ``data/bronze/icd9pcs/...`` tree.
    """
    sess = session or _http.make_session()

    if fy not in icd9cm.PER_FY_URLS:
        raise KeyError(
            f"icd9pcs: FY{fy} not in supported range; known FYs: "
            f"{sorted(icd9cm.PER_FY_URLS)}"
        )

    # ICD-9 procedure flat files ship inside the very same per-FY ZIP as
    # diagnoses (config.ICD9PCS_SOURCES == config.ICD9CM_SOURCES). Reuse the
    # icd9cm bronze pull instead of writing a duplicate icd9pcs tree.
    bronze = icd9cm.download_bronze(fy, session=sess, download_date=download_date)

    rows, source_url = icd9cm.extract_rows(fy, want="sg", bronze_paths=bronze)
    if not rows:
        raise RuntimeError(
            f"icd9pcs fy{fy}: zero procedure rows extracted; parser regression"
        )
    table = icd9cm.build_table(
        rows,
        fy=fy,
        code_system=_CODE_SYSTEM,
        source_url=source_url,
    )

    out_dir = output_dir if output_dir is not None else config.SILVER_PER_SOURCE
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"icd9pcs_fy{fy}.parquet"
    pq.write_table(table, out_path)
    print(
        f"[icd9pcs] fy{fy}: wrote {table.num_rows} rows to {out_path}",
        file=sys.stderr,
    )
    return out_path


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_fy`` over inclusive ``[start_fy, end_fy]``.

    Per-FY failures are logged and skipped. Returns the list of successful
    parquet paths.
    """
    if end_fy < start_fy:
        raise ValueError(f"end_fy ({end_fy}) must be >= start_fy ({start_fy})")
    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for fy in range(start_fy, end_fy + 1):
        try:
            out.append(extract_fy(fy, session=sess, download_date=download_date))
        except Exception as exc:  # noqa: BLE001
            print(
                f"[icd9pcs] FY{fy} FAILED: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((fy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[icd9pcs] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for fy, msg in failures:
            print(f"  FY{fy}: {msg}", file=sys.stderr)
    print(
        f"[icd9pcs] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
        file=sys.stderr,
    )
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_fy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range expects 'START-END' (e.g., 2008-2015), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.icd9pcs",
        description=(
            "Download and parse CMS ICD-9-CM Volume 3 (procedure) code-title "
            "ZIPs for one or more fiscal years; emits "
            "data/silver/per_source/icd9pcs_fy{FY}.parquet."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fy", type=int, help="Single fiscal year (e.g., 2014).")
    group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        metavar="START-END",
        help="Inclusive FY range, e.g., '2008-2015'.",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    session = _http.make_session()
    if args.fy is not None:
        extract_fy(args.fy, session=session, download_date=args.download_date)
    else:
        start_fy, end_fy = args.fy_range
        extract_fy_range(
            start_fy, end_fy, session=session, download_date=args.download_date
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
