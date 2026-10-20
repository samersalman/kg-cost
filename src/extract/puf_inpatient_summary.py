"""Medicare Inpatient PUF *National Summary* extractor (Phase 5 Batch 2b).

Per-FY aggregate XLSX from the **NBER mirror** of the retired CMS
Medicare-Provider-Charge-Data page. URL pattern verified 2026-05-01
(HEAD = 200 on all three FYs):
``data.nber.org/providerchargepuf/{YYYY}/{basename}_XLSX.zip``
where ``basename`` is ``IPPS_NAT_STATE_SUM`` for FY2011 and
``Inpatient_Summary_{YYYY}`` for FY2012 / FY2013. NBER mirrors the CMS
ZIPs verbatim; the inner XLSX member is the canonical
``Medicare_Charge_Inpatient_DRG100_DRG_Summary_Reports_FY{YYYY}.xlsx`` in
all three cases. Note: the original CMS Downloads/ URLs (which used the
``Inpatient_Summary_{YYYY}_XLSX.zip`` slug for ALL three FYs) are retired
as of 2026-05-01.

**Plan-v10.md scope deviation**: the FY2011-FY2013 archives ship XLSXs named
``Medicare_Charge_Inpatient_DRG100_..._FY{YYYY}.xlsx`` with ~100 by-DRG rows
each — i.e. they are ALSO top-100 only, mirroring the per-provider PUF.
The all-DRG variant (``..._DRGALL_..._FY{YYYY}.xlsx``) starts FY2014. So this
extractor does NOT actually backfill the ~650 missing DRGs as plan-v10.md
premised; it provides a national-aggregate cross-check at top-100 cardinality.
The row-count test threshold is ``>= 80`` (empirical FY2013 ~106) instead of
plan-v10.md's unmeetable ``>= 600``.

Schema is a sibling of ``puf_inpatient`` (Batch 2a): 27 columns (Session 8.5
schema rename dropped ``mean_allowed`` and added ``payment_amount`` +
``payment_amount_concept`` + ``total_payment_amount``) with percentiles +
``n_providers`` set NULL, ``source = "puf_top100_summary"``,
``source_host = "nber"``, ``code_system = "MS-DRG"``, ``scope = "national"``,
``code`` zero-padded to 3 digits. Every row carries
``payment_amount_concept = "total_payment_allpayer"`` because the source
column is "Average Total Payments" (all-payer total, NOT Medicare-only
allowed). Silver lands at ``data/silver/per_source/puf/inpatient_summary/
puf_inpatient_summary_fy{fy}.parquet`` — the two-level subdir keeps DuckDB's
non-recursive ``codes_master`` glob from picking these up.

CLI::

    python -m src.extract.puf_inpatient_summary --fy 2013
    python -m src.extract.puf_inpatient_summary --fy-range 2011-2013
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http

_BRONZE_SOURCE: str = "puf_inpatient_summary"
# Silver-layer source-tag (distinct from the bronze tree path's literal).
_SILVER_SOURCE_TAG: str = "puf_top100_summary"
# Silver-layer host-tag (this extractor sources from NBER's mirror, not CMS).
_SILVER_SOURCE_HOST: str = "nber"

# Module-local URL dict (main agent will wire to config.PUF_INPATIENT_SUMMARY_SOURCES).
# NBER mirror (data.nber.org/providerchargepuf/) — the original CMS
# Downloads/ URLs are retired as of 2026-05-01. NBER mirrors the CMS ZIPs
# verbatim; FY2011 uses the legacy ``IPPS_NAT_STATE_SUM_XLSX.zip`` basename
# while FY2012 / FY2013 use ``Inpatient_Summary_{YYYY}_XLSX.zip``. All three
# inner XLSX members are the canonical
# ``Medicare_Charge_Inpatient_DRG100_DRG_Summary_Reports_FY{YYYY}.xlsx``.
_PUF_INPATIENT_SUMMARY_SOURCES: dict[int, str] = {
    2011: (
        "https://data.nber.org/providerchargepuf/2011/"
        "IPPS_NAT_STATE_SUM_XLSX.zip"
    ),
    2012: (
        "https://data.nber.org/providerchargepuf/2012/"
        "Inpatient_Summary_2012_XLSX.zip"
    ),
    2013: (
        "https://data.nber.org/providerchargepuf/2013/"
        "Inpatient_Summary_2013_XLSX.zip"
    ),
}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "drg": ("DRG DEFINITION", "DRG_DEFINITION", "MS-DRG DEFINITION", "DRG",
            "MS-DRG", "MS_DRG", "DRG_CD"),
    "n": ("TOTAL DISCHARGES", "TOT_DSCHRGS", "N_DISCHARGES", "DISCHARGES",
          "TOTAL_DISCHARGES"),
    "mean_charge": ("AVERAGE COVERED CHARGES", "MEAN COVERED CHARGES",
                    "AVG SUBMITTED CHARGES", "AVG SUBMTD CHRG",
                    "MEAN SUBMITTED CHARGES", "AVG_SUBMTD_CVRD_CHRG"),
    "payment_amount": ("AVERAGE TOTAL PAYMENTS", "MEAN TOTAL PAYMENTS",
                       "AVG TOT PYMT AMT", "AVG_TOT_PYMT_AMT"),
    "mean_payment": ("AVERAGE MEDICARE PAYMENTS", "MEAN MEDICARE PAYMENTS",
                     "AVG MDCR PYMT AMT", "AVG_MDCR_PYMT_AMT",
                     "MEAN MEDICARE PAYMENT", "AVERAGE MEDICARE PAYMENT"),
}
# `mean_payment` is optional: NBER's FY2011 ZIP ships only "Average Total
# Payments" (no separate Medicare Payments column), so FY2011 silver rows
# carry `payment_amount` (concept = `total_payment_allpayer`) populated AND
# `total_payment_amount` populated, but `mean_payment` IS NULL by design.
# FY2012/FY2013 ship both.
_REQUIRED_LOGICAL: tuple[str, ...] = ("drg", "n", "mean_charge")
_HEADER_PRIMARY_ALIASES: frozenset[str] = frozenset(
    a.upper() for a in _HEADER_ALIASES["drg"]
)
_DRG_PREFIX_RE = re.compile(r"^\s*([0-9]{3})\s*[-:.]?\s*(.+)?$")


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).upper()


def _resolve_columns(header: list[object]) -> dict[str, int]:
    norm = [_normalize_header(h) for h in header]
    out: dict[str, int] = {}
    for logical, alts in _HEADER_ALIASES.items():
        for alt in alts:
            try:
                out[logical] = norm.index(alt.upper())
                break
            except ValueError:
                continue
    missing = [k for k in _REQUIRED_LOGICAL if k not in out]
    if missing:
        raise RuntimeError(
            f"puf_inpatient_summary: required columns {missing} not found; "
            f"saw {norm[:20]}..."
        )
    return out


def _find_header_index(rows: list[list[object]]) -> int:
    """Mirror src/extract/opps.py:_find_header_index — scan first 30 rows."""
    for i, row in enumerate(rows[:30]):
        if not row:
            continue
        for cell in row:
            norm = _normalize_header(cell)
            if not norm:
                continue
            if norm in _HEADER_PRIMARY_ALIASES:
                return i
            break
    sampled = [(_normalize_header(r[0]) if r else "") for r in rows[:30]]
    raise RuntimeError(
        f"puf_inpatient_summary: header row not found in first 30 rows; "
        f"first-cells: {sampled}"
    )


def _parse_drg_code(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = _DRG_PREFIX_RE.match(s)
    if m:
        return m.group(1)
    if s.isdigit():
        return s.zfill(3)[:3]
    return None


def _parse_money(value: object) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.upper() in {"NA", "N/A", "NULL", "--", "-", "."}:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _parse_int(value: object) -> int | None:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if not s or s.upper() in {"NA", "N/A", "NULL", "--", "-", "."}:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _open_zip(path: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(
            f"puf_inpatient_summary: corrupt ZIP at {path}: {exc}"
        ) from exc


def _find_xlsx_member(zf: zipfile.ZipFile) -> str:
    members = [m for m in zf.namelist() if m.lower().endswith(".xlsx")]
    if not members:
        raise RuntimeError(
            f"puf_inpatient_summary: no .xlsx in ZIP (saw {zf.namelist()})"
        )
    preferred = [m for m in members if "drg" in Path(m).name.lower()]
    return (preferred or members)[0]


def _extract_member(zf: zipfile.ZipFile, member: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zf.open(member, "r") as src, dest.open("wb") as out:
        while chunk := src.read(64 * 1024):
            out.write(chunk)
    return dest


def _pick_by_drg_sheet(path: Path) -> str | None:
    """Pick the ``Averages_by_DRG`` worksheet. Mirrors ipps._pick_fr_sheet."""
    try:
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    except Exception:  # noqa: BLE001
        return None
    try:
        names = list(wb.sheetnames)
    finally:
        wb.close()
    if len(names) <= 1:
        return None
    by_drg = [n for n in names if "by_drg" in n.lower().replace(" ", "_")]
    no_state = [n for n in by_drg if "state" not in n.lower()]
    return (no_state or by_drg or [None])[0]


def _read_xlsx_rows(
    path: Path, *, sheet_name: str | None = None
) -> list[list[object]]:
    try:
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"puf_inpatient_summary: failed to load XLSX {path}: {exc}"
        ) from exc
    try:
        if sheet_name is not None:
            if sheet_name not in wb.sheetnames:
                raise RuntimeError(
                    f"puf_inpatient_summary: sheet {sheet_name!r} not in "
                    f"{path} (saw {wb.sheetnames})"
                )
            ws = wb[sheet_name]
        else:
            ws = wb.active
        return [list(t) for t in ws.iter_rows(values_only=True)]
    finally:
        wb.close()


@dataclass(frozen=True)
class _ParsedRow:
    code: str
    n: int
    mean_charge: float | None
    mean_payment: float | None
    payment_amount: float | None


# Locked schema (27 cols) — sibling of puf_inpatient (Batch 2a). Session 8.5
# rename: ``mean_allowed`` dropped; ``payment_amount`` +
# ``payment_amount_concept`` + ``total_payment_amount`` added (DEV 4 fix
# from `plans/Session 8.5 flags.md`). Every row this extractor emits has
# concept = "total_payment_allpayer" because the source column is
# "Average Total Payments" (all-payer total, NOT Medicare-only allowed).
# Percentile and n_providers fields are nullable because the National
# Summary aggregate has no per-provider distribution by design.
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
        *(pa.field(f"p{p}_charge", pa.float64(), nullable=True)
          for p in (10, 25, 50, 75, 90)),
        *(pa.field(f"p{p}_payment", pa.float64(), nullable=True)
          for p in (10, 25, 50, 75, 90)),
        pa.field("n_providers", pa.int32(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("source_host", pa.string(), nullable=False),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
    ]
)


def _build_table(
    rows: list[_ParsedRow], *, fy: int, eff_start: date, eff_end: date
) -> pa.Table:
    n_rows = len(rows)
    null_f64 = pa.array([None] * n_rows, type=pa.float64())
    payment_amount_values = [r.payment_amount for r in rows]
    arrays = [
        pa.array(["national"] * n_rows, type=pa.string()),
        pa.array([r.code for r in rows], type=pa.string()),
        pa.array(["MS-DRG"] * n_rows, type=pa.string()),
        pa.array([fy] * n_rows, type=pa.int32()),
        pa.array([None] * n_rows, type=pa.string()),  # place_of_srvc
        pa.array([r.n for r in rows], type=pa.int64()),
        pa.array([r.mean_charge for r in rows], type=pa.float64()),
        pa.array([r.mean_payment for r in rows], type=pa.float64()),
        # Session 8.5 schema rename (DEV 4): payment_amount + concept +
        # total_payment_amount. Every row this extractor emits has
        # concept = "total_payment_allpayer" (source col = Average Total
        # Payments, all-payer total). total_payment_amount is the
        # self-documenting alias for inpatient consumers — same value
        # as payment_amount when concept = total_payment_allpayer.
        pa.array(payment_amount_values, type=pa.float64()),
        pa.array(["total_payment_allpayer"] * n_rows, type=pa.string()),
        pa.array(payment_amount_values, type=pa.float64()),
        null_f64,  # mean_payment_stdzd
        *([null_f64] * 10),  # p10..p90 charge + payment (10 cols)
        pa.array([None] * n_rows, type=pa.int32()),  # n_providers
        pa.array([_SILVER_SOURCE_TAG] * n_rows, type=pa.string()),
        pa.array([_SILVER_SOURCE_HOST] * n_rows, type=pa.string()),
        pa.array([eff_start] * n_rows, type=pa.date32()),
        pa.array([eff_end] * n_rows, type=pa.date32()),
    ]
    return pa.Table.from_arrays(arrays, schema=_PARQUET_SCHEMA)


def _output_dir_default() -> Path:
    return config.SILVER_PER_SOURCE / "puf" / "inpatient_summary"


def _parse_xlsx_to_rows(xlsx_path: Path) -> list[_ParsedRow]:
    sheet = _pick_by_drg_sheet(xlsx_path)
    rows = _read_xlsx_rows(xlsx_path, sheet_name=sheet)
    if not rows:
        raise RuntimeError(
            f"puf_inpatient_summary: XLSX {xlsx_path} parsed to zero rows"
        )
    header_idx = _find_header_index(rows)
    cols = _resolve_columns(list(rows[header_idx]))

    out: list[_ParsedRow] = []
    seen: set[str] = set()
    for raw in rows[header_idx + 1 :]:
        if not raw or len(raw) <= cols["drg"]:
            continue
        code = _parse_drg_code(raw[cols["drg"]])
        if code is None or code in seen:
            continue
        seen.add(code)
        n = _parse_int(raw[cols["n"]]) if len(raw) > cols["n"] else None
        if n is None:
            continue
        mc = (_parse_money(raw[cols["mean_charge"]])
              if len(raw) > cols["mean_charge"] else None)
        mp_idx = cols.get("mean_payment")
        mp = (_parse_money(raw[mp_idx])
              if mp_idx is not None and len(raw) > mp_idx else None)
        pa_idx = cols.get("payment_amount")
        pa_val = (_parse_money(raw[pa_idx])
                  if pa_idx is not None and len(raw) > pa_idx else None)
        # `payment_amount` is NOT NULL in the locked schema (DEV 4 invariant
        # from `plans/Session 8.5 flags.md`). Defensive filter against future
        # source-data drift: drop rows whose Average Total Payments is
        # blank/NA. Empirical FY2011 silver had 0 such rows; this filter is
        # cheap insurance.
        if pa_val is None:
            continue
        out.append(_ParsedRow(
            code=code, n=n, mean_charge=mc, mean_payment=mp,
            payment_amount=pa_val,
        ))

    if not out:
        raise RuntimeError(
            f"puf_inpatient_summary: XLSX {xlsx_path} yielded zero rows; "
            f"check header detection / DRG regex"
        )
    return out


def extract_fy(
    fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path | None:
    """Extract the National Summary parquet for fiscal year ``fy``.

    Returns the silver parquet path on success, or ``None`` if the URL 404s
    (mirrors HCPCS pre-2017 / OPPS pre-2017 log+skip precedent). Raises
    ``KeyError`` if ``fy`` is not in scope; raises ``RuntimeError`` on
    ZIP / XLSX parse failure (those are real bugs, not data unavailability).
    """
    if fy not in config.PUF_INPATIENT_SUMMARY_FYS:
        raise KeyError(
            f"puf_inpatient_summary fy{fy}: not in "
            f"config.PUF_INPATIENT_SUMMARY_FYS "
            f"({config.PUF_INPATIENT_SUMMARY_FYS})"
        )
    if fy not in _PUF_INPATIENT_SUMMARY_SOURCES:
        print(
            f"[puf_inpatient_summary] fy{fy}: missing entry in "
            f"_PUF_INPATIENT_SUMMARY_SOURCES; skipping",
            file=sys.stderr,
        )
        return None

    sess = session or _http.make_session()
    out_dir = output_dir if output_dir is not None else _output_dir_default()
    out_dir.mkdir(parents=True, exist_ok=True)

    url = _PUF_INPATIENT_SUMMARY_SOURCES[fy]
    print(f"[puf_inpatient_summary] fy{fy}: URL {url}", file=sys.stderr)

    filename = Path(url).name or f"puf_inpatient_summary_fy{fy}.zip"
    try:
        bronze_path = _http.download_to_bronze(
            url, source=_BRONZE_SOURCE, fy=fy, filename=filename,
            download_date=download_date, session=sess, period="fy",
        )
    except requests.HTTPError as exc:
        print(
            f"[puf_inpatient_summary] fy{fy}: HTTP "
            f"{getattr(exc.response, 'status_code', '?')} on {url}; skipping",
            file=sys.stderr,
        )
        return None

    with _open_zip(bronze_path) as zf:
        member = _find_xlsx_member(zf)
        xlsx_dest = bronze_path.parent / Path(member).name
        _extract_member(zf, member, xlsx_dest)
    print(
        f"[puf_inpatient_summary] fy{fy}: parsing {xlsx_dest.name}",
        file=sys.stderr,
    )

    parsed = _parse_xlsx_to_rows(xlsx_dest)
    eff_start, eff_end = config.fy_interval(fy)
    table = _build_table(parsed, fy=fy, eff_start=eff_start, eff_end=eff_end)

    out_path = out_dir / f"puf_inpatient_summary_fy{fy}.parquet"
    pq.write_table(table, str(out_path))
    print(
        f"[puf_inpatient_summary] fy{fy}: wrote {table.num_rows} rows to "
        f"{out_path}",
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
    """Loop ``extract_fy`` inclusively. Per-FY failures are log+skip."""
    if end_fy < start_fy:
        raise ValueError(f"end_fy ({end_fy}) must be >= start_fy ({start_fy})")
    sess = session or _http.make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for fy in range(start_fy, end_fy + 1):
        if fy not in config.PUF_INPATIENT_SUMMARY_FYS:
            print(
                f"[puf_inpatient_summary] fy{fy}: not in "
                f"config.PUF_INPATIENT_SUMMARY_FYS, skipping",
                file=sys.stderr,
            )
            continue
        try:
            silver_path = extract_fy(fy, session=sess, download_date=download_date)
            if silver_path is not None:
                out.append(silver_path)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[puf_inpatient_summary] fy{fy} FAILED: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((fy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[puf_inpatient_summary] bulk run completed with "
            f"{len(failures)} failure(s):",
            file=sys.stderr,
        )
        for fy, msg in failures:
            print(f"  FY{fy}: {msg}", file=sys.stderr)
    print(
        f"[puf_inpatient_summary] bulk run summary: {len(out)} succeeded, "
        f"{len(failures)} failed",
        file=sys.stderr,
    )
    return out


def _parse_fy_range(spec: str) -> tuple[int, int]:
    parts = spec.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range expects 'START-END' (e.g., '2011-2013'), got {spec!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range integers required, got {spec!r}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.puf_inpatient_summary",
        description=(
            "Download and parse the per-FY CMS Medicare Inpatient PUF "
            "National Summary aggregate XLSX (FY2011-FY2013)."
        ),
    )
    fy_group = parser.add_mutually_exclusive_group(required=True)
    fy_group.add_argument(
        "--fy", type=int,
        help="Single fiscal year (must be in {2011, 2012, 2013}).",
    )
    fy_group.add_argument(
        "--fy-range", type=_parse_fy_range, metavar="START-END",
        help="Inclusive FY range, e.g., '2011-2013'.",
    )
    parser.add_argument(
        "--download-date", default=None,
        help="Override bronze-path date stamp (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args(argv)

    if args.fy is not None:
        if args.fy not in config.PUF_INPATIENT_SUMMARY_FYS:
            parser.error(
                f"--fy must be in {config.PUF_INPATIENT_SUMMARY_FYS}, "
                f"got {args.fy!r}"
            )
        extract_fy(args.fy, download_date=args.download_date)
        return 0

    start_fy, end_fy = args.fy_range
    extract_fy_range(start_fy, end_fy, download_date=args.download_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
