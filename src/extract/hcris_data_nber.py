"""HCRIS provider-level data extractor — NBER long-form CSV (FY2008-FY2014).

Phase 11 / Session 12 (2026-05-02), extended Session 1.5 (2026-05-02).
Backfills the FY2008-FY2014 era for which CMS never published the SAS-form
provider-level dataset that :mod:`src.extract.hcris_data` consumes. NBER
mirrors the same underlying HCRIS worksheet-cell matrix in CSV long-form,
schema ``(rpt_rec_num, wksht_cd, line_num, clmn_num, itm_val_num)``, one row
per worksheet cell per provider.

**Form 2552-96 vs Form 2552-10** — HCRIS uses two distinct cost-report forms
across the FY range we cover:

  * **Form 2552-10** (FY2011+): Worksheet C Part 1 (``C000001``) for charges,
    Worksheet D-3 hospital (``D3*``) for cost/charge ratios. ``clmn_num`` and
    ``line_num`` are pure integers. Mapper: :func:`_build_cell_name`.
  * **Form 2552-96** (FY2008-FY2010): Worksheet C Part 1 (``C000001``) for
    charges (alpha-suffixed columns like ``"5A00"`` are common). Worksheet D-3
    does NOT exist; Medicare cost / charges live on Worksheets D-0 (``D00*``),
    D-1 (``D10*``), and D-4 (``D40*``) with alpha-coded sub-line indicators
    (e.g. ``"A185"``). Mapper: :func:`_build_cell_name_2552_96`.

``_load_and_pivot`` dispatches between the two mappers based on FY (the
``_FORM_2552_96_FYS`` frozenset), and uses a wider ``_KEEP_PREFIXES_2552_96``
retain-set for the 2552-96 era (adds ``D0_HOS_C2_``, ``D1_HOS_C2_``,
``D4_HOS_C2_`` on top of the 2552-10 baseline). The mapper preserves the
alphanumeric ``clmn_num`` / ``line_num`` tokens literally in the pivoted cell
name (``C_1_C5A_30``, ``D0_HOS_C2_A185``).

This module:

  1. Downloads ``hosp_nmrc2552_{96|10}_{YYYY}_long.csv`` from
     ``https://data.nber.org/hcris/`` (Form 2552-96 for FY2008-FY2010, Form
     2552-10 for FY2011+). Sizes range from ~250 MB (FY2010) to ~800 MB
     (FY2011-FY2014) — see ``plans/hcris-url-discovery.md`` "Session 9.5 NBER
     pre-check" for the per-FY HEAD-probe manifest.
  2. Filters the long-form rows to the worksheet families consumed by
     :mod:`src.transform.hcris_ccrs`: Worksheet C Part 1 (``C00*``) for both
     forms; Worksheet D-3 (``D3*``) for Form 2552-10; Worksheets D-0/D-1/D-4
     (``D00*``/``D10*``/``D40*``) for Form 2552-96.
  3. Pivots ``(rpt_rec_num × wksht_cd × line_num × clmn_num)`` into the wide
     SAS-shape cell-column layout (e.g. ``C_1_C5_30``, ``D3_HOS_C2_30``,
     ``C_1_C5A_30``, ``D0_HOS_C2_A185``) so ``_resolve_group_cells`` /
     ``_compute_ccrs_for_fy`` in ``src/transform/hcris_ccrs.py`` can consume
     the parquet without code changes (the matching widening of
     ``_CELL_TOKEN_RE`` lives there).
  4. Trims to the form-version-appropriate ``_KEEP_PREFIXES`` cohort.
  5. Writes ``data/silver/per_source/hcris/data/hcris_data_fy{fy}.parquet`` —
     same path the SAS extractor uses, so :mod:`src.transform.hcris_ccrs`
     picks up both eras transparently.

Identifier mapping note: NBER long-form is keyed on ``rpt_rec_num`` (the
cost-report record number). The ``prvdr_num`` (CMS Certification Number) is
only available via a join to the separate ``csts_chrgs_*.csv`` provider-aggregate
table. To stay 1:1 schema-compatible with the SAS path (which carries both
``rpt_rec_num`` AND ``prvdr_num``), we emit ``prvdr_num = rpt_rec_num`` in the
silver parquet. ``rpt_rec_num`` uniquely identifies a provider's annual cost
report, so the per-provider aggregation semantics in
``hcris_ccrs._compute_ccrs_for_fy`` (which SELECTs on ``prvdr_num``) are
preserved. The ``source`` constant tags rows as ``"hcris_data_nber"`` so
downstream can distinguish the two eras when needed.

CLI:

    python -m src.extract.hcris_data_nber --fy 2014
    python -m src.extract.hcris_data_nber --fy-range 2008-2014
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src import config
from src.extract import _http
from src.extract.hcris_data import _KEEP_PREFIXES

_BRONZE_SOURCE: str = "hcris_data_nber"
_SOURCE_TAG: str = "hcris_data_nber"

# Form 2552-96 FYs route through ``_build_cell_name_2552_96`` and the wider
# ``_KEEP_PREFIXES_2552_96`` retain-set inside ``_load_and_pivot``. Session
# 1.5 (2026-05-02) replaced the prior ``extract_fy`` short-circuit with a
# real mapper; FY2008-FY2010 now produces non-empty parquets keyed on the
# Form 2552-96 worksheet topology (D-0 / D-1 / D-4 instead of D-3).
_FORM_2552_96_FYS: frozenset[int] = frozenset({2008, 2009, 2010})

# Form 2552-96 (FY2008-FY2010) cell prefixes. Worksheet D-3 is replaced by
# D-0 / D-1 / D-4 in Form 2552-96 (Worksheet D-3 was introduced in 2552-10).
# Wider superset of ``_KEEP_PREFIXES`` (the 2552-10 baseline imported above).
_KEEP_PREFIXES_2552_96: tuple[str, ...] = _KEEP_PREFIXES + (
    "D0_HOS_C2_",
    "D1_HOS_C2_",
    "D4_HOS_C2_",
)

# Per-FY URLs verified 2026-05-02 (see plans/hcris-url-discovery.md
# "Session 9.5 NBER pre-check").
_HCRIS_NBER_LONG_URLS: dict[int, str] = {
    2008: "https://data.nber.org/hcris/2552-96/hosp_nmrc2552_96_2008_long.csv",
    2009: "https://data.nber.org/hcris/2552-96/hosp_nmrc2552_96_2009_long.csv",
    2010: "https://data.nber.org/hcris/2552-96/hosp_nmrc2552_96_2010_long.csv",
    2011: "https://data.nber.org/hcris/2552-10/hosp_nmrc2552_10_2011_long.csv",
    2012: "https://data.nber.org/hcris/2552-10/hosp_nmrc2552_10_2012_long.csv",
    2013: "https://data.nber.org/hcris/2552-10/hosp_nmrc2552_10_2013_long.csv",
    2014: "https://data.nber.org/hcris/2552-10/hosp_nmrc2552_10_2014_long.csv",
}


def _decode_nber_token(s: object) -> str | None:
    """Decode an NBER long-form zero-padded clmn_num/line_num token.

    NBER's long-form CSV stores HCRIS cost-report column / line codes as
    fixed-width zero-padded strings where the last 2 characters are the
    sub-indicator (sub-column for ``clmn_num``, sub-line for ``line_num``).
    ``"00"`` means no sub-indicator; non-zero means the major and sub
    indicators concatenate into the SAS canonical form.

    Pure-numeric tokens (the 99% case)::

        "00100" -> "1"     (col 1, sub 00)
        "00500" -> "5"     (col 5, sub 00)
        "0500"  -> "5"     (FY2008/2552-96 uses 4-char tokens)
        "03000" -> "30"    (line 30, sub 00)
        "03001" -> "3001"  (line 30, sub 01 -> SAS concat form, matches
                            ``C_1_C5_4201`` style on the SAS path)
        "04201" -> "4201"  (line 42 sub 01)
        "02500" -> "25"    (line 25, sub 00)
        "55300" -> "553"   (line 553 sub 00 — bare)

    Alphanumeric tokens (FY2008/2552-96 sub-column letters; rare)::

        "5A00" -> "5A"     (column 5, alpha sub-col A, sub 00 -> bare 5A)
        "5A01" -> "5A01"   (column 5, alpha sub-col A, sub 01 -> concat)

    Returns ``None`` for empty / null / non-decodable input.
    """
    if s is None:
        return None
    # Numeric inputs (test fixtures pass int / float; real NBER data is
    # always string). Coerce exact integers to their bare string form so
    # ``5`` / ``5.0`` decode to ``"5"`` regardless of upstream dtype.
    if isinstance(s, (int, float)) and not isinstance(s, bool):
        try:
            return str(int(s))
        except (ValueError, OverflowError):
            return None
    s = str(s).strip().upper()
    if not s or s == "NAN":
        return None
    if s.isdigit():
        # Drop subline=00 sentinel; otherwise emit the full integer (no
        # leading zeros) so SAS-shape columns like C_1_C5_4201 line up.
        if len(s) >= 3 and s.endswith("00"):
            major = s[:-2].lstrip("0")
            return major if major else "0"
        stripped = s.lstrip("0")
        return stripped if stripped else "0"
    # Alphanumeric path. Last-2 "00" still means "no subline".
    if len(s) >= 3 and s.endswith("00"):
        return s[:-2]
    return s


def _build_cell_name(wksht_cd: str, clmn_num: object, line_num: object) -> str | None:
    """Map a Form 2552-10 (FY2011+) ``(wksht_cd, clmn_num, line_num)`` triple to
    the SAS-shape column name.

    Returns ``None`` for worksheet families we don't retain, so callers can
    drop those rows before pivoting.

    NBER ``wksht_cd`` values look like ``'C000001'`` (Worksheet C Part 1)
    and ``'D30A180'`` (Worksheet D-3 hospital, sub-variant A). We use a
    defensive prefix match — ``C00`` for the C-Part-1 family and ``D3``
    for the D-3 family — and reconstruct the column name in the SAS
    canonical format consumed by :mod:`src.transform.hcris_ccrs`.

    ``clmn_num`` / ``line_num`` are NBER zero-padded tokens; the decode
    happens in :func:`_decode_nber_token` (e.g. ``"00500"`` -> ``"5"``,
    ``"03000"`` -> ``"30"``, ``"04201"`` -> ``"4201"``).
    """
    if wksht_cd is None:
        return None
    code = str(wksht_cd).strip().upper()
    col = _decode_nber_token(clmn_num)
    line = _decode_nber_token(line_num)
    if col is None or line is None:
        return None
    # Form 2552-10 uses pure-numeric line/column tokens. Reject any
    # alphanumeric residue (Form 2552-96 contamination): ``"5A"``,
    # ``"30A"``, etc., would produce a cell name that won't match the
    # FY2015+ SAS canonical schema. Callers should dispatch to
    # :func:`_build_cell_name_2552_96` for FY2008-FY2010.
    if not (col.isdigit() and line.isdigit()):
        return None
    if code.startswith("C00"):
        return f"C_1_C{col}_{line}"
    if code.startswith("D3"):
        return f"D3_HOS_C{col}_{line}"
    return None


def _build_cell_name_2552_96(
    wksht_cd: str, clmn_num: object, line_num: object
) -> str | None:
    """Form 2552-96 (FY2008-FY2010) cell-name builder.

    Form 2552-96 has NO Worksheet D-3. Medicare cost / charges live on
    Worksheets D-0, D-1, D-4 instead. ``clmn_num`` can be alphanumeric
    (e.g. ``"5A00"`` for sub-column A).

    Worksheet topology mapping:

      * ``C000001`` (Worksheet C Part 1) -> ``C_1_C{col}_{line}``
      * ``D000001`` (Worksheet D-0)      -> ``D0_HOS_C{col}_{line}``
      * ``D100001`` (Worksheet D-1)      -> ``D1_HOS_C{col}_{line}``
      * ``D400001`` (Worksheet D-4)      -> ``D4_HOS_C{col}_{line}``

    Sub-indicator semantics match :func:`_decode_nber_token`: trailing
    ``"00"`` means bare line/column, non-zero means concat encoding.

    Returns ``None`` for unknown ``wksht_cd`` prefixes or empty tokens.
    """
    if wksht_cd is None:
        return None
    code = str(wksht_cd).strip().upper()
    col = _decode_nber_token(clmn_num)
    line = _decode_nber_token(line_num)
    if col is None or line is None:
        return None

    if code.startswith("C00"):
        return f"C_1_C{col}_{line}"
    if code.startswith("D00"):
        return f"D0_HOS_C{col}_{line}"
    if code.startswith("D10"):
        return f"D1_HOS_C{col}_{line}"
    if code.startswith("D40"):
        return f"D4_HOS_C{col}_{line}"
    return None


def _load_and_pivot(csv_path: Path, fy: int) -> pd.DataFrame:
    """Read the NBER long-form CSV and pivot to the SAS-shape wide frame.

    Dispatches between the Form 2552-10 and Form 2552-96 cell-name mappers
    based on ``fy``: FY2008-FY2010 use :func:`_build_cell_name_2552_96` +
    :data:`_KEEP_PREFIXES_2552_96` (Worksheets C-Part-1 / D-0 / D-1 / D-4);
    FY2011+ use :func:`_build_cell_name` + :data:`_KEEP_PREFIXES`
    (Worksheets C-Part-1 / D-3 hospital).

    Filters to the form-version-appropriate worksheet families, builds the
    cell-name column, and pivots ``(rpt_rec_num × cell_name)`` into a wide
    matrix. Only columns matching one of the kept prefixes are retained.
    """
    is_2552_96 = fy in _FORM_2552_96_FYS
    if is_2552_96:
        mapper = _build_cell_name_2552_96
        keep_prefixes = _KEEP_PREFIXES_2552_96
        # Form 2552-96 routes Worksheet C Part 1 + D-0/D-1/D-4 hospital.
        wksht_prefixes = ("C00", "D00", "D10", "D40")
        wksht_label = "C Part 1 + D-0/D-1/D-4 (Form 2552-96)"
    else:
        mapper = _build_cell_name
        keep_prefixes = _KEEP_PREFIXES
        wksht_prefixes = ("C00", "D3")
        wksht_label = "C Part 1 + D-3 (Form 2552-10)"

    print(
        f"[hcris_data_nber] reading {csv_path.name} "
        f"({csv_path.stat().st_size:,} bytes); FY{fy} -> {wksht_label}",
        file=sys.stderr,
    )
    # NBER long-form clmn_num / line_num occasionally carry alphanumeric
    # sub-line indicators (e.g. "5A00", "A185"). Form 2552-96 mapper handles
    # these; the 2552-10 mapper int-casts within try/except and drops them.
    df = pd.read_csv(
        csv_path,
        dtype={
            "rpt_rec_num": "string",
            "wksht_cd": "string",
            "clmn_num": "string",
            "line_num": "string",
            "itm_val_num": "float64",
        },
        usecols=["rpt_rec_num", "wksht_cd", "clmn_num", "line_num", "itm_val_num"],
        low_memory=False,
    )
    print(
        f"[hcris_data_nber] {len(df):,} rows loaded; "
        f"filtering to Worksheet {wksht_label}",
        file=sys.stderr,
    )

    # Defensive prefix match — drop everything else early to shrink memory.
    code_upper = df["wksht_cd"].astype("string").str.upper()
    keep_mask = pd.Series(False, index=df.index)
    for wp in wksht_prefixes:
        keep_mask = keep_mask | code_upper.str.startswith(wp).fillna(False)
    df = df[keep_mask].copy()
    print(
        f"[hcris_data_nber] {len(df):,} rows after wksht_cd prefix filter",
        file=sys.stderr,
    )

    df["cell_name"] = [
        mapper(w, c, l)
        for w, c, l in zip(df["wksht_cd"], df["clmn_num"], df["line_num"])
    ]
    df = df.dropna(subset=["cell_name", "rpt_rec_num"])
    if df.empty:
        raise RuntimeError(
            f"{csv_path}: no rows survived prefix + cell-name reconstruction"
        )

    # Pre-prune: only pivot the cells that match a kept prefix. Saves a huge
    # amount of memory vs. pivoting then trimming.
    kept_cell = df["cell_name"].astype(str).apply(
        lambda c: any(c.startswith(p) for p in keep_prefixes)
    )
    df = df[kept_cell]
    print(
        f"[hcris_data_nber] {len(df):,} rows after _KEEP_PREFIXES trim; pivoting",
        file=sys.stderr,
    )

    wide = df.pivot_table(
        index="rpt_rec_num",
        columns="cell_name",
        values="itm_val_num",
        aggfunc="first",
    )
    wide = wide.reset_index()
    wide.columns.name = None
    print(
        f"[hcris_data_nber] pivoted to {len(wide):,} provider rows × "
        f"{len(wide.columns)} columns",
        file=sys.stderr,
    )
    return wide


def _df_to_table(df: pd.DataFrame, fy: int) -> pa.Table:
    """Add constant columns + emit a parquet-friendly Arrow table.

    Mirrors :func:`src.extract.hcris_data._df_to_table` so the silver schema
    is interoperable across the SAS and NBER eras. Emits ``prvdr_num``
    aliased to ``rpt_rec_num`` so downstream ``hcris_ccrs`` (which keys on
    ``prvdr_num``) works without modification.
    """
    eff_start, eff_end = config.fy_interval(fy)

    if "rpt_rec_num" not in df.columns:
        raise RuntimeError("Pivoted frame missing rpt_rec_num index column")

    df = df.copy()
    df["rpt_rec_num"] = df["rpt_rec_num"].astype("string")
    # Emit prvdr_num = rpt_rec_num for schema parity with the SAS path.
    df["prvdr_num"] = df["rpt_rec_num"]

    # Form-version-aware kept-prefix set (Form 2552-96 has D-0/D-1/D-4
    # instead of D-3, so the wider retain-set is the canonical one for
    # FY2008-FY2010).
    keep_prefixes = (
        _KEEP_PREFIXES_2552_96 if fy in _FORM_2552_96_FYS else _KEEP_PREFIXES
    )

    # All cell columns -> float64 (NaN for missing).
    cell_cols = [
        c for c in df.columns
        if c not in ("rpt_rec_num", "prvdr_num")
        and any(c.startswith(p) for p in keep_prefixes)
    ]
    for c in cell_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.assign(
        fy=fy,
        source=_SOURCE_TAG,
        effective_start=eff_start,
        effective_end=eff_end,
    )

    front = ["rpt_rec_num", "prvdr_num", "fy", "source", "effective_start", "effective_end"]
    ordered = front + cell_cols
    df = df[ordered]

    fields: list[pa.Field] = []
    for c in ordered:
        if c in ("rpt_rec_num", "prvdr_num", "source"):
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
    """Download + parse NBER long-form HCRIS for a single FY.

    Returns the path to the per-FY parquet, or ``None`` if the URL is missing
    / 404s. Form 2552-96 (FY2008-FY2010) and Form 2552-10 (FY2011+) are both
    handled; ``_load_and_pivot`` dispatches the right cell-name mapper based
    on ``fy``.
    """
    url = _HCRIS_NBER_LONG_URLS.get(fy)
    if not url:
        print(
            f"[hcris_data_nber] FY{fy} not in NBER URL manifest "
            "(NBER long-form provider-level HCRIS only spans FY2008-FY2014); "
            "skipping",
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
            f"[hcris_data_nber] HTTPError for FY{fy}: {exc}; skipping",
            file=sys.stderr,
        )
        return None

    wide = _load_and_pivot(bronze, fy)
    table = _df_to_table(wide, fy)

    out = config.SILVER_PER_SOURCE / "hcris" / "data" / f"hcris_data_fy{fy}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[hcris_data_nber] FY{fy}: {table.num_rows} provider rows × "
        f"{len(table.schema.names)} cols -> {out}",
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
                f"[hcris_data_nber] FY{fy} failed: {exc}; continuing",
                file=sys.stderr,
            )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.hcris_data_nber",
        description=(
            "Download and parse the NBER long-form HCRIS CSV "
            "(hosp_nmrc2552_*_long.csv, ~250-800 MB × 7 FYs FY2008-FY2014). "
            "Pivots into the same wide cell-column shape as the SAS path so "
            "src/transform/hcris_ccrs.py picks up both eras transparently."
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
