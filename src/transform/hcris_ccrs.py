"""Compute national-mean CCRs per (fy, cost_center_group).

Joins:

  * ``data/silver/hcris_cost_centers.parquet`` (FY2021+ definition table; the
    19-cost-center group → HCRIS cell-ref taxonomy)
  * ``data/silver/per_source/hcris_data_fy{fy}.parquet`` (FY2015+ provider-level
    cell matrix from cms.gov SAS via :mod:`src.extract.hcris_data`. Phase 11
    NBER long-form path for FY2008-FY2014 deferred to Session 1.5 — the
    Form-2552-96 worksheet schema differs from Form-2552-10's; see
    plans/Session 1 flags.md FLAG 5)

For each ``(fy, cost_center_group)``:

  1. Resolve the set of cost cells (``C_1_C5_*``) and charge cells (``C_1_C6_*``,
     ``C_1_C7_*``) attached to that group via the definition table's
     ``hcris_cost_cell_ref`` / ``hcris_charges_cell_ref`` columns. Charge cells
     can carry a multi-cell expression like ``C_1_C6_30, C_1_C7_30`` (Col 6 +
     Col 7 = inpatient + outpatient). Parser splits on whitespace and commas.
  2. Per provider, sum the cost cells -> ``provider_cost`` and charge cells ->
     ``provider_charges``.
  3. National-mean CCR = ``sum(provider_cost) / sum(provider_charges)`` across
     all providers with both numerator AND denominator > 0.

Forward-fill: the FY2021 cost-center taxonomy is propagated backward to
FY2015-FY2020 (the IPPS preamble has carried the 19-group taxonomy stably for
15+ years; only minor additions like Cell Therapy Drugs in FY2026). So for any
FY in [2015, 2020] we look up the definition table for that FY first; if absent,
fall back to FY2021's definition.

Output: ``data/silver/national_ccr.parquet`` with schema:

    fy: int32
    cost_center_group: str
    n_providers: int32
    sum_cost: float64
    sum_charges: float64
    ccr_value: float64       -- sum_cost / sum_charges
    source: str              -- always "hcris_ccr_national"
    source_host: str         -- "cms_gov_hcris" (cms.gov FY2015-FY2026; NBER FY2008-FY2014 deferred to Session 1.5)
    effective_start: date32
    effective_end: date32

CLI:

    python -m src.transform.hcris_ccrs
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src import config

_FORWARD_FILL_BACK_TO_FY: int = 2008  # earliest FY we attempt CCRs for (Session 1.5: Form 2552-96 worksheet mapper landed; see plans/Session 1 flags.md FLAG 5 + plans/2026-05-02-session-1.5-fix-flags.md)
_DEFINITION_FALLBACK_FY: int = 2021   # earliest FY the definition table exists

# Cell-ref token recognizer: e.g. C_1_C5_30, C_1_C7_92.01, D3_HOS_C2_30.
# Alphanumeric column / line components support Form 2552-96 cells
# (e.g. C_1_C5A_30, D0_HOS_C2_A185). The strict-superset of the original
# Form 2552-10 pattern so existing FY2015+ tokens still match.
_CELL_TOKEN_RE = re.compile(r"[A-Z][A-Z0-9_]*_C[0-9A-Z]+_[0-9A-Z]+(?:\.[0-9]+)?", re.IGNORECASE)

# Canonical 19-cost-center-group names. The CMS supplemental table renamed
# 5 groups in FY2024 (added "(excluding ...)" qualifiers); we map back to the
# FY2021-FY2023 canonical short names so cross-FY joins line up cleanly.
# Also folds the unicode-hyphen variant of "Cardiac Catheterization" present
# in some FYs.
_CANONICAL_GROUP_RENAMES: dict[str, str] = {
    "Cardiac Catheteri-zation": "Cardiac Catheterization",
    "Cardiology (excluding Cardiac Catheterization)": "Cardiology",
    "Operating Room (excluding Labor & Delivery)": "Operating Room",
    "Radiology (excluding CT scan)": "Radiology",
    "Supplies and Equipment (excluding Implantable Devices)": "Supplies and Equipment",
}


def _canonicalize_group(name: str) -> str:
    """Map FY2024+ verbose group names back to the FY2021-FY2023 short form."""
    s = " ".join(str(name).split())  # normalize whitespace runs
    return _CANONICAL_GROUP_RENAMES.get(s, s)


_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("fy", pa.int32(), nullable=False),
        pa.field("cost_center_group", pa.string(), nullable=False),
        pa.field("n_providers", pa.int32(), nullable=False),
        pa.field("sum_cost", pa.float64(), nullable=True),
        pa.field("sum_charges", pa.float64(), nullable=True),
        pa.field("ccr_value", pa.float64(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("source_host", pa.string(), nullable=False),
        pa.field("effective_start", pa.date32(), nullable=False),
        pa.field("effective_end", pa.date32(), nullable=False),
    ]
)


def _split_cells(s: str | None) -> list[str]:
    """Extract cell-ref tokens from a free-text cell.

    The supplemental table sometimes packs multiple refs per cell, e.g.
    ``"C_1_C6_30, C_1_C7_30"`` or even just whitespace-separated. Returns a
    deduplicated list preserving order. Returns ``[]`` for None / blank.
    """
    if not s:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for tok in _CELL_TOKEN_RE.findall(str(s)):
        norm = tok.strip()
        # Dot-suffix subline numbers (e.g. "92.01") collapse to underscore form
        # since SAS columns store them as e.g. ``C_1_C5_92.01`` literal too —
        # we keep the dot. Just normalize underscoring.
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def _resolve_group_cells(defs_df: pd.DataFrame) -> pd.DataFrame:
    """Per (fy, cost_center_group): collect all cost / charge cell refs.

    Returns one row per (fy, cost_center_group) with two list-typed columns
    ``cost_cells`` and ``charge_cells``. Multiple definition rows per group
    (one per Worksheet C line) are flattened.
    """
    # Canonicalize group names BEFORE grouping so FY2024+ verbose names fold
    # into the canonical short form and the per-FY group set stays at 19.
    defs_df = defs_df.copy()
    defs_df["cost_center_group"] = defs_df["cost_center_group"].map(_canonicalize_group)
    grouped = defs_df.groupby(["fy", "cost_center_group"], sort=True)

    rows: list[dict] = []
    for (fy, grp), sub in grouped:
        cost_cells: list[str] = []
        charge_cells: list[str] = []
        seen_cost: set[str] = set()
        seen_charge: set[str] = set()
        for _, r in sub.iterrows():
            for tok in _split_cells(r.get("hcris_cost_cell_ref")):
                if tok not in seen_cost:
                    seen_cost.add(tok)
                    cost_cells.append(tok)
            for tok in _split_cells(r.get("hcris_charges_cell_ref")):
                if tok not in seen_charge:
                    seen_charge.add(tok)
                    charge_cells.append(tok)
        rows.append(
            {
                "fy": int(fy),
                "cost_center_group": grp,
                "cost_cells": cost_cells,
                "charge_cells": charge_cells,
            }
        )
    return pd.DataFrame(rows)


def _compute_ccrs_for_fy(
    fy: int,
    defs_for_fy: pd.DataFrame,
    provider_path: Path,
) -> pd.DataFrame:
    """Compute (cost_center_group -> national CCR) for a single FY.

    Reads only the cell columns referenced by ``defs_for_fy`` from the
    provider parquet (DuckDB column pruning). Sums per provider, then
    aggregates to national totals.
    """
    needed_cells: set[str] = set()
    for _, r in defs_for_fy.iterrows():
        needed_cells.update(r["cost_cells"])
        needed_cells.update(r["charge_cells"])

    con = duckdb.connect()
    try:
        # Inspect which cells actually exist in the parquet (column pruning).
        present_cols = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{str(provider_path)}')"
            ).fetchall()
        }
        usable = sorted(needed_cells & present_cols)
        if not usable:
            print(
                f"[hcris_ccrs] FY{fy}: no overlap between definition cell refs "
                f"({len(needed_cells)}) and provider columns ({len(present_cols)}); "
                "skipping",
                file=sys.stderr,
            )
            return pd.DataFrame()
        select_cols = ["prvdr_num"] + usable
        select_sql = ", ".join(f'"{c}"' for c in select_cols)
        prov_df = con.execute(
            f"SELECT {select_sql} FROM read_parquet('{str(provider_path)}')"
        ).df()
    finally:
        con.close()

    out_rows: list[dict] = []
    for _, r in defs_for_fy.iterrows():
        cost_cells = [c for c in r["cost_cells"] if c in prov_df.columns]
        charge_cells = [c for c in r["charge_cells"] if c in prov_df.columns]

        # Per-provider sum across the relevant cells. NaN treated as 0 for
        # summation but providers with all-NaN are dropped (no data).
        if cost_cells:
            cost_per_prov = prov_df[cost_cells].sum(axis=1, min_count=1)
        else:
            cost_per_prov = pd.Series([float("nan")] * len(prov_df), index=prov_df.index)
        if charge_cells:
            chrg_per_prov = prov_df[charge_cells].sum(axis=1, min_count=1)
        else:
            chrg_per_prov = pd.Series([float("nan")] * len(prov_df), index=prov_df.index)

        # National numerator/denominator: sum across providers with both > 0.
        mask = cost_per_prov.notna() & chrg_per_prov.notna() & (chrg_per_prov > 0)
        n_prov = int(mask.sum())
        sum_cost = float(cost_per_prov[mask].sum()) if n_prov else None
        sum_chrg = float(chrg_per_prov[mask].sum()) if n_prov else None
        ccr = (sum_cost / sum_chrg) if (sum_chrg and sum_chrg > 0) else None

        out_rows.append(
            {
                "fy": fy,
                "cost_center_group": r["cost_center_group"],
                "n_providers": n_prov,
                "sum_cost": sum_cost,
                "sum_charges": sum_chrg,
                "ccr_value": ccr,
            }
        )

    return pd.DataFrame(out_rows)


def build_national_ccr(
    *,
    silver_dir: Path | None = None,
    out_path: Path | None = None,
) -> Path:
    """Build ``data/silver/national_ccr.parquet``.

    Reads the unioned cost-center supplemental and per-FY provider data; emits
    one row per ``(fy, cost_center_group)`` with the national-mean CCR.
    """
    silver = silver_dir or config.SILVER
    out = out_path or (silver / "national_ccr.parquet")

    defs_path = silver / "hcris_cost_centers.parquet"
    if not defs_path.exists():
        raise FileNotFoundError(
            f"{defs_path} missing; run "
            "`python -m src.extract.hcris_cost_centers --fy-range 2021-2026 && "
            "python -m src.extract.hcris_cost_centers --build-silver` first."
        )

    defs_df = pq.read_table(defs_path).to_pandas()
    if defs_df.empty:
        raise RuntimeError(f"{defs_path} is empty")

    defs_resolved = _resolve_group_cells(defs_df)

    # Build a per-FY definition table, with forward-fill for FY < 2021.
    fallback = defs_resolved[defs_resolved["fy"] == _DEFINITION_FALLBACK_FY]
    if fallback.empty:
        raise RuntimeError(
            f"FY{_DEFINITION_FALLBACK_FY} definition not present in {defs_path}; "
            "needed for forward-fill to FY2015-FY2020."
        )

    per_source = config.SILVER_PER_SOURCE / "hcris" / "data"
    provider_paths = sorted(per_source.glob("hcris_data_fy*.parquet"))
    if not provider_paths:
        raise FileNotFoundError(
            f"No hcris_data_fy*.parquet under {per_source}; run "
            "`python -m src.extract.hcris_data --fy-range 2015-2026` first."
        )

    all_rows: list[pd.DataFrame] = []
    for prov_path in provider_paths:
        # Parse fy from filename suffix (hcris_data_fy{fy}.parquet).
        m = re.search(r"hcris_data_fy(\d{4})\.parquet$", prov_path.name)
        if not m:
            print(
                f"[hcris_ccrs] cannot parse FY from {prov_path.name}; skipping",
                file=sys.stderr,
            )
            continue
        fy = int(m.group(1))
        if fy < _FORWARD_FILL_BACK_TO_FY:
            print(
                f"[hcris_ccrs] FY{fy} below forward-fill floor {_FORWARD_FILL_BACK_TO_FY}; "
                "skipping",
                file=sys.stderr,
            )
            continue

        # Definition: prefer per-FY, else FY2021 fallback (forward-fill).
        defs_for_fy = defs_resolved[defs_resolved["fy"] == fy]
        if defs_for_fy.empty:
            defs_for_fy = fallback.copy()
            defs_for_fy = defs_for_fy.assign(fy=fy)

        ccr_df = _compute_ccrs_for_fy(fy, defs_for_fy, prov_path)
        if ccr_df.empty:
            continue
        all_rows.append(ccr_df)
        print(
            f"[hcris_ccrs] FY{fy}: {len(ccr_df)} cost-center groups computed",
            file=sys.stderr,
        )

    if not all_rows:
        raise RuntimeError("No FY produced CCR rows; something is wrong upstream.")

    full = pd.concat(all_rows, ignore_index=True)
    # Add constants. effective_start/end from fy_interval; source/source_host fixed.
    eff_starts = []
    eff_ends = []
    for fy in full["fy"].astype(int):
        s, e = config.fy_interval(int(fy))
        eff_starts.append(s)
        eff_ends.append(e)
    full = full.assign(
        source="hcris_ccr_national",
        source_host="cms_gov_hcris",
        effective_start=eff_starts,
        effective_end=eff_ends,
    )

    table = pa.Table.from_pandas(
        full[
            [
                "fy",
                "cost_center_group",
                "n_providers",
                "sum_cost",
                "sum_charges",
                "ccr_value",
                "source",
                "source_host",
                "effective_start",
                "effective_end",
            ]
        ],
        schema=_PARQUET_SCHEMA,
        preserve_index=False,
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(
        f"[hcris_ccrs] wrote {table.num_rows} rows -> {out}",
        file=sys.stderr,
    )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.transform.hcris_ccrs",
        description=(
            "Compute national-mean CCRs per (fy, cost_center_group) by joining "
            "the FY2021+ HCRIS cost-center definition table with the FY2015+ "
            "provider-level HCRIS SAS-derived cell matrix."
        ),
    )
    parser.add_argument("--silver-dir", default=None, type=Path)
    parser.add_argument("--output", default=None, type=Path)
    args = parser.parse_args(argv)
    build_national_ccr(silver_dir=args.silver_dir, out_path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
