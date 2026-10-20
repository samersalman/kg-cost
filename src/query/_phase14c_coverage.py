"""Phase 14c coverage diagnostic — counts rows by (fy, inpatient_confidence,
annotation_status) over the unioned annotated parquet emitted by
``src.query.run_phase14c``.

Outputs:
  * a parquet file of the per-(fy, inpatient_confidence, annotation_status)
    counts (one row per group; columns
    ``[fy, inpatient_confidence, annotation_status, n_rows, pct_of_fy]``); and
  * a markdown table printed to stdout for SAP v3 to embed verbatim.

Usage::

    python3 -m src.query._phase14c_coverage \
        [--input data/derived/cohort_annotated.parquet] \
        [--output data/derived/cohort_coverage_by_fy_confidence.parquet]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config import ROOT

_DEFAULT_INPUT = ROOT / "data" / "derived" / "cohort_annotated.parquet"
_DEFAULT_OUTPUT = (
    ROOT / "data" / "derived" / "cohort_coverage_by_fy_confidence.parquet"
)


def build_coverage(input_path: Path, output_path: Path) -> pd.DataFrame:
    """Compute per-(fy, inpatient_confidence, annotation_status) counts.

    Returns the resulting DataFrame and writes it to ``output_path``. Also
    prints a markdown-table view of the rows to stdout (SAP v3 embeds it
    verbatim).
    """
    if not input_path.exists():
        raise FileNotFoundError(f"input parquet not found: {input_path}")

    df = pd.read_parquet(input_path)
    # Defensive: cohort_annotator emits inpatient_confidence as a default-
    # 'none' string; coerce missing/null to literal 'none' so groupby keys
    # never carry NaN.
    df["inpatient_confidence"] = df["inpatient_confidence"].fillna("none").astype(str)
    df["annotation_status"] = df["annotation_status"].fillna("no_match").astype(str)
    df["fy"] = df["fy"].astype("Int64")

    grouped = (
        df.groupby(["fy", "inpatient_confidence", "annotation_status"], dropna=False)
        .size()
        .reset_index(name="n_rows")
    )
    fy_totals = grouped.groupby("fy")["n_rows"].transform("sum")
    grouped["pct_of_fy"] = grouped["n_rows"] / fy_totals.where(fy_totals > 0, 1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_parquet(output_path, index=False)

    _print_markdown_table(grouped)
    return grouped


def _print_markdown_table(df: pd.DataFrame) -> None:
    """Print a markdown table of the coverage breakdown to stdout."""
    print("| fy | inpatient_confidence | annotation_status | n_rows | pct_of_fy |")
    print("|----|----------------------|-------------------|--------|-----------|")
    for _, r in df.sort_values(
        ["fy", "inpatient_confidence", "annotation_status"]
    ).iterrows():
        print(
            f"| {r['fy']} | {r['inpatient_confidence']} | "
            f"{r['annotation_status']} | {r['n_rows']} | "
            f"{r['pct_of_fy']:.4f} |"
        )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="_phase14c_coverage",
        description=(
            "Build a per-(fy, inpatient_confidence, annotation_status) "
            "coverage diagnostic from the Phase 14c unioned annotated parquet."
        ),
    )
    p.add_argument(
        "--input",
        type=Path,
        default=_DEFAULT_INPUT,
        help=f"Input parquet (default: {_DEFAULT_INPUT}).",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"Output parquet (default: {_DEFAULT_OUTPUT}).",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    try:
        build_coverage(args.input, args.output)
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
