"""Phase 14c — full NTDB cost annotation, FY-sharded with checkpointing.

Orchestrates an FY-by-FY cost annotation of the full NTDB
``annual_cohort.parquet`` (~10.08M rows across FY2007-FY2024). Each FY is
processed independently and flushed to its own checkpoint parquet under
``--checkpoint-dir``. After all FYs run, the per-FY checkpoints are unioned
into a single ``cohort_annotated.parquet`` under ``--output-dir``
via a DuckDB ``COPY (... FROM read_parquet(<glob>)) TO <final>`` step using
an atomic ``.partial`` -> final rename so an interrupted union can never
corrupt the final file.

Restartable: if a checkpoint file already exists for a given FY, that FY is
skipped (logged as ``[skip] fy=...``). Re-running the same command after a
crash resumes from the next un-checkpointed FY. The union step also globs
``cohort_annotated_fy*.parquet`` from disk at union time (mirrors
the ``src.transform.icd_drg._union_checkpoints`` pattern fixed in Session
11), so a partial-range re-run cannot silently overwrite the final parquet
with only the current invocation's FYs.

In-memory adapter approach
--------------------------
``src.query._ntdb_adapter.load_ntdb_cohort`` is a path-based loader (it
opens the parquet via ``pd.read_parquet``). Rather than refactor the
adapter to accept an in-memory DataFrame (Workstream B's territory), we
filter the source parquet to the target FY via DuckDB
(``SELECT * FROM read_parquet(?) WHERE YEAR = ?``), write the FY-restricted
DataFrame to a ``tempfile.NamedTemporaryFile`` parquet, then call
``load_ntdb_cohort(temp_path, attach_procedures=...)`` on that temp file.
The temp file is deleted automatically when the context exits. This keeps
the adapter contract untouched and reuses ALL of its behavior (procedure
attachment, dtype coercion, hosp_disp mapping) per FY shard.

Wall-time / memory expectations
-------------------------------
At Session 3.5 cohort_annotator throughput (~150-300 rows/s in parquet
mode on a single core), the full 10.08M-row run is roughly
~10-20 hours total wall time across 18 FY shards. Memory per shard is
bounded by the largest single FY (~600k rows for FY2024), which fits
comfortably in <2 GB resident.

CLI
---
::

    python3 -m src.query.run_phase14c \
        --source-parquet PATH \
        [--output-dir data/derived] \
        [--checkpoint-dir data/derived/_checkpoints] \
        [--fy-range 2007-2024] \
        [--no-attach-procedures] \
        [--ntdb-duckdb PATH]

``--source-parquet`` is required: it is the path to your own NTDB
``annual_cohort.parquet``. The NTDB is restricted data and no cohort file is
included in this repository. For FY2019-FY2024 shards, unless
``--no-attach-procedures`` is given, the NTDB DuckDB path must come from
``--ntdb-duckdb`` or the ``KG_COST_NTDB_DUCKDB`` environment variable; there is
no built-in default (a missing path raises ``RuntimeError`` when the first
FY2019 shard is reached).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import duckdb

from src.config import ROOT
from src.query._ntdb_adapter import load_ntdb_cohort
from src.query.cohort_annotator import annotate_cohort

_DEFAULT_OUTPUT_DIR = ROOT / "data" / "derived"
_DEFAULT_CHECKPOINT_DIR = _DEFAULT_OUTPUT_DIR / "_checkpoints"
_DEFAULT_FY_RANGE: tuple[int, int] = (2007, 2024)
_FINAL_BASENAME = "cohort_annotated.parquet"
_CHECKPOINT_GLOB = "cohort_annotated_fy*.parquet"


def _checkpoint_path(checkpoint_dir: Path, fy: int) -> Path:
    return checkpoint_dir / f"cohort_annotated_fy{fy}.parquet"


def _read_fy_shard(source_parquet: Path, fy: int):
    """Filter ``source_parquet`` to rows where YEAR == fy via DuckDB.

    Returns a pandas DataFrame restricted to that FY. The filter runs in
    DuckDB (parquet predicate pushdown) so we never materialize the full
    10M-row source in memory.
    """
    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT * FROM read_parquet(?) WHERE YEAR = ?",
            [str(source_parquet), int(fy)],
        ).df()
    finally:
        con.close()
    return df


def _annotate_fy_shard(
    fy: int,
    source_parquet: Path,
    duckdb_path: Optional[Path],
    attach_procedures: bool,
):
    """Read FY shard, run the NTDB adapter, then annotate. Returns DataFrame.

    Uses a NamedTemporaryFile to bridge the in-memory FY-filtered DataFrame
    into the path-based ``load_ntdb_cohort`` API without modifying the
    adapter. Empty shards (FY with zero source rows) short-circuit to an
    empty annotated DataFrame.
    """
    raw_df = _read_fy_shard(source_parquet, fy)
    if raw_df.empty:
        sys.stderr.write(f"[run_phase14c] fy={fy} empty shard; emitting 0 rows\n")
        return raw_df

    with tempfile.NamedTemporaryFile(
        suffix=f"_ntdb_fy{fy}.parquet", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        raw_df.to_parquet(tmp_path, index=False)
        cohort_df = load_ntdb_cohort(
            tmp_path,
            attach_procedures=attach_procedures,
            duckdb_path=duckdb_path,
        )
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    if cohort_df.empty:
        return cohort_df

    result = annotate_cohort(
        cohort_df,
        source="parquet",
        default_year=fy,
        fy_column="fy",
    )
    return result.df


def run(
    source_parquet: Path,
    output_dir: Path = _DEFAULT_OUTPUT_DIR,
    checkpoint_dir: Path = _DEFAULT_CHECKPOINT_DIR,
    fy_range: tuple[int, int] = _DEFAULT_FY_RANGE,
    attach_procedures: bool = True,
    duckdb_path: Optional[Path] = None,
) -> tuple[Path, int, int]:
    """Run the full FY-sharded annotation. Public entry point for tests.

    Parameters mirror the CLI flags. Returns
    ``(final_output_path, total_rows, n_fy_shards)``.
    """
    source_parquet = Path(source_parquet)
    output_dir = Path(output_dir)
    checkpoint_dir = Path(checkpoint_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    if not source_parquet.exists():
        raise FileNotFoundError(f"source parquet not found: {source_parquet}")

    start_fy, end_fy = fy_range
    if start_fy > end_fy:
        raise ValueError(f"invalid fy_range {fy_range}: start > end")

    n_shards = 0
    t0 = time.time()
    for fy in range(start_fy, end_fy + 1):
        ckpt = _checkpoint_path(checkpoint_dir, fy)
        if ckpt.exists():
            print(f"[skip] fy={fy} checkpoint exists ({ckpt})")
            n_shards += 1
            continue

        t_fy = time.time()
        sys.stderr.write(f"[run_phase14c] fy={fy} starting...\n")
        try:
            df = _annotate_fy_shard(
                fy=fy,
                source_parquet=source_parquet,
                duckdb_path=duckdb_path,
                attach_procedures=attach_procedures,
            )
        except Exception as e:
            sys.stderr.write(
                f"[run_phase14c] fy={fy} FAILED: {type(e).__name__}: {e}\n"
            )
            raise

        # Skip checkpoint emission for empty FY shards. Writing the raw NTDB
        # schema for an empty shard would corrupt the annotated-schema union
        # downstream; and writing an empty annotated parquet is non-trivial
        # without round-tripping a single dummy row. The skip-on-exists guard
        # is still correct on re-run: an empty FY just re-does the cheap
        # DuckDB filter probe.
        if df.empty:
            n_shards += 1
            elapsed_fy = time.time() - t_fy
            print(f"[done] fy={fy} rows=0 (no checkpoint written) elapsed={elapsed_fy:.1f}s")
            continue

        df.to_parquet(ckpt, index=False)
        n_shards += 1
        elapsed_fy = time.time() - t_fy
        print(f"[done] fy={fy} rows={len(df)} elapsed={elapsed_fy:.1f}s -> {ckpt}")

    # Union all on-disk checkpoints into the final parquet via DuckDB COPY.
    # Glob disk at union time (mirrors src.transform.icd_drg._union_checkpoints
    # pattern) so a partial-range re-run cannot silently overwrite the final
    # parquet with only this invocation's FYs.
    final_path = output_dir / _FINAL_BASENAME
    partial_path = final_path.with_suffix(final_path.suffix + ".partial")
    existing = sorted(str(p) for p in checkpoint_dir.glob(_CHECKPOINT_GLOB))
    if not existing:
        raise RuntimeError(
            f"no checkpoints found under {checkpoint_dir} matching {_CHECKPOINT_GLOB}; "
            "nothing to union"
        )

    con = duckdb.connect()
    try:
        in_list = ", ".join(f"'{p}'" for p in existing)
        out_sql = str(partial_path).replace("'", "''")
        # ``union_by_name=True`` defends against future cohort_annotator
        # schema drift between FY shards (e.g. an added column landing in
        # later FYs but not earlier ones); aligns columns by name not by
        # ordinal position.
        con.execute(
            f"COPY (SELECT * FROM read_parquet([{in_list}], union_by_name=True)) "
            f"TO '{out_sql}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # Atomic rename: .partial -> final.
    os.replace(partial_path, final_path)

    # Count final rows for the summary.
    con = duckdb.connect()
    try:
        n_total = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{str(final_path).replace(chr(39), chr(39)+chr(39))}')"
        ).fetchone()[0]
    finally:
        con.close()

    elapsed = time.time() - t0
    print(
        f"[run_phase14c] total_rows={n_total} n_fy_shards={n_shards} "
        f"output_path={final_path} elapsed={elapsed:.1f}s"
    )
    return final_path, int(n_total), int(n_shards)


def _parse_fy_range(s: str) -> tuple[int, int]:
    if "-" not in s:
        fy = int(s)
        return (fy, fy)
    a, b = s.split("-", 1)
    return (int(a), int(b))


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_phase14c",
        description=(
            "Phase 14c — FY-sharded NTDB cost annotation with checkpointing. "
            "Restartable: existing per-FY checkpoints are skipped on re-run."
        ),
    )
    p.add_argument(
        "--source-parquet",
        type=Path,
        required=True,
        help=(
            "Path to your NTDB annual_cohort.parquet (required; NTDB is "
            "restricted data and no cohort file is included in this repo)."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=_DEFAULT_OUTPUT_DIR,
        help=f"Final unioned parquet output dir (default: {_DEFAULT_OUTPUT_DIR}).",
    )
    p.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=_DEFAULT_CHECKPOINT_DIR,
        help=f"Per-FY checkpoint dir (default: {_DEFAULT_CHECKPOINT_DIR}).",
    )
    p.add_argument(
        "--fy-range",
        type=str,
        default=f"{_DEFAULT_FY_RANGE[0]}-{_DEFAULT_FY_RANGE[1]}",
        help=(
            f"Inclusive FY range, e.g. '2007-2024' (default: "
            f"{_DEFAULT_FY_RANGE[0]}-{_DEFAULT_FY_RANGE[1]})."
        ),
    )
    p.add_argument(
        "--no-attach-procedures",
        action="store_true",
        help=(
            "Skip ICD-10-PCS attachment from the NTDB DuckDB. "
            "(Default: attach procedures for FY2019-FY2024 rows.)"
        ),
    )
    p.add_argument(
        "--ntdb-duckdb",
        type=Path,
        default=None,
        help=(
            "Override path to NTDB DuckDB file for procedure attachment. "
            "Falls back to the KG_COST_NTDB_DUCKDB env var; there is no "
            "built-in default. Needed (by flag or env var) for FY2019-FY2024 "
            "shards unless --no-attach-procedures is given."
        ),
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    fy_range = _parse_fy_range(args.fy_range)
    try:
        run(
            source_parquet=args.source_parquet,
            output_dir=args.output_dir,
            checkpoint_dir=args.checkpoint_dir,
            fy_range=fy_range,
            attach_procedures=not args.no_attach_procedures,
            duckdb_path=args.ntdb_duckdb,
        )
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("[run_phase14c] interrupted; per-FY checkpoints preserved.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
