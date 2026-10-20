"""Phase 7b — ICD->DRG deterministic reachability + modal enumeration.

Replaces the originally-planned NIS-derived probabilistic edge with a
free, grouper-driven deterministic alternative: for each ICD-10-CM code
we run the MS-DRG grouper with 5 synthetic-claim permutations to
discover (a) the SET of DRGs the code can anchor (reachability) and
(b) the most-frequent DRG (modal). Outputs:

  data/gold/icd_principal_drg_reachability.parquet
  data/gold/icd_principal_drg_modal.parquet

Coverage: FY2023-FY2026 ICD-10-CM only (Java V40+ grouper). FY2019-
FY2022 drgpy fallback DEFERRED to Session 9.5 -- no production drgpy
wrapper exists in src/grouper/ yet (only in tests/test_drgpy_cross.py),
and per plan-v12.md line 388-391 this scope-down is explicitly allowed
risk-mitigation. Pre-FY2019 (FY2008-FY2018) ICD->DRG is permanently
deferred (no grouper coverage).

Pre-MDC DRGs (001-018), reserved DRGs (998/999), and ungroupable
returns (DRG 000) are surfaced rather than filtered, so the gap is
visible. Newborn (P-prefix) ICD codes route through the locked Phase 4
newborn override (age=0, sex='F', dispo='03') so MDC-15 routing works.

Schema (locked, plan-v12.md lines 251-273):

  icd_principal_drg_reachability.parquet
    icd_cm: VARCHAR              -- ICD-10-CM code
    fy: INTEGER
    ms_drg: VARCHAR
    mdc: VARCHAR
    source: VARCHAR              -- 'grouper_enumeration_v9_2026-05'
    permutations_hit: TINYINT    -- 1-5
    Compound key: (icd_cm, fy, ms_drg)

  icd_principal_drg_modal.parquet
    icd_cm: VARCHAR
    fy: INTEGER
    ms_drg: VARCHAR
    mdc: VARCHAR
    source: VARCHAR              -- 'grouper_modal_default_2026-05'
    modal_permutation: VARCHAR
    permutations_total: TINYINT  -- 5
    Compound key: (icd_cm, fy)
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterable, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src import config
from src.grouper.grouper import ClaimRecord, GrouperError, assign


# ---------------------------------------------------------------------------
# Locked permutation set (plan-v12.md line 238-245)
# ---------------------------------------------------------------------------

# Per-permutation overrides: each dict supplies sdx, pcs, age, sex, dispo.
# Newborn override is applied later in _build_record on top of these defaults.
_PERMUTATIONS: list[tuple[str, dict]] = [
    ("baseline",       {"sdx": (), "pcs": (), "age": 65, "sex": "F", "dispo": "01"}),
    ("with_mcc",       {"sdx": ("I9789",), "pcs": (), "age": 65, "sex": "F", "dispo": "01"}),
    ("with_cc",        {"sdx": ("Z87891",), "pcs": (), "age": 65, "sex": "F", "dispo": "01"}),
    ("with_or_proc",   {"sdx": (), "pcs": ("0DTJ4ZZ",), "age": 65, "sex": "F", "dispo": "01"}),
    ("male_baseline",  {"sdx": (), "pcs": (), "age": 65, "sex": "M", "dispo": "01"}),
]
_N_PERM = len(_PERMUTATIONS)

# Source tags (plan-v12.md lines 258, 268).
_REACHABILITY_SOURCE = "grouper_enumeration_v9_2026-05"
_MODAL_SOURCE = "grouper_modal_default_2026-05"

# Coverage scope: FY2019-FY2026. FY2023+ via Java grouper (V40+); FY2019-
# FY2022 via drgpy fallback wired into ``src.grouper.grouper.assign`` in
# Session 9.5 (FLAG 3 work package). Pre-FY2019 ICD-10 has no grouper at
# all (NTIS-paid V25-V34 + Windows/.exe V35-V37 + V38.1/V39 model-v1
# driver-incompatible).
_SUPPORTED_FYS: tuple[int, ...] = (2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026)


# ---------------------------------------------------------------------------
# Pyarrow schemas (locked — keep aligned with module docstring)
# ---------------------------------------------------------------------------


_REACHABILITY_SCHEMA = pa.schema([
    pa.field("icd_cm", pa.string(), nullable=False),
    pa.field("fy", pa.int32(), nullable=False),
    pa.field("ms_drg", pa.string(), nullable=False),
    pa.field("mdc", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("permutations_hit", pa.int8(), nullable=False),
])

_MODAL_SCHEMA = pa.schema([
    pa.field("icd_cm", pa.string(), nullable=False),
    pa.field("fy", pa.int32(), nullable=False),
    pa.field("ms_drg", pa.string(), nullable=False),
    pa.field("mdc", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("modal_permutation", pa.string(), nullable=False),
    pa.field("permutations_total", pa.int8(), nullable=False),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_newborn_pdx(pdx: str) -> bool:
    """Phase 4 locked landmine #1: ICD-10-CM chapter P00-P96 = perinatal."""
    return bool(pdx) and pdx[0].upper() == "P"


def _build_record(pdx: str, perm: dict, fy: int) -> ClaimRecord:
    """Construct a ClaimRecord for one permutation; apply newborn override.

    For ICD codes whose first character is 'P' (chapter P00-P96, Certain
    conditions originating in the perinatal period), override age/sex/dispo
    so MDC-15 routing engages. ``fy`` is currently unused inside the record
    itself — the grouper version is keyed off the FY at ``assign`` time —
    but we keep it in the signature for forward compatibility (e.g. if FY
    ever influences the synthetic claim shape).
    """
    del fy  # reserved for future use; assign() consumes the FY directly
    if _is_newborn_pdx(pdx):
        age = 0
        sex = "F"
        dispo = "03"
        age_days_admit = 0
        age_days_discharge = 0
    else:
        age = int(perm["age"])
        sex = str(perm["sex"])
        dispo = str(perm["dispo"])
        age_days_admit = 0
        age_days_discharge = 0
    return ClaimRecord(
        pdx=pdx,
        sdx=tuple(perm["sdx"]),
        pcs=tuple(perm["pcs"]),
        sex=sex,
        age=age,
        discharge_disp=dispo,
        los=5,
        admit_date=None,
        age_days_admit=age_days_admit,
        age_days_discharge=age_days_discharge,
        poa=(),  # all-Y default per Phase 4 locked contract
    )


def _run_permutations(pdx: str, fy: int) -> list[tuple[str, str, str]]:
    """Run all 5 permutations for a single (pdx, fy).

    Returns a list of (perm_label, ms_drg, mdc) tuples, length always
    ``_N_PERM``. On a per-permutation ``GrouperError`` we log to stderr and
    append (perm_label, "000", "00") so the failure is visible but the loop
    survives. "000" matches the Java driver's ungroupable convention
    (KGCostGrouperDriver.java:197) and the module docstring; reserves "999"
    for a legitimate CMS DRG 999 ("UNGROUPABLE") return from the grouper.
    """
    out: list[tuple[str, str, str]] = []
    for perm_label, perm in _PERMUTATIONS:
        record = _build_record(pdx, perm, fy)
        try:
            res = assign(record, fy)
            out.append((perm_label, res.ms_drg, res.mdc))
        except GrouperError as e:
            sys.stderr.write(
                f"[icd_drg] grouper error pdx={pdx} fy={fy} perm={perm_label}: {e}\n"
            )
            out.append((perm_label, "000", "00"))
    return out


def _compute_modal(
    rows: list[tuple[str, str, str]],
) -> tuple[str, str, str]:
    """Most-frequent ms_drg across permutations; tie-break by lowest DRG number.

    Returns (modal_drg, mdc, modal_perm_label) where ``modal_perm_label`` is
    the FIRST permutation that produced the modal DRG (deterministic order
    via ``_PERMUTATIONS``). The ``mdc`` reported is the mdc associated with
    the modal DRG in the first matching row.
    """
    if not rows:
        raise ValueError("_compute_modal: empty rows")
    counter: Counter[str] = Counter(drg for _, drg, _ in rows)
    max_count = max(counter.values())
    # Tie-break: lowest DRG number (lexicographic on zero-padded 3-char string
    # is equivalent to numeric for "000".."999").
    candidates = sorted(drg for drg, c in counter.items() if c == max_count)
    modal_drg = candidates[0]
    # Find first row with this DRG to anchor mdc + perm label.
    for perm_label, drg, mdc in rows:
        if drg == modal_drg:
            return modal_drg, mdc, perm_label
    raise RuntimeError("unreachable: modal_drg not found in rows")


def _compute_reachability(
    icd: str,
    fy: int,
    rows: list[tuple[str, str, str]],
) -> list[dict]:
    """Aggregate permutations into reachability rows.

    Compound key (icd_cm, fy, ms_drg). ``permutations_hit`` counts how many
    of the 5 permutations routed to this DRG. ``mdc`` is taken from the
    FIRST permutation that produced this DRG (mdc is a function of (pdx,
    drg) so all permutations producing the same DRG should agree on mdc;
    if they didn't, that'd indicate a grouper bug worth surfacing — but for
    now we just take first-seen).
    """
    by_drg: dict[str, list[tuple[str, str]]] = {}  # drg -> [(perm_label, mdc)]
    for perm_label, drg, mdc in rows:
        by_drg.setdefault(drg, []).append((perm_label, mdc))
    out: list[dict] = []
    for drg, hits in by_drg.items():
        # mdc from the first row with this drg.
        mdc = hits[0][1]
        out.append({
            "icd_cm": icd,
            "fy": fy,
            "ms_drg": drg,
            "mdc": mdc,
            "source": _REACHABILITY_SOURCE,
            "permutations_hit": len(hits),
        })
    return out


# ---------------------------------------------------------------------------
# Code-list loading from codes_master
# ---------------------------------------------------------------------------


def _load_icd10cm_codes(fy: int, codes_master: Path) -> list[str]:
    """Distinct ICD-10-CM codes for a given FY from codes_master.parquet."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT DISTINCT code FROM read_parquet(?) "
            "WHERE code_system = 'ICD-10-CM' AND fy_or_cy = ? "
            "ORDER BY code",
            [str(codes_master), fy],
        ).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Per-FY enumeration with checkpointing
# ---------------------------------------------------------------------------


def enumerate_fy(
    fy: int,
    codes: list[str],
    output_dir: Path,
) -> tuple[Path, Path]:
    """Run permutation enumeration for one FY; write checkpoint parquets.

    Returns (reachability_checkpoint, modal_checkpoint) paths. Uses tqdm if
    available; else stderr progress every 500 codes. Catches KeyboardInterrupt
    so partial work flushes to disk before re-raising.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    reach_ckpt = output_dir / f"_checkpoint_reachability_fy{fy}.parquet"
    modal_ckpt = output_dir / f"_checkpoint_modal_fy{fy}.parquet"

    # Optional tqdm.
    try:
        from tqdm import tqdm  # type: ignore[import-not-found]
        iterator: Iterable[str] = tqdm(
            codes, desc=f"FY{fy}", unit="icd", file=sys.stderr,
        )
    except Exception:
        iterator = codes  # plain

    reach_rows: list[dict] = []
    modal_rows: list[dict] = []

    n_total = len(codes)
    t0 = time.time()
    interrupted = False
    try:
        for i, icd in enumerate(iterator):
            try:
                perm_rows = _run_permutations(icd, fy)
                reach_rows.extend(_compute_reachability(icd, fy, perm_rows))
                modal_drg, modal_mdc, modal_perm = _compute_modal(perm_rows)
                modal_rows.append({
                    "icd_cm": icd,
                    "fy": fy,
                    "ms_drg": modal_drg,
                    "mdc": modal_mdc,
                    "source": _MODAL_SOURCE,
                    "modal_permutation": modal_perm,
                    "permutations_total": _N_PERM,
                })
            except KeyboardInterrupt:
                interrupted = True
                sys.stderr.write(
                    f"\n[icd_drg] KeyboardInterrupt at FY{fy} icd={icd} "
                    f"({i}/{n_total}); flushing checkpoint...\n"
                )
                break
            except Exception as e:
                # Defensive: a non-GrouperError shouldn't escape _run_permutations
                # (it catches GrouperError per-perm), but anything else means
                # logic bug — log and skip the icd entirely so the loop survives.
                sys.stderr.write(
                    f"[icd_drg] unexpected error pdx={icd} fy={fy}: {type(e).__name__}: {e}\n"
                )
                continue

            # Plain-iter progress every 500 codes (tqdm handles itself).
            if "tqdm" not in str(type(iterator)) and (i + 1) % 500 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                sys.stderr.write(
                    f"[icd_drg] FY{fy} {i + 1}/{n_total} ({rate:.1f} icd/s)\n"
                )
    except KeyboardInterrupt:
        # Outer guard in case tqdm swallows then re-raises.
        interrupted = True
        sys.stderr.write(
            f"\n[icd_drg] KeyboardInterrupt at FY{fy}; flushing checkpoint...\n"
        )

    # Flush checkpoints (always, even on interrupt or empty — tests need files).
    _write_parquet(reach_rows, reach_ckpt, _REACHABILITY_SCHEMA)
    _write_parquet(modal_rows, modal_ckpt, _MODAL_SCHEMA)

    elapsed = time.time() - t0
    sys.stderr.write(
        f"[icd_drg] FY{fy} done: codes={len(modal_rows)} reach_rows={len(reach_rows)} "
        f"elapsed={elapsed:.1f}s\n"
    )

    if interrupted:
        # Re-raise so the CLI exits non-zero and the operator knows it stopped.
        raise KeyboardInterrupt(f"icd_drg FY{fy} interrupted after {len(modal_rows)} codes")

    return reach_ckpt, modal_ckpt


def _write_parquet(rows: list[dict], path: Path, schema: pa.Schema) -> None:
    """Materialize rows-of-dicts to parquet under the locked schema.

    Empty input writes an empty parquet with the schema header (so downstream
    tests can read it without crashing). Casts integer columns to the
    schema-declared widths (int8/int32) explicitly.
    """
    if not rows:
        empty = {f.name: [] for f in schema}
        tbl = pa.table(empty, schema=schema)
        pq.write_table(tbl, path)
        return
    # Build columnar dict explicitly to control types.
    cols: dict[str, list] = {f.name: [] for f in schema}
    for r in rows:
        for f in schema:
            cols[f.name].append(r[f.name])
    tbl = pa.table(cols, schema=schema)
    pq.write_table(tbl, path)


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------


def build_icd_drg(
    fy_range: tuple[int, int],
    output_dir: Path,
    codes_master: Optional[Path] = None,
    limit: Optional[int] = None,
) -> tuple[Path, Path]:
    """Enumerate per FY then UNION checkpoints into final gold parquets.

    Parameters
    ----------
    fy_range:
        Inclusive (start_fy, end_fy). Filtered to the intersection with
        ``_SUPPORTED_FYS`` (FY2023-FY2026). FYs outside the support set are
        skipped with a stderr warning rather than erroring — partial coverage
        is the documented Session 9 scope.
    output_dir:
        Where checkpoints + final parquets land. Defaults via CLI to
        ``data/gold/``.
    codes_master:
        Path to ``codes_master.parquet``. Defaults to ``config.SILVER /
        codes_master.parquet``.
    limit:
        If set, truncate each FY's code list to this many codes (smoke mode).

    Returns
    -------
    (final_reachability_path, final_modal_path)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    if codes_master is None:
        codes_master = config.SILVER / "codes_master.parquet"
    if not codes_master.exists():
        raise FileNotFoundError(
            f"codes_master parquet not found: {codes_master}. "
            "Run `python3 -m src.transform.codes_master` first."
        )

    start_fy, end_fy = fy_range
    requested = list(range(start_fy, end_fy + 1))
    fys = [fy for fy in requested if fy in _SUPPORTED_FYS]
    skipped = [fy for fy in requested if fy not in _SUPPORTED_FYS]
    if skipped:
        sys.stderr.write(
            f"[icd_drg] skipping FYs without Java grouper coverage: {skipped} "
            "(FY2019-FY2022 deferred to Session 9.5; pre-FY2019 deferred indefinitely)\n"
        )
    if not fys:
        raise ValueError(
            f"No supported FYs in range {fy_range}. Supported: {_SUPPORTED_FYS}"
        )

    reach_ckpts: list[Path] = []
    modal_ckpts: list[Path] = []
    for fy in fys:
        codes = _load_icd10cm_codes(fy, codes_master)
        if limit is not None:
            codes = codes[:limit]
        sys.stderr.write(f"[icd_drg] FY{fy}: enumerating {len(codes)} codes...\n")
        rckpt, mckpt = enumerate_fy(fy, codes, output_dir)
        reach_ckpts.append(rckpt)
        modal_ckpts.append(mckpt)

    # Union checkpoints into final gold parquets via DuckDB. Glob the
    # output dir at union time (NOT the in-memory `reach_ckpts`/`modal_ckpts`
    # lists) so a partial-range re-run cannot silently overwrite the gold
    # parquet with only the current invocation's FYs. See CLAUDE.md
    # "Open questions" entry for `_union_checkpoints` (Session 9.5 surface,
    # closed Session 11 Step 8).
    final_reach = output_dir / "icd_principal_drg_reachability.parquet"
    final_modal = output_dir / "icd_principal_drg_modal.parquet"

    _union_checkpoints(
        output_dir,
        "_checkpoint_reachability_fy*.parquet",
        final_reach,
        _REACHABILITY_SCHEMA,
    )
    _union_checkpoints(
        output_dir,
        "_checkpoint_modal_fy*.parquet",
        final_modal,
        _MODAL_SCHEMA,
    )

    return final_reach, final_modal


def _union_checkpoints(
    output_dir: Path,
    glob_pattern: str,
    output: Path,
    schema: pa.Schema,
) -> None:
    """UNION ALL every per-FY checkpoint matching ``glob_pattern`` in
    ``output_dir`` into a single parquet via DuckDB.

    The function globs the output directory at union time so partial-range
    re-runs (e.g. ``--fy-range 2025-2026`` after an earlier
    ``--fy-range 2019-2022`` run) cannot silently overwrite the gold parquet
    with only the current invocation's FYs — every checkpoint on disk is
    included. See CLAUDE.md "Open questions" entry for `_union_checkpoints`.
    """
    existing = sorted(str(p) for p in output_dir.glob(glob_pattern))
    if not existing:
        # Write empty parquet with schema header.
        empty = {f.name: [] for f in schema}
        pq.write_table(pa.table(empty, schema=schema), output)
        return
    con = duckdb.connect()
    try:
        out_sql = str(output).replace("'", "''")
        # DuckDB COPY does parameter-binding-into-destination poorly per the
        # codes_master.py landmine; inline output path, parameter-bind input.
        in_list = ", ".join(f"'{p}'" for p in existing)
        con.execute(
            f"COPY (SELECT * FROM read_parquet([{in_list}])) "
            f"TO '{out_sql}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_fy_range(s: str) -> tuple[int, int]:
    if "-" not in s:
        fy = int(s)
        return (fy, fy)
    a, b = s.split("-", 1)
    return (int(a), int(b))


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Phase 7b: ICD->DRG deterministic enumeration via grouper. "
            "Coverage scope FY2023-FY2026 only this session; FY2019-FY2022 "
            "drgpy fallback deferred to Session 9.5."
        ),
    )
    p.add_argument(
        "--fy-range",
        default="2019-2026",
        help=(
            "Inclusive FY range (default 2019-2026). Single FY: "
            "'--fy-range 2024'. FY2019-FY2022 routes via drgpy fallback; "
            "FY2023+ via Java grouper."
        ),
    )
    p.add_argument(
        "--output-dir",
        default=str(config.GOLD),
        help=f"Output dir for parquets + checkpoints (default {config.GOLD}).",
    )
    p.add_argument(
        "--smoke",
        action="store_true",
        help="Smoke mode: must be paired with --limit to truncate codes per FY.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Truncate each FY's code list to N codes (smoke).",
    )
    args = p.parse_args(argv)

    fy_range = _parse_fy_range(args.fy_range)
    output_dir = Path(args.output_dir)

    if args.smoke and args.limit is None:
        sys.stderr.write(
            "[icd_drg] --smoke requires --limit N (e.g. --limit 100)\n"
        )
        return 2

    try:
        reach_path, modal_path = build_icd_drg(
            fy_range=fy_range,
            output_dir=output_dir,
            limit=args.limit,
        )
    except KeyboardInterrupt:
        sys.stderr.write("[icd_drg] interrupted; checkpoint(s) flushed.\n")
        return 130

    # Final row counts to stdout (stderr reserved for progress).
    n_reach = pq.ParquetFile(reach_path).metadata.num_rows
    n_modal = pq.ParquetFile(modal_path).metadata.num_rows
    print(f"reachability: {reach_path} rows={n_reach}")
    print(f"modal:        {modal_path} rows={n_modal}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
