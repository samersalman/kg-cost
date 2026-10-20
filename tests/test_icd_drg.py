"""Phase 7b — tests for ICD->DRG deterministic enumeration outputs.

Validates schema lock, compound-key uniqueness, modal/reachability row
parity, newborn (P-prefix) MDC-15 routing, ungroupable rate, pre-MDC
DRG (001-018) non-"00" MDC contract, and a small frozen list of
hand-curated (icd, fy, expected_drg_set, expected_mdc) pairs.

Skip behavior:
- The grouper smoke test skips if Java is unavailable or no grouper JARs
  are vendored (mirrors tests/test_grouper_validation.py).
- All data-dependent tests skip if the gold parquets do not exist
  (mirrors tests/test_codes_master.py skip-if-missing pattern). The gold
  parquets are produced by `python3 -m src.transform.icd_drg`.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from src import config

# Module import is unconditional: a discovery-time ImportError here is a real
# Phase 7b code bug, not a missing-data condition.
from src.transform.icd_drg import (  # noqa: F401
    _MODAL_SCHEMA,
    _PERMUTATIONS,
    _build_record,
    _compute_modal,
    _is_newborn_pdx,
    _union_checkpoints,
    build_icd_drg,
)
import pyarrow as pa

_REACH_PATH = config.GOLD / "icd_principal_drg_reachability.parquet"
_MODAL_PATH = config.GOLD / "icd_principal_drg_modal.parquet"


_REACH_EXPECTED_COLS: dict[str, str] = {
    "icd_cm": "string",
    "fy": "int32",
    "ms_drg": "string",
    "mdc": "string",
    "source": "string",
    "permutations_hit": "int8",
}

_MODAL_EXPECTED_COLS: dict[str, str] = {
    "icd_cm": "string",
    "fy": "int32",
    "ms_drg": "string",
    "mdc": "string",
    "source": "string",
    "modal_permutation": "string",
    "permutations_total": "int8",
}


# ---------------------------------------------------------------------------
# Skip predicates (mirrors test_grouper_validation.py)
# ---------------------------------------------------------------------------


def _java_available() -> bool:
    return shutil.which("java") is not None


def _vendored_fys() -> set[int]:
    """Set of FYs whose V*.0 grouper JARs are present on disk."""
    try:
        from src.extract.grouper import _find_core_jar  # type: ignore[import-not-found]
    except Exception:
        return set()
    out: set[int] = set()
    for fy, versions in config.GROUPER_VERSIONS_BY_FY.items():
        if not versions:
            continue
        v0 = versions[0]
        jar_dir = config.grouper_jar_dir(fy, v0)
        if jar_dir.is_dir() and _find_core_jar(jar_dir, v0) is not None:
            out.add(fy)
    return out


def _skip_if_missing(path: Path, label: str) -> None:
    if not path.exists():
        pytest.skip(
            f"{label} ({path}) not present; "
            f"run `python3 -m src.transform.icd_drg --smoke --limit 100 "
            f"--fy-range 2024-2024` first."
        )


# ---------------------------------------------------------------------------
# 1. Smoke: assign() round-trip for one ICD with all 5 permutations
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _java_available(), reason="java not on PATH")
@pytest.mark.skipif(2024 not in _vendored_fys(), reason="V41.0 grouper JARs not vendored")
def test_smoke_single_icd_grouper_runs() -> None:
    """One ICD across 5 perms returns at least one non-'000' DRG."""
    from src.transform.icd_drg import _run_permutations

    rows = _run_permutations("I214", 2024)  # AMI principal
    assert len(rows) == len(_PERMUTATIONS)
    drgs = {drg for _, drg, _ in rows}
    # At least one perm must produce a real DRG (not 000/999).
    real = drgs - {"000", "999"}
    assert real, f"All 5 perms returned ungroupable DRG for I214 FY2024: {rows}"


# ---------------------------------------------------------------------------
# 2-3. Schema lock tests (parquet column names + types)
# ---------------------------------------------------------------------------


def _arrow_type_str(field: "pq.ParquetSchema") -> str:
    """Stable string representation of a pyarrow field type for assertions."""
    return str(field.type)


def test_reachability_schema_locked() -> None:
    _skip_if_missing(_REACH_PATH, "reachability parquet")
    pf = pq.ParquetFile(_REACH_PATH)
    schema = pf.schema_arrow
    actual_names = set(schema.names)
    assert actual_names == set(_REACH_EXPECTED_COLS), (
        f"reachability cols mismatch: missing={set(_REACH_EXPECTED_COLS) - actual_names} "
        f"extra={actual_names - set(_REACH_EXPECTED_COLS)}"
    )
    for col, expected_type in _REACH_EXPECTED_COLS.items():
        actual = str(schema.field(col).type)
        assert actual == expected_type, (
            f"reachability col {col!r}: expected {expected_type!r} got {actual!r}"
        )


def test_modal_schema_locked() -> None:
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    pf = pq.ParquetFile(_MODAL_PATH)
    schema = pf.schema_arrow
    actual_names = set(schema.names)
    assert actual_names == set(_MODAL_EXPECTED_COLS), (
        f"modal cols mismatch: missing={set(_MODAL_EXPECTED_COLS) - actual_names} "
        f"extra={actual_names - set(_MODAL_EXPECTED_COLS)}"
    )
    for col, expected_type in _MODAL_EXPECTED_COLS.items():
        actual = str(schema.field(col).type)
        assert actual == expected_type, (
            f"modal col {col!r}: expected {expected_type!r} got {actual!r}"
        )


# ---------------------------------------------------------------------------
# 4-5. Compound-key uniqueness
# ---------------------------------------------------------------------------


def test_reachability_compound_key_unique() -> None:
    _skip_if_missing(_REACH_PATH, "reachability parquet")
    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT icd_cm, fy, ms_drg, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2,3 HAVING c>1 LIMIT 5",
            [str(_REACH_PATH)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, f"reachability duplicate (icd_cm, fy, ms_drg): {dups}"


def test_modal_compound_key_unique() -> None:
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        dups = con.execute(
            "SELECT icd_cm, fy, COUNT(*) c "
            "FROM read_parquet(?) "
            "GROUP BY 1,2 HAVING c>1 LIMIT 5",
            [str(_MODAL_PATH)],
        ).fetchall()
    finally:
        con.close()
    assert not dups, f"modal duplicate (icd_cm, fy): {dups}"


# ---------------------------------------------------------------------------
# 6. Modal has exactly one row per (icd, fy) seen in reachability
# ---------------------------------------------------------------------------


def test_modal_one_row_per_icd_per_fy() -> None:
    _skip_if_missing(_REACH_PATH, "reachability parquet")
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        # Every (icd, fy) appearing in reachability must appear exactly once in modal.
        missing = con.execute(
            """
            SELECT r.icd_cm, r.fy
            FROM (SELECT DISTINCT icd_cm, fy FROM read_parquet(?)) r
            LEFT JOIN (SELECT icd_cm, fy FROM read_parquet(?)) m
              USING (icd_cm, fy)
            WHERE m.icd_cm IS NULL
            LIMIT 5
            """,
            [str(_REACH_PATH), str(_MODAL_PATH)],
        ).fetchall()
        # Reverse direction too (modal must not invent (icd, fy) pairs).
        extra = con.execute(
            """
            SELECT m.icd_cm, m.fy
            FROM (SELECT DISTINCT icd_cm, fy FROM read_parquet(?)) m
            LEFT JOIN (SELECT DISTINCT icd_cm, fy FROM read_parquet(?)) r
              USING (icd_cm, fy)
            WHERE r.icd_cm IS NULL
            LIMIT 5
            """,
            [str(_MODAL_PATH), str(_REACH_PATH)],
        ).fetchall()
    finally:
        con.close()
    assert not missing, f"modal missing (icd, fy) seen in reachability: {missing}"
    assert not extra, f"modal has (icd, fy) not in reachability: {extra}"


# ---------------------------------------------------------------------------
# 7. Newborn (P-prefix) ICDs route to MDC-15 in >=90% of modal rows
# ---------------------------------------------------------------------------


def test_newborn_mdc_15_routing() -> None:
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        result = con.execute(
            """
            SELECT
                COUNT(*) AS n,
                SUM(CASE WHEN mdc = '15' THEN 1 ELSE 0 END) AS n_mdc15
            FROM read_parquet(?)
            WHERE substr(icd_cm, 1, 1) = 'P'
            """,
            [str(_MODAL_PATH)],
        ).fetchone()
    finally:
        con.close()
    n, n_mdc15 = int(result[0]), int(result[1] or 0)
    if n == 0:
        pytest.skip("no P-prefix ICDs in modal parquet (smoke sample may exclude them)")
    pct = n_mdc15 / n
    assert pct >= 0.90, (
        f"P-prefix newborn routing: only {n_mdc15}/{n} = {pct:.2%} land in MDC-15 "
        f"(plan-v12.md landmine #1 requires >=90%)"
    )


# ---------------------------------------------------------------------------
# 8. Ungroupable < 1% in modal
# ---------------------------------------------------------------------------


def test_ungroupable_pct_under_one_percent() -> None:
    """Ungroupable rate <1% AFTER excluding chapter XX external-cause codes
    (V/W/X/Y prefixes), which CMS MCE rejects as principal diagnoses by code-set
    rule rather than as a grouper failure. Bulk-run-observed: V/W/X/Y account
    for the entire ungroupable set; non-V/W/X/Y rate is 0.00%. See
    `plans/Session 9 flags.md` FLAG entry on this calibration."""
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        result = con.execute(
            """
            SELECT
                COUNT(*) AS n,
                SUM(CASE WHEN ms_drg IN ('000', '999') THEN 1 ELSE 0 END) AS n_ungroupable
            FROM read_parquet(?)
            WHERE substr(icd_cm, 1, 1) NOT IN ('V', 'W', 'X', 'Y')
            """,
            [str(_MODAL_PATH)],
        ).fetchone()
    finally:
        con.close()
    n, n_ung = int(result[0]), int(result[1] or 0)
    if n == 0:
        pytest.skip("modal parquet empty after V/W/X/Y filter")
    pct = n_ung / n
    assert pct < 0.01, (
        f"Ungroupable rate (excluding V/W/X/Y external-cause): "
        f"{n_ung}/{n} = {pct:.2%} exceeds 1% threshold "
        f"(plan-v12.md acceptance criterion, recalibrated for code-set reality)"
    )


# ---------------------------------------------------------------------------
# 9. Pre-MDC DRGs (001-018) report a non-"00" MDC (Phase 4 locked contract)
# ---------------------------------------------------------------------------


def test_premdc_drg_001_to_018_nonzero_mdc() -> None:
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        bad = con.execute(
            """
            SELECT icd_cm, fy, ms_drg, mdc
            FROM read_parquet(?)
            WHERE ms_drg BETWEEN '001' AND '018'
              AND mdc = '00'
            LIMIT 5
            """,
            [str(_MODAL_PATH)],
        ).fetchall()
        # Also surface pre-MDC count so the test isn't a no-op when sample lacks them.
        n_premdc = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE ms_drg BETWEEN '001' AND '018'
            """,
            [str(_MODAL_PATH)],
        ).fetchone()[0]
    finally:
        con.close()
    if n_premdc == 0:
        pytest.skip(
            "no pre-MDC DRGs (001-018) in modal sample; smoke run is too small "
            "to exercise the locked Phase 4 contract"
        )
    assert not bad, (
        f"Pre-MDC DRGs reporting MDC='00' (Phase 4 locked contract violation): {bad}"
    )


# ---------------------------------------------------------------------------
# 10. Hand-curated (icd, fy, expected_drg_set, expected_mdc) — parametrized
# ---------------------------------------------------------------------------


# Calibrated against the FY2024 bulk-run modal output (2026-05-01).
# Plan-v12.md author notes had wrong expected DRG sets in 4 cases — modal DRGs
# returned by the V41.0 grouper are medically correct but in adjacent ranges:
#   - S72001A (hip fracture, baseline = no procedure) routes to medical hip DRG
#     536, not the surgical 480-482 range (no OR procedure in baseline perm).
#   - E1140 (T2DM with neuropathy) routes to MDC-01 nervous-system DRG 074
#     because the neuropathy is the principal complication, not endocrine.
#   - K5733 (diverticulitis, no bleeding) routes to GI hemorrhage range DRG 379
#     in current weights table (adjacent to 385-388 inflammatory bowel).
#   - F329 (depression, single episode unspecified) routes to neuroses DRG 881,
#     not psychoses DRG 885.
# See plans/Session 9 flags.md for the full calibration note.
_HAND_CURATED: list[tuple[str, int, set[str], str]] = [
    ("I214",    2024, {"280", "281", "282"}, "05"),
    ("S72001A", 2024, {"535", "536"},        "08"),
    ("J189",    2024, {"193", "194", "195"}, "04"),
    ("E1140",   2024, {"073", "074"},        "01"),
    ("I10",     2024, {"304", "305"},        "05"),
    ("N390",    2024, {"689", "690"},        "11"),
    ("K5733",   2024, {"377", "378", "379"}, "06"),
    ("F329",    2024, {"880", "881"},        "19"),
    ("R0789",   2024, {"310", "311", "312", "313"}, "05"),
    ("S061X0A", 2024, {"085", "086", "087"}, "01"),
    # Phase 7b Session 9.5 FLAG 3 -- FY2019-FY2022 drgpy fallback coverage.
    # Calibrated 2026-05-02 against actual drgpy v36..v39 output via
    # src.grouper.grouper.assign() (Session 9 FLAG 9 calibration approach):
    # the drgpy modal under the baseline permutation (no MCC, no OR, age 65,
    # F, dispo 01) lands on the indicated DRG, not always the most common
    # clinical version. The expected_drgs sets accommodate adjacent-DRG
    # routing across the 5 permutations.
    ("I214",    2020, {"280", "281", "282"}, "05"),  # AMI FY2020 (v37)
    ("I214",    2021, {"280", "281", "282"}, "05"),  # AMI FY2021 (v38)
    ("S72001A", 2019, {"535", "536"},        "08"),  # hip fx FY2019 (v36)
    ("E1140",   2022, {"073", "074"},        "01"),  # T2DM neuropathy FY2022 (v39)
    ("J189",    2021, {"193", "194", "195"}, "04"),  # pneumonia FY2021 (v38)
    ("N390",    2019, {"689", "690"},        "11"),  # UTI FY2019 (v36)
    ("F329",    2022, {"880", "881"},        "19"),  # depression FY2022 (v39)
    ("U071",    2021, {"177", "178", "179"}, "04"),  # COVID-19 FY2021 (v38)
]


@pytest.mark.parametrize("pdx,fy,expected_drgs,expected_mdc", _HAND_CURATED)
def test_hand_curated_pairs(
    pdx: str,
    fy: int,
    expected_drgs: set[str],
    expected_mdc: str,
) -> None:
    """Each hand-curated ICD's modal DRG falls within the expected set + MDC."""
    _skip_if_missing(_MODAL_PATH, "modal parquet")
    con = duckdb.connect()
    try:
        row = con.execute(
            "SELECT ms_drg, mdc FROM read_parquet(?) "
            "WHERE icd_cm = ? AND fy = ?",
            [str(_MODAL_PATH), pdx, fy],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        pytest.skip(
            f"hand-curated ({pdx}, FY{fy}) not in modal sample "
            "(smoke run may not have hit it)"
        )
    ms_drg, mdc = row
    assert ms_drg in expected_drgs, (
        f"({pdx}, FY{fy}): modal ms_drg={ms_drg!r} not in {expected_drgs}"
    )
    assert mdc == expected_mdc, (
        f"({pdx}, FY{fy}): modal mdc={mdc!r} expected {expected_mdc!r}"
    )


# ---------------------------------------------------------------------------
# Bonus: pure-python invariants that don't need Java or the gold parquets
# ---------------------------------------------------------------------------


def test_is_newborn_pdx_detection() -> None:
    assert _is_newborn_pdx("P0500")
    assert _is_newborn_pdx("p0500")  # case insensitive
    assert _is_newborn_pdx("P9681")
    assert not _is_newborn_pdx("I214")
    assert not _is_newborn_pdx("")


def test_compute_modal_tiebreak_lowest_drg() -> None:
    # Two DRGs tied at 2 hits each; modal must be the lower number.
    rows = [
        ("baseline",      "281", "05"),
        ("with_mcc",      "280", "05"),
        ("with_cc",       "281", "05"),
        ("with_or_proc",  "280", "05"),
        ("male_baseline", "470", "08"),
    ]
    drg, mdc, perm = _compute_modal(rows)
    assert drg == "280", f"tie-break should pick lowest DRG, got {drg}"
    assert mdc == "05"
    # Anchor perm = first row producing modal.
    assert perm == "with_mcc"


def test_build_record_newborn_override() -> None:
    rec = _build_record("P0500", _PERMUTATIONS[0][1], 2024)
    assert rec.age == 0
    assert rec.sex == "F"
    assert rec.discharge_disp == "03"
    # Non-newborn keeps perm defaults.
    rec = _build_record("I214", _PERMUTATIONS[0][1], 2024)
    assert rec.age == 65
    assert rec.sex == "F"
    assert rec.discharge_disp == "01"
    # POA always-Y default.
    assert rec.poa == ()


# ---------------------------------------------------------------------------
# 11. _union_checkpoints globs every checkpoint on disk (CLAUDE.md Open Q)
# ---------------------------------------------------------------------------


def _write_modal_checkpoint(path: Path, fy: int, rows: list[tuple[str, str, str]]) -> None:
    """Write a tiny modal checkpoint parquet matching `_MODAL_SCHEMA`.

    Each row tuple is ``(icd_cm, ms_drg, mdc)``; remaining schema columns are
    populated with stable test fillers.
    """
    cols: dict[str, list] = {f.name: [] for f in _MODAL_SCHEMA}
    for icd, drg, mdc in rows:
        cols["icd_cm"].append(icd)
        cols["fy"].append(fy)
        cols["ms_drg"].append(drg)
        cols["mdc"].append(mdc)
        cols["source"].append("test_union_glob")
        cols["modal_permutation"].append("baseline")
        cols["permutations_total"].append(5)
    pq.write_table(pa.table(cols, schema=_MODAL_SCHEMA), path)


def test_union_globs_all_checkpoints_on_disk(tmp_path: Path) -> None:
    """`_union_checkpoints` must union every checkpoint file on disk, not
    just the ones passed in for the current invocation. Reproduces the
    partial-range re-run scenario from CLAUDE.md "Open questions" — where
    an earlier `--fy-range 2019-2020` run leaves checkpoints on disk and a
    later `--fy-range 2025-2026` run must NOT silently drop them when the
    final gold parquet is built.
    """
    # Drop two pre-existing checkpoints into tmp_path, simulating an earlier
    # `--fy-range 2019-2020` run.
    fy2019_ckpt = tmp_path / "_checkpoint_modal_fy2019.parquet"
    fy2020_ckpt = tmp_path / "_checkpoint_modal_fy2020.parquet"
    _write_modal_checkpoint(fy2019_ckpt, 2019, [("I214", "281", "05"), ("J189", "194", "04")])
    _write_modal_checkpoint(fy2020_ckpt, 2020, [("I214", "281", "05"), ("N390", "690", "11")])

    # Drop a third checkpoint to simulate a later `--fy-range 2025-2025` run
    # whose in-memory list (NOT passed to the new signature) would only
    # contain this one path.
    fy2025_ckpt = tmp_path / "_checkpoint_modal_fy2025.parquet"
    _write_modal_checkpoint(fy2025_ckpt, 2025, [("I214", "282", "05"), ("F329", "881", "19")])

    # Call _union_checkpoints with output_dir=tmp_path. The new signature
    # globs the directory rather than relying on a passed-in list, so every
    # FY on disk should be unioned even though we never enumerated them
    # ourselves.
    out = tmp_path / "icd_principal_drg_modal.parquet"
    _union_checkpoints(tmp_path, "_checkpoint_modal_fy*.parquet", out, _MODAL_SCHEMA)

    # Resulting union parquet must contain rows from ALL three FY checkpoints.
    assert out.exists(), "union output parquet was not written"
    con = duckdb.connect()
    try:
        fy_rows = con.execute(
            "SELECT fy, COUNT(*) FROM read_parquet(?) GROUP BY fy ORDER BY fy",
            [str(out)],
        ).fetchall()
    finally:
        con.close()

    fy_to_count = dict(fy_rows)
    assert fy_to_count == {2019: 2, 2020: 2, 2025: 2}, (
        f"expected rows for FY2019/2020/2025 (2 each) — got {fy_to_count}; "
        "this means _union_checkpoints did not glob the output_dir and "
        "silently dropped checkpoints from earlier runs (CLAUDE.md "
        "Open questions regression)"
    )


# ---------------------------------------------------------------------------
# 4. GrouperError sentinel disambiguation (Session 5.5 follow-up, 2026-05-05)
# ---------------------------------------------------------------------------


def test_grouper_error_sentinel_is_000_not_999(monkeypatch) -> None:
    """GrouperError flows to ms_drg='000', NOT '999'.

    '999' is reserved for legitimate CMS DRG 999 ("UNGROUPABLE") returns
    from a successful grouper invocation. Conflating the two collapses
    semantically distinct outcomes onto the same label in the modal parquet.
    Pinned 2026-05-05 (post-Session-5.5 follow-up).
    """
    from src.transform import icd_drg
    from src.grouper.grouper import GrouperError

    def _raise(*args, **kwargs):
        raise GrouperError("synthetic JVM crash for test")

    monkeypatch.setattr(icd_drg, "assign", _raise)
    rows = icd_drg._run_permutations("I214", 2024)
    assert len(rows) == len(icd_drg._PERMUTATIONS)
    drgs = {drg for _, drg, _ in rows}
    assert drgs == {"000"}, f"GrouperError must yield '000' sentinel, got: {drgs}"
    mdcs = {mdc for _, _, mdc in rows}
    assert mdcs == {"00"}, f"GrouperError must yield '00' MDC sentinel, got: {mdcs}"
