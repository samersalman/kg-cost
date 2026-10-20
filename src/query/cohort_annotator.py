"""Cohort cost annotator — Phase 10 Step 4.

Library + CLI that joins a research cohort (one row per patient) to the
KG-COST graph (or its underlying gold parquets) and appends 18 cost
columns: inpatient DRG + allowed/charge/CCR cost, plus per-CPT sums
across PFS facility / PFS non-facility / OPPS / ASC.

Two modes:
  * ``bolt``   — opens one Neo4j driver, reuses one session across rows.
                 Uses ``src.query._bolt.run_query`` for Q1/Q2/Q7 and a
                 small set of private inline queries for OPPS / ASC /
                 PFS-non-facility / inpatient-charge. Slow on >1k rows.
  * ``parquet``— reads ``data/gold/code_costs.parquet`` and
                 ``data/gold/icd_principal_drg_modal.parquet`` once at
                 startup, builds in-memory dict indexes keyed by
                 ``(code, year, setting)``. Bypasses the graph but
                 produces identical values.

Both modes MUST agree within float64 epsilon on any row where both
resolve a value — they read the same gold parquet via different
mechanisms (the graph is a 1:1 lift of code_costs.parquet for the
relevant setting/concept rows).

ICD-9-era rows (``dx_principal_system='ICD-9-CM'``) route through the
GEMs forward-bridge (Branch D in PHASES-2.md decision tree): ICD-9 ->
ICD-10-CM via ``_pick_best_gems_target`` -> modal DRG at max(fy, 2019)
-> cost lookup at max(fy, 2008). Resolved rows carry
``inpatient_confidence='approximate'`` and
``inpatient_allowed_source='gems_bridged_modal'``. Pre-2017 ICD-10 is
back-translated through the FY2019 modal index with native-FY cost
(Branch B; ``inpatient_allowed_source='gems_bridged_back_translated'``).
FY < 2017 ICD-10 is out-of-GEMs-era (Branch C); rows tag
``fy<N>_pre_icd10_era``.

The CLI now accepts ``--input-format ntdb`` for direct reads of
``annual_cohort.parquet`` via ``src.query._ntdb_adapter.load_ntdb_cohort``.
That path returns an already-typed DataFrame (Int64 ``fy``/``age``, etc.) so
the CSV-mode numeric-cast loop is skipped. ``--sample-rows N`` truncates the
input to the first N rows for smoke tests, ``--ntdb-duckdb PATH`` overrides
the NTDB DuckDB file used for procedure attachment, and the output writer
now dispatches on suffix (``.parquet``/``.pq`` -> parquet; otherwise CSV).
"""

from __future__ import annotations

import argparse
import functools
import math
import signal
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from src.config import ROOT

# ---------------------------------------------------------------------------
# Constants — output schema, parquet paths, thresholds
# ---------------------------------------------------------------------------

# Order matters: this is the column-append order in the output CSV.
_OUTPUT_COLUMNS: tuple[str, ...] = (
    "inpatient_drg",
    "inpatient_drg_title",
    "inpatient_allowed_mean",
    "inpatient_allowed_p10",
    "inpatient_allowed_p90",
    "inpatient_allowed_n",
    "inpatient_allowed_source",
    "inpatient_confidence",
    "inpatient_charge_mean",
    "inpatient_ccr_cost",
    "pfs_facility_allowed_total",
    "pfs_nonfacility_allowed_total",
    "outpatient_apc_allowed_total",
    "asc_allowed_total",
    "outpatient_confidence",
    "n_cpts_matched",
    "n_cpts_missing",
    "annotation_status",
    "annotation_notes",
)

_CODE_COSTS_PATH = ROOT / "data" / "gold" / "code_costs.parquet"
_ICD_MODAL_PATH = ROOT / "data" / "gold" / "icd_principal_drg_modal.parquet"
_DRG_TITLES_PATH = ROOT / "data" / "silver" / "ms_drg_weights.parquet"
_GEMS_PATH = ROOT / "data" / "silver" / "gems_2018.parquet"
_GEMS_OVERRIDES_PATH = ROOT / "src" / "query" / "gems_overrides.yaml"

# FYs without inpatient routing support (CLAUDE.md schema lock + Phase 7b
# coverage). ICD-10-CM modal only covers FY2019+; ICD-9 era is deferred.
_INPATIENT_MIN_FY = 2019

# Upper bound of the modal-DRG FY range — matches the max FY present in
# ``data/gold/icd_principal_drg_modal.parquet`` (FY2019..FY2026 per Session 9.5
# CLAUDE.md state). Used by Branch B (and any other lex-min modal sweep) to
# bound the FY iteration without re-querying the parquet at module import.
_MODAL_MAX_FY = 2026

# Bolt mode periodic-checkpoint cadence (rows).
_PARTIAL_CHECKPOINT_EVERY = 1000

# Phase 13 sensitivity-floor — confidence ranking used by --confidence-floor.
# Strict ordering "high" > "approximate" > "none". A row is rejected when its
# resolved ``inpatient_confidence`` rank is BELOW the floor's rank. ``"none"``
# floor (default) accepts every row (rank 0 is never < 0).
_CONFIDENCE_RANK: dict[str, int] = {"none": 0, "approximate": 1, "high": 2}
_CONFIDENCE_FLOOR_CHOICES: tuple[str, ...] = ("high", "approximate", "none")
_SCOPE_CHOICES: tuple[str, ...] = ("ip", "op", "all")

# Inpatient cost-bearing columns nulled (or reset, in the case of
# inpatient_confidence) when --confidence-floor rejects a row. Plan #3
# documents these as "the 11 inpatient cost cols"; the actual schema
# enumerates 10 columns prefixed ``inpatient_*``. We null 9 cost-bearing
# columns and reset ``inpatient_confidence`` to ``"none"`` to preserve
# the always-one-of-{high,approximate,none} schema invariant. Documented
# below in ``_apply_confidence_floor``.
_INPATIENT_COST_COLS_TO_NULL: tuple[str, ...] = (
    "inpatient_drg",
    "inpatient_drg_title",
    "inpatient_allowed_mean",
    "inpatient_allowed_p10",
    "inpatient_allowed_p90",
    "inpatient_allowed_n",
    "inpatient_allowed_source",
    "inpatient_charge_mean",
    "inpatient_ccr_cost",
)

# Outpatient/CPT cost totals zeroed when --scope=all fires alongside a
# confidence-floor rejection. Set to 0.0 (their NaN-free default), not NaN,
# to match the per-row dataclass default and keep downstream sums stable.
_OUTPATIENT_COST_COLS_TO_ZERO: tuple[str, ...] = (
    "pfs_facility_allowed_total",
    "pfs_nonfacility_allowed_total",
    "outpatient_apc_allowed_total",
    "asc_allowed_total",
)


# ---------------------------------------------------------------------------
# Inline private queries (kept here, NOT in sample_queries.cypher)
# ---------------------------------------------------------------------------

_PFS_NONFAC_QUERY = """
MATCH (h:HCPCS {code: $cpt})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'pfs_nonfacility'
  AND obs.concept = 'allowed_payment'
  AND obs.year = $cy
RETURN obs.value AS allowed
LIMIT 1
"""

_OPPS_QUERY = """
MATCH (h:HCPCS {code: $cpt})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'opps'
  AND obs.concept = 'allowed_payment'
  AND obs.year = $cy
RETURN obs.value AS allowed
LIMIT 1
"""

_ASC_QUERY = """
MATCH (h:HCPCS {code: $cpt})-[:OBSERVED_AS]->(obs:CostObservation)
WHERE obs.setting = 'asc'
  AND obs.concept = 'allowed_payment'
  AND obs.year = $cy
RETURN obs.value AS allowed
LIMIT 1
"""

_INPATIENT_CHARGE_QUERY = """
MATCH (icd:ICDCMCode {code: $icd_code, code_system: 'ICD-10-CM'})-[r:IS_MODAL_FOR]->(drg:MSDRG)
WHERE r.fy = $fy AND drg.fy = $fy
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'charge', year: $fy})
RETURN drg.code AS msdrg, obs.value AS charge
LIMIT 1
"""

# Bolt-mode query for direct DRG cost lookup (used when grouper has assigned
# an exact DRG from PCS-bearing rows; avoids the modal-DRG hop).
_DRG_COST_QUERY = """
MATCH (drg:MSDRG {code: $drg, fy: $fy})
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(obs:CostObservation {concept: 'allowed_payment', year: $fy})
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(charge_obs:CostObservation {concept: 'charge', year: $fy})
OPTIONAL MATCH (drg)-[:OBSERVED_AS]->(ccr_obs:CostObservation {concept: 'ccr_cost', year: $fy})
RETURN drg.code AS msdrg,
       drg.title AS title,
       obs.value AS allowed,
       obs.p10 AS p10,
       obs.p90 AS p90,
       obs.n AS n,
       obs.source AS source,
       charge_obs.value AS charge,
       ccr_obs.value AS ccr_cost
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class AnnotationResult:
    df: pd.DataFrame
    coverage: float
    n_rows: int
    n_ok: int
    n_partial: int
    n_no_match: int
    unmatched_rows: list[tuple[int, str]] = field(default_factory=list)
    mode: str = "parquet"


# ---------------------------------------------------------------------------
# Per-row resolution dataclasses
# ---------------------------------------------------------------------------


@dataclass
class _InpatientCols:
    drg: Optional[str] = None
    drg_title: Optional[str] = None
    allowed_mean: float = math.nan
    allowed_p10: float = math.nan
    allowed_p90: float = math.nan
    allowed_n: Optional[int] = None
    allowed_source: Optional[str] = None
    charge_mean: float = math.nan
    ccr_cost: float = math.nan
    resolved: bool = False  # True iff DRG was looked up (cost may be NaN)


@dataclass
class _CptCols:
    pfs_facility_total: float = 0.0
    pfs_nonfacility_total: float = 0.0
    opps_total: float = 0.0
    asc_total: float = 0.0
    n_matched: int = 0
    n_missing: int = 0
    # Per-setting "any match" flags (used for partial/ok logic)
    any_pfs_facility: bool = False
    any_pfs_nonfacility: bool = False
    any_opps: bool = False
    any_asc: bool = False


# ---------------------------------------------------------------------------
# Lookup backends — abstract over bolt-vs-parquet
# ---------------------------------------------------------------------------


class _LookupBackend:
    """Interface. Two implementations: parquet (in-memory dicts) + bolt."""

    mode: str = "parquet"

    def lookup_inpatient(self, icd_code: str, fy: int) -> _InpatientCols:
        raise NotImplementedError

    def lookup_drg_cost(self, drg: str, fy: int) -> _InpatientCols:
        """Look up cost for a specific MS-DRG (not via modal anchor).

        Used when the grouper has computed an exact DRG from a PCS-bearing
        inpatient row. Returns an unresolved ``_InpatientCols`` if the DRG
        is absent from the graph at that FY.
        """
        raise NotImplementedError

    def lookup_cpt_setting(self, cpt: str, year: int, setting: str) -> Optional[float]:
        """Return allowed_payment for (cpt, year, setting), or None if absent.

        ``setting`` is one of ``pfs_facility``, ``pfs_nonfacility``, ``opps``,
        ``asc``. NaN values in the underlying data are treated as "absent"
        (return None) since NaN means "not in graph for that setting".
        """
        raise NotImplementedError

    def lookup_gems_targets(self, icd9: str) -> list[dict]:
        """Forward-GEMs ICD-9-CM -> ICD-10-CM targets (non-reimbursement).

        Returns a list of dicts (possibly empty) with at least keys
        ``target_code``, ``approximate_flag``, ``no_map_flag``,
        ``combination_flag``. Order is the source-row order from
        ``data/silver/gems_2018.parquet``.
        """
        raise NotImplementedError

    def lookup_modal_drg(self, icd_cm: str, fy: int) -> Optional[str]:
        """Modal MS-DRG for an ICD-10-CM principal at a given FY.

        Returns the DRG code as str, or None if (icd_cm, fy) is not in the
        modal index. This is a thin lookup primitive separate from
        ``lookup_inpatient`` (which also pulls cost columns).
        """
        raise NotImplementedError

    def close(self) -> None:
        pass


# -------- parquet backend ---------------------------------------------------


class _ParquetBackend(_LookupBackend):
    mode = "parquet"

    def __init__(self) -> None:
        if not _CODE_COSTS_PATH.exists():
            raise FileNotFoundError(
                f"code_costs.parquet not found at {_CODE_COSTS_PATH}. "
                f"Run `python3 -m src.transform.code_costs` first."
            )
        if not _ICD_MODAL_PATH.exists():
            raise FileNotFoundError(
                f"icd_principal_drg_modal.parquet not found at {_ICD_MODAL_PATH}. "
                f"Run `python3 -m src.transform.icd_drg --fy-range 2019-2026` first."
            )
        if not _GEMS_PATH.exists():
            raise FileNotFoundError(
                f"gems_2018.parquet not found at {_GEMS_PATH}. "
                f"Run `python -m src.extract.gems` first."
            )
        # Lazy import duckdb so library can be imported without it for the bolt path
        import duckdb

        con = duckdb.connect(":memory:")

        # Cost lookup: (code, year, setting) -> dict of cost columns
        cost_df = con.execute(
            f"""
            SELECT code, fy_or_cy AS year, setting,
                   charge, allowed_payment, allowed_payment_n,
                   allowed_payment_source, payment_p10, payment_p90,
                   ccr_cost
            FROM read_parquet('{_CODE_COSTS_PATH.as_posix()}')
            """
        ).fetchdf()
        self._cost_index: dict[tuple[str, int, str], dict[str, Any]] = {
            (str(r.code), int(r.year), str(r.setting)): {
                "charge": r.charge,
                "allowed_payment": r.allowed_payment,
                "allowed_payment_n": r.allowed_payment_n,
                "allowed_payment_source": r.allowed_payment_source,
                "payment_p10": r.payment_p10,
                "payment_p90": r.payment_p90,
                "ccr_cost": r.ccr_cost,
            }
            for r in cost_df.itertuples(index=False)
        }

        # Modal DRG lookup: (icd_cm, fy) -> ms_drg
        modal_df = con.execute(
            f"""
            SELECT icd_cm, fy, ms_drg
            FROM read_parquet('{_ICD_MODAL_PATH.as_posix()}')
            """
        ).fetchdf()
        self._modal_index: dict[tuple[str, int], str] = {
            (str(r.icd_cm), int(r.fy)): str(r.ms_drg)
            for r in modal_df.itertuples(index=False)
        }

        # DRG titles: (ms_drg, fy) -> title (best-effort; titles in silver
        # ms_drg_weights.parquet keyed by (ms_drg, fy))
        self._title_index: dict[tuple[str, int], str] = {}
        if _DRG_TITLES_PATH.exists():
            title_df = con.execute(
                f"""
                SELECT ms_drg, fy, title
                FROM read_parquet('{_DRG_TITLES_PATH.as_posix()}')
                WHERE title IS NOT NULL
                """
            ).fetchdf()
            self._title_index = {
                (str(r.ms_drg), int(r.fy)): str(r.title)
                for r in title_df.itertuples(index=False)
            }

        # GEMs forward index: ICD-9-CM source_code -> list of forward target
        # dicts (non-reimbursement direction). Phase 12 scaffold (Session 1);
        # Session 2 will consume this for ICD-9 cohort routing.
        gems_df = con.execute(
            f"""
            SELECT source_code, target_code, approximate_flag, no_map_flag,
                   combination_flag, scenario, choice_list
            FROM read_parquet('{_GEMS_PATH.as_posix()}')
            WHERE source_system = 'ICD-9-CM'
              AND target_system = 'ICD-10-CM'
              AND is_reimbursement = false
            """
        ).fetchdf()
        self._gems_index: dict[str, list[dict]] = {}
        for r in gems_df.itertuples(index=False):
            entry = {
                "target_code": str(r.target_code) if r.target_code is not None else None,
                "approximate_flag": bool(r.approximate_flag) if r.approximate_flag is not None else None,
                "no_map_flag": bool(r.no_map_flag) if r.no_map_flag is not None else None,
                "combination_flag": bool(r.combination_flag) if r.combination_flag is not None else None,
                "scenario": _coerce_int(r.scenario),
                "choice_list": _coerce_int(r.choice_list),
            }
            key = str(r.source_code)
            self._gems_index.setdefault(key, []).append(entry)
        con.close()

    def lookup_inpatient(self, icd_code: str, fy: int) -> _InpatientCols:
        cols = _InpatientCols()
        drg = self._modal_index.get((_normalize_icd10cm(icd_code), fy))
        if drg is None:
            return cols
        cols.drg = drg
        cols.resolved = True
        cols.drg_title = self._title_index.get((drg, fy))
        cost = self._cost_index.get((drg, fy, "inpatient"))
        if cost is not None:
            cols.allowed_mean = _coerce_float(cost.get("allowed_payment"))
            cols.allowed_p10 = _coerce_float(cost.get("payment_p10"))
            cols.allowed_p90 = _coerce_float(cost.get("payment_p90"))
            cols.allowed_n = _coerce_int(cost.get("allowed_payment_n"))
            cols.allowed_source = _coerce_str(cost.get("allowed_payment_source"))
            cols.charge_mean = _coerce_float(cost.get("charge"))
            cols.ccr_cost = _coerce_float(cost.get("ccr_cost"))
        return cols

    def lookup_drg_cost(self, drg: str, fy: int) -> _InpatientCols:
        cols = _InpatientCols()
        cols.drg = drg
        cols.resolved = True
        cols.drg_title = self._title_index.get((drg, fy))
        cost = self._cost_index.get((drg, fy, "inpatient"))
        if cost is not None:
            cols.allowed_mean = _coerce_float(cost.get("allowed_payment"))
            cols.allowed_p10 = _coerce_float(cost.get("payment_p10"))
            cols.allowed_p90 = _coerce_float(cost.get("payment_p90"))
            cols.allowed_n = _coerce_int(cost.get("allowed_payment_n"))
            cols.allowed_source = _coerce_str(cost.get("allowed_payment_source"))
            cols.charge_mean = _coerce_float(cost.get("charge"))
            cols.ccr_cost = _coerce_float(cost.get("ccr_cost"))
        return cols

    def lookup_cpt_setting(self, cpt: str, year: int, setting: str) -> Optional[float]:
        cost = self._cost_index.get((cpt, year, setting))
        if cost is None:
            return None
        v = _coerce_float(cost.get("allowed_payment"))
        if math.isnan(v):
            return None
        return v

    def lookup_gems_targets(self, icd9: str) -> list[dict]:
        return list(self._gems_index.get(_normalize_icd9(icd9), []))

    def lookup_modal_drg(self, icd_cm: str, fy: int) -> Optional[str]:
        return self._modal_index.get((_normalize_icd10cm(icd_cm), int(fy)))


# -------- bolt backend ------------------------------------------------------


class _BoltBackend(_LookupBackend):
    mode = "bolt"

    def __init__(self) -> None:
        # Lazy import so parquet-only callers don't need neo4j installed
        from src.query import _bolt

        self._bolt = _bolt
        self._driver = _bolt.get_driver()
        # Per-instance caches for repeat lookups across cohort rows. Kept
        # as plain dicts (not lru_cache on a bound method) so the lifecycle
        # is tied to the backend instance and there's no cross-test leakage.
        self._gems_cache: dict[str, list[dict]] = {}
        self._modal_cache: dict[tuple[str, int], Optional[str]] = {}

    def lookup_inpatient(self, icd_code: str, fy: int) -> _InpatientCols:
        cols = _InpatientCols()
        # Normalize once for all Cypher params. The graph's :ICDCMCode
        # nodes use the dotless canonical form (e.g. "S065X1A") while
        # NTDB delivers dotted codes ("S06.5X1A").
        icd_code_norm = _normalize_icd10cm(icd_code)
        # Q1 — modal DRG with allowed cost
        rows = self._bolt.run_query(
            self._driver, "Q1_icd_modal_cost", icd_code=icd_code_norm, fy=fy
        )
        if not rows:
            return cols
        r = rows[0]
        if r.get("msdrg") is None:
            return cols
        cols.drg = str(r["msdrg"])
        cols.resolved = True
        cols.drg_title = r.get("title")
        cols.allowed_mean = _coerce_float(r.get("allowed"))
        cols.allowed_p10 = _coerce_float(r.get("p10"))
        cols.allowed_p90 = _coerce_float(r.get("p90"))
        cols.allowed_n = _coerce_int(r.get("n"))

        # Source: Q1 doesn't return it directly; pull via a one-shot Cypher
        # against the same observation. Cheap (single hop).
        with self._driver.session() as sess:
            src_row = sess.run(
                """
                MATCH (drg:MSDRG {code: $drg, fy: $fy})-[:OBSERVED_AS]->(obs:CostObservation)
                WHERE obs.concept = 'allowed_payment' AND obs.year = $fy
                RETURN obs.source AS source LIMIT 1
                """,
                drg=cols.drg,
                fy=fy,
            ).single()
            if src_row is not None:
                cols.allowed_source = src_row.get("source")

        # Inpatient charge
        with self._driver.session() as sess:
            charge_row = sess.run(
                _INPATIENT_CHARGE_QUERY, icd_code=icd_code_norm, fy=fy
            ).single()
            if charge_row is not None:
                cols.charge_mean = _coerce_float(charge_row.get("charge"))

        # Q7 — CCR-adjusted cost (concept='ccr_cost')
        ccr_rows = self._bolt.run_query(
            self._driver, "Q7_ccr_adjusted_cost", icd_code=icd_code_norm, fy=fy
        )
        if ccr_rows:
            cols.ccr_cost = _coerce_float(ccr_rows[0].get("ccr_cost"))
        return cols

    def lookup_drg_cost(self, drg: str, fy: int) -> _InpatientCols:
        cols = _InpatientCols()
        with self._driver.session() as sess:
            row = sess.run(_DRG_COST_QUERY, drg=drg, fy=fy).single()
        if row is None or row.get("msdrg") is None:
            return cols
        cols.drg = drg
        cols.resolved = True
        cols.drg_title = row.get("title")
        cols.allowed_mean = _coerce_float(row.get("allowed"))
        cols.allowed_p10 = _coerce_float(row.get("p10"))
        cols.allowed_p90 = _coerce_float(row.get("p90"))
        cols.allowed_n = _coerce_int(row.get("n"))
        cols.allowed_source = row.get("source")
        cols.charge_mean = _coerce_float(row.get("charge"))
        cols.ccr_cost = _coerce_float(row.get("ccr_cost"))
        return cols

    def lookup_cpt_setting(self, cpt: str, year: int, setting: str) -> Optional[float]:
        if setting == "pfs_facility":
            rows = self._bolt.run_query(
                self._driver, "Q2_cpt_pfs_facility", cpt=cpt, cy=year
            )
            if not rows:
                return None
            v = _coerce_float(rows[0].get("allowed"))
        elif setting == "pfs_nonfacility":
            with self._driver.session() as sess:
                row = sess.run(_PFS_NONFAC_QUERY, cpt=cpt, cy=year).single()
            if row is None:
                return None
            v = _coerce_float(row.get("allowed"))
        elif setting == "opps":
            with self._driver.session() as sess:
                row = sess.run(_OPPS_QUERY, cpt=cpt, cy=year).single()
            if row is None:
                return None
            v = _coerce_float(row.get("allowed"))
        elif setting == "asc":
            with self._driver.session() as sess:
                row = sess.run(_ASC_QUERY, cpt=cpt, cy=year).single()
            if row is None:
                return None
            v = _coerce_float(row.get("allowed"))
        else:
            raise ValueError(f"unknown setting: {setting}")
        if math.isnan(v):
            return None
        return v

    def lookup_gems_targets(self, icd9: str) -> list[dict]:
        """Forward-GEMs lookup via Q5_gems_forward; per-instance cached.

        Normalizes the Q5 result-row alias ``target`` -> ``target_code`` so
        the returned dict shape matches what ``_pick_best_gems_target`` and
        the parquet backend expect. Returns a defensive copy on cache hit
        so callers can't mutate the cached entries.
        """
        # Normalize once; cache key and Cypher parameter MUST agree
        # (Session 3.5 FLAG 1 Risk #2 — see plans/Session 3 flags.md).
        key = _normalize_icd9(icd9)
        if key in self._gems_cache:
            return [dict(r) for r in self._gems_cache[key]]
        rows = self._bolt.run_query(
            self._driver, "Q5_gems_forward", icd9=key
        )
        out: list[dict] = []
        for r in rows:
            out.append({
                "target_code": r.get("target"),
                "approximate_flag": r.get("approximate_flag"),
                "no_map_flag": r.get("no_map_flag"),
                "combination_flag": r.get("combination_flag"),
                "scenario": r.get("scenario"),
                "choice_list": r.get("choice_list"),
            })
        self._gems_cache[key] = out
        return [dict(r) for r in out]

    def lookup_modal_drg(self, icd_cm: str, fy: int) -> Optional[str]:
        """Modal MS-DRG lookup via IS_MODAL_FOR; per-instance cached.

        One-shot inline Cypher (not a sample-queries entry) since the
        result is a scalar DRG code, not a result-set. Returns None when
        the (icd_cm, fy) pair is absent from the modal index.
        """
        # Normalize once; cache key and Cypher parameter MUST agree.
        # The graph's :ICDCMCode nodes use the dotless canonical form
        # from codes_master, while NTDB delivers dotted codes ("S06.5X1A").
        key = (_normalize_icd10cm(icd_cm), int(fy))
        if key in self._modal_cache:
            return self._modal_cache[key]
        with self._driver.session() as sess:
            row = sess.run(
                "MATCH (i:ICDCMCode {code:$c, code_system:'ICD-10-CM'})"
                "-[r:IS_MODAL_FOR]->(d:MSDRG) "
                "WHERE r.fy=$fy RETURN d.code AS drg LIMIT 1",
                c=key[0], fy=key[1],
            ).single()
        drg = str(row["drg"]) if row and row.get("drg") is not None else None
        self._modal_cache[key] = drg
        return drg

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Coercion helpers — silver/gold parquet values come back via duckdb as a
# mix of numpy / Python types; coerce to plain floats / ints / str / NaN.
# ---------------------------------------------------------------------------


def _coerce_float(v: Any) -> float:
    if v is None:
        return math.nan
    try:
        f = float(v)
    except (TypeError, ValueError):
        return math.nan
    if math.isnan(f):
        return math.nan
    return f


def _coerce_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _coerce_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s == "" or s.lower() == "nan":
        return None
    return s


def _normalize_icd9(code: Optional[str]) -> str:
    """Strip the conventional decimal point from ICD-9-CM codes.

    GEMs silver (``data/silver/gems_2018.parquet``) stores ICD-9-CM
    source codes in no-decimal form (e.g. ``9160``); some upstream
    sources (notably NTDB ``annual_cohort``) deliver them with the
    decimal embedded (``916.0``). This helper normalizes either form to
    the no-decimal canonical key used by the GEMs index. See Session 3.5
    FLAG 1 (plans/Session 3 flags.md).
    """
    if code is None:
        return ""
    return str(code).replace(".", "")


def _normalize_icd10cm(code: Optional[str]) -> str:
    """Normalize ICD-10-CM code for lookup against the modal/cost index.

    Strips ``.``, uppercases, and converts None to empty string. Mirrors
    _normalize_icd9 for ICD-9-CM. Required because NTDB stores codes with
    dots ("S06.5X1A") while the index uses dotless ("S065X1A").
    """
    if code is None:
        return ""
    return str(code).replace(".", "").upper()


def _lex_min_modal_drg(
    backend: "_LookupBackend",
    icd_cm: str,
    min_fy: int,
    max_fy: int,
) -> tuple[Optional[str], Optional[int]]:
    """Resolve modal DRG via lex-min over FYs in ``[min_fy, max_fy]``.

    Iterates all FYs in the inclusive range, collects every
    ``(drg, fy)`` returned by ``backend.lookup_modal_drg`` (skipping
    ``None`` results), and returns the lexicographically smallest DRG
    paired with the FIRST FY (ascending) that produced it. Returns
    ``(None, None)`` when no FY yields a modal DRG.

    Used by Branch B (back-translation of FY2017-FY2018 ICD-10-CM
    cohorts) to widen the modal proxy beyond the original FY2019-only
    lookup. Closes the ``icd10_back_translation_no_modal`` gap for
    trauma S/T-prefix codes that have empty FY2019 modal but populate
    FY2020+. Cost lookup at the call site stays at the encounter's
    native FY — only modal DRG resolution is widened.
    """
    icd_cm_norm = _normalize_icd10cm(icd_cm)
    hits: list[tuple[str, int]] = []  # (drg, fy)
    for fy in range(min_fy, max_fy + 1):
        drg = backend.lookup_modal_drg(icd_cm_norm, fy)
        if drg is not None:
            hits.append((drg, fy))
    if not hits:
        return (None, None)
    lex_min_drg = min(drg for drg, _ in hits)
    hit_fy = next(fy for drg, fy in hits if drg == lex_min_drg)
    return (lex_min_drg, hit_fy)


# ---------------------------------------------------------------------------
# GEMs disambiguation — manual override layer + 6-tier picker
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _load_gems_overrides() -> dict[str, str]:
    """Load the manual ICD-9 -> ICD-10 override layer.

    Flattens both top-level mappings and nested sections (e.g. ``trauma:``)
    into a single ``{icd9: icd10}`` dict. Empty file returns ``{}``.
    Cached for the process lifetime via functools.lru_cache.

    Defensive: missing file or empty YAML -> ``{}``. If a value is itself a
    dict (a section like ``trauma: {...}``), recurse one level and merge
    its mappings into the flat result. Non-string values otherwise are
    skipped silently.
    """
    if not _GEMS_OVERRIDES_PATH.exists():
        return {}
    try:
        import yaml
    except ImportError:
        return {}
    try:
        with _GEMS_OVERRIDES_PATH.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except Exception:
        return {}
    if not raw or not isinstance(raw, dict):
        return {}
    flat: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(v, str):
            flat[str(k)] = v
        elif isinstance(v, dict):
            # Nested section (e.g. trauma: {...}); recurse one level only.
            for inner_k, inner_v in v.items():
                if isinstance(inner_v, str):
                    flat[str(inner_k)] = inner_v
        # else: skip (numbers, lists, None) — nothing actionable
    return flat


def _pick_best_gems_target(
    targets: list[dict],
    icd9: str,
    overrides: dict[str, str],
) -> Optional[dict]:
    """6-tier GEMs disambiguator — returns best ICD-10-CM target dict or None.

    Order (Decision #1 in plan):
      * override   — manual override layer wins outright
      * Tier 0     — all candidates have no_map_flag=True -> None
      * Tier 1     — narrow to (scenario==1 AND choice_list==1)
      * Tier 2     — prefer combination_flag=False
      * Tier 3     — prefer approximate_flag=False
      * Tier 4     — longest target_code within the same root (first 3 chars)
      * Tier 5     — lex-min on target_code (final tiebreak)

    Return shape: ``{"target_code": str, "tier": int_or_str,
    "truncated_n": int_or_None}``. ``truncated_n`` is the count of
    Tier-1-surviving candidates when ``combination_flag`` is True (caller
    decides whether to emit the ``gems_combination_truncated`` tag).
    Override hits return ``tier="override"``; non-override hits return
    the highest tier that did meaningful narrowing.
    """
    # Override-first: short-circuit before consulting GEMs candidates.
    # Defensive: support both `9160:` and `916.0:` YAML key forms by
    # normalizing both sides of the lookup. The lookup input AND every
    # override key get decimal-stripped before comparison
    # (Session 3.5 FLAG 1 — plans/Session 3 flags.md).
    icd9_key = _normalize_icd9(icd9)
    override_target: Optional[str] = None
    for ok, ov in overrides.items():
        if _normalize_icd9(ok) == icd9_key:
            override_target = ov
            break
    if override_target is not None:
        return {
            "target_code": str(override_target),
            "tier": "override",
            "truncated_n": None,
        }

    if not targets:
        return None

    # Tier 0: all candidates are no-map -> no usable target.
    if all(bool(t.get("no_map_flag")) for t in targets):
        return None

    # Drop sentinel / no-map rows from the working set so they can't slip
    # through the cascade as the "shortest" / "lex-min" winner.
    working = [t for t in targets if not t.get("no_map_flag")]
    if not working:
        return None

    # Tier 1: narrow to (scenario==1, choice_list==1) — the CMS canonical
    # first-cluster for combination targets.
    tier1 = [
        t for t in working
        if t.get("scenario") == 1 and t.get("choice_list") == 1
    ]
    if tier1:
        narrowed = tier1
        achieved_tier: int = 1
    else:
        # Fall through; better some answer than none.
        narrowed = working
        achieved_tier = 0  # we'll bump as later tiers narrow

    # Combination-truncation signal: if any surviving candidate is part of a
    # combination (combination_flag=True) AND we have >1 candidates, expose
    # the count so the caller can emit gems_combination_truncated.
    truncated_n: Optional[int] = None
    if len(narrowed) > 1 and any(bool(t.get("combination_flag")) for t in narrowed):
        truncated_n = len(narrowed)

    # Tier 2: prefer combination_flag=False (single-target rows beat
    # combination-cluster rows).
    tier2 = [t for t in narrowed if not bool(t.get("combination_flag"))]
    if tier2 and len(tier2) < len(narrowed):
        narrowed = tier2
        achieved_tier = max(achieved_tier, 2)
    elif tier2:
        narrowed = tier2

    # Tier 3: prefer approximate_flag=False (exact maps beat approximate).
    tier3 = [t for t in narrowed if not bool(t.get("approximate_flag"))]
    if tier3 and len(tier3) < len(narrowed):
        narrowed = tier3
        achieved_tier = max(achieved_tier, 3)
    elif tier3:
        narrowed = tier3

    if len(narrowed) == 1:
        winner = narrowed[0]
        return {
            "target_code": str(winner["target_code"]),
            "tier": max(achieved_tier, 1) if tier1 else achieved_tier,
            "truncated_n": truncated_n,
        }

    # Tier 4: longest target_code within the same 3-char root.
    # Group by root; if a single root dominates, restrict; then take longest.
    def _root(t: dict) -> str:
        code = str(t.get("target_code") or "")
        return code[:3]

    roots = {_root(t) for t in narrowed}
    if len(roots) == 1:
        max_len = max(len(str(t.get("target_code") or "")) for t in narrowed)
        tier4 = [t for t in narrowed if len(str(t.get("target_code") or "")) == max_len]
        if tier4 and len(tier4) < len(narrowed):
            narrowed = tier4
            achieved_tier = max(achieved_tier, 4)
        elif tier4:
            narrowed = tier4

    # Tier 5: final lex-min tiebreak on target_code.
    if len(narrowed) > 1:
        narrowed = sorted(narrowed, key=lambda t: str(t.get("target_code") or ""))
        achieved_tier = max(achieved_tier, 5)

    winner = narrowed[0]
    final_tier = achieved_tier if achieved_tier > 0 else 1
    return {
        "target_code": str(winner["target_code"]),
        "tier": final_tier,
        "truncated_n": truncated_n,
    }


# ---------------------------------------------------------------------------
# Grouper integration — lazy JVM, never raises
# ---------------------------------------------------------------------------

# Lazy grouper import — JVM is heavyweight; only spin up on first use.
_grouper_module = None


def _get_grouper():
    global _grouper_module
    if _grouper_module is None:
        from src.grouper import grouper as _g
        _grouper_module = _g
    return _grouper_module


def _coerce_sex(v: Any) -> str:
    """Cohort schema: 1=M, 2=F. Grouper expects 'M' / 'F' / 'U'."""
    if v in (1, "1"):
        return "M"
    if v in (2, "2"):
        return "F"
    return "U"


def _coerce_disposition(v: Any) -> str:
    """UB-04 2-digit code as string, default '01' (discharged home)."""
    s = _coerce_str(v)
    if s is None or s == "":
        return "01"
    # Zero-pad to 2 chars
    return s.zfill(2)[:2]


def _build_claim_record(row: pd.Series, fy: int):
    """Build a grouper ClaimRecord from a cohort row.

    Caller has already verified ICD-10-CM dx + ICD-10-PCS pcs + fy >= 2019.
    """
    g = _get_grouper()
    pdx = _coerce_str(row.get("dx_principal")) or ""
    sdx = tuple(_split_codes(row.get("dx_secondary")))
    pcs = tuple(_split_codes(row.get("pcs")))
    sex = _coerce_sex(row.get("sex"))
    age = _coerce_int(row.get("age")) or 0
    disp = _coerce_disposition(row.get("discharge_disp"))
    return g.ClaimRecord(
        pdx=pdx,
        sdx=sdx,
        pcs=pcs,
        sex=sex,
        age=age,
        discharge_disp=disp,
        los=1,  # cohort schema doesn't carry LOS; grouper doesn't consume on V43.1
    )


def _invoke_grouper(row: pd.Series, fy: int) -> Optional[str]:
    """Run grouper.assign and return the DRG code, or None on failure /
    ungroupable.

    Never raises — grouper errors fall back to modal routing in the caller.
    """
    try:
        g = _get_grouper()
        record = _build_claim_record(row, fy)
        result = g.assign(record, fy=fy)
        drg = result.ms_drg
        # "000" is the ungroupable sentinel; treat as None to fall back to modal
        if not drg or drg == "000":
            return None
        return str(drg)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Per-row routing
# ---------------------------------------------------------------------------


def _split_codes(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [c.strip() for c in s.split(";") if c.strip()]


def _resolve_cpt_settings(
    backend: _LookupBackend, cpt_list: list[str], year: int
) -> _CptCols:
    cols = _CptCols()
    for cpt in cpt_list:
        per_cpt_matched = False
        for setting, attr_total, attr_any in (
            ("pfs_facility", "pfs_facility_total", "any_pfs_facility"),
            ("pfs_nonfacility", "pfs_nonfacility_total", "any_pfs_nonfacility"),
            ("opps", "opps_total", "any_opps"),
            ("asc", "asc_total", "any_asc"),
        ):
            v = backend.lookup_cpt_setting(cpt, year, setting)
            if v is not None:
                setattr(cols, attr_total, getattr(cols, attr_total) + v)
                setattr(cols, attr_any, True)
                per_cpt_matched = True
        if per_cpt_matched:
            cols.n_matched += 1
        else:
            cols.n_missing += 1
    return cols


def _annotate_row(
    row: pd.Series, backend: _LookupBackend, default_year: int, fy_column: str
) -> dict[str, Any]:
    """Return a dict with the 19 output columns for one cohort row."""
    out: dict[str, Any] = {col: math.nan for col in _OUTPUT_COLUMNS}
    out["inpatient_drg"] = ""
    out["inpatient_drg_title"] = ""
    out["inpatient_allowed_source"] = ""
    # Phase 12 scaffold (Session 1): default to "none". Session 2's
    # decision tree (A/B/C/D in PHASES-2.md:89-132) overwrites this per
    # row — high (native FY≥2019), approximate (GEMs-bridged or FY2017-2018
    # back-translation), or stays "none" when no route resolves. "none" is
    # an explicit out-of-band sentinel that downstream sensitivity
    # analyses (the downstream fracture-cost study, Phase 14) can filter on.
    out["inpatient_confidence"] = "none"
    # Phase 12 (Session 2): outpatient confidence is reserved-schema only; populated in Phase 8+.
    out["outpatient_confidence"] = None
    out["annotation_status"] = "no_match"
    out["annotation_notes"] = ""
    out["n_cpts_matched"] = 0
    out["n_cpts_missing"] = 0

    notes: list[str] = []

    # FY resolution
    fy_raw = row.get(fy_column)
    fy: int
    if fy_raw is None or (isinstance(fy_raw, float) and math.isnan(fy_raw)) or fy_raw == "":
        fy = default_year
    else:
        try:
            fy = int(fy_raw)
        except (TypeError, ValueError):
            fy = default_year

    dx_principal = _coerce_str(row.get("dx_principal")) or ""
    dx_system = _coerce_str(row.get("dx_principal_system")) or ""
    cpts_raw = row.get("cpts")

    # ---- Inpatient routing ------------------------------------------------
    # Decision tree A/B/C/D (PHASES-2.md:89-132 + Session 2 plan):
    #   A — ICD-10-CM, FY >= 2019: native (grouper-or-modal). confidence=high
    #   B — ICD-10-CM, 2017 <= FY < 2019: back-translate via FY2019 modal,
    #       native-FY cost. confidence=approximate.
    #   C — ICD-10-CM, FY < 2017: pre-ICD-10 era; tag and skip.
    #   D — ICD-9-CM: GEMs forward -> ICD-10 -> modal at max(fy, 2019)
    #       -> cost at max(fy, 2008). confidence=approximate.
    requested_inpatient = False
    inpatient = _InpatientCols()
    if dx_principal:
        requested_inpatient = True

        # ----- Branch A: ICD-10-CM, FY >= 2019 (native) -------------------
        if dx_system in ("", "ICD-10-CM") and fy >= _INPATIENT_MIN_FY:
            # Try the grouper first if PCS is present (surgical-DRG accuracy).
            # Falls back to modal lookup otherwise, or if grouper fails /
            # returns "000" (ungroupable sentinel).
            pcs_codes = _split_codes(row.get("pcs"))
            used_grouper = False
            if pcs_codes:
                grouper_drg = _invoke_grouper(row, fy)
                if grouper_drg is not None:
                    try:
                        inpatient = backend.lookup_drg_cost(grouper_drg, fy)
                        used_grouper = inpatient.resolved
                    except Exception as e:
                        notes.append(f"grouper_drg_lookup_error:{type(e).__name__}")
            if not inpatient.resolved:
                try:
                    inpatient = backend.lookup_inpatient(dx_principal, fy)
                except Exception as e:
                    notes.append(f"inpatient_lookup_error:{type(e).__name__}")
            if not inpatient.resolved:
                notes.append("dx_not_in_modal_index")
            else:
                out["inpatient_confidence"] = "high"
                if used_grouper:
                    notes.append(f"drg_via_grouper:{inpatient.drg}")

        # ----- Branch B: ICD-10-CM, 2017 <= FY < 2019 (back-translate) ----
        elif dx_system in ("", "ICD-10-CM") and 2017 <= fy < _INPATIENT_MIN_FY:
            # Widen modal proxy lookup to lex-min over [_INPATIENT_MIN_FY,
            # _MODAL_MAX_FY] (Session 4 FLAG 3 fix). Closes the gap for
            # trauma S/T-prefix codes with empty FY2019 modal but populated
            # FY2020+. Cost lookup stays at the encounter's native FY.
            hit_fy: Optional[int] = None
            try:
                drg, hit_fy = _lex_min_modal_drg(
                    backend, dx_principal, _INPATIENT_MIN_FY, _MODAL_MAX_FY
                )
            except Exception as e:
                drg = None
                notes.append(f"back_translation_error:{type(e).__name__}")
            if drg is None:
                notes.append("icd10_back_translation_no_modal")
            else:
                notes.append(f"back_translation_modal_fy={hit_fy}")
                try:
                    inpatient = backend.lookup_drg_cost(drg, fy)
                except Exception as e:
                    notes.append(f"back_translation_cost_error:{type(e).__name__}")
                if inpatient.resolved:
                    inpatient.allowed_source = "gems_bridged_back_translated"
                    out["inpatient_confidence"] = "approximate"
                    notes.append(
                        f"gems_bridged_back_translated:icd10={dx_principal},"
                        f"drg={drg},fy={fy}"
                    )
                else:
                    notes.append(
                        f"icd10_back_translation_no_cost:drg={drg},fy={fy}"
                    )

        # ----- Branch C: ICD-10-CM, FY < 2017 (pre-ICD-10 era) ------------
        elif dx_system in ("", "ICD-10-CM") and fy < 2017:
            notes.append(f"fy{fy}_pre_icd10_era")

        # ----- Branch D: ICD-9-CM (GEMs forward bridge) -------------------
        elif dx_system == "ICD-9-CM":
            try:
                targets = backend.lookup_gems_targets(dx_principal)
            except NotImplementedError:
                # Bolt backend stub; Batch 4 wires this up. Keep
                # ICD-9 rows as no_match for now and tag explicitly.
                targets = []
                notes.append("gems_lookup_not_implemented")
            except Exception as e:
                targets = []
                notes.append(f"gems_lookup_error:{type(e).__name__}")
            if not targets:
                notes.append("gems_no_target")
            else:
                overrides = _load_gems_overrides()
                best = _pick_best_gems_target(targets, dx_principal, overrides)
                if best is None:
                    notes.append("gems_no_map")
                else:
                    icd10 = best["target_code"]
                    tier = best["tier"]
                    truncated_n = best.get("truncated_n")
                    # Decision #2: combination-truncation provenance tag
                    if truncated_n and truncated_n > 1:
                        notes.append(
                            f"gems_combination_truncated:src={dx_principal},"
                            f"n_codes={truncated_n}"
                        )
                    notes.append(f"gems_resolved:tier_{tier}")
                    modal_fy = max(fy, _INPATIENT_MIN_FY)
                    try:
                        drg = backend.lookup_modal_drg(icd10, modal_fy)
                    except NotImplementedError:
                        drg = None
                        notes.append("modal_lookup_not_implemented")
                    except Exception as e:
                        drg = None
                        notes.append(f"modal_lookup_error:{type(e).__name__}")
                    if drg is None:
                        notes.append(f"gems_bridged_no_modal:icd10={icd10}")
                        # Decision #7 heuristic: any GEMs-bridged ICD-10 that
                        # misses the modal index is treated as a post-2018
                        # ICD-10 (the modal index covers FY2019+ for codes
                        # that existed at any covered FY). Always emit
                        # alongside gems_bridged_no_modal.
                        notes.append("icd10_post_gems_era")
                    else:
                        cost_fy = max(fy, 2008)
                        try:
                            inpatient = backend.lookup_drg_cost(drg, cost_fy)
                        except Exception as e:
                            notes.append(
                                f"gems_bridged_cost_error:{type(e).__name__}"
                            )
                        if inpatient.resolved:
                            inpatient.allowed_source = "gems_bridged_modal"
                            out["inpatient_confidence"] = "approximate"
                            notes.append(
                                f"gems_bridged:icd9={dx_principal},"
                                f"icd10={icd10},drg={drg}"
                            )
                        else:
                            notes.append(
                                f"gems_bridged_no_cost:drg={drg},fy={cost_fy}"
                            )

        else:
            notes.append(f"unknown_dx_system:{dx_system}")

    if inpatient.resolved:
        out["inpatient_drg"] = inpatient.drg or ""
        out["inpatient_drg_title"] = inpatient.drg_title or ""
        out["inpatient_allowed_mean"] = inpatient.allowed_mean
        out["inpatient_allowed_p10"] = inpatient.allowed_p10
        out["inpatient_allowed_p90"] = inpatient.allowed_p90
        out["inpatient_allowed_n"] = (
            inpatient.allowed_n if inpatient.allowed_n is not None else math.nan
        )
        out["inpatient_allowed_source"] = inpatient.allowed_source or ""
        out["inpatient_charge_mean"] = inpatient.charge_mean
        out["inpatient_ccr_cost"] = inpatient.ccr_cost

    # ---- CPT routing ------------------------------------------------------
    cpt_list = _split_codes(cpts_raw)
    requested_cpts = bool(cpt_list)
    cpts_resolved_any = False
    if cpt_list:
        cpt_cols = _resolve_cpt_settings(backend, cpt_list, fy)
        out["pfs_facility_allowed_total"] = cpt_cols.pfs_facility_total
        out["pfs_nonfacility_allowed_total"] = cpt_cols.pfs_nonfacility_total
        out["outpatient_apc_allowed_total"] = cpt_cols.opps_total
        out["asc_allowed_total"] = cpt_cols.asc_total
        out["n_cpts_matched"] = cpt_cols.n_matched
        out["n_cpts_missing"] = cpt_cols.n_missing
        cpts_resolved_any = cpt_cols.n_matched > 0
        if cpt_cols.n_missing > 0:
            notes.append(f"{cpt_cols.n_missing}_cpts_missing")

    # ---- Status logic -----------------------------------------------------
    inpatient_resolved = inpatient.resolved
    nothing_requested = not (requested_inpatient or requested_cpts)
    everything_resolved = (
        (not requested_inpatient or inpatient_resolved)
        and (not requested_cpts or (cpts_resolved_any and out["n_cpts_missing"] == 0))
    )
    something_resolved = inpatient_resolved or cpts_resolved_any

    if nothing_requested:
        out["annotation_status"] = "no_match"
        notes.append("empty_row_no_codes")
    elif everything_resolved:
        out["annotation_status"] = "ok"
    elif something_resolved:
        out["annotation_status"] = "partial"
    else:
        out["annotation_status"] = "no_match"
        if dx_principal and not inpatient_resolved and dx_system not in ("ICD-9-CM",):
            if fy >= _INPATIENT_MIN_FY and dx_system in ("", "ICD-10-CM"):
                # icd_not_in_graph variant — bogus or out-of-range code
                if "dx_not_in_modal_index" in notes:
                    notes = [n for n in notes if n != "dx_not_in_modal_index"]
                    notes.append("dx_not_in_graph")

    out["annotation_notes"] = ";".join(notes)
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@contextmanager
def _backend_context(source: str) -> Iterable[_LookupBackend]:
    if source == "parquet":
        backend: _LookupBackend = _ParquetBackend()
    elif source == "bolt":
        backend = _BoltBackend()
    else:
        raise ValueError(f"source must be 'bolt' or 'parquet', got {source!r}")
    try:
        yield backend
    finally:
        backend.close()


def _row_below_floor(row_confidence: str, floor: str) -> bool:
    """True iff ``row_confidence`` rank is strictly below ``floor`` rank.

    Both values must be in ``_CONFIDENCE_RANK``. Used by
    ``_apply_confidence_floor`` to gate per-row rejection. When floor
    is ``"none"`` (default), no row qualifies (rank 0 is never < 0).
    """
    return _CONFIDENCE_RANK[row_confidence] < _CONFIDENCE_RANK[floor]


def _row_attempted_inpatient(out_df: pd.DataFrame, idx: Any) -> bool:
    """True iff inpatient routing was attempted for this row.

    Detected via the original cohort's ``dx_principal`` column when present
    (the input column is preserved at the front of out_df). When the
    cohort has no ``dx_principal`` column at all, treat every row as a
    non-inpatient-attempt (no inpatient routing means the floor is a
    no-op for the row).
    """
    if "dx_principal" not in out_df.columns:
        return False
    v = out_df.at[idx, "dx_principal"]
    if v is None:
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    s = str(v).strip()
    return s != "" and s.lower() != "nan"


def _apply_confidence_floor(
    out_df: pd.DataFrame,
    confidence_floor: str,
    scope: str,
) -> tuple[int, int, int]:
    """Apply Phase 13 sensitivity-floor rejection in-place on ``out_df``.

    Plan Decision #3 (line 22 of plan): when a row's inpatient confidence
    is below ``confidence_floor``, null the 9 cost-bearing
    ``inpatient_*`` columns, reset ``inpatient_confidence`` to ``"none"``
    (preserves the always-one-of-{high,approximate,none} schema
    invariant), flip ``annotation_status`` to ``no_match``, and append
    ``confidence_floor_exceeded`` to ``annotation_notes``.

    The floor only applies to rows where INPATIENT ROUTING WAS ATTEMPTED
    (i.e., the input cohort row carried a non-empty ``dx_principal``).
    Pure CPT-only / outpatient rows skip the floor entirely — their
    ``inpatient_confidence`` default of ``"none"`` is a schema-default,
    not a "DRG lookup failed" signal, and rejecting them would zero out
    successful CPT annotations.

    With ``scope='all'``, the 4 outpatient/CPT cost totals are also
    zeroed (set to 0.0, matching the per-row dataclass default — NOT
    NaN). With ``scope='ip'`` (default) outpatient totals are left
    untouched. ``scope='op'`` is reserved for Phase 8+ outpatient
    confidence scoring and is currently a no-op (outpatient_confidence
    is always None this phase).

    Returns the post-floor (n_ok, n_partial, n_no_match) triple.
    """
    if confidence_floor == "none":
        # Fast path: floor is the bottom of the rank; no row is below.
        n_ok = int((out_df["annotation_status"] == "ok").sum())
        n_partial = int((out_df["annotation_status"] == "partial").sum())
        n_no_match = int((out_df["annotation_status"] == "no_match").sum())
        return n_ok, n_partial, n_no_match

    for idx in out_df.index:
        # Skip rows where inpatient routing wasn't attempted (CPT-only
        # outpatient rows). Their schema-default confidence='none' is not
        # a routing failure.
        if not _row_attempted_inpatient(out_df, idx):
            continue
        confidence = str(out_df.at[idx, "inpatient_confidence"] or "none")
        if not _row_below_floor(confidence, confidence_floor):
            continue
        # Null the 9 inpatient cost-bearing cols. Use NaN for numerics,
        # empty-string for the str-typed drg/drg_title/source cols (matches
        # the _annotate_row default initialization).
        for col in _INPATIENT_COST_COLS_TO_NULL:
            if col in ("inpatient_drg", "inpatient_drg_title", "inpatient_allowed_source"):
                out_df.at[idx, col] = ""
            else:
                out_df.at[idx, col] = math.nan
        # Reset confidence to "none" — keeps schema invariant intact.
        out_df.at[idx, "inpatient_confidence"] = "none"
        # Flip status. Tag the rejection in notes (append, don't replace).
        out_df.at[idx, "annotation_status"] = "no_match"
        existing_notes = str(out_df.at[idx, "annotation_notes"] or "")
        new_note = "confidence_floor_exceeded"
        if existing_notes:
            out_df.at[idx, "annotation_notes"] = f"{existing_notes};{new_note}"
        else:
            out_df.at[idx, "annotation_notes"] = new_note
        # --scope=all also zeros the 4 outpatient/CPT totals.
        if scope == "all":
            for col in _OUTPATIENT_COST_COLS_TO_ZERO:
                out_df.at[idx, col] = 0.0

    n_ok = int((out_df["annotation_status"] == "ok").sum())
    n_partial = int((out_df["annotation_status"] == "partial").sum())
    n_no_match = int((out_df["annotation_status"] == "no_match").sum())
    return n_ok, n_partial, n_no_match


def annotate_cohort(
    cohort_df: pd.DataFrame,
    source: str = "bolt",
    default_year: int = 2024,
    fy_column: str = "fy",
    partial_output: Optional[Path] = None,
    confidence_floor: str = "none",
    scope: str = "ip",
) -> AnnotationResult:
    """Annotate a cohort DataFrame with cost columns. See module docstring.

    ``confidence_floor`` (Phase 13 sensitivity gate) — one of
    ``{"high", "approximate", "none"}``. Default ``"none"`` is a no-op.
    Higher values reject inpatient rows whose resolved confidence is
    below the floor (e.g. ``"high"`` rejects all GEMs-bridged + back-
    translated rows). Rejected rows have inpatient cost cols nulled,
    ``annotation_status`` flipped to ``no_match``, and a
    ``confidence_floor_exceeded`` note appended.

    ``scope`` — one of ``{"ip", "op", "all"}``. Controls which cost
    columns the rejection filter zeros. ``"ip"`` (default) only the
    inpatient cols. ``"all"`` ALSO zeros the 4 outpatient/CPT totals.
    ``"op"`` is reserved for Phase 8+ outpatient confidence scoring
    (currently a no-op).
    """
    if confidence_floor not in _CONFIDENCE_FLOOR_CHOICES:
        raise ValueError(
            f"confidence_floor must be one of {_CONFIDENCE_FLOOR_CHOICES}, "
            f"got {confidence_floor!r}"
        )
    if scope not in _SCOPE_CHOICES:
        raise ValueError(
            f"scope must be one of {_SCOPE_CHOICES}, got {scope!r}"
        )

    rows: list[dict[str, Any]] = []
    unmatched: list[tuple[int, str]] = []

    with _backend_context(source) as backend:
        for i, (idx, row) in enumerate(cohort_df.iterrows()):
            cols = _annotate_row(row, backend, default_year, fy_column)
            rows.append(cols)

            # Periodic checkpoint for long bolt runs
            if (
                partial_output is not None
                and (i + 1) % _PARTIAL_CHECKPOINT_EVERY == 0
            ):
                _write_partial(cohort_df.iloc[: i + 1], rows, partial_output)

        out_df = cohort_df.reset_index(drop=True).copy()
        appended = pd.DataFrame(rows, index=out_df.index)
        for col in _OUTPUT_COLUMNS:
            out_df[col] = appended[col]

        # Phase 13 post-materialization filter — runs AFTER the per-row
        # annotation loop, BEFORE final counters are tallied. Coverage and
        # status counts in AnnotationResult reflect the POST-floor state.
        n_ok, n_partial, n_no_match = _apply_confidence_floor(
            out_df, confidence_floor, scope
        )

        # Rebuild unmatched_rows from the post-floor state so newly-rejected
        # rows show up there too (their notes now carry confidence_floor_exceeded).
        for idx in out_df.index:
            if out_df.at[idx, "annotation_status"] == "no_match":
                notes = str(out_df.at[idx, "annotation_notes"] or "")
                unmatched.append((int(idx), notes))

        n_rows = len(out_df)
        coverage = (n_ok + n_partial) / n_rows if n_rows else 0.0
        return AnnotationResult(
            df=out_df,
            coverage=coverage,
            n_rows=n_rows,
            n_ok=n_ok,
            n_partial=n_partial,
            n_no_match=n_no_match,
            unmatched_rows=unmatched,
            mode=backend.mode,
        )


def _write_partial(
    cohort_so_far: pd.DataFrame, rows: list[dict[str, Any]], path: Path
) -> None:
    df = cohort_so_far.reset_index(drop=True).copy()
    appended = pd.DataFrame(rows, index=df.index)
    for col in _OUTPUT_COLUMNS:
        df[col] = appended[col]
    df.to_csv(path, index=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cohort_annotator",
        description="Annotate a cohort CSV with KG-COST cost columns.",
    )
    p.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Input path. CSV by default; pass --input-format ntdb for NTDB annual_cohort.parquet.",
    )
    p.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output path. .csv (default) or .parquet/.pq (auto-detected by suffix).",
    )
    p.add_argument(
        "--source",
        choices=("bolt", "parquet"),
        default="bolt",
        help="Lookup backend (default: bolt).",
    )
    p.add_argument(
        "--year", type=int, default=2024, help="Default FY when missing (default: 2024)."
    )
    p.add_argument(
        "--fy-column",
        default="fy",
        help="Name of FY column in the input CSV (default: fy).",
    )
    p.add_argument(
        "--confidence-floor",
        choices=_CONFIDENCE_FLOOR_CHOICES,
        default="none",
        help=(
            "Reject inpatient rows below this confidence tier. 'none' (default) "
            "passes everything through. 'approximate' rejects rows where confidence "
            "is 'none' (no DRG resolved). 'high' rejects everything except native "
            "FY>=2019 ICD-10 routing - strictest sensitivity setting."
        ),
    )
    p.add_argument(
        "--scope",
        choices=_SCOPE_CHOICES,
        default="ip",
        help=(
            "Which cost columns the rejection filter zeroes when --confidence-floor "
            "fires. 'ip' (default) only zeros the inpatient cost cols. 'all' also "
            "zeros the 4 outpatient/CPT cost columns. 'op' is reserved for Phase 8+ "
            "outpatient confidence scoring (currently a no-op since "
            "outpatient_confidence is always None)."
        ),
    )
    p.add_argument(
        "--input-format",
        choices=["csv", "ntdb"],
        default="csv",
        help=(
            "csv: read --input as CSV (default). ntdb: read --input as NTDB "
            "annual_cohort.parquet via _ntdb_adapter.load_ntdb_cohort."
        ),
    )
    p.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="If set, process only N rows of input. Useful for smoke tests.",
    )
    p.add_argument(
        "--sample-strategy",
        choices=["head", "random", "stratified"],
        default="head",
        help=(
            "How --sample-rows picks rows (NTDB-only). 'head' (default) takes "
            "the first N rows (back-compat; collapses to a single FY on "
            "FY-sorted annual_cohort.parquet). 'random' uniform-samples N rows "
            "with --sample-seed for reproducibility. 'stratified' balances "
            "across YEAR groups (~N/n_fys per FY, then reshuffled and "
            "right-sized to N)."
        ),
    )
    p.add_argument(
        "--sample-seed",
        type=int,
        default=0,
        help="Random seed for --sample-strategy random/stratified (default: 0).",
    )
    p.add_argument(
        "--ntdb-duckdb",
        type=Path,
        default=None,
        help=(
            "Override path to NTDB DuckDB file for procedure attachment. "
            "Defaults to the KG_COST_NTDB_DUCKDB env var; there is no built-in "
            "default (NTDB is restricted data, not included in this repo)."
        ),
    )
    return p


def _install_sigint_handler() -> None:
    """Make Ctrl+C raise KeyboardInterrupt cleanly (default on POSIX, but be explicit)."""
    signal.signal(signal.SIGINT, signal.default_int_handler)


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    _install_sigint_handler()

    if not args.input.exists():
        print(f"error: input file not found: {args.input}", file=sys.stderr)
        return 2

    if args.input_format == "csv" and args.sample_strategy != "head":
        print(
            "error: --sample-strategy is supported only with --input-format ntdb; "
            f"got --input-format=csv with --sample-strategy={args.sample_strategy}",
            file=sys.stderr,
        )
        raise SystemExit(2)

    if args.input_format == "ntdb":
        from src.query._ntdb_adapter import load_ntdb_cohort

        cohort_df = load_ntdb_cohort(
            args.input,
            sample_rows=args.sample_rows,
            duckdb_path=args.ntdb_duckdb,
            sample_strategy=args.sample_strategy,
            sample_seed=args.sample_seed,
        )
    else:
        cohort_df = pd.read_csv(
            args.input, dtype=str, keep_default_na=False, na_values=[""]
        )
        # Cast known numeric cols if present
        for col in (args.fy_column, "age", "sex"):
            if col in cohort_df.columns:
                cohort_df[col] = pd.to_numeric(cohort_df[col], errors="coerce")
        if args.sample_rows is not None:
            cohort_df = cohort_df.head(args.sample_rows)

    partial_path = args.output.with_suffix(args.output.suffix + ".partial")
    try:
        result = annotate_cohort(
            cohort_df,
            source=args.source,
            default_year=args.year,
            fy_column=args.fy_column,
            partial_output=partial_path,
            confidence_floor=args.confidence_floor,
            scope=args.scope,
        )
    except KeyboardInterrupt:
        print(
            f"interrupted; partial output (if any) written to {partial_path}",
            file=sys.stderr,
        )
        return 130

    args.output.parent.mkdir(parents=True, exist_ok=True)
    out_suffix = args.output.suffix.lower()
    if out_suffix in (".parquet", ".pq"):
        result.df.to_parquet(args.output, index=False)
    else:
        result.df.to_csv(args.output, index=False)
    if partial_path.exists():
        try:
            partial_path.unlink()
        except OSError:
            pass

    print(
        f"[cohort_annotator] mode={result.mode} rows={result.n_rows} "
        f"ok={result.n_ok} partial={result.n_partial} no_match={result.n_no_match} "
        f"coverage={result.coverage:.3f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
