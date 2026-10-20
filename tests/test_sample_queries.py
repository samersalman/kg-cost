"""Tests for src/query/sample_queries.cypher (8 named queries).

Always-on (no Neo4j required):
  - Parse the file via _bolt.read_query() / list_queries()
  - Assert all 8 expected query names exist
  - Assert each query body ends with `;`
  - Assert default-params dict is non-empty for every query
  - Light-touch syntactic guard: open/close paren counts must match

Live-graph (gated by KG_COST_RUN_NEO4J=1 + reachable target):
  - Parametrized: for each of the 8 queries, run with default params,
    assert row count >= 1 and apply per-query credibility validators
    (regex on DRG codes, value ranges, GEMs target sys, etc.)

Column aliases verified against src/query/sample_queries.cypher (Step 2):
  Q1: msdrg, title, allowed, p10, p90, n
  Q2: cpt, descriptor, allowed, source
  Q3: hcpcs, apc, apc_title, payment_rate
  Q4: msdrg, title
  Q5: target, description, approximate_flag, no_map_flag, combination_flag, scenario, choice_list
  Q6: icd_code, code_system, description
  Q7: msdrg, title, ccr_cost, ccr_value, cost_center
  Q8: year, allowed, p10, p90, n
"""

from __future__ import annotations

import os
import re
import subprocess

import pytest

from src.query._bolt import (
    get_driver,
    list_queries,
    read_query,
    run_query,
)

EXPECTED_NAMES = {
    "Q1_icd_modal_cost",
    "Q2_cpt_pfs_facility",
    "Q3_hcpcs_apc_drilldown",
    "Q4_icd_can_anchor_all",
    "Q5_gems_forward",
    "Q6_drg_reverse_icds",
    "Q7_ccr_adjusted_cost",
    "Q8_cost_distribution_by_year",
}


# ============================================================
# Always-on parser tests (no Neo4j required)
# ============================================================

def test_all_8_queries_present() -> None:
    names = set(list_queries())
    assert names == EXPECTED_NAMES, f"missing/extra queries: {names ^ EXPECTED_NAMES}"


def test_each_query_has_default_params_and_ends_with_semicolon() -> None:
    for name in list_queries():
        cypher, defaults = read_query(name)
        assert defaults, f"{name} has no default params"
        assert cypher.rstrip().endswith(";"), f"{name} body does not end with ';'"


def test_query_body_no_unbalanced_parens() -> None:
    """Light-touch syntactic guard - open/close paren counts must match."""
    for name in list_queries():
        cypher, _ = read_query(name)
        n_open = cypher.count("(")
        n_close = cypher.count(")")
        assert n_open == n_close, (
            f"{name}: unbalanced parens ({n_open} open, {n_close} close)"
        )


# ============================================================
# Live-graph tests (gated by KG_COST_RUN_NEO4J=1 + reachable target)
# ============================================================

def _docker_reachable() -> bool:
    """Return True iff `docker info` responds within 10 s."""
    try:
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _live_target_reachable() -> bool:
    """Mirror tests/test_neo4j_smoke.py: reachable iff Docker up OR
    KG_COST_NEO4J_HOME set + valid neo4j-admin binary."""
    try:
        from src.load._config import get_neo4j_config
        cfg = get_neo4j_config()
    except (ValueError, ImportError):
        return False
    if cfg.import_mode == "local-admin":
        return cfg.neo4j_home is not None and (cfg.neo4j_home / "bin/neo4j-admin").exists()
    if cfg.import_mode == "docker":
        return _docker_reachable()
    return False


_LIVE_SKIP_MARKS = [
    pytest.mark.skipif(
        os.getenv("KG_COST_RUN_NEO4J") != "1",
        reason="set KG_COST_RUN_NEO4J=1 to run live-bolt tests",
    ),
    pytest.mark.skipif(
        not _live_target_reachable(),
        reason="Neo4j target not reachable",
    ),
]


@pytest.fixture(scope="module")
def driver():
    drv = get_driver()
    drv.verify_connectivity()
    yield drv
    drv.close()


# ---- Per-query validators (column names match Step 2 cypher aliases) ----

_DRG_RE = re.compile(r"^\d{3,4}$")
_ICD10_RE = re.compile(r"^[A-TV-Z]\d[A-Z0-9]{0,5}$")  # rough ICD-10-CM shape


def _validate_q1_icd_modal_cost(rows: list[dict]) -> None:
    """Q1: I21.4 modal DRG (AMI cluster) with allowed_payment cost."""
    assert len(rows) >= 1, "Q1 returned 0 rows"
    r = rows[0]
    assert _DRG_RE.match(str(r["msdrg"])), f"msdrg shape: {r['msdrg']!r}"
    assert isinstance(r["title"], str) and r["title"], f"title: {r['title']!r}"
    if r.get("allowed") is not None:
        assert 5_000 <= r["allowed"] <= 30_000, (
            f"AMI inpatient allowed out of range: {r['allowed']}"
        )


def _validate_q2_cpt_pfs_facility(rows: list[dict]) -> None:
    """Q2: CPT 27447 (TKA) PFS facility allowed in [800, 2000]."""
    assert len(rows) >= 1, "Q2 returned 0 rows"
    r = rows[0]
    assert r["cpt"] == "27447", f"cpt: {r['cpt']!r}"
    val = r["allowed"]
    assert isinstance(val, (int, float)), f"allowed not numeric: {val!r}"
    assert 800 <= val <= 2_000, f"PFS allowed for TKA out of range: {val}"


def _validate_q3_hcpcs_apc_drilldown(rows: list[dict]) -> None:
    """Q3: HCPCS 27447 → APC. APC code shape: 4-5 digits."""
    assert len(rows) >= 1, "Q3 returned 0 rows"
    for r in rows:
        apc = str(r["apc"])
        assert apc.isdigit() and 4 <= len(apc) <= 5, f"apc shape: {apc!r}"


def _validate_q4_icd_can_anchor_all(rows: list[dict]) -> None:
    """Q4: I21.4 CAN_ANCHOR → at least 3 distinct DRGs reachable."""
    assert len(rows) >= 3, f"expected >=3 reachable DRGs from I214, got {len(rows)}"
    for r in rows:
        assert _DRG_RE.match(str(r["msdrg"])), f"msdrg shape: {r['msdrg']!r}"
        assert isinstance(r["title"], str) and r["title"], f"title: {r['title']!r}"


def _validate_q5_gems_forward(rows: list[dict]) -> None:
    """Q5: ICD-9 410.41 GEMs forward → ICD-10-CM cardiovascular (I-chapter).

    Post-Batch-4 widening: every row carries 7 keys
    ``(target, description, approximate_flag, no_map_flag,
    combination_flag, scenario, choice_list)``. The 5 disambiguation
    columns are consumed by ``_pick_best_gems_target`` in the bolt path.
    """
    assert len(rows) >= 1, "Q5 returned 0 rows"
    expected_keys = {
        "target",
        "description",
        "approximate_flag",
        "no_map_flag",
        "combination_flag",
        "scenario",
        "choice_list",
    }
    for r in rows:
        missing = expected_keys - set(r.keys())
        assert not missing, f"Q5 row missing keys: {missing}"
        tgt = str(r["target"])
        assert _ICD10_RE.match(tgt), f"GEMs target shape: {tgt!r}"
        # 410.41 is acute MI → cardiovascular (I-chapter) expected
        assert tgt.startswith("I"), f"expected I-chapter target for 410.41, got {tgt!r}"


def _validate_q6_drg_reverse_icds(rows: list[dict]) -> None:
    """Q6: DRG 872 (Septicemia w/o MV) reverse → ICD-10-CM codes."""
    assert len(rows) >= 1, "Q6 returned 0 rows"
    for r in rows[:5]:
        icd = str(r["icd_code"])
        assert _ICD10_RE.match(icd), f"icd shape: {icd!r}"
        assert r["code_system"] == "ICD-10-CM", (
            f"expected ICD-10-CM, got {r['code_system']!r}"
        )


def _validate_q7_ccr_adjusted_cost(rows: list[dict]) -> None:
    """Q7: I21.4 ccr_cost = charge × CCR. CCR ratio in [0.05, 0.6]."""
    assert len(rows) >= 1, "Q7 returned 0 rows"
    r = rows[0]
    assert _DRG_RE.match(str(r["msdrg"])), f"msdrg shape: {r['msdrg']!r}"
    if r.get("ccr_cost") is not None:
        assert r["ccr_cost"] > 0, f"ccr_cost not positive: {r['ccr_cost']}"
        ccr = r.get("ccr_value")
        if ccr is not None:
            assert 0.05 <= ccr <= 0.6, f"ccr_value out of range: {ccr}"


def _validate_q8_cost_distribution_by_year(rows: list[dict]) -> None:
    """Q8: CPT 27447 PFS facility allowed across FY2019-FY2026.
    At least 2 distinct years should be present (multi-year sweep)."""
    assert len(rows) >= 1, "Q8 returned 0 rows"
    years = [r["year"] for r in rows]
    assert len(set(years)) >= 2, (
        f"only {len(set(years))} distinct year(s) in result: {sorted(set(years))}"
    )
    for r in rows:
        assert 2019 <= r["year"] <= 2026, f"year out of range: {r['year']}"


VALIDATORS = {
    "Q1_icd_modal_cost": _validate_q1_icd_modal_cost,
    "Q2_cpt_pfs_facility": _validate_q2_cpt_pfs_facility,
    "Q3_hcpcs_apc_drilldown": _validate_q3_hcpcs_apc_drilldown,
    "Q4_icd_can_anchor_all": _validate_q4_icd_can_anchor_all,
    "Q5_gems_forward": _validate_q5_gems_forward,
    "Q6_drg_reverse_icds": _validate_q6_drg_reverse_icds,
    "Q7_ccr_adjusted_cost": _validate_q7_ccr_adjusted_cost,
    "Q8_cost_distribution_by_year": _validate_q8_cost_distribution_by_year,
}


@pytest.mark.parametrize("name", sorted(EXPECTED_NAMES))
@pytest.mark.skipif(
    os.getenv("KG_COST_RUN_NEO4J") != "1",
    reason="set KG_COST_RUN_NEO4J=1 to run live-bolt tests",
)
@pytest.mark.skipif(
    not _live_target_reachable(),
    reason="Neo4j target not reachable",
)
def test_query_runs_with_credible_values(driver, name: str) -> None:
    """Each named query returns >=1 row and satisfies per-query credibility checks."""
    rows = run_query(driver, name)
    assert rows, f"{name} returned 0 rows"
    VALIDATORS[name](rows)
