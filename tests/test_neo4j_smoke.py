"""End-to-end Neo4j smoke test (opt-in).

Runs ``neo4j-admin database import full`` (via either docker-compose or
local-admin mode, dictated by ``KG_COST_IMPORT_MODE`` in .env), applies
post-load schema, runs the 4 sample queries from ``post_load.cypher``, and
verifies counts. Wall time target: 5 min. Re-uses the existing
``data/neo4j_import/*.csv`` (Phase 8/9 artifacts).

Gated by env var ``KG_COST_RUN_NEO4J=1`` AND a reachable Neo4j target. The
target is reachable if EITHER the Docker daemon is up (docker mode) OR
``KG_COST_NEO4J_HOME`` is set to a valid Neo4j 5.x install (local-admin
mode). Skips silently otherwise.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Callable

import pytest

from src.config import NEO4J_IMPORT, ROOT

_THREE_DIGIT = re.compile(r"^\d{3}$")


def _docker_reachable() -> bool:
    """Return True iff ``docker info`` responds within 10 s."""
    try:
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _neo4j_target_reachable() -> bool:
    """True if either Docker is reachable (docker mode) or KG_COST_NEO4J_HOME
    is set to a valid Neo4j install (local-admin mode). Mode is read from
    .env via get_neo4j_config()."""
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


pytestmark = [
    pytest.mark.skipif(
        os.getenv("KG_COST_RUN_NEO4J") != "1",
        reason="set KG_COST_RUN_NEO4J=1 to run the live-Neo4j smoke (5 min)",
    ),
    pytest.mark.skipif(
        not _neo4j_target_reachable(),
        reason="Neo4j target not reachable (need Docker daemon up OR KG_COST_NEO4J_HOME set)",
    ),
]


@pytest.fixture(scope="module")
def live_graph_results() -> tuple[dict, dict]:
    """Run the orchestrator end-to-end; return (load_summary, smoke).

    ``run_import.py`` writes both ``_load_summary.json`` (per-label and
    per-rel-type counts) and ``_smoke_results.json`` (4 sample queries
    from ``post_load.cypher``) into ``data/neo4j_import/``. The fixture
    is module-scoped so the 5-minute import runs once even when many
    parametrized tests share it.
    """
    r = subprocess.run(
        ["python3", "-m", "src.load.run_import"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert r.returncode == 0, (
        f"run_import.py failed (rc={r.returncode}):\n"
        f"STDOUT: {r.stdout}\nSTDERR: {r.stderr}"
    )
    load_summary_path = NEO4J_IMPORT / "_load_summary.json"
    smoke_path = NEO4J_IMPORT / "_smoke_results.json"
    assert load_summary_path.exists(), f"missing {load_summary_path}"
    assert smoke_path.exists(), f"missing {smoke_path}"
    load_summary = json.loads(load_summary_path.read_text())
    smoke = json.loads(smoke_path.read_text())
    return load_summary, smoke
    # NOTE: per plan-v13 risk register, we deliberately do NOT delete the
    # local /data volume during teardown — the data volume is not committed
    # and rebuilding is expensive.


def test_all_labels_populated(
    live_graph_results: tuple[dict, dict],
) -> None:
    load, _ = live_graph_results
    nodes = load["nodes"]
    n_pop = sum(1 for v in nodes.values() if v > 0)
    assert n_pop >= 12, (
        f"expected >= 12 populated labels, got {n_pop}: {nodes}"
    )


def test_all_rel_types_populated(
    live_graph_results: tuple[dict, dict],
) -> None:
    load, _ = live_graph_results
    rels = load["rels"]
    n_pop = sum(1 for v in rels.values() if v > 0)
    assert n_pop >= 10, (
        f"expected >= 10 populated rel types, got {n_pop}: {rels}"
    )


def _validate_q1_i214_modal(rows: list[dict]) -> None:
    """Q1: I21.4 modal DRG → exactly the AMI cluster (a 3-digit MS-DRG)."""
    assert len(rows) >= 1, "q1_i214_modal returned 0 rows"
    for row in rows:
        msdrg = row.get("msdrg")
        assert isinstance(msdrg, str) and _THREE_DIGIT.match(msdrg), (
            f"q1_i214_modal: msdrg must be a 3-digit string, got {msdrg!r}"
        )
        title = row.get("drg.title")
        assert isinstance(title, str) and len(title) > 0, (
            f"q1_i214_modal: drg.title must be a non-empty string, got {title!r}"
        )


def _validate_q2_27447_pfs(rows: list[dict]) -> None:
    """Q2: CPT 27447 (TKA) PFS facility allowed ∈ [800, 2000]."""
    assert len(rows) >= 1, "q2_27447_pfs returned 0 rows"
    for row in rows:
        code = row.get("h.code")
        assert code == "27447", (
            f"q2_27447_pfs: h.code must be '27447', got {code!r}"
        )
        value = row.get("obs.value")
        assert isinstance(value, (int, float)), (
            f"q2_27447_pfs: obs.value must be numeric, got {value!r}"
        )
        assert 800 <= value <= 2000, (
            f"q2_27447_pfs: obs.value (PFS facility allowed for TKA) "
            f"must be in [800, 2000], got {value}"
        )


def _validate_q3_i21_anchors(rows: list[dict]) -> None:
    """Q3: I21* CAN_ANCHOR → ≥3 distinct DRGs reachable, each 3-digit."""
    assert len(rows) >= 3, (
        f"q3_i21_anchors: expected ≥3 distinct DRGs reachable from I21*, "
        f"got {len(rows)}"
    )
    for row in rows:
        code = row.get("drg.code")
        assert isinstance(code, str) and _THREE_DIGIT.match(code), (
            f"q3_i21_anchors: drg.code must be a 3-digit string, got {code!r}"
        )


def _validate_q4_41041_gems(rows: list[dict]) -> None:
    """Q4: ICD-9 410.41 GEMs → ICD-10-CM I-chapter (cardiovascular)."""
    assert len(rows) >= 1, "q4_41041_gems returned 0 rows"
    for row in rows:
        tgt = row.get("tgt.code")
        assert isinstance(tgt, str) and tgt.startswith("I"), (
            f"q4_41041_gems: tgt.code must start with 'I' "
            f"(ICD-10-CM cardiovascular chapter), got {tgt!r}"
        )


_QUERY_VALIDATORS: list[tuple[str, Callable[[list[dict]], None]]] = [
    ("q1_i214_modal", _validate_q1_i214_modal),
    ("q2_27447_pfs", _validate_q2_27447_pfs),
    ("q3_i21_anchors", _validate_q3_i21_anchors),
    ("q4_41041_gems", _validate_q4_41041_gems),
]


@pytest.mark.parametrize(
    ("query_key", "validator"),
    _QUERY_VALIDATORS,
    ids=[k for k, _ in _QUERY_VALIDATORS],
)
def test_sample_query_nonempty(
    live_graph_results: tuple[dict, dict],
    query_key: str,
    validator: Callable[[list[dict]], None],
) -> None:
    """Each sample query returns rows AND those rows satisfy per-query
    range-credibility checks (plan-v15 Phase 10 Step 6c)."""
    _, smoke = live_graph_results
    rows = smoke.get(query_key, [])
    validator(rows)
