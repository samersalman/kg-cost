"""Phase 9 import orchestrator for KG-COST.

Probes Docker; if reachable, builds CSVs if stale, runs the bulk-import shell,
applies post-load schema, and verifies the live graph against expected
node/rel counts. If Docker is not reachable, logs a one-line resume command
and exits 0 (a non-blocking environmental skip per CLAUDE.md).

Outputs:
  - data/neo4j_import/_load_summary.json   {"nodes": {label: count}, "rels": {type: count}}
  - data/neo4j_import/_smoke_results.json  {"q1_i214_modal": [...], "q2_27447_pfs": [...], ...}

Usage:
    python -m src.load.run_import                    # full pipeline
    python -m src.load.run_import --skip-build       # reuse existing CSVs
    python -m src.load.run_import --skip-import      # only re-run sanity queries
    python -m src.load.run_import --check-only       # probe docker, exit 0/1

NOTE: Live-graph queries require the `neo4j` Python driver
(`pip install neo4j>=5`). Not currently in pyproject.toml [dev]; add when
Phase 9 ships. The `query_via_cypher_shell()` fallback is a stub that raises
with a clear install hint.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from src.config import GOLD, NEO4J_IMPORT, ROOT, SILVER
from src.load._config import Neo4jConfig, get_neo4j_config

LOG = logging.getLogger(__name__)
COMPOSE_FILE = ROOT / "neo4j" / "docker-compose.yml"
IMPORT_SCRIPT = ROOT / "src" / "load" / "neo4j_import.sh"
BUILD_SUMMARY = NEO4J_IMPORT / "_build_summary.json"
LOAD_SUMMARY = NEO4J_IMPORT / "_load_summary.json"
SMOKE_RESULTS = NEO4J_IMPORT / "_smoke_results.json"

EXPECTED_LABELS = [
    "ICDCMCode", "ICDPCSCode", "HCPCS", "MSDRG", "APC",
    "CostObservation", "IPPSStandardizedAmount",
    "OPPSConversionFactor", "PFSConversionFactor",
    "CCR", "CostCenter", "GEMMapping",
]
EXPECTED_REL_TYPES = [
    "CAN_ANCHOR", "IS_MODAL_FOR", "OBSERVED_AS", "MAPPED_TO_APC",
    "HAS_STD_AMOUNT", "USES_CCR", "IN_COST_CENTER",
    "MAPS_FROM", "MAPS_TO", "IS_REIMBURSEMENT_OF",
]

TOLERANCE_PCT = 1.0  # ±1% per plan-v14 (Session 10.5); bulk import does not lose rows


def docker_reachable() -> bool:
    """Return True iff `docker info` succeeds (daemon running and accessible)."""
    if shutil.which("docker") is None:
        return False
    try:
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def docker_compose_cmd() -> list[str]:
    """Return the docker compose CLI as a list. Modern (`docker compose`)."""
    return ["docker", "compose", "-f", str(COMPOSE_FILE)]


def csvs_stale(summary_path: Path) -> bool:
    """True if any silver/gold parquet is newer than the build summary,
    OR the summary doesn't exist. Conservative trigger for re-build."""
    if not summary_path.exists():
        return True
    summary_mtime = summary_path.stat().st_mtime
    sources: list[Path] = []
    if SILVER.exists():
        sources += list(SILVER.glob("*.parquet"))
    if GOLD.exists():
        sources += list(GOLD.glob("*.parquet"))
    return any(p.stat().st_mtime > summary_mtime for p in sources)


def run_build_csvs() -> None:
    LOG.info("Building CSVs via src.load.build_csvs ...")
    r = subprocess.run(
        [sys.executable, "-m", "src.load.build_csvs"],
        cwd=ROOT, capture_output=False,
    )
    if r.returncode != 0:
        LOG.error("build_csvs.py failed (exit %s)", r.returncode)
        sys.exit(r.returncode)


def run_neo4j_import_sh() -> None:
    LOG.info("Running neo4j_import.sh ...")
    r = subprocess.run(["bash", str(IMPORT_SCRIPT)], cwd=ROOT)
    if r.returncode != 0:
        LOG.error("neo4j_import.sh failed (exit %s)", r.returncode)
        sys.exit(r.returncode)


def _resolve_bundled_jdk_home(neo4j_home: Path) -> Path:
    """Locate the Zulu 21 JDK bundled by Neo4j Desktop's runtime cache.

    Searches ``<neo4j_home>/../../../Cache/runtime/zulu21*-jdk*/zulu-21.jdk/
    Contents/Home``. neo4j-admin 2026.04.0 requires Java 21+, but the
    project's system Java is 17 (used by the Phase 4 grouper), so we resolve
    a bundled JDK and pass it via JAVA_HOME to the local subprocess env only.

    Raises:
        RuntimeError: if no matching directory is found.
    """
    cache_root = neo4j_home.parents[2] / "Cache" / "runtime"
    candidates = sorted(cache_root.glob("zulu21*-jdk*"))
    for cand in reversed(candidates):  # latest lexicographically first
        jdk_home = cand / "zulu-21.jdk" / "Contents" / "Home"
        if (jdk_home / "bin" / "java").exists():
            return jdk_home
    raise RuntimeError(
        f"Could not locate bundled Zulu 21 JDK under {cache_root} "
        f"(searched zulu21*-jdk*/zulu-21.jdk/Contents/Home)"
    )


def _local_env(jdk_home: Path) -> dict[str, str]:
    """Subprocess env with JAVA_HOME and PATH overridden for neo4j-admin.

    Never mutates os.environ — returns a fresh dict for use as the ``env``
    kwarg of subprocess.run().
    """
    env = {**os.environ}
    env["JAVA_HOME"] = str(jdk_home)
    env["PATH"] = f"{jdk_home}/bin:" + os.environ.get("PATH", "")
    return env


def _wait_for_dbms_stopped(
    neo4j_bin: Path, env: dict[str, str], timeout_s: int = 60, interval_s: int = 2,
) -> bool:
    """Poll ``neo4j status`` until it reports stopped. Returns True if stopped
    within timeout, False otherwise (caller logs and proceeds anyway)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = subprocess.run(
            [str(neo4j_bin), "status"],
            env=env, capture_output=True, text=True, check=False,
        )
        if "Neo4j is not running" in (r.stdout + r.stderr):
            return True
        time.sleep(interval_s)
    return False


def _stage_csvs(neo4j_home: Path) -> int:
    """Symlink (or copy) every CSV + *_summary.json from data/neo4j_import/
    into ``<neo4j_home>/import/``. Idempotent. Returns count staged."""
    import_dir = neo4j_home / "import"
    import_dir.mkdir(parents=True, exist_ok=True)
    staged = 0
    sources = list(NEO4J_IMPORT.glob("*.csv")) + list(NEO4J_IMPORT.glob("*_summary.json"))
    for src in sources:
        dst = import_dir / src.name
        # Idempotent: skip if symlink already points at src
        if dst.is_symlink():
            try:
                if dst.resolve() == src.resolve():
                    LOG.debug("staged (symlink already correct): %s", dst)
                    staged += 1
                    continue
            except OSError:
                pass
            dst.unlink(missing_ok=True)
        elif dst.exists():
            dst.unlink(missing_ok=True)
        try:
            dst.symlink_to(src)
            LOG.debug("symlinked: %s -> %s", dst, src)
        except OSError:
            shutil.copy2(src, dst)
            LOG.debug("copied (symlink failed): %s -> %s", src, dst)
        staged += 1
    LOG.info("Staged %d files into %s", staged, import_dir)
    return staged


# Flag list for `neo4j-admin database import full`. Mirrors
# src/load/neo4j_import.sh:42-72 verbatim, except paths are local instead of
# /var/lib/neo4j/import/. Built dynamically once neo4j_home is known.
def _admin_import_argv(neo4j_home: Path, database: str) -> list[str]:
    imp = neo4j_home / "import"
    return [
        str(neo4j_home / "bin" / "neo4j-admin"),
        "database", "import", "full", database,
        "--overwrite-destination=true",
        "--multiline-fields=true",
        "--ignore-empty-strings=true",
        "--skip-bad-relationships=false",
        "--skip-duplicate-nodes=false",
        f"--nodes=ICDCMCode={imp / 'nodes_icd_cm.csv'}",
        f"--nodes=ICDPCSCode={imp / 'nodes_icd_pcs.csv'}",
        f"--nodes=HCPCS={imp / 'nodes_hcpcs.csv'}",
        f"--nodes=MSDRG={imp / 'nodes_msdrg.csv'}",
        f"--nodes=APC={imp / 'nodes_apc.csv'}",
        f"--nodes=IPPSStandardizedAmount={imp / 'nodes_ipps_std_amount.csv'}",
        f"--nodes=OPPSConversionFactor={imp / 'nodes_opps_cf.csv'}",
        f"--nodes=PFSConversionFactor={imp / 'nodes_pfs_cf.csv'}",
        f"--nodes=CostCenter={imp / 'nodes_cost_center.csv'}",
        f"--nodes=CCR={imp / 'nodes_ccr.csv'}",
        f"--nodes=GEMMapping={imp / 'nodes_gem_mapping.csv'}",
        f"--nodes=CostObservation={imp / 'nodes_cost_observation.csv'}",
        f"--relationships=CAN_ANCHOR={imp / 'rels_can_anchor.csv'}",
        f"--relationships=IS_MODAL_FOR={imp / 'rels_is_modal_for.csv'}",
        f"--relationships=OBSERVED_AS={imp / 'rels_observed_as_msdrg.csv'}",
        f"--relationships=OBSERVED_AS={imp / 'rels_observed_as_hcpcs.csv'}",
        f"--relationships=MAPPED_TO_APC={imp / 'rels_mapped_to_apc.csv'}",
        f"--relationships=HAS_STD_AMOUNT={imp / 'rels_has_std_amount.csv'}",
        f"--relationships=USES_CCR={imp / 'rels_uses_ccr.csv'}",
        f"--relationships=IN_COST_CENTER={imp / 'rels_in_cost_center.csv'}",
        f"--relationships=MAPS_FROM={imp / 'rels_maps_from_icd_cm.csv'}",
        f"--relationships=MAPS_FROM={imp / 'rels_maps_from_icd_pcs.csv'}",
        f"--relationships=MAPS_TO={imp / 'rels_maps_to_icd_cm.csv'}",
        f"--relationships=MAPS_TO={imp / 'rels_maps_to_icd_pcs.csv'}",
        f"--relationships=IS_REIMBURSEMENT_OF={imp / 'rels_is_reimbursement_of.csv'}",
    ]


def _wait_for_bolt(cfg: Neo4jConfig, timeout_s: int = 300, interval_s: int = 10) -> None:
    """Poll bolt connectivity via the `neo4j` Python driver. Raises
    RuntimeError if bolt does not bind within ``timeout_s``."""
    try:
        from neo4j import GraphDatabase
    except ImportError as e:
        raise RuntimeError(
            "Python `neo4j` driver required for local-admin mode bolt poll; "
            "install via `uv pip install 'neo4j>=5'`"
        ) from e
    deadline = time.time() + timeout_s
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            with GraphDatabase.driver(cfg.uri, auth=cfg.auth) as drv:
                drv.verify_connectivity()
            LOG.info("Bolt is reachable at %s", cfg.uri)
            return
        except Exception as e:  # noqa: BLE001
            last_exc = e
            time.sleep(interval_s)
    raise RuntimeError(
        f"Bolt did not bind at {cfg.uri} within {timeout_s}s "
        f"(last error: {last_exc})"
    )


def _run_cypher_shell(cfg: Neo4jConfig, env: dict[str, str], cypher_path: Path) -> None:
    """Apply a .cypher file via cypher-shell using the local-admin binary."""
    assert cfg.neo4j_home is not None  # pyrelax
    cmd = [
        str(cfg.neo4j_home / "bin" / "cypher-shell"),
        "-a", cfg.uri,
        "-u", cfg.user,
        "-p", cfg.password,
        "-d", cfg.database,
        "-f", str(cypher_path),
    ]
    LOG.info("cypher-shell -f %s ...", cypher_path)
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if r.stdout:
        LOG.info("cypher-shell stdout:\n%s", r.stdout)
    if r.returncode != 0:
        LOG.error("cypher-shell stderr:\n%s", r.stderr)
        raise subprocess.CalledProcessError(r.returncode, cmd, r.stdout, r.stderr)
    elif r.stderr:
        LOG.warning("cypher-shell stderr (non-fatal):\n%s", r.stderr)


def _run_local_admin_import(cfg: Neo4jConfig) -> None:
    """Local-admin mode: stop Desktop DBMS → stage CSVs → neo4j-admin import →
    start DBMS → wait for bolt → apply schema → apply post-load.

    Uses the Zulu 21 JDK bundled by Neo4j Desktop (neo4j-admin 2026.04.0 needs
    Java 21+). JAVA_HOME is exported only into the subprocess env, never the
    process env — so the Phase 4 Java 17 grouper is unaffected.
    """
    assert cfg.neo4j_home is not None
    neo4j_home = cfg.neo4j_home
    neo4j_bin = neo4j_home / "bin" / "neo4j"

    # 1. Resolve bundled JDK and build local subprocess env
    jdk_home = _resolve_bundled_jdk_home(neo4j_home)
    LOG.info("Using bundled JDK at %s", jdk_home)
    env = _local_env(jdk_home)

    # 2. Stop the DBMS (Desktop daemon supervisor may restart it; we warn and proceed)
    LOG.info("Stopping Neo4j DBMS ...")
    subprocess.run([str(neo4j_bin), "stop"], env=env, check=False)
    if not _wait_for_dbms_stopped(neo4j_bin, env, timeout_s=60, interval_s=2):
        LOG.warning(
            "DBMS did not report stopped within 60s; continuing anyway. "
            "neo4j-admin will fail loudly if the DB is locked."
        )
    else:
        LOG.info("DBMS stopped.")

    # 3. Stage CSVs into neo4j_home/import/
    _stage_csvs(neo4j_home)

    # 4. Run neo4j-admin database import full
    argv = _admin_import_argv(neo4j_home, cfg.database)
    LOG.info("Running neo4j-admin database import full %s ...", cfg.database)
    r = subprocess.run(argv, env=env, cwd=ROOT, capture_output=True, text=True, check=False)
    if r.stdout:
        LOG.info("admin import stdout:\n%s", r.stdout)
    if r.returncode != 0:
        LOG.error("admin import stderr:\n%s", r.stderr)
        raise subprocess.CalledProcessError(r.returncode, argv, r.stdout, r.stderr)
    elif r.stderr:
        LOG.info("admin import stderr (non-fatal):\n%s", r.stderr)

    # 5. Start DBMS and poll bolt
    LOG.info("Starting Neo4j DBMS ...")
    subprocess.run([str(neo4j_bin), "start"], env=env, check=True)
    _wait_for_bolt(cfg, timeout_s=300, interval_s=10)

    # 6. Apply schema
    _run_cypher_shell(cfg, env, ROOT / "src" / "load" / "schema.cypher")

    # 7. Apply post-load
    _run_cypher_shell(cfg, env, ROOT / "src" / "load" / "post_load.cypher")


def _dispatch_import(cfg: Neo4jConfig) -> None:
    """Route to the import path matching cfg.import_mode."""
    if cfg.import_mode == "docker":
        run_neo4j_import_sh()
    elif cfg.import_mode == "local-admin":
        _run_local_admin_import(cfg)
    elif cfg.import_mode == "bolt-apoc":
        raise NotImplementedError(
            "bolt-apoc mode reserved for managed-Neo4j environments; "
            "not implemented this session"
        )
    else:
        raise ValueError(f"Unknown import_mode: {cfg.import_mode!r}")


def query_live_graph() -> dict:
    """Connect via bolt; return label/rel counts and 4 sample-query results.
    Requires the `neo4j` Python driver. If not installed, fall back to
    cypher-shell-via-docker-exec (currently a stub).
    """
    try:
        from neo4j import GraphDatabase
    except ImportError:
        LOG.warning("Python `neo4j` driver not installed; falling back to cypher-shell")
        return query_via_cypher_shell()

    cfg = get_neo4j_config()
    counts_nodes: dict[str, int] = {}
    counts_rels: dict[str, int] = {}
    smoke: dict[str, list] = {}

    with GraphDatabase.driver(cfg.uri, auth=cfg.auth) as driver:
        with driver.session() as s:
            for label in EXPECTED_LABELS:
                rec = s.run(f"MATCH (n:`{label}`) RETURN count(n) AS c").single()
                counts_nodes[label] = rec["c"] if rec else 0
            for rel in EXPECTED_REL_TYPES:
                rec = s.run(f"MATCH ()-[r:`{rel}`]->() RETURN count(r) AS c").single()
                counts_rels[rel] = rec["c"] if rec else 0
            # 4 sample queries (matching post_load.cypher acceptance)
            smoke["q1_i214_modal"] = [
                dict(r) for r in s.run(
                    """MATCH (icd:ICDCMCode {code: 'I214', code_system: 'ICD-10-CM'})
                       -[r:IS_MODAL_FOR]->(drg:MSDRG)
                       WHERE r.fy = 2024 AND drg.fy = 2024
                       RETURN drg.code AS msdrg, drg.title LIMIT 5"""
                )
            ]
            smoke["q2_27447_pfs"] = [
                dict(r) for r in s.run(
                    """MATCH (h:HCPCS {code: '27447'})-[:OBSERVED_AS]->(obs:CostObservation)
                       WHERE obs.setting = 'pfs_facility' AND obs.concept = 'allowed_payment'
                         AND obs.year = 2024
                       RETURN h.code, obs.value, obs.source LIMIT 5"""
                )
            ]
            smoke["q3_i21_anchors"] = [
                dict(r) for r in s.run(
                    """MATCH (icd:ICDCMCode)-[r:CAN_ANCHOR]->(drg:MSDRG)
                       WHERE icd.code STARTS WITH 'I21' AND r.fy = 2024 AND drg.fy = 2024
                       RETURN DISTINCT drg.code, drg.title ORDER BY drg.code LIMIT 25"""
                )
            ]
            smoke["q4_41041_gems"] = [
                dict(r) for r in s.run(
                    """MATCH (src:ICDCMCode {code: '41041', code_system: 'ICD-9-CM'})
                       <-[:MAPS_FROM]-(gem:GEMMapping)-[:MAPS_TO]->(tgt:ICDCMCode)
                       WHERE gem.is_reimbursement = false AND tgt.code_system = 'ICD-10-CM'
                       RETURN tgt.code, gem.approximate_flag LIMIT 10"""
                )
            ]
    return {"nodes": counts_nodes, "rels": counts_rels, "smoke": smoke}


def query_via_cypher_shell() -> dict:
    """Fallback: use `docker compose exec cypher-shell` to run counts + samples.
    Stub — raises with a clear install hint.
    """
    raise RuntimeError(
        "cypher-shell fallback path not yet implemented; "
        "install `neo4j` Python driver: `uv pip install 'neo4j>=5'`"
    )


# CSV → label / rel-type crosswalk. Three rel types split across 2 CSVs each
# (OBSERVED_AS, MAPS_FROM, MAPS_TO) so we aggregate before comparing to live.
_CSV_TO_LABEL = {
    "nodes_icd_cm.csv": "ICDCMCode",
    "nodes_icd_pcs.csv": "ICDPCSCode",
    "nodes_hcpcs.csv": "HCPCS",
    "nodes_msdrg.csv": "MSDRG",
    "nodes_apc.csv": "APC",
    "nodes_cost_observation.csv": "CostObservation",
    "nodes_ipps_std_amount.csv": "IPPSStandardizedAmount",
    "nodes_opps_cf.csv": "OPPSConversionFactor",
    "nodes_pfs_cf.csv": "PFSConversionFactor",
    "nodes_ccr.csv": "CCR",
    "nodes_cost_center.csv": "CostCenter",
    "nodes_gem_mapping.csv": "GEMMapping",
}
_CSV_TO_REL = {
    "rels_can_anchor.csv": "CAN_ANCHOR",
    "rels_is_modal_for.csv": "IS_MODAL_FOR",
    "rels_observed_as_msdrg.csv": "OBSERVED_AS",
    "rels_observed_as_hcpcs.csv": "OBSERVED_AS",
    "rels_mapped_to_apc.csv": "MAPPED_TO_APC",
    "rels_has_std_amount.csv": "HAS_STD_AMOUNT",
    "rels_uses_ccr.csv": "USES_CCR",
    "rels_in_cost_center.csv": "IN_COST_CENTER",
    "rels_maps_from_icd_cm.csv": "MAPS_FROM",
    "rels_maps_from_icd_pcs.csv": "MAPS_FROM",
    "rels_maps_to_icd_cm.csv": "MAPS_TO",
    "rels_maps_to_icd_pcs.csv": "MAPS_TO",
    "rels_is_reimbursement_of.csv": "IS_REIMBURSEMENT_OF",
}


def verify_counts(load_counts: dict, build_counts: dict) -> tuple[list[str], list[str]]:
    """Check live counts against build summary; return (warnings, errors)."""
    expected_nodes: dict[str, int] = {}
    for csv, label in _CSV_TO_LABEL.items():
        expected_nodes[label] = expected_nodes.get(label, 0) + build_counts.get(csv, 0)
    expected_rels: dict[str, int] = {}
    for csv, rel in _CSV_TO_REL.items():
        expected_rels[rel] = expected_rels.get(rel, 0) + build_counts.get(csv, 0)

    warnings: list[str] = []
    errors: list[str] = []
    actual_nodes = load_counts["nodes"]
    actual_rels = load_counts["rels"]

    for label, expected in expected_nodes.items():
        actual = actual_nodes.get(label, 0)
        if actual == 0 and expected > 0:
            errors.append(f"Label {label}: expected {expected}, got 0")
        elif expected > 0:
            delta_pct = abs(actual - expected) / expected * 100
            if delta_pct > TOLERANCE_PCT:
                warnings.append(
                    f"Label {label}: expected {expected}, got {actual} "
                    f"(Δ {delta_pct:.1f}%)"
                )
    for rel, expected in expected_rels.items():
        actual = actual_rels.get(rel, 0)
        if actual == 0 and expected > 0:
            errors.append(f"Rel {rel}: expected {expected}, got 0")
        elif expected > 0:
            delta_pct = abs(actual - expected) / expected * 100
            if delta_pct > TOLERANCE_PCT:
                warnings.append(
                    f"Rel {rel}: expected {expected}, got {actual} "
                    f"(Δ {delta_pct:.1f}%)"
                )
    return warnings, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip build_csvs.py even if CSVs are stale")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip neo4j_import.sh; only re-run live-graph queries")
    parser.add_argument("--check-only", action="store_true",
                        help="Probe docker daemon; exit 0 reachable, 1 unreachable")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    cfg = get_neo4j_config()
    LOG.info("Import mode: %s", cfg.import_mode)

    # Mode-aware reachability gate. docker mode requires Docker daemon;
    # local-admin mode is gated by KG_COST_NEO4J_HOME (already validated in
    # get_neo4j_config()); bolt-apoc raises later in _dispatch_import.
    if cfg.import_mode == "docker":
        if not docker_reachable():
            if args.check_only:
                LOG.warning("Docker daemon unreachable.")
                return 1
            LOG.warning(
                "SKIP: Docker daemon unreachable. Resume with:\n"
                "  open -a Docker  # macOS\n"
                "  python -m src.load.run_import"
            )
            return 0
        if args.check_only:
            LOG.info("Docker daemon reachable.")
            return 0
    elif args.check_only:
        # local-admin / bolt-apoc: KG_COST_NEO4J_HOME validated in config load
        LOG.info("Import mode %s; config loaded OK.", cfg.import_mode)
        return 0

    if not args.skip_build and csvs_stale(BUILD_SUMMARY):
        run_build_csvs()
    elif args.skip_build:
        LOG.info("--skip-build: reusing existing CSVs")
    else:
        LOG.info("CSVs are fresh; skipping build")

    if not args.skip_import:
        _dispatch_import(cfg)
    else:
        LOG.info("--skip-import: skipping bulk import")

    LOG.info("Querying live graph for sanity counts ...")
    load_counts = query_live_graph()
    LOAD_SUMMARY.write_text(json.dumps(
        {"nodes": load_counts["nodes"], "rels": load_counts["rels"]},
        indent=2, sort_keys=True,
    ))
    SMOKE_RESULTS.write_text(json.dumps(load_counts["smoke"], indent=2, default=str))

    build_counts = json.loads(BUILD_SUMMARY.read_text())
    warnings, errors = verify_counts(load_counts, build_counts)
    for w in warnings:
        LOG.warning(w)
    for e in errors:
        LOG.error(e)

    n_labels = sum(1 for c in load_counts["nodes"].values() if c > 0)
    n_rels = sum(1 for c in load_counts["rels"].values() if c > 0)
    n_smoke_pass = sum(1 for r in load_counts["smoke"].values() if r)
    LOG.info(
        "Acceptance: %d/12 labels, %d/10 rel types, %d/4 sample queries non-empty",
        n_labels, n_rels, n_smoke_pass,
    )
    if errors or n_smoke_pass < 4:
        LOG.error("Phase 9 acceptance FAILED")
        return 1
    LOG.info("Phase 9 acceptance PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
