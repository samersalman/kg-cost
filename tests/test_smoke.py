"""Phase 0 smoke tests: package imports, path constants, and config helpers."""

from datetime import date
from pathlib import Path

from src import config
from src.config import (
    BRONZE,
    GOLD,
    HCPCS_CYS,
    ICD9_TIER_BC_FYS,
    ICD10CM_SOURCES,
    NEO4J_IMPORT,
    ROOT,
    SILVER,
    SILVER_PER_SOURCE,
    TIER_A_FYS,
    bronze_dir,
    cy_interval,
    fy_interval,
)


def test_root_and_paths() -> None:
    assert ROOT.exists()
    assert (ROOT / "pyproject.toml").exists()
    for p in (BRONZE, SILVER, GOLD, NEO4J_IMPORT, SILVER_PER_SOURCE):
        assert isinstance(p, Path)


def test_tier_a_fys() -> None:
    assert len(TIER_A_FYS) == 11
    assert TIER_A_FYS == list(range(2016, 2027))


def test_icd10cm_sources_manifest() -> None:
    assert len(ICD10CM_SOURCES) == 11
    for fy in range(2016, 2027):
        assert fy in ICD10CM_SOURCES
        assert ICD10CM_SOURCES[fy].startswith("https://ftp.cdc.gov/")


def test_fy_interval() -> None:
    assert fy_interval(2026) == (date(2025, 10, 1), date(2026, 9, 30))


def test_cy_interval() -> None:
    assert cy_interval(2020) == (date(2020, 1, 1), date(2020, 12, 31))


def test_bronze_dir() -> None:
    assert bronze_dir("icd10cm", 2026, "2026-04-29") == (
        config.BRONZE / "icd10cm" / "fy2026" / "2026-04-29"
    )


def test_bronze_dir_cy_period() -> None:
    assert bronze_dir("hcpcs", 2020, "2026-04-29", period="cy") == (
        config.BRONZE / "hcpcs" / "cy2020" / "2026-04-29"
    )


def test_icd9_tier_bc_fys() -> None:
    assert ICD9_TIER_BC_FYS == list(range(2008, 2016))
    assert len(ICD9_TIER_BC_FYS) == 8


def test_hcpcs_cys() -> None:
    assert HCPCS_CYS == list(range(2008, 2027))
    assert len(HCPCS_CYS) == 19


def test_pyreadstat_importable() -> None:
    """pyreadstat is required by Phase 6 HCRIS extractor (Session 9.5).

    Reads provider-level HCRIS SAS datasets without a SAS license. Wheel
    bundles libreadstat for macOS arm64. If this probe ever fails, the
    Phase 6 transform (`src.transform.hcris_ccrs`) will skip with a
    clear error rather than crashing mid-run.
    """
    import pyreadstat  # noqa: F401

    assert hasattr(pyreadstat, "read_sas7bdat")


def test_docker_probe_warn_only() -> None:
    """Warn-only probe for docker availability.

    Phase 9 (Neo4j bulk import) can use Docker. This test never fails — it
    just records whether ``docker --version`` is reachable on the dev
    machine. A failed probe is expected on minimal CI / first-clone
    setups; the live-Neo4j smoke test (`test_neo4j_smoke.py`) is gated by
    both ``KG_COST_RUN_NEO4J=1`` and a reachable Neo4j target (Docker OR
    local Desktop install), so a missing Docker here just means the docker
    branch of that gate stays closed.
    """
    import shutil
    import subprocess

    if shutil.which("docker") is None:
        # warn-only — record but do not fail
        return
    try:
        subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            timeout=5,
            check=True,
        )
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        pass  # warn-only


def test_neo4j_target_present() -> None:
    """Warn-only probe for a Neo4j import target (Docker OR local Desktop).

    Phase 9 bulk import requires either a reachable Docker daemon
    (``KG_COST_IMPORT_MODE=docker``) or a valid ``KG_COST_NEO4J_HOME``
    pointing at a Neo4j 5.x / 2026.x install (``KG_COST_IMPORT_MODE=local-admin``).
    This test never fails — it just records which (if any) is configured.
    The live-Neo4j smoke test (`test_neo4j_smoke.py`) gates on the
    reachability of the configured target; a missing target here just means
    that gate stays closed.
    """
    import shutil
    import subprocess

    docker_ok = False
    if shutil.which("docker") is not None:
        try:
            r = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=10,
                check=False,
            )
            docker_ok = r.returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            docker_ok = False

    local_ok = False
    try:
        from src.load._config import get_neo4j_config
        cfg = get_neo4j_config()
        if cfg.import_mode == "local-admin" and cfg.neo4j_home is not None:
            local_ok = (cfg.neo4j_home / "bin" / "neo4j-admin").exists()
    except (ValueError, ImportError):
        local_ok = False

    if docker_ok or local_ok:
        return
    # warn-only — neither target configured; fall through silently
