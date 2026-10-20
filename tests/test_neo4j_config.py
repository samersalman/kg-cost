"""Tests for src.load._config.get_neo4j_config (Phase 9 / Session 10.5).

Validates env-var precedence, default values, mode validation, and the
local-admin contract requiring a real bin/neo4j-admin under KG_COST_NEO4J_HOME.

Each test clears the relevant env vars via monkeypatch and passes a tmp_path
repo_root so the existing repo-root .env file does not leak in.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.load._config import VALID_MODES, Neo4jConfig, get_neo4j_config

_ENV_KEYS = (
    "NEO4J_URI",
    "NEO4J_USER",
    "NEO4J_PASSWORD",
    "NEO4J_DATABASE",
    "KG_COST_IMPORT_MODE",
    "KG_COST_NEO4J_HOME",
)


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_defaults_when_no_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With no env vars and no .env at the supplied repo_root, defaults apply."""
    _clear_env(monkeypatch)
    cfg = get_neo4j_config(repo_root=tmp_path)
    assert isinstance(cfg, Neo4jConfig)
    assert cfg.uri == "bolt://localhost:7687"
    assert cfg.user == "neo4j"
    assert cfg.password == "kgcostlocal"
    assert cfg.database == "neo4j"
    assert cfg.import_mode == "docker"
    assert cfg.neo4j_home is None
    assert cfg.auth == ("neo4j", "kgcostlocal")


def test_env_vars_override_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Env vars take precedence over defaults."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("NEO4J_URI", "neo4j://example.com:7687")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    monkeypatch.setenv("KG_COST_IMPORT_MODE", "bolt-apoc")
    cfg = get_neo4j_config(repo_root=tmp_path)
    assert cfg.uri == "neo4j://example.com:7687"
    assert cfg.password == "secret"
    assert cfg.import_mode == "bolt-apoc"
    # Unset vars still pick up defaults
    assert cfg.user == "neo4j"
    assert cfg.database == "neo4j"


def test_invalid_import_mode_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unrecognized KG_COST_IMPORT_MODE value raises ValueError."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("KG_COST_IMPORT_MODE", "kubernetes")
    with pytest.raises(ValueError, match="KG_COST_IMPORT_MODE"):
        get_neo4j_config(repo_root=tmp_path)


def test_local_admin_without_home_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KG_COST_IMPORT_MODE=local-admin requires KG_COST_NEO4J_HOME."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("KG_COST_IMPORT_MODE", "local-admin")
    with pytest.raises(ValueError, match="KG_COST_NEO4J_HOME"):
        get_neo4j_config(repo_root=tmp_path)


def test_local_admin_home_missing_neo4j_admin_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A KG_COST_NEO4J_HOME that lacks bin/neo4j-admin is rejected."""
    _clear_env(monkeypatch)
    fake_home = tmp_path / "fake_neo4j_home"
    fake_home.mkdir()
    monkeypatch.setenv("KG_COST_IMPORT_MODE", "local-admin")
    monkeypatch.setenv("KG_COST_NEO4J_HOME", str(fake_home))
    with pytest.raises(ValueError, match="bin/neo4j-admin"):
        get_neo4j_config(repo_root=tmp_path)


def test_local_admin_with_valid_home_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A KG_COST_NEO4J_HOME containing bin/neo4j-admin is accepted."""
    _clear_env(monkeypatch)
    fake_home = tmp_path / "neo4j_home"
    (fake_home / "bin").mkdir(parents=True)
    admin = fake_home / "bin" / "neo4j-admin"
    admin.write_text("#!/bin/sh\nexit 0\n")
    admin.chmod(0o755)
    monkeypatch.setenv("KG_COST_IMPORT_MODE", "local-admin")
    monkeypatch.setenv("KG_COST_NEO4J_HOME", str(fake_home))
    cfg = get_neo4j_config(repo_root=tmp_path)
    assert cfg.import_mode == "local-admin"
    assert cfg.neo4j_home == fake_home.resolve()


def test_valid_modes_constant() -> None:
    """Sanity-check VALID_MODES is the contract Step 5 will dispatch on."""
    assert set(VALID_MODES) == {"docker", "local-admin", "bolt-apoc"}
