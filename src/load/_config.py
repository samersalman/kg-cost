"""Runtime config for the Neo4j load (Phase 9 / Session 10.5).

Resolves bolt URI, credentials, database name, and import mode from .env file
(if present) → environment variables → defaults. The .env file lives at repo
root; loading is best-effort (no error if missing — env vars or defaults take
over).

Defaults preserve the docker-compose stack (`kgcostlocal`) for backward-compat;
override via env or .env to point at a Neo4j Desktop / standalone instance.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

VALID_MODES = ("docker", "local-admin", "bolt-apoc")


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str
    database: str
    import_mode: str
    neo4j_home: Path | None  # required when import_mode == "local-admin"

    @property
    def auth(self) -> tuple[str, str]:
        return (self.user, self.password)


def _load_dotenv(repo_root: Path) -> None:
    """Best-effort: load `.env` at repo root into os.environ if present.
    No-op on missing python-dotenv or missing .env file. Existing env vars
    win (override=False)."""
    env_path = repo_root / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv as _ld
    except ImportError:
        return
    _ld(env_path, override=False)


def get_neo4j_config(repo_root: Path | None = None) -> Neo4jConfig:
    """Resolve Neo4jConfig from .env → env → defaults.

    Args:
        repo_root: repo root for .env discovery. Defaults to src.config.ROOT.

    Raises:
        ValueError: if KG_COST_IMPORT_MODE is not a recognized value, or if
            mode is "local-admin" without KG_COST_NEO4J_HOME set, or if
            KG_COST_NEO4J_HOME (when set) does not contain bin/neo4j-admin.
    """
    if repo_root is None:
        from src.config import ROOT as _ROOT
        repo_root = _ROOT
    _load_dotenv(repo_root)

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "kgcostlocal")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    mode = os.getenv("KG_COST_IMPORT_MODE", "docker")
    if mode not in VALID_MODES:
        raise ValueError(
            f"KG_COST_IMPORT_MODE={mode!r} is not one of {VALID_MODES}"
        )
    home_raw = os.getenv("KG_COST_NEO4J_HOME")
    home = Path(home_raw).expanduser().resolve() if home_raw else None
    if mode == "local-admin":
        if home is None:
            raise ValueError(
                "KG_COST_IMPORT_MODE=local-admin requires KG_COST_NEO4J_HOME"
            )
        if not (home / "bin" / "neo4j-admin").exists():
            raise ValueError(
                f"KG_COST_NEO4J_HOME={home} does not contain bin/neo4j-admin"
            )
    return Neo4jConfig(
        uri=uri, user=user, password=password, database=database,
        import_mode=mode, neo4j_home=home,
    )
