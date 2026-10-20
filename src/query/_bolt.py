"""Bolt driver helper for Phase 10 sample queries and cohort_annotator.

Wraps neo4j-python-driver and parses src/query/sample_queries.cypher into
a name -> (cypher, default_params) cache. Re-parses if the file mtime changes.

This module is the single source of truth for `(driver, query) -> records`
and is reused by:
  - tests/test_sample_queries.py (parses + runs each query)
  - src/query/cohort_annotator.py (bolt-mode lookups)
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import neo4j

from src.load._config import get_neo4j_config

_QUERIES_PATH = Path(__file__).resolve().parent / "sample_queries.cypher"
_HEADER_RE = re.compile(r"^//\s*===\s*(Q\d+_\w+)\s*===\s*$")
_PARAM_RE = re.compile(r"^//\s*:param\s+(\w+)\s*=>\s*(.+?)\s*$")

# Module-level cache: (mtime_ns, {name: (cypher_text, default_params)})
_CACHE: tuple[int, dict[str, tuple[str, dict[str, Any]]]] | None = None


def _parse_queries(text: str) -> dict[str, tuple[str, dict[str, Any]]]:
    """Parse the sample_queries.cypher format. Returns a dict keyed by query name.

    Format (one query):
        // === Q1_icd_modal_cost ===
        // :param icd_code => 'I214'
        // :param fy => 2024
        MATCH ... ;

    Headers and param-comments are stripped from the body. The body ends at
    the next `// ===` marker or EOF. The trailing semicolon is preserved.
    """
    queries: dict[str, tuple[str, dict[str, Any]]] = {}
    current_name: str | None = None
    current_params: dict[str, Any] = {}
    current_body: list[str] = []

    def flush() -> None:
        if current_name is None:
            return
        body = "\n".join(current_body).strip()
        if body:
            queries[current_name] = (body, dict(current_params))

    for line in text.splitlines():
        stripped = line.strip()
        m_header = _HEADER_RE.match(line)
        if m_header:
            flush()
            current_name = m_header.group(1)
            current_params = {}
            current_body = []
            continue
        if current_name is None:
            continue
        m_param = _PARAM_RE.match(line)
        if m_param:
            param_name = m_param.group(1)
            param_default_str = m_param.group(2)
            try:
                param_default = ast.literal_eval(param_default_str)
            except (SyntaxError, ValueError) as e:
                raise ValueError(
                    f"Could not parse default for ${param_name} in {current_name}: "
                    f"{param_default_str!r} ({e})"
                )
            current_params[param_name] = param_default
            continue
        # comment line that isn't a header or param - skip silently to keep
        # bodies clean; allows annotation comments inside the file
        if stripped.startswith("//"):
            continue
        current_body.append(line)
    flush()
    return queries


def _load_cache() -> dict[str, tuple[str, dict[str, Any]]]:
    """Read sample_queries.cypher (cache by mtime; re-parses on change)."""
    global _CACHE
    if not _QUERIES_PATH.exists():
        raise FileNotFoundError(f"sample_queries.cypher not found at {_QUERIES_PATH}")
    mtime = _QUERIES_PATH.stat().st_mtime_ns
    if _CACHE is not None and _CACHE[0] == mtime:
        return _CACHE[1]
    queries = _parse_queries(_QUERIES_PATH.read_text())
    _CACHE = (mtime, queries)
    return queries


def get_driver() -> neo4j.Driver:
    """Return a Bolt driver from get_neo4j_config(). Caller closes."""
    cfg = get_neo4j_config()
    return neo4j.GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))


def read_query(name: str) -> tuple[str, dict[str, Any]]:
    """Return (cypher_text, default_params) for the named query."""
    queries = _load_cache()
    if name not in queries:
        raise KeyError(
            f"Query {name!r} not found in sample_queries.cypher. "
            f"Available: {sorted(queries)}"
        )
    return queries[name]


def run_query(
    driver: neo4j.Driver,
    name: str,
    **params: Any,
) -> list[dict[str, Any]]:
    """Run the named query with merged (defaults + caller-supplied) params.

    Returns a list of records as dicts. No retry; caller decides retry strategy.
    Uses get_neo4j_config().database for the session target.
    """
    cypher, defaults = read_query(name)
    merged = {**defaults, **params}
    cfg = get_neo4j_config()
    with driver.session(database=cfg.database) as session:
        return [r.data() for r in session.run(cypher, **merged)]


def list_queries() -> list[str]:
    """Return the list of query names defined in sample_queries.cypher."""
    return sorted(_load_cache().keys())
