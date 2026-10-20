"""Tests for src/load/schema.cypher.

Lightweight regex-based linter: confirms the cypher script is well-formed,
references all 12 expected node labels, declares constraints with correct
property keys, and matches Neo4j 5.x syntax. Does NOT execute Cypher — runs
on every CI build.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.config import ROOT

SCHEMA_PATH: Path = ROOT / "src" / "load" / "schema.cypher"

EXPECTED_LABELS: set[str] = {
    "ICDCMCode",
    "ICDPCSCode",
    "HCPCS",
    "MSDRG",
    "APC",
    "CostObservation",
    "IPPSStandardizedAmount",
    "OPPSConversionFactor",
    "PFSConversionFactor",
    "CCR",
    "CostCenter",
    "GEMMapping",
}

EXPECTED_CONSTRAINT_NAMES: set[str] = {
    "icd_cm_pk",
    "icd_pcs_pk",
    "hcpcs_pk",
    "msdrg_pk",
    "apc_pk",
    "cost_obs_pk",
    "ipps_std_pk",
    "opps_cf_pk",
    "pfs_cf_pk",
    "ccr_pk",
    "cost_center_pk",
    "gem_pk",
}

# Each constraint declares which property(ies) must be unique. Map
# constraint_name -> set of property names.
EXPECTED_CONSTRAINT_PROPS: dict[str, set[str]] = {
    "icd_cm_pk": {"code", "code_system"},
    "icd_pcs_pk": {"code", "code_system"},
    "hcpcs_pk": {"code"},
    "msdrg_pk": {"code", "fy"},
    "apc_pk": {"code", "cy", "quarter"},
    "cost_obs_pk": {"id"},
    "ipps_std_pk": {"fy"},
    "opps_cf_pk": {"cy", "quarter"},
    "pfs_cf_pk": {"cy", "kind"},
    "ccr_pk": {"fy", "cost_center_group"},
    "cost_center_pk": {"name"},
    "gem_pk": {"id"},
}


@pytest.fixture(scope="module")
def schema_text() -> str:
    return SCHEMA_PATH.read_text()


def test_schema_file_exists() -> None:
    assert SCHEMA_PATH.exists(), f"missing {SCHEMA_PATH}"


def test_constraint_count(schema_text: str) -> None:
    constraints = re.findall(r"CREATE CONSTRAINT (\w+)", schema_text)
    assert len(constraints) == 12, (
        f"expected 12 constraints, got {len(constraints)}: {constraints}"
    )
    assert set(constraints) == EXPECTED_CONSTRAINT_NAMES


def test_index_count(schema_text: str) -> None:
    indexes = re.findall(r"CREATE INDEX (\w+)", schema_text)
    assert len(indexes) >= 5, f"expected >= 5 indexes, got {len(indexes)}"


def test_all_labels_referenced(schema_text: str) -> None:
    # Each label must appear at least once in a "FOR (n:LABEL)" clause.
    found = set(re.findall(r"FOR\s*\(\s*n\s*:\s*(\w+)\s*\)", schema_text))
    missing = EXPECTED_LABELS - found
    assert not missing, f"schema missing label(s): {missing}"


@pytest.mark.parametrize(
    "name,props", sorted(EXPECTED_CONSTRAINT_PROPS.items())
)
def test_constraint_property_set(
    schema_text: str, name: str, props: set[str]
) -> None:
    # Match: CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:Label)
    #        REQUIRE (n.a, n.b) IS UNIQUE
    # OR:    REQUIRE n.a IS UNIQUE   (single prop)
    pat = rf"CREATE CONSTRAINT {name}\b.*?REQUIRE\s*\(?(.+?)\)?\s*IS\s+UNIQUE"
    m = re.search(pat, schema_text, re.DOTALL)
    assert m, f"could not parse REQUIRE clause for {name}"
    actual = set(re.findall(r"n\.(\w+)", m.group(1)))
    assert actual == props, f"{name}: expected {props}, got {actual}"


def test_idempotent_modifier(schema_text: str) -> None:
    # Every CREATE CONSTRAINT/INDEX must be IF NOT EXISTS for re-runnability.
    constraint_blocks = re.findall(r"CREATE CONSTRAINT[^;]*;", schema_text)
    assert constraint_blocks, "no CREATE CONSTRAINT statements found"
    for block in constraint_blocks:
        assert "IF NOT EXISTS" in block, f"missing IF NOT EXISTS: {block!r}"
    index_blocks = re.findall(r"CREATE INDEX[^;]*;", schema_text)
    assert index_blocks, "no CREATE INDEX statements found"
    for block in index_blocks:
        assert "IF NOT EXISTS" in block, f"missing IF NOT EXISTS: {block!r}"


def test_constraint_names_short(schema_text: str) -> None:
    constraints = re.findall(r"CREATE CONSTRAINT (\w+)", schema_text)
    indexes = re.findall(r"CREATE INDEX (\w+)", schema_text)
    for name in constraints + indexes:
        assert len(name) <= 30, f"name too long: {name}"
