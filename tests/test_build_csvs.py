"""Tests for src/load/build_csvs.py.

Data-dependent. Skipped on a fresh checkout where silver/gold parquets are
absent. Verifies:
  - 12 node CSVs + 13 rel CSVs are written
  - Each CSV has the expected Neo4j 5.x typed-column header
  - No duplicate IDs within a node-label CSV
  - All :START_ID and :END_ID values in rel CSVs resolve to a node CSV
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from src.config import GOLD, NEO4J_IMPORT, SILVER

# Skip the whole module if required parquets aren't on disk.
REQUIRED_PARQUETS: list[Path] = [
    SILVER / "codes_master.parquet",
    SILVER / "ms_drg_weights.parquet",
    SILVER / "opps_apc_rates.parquet",
    SILVER / "opps_hcpcs_rates.parquet",
    SILVER / "national_ccr.parquet",
    SILVER / "ipps_std_amounts.parquet",
    SILVER / "gems_2018.parquet",
    SILVER / "pfs_rates.parquet",
    GOLD / "code_costs.parquet",
    GOLD / "icd_principal_drg_modal.parquet",
    GOLD / "icd_principal_drg_reachability.parquet",
]
pytestmark = pytest.mark.skipif(
    not all(p.exists() for p in REQUIRED_PARQUETS),
    reason="silver/gold parquets not present; data-dependent test",
)

from src.load.build_csvs import NODE_CSV_NAMES, REL_CSV_NAMES  # noqa: E402

# Sentinel column subsets per node CSV. Full header is over-fitted; we only
# spot-check the ID column, :LABEL, and a handful of payload columns whose
# spellings are decision-locked.
EXPECTED_NODE_HEADERS: dict[str, list[str]] = {
    "nodes_icd_cm.csv": ["id:ID(IcdCm-ID)", ":LABEL", "code", "code_system"],
    "nodes_icd_pcs.csv": ["id:ID(IcdPcs-ID)", ":LABEL", "code", "code_system"],
    "nodes_hcpcs.csv": ["id:ID(Hcpcs-ID)", ":LABEL", "code", "code_type"],
    "nodes_msdrg.csv": ["id:ID(MsDrg-ID)", ":LABEL", "code", "fy:int"],
    "nodes_apc.csv": ["id:ID(Apc-ID)", ":LABEL", "code", "cy:int", "quarter"],
    "nodes_cost_observation.csv": [
        "id:ID(Obs-ID)",
        ":LABEL",
        "value:double",
        "concept",
        "setting",
        "year:int",
    ],
    "nodes_ipps_std_amount.csv": [
        "id:ID(IppsStdAmount-ID)",
        ":LABEL",
        "fy:int",
    ],
    "nodes_opps_cf.csv": ["id:ID(OppsCf-ID)", ":LABEL", "cy:int", "quarter"],
    "nodes_pfs_cf.csv": ["id:ID(PfsCf-ID)", ":LABEL", "cy:int", "kind"],
    "nodes_ccr.csv": [
        "id:ID(Ccr-ID)",
        ":LABEL",
        "fy:int",
        "cost_center_group",
    ],
    "nodes_cost_center.csv": ["id:ID(CostCenter-ID)", ":LABEL", "name"],
    "nodes_gem_mapping.csv": ["id:ID(Gem-ID)", ":LABEL"],
}

EXPECTED_REL_HEADERS: dict[str, list[str]] = {
    "rels_can_anchor.csv": [
        ":START_ID(IcdCm-ID)",
        ":END_ID(MsDrg-ID)",
        ":TYPE",
    ],
    "rels_is_modal_for.csv": [
        ":START_ID(IcdCm-ID)",
        ":END_ID(MsDrg-ID)",
        ":TYPE",
    ],
    "rels_observed_as_msdrg.csv": [
        ":START_ID(MsDrg-ID)",
        ":END_ID(Obs-ID)",
        ":TYPE",
    ],
    "rels_observed_as_hcpcs.csv": [
        ":START_ID(Hcpcs-ID)",
        ":END_ID(Obs-ID)",
        ":TYPE",
    ],
    "rels_mapped_to_apc.csv": [
        ":START_ID(Hcpcs-ID)",
        ":END_ID(Apc-ID)",
        ":TYPE",
    ],
    "rels_has_std_amount.csv": [
        ":START_ID(MsDrg-ID)",
        ":END_ID(IppsStdAmount-ID)",
        ":TYPE",
    ],
    "rels_uses_ccr.csv": [
        ":START_ID(Obs-ID)",
        ":END_ID(Ccr-ID)",
        ":TYPE",
    ],
    "rels_in_cost_center.csv": [
        ":START_ID(Ccr-ID)",
        ":END_ID(CostCenter-ID)",
        ":TYPE",
    ],
    "rels_maps_from_icd_cm.csv": [
        ":START_ID(Gem-ID)",
        ":END_ID(IcdCm-ID)",
        ":TYPE",
    ],
    "rels_maps_from_icd_pcs.csv": [
        ":START_ID(Gem-ID)",
        ":END_ID(IcdPcs-ID)",
        ":TYPE",
    ],
    "rels_maps_to_icd_cm.csv": [
        ":START_ID(Gem-ID)",
        ":END_ID(IcdCm-ID)",
        ":TYPE",
    ],
    "rels_maps_to_icd_pcs.csv": [
        ":START_ID(Gem-ID)",
        ":END_ID(IcdPcs-ID)",
        ":TYPE",
    ],
    "rels_is_reimbursement_of.csv": [
        ":START_ID(Gem-ID)",
        ":END_ID(Gem-ID)",
        ":TYPE",
    ],
}

# Map ID-space -> node CSV file, used by the referential-integrity check.
LABEL_TO_CSV: dict[str, str] = {
    "IcdCm-ID": "nodes_icd_cm.csv",
    "IcdPcs-ID": "nodes_icd_pcs.csv",
    "Hcpcs-ID": "nodes_hcpcs.csv",
    "MsDrg-ID": "nodes_msdrg.csv",
    "Apc-ID": "nodes_apc.csv",
    "Obs-ID": "nodes_cost_observation.csv",
    "IppsStdAmount-ID": "nodes_ipps_std_amount.csv",
    "OppsCf-ID": "nodes_opps_cf.csv",
    "PfsCf-ID": "nodes_pfs_cf.csv",
    "Ccr-ID": "nodes_ccr.csv",
    "CostCenter-ID": "nodes_cost_center.csv",
    "Gem-ID": "nodes_gem_mapping.csv",
}

# Each rel CSV's (start_space, end_space).
REL_TO_ENDPOINTS: dict[str, tuple[str, str]] = {
    "rels_can_anchor.csv": ("IcdCm-ID", "MsDrg-ID"),
    "rels_is_modal_for.csv": ("IcdCm-ID", "MsDrg-ID"),
    "rels_observed_as_msdrg.csv": ("MsDrg-ID", "Obs-ID"),
    "rels_observed_as_hcpcs.csv": ("Hcpcs-ID", "Obs-ID"),
    "rels_mapped_to_apc.csv": ("Hcpcs-ID", "Apc-ID"),
    "rels_has_std_amount.csv": ("MsDrg-ID", "IppsStdAmount-ID"),
    "rels_uses_ccr.csv": ("Obs-ID", "Ccr-ID"),
    "rels_in_cost_center.csv": ("Ccr-ID", "CostCenter-ID"),
    "rels_maps_from_icd_cm.csv": ("Gem-ID", "IcdCm-ID"),
    "rels_maps_from_icd_pcs.csv": ("Gem-ID", "IcdPcs-ID"),
    "rels_maps_to_icd_cm.csv": ("Gem-ID", "IcdCm-ID"),
    "rels_maps_to_icd_pcs.csv": ("Gem-ID", "IcdPcs-ID"),
    "rels_is_reimbursement_of.csv": ("Gem-ID", "Gem-ID"),
}


def _read_header(csv_path: Path) -> list[str]:
    with csv_path.open() as f:
        return next(csv.reader(f))


@pytest.fixture(scope="module")
def build_summary() -> dict[str, int]:
    """Return the on-disk build summary, building if absent.

    Triggers a build only when ``_build_summary.json`` is missing — never
    overwrites a pre-existing build (Phase 8/9 artifacts on the dev box are
    expensive to recompute).
    """
    p = NEO4J_IMPORT / "_build_summary.json"
    if not p.exists():
        from src.load.build_csvs import main as build_main

        build_main()
    return json.loads(p.read_text())


def test_node_csvs_exist(build_summary: dict[str, int]) -> None:
    for name in NODE_CSV_NAMES.values():
        path = NEO4J_IMPORT / name
        assert path.exists(), f"missing {path}"
        assert build_summary.get(name, 0) > 0, f"{name} is empty"


def test_rel_csvs_exist(build_summary: dict[str, int]) -> None:
    # 13 rel CSVs (10 plan TYPEs, 3 split into 6 sub-CSVs).
    rel_csv_files = list(EXPECTED_REL_HEADERS.keys())
    assert len(rel_csv_files) == 13
    for name in rel_csv_files:
        path = NEO4J_IMPORT / name
        assert path.exists(), f"missing {path}"
        assert build_summary.get(name, 0) > 0, f"{name} is empty"
    # Cross-check against build_csvs.REL_CSV_NAMES — same 13 files.
    assert set(REL_CSV_NAMES.values()) == set(rel_csv_files)


@pytest.mark.parametrize(
    "csv_name,sentinel_cols", sorted(EXPECTED_NODE_HEADERS.items())
)
def test_node_csv_header(
    build_summary: dict[str, int], csv_name: str, sentinel_cols: list[str]
) -> None:
    header = _read_header(NEO4J_IMPORT / csv_name)
    for col in sentinel_cols:
        assert col in header, (
            f"{csv_name} missing column {col!r}; got {header}"
        )


@pytest.mark.parametrize(
    "csv_name,sentinel_cols", sorted(EXPECTED_REL_HEADERS.items())
)
def test_rel_csv_header(
    build_summary: dict[str, int], csv_name: str, sentinel_cols: list[str]
) -> None:
    header = _read_header(NEO4J_IMPORT / csv_name)
    for col in sentinel_cols:
        assert col in header, (
            f"{csv_name} missing column {col!r}; got {header}"
        )


@pytest.mark.parametrize("csv_name", sorted(NODE_CSV_NAMES.values()))
def test_no_duplicate_node_ids(csv_name: str) -> None:
    """Every node CSV must have unique IDs in the first column.

    The first column header is always ``id:ID(<Space>)``; we read it via
    the typed name (DuckDB preserves the literal CSV header as the column
    identifier) and compare ``count(*)`` to ``count(distinct id)``.
    """
    p = NEO4J_IMPORT / csv_name
    con = duckdb.connect()
    # Discover the typed first-column name from the actual header.
    header = _read_header(p)
    first = header[0]
    n_total = con.execute(
        f"SELECT count(*) FROM read_csv_auto('{p.as_posix()}')"
    ).fetchone()[0]
    n_distinct = con.execute(
        f'SELECT count(DISTINCT "{first}") '
        f"FROM read_csv_auto('{p.as_posix()}')"
    ).fetchone()[0]
    assert n_total == n_distinct, (
        f"{csv_name}: {n_total - n_distinct} duplicate IDs "
        f"(total={n_total}, distinct={n_distinct})"
    )


@pytest.mark.parametrize(
    "rel_csv,endpoints", sorted(REL_TO_ENDPOINTS.items())
)
def test_referential_integrity(
    build_summary: dict[str, int],
    rel_csv: str,
    endpoints: tuple[str, str],
) -> None:
    """Every :START_ID and :END_ID value must resolve to a node CSV.

    Reads each CSV with ``read_csv_auto`` (which preserves the literal
    typed header as the column name) and ANTI JOINs the rel against the
    target node CSV on the ID-space-tagged columns.
    """
    start_space, end_space = endpoints
    rel_path = NEO4J_IMPORT / rel_csv
    start_node_path = NEO4J_IMPORT / LABEL_TO_CSV[start_space]
    end_node_path = NEO4J_IMPORT / LABEL_TO_CSV[end_space]

    rel_start_col = f":START_ID({start_space})"
    rel_end_col = f":END_ID({end_space})"
    start_id_col = f"id:ID({start_space})"
    end_id_col = f"id:ID({end_space})"

    con = duckdb.connect()
    unmapped_start = con.execute(
        f"""
        SELECT count(*) FROM read_csv_auto('{rel_path.as_posix()}') r
        ANTI JOIN read_csv_auto('{start_node_path.as_posix()}') n
          ON r."{rel_start_col}" = n."{start_id_col}"
        """
    ).fetchone()[0]
    unmapped_end = con.execute(
        f"""
        SELECT count(*) FROM read_csv_auto('{rel_path.as_posix()}') r
        ANTI JOIN read_csv_auto('{end_node_path.as_posix()}') n
          ON r."{rel_end_col}" = n."{end_id_col}"
        """
    ).fetchone()[0]
    assert unmapped_start == 0, (
        f"{rel_csv}: {unmapped_start} unmapped START_IDs "
        f"(target={LABEL_TO_CSV[start_space]})"
    )
    assert unmapped_end == 0, (
        f"{rel_csv}: {unmapped_end} unmapped END_IDs "
        f"(target={LABEL_TO_CSV[end_space]})"
    )
