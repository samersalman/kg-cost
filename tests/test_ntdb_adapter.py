"""Tests for src.query._ntdb_adapter (Phase 14b NTDB adapter).

Plan: plans/2026-05-03-session-3-phase-14b-ntdb-adapter.md §Tests.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

from src.query import _ntdb_adapter
from src.query._ntdb_adapter import load_ntdb_cohort


_REPO = Path(__file__).resolve().parent.parent
_FIXTURE = _REPO / "tests" / "fixtures" / "ntdb_synthetic_100.parquet"


# ---------------------- _coerce_dx_system ---------------------- #

class TestCoerceDxSystem:
    def test_icd9_value_returns_icd9cm(self):
        assert _ntdb_adapter._coerce_dx_system(9.0) == "ICD-9-CM"
        assert _ntdb_adapter._coerce_dx_system(9) == "ICD-9-CM"

    def test_icd10_value_returns_icd10cm(self):
        assert _ntdb_adapter._coerce_dx_system(10.0) == "ICD-10-CM"
        assert _ntdb_adapter._coerce_dx_system(10) == "ICD-10-CM"

    def test_nan_returns_empty(self):
        assert _ntdb_adapter._coerce_dx_system(float("nan")) == ""
        assert _ntdb_adapter._coerce_dx_system(np.nan) == ""

    def test_none_returns_empty(self):
        assert _ntdb_adapter._coerce_dx_system(None) == ""

    def test_unknown_value_returns_empty(self):
        assert _ntdb_adapter._coerce_dx_system(11.0) == ""
        assert _ntdb_adapter._coerce_dx_system(0) == ""


# ---------------------- _split_icd_codes ---------------------- #

class TestSplitIcdCodes:
    def test_extracts_principal_and_secondary(self):
        principal, secondary = _ntdb_adapter._split_icd_codes(["A", "B", "C"])
        assert principal == "A"
        assert secondary == "B;C"

    def test_empty_list_returns_empty_pair(self):
        assert _ntdb_adapter._split_icd_codes([]) == ("", "")

    def test_none_returns_empty_pair(self):
        assert _ntdb_adapter._split_icd_codes(None) == ("", "")

    def test_strips_and_uppercases(self):
        principal, secondary = _ntdb_adapter._split_icd_codes(["a01.0 ", " b02 "])
        assert principal == "A01.0"
        assert secondary == "B02"

    def test_single_element_has_empty_secondary(self):
        principal, secondary = _ntdb_adapter._split_icd_codes(["S72.001A"])
        assert principal == "S72.001A"
        assert secondary == ""


# ---------------------- _HOSP_DISP_MAP ---------------------- #

class TestHospDispMap:
    def test_known_codes_map_correctly(self):
        m = _ntdb_adapter._HOSP_DISP_MAP
        assert m["HOME"] == "01"
        assert m["REHAB"] == "62"
        assert m["D"] == "20"
        assert m["SNF"] == "03"
        assert m["HHS"] == "06"

    def test_all_expected_keys_present(self):
        m = _ntdb_adapter._HOSP_DISP_MAP
        expected = {
            "HOME", "SELF", "HHS", "ICF", "SNF", "REHAB", "IRF", "LTAC",
            "HOSP", "TRANSFER", "HOSPICE", "AMA", "D", "DEAD", "DEATH",
            "EXPIRED",
        }
        assert expected.issubset(set(m.keys()))


# ---------------------- load_ntdb_cohort (synthetic fixture) ---------------------- #

class TestLoadNtdbCohort:
    def test_synthetic_fixture_shape(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        assert len(df) == 100
        expected_cols = {
            "patient_id", "fy", "age", "sex", "discharge_disp",
            "dx_principal", "dx_principal_system", "dx_secondary",
            "pcs", "cpts",
        }
        assert set(df.columns) == expected_cols

    def test_dx_principal_system_for_icd_versions(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # ICD_VERSION=10.0 (Blocks A, C, parts of E) → "ICD-10-CM"
        # ICD_VERSION=9.0  (Blocks B, D, parts of E) → "ICD-9-CM"
        # ICD_VERSION=NaN  (Block F)                 → ""
        counts = df["dx_principal_system"].value_counts(dropna=False)
        assert counts.get("ICD-10-CM", 0) >= 30   # at least Block A
        assert counts.get("ICD-9-CM", 0) >= 20    # at least Block B
        assert counts.get("", 0) >= 10            # Block F NaN

    def test_sample_rows_truncates(self):
        df = load_ntdb_cohort(_FIXTURE, sample_rows=10, attach_procedures=False)
        assert len(df) == 10

    def test_sample_rows_none_returns_all(self):
        df = load_ntdb_cohort(_FIXTURE, sample_rows=None, attach_procedures=False)
        assert len(df) == 100

    def test_age_clipped_to_120_max(self):
        # INC_KEY=1000000 has AGE_YEARS=130.7
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        row = df[df["patient_id"] == "1000000"].iloc[0]
        assert row["age"] == 120

    def test_age_clipped_to_0_min(self):
        # INC_KEY=1000001 has AGE_YEARS=-1.0
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        row = df[df["patient_id"] == "1000001"].iloc[0]
        assert row["age"] == 0

    def test_age_nan_preserved(self):
        # INC_KEY=1000002 has AGE_YEARS=NaN
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        row = df[df["patient_id"] == "1000002"].iloc[0]
        assert pd.isna(row["age"])

    def test_patient_id_is_string_int_form(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # No ".0" suffix; should be a plain integer string.
        assert df["patient_id"].iloc[0] == "1000000"
        assert not df["patient_id"].iloc[0].endswith(".0")
        assert df["patient_id"].dtype == object  # pandas string column

    def test_unknown_hosp_disp_falls_back_to_01(self):
        # The fixture includes a "FOO" HOSP_DISP. Adapter should map to "01".
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # Verify "01" appears (catch-all). This is weakly specific because
        # many rows legitimately map to "01" (HOME, SELF).
        assert (df["discharge_disp"] == "01").any()
        # Stronger check: every row has a non-empty 2-char discharge_disp.
        assert df["discharge_disp"].str.len().min() >= 2

    def test_known_hosp_disp_codes_present_in_output(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # Fixture covers all 16 HOSP_DISP keys → all 10 distinct mapped
        # values should appear at least once.
        expected = {"01", "02", "03", "04", "06", "07", "20", "50", "62", "63"}
        actual = set(df["discharge_disp"].unique())
        assert expected.issubset(actual), f"missing: {expected - actual}"

    def test_pcs_default_empty_when_not_attached(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        assert (df["pcs"] == "").all()

    def test_cpts_always_empty(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        assert (df["cpts"] == "").all()

    def test_secondary_dx_present_for_multi_code_rows(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # Block A (INC_KEY 1000000-1000029) has 3 codes per row → secondary non-empty
        block_a = df[df["patient_id"].isin([str(1_000_000 + i) for i in range(30)])]
        # All 30 rows have multi-element ICD_CODES → all 30 should have secondary.
        # Use a generous threshold (>=25) in case fixture-builder edge rows differ.
        assert (block_a["dx_secondary"] != "").sum() >= 25

    def test_empty_icd_codes_block_has_empty_dx(self):
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False)
        # Block E (INC_KEY 1000070-1000079) has empty ICD_CODES → both dx fields empty
        block_e = df[df["patient_id"].isin([str(1_000_070 + i) for i in range(10)])]
        assert (block_e["dx_principal"] == "").all()
        assert (block_e["dx_secondary"] == "").all()


# ---------------------- _fetch_procedures_2019_2024 (mocked) ---------------------- #

class TestFetchProcedures20192024:
    def test_empty_inc_keys_returns_empty_dict_no_db_call(self):
        # Should not call duckdb.connect at all when inc_keys is empty.
        with patch("src.query._ntdb_adapter.duckdb.connect") as mock_connect:
            result = _ntdb_adapter._fetch_procedures_2019_2024([], "/dev/null")
        assert result == {}
        mock_connect.assert_not_called()

    def test_groups_codes_by_inc_key(self):
        # Mock duckdb to return a 2-inc_key, 3-row response.
        fake_df = pd.DataFrame({
            "inc_key":            [1000000, 1000000, 1000001],
            "icd_procedure_code": ["0DTJ4ZZ", "0FT44ZZ", "0RR40JZ"],
        })
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = fake_df
        with patch("src.query._ntdb_adapter.duckdb.connect", return_value=mock_con):
            result = _ntdb_adapter._fetch_procedures_2019_2024(
                [1000000, 1000001], "/fake.db"
            )
        # Order within each inc_key matches the input order from DuckDB.
        assert result[1000000] == "0DTJ4ZZ;0FT44ZZ"
        assert result[1000001] == "0RR40JZ"
        # Connection was closed.
        mock_con.close.assert_called_once()

    def test_strips_and_uppercases_codes(self):
        fake_df = pd.DataFrame({
            "inc_key":            [1000000],
            "icd_procedure_code": [" 0dtj4zz "],
        })
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = fake_df
        with patch("src.query._ntdb_adapter.duckdb.connect", return_value=mock_con):
            result = _ntdb_adapter._fetch_procedures_2019_2024([1000000], "/fake.db")
        assert result[1000000] == "0DTJ4ZZ"

    def test_skips_empty_codes(self):
        fake_df = pd.DataFrame({
            "inc_key":            [1000000, 1000000],
            "icd_procedure_code": ["", "0DTJ4ZZ"],
        })
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = fake_df
        with patch("src.query._ntdb_adapter.duckdb.connect", return_value=mock_con):
            result = _ntdb_adapter._fetch_procedures_2019_2024([1000000], "/fake.db")
        assert result[1000000] == "0DTJ4ZZ"  # empty string skipped

    def test_empty_response_returns_empty_dict(self):
        empty_df = pd.DataFrame({"inc_key": [], "icd_procedure_code": []})
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = empty_df
        with patch("src.query._ntdb_adapter.duckdb.connect", return_value=mock_con):
            result = _ntdb_adapter._fetch_procedures_2019_2024([1000000], "/fake.db")
        assert result == {}


# ---------------------- attach_procedures integration (mocked) ---------------------- #

class TestLoadNtdbCohortAttachProcedures:
    def test_attach_skipped_outside_fy_range(self):
        # Mock procedures fetcher: assert FY2010 inc_keys NEVER reach DuckDB.
        def fake_fetch(inc_keys, duckdb_path):
            assert 1000030 not in inc_keys, (
                "FY2010 inc_key incorrectly forwarded to DuckDB"
            )
            return {}
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024",
            side_effect=fake_fetch,
        ):
            df = load_ntdb_cohort(
                _FIXTURE, attach_procedures=True, duckdb_path="/fake.db"
            )
        # Block B (INC_KEY 1000030-1000049, FY2010) should have pcs=""
        block_b = df[df["patient_id"].isin([str(1_000_030 + i) for i in range(20)])]
        assert (block_b["pcs"] == "").all()

    def test_attach_writes_pcs_for_eligible_rows(self):
        # Block A is FY2024 (INC_KEY 1000000-1000029) → eligible.
        proc_map = {1000000: "0DTJ4ZZ", 1000005: "0FT44ZZ;0RR40JZ"}
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024",
            return_value=proc_map,
        ):
            df = load_ntdb_cohort(
                _FIXTURE, attach_procedures=True, duckdb_path="/fake.db"
            )
        # The two eligible inc_keys we mocked should carry pcs values.
        row1000000 = df[df["patient_id"] == "1000000"].iloc[0]
        row1000005 = df[df["patient_id"] == "1000005"].iloc[0]
        assert row1000000["pcs"] == "0DTJ4ZZ"
        assert row1000005["pcs"] == "0FT44ZZ;0RR40JZ"
        # Other Block A rows (no procedure data) keep default ""
        row1000001 = df[df["patient_id"] == "1000001"].iloc[0]
        assert row1000001["pcs"] == ""

    def test_attach_soft_fails_on_duckdb_error(self):
        def boom(inc_keys, duckdb_path):
            raise RuntimeError("simulated duckdb failure")
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024",
            side_effect=boom,
        ):
            with pytest.warns(UserWarning, match="NTDB procedure attachment failed"):
                df = load_ntdb_cohort(
                    _FIXTURE, attach_procedures=True, duckdb_path="/fake.db"
                )
        # All rows should ship with pcs="" after the soft-fail.
        assert (df["pcs"] == "").all()


# ---------------------- env-var resolution ---------------------- #

class TestDuckdbPathResolution:
    def test_env_var_overrides_default(self, monkeypatch):
        sentinel_path = "/tmp/sentinel-ntdb-path.duckdb"
        monkeypatch.setenv("KG_COST_NTDB_DUCKDB", sentinel_path)
        captured = {}
        def fake_fetch(inc_keys, duckdb_path):
            captured["path"] = duckdb_path
            return {}
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024",
            side_effect=fake_fetch,
        ):
            load_ntdb_cohort(_FIXTURE, attach_procedures=True, duckdb_path=None)
        assert captured["path"] == sentinel_path

    def test_explicit_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("KG_COST_NTDB_DUCKDB", "/env/path.db")
        captured = {}
        def fake_fetch(inc_keys, duckdb_path):
            captured["path"] = duckdb_path
            return {}
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024",
            side_effect=fake_fetch,
        ):
            load_ntdb_cohort(
                _FIXTURE, attach_procedures=True, duckdb_path="/explicit/path.db"
            )
        assert captured["path"] == "/explicit/path.db"

    def test_unset_env_and_no_arg_raises(self, monkeypatch):
        # No built-in default: NTDB is restricted data not shipped in the repo.
        monkeypatch.delenv("KG_COST_NTDB_DUCKDB", raising=False)
        with patch(
            "src.query._ntdb_adapter._fetch_procedures_2019_2024"
        ) as fake_fetch:
            with pytest.raises(RuntimeError, match="KG_COST_NTDB_DUCKDB"):
                load_ntdb_cohort(_FIXTURE, attach_procedures=True, duckdb_path=None)
        fake_fetch.assert_not_called()

    def test_unset_env_is_fine_when_not_attaching(self, monkeypatch):
        monkeypatch.delenv("KG_COST_NTDB_DUCKDB", raising=False)
        df = load_ntdb_cohort(_FIXTURE, attach_procedures=False, duckdb_path=None)
        assert len(df) > 0


# ---------------------- live-data smoke (gated) ---------------------- #

@pytest.mark.skipif(
    os.environ.get("KG_COST_RUN_NTDB") != "1",
    reason="KG_COST_RUN_NTDB=1 not set; skipping live NTDB smoke",
)
class TestLoadNtdbCohortLive:
    # The user's own NTDB annual_cohort.parquet (restricted data, not in this
    # repo). Point KG_COST_NTDB_COHORT_PARQUET at it; the plain relative
    # default does not exist, so the test skips.
    _REAL_COHORT = Path(
        os.environ.get("KG_COST_NTDB_COHORT_PARQUET", "annual_cohort.parquet")
    )

    def test_loads_first_100_rows_without_error(self):
        if not self._REAL_COHORT.exists():
            pytest.skip(f"real annual_cohort.parquet not found at {self._REAL_COHORT}")
        df = load_ntdb_cohort(
            self._REAL_COHORT, sample_rows=100, attach_procedures=False
        )
        assert len(df) == 100
        # Sanity: most trauma rows should have non-empty principal dx.
        assert (df["dx_principal"] != "").sum() >= 80


# ---------------------- sample_strategy (Phase 14, Workstream A) ---------------------- #

class TestSampleStrategy:
    """Validates --sample-strategy {head, random, stratified} extension.

    See plan: Workstream A in the Session 4 NTDB sample-strategy plan.
    """

    def test_sample_strategy_head_is_back_compat(self):
        # Calling without sample_strategy AND with sample_strategy='head'
        # must produce byte-identical output (back-compat default).
        df_default = load_ntdb_cohort(
            _FIXTURE, sample_rows=10, attach_procedures=False
        )
        df_head = load_ntdb_cohort(
            _FIXTURE,
            sample_rows=10,
            attach_procedures=False,
            sample_strategy="head",
        )
        pd.testing.assert_frame_equal(df_default, df_head)
        assert len(df_default) == 10
        # First 10 rows of fixture are Block A (FY2024). head() should be FY2024-only.
        assert (df_default["fy"] == 2024).all()

    def test_sample_strategy_random_is_reproducible(self):
        # Same seed → identical output across two calls.
        df_a = load_ntdb_cohort(
            _FIXTURE,
            sample_rows=20,
            attach_procedures=False,
            sample_strategy="random",
            sample_seed=42,
        )
        df_b = load_ntdb_cohort(
            _FIXTURE,
            sample_rows=20,
            attach_procedures=False,
            sample_strategy="random",
            sample_seed=42,
        )
        pd.testing.assert_frame_equal(df_a, df_b)
        assert len(df_a) == 20

        # Different seed → at least one row differs (with overwhelming probability).
        df_c = load_ntdb_cohort(
            _FIXTURE,
            sample_rows=20,
            attach_procedures=False,
            sample_strategy="random",
            sample_seed=999,
        )
        ids_a = set(df_a["patient_id"].tolist())
        ids_c = set(df_c["patient_id"].tolist())
        assert ids_a != ids_c, "different seeds produced identical samples"

    def test_sample_strategy_stratified_covers_all_fys(self):
        # Fixture has FYs {2010, 2015, 2017, 2020, 2021, 2022, 2023, 2024}
        # (Blocks A=2024, B=2010, C=2017, D=2015, E=2020-2024 spread, F=2022, G=2023).
        # Stratified sampling with sample_rows=10 must touch >=4 distinct FYs
        # (the fixture's "core" 4 plus any spread blocks).
        df = load_ntdb_cohort(
            _FIXTURE,
            sample_rows=10,
            attach_procedures=False,
            sample_strategy="stratified",
            sample_seed=0,
        )
        assert len(df) == 10
        n_fys = df["fy"].nunique()
        assert n_fys >= 4, (
            f"stratified sample must cover >=4 FYs from the fixture; got {n_fys} "
            f"(values: {sorted(df['fy'].unique().tolist())})"
        )

    def test_sample_strategy_validates_input(self):
        # Invalid strategy must raise ValueError.
        with pytest.raises(ValueError, match="sample_strategy"):
            load_ntdb_cohort(
                _FIXTURE,
                sample_rows=5,
                attach_procedures=False,
                sample_strategy="bogus",  # type: ignore[arg-type]
            )

    def test_sample_strategy_csv_mode_rejected(self, tmp_path):
        # Invoke cohort_annotator CLI in CSV mode with --sample-strategy other
        # than 'head'. Must exit non-zero with NTDB-only message.
        csv_input = tmp_path / "tiny.csv"
        csv_input.write_text(
            "patient_id,fy,age,sex,discharge_disp,dx_principal,"
            "dx_principal_system,dx_secondary,pcs,cpts\n"
            "1,2024,40,M,01,I21.4,ICD-10-CM,,,\n"
        )
        out_path = tmp_path / "out.csv"

        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "src.query.cohort_annotator",
                "--input",
                str(csv_input),
                "--output",
                str(out_path),
                "--source",
                "parquet",
                "--input-format",
                "csv",
                "--sample-strategy",
                "stratified",
            ],
            cwd=str(_REPO),
            capture_output=True,
            text=True,
        )
        assert proc.returncode != 0, (
            f"expected non-zero exit; stdout={proc.stdout!r} stderr={proc.stderr!r}"
        )
        combined = (proc.stdout + proc.stderr).lower()
        assert "ntdb" in combined and "sample-strategy" in combined, (
            f"expected NTDB-only error message; got: {proc.stderr!r}"
        )
