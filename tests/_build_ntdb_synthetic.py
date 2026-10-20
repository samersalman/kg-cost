"""One-shot script: builds tests/fixtures/ntdb_synthetic_100.parquet.

Re-run only when the schema changes. Output is committed; do not regenerate
casually.

Row composition (100 rows; INC_KEY = 1_000_000 + i, 0 <= i < 100):

- Block A (rows 0-29):    30 FY2024 ICD-10 admissions, multi-element ICD_CODES.
- Block B (rows 30-49):   20 FY2010 ICD-9 admissions, single-element ICD_CODES.
- Block C (rows 50-59):   10 FY2017 ICD-10 admissions (Branch B back-translation).
- Block D (rows 60-69):   10 FY2015 ICD-9 transition-year admissions.
- Block E (rows 70-79):   10 rows with empty ICD_CODES `[]`.
- Block F (rows 80-89):   10 rows with NaN ICD_VERSION (defensive).
- Block G (rows 90-99):   10 rows reserved for HOSP_DISP-variant coverage.

HOSP_DISP coverage: all 16 keys of `_HOSP_DISP_MAP` from
`src/query/_ntdb_adapter.py` are exercised across the 100 rows by overlaying
HOSP_DISP values onto Block-G rows AND a few rows from earlier blocks. Two
unknowns ("FOO" and "") are also planted to test the fallback path.

AGE_YEARS edges:
  - One value > 120 (row 0 = 130.7) tests upstream clipping.
  - One value < 0 (row 1 = -1.0) tests negative-age handling.
  - One NaN (row 2) tests missing-age handling.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


_REPO = Path(__file__).resolve().parent.parent
_OUTPUT = _REPO / "tests" / "fixtures" / "ntdb_synthetic_100.parquet"


# Realistic ICD-10 trauma/cardiac codes for Block A and C
_ICD10_PRINCIPALS = [
    "S72.001A", "S72.002A", "S72.011A", "S72.021A", "S72.044A",
    "I21.4",    "I21.3",    "I21.A1",   "I50.21",   "I50.23",
    "S06.0X0A", "S06.5X0A", "S06.6X0A", "S06.9X0A", "S02.0XXA",
    "S22.000A", "S22.32XA", "S22.41XA", "S32.000A", "S32.10XA",
    "S62.001A", "S82.001A", "S82.101A", "S82.201A", "S82.401A",
    "S92.001A", "S42.001A", "S42.201A", "S42.301A", "S42.401A",
]
_ICD10_SECONDARIES = [
    "I10",      "E11.9",    "Z79.4",    "F32.9",    "K21.9",
    "N18.3",    "J44.9",    "I25.10",   "M19.90",   "G47.33",
]

# Realistic ICD-9 codes for Block B and D
_ICD9_PRINCIPALS = [
    "812.20", "820.00", "820.21", "820.8",  "823.20",
    "410.71", "410.91", "428.0",  "428.21", "428.23",
    "850.0",  "850.5",  "851.00", "851.05", "852.00",
    "807.00", "807.05", "807.4",  "808.0",  "808.43",
]

# Block G + overlay HOSP_DISP coverage targets.
# 16 known keys + 2 unknowns ("FOO", "") = 18 distinct values to plant.
_HOSP_DISP_KEYS_TO_COVER = [
    "HOME", "SELF", "HHS", "ICF", "SNF",
    "REHAB", "IRF", "LTAC", "HOSP", "TRANSFER",
    "HOSPICE", "AMA", "D", "DEAD", "DEATH",
    "EXPIRED", "FOO", "",
]


def build() -> pd.DataFrame:
    rows: list[dict] = []
    inc_key_base = 1_000_000

    # ---------- Block A: 30 FY2024 ICD-10 multi-dx (rows 0-29) ----------
    for i in range(30):
        principal = _ICD10_PRINCIPALS[i % len(_ICD10_PRINCIPALS)]
        sec1 = _ICD10_SECONDARIES[i % len(_ICD10_SECONDARIES)]
        sec2 = _ICD10_SECONDARIES[(i + 3) % len(_ICD10_SECONDARIES)]
        codes = [principal, sec1, sec2]

        # AGE edges
        if i == 0:
            age = 130.7  # > 120 (clip target)
        elif i == 1:
            age = -1.0   # negative
        elif i == 2:
            age = float("nan")
        else:
            age = float(20 + i * 1.5)  # mix of decimal ages

        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2024,
            "AGE_YEARS": age,
            "SEX": ("M", "F", "U", "")[i % 4],
            "HOSP_DISP": "HOME",  # default; overlaid below for coverage
            "ICD_VERSION": 10.0,
            "ICD_CODES": codes,
        })

    # ---------- Block B: 20 FY2010 ICD-9 single-dx (rows 30-49) ----------
    for j in range(20):
        i = 30 + j
        principal = _ICD9_PRINCIPALS[j % len(_ICD9_PRINCIPALS)]
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2010,
            "AGE_YEARS": float(45 + j),
            "SEX": ("M", "F")[j % 2],
            "HOSP_DISP": "HOME",
            "ICD_VERSION": 9.0,
            "ICD_CODES": [principal],
        })

    # ---------- Block C: 10 FY2017 ICD-10 (rows 50-59) ----------
    for j in range(10):
        i = 50 + j
        principal = _ICD10_PRINCIPALS[(j + 5) % len(_ICD10_PRINCIPALS)]
        sec1 = _ICD10_SECONDARIES[(j + 1) % len(_ICD10_SECONDARIES)]
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2017,
            "AGE_YEARS": float(60 + j * 2),
            "SEX": ("M", "F")[j % 2],
            "HOSP_DISP": "HOME",
            "ICD_VERSION": 10.0,
            "ICD_CODES": [principal, sec1],
        })

    # ---------- Block D: 10 FY2015 ICD-9 transition-year (rows 60-69) ----------
    for j in range(10):
        i = 60 + j
        principal = _ICD9_PRINCIPALS[(j + 5) % len(_ICD9_PRINCIPALS)]
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2015,
            "AGE_YEARS": float(35 + j * 3),
            "SEX": ("M", "F")[j % 2],
            "HOSP_DISP": "HOME",
            "ICD_VERSION": 9.0,
            "ICD_CODES": [principal],
        })

    # ---------- Block E: 10 rows with empty ICD_CODES (rows 70-79) ----------
    for j in range(10):
        i = 70 + j
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2020 + (j % 5),  # spread across FYs
            "AGE_YEARS": float(50 + j),
            "SEX": ("M", "F", "U")[j % 3],
            "HOSP_DISP": "HOME",
            "ICD_VERSION": (10.0 if j % 2 == 0 else 9.0),
            "ICD_CODES": [],  # explicit empty list
        })

    # ---------- Block F: 10 rows with NaN ICD_VERSION (rows 80-89) ----------
    for j in range(10):
        i = 80 + j
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2022,
            "AGE_YEARS": float(40 + j),
            "SEX": ("M", "F")[j % 2],
            "HOSP_DISP": "HOME",
            "ICD_VERSION": float("nan"),
            # Non-empty so we test the dx_system="" defaulting path
            "ICD_CODES": [_ICD10_PRINCIPALS[j % len(_ICD10_PRINCIPALS)]],
        })

    # ---------- Block G: 10 rows reserved for HOSP_DISP variants (rows 90-99) ----------
    # We need 18 distinct HOSP_DISP values total (16 _HOSP_DISP_MAP keys + "FOO" + "").
    # Block G holds 10; the remaining 8 are overlaid onto early Block-A rows below.
    block_g_dispositions = _HOSP_DISP_KEYS_TO_COVER[:10]  # first 10
    for j in range(10):
        i = 90 + j
        rows.append({
            "INC_KEY": inc_key_base + i,
            "YEAR": 2023,
            "AGE_YEARS": float(70 + j),
            "SEX": ("M", "F")[j % 2],
            "HOSP_DISP": block_g_dispositions[j],
            "ICD_VERSION": 10.0,
            "ICD_CODES": [_ICD10_PRINCIPALS[j % len(_ICD10_PRINCIPALS)]],
        })

    # Overlay remaining HOSP_DISP values (indices 10-17 of _HOSP_DISP_KEYS_TO_COVER)
    # onto Block-A rows 3-10 to ensure full coverage without exceeding 100 rows.
    overlay_dispositions = _HOSP_DISP_KEYS_TO_COVER[10:]  # last 8
    for offset, disp in enumerate(overlay_dispositions):
        target_idx = 3 + offset  # rows 3..10 in Block A
        rows[target_idx]["HOSP_DISP"] = disp

    return pd.DataFrame(rows)


def main() -> None:
    df = build()
    assert len(df) == 100, f"expected 100 rows, got {len(df)}"

    # Verify HOSP_DISP coverage before writing
    present = set(df["HOSP_DISP"].astype(str).tolist())
    missing = [k for k in _HOSP_DISP_KEYS_TO_COVER if k not in present]
    assert not missing, f"missing HOSP_DISP coverage: {missing}"

    # Verify INC_KEY uniqueness
    assert df["INC_KEY"].nunique() == 100, "INC_KEY collisions"

    # Cast ICD_CODES to pyarrow list<string> via explicit schema
    schema = pa.schema([
        pa.field("INC_KEY", pa.int64()),
        pa.field("YEAR", pa.int64()),
        pa.field("AGE_YEARS", pa.float64()),
        pa.field("SEX", pa.string()),
        pa.field("HOSP_DISP", pa.string()),
        pa.field("ICD_VERSION", pa.float64()),
        pa.field("ICD_CODES", pa.list_(pa.string())),
    ])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, _OUTPUT)
    print(f"wrote {_OUTPUT} ({len(df)} rows)")


if __name__ == "__main__":
    main()
