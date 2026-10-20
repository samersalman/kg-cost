"""NTDB cohort schema adapter for ``cohort_annotator``.

This module maps the NTDB ``annual_cohort.parquet`` schema onto the input
contract consumed by :func:`src.query.cohort_annotator.annotate_cohort`. The
public entry point is :func:`load_ntdb_cohort`, which returns a
``pandas.DataFrame`` with the columns the annotator expects (``patient_id``,
``fy``, ``age``, ``sex``, ``discharge_disp``, ``dx_principal``,
``dx_principal_system``, ``dx_secondary``, ``pcs``, ``cpts``).

Procedure attachment scope
--------------------------
``annual_cohort.parquet`` does NOT carry an ICD-procedure column. Procedures
live in separate sources, and only **FY2019-FY2024** procedure attachment
materially changes annotator routing under the current cohort_annotator
decision tree:

* **FY2019-FY2024 (ICD-10-PCS)**: attached here from the NTDB DuckDB table
  ``v_icd_procedure_2019_2024`` joined on ``inc_key``. These rows reach
  Branch A (native grouper) in ``cohort_annotator._annotate_row``, which
  consumes ``pcs`` to assign a surgical DRG via ``src.grouper.grouper.assign``.
* **FY2007-FY2011 (ICD-9-CM PUF procedures)**: NOT attached. ICD-9 inputs
  hit Branch D (GEMs forward bridge), which routes by diagnosis only and
  ignores ``pcs``. Attaching procedures yields no routing benefit.
* **FY2012-FY2016**: unrecoverable as relational. NTDB compiled CSVs for
  this era are denormalized with ~99% null procedure cells. (Out of scope.)
* **FY2017-FY2018 (ICD-10-PCS)**: NOT attached. These rows hit Branch B
  (back-translation through the FY2019 modal proxy), which routes by
  diagnosis only and ignores ``pcs``. Attaching procedures yields no
  routing benefit.

DuckDB path resolution
----------------------
The DuckDB connection target resolves in this order: explicit ``duckdb_path``
arg → ``KG_COST_NTDB_DUCKDB`` environment variable. There is no built-in
default: the NTDB is restricted data that is not included in this repository.
If procedures must be attached (``attach_procedures=True`` and at least one
FY2019-FY2024 row is present) and neither is set, ``load_ntdb_cohort`` raises
``RuntimeError``. A failed connection or query still raises a soft warning and
ships ``pcs=''`` for eligible rows; the cohort itself is always returned.
"""

from __future__ import annotations

import math
import os
import warnings
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

__all__ = ["load_ntdb_cohort"]

_PROC_FY_MIN = 2019
_PROC_FY_MAX = 2024

_HOSP_DISP_MAP: dict[str, str] = {
    "HOME": "01",
    "SELF": "01",
    "HHS": "06",
    "ICF": "04",
    "SNF": "03",
    "REHAB": "62",
    "IRF": "62",
    "LTAC": "63",
    "HOSP": "02",
    "TRANSFER": "02",
    "HOSPICE": "50",
    "AMA": "07",
    "D": "20",
    "DEAD": "20",
    "DEATH": "20",
    "EXPIRED": "20",
}


def _coerce_dx_system(icd_version) -> str:
    """Map NTDB ``ICD_VERSION`` (9.0 / 10.0 / NaN / str) to annotator enum.

    Returns ``'ICD-9-CM'`` for 9, ``'ICD-10-CM'`` for 10, and ``''`` for
    anything else (including NaN, None, and non-numeric strings).
    """
    if icd_version is None:
        return ""
    try:
        if pd.isna(icd_version):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        v = int(float(icd_version))
    except (TypeError, ValueError):
        return ""
    if v == 9:
        return "ICD-9-CM"
    if v == 10:
        return "ICD-10-CM"
    return ""


def _split_icd_codes(codes) -> tuple[str, str]:
    """Split an NTDB ``ICD_CODES`` list into ``(principal, secondary)``.

    The principal is the first non-empty code. The secondary is the
    remaining codes joined by ``;``. All codes are stripped and
    upper-cased; empty strings (after strip) are skipped. ``None`` and
    empty input both yield ``('', '')``.
    """
    if codes is None:
        return "", ""
    try:
        cleaned = [str(c).strip().upper() for c in codes if str(c).strip()]
    except TypeError:
        return "", ""
    if not cleaned:
        return "", ""
    head, *tail = cleaned
    return head, ";".join(tail)


def _fetch_procedures_2019_2024(
    inc_keys: list[int],
    duckdb_path: str | Path,
) -> dict[int, str]:
    """Return ``{inc_key: 'PCS1;PCS2;...'}`` from ``v_icd_procedure_2019_2024``.

    Opens a read-only DuckDB connection, queries by ``inc_key`` membership,
    groups procedure codes per admission, and always closes the connection
    on exit. Empty / whitespace procedure codes are skipped defensively.
    """
    if not inc_keys:
        return {}
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        df = con.execute(
            "SELECT inc_key, icd_procedure_code FROM v_icd_procedure_2019_2024 "
            "WHERE inc_key = ANY(?::BIGINT[])",
            [list(inc_keys)],
        ).df()
    finally:
        con.close()
    if df.empty:
        return {}
    cleaned = (
        df.assign(
            icd_procedure_code=df["icd_procedure_code"].astype(str).str.strip().str.upper()
        )
        .query("icd_procedure_code != ''")
    )
    if cleaned.empty:
        return {}
    return (
        cleaned.groupby("inc_key")["icd_procedure_code"]
        .apply(lambda s: ";".join(s))
        .to_dict()
    )


def load_ntdb_cohort(
    cohort_parquet: str | Path,
    *,
    sample_rows: int | None = None,
    attach_procedures: bool = True,
    duckdb_path: str | Path | None = None,
    sample_strategy: Literal["head", "random", "stratified"] = "head",
    sample_seed: int = 0,
) -> pd.DataFrame:
    """Read NTDB ``annual_cohort.parquet`` and emit annotator-shaped DataFrame.

    Parameters
    ----------
    cohort_parquet
        Path to NTDB ``annual_cohort.parquet`` (or any parquet with the same
        schema: ``INC_KEY``, ``YEAR``, ``AGE_YEARS``, ``SEX``, ``HOSP_DISP``,
        ``ICD_VERSION``, ``ICD_CODES``).
    sample_rows
        If set, take only ``sample_rows`` rows of the cohort. The selection
        method is governed by ``sample_strategy``.
    attach_procedures
        If True (default), attach ICD-10-PCS codes from
        ``v_icd_procedure_2019_2024`` for FY2019-FY2024 rows. Soft-fails to
        ``pcs=''`` with a warning if the DuckDB is unreachable.
    duckdb_path
        Path to the NTDB DuckDB. Falls back to the ``KG_COST_NTDB_DUCKDB`` env
        var. There is no built-in default; if neither is set and procedures
        must be attached, ``RuntimeError`` is raised (see Raises).
    sample_strategy
        One of ``'head'`` (default; back-compat first-N rows), ``'random'``
        (uniform random sample seeded by ``sample_seed``), or ``'stratified'``
        (per-FY balanced sample of approximately ``sample_rows / n_fys`` rows
        per ``YEAR`` group, then reshuffled and right-sized to ``sample_rows``
        using ``sample_seed`` for reproducibility). Ignored when
        ``sample_rows`` is None.
    sample_seed
        Random seed used by the ``random`` and ``stratified`` strategies.
        Defaults to ``0`` for reproducible smoke runs.

    Returns
    -------
    pd.DataFrame
        10 columns: ``patient_id``, ``fy``, ``age``, ``sex``, ``discharge_disp``,
        ``dx_principal_system``, ``dx_principal``, ``dx_secondary``, ``pcs``,
        ``cpts``. ``cpts`` is always ``''`` (NTDB has no CPT data).

    Raises
    ------
    RuntimeError
        If ``attach_procedures`` is True, the cohort has FY2019-FY2024 rows,
        and neither ``duckdb_path`` nor ``KG_COST_NTDB_DUCKDB`` is set.
    """
    if sample_strategy not in ("head", "random", "stratified"):
        raise ValueError(
            f"sample_strategy must be one of 'head', 'random', 'stratified'; "
            f"got {sample_strategy!r}"
        )

    duckdb_path = duckdb_path or os.environ.get("KG_COST_NTDB_DUCKDB")

    cohort = pd.read_parquet(cohort_parquet)
    if sample_rows is not None:
        if sample_strategy == "head":
            cohort = cohort.head(sample_rows)
        elif sample_strategy == "random":
            n = min(sample_rows, len(cohort))
            cohort = cohort.sample(n=n, random_state=sample_seed)
        else:  # stratified
            n_fys = max(cohort["YEAR"].nunique(), 1)
            ceil_per_fy = math.ceil(sample_rows / n_fys)
            picked = cohort.groupby("YEAR", group_keys=False).head(ceil_per_fy)
            picked = picked.sample(frac=1, random_state=sample_seed)
            cohort = picked.head(sample_rows)

    age = (
        pd.to_numeric(cohort["AGE_YEARS"], errors="coerce")
        .round()
        .clip(0, 120)
        .astype("Int64")
    )
    discharge_disp = (
        cohort["HOSP_DISP"]
        .fillna("")
        .astype(str)
        .str.upper()
        .map(_HOSP_DISP_MAP)
        .fillna("01")
    )

    out = pd.DataFrame(
        {
            "patient_id": cohort["INC_KEY"].astype("Int64").astype(str),
            "fy": pd.to_numeric(cohort["YEAR"], errors="coerce").astype("Int64"),
            "age": age,
            "sex": cohort["SEX"].fillna("").astype(str),
            "discharge_disp": discharge_disp,
            "dx_principal_system": cohort["ICD_VERSION"].apply(_coerce_dx_system),
        }
    )
    dx_split = cohort["ICD_CODES"].apply(_split_icd_codes)
    out["dx_principal"] = dx_split.str[0]
    out["dx_secondary"] = dx_split.str[1]
    out["pcs"] = ""
    out["cpts"] = ""

    if attach_procedures:
        eligible_mask = out["fy"].between(
            _PROC_FY_MIN, _PROC_FY_MAX, inclusive="both"
        )
        eligible_inc_keys = (
            cohort.loc[eligible_mask.values, "INC_KEY"]
            .dropna()
            .astype(int)
            .tolist()
        )
        if eligible_inc_keys:
            if not duckdb_path:
                raise RuntimeError(
                    "KG_COST_NTDB_DUCKDB is not set and no duckdb_path was "
                    "given. Attaching FY2019-FY2024 procedures reads an NTDB "
                    "DuckDB, which is restricted data and is not included in "
                    "this repository. Set the KG_COST_NTDB_DUCKDB environment "
                    "variable (or pass --ntdb-duckdb) to the path of your own "
                    "NTDB DuckDB, or disable procedure attachment "
                    "(attach_procedures=False in code, or "
                    "--no-attach-procedures with src.query.run_phase14c)."
                )
            try:
                proc_map = _fetch_procedures_2019_2024(
                    eligible_inc_keys, duckdb_path=duckdb_path
                )
            except Exception as e:
                warnings.warn(
                    f"NTDB procedure attachment failed ({e}); "
                    "shipping pcs='' for eligible rows."
                )
                proc_map = {}
            if proc_map:
                eligible_keys_series = cohort.loc[
                    eligible_mask.values, "INC_KEY"
                ].astype("Int64")
                mapped = eligible_keys_series.map(
                    lambda k: proc_map.get(int(k), "") if pd.notna(k) else ""
                )
                out.loc[eligible_mask.values, "pcs"] = mapped.values

    return out
