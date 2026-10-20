"""Single-code cost lookup API (Workstream A — additive front door).

Public Python entrypoint::

    from src.query import get_cost
    get_cost(code="27447", code_type="cpt", fy=2023)

CLI::

    python3 -m src.query.lookup --code 27447 --type cpt --fy 2023
    python3 -m src.query.lookup --code 470 --type drg --fy 2020
    python3 -m src.query.lookup --code S72.001A --type icd10cm --fy 2023
    python3 -m src.query.lookup --code 0SR9069 --type icd10pcs --fy 2023

All eight supported ``code_type`` values resolve against the existing
``_ParquetBackend`` from ``src.query.cohort_annotator`` and the underlying
gold parquets (``code_costs.parquet``, ``icd_principal_drg_modal.parquet``).
The engine is reused as-is — this module is a thin wrapper that flattens a
single (code, fy[, setting]) tuple into the same dict shape across all
code systems.

Output schema (always present; missing fields are ``None``)::

    {
        "code":        str,          # echoed input (post-normalization for ICD)
        "code_type":   str,          # echoed input (lowercased)
        "fy":          int,          # echoed input
        "setting":     str|None,     # resolved setting (inpatient/opps/pfs_*/asc/None)
        "mean":        float|None,   # allowed_payment mean
        "median":      float|None,   # NOT stored in code_costs.parquet -> None
        "p10":         float|None,
        "p90":         float|None,
        "n":           int|None,
        "source":      str|None,     # allowed_payment_source string
        "confidence":  str,          # high | approximate | none
        "notes":       list[str],    # provenance / fallback annotations
    }

Confidence semantics (per CLAUDE.md):
    * ``high``        — native FY2019+ ICD-10 grouper-or-modal path; direct DRG /
                        CPT lookup hit at the requested FY.
    * ``approximate`` — GEMs forward-bridge (ICD-9 era) or back-translation
                        (FY2017–FY2018 ICD-10 via modal-widened lex-min).
    * ``none``        — unresolved (out-of-era, no GEMs target, missing FY,
                        unknown code).

Out-of-coverage queries (unknown code, FY out of range, PCS without direct
cost mapping) return ``confidence='none'`` cleanly — no exceptions for missing
data. Exceptions are reserved for programmer errors (bad ``code_type`` enum,
missing required arg).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Any, Literal, Optional

# Reuse the existing parquet engine + canonical normalizers; do NOT modify
# cohort_annotator.py. Underscore-prefixed names are package-internal but
# fair game from inside ``src.query``.
from src.query.cohort_annotator import (
    _CODE_COSTS_PATH,
    _INPATIENT_MIN_FY,
    _MODAL_MAX_FY,
    _ParquetBackend,
    _lex_min_modal_drg,
    _normalize_icd10cm,
    _normalize_icd9,
)
from src.grouper.grouper import _normalize_code as _normalize_icd10pcs

CodeType = Literal[
    "cpt", "hcpcs", "drg", "apc", "icd10cm", "icd10pcs", "icd9cm", "icd9pcs"
]

_VALID_CODE_TYPES: tuple[str, ...] = (
    "cpt",
    "hcpcs",
    "drg",
    "apc",
    "icd10cm",
    "icd10pcs",
    "icd9cm",
    "icd9pcs",
)

# CPT/HCPCS settings in priority order — used when caller doesn't specify
# --setting and we need to pick a "primary" answer to return.
_CPT_SETTING_PRIORITY: tuple[str, ...] = (
    "pfs_nonfacility",
    "pfs_facility",
    "opps",
    "asc",
)

_APC_RATES_PATH = _CODE_COSTS_PATH.parent.parent / "silver" / "opps_apc_rates.parquet"


def _empty_result(
    code: str,
    code_type: str,
    fy: int,
    setting: Optional[str],
    notes: list[str],
) -> dict[str, Any]:
    """Build a confidence=none result skeleton (used for misses + errors)."""
    return {
        "code": code,
        "code_type": code_type,
        "fy": fy,
        "setting": setting,
        "mean": None,
        "median": None,
        "p10": None,
        "p90": None,
        "n": None,
        "source": None,
        "confidence": "none",
        "notes": list(notes),
    }


def _nan_to_none(v: Any) -> Optional[float]:
    """Coerce float / numpy float / NaN -> Python float or None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return f


def _resolve_cpt_or_hcpcs(
    backend: _ParquetBackend,
    code: str,
    fy: int,
    setting: Optional[str],
) -> dict[str, Any]:
    """Resolve a CPT/HCPCS code at one or more settings.

    When ``setting`` is provided, returns a single-setting result.
    When ``setting`` is None, iterates ``_CPT_SETTING_PRIORITY`` and returns
    the first hit, annotating which setting won. Misses on all settings
    return confidence='none'.
    """
    code_n = str(code).strip().upper()
    notes: list[str] = []

    if setting:
        # Caller pinned a setting — single lookup, single answer.
        record = backend._cost_index.get((code_n, fy, setting))
        if record is None:
            notes.append(f"no_record_at_setting:{setting}")
            return _empty_result(code_n, "cpt_or_hcpcs", fy, setting, notes)
        mean = _nan_to_none(record.get("allowed_payment"))
        if mean is None:
            notes.append("allowed_payment_nan")
            return _empty_result(code_n, "cpt_or_hcpcs", fy, setting, notes)
        return {
            "code": code_n,
            "code_type": "cpt_or_hcpcs",
            "fy": fy,
            "setting": setting,
            "mean": mean,
            "median": _nan_to_none(record.get("payment_p50"))
            if "payment_p50" in record
            else None,
            "p10": _nan_to_none(record.get("payment_p10")),
            "p90": _nan_to_none(record.get("payment_p90")),
            "n": int(record["allowed_payment_n"])
            if record.get("allowed_payment_n") is not None
            and not (
                isinstance(record["allowed_payment_n"], float)
                and math.isnan(record["allowed_payment_n"])
            )
            else None,
            "source": record.get("allowed_payment_source"),
            "confidence": "high",
            "notes": notes,
        }

    # No setting pinned — try priority list.
    for candidate in _CPT_SETTING_PRIORITY:
        record = backend._cost_index.get((code_n, fy, candidate))
        if record is None:
            continue
        mean = _nan_to_none(record.get("allowed_payment"))
        if mean is None:
            continue
        notes.append(f"setting_auto_selected:{candidate}")
        return {
            "code": code_n,
            "code_type": "cpt_or_hcpcs",
            "fy": fy,
            "setting": candidate,
            "mean": mean,
            "median": None,
            "p10": _nan_to_none(record.get("payment_p10")),
            "p90": _nan_to_none(record.get("payment_p90")),
            "n": int(record["allowed_payment_n"])
            if record.get("allowed_payment_n") is not None
            and not (
                isinstance(record["allowed_payment_n"], float)
                and math.isnan(record["allowed_payment_n"])
            )
            else None,
            "source": record.get("allowed_payment_source"),
            "confidence": "high",
            "notes": notes,
        }

    notes.append("no_record_in_any_setting")
    return _empty_result(code_n, "cpt_or_hcpcs", fy, None, notes)


def _resolve_drg(
    backend: _ParquetBackend, code: str, fy: int
) -> dict[str, Any]:
    """Resolve an MS-DRG cost row at the requested fiscal year.

    Note: ``_ParquetBackend.lookup_drg_cost`` sets ``resolved=True``
    unconditionally (intended for the cohort path where any DRG anchor is
    enough). For the single-code lookup contract we tighten that: a DRG with
    no cost row at the requested FY returns ``confidence='none'``.
    """
    code_n = str(code).strip()
    inpatient = backend.lookup_drg_cost(code_n, fy)
    mean = _nan_to_none(inpatient.allowed_mean)
    if mean is None:
        # No cost row (or all-NaN cost row) at this FY — out of coverage.
        return _empty_result(
            code_n,
            "drg",
            fy,
            "inpatient",
            ["drg_not_in_cost_index_at_fy"],
        )
    return {
        "code": code_n,
        "code_type": "drg",
        "fy": fy,
        "setting": "inpatient",
        "mean": mean,
        "median": None,
        "p10": _nan_to_none(inpatient.allowed_p10),
        "p90": _nan_to_none(inpatient.allowed_p90),
        "n": inpatient.allowed_n,
        "source": inpatient.allowed_source,
        "confidence": "high",
        "notes": [
            f"drg_title:{inpatient.drg_title}" if inpatient.drg_title else "no_title",
            f"charge_mean:{_nan_to_none(inpatient.charge_mean)}",
            f"ccr_cost:{_nan_to_none(inpatient.ccr_cost)}",
        ],
    }


def _resolve_apc(code: str, fy: int) -> dict[str, Any]:
    """Resolve an APC code from ``data/silver/opps_apc_rates.parquet``.

    APC cost is not exposed by ``_ParquetBackend`` (the cost graph keys OPPS by
    HCPCS/CPT, not APC). We read silver directly. Returns confidence='high'
    on hit, confidence='none' on miss.
    """
    if not _APC_RATES_PATH.exists():
        return _empty_result(
            str(code).strip(), "apc", fy, "opps", ["apc_rates_parquet_missing"]
        )
    import duckdb

    code_n = str(code).strip().zfill(4)  # APC codes are 4-digit zero-padded
    con = duckdb.connect(":memory:")
    try:
        # APC rates are quarterly; take the last quarter of the CY as a stable
        # year-end snapshot. Aggregate over quarters if multiple rows present.
        row = con.execute(
            f"""
            SELECT
                apc,
                AVG(payment_rate) AS mean_rate,
                MIN(payment_rate) AS min_rate,
                MAX(payment_rate) AS max_rate,
                COUNT(*) AS n_quarters,
                MAX(apc_title) AS apc_title
            FROM read_parquet('{_APC_RATES_PATH.as_posix()}')
            WHERE apc = ? AND cy = ?
            GROUP BY apc
            """,
            [code_n, fy],
        ).fetchone()
    finally:
        con.close()
    if row is None or row[1] is None:
        return _empty_result(
            code_n, "apc", fy, "opps", ["apc_not_in_silver_at_fy"]
        )
    mean = _nan_to_none(row[1])
    if mean is None:
        return _empty_result(
            code_n, "apc", fy, "opps", ["apc_payment_rate_nan"]
        )
    return {
        "code": code_n,
        "code_type": "apc",
        "fy": fy,
        "setting": "opps",
        "mean": mean,
        "median": None,
        "p10": _nan_to_none(row[2]),
        "p90": _nan_to_none(row[3]),
        "n": int(row[4]) if row[4] is not None else None,
        "source": "cms_opps_addendum_a",
        "confidence": "high",
        "notes": [f"apc_title:{row[5]}" if row[5] else "no_title"],
    }


def _resolve_icd10cm(
    backend: _ParquetBackend, code: str, fy: int
) -> dict[str, Any]:
    """ICD-10-CM principal -> modal MS-DRG -> DRG cost (two-step).

    Mirrors cohort_annotator Branch A (FY>=2019, native) and Branch B
    (FY2017–FY2018, back-translation via lex-min modal sweep). FY<2017
    returns confidence='none' (pre-ICD-10 era).
    """
    code_n = _normalize_icd10cm(code)
    notes: list[str] = []

    if fy < 2017:
        notes.append(f"fy{fy}_pre_icd10_era")
        return _empty_result(code_n, "icd10cm", fy, "inpatient", notes)

    if fy >= _INPATIENT_MIN_FY:
        # Branch A: native modal lookup at the requested FY.
        inpatient = backend.lookup_inpatient(code_n, fy)
        if not inpatient.resolved:
            notes.append("icd10cm_not_in_modal_at_fy")
            return _empty_result(code_n, "icd10cm", fy, "inpatient", notes)
        mean = _nan_to_none(inpatient.allowed_mean)
        if mean is None:
            notes.append(f"modal_drg_no_cost:drg={inpatient.drg},fy={fy}")
            return _empty_result(code_n, "icd10cm", fy, "inpatient", notes)
        return {
            "code": code_n,
            "code_type": "icd10cm",
            "fy": fy,
            "setting": "inpatient",
            "mean": mean,
            "median": None,
            "p10": _nan_to_none(inpatient.allowed_p10),
            "p90": _nan_to_none(inpatient.allowed_p90),
            "n": inpatient.allowed_n,
            "source": inpatient.allowed_source,
            "confidence": "high",
            "notes": [
                f"resolved_via_modal_drg:{inpatient.drg}",
                f"drg_title:{inpatient.drg_title}" if inpatient.drg_title else "no_title",
            ],
        }

    # Branch B: 2017 <= fy < 2019, back-translate via lex-min modal sweep
    # over [_INPATIENT_MIN_FY, _MODAL_MAX_FY]. Cost lookup at the native FY.
    drg, hit_fy = _lex_min_modal_drg(
        backend, code_n, _INPATIENT_MIN_FY, _MODAL_MAX_FY
    )
    if drg is None:
        notes.append("icd10_back_translation_no_modal")
        return _empty_result(code_n, "icd10cm", fy, "inpatient", notes)
    inpatient = backend.lookup_drg_cost(drg, fy)
    mean = _nan_to_none(inpatient.allowed_mean)
    if mean is None:
        notes.append(f"icd10_back_translation_no_cost:drg={drg},fy={fy}")
        return _empty_result(code_n, "icd10cm", fy, "inpatient", notes)
    return {
        "code": code_n,
        "code_type": "icd10cm",
        "fy": fy,
        "setting": "inpatient",
        "mean": mean,
        "median": None,
        "p10": _nan_to_none(inpatient.allowed_p10),
        "p90": _nan_to_none(inpatient.allowed_p90),
        "n": inpatient.allowed_n,
        "source": "gems_bridged_back_translated",
        "confidence": "approximate",
        "notes": [
            f"back_translated_modal_fy={hit_fy}",
            f"resolved_via_drg:{drg}",
        ],
    }


def _resolve_icd9cm(
    backend: _ParquetBackend, code: str, fy: int
) -> dict[str, Any]:
    """ICD-9-CM principal -> GEMs forward -> ICD-10-CM -> modal DRG -> cost.

    Mirrors cohort_annotator Branch D. Modal lookup at max(fy, 2019); cost
    lookup at max(fy, 2008). Confidence is always 'approximate' on hit.
    """
    from src.query.cohort_annotator import (
        _load_gems_overrides,
        _pick_best_gems_target,
    )

    code_n = _normalize_icd9(code)
    notes: list[str] = []
    targets = backend.lookup_gems_targets(code_n)
    if not targets:
        notes.append("gems_no_target")
        return _empty_result(code_n, "icd9cm", fy, "inpatient", notes)
    overrides = _load_gems_overrides()
    best = _pick_best_gems_target(targets, code_n, overrides)
    if best is None:
        notes.append("gems_no_map")
        return _empty_result(code_n, "icd9cm", fy, "inpatient", notes)
    icd10 = best["target_code"]
    notes.append(f"gems_resolved_tier:{best['tier']}")
    notes.append(f"gems_target:{icd10}")
    modal_fy = max(fy, _INPATIENT_MIN_FY)
    drg = backend.lookup_modal_drg(icd10, modal_fy)
    if drg is None:
        notes.append(f"gems_bridged_no_modal:icd10={icd10}")
        return _empty_result(code_n, "icd9cm", fy, "inpatient", notes)
    cost_fy = max(fy, 2008)
    inpatient = backend.lookup_drg_cost(drg, cost_fy)
    mean = _nan_to_none(inpatient.allowed_mean)
    if mean is None:
        notes.append(f"gems_bridged_no_cost:drg={drg},fy={cost_fy}")
        return _empty_result(code_n, "icd9cm", fy, "inpatient", notes)
    return {
        "code": code_n,
        "code_type": "icd9cm",
        "fy": fy,
        "setting": "inpatient",
        "mean": mean,
        "median": None,
        "p10": _nan_to_none(inpatient.allowed_p10),
        "p90": _nan_to_none(inpatient.allowed_p90),
        "n": inpatient.allowed_n,
        "source": "gems_bridged_modal",
        "confidence": "approximate",
        "notes": notes + [f"resolved_via_drg:{drg}", f"cost_fy:{cost_fy}"],
    }


def _resolve_icd10pcs(code: str, fy: int) -> dict[str, Any]:
    """ICD-10-PCS has no direct cost mapping in the gold layer.

    PCS codes only carry cost when used as an inpatient procedure on a
    grouper-bearing claim (cohort path). For a single-code lookup, return
    confidence='none' with a note explaining the gap. Caller should use the
    cohort_annotator with a constructed ClaimRecord instead.
    """
    code_n = _normalize_icd10pcs(code) if code else ""
    return _empty_result(
        code_n,
        "icd10pcs",
        fy,
        None,
        [
            "icd10pcs_no_direct_cost_mapping",
            "use_cohort_annotator_for_procedure_costing",
        ],
    )


def _resolve_icd9pcs(code: str, fy: int) -> dict[str, Any]:
    """ICD-9-PCS has no direct cost mapping (same rationale as icd10pcs)."""
    code_n = _normalize_icd9(code) if code else ""
    return _empty_result(
        code_n,
        "icd9pcs",
        fy,
        None,
        [
            "icd9pcs_no_direct_cost_mapping",
            "use_cohort_annotator_for_procedure_costing",
        ],
    )


def get_cost(
    code: str,
    code_type: CodeType,
    fy: int,
    setting: Optional[str] = None,
) -> dict[str, Any]:
    """Single-code cost lookup.

    See module docstring for the full output schema, confidence semantics, and
    per-code-type routing rules.

    Raises:
        ValueError: if ``code_type`` is not one of the eight supported enum
            values, or if ``code`` is empty/None.
    """
    if not code or not str(code).strip():
        raise ValueError("code must be a non-empty string")
    if not isinstance(fy, int):
        raise ValueError(f"fy must be int, got {type(fy).__name__}")
    code_type_n = str(code_type).strip().lower()
    if code_type_n not in _VALID_CODE_TYPES:
        raise ValueError(
            f"code_type must be one of {_VALID_CODE_TYPES}; got {code_type!r}"
        )

    # APC doesn't need the parquet backend (it reads silver directly), and PCS
    # types return immediately. Defer backend construction for those.
    if code_type_n == "apc":
        return _resolve_apc(code, fy)
    if code_type_n == "icd10pcs":
        return _resolve_icd10pcs(code, fy)
    if code_type_n == "icd9pcs":
        return _resolve_icd9pcs(code, fy)

    # Parquet backend handles everything else. If the gold parquets are
    # missing (fresh checkout, before pipeline runs), return a clean
    # confidence='none' instead of crashing — matches the "no exceptions
    # for missing data" contract.
    try:
        backend = _ParquetBackend()
    except FileNotFoundError as e:
        return _empty_result(
            str(code).strip(),
            code_type_n,
            fy,
            None,
            [f"backend_unavailable:{type(e).__name__}", str(e)],
        )

    if code_type_n in ("cpt", "hcpcs"):
        result = _resolve_cpt_or_hcpcs(backend, code, fy, setting)
        # Echo the canonical code_type back to caller (was set to the
        # combined "cpt_or_hcpcs" sentinel inside the resolver).
        result["code_type"] = code_type_n
        return result
    if code_type_n == "drg":
        return _resolve_drg(backend, code, fy)
    if code_type_n == "icd10cm":
        return _resolve_icd10cm(backend, code, fy)
    if code_type_n == "icd9cm":
        return _resolve_icd9cm(backend, code, fy)

    # Unreachable — enum check above is exhaustive.
    raise ValueError(f"unhandled code_type: {code_type_n}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3 -m src.query.lookup",
        description="Single-code cost lookup against the KG-COST gold parquets.",
    )
    p.add_argument("--code", required=True, help="Code value (e.g., 27447, 470, S72.001A)")
    p.add_argument(
        "--type",
        required=True,
        choices=_VALID_CODE_TYPES,
        dest="code_type",
        help="Code system",
    )
    p.add_argument("--fy", required=True, type=int, help="Fiscal year (or calendar year for CPT/HCPCS)")
    p.add_argument(
        "--setting",
        default=None,
        choices=("inpatient", "opps", "pfs_facility", "pfs_nonfacility", "asc"),
        help="Optional setting filter (CPT/HCPCS only; ignored for other types)",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = get_cost(
        code=args.code,
        code_type=args.code_type,
        fy=args.fy,
        setting=args.setting,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
