"""drgpy MS-DRG grouper wrapper. Phase 7b FY2019-FY2022 fallback.

Bridges the pure-Python drgpy library (v36..v40, supports ICD-10 only) to the
ClaimRecord/GrouperResult contract used by ``src/grouper/grouper.py``. CMS
never published a public Java grouper for V36..V39 (FY2019..FY2022), so drgpy
is the only free option for that window. Coverage:

  FY2019 -> drgpy v36
  FY2020 -> drgpy v37
  FY2021 -> drgpy v38
  FY2022 -> drgpy v39
  FY2023 -> drgpy v40 (also covered by Java V40.0 -- Java preferred)

Outside FY2019..FY2023 we raise GrouperError so the dispatcher in
``src/grouper/grouper.py`` can surface a clean message.

Engineering notes
-----------------
- Engines are heavy (~MB of dict state) and read-only; we cache one per
  drgpy version in a module-level dict for the life of the process.
- drgpy returns DRG only. We derive ``mdc`` from drgpy's ``drgmap``
  (Appendix A) and fall back to the ``_MDC*`` features when ``drgmap`` blanks
  the field (pre-MDC DRGs 001-018, 981-989). ``severity_tier`` is inferred
  from the ``_MCC`` / ``_CC`` feature presence. ``or_procedure_flag`` is
  ``_ORPCS_ANY`` in features. ``sdc_indicators`` is empty -- drgpy is not an
  MCE/SDC pipeline, so the Java-grouper-only sdc dict stays empty.
- Codes are upper-cased and decimal points stripped (parity with the
  existing ``_normalize_code`` in ``grouper.py``). drgpy itself returns
  '000' on unrecognized codes; we surface that as ms_drg='000' rather than
  raising, mirroring Java grouper behaviour for ungroupable claims.
- POA is unused: drgpy has no POA model (HACs require POA). For the Phase 7b
  enumeration that is fine; the locked permutation set defaults to all-Y POA
  and we don't enumerate HAC routing.
- Discharge disposition: drgpy only takes ``is_alive``; we map dispo='20'
  (expired) to ``is_alive=False`` and everything else to True, matching the
  pattern in ``tests/test_drgpy_cross.py``.
"""

from __future__ import annotations

import threading
from typing import Optional

from src.grouper.grouper import ClaimRecord, GrouperError, GrouperResult


# ---------------------------------------------------------------------------
# FY -> drgpy version mapping
# ---------------------------------------------------------------------------

# Modern-era cadence: one drgpy version per FY, V36 starting FY2019. Verified
# against drgpy 0.0.18 site-packages data dir on 2026-05-02:
#   site-packages/drgpy/data/{v36,v37,v38,v39,v40}/
# All five versions support the canonical sepsis+MCC=871, hip=470, AMI=282,
# newborn P0500=794 round-trip (see test_drgpy_wrapper.py).
_FY_TO_DRGPY_VERSION: dict[int, str] = {
    2019: "v36",
    2020: "v37",
    2021: "v38",
    2022: "v39",
    2023: "v40",  # also covered by Java V40.0 -- dispatcher prefers Java
}


# ---------------------------------------------------------------------------
# Engine cache
# ---------------------------------------------------------------------------

_ENGINE_CACHE: dict[str, object] = {}
_ENGINE_LOCK = threading.Lock()


def _get_engine(drgpy_version: str) -> object:
    """Lazy, thread-safe DRGEngine instantiation, one per drgpy version."""
    if drgpy_version in _ENGINE_CACHE:
        return _ENGINE_CACHE[drgpy_version]
    with _ENGINE_LOCK:
        if drgpy_version in _ENGINE_CACHE:
            return _ENGINE_CACHE[drgpy_version]
        try:
            from drgpy.msdrg import DRGEngine
        except ImportError as e:
            raise GrouperError(
                "drgpy not installed; cannot dispatch FY2019-FY2022 to drgpy "
                "fallback. Install with `uv pip install drgpy` or rerun with "
                f"FY in {{2023..2026}} (Java-grouper era). Inner: {e!r}"
            )
        try:
            engine = DRGEngine(version=drgpy_version)
        except Exception as e:  # noqa: BLE001 -- drgpy raises bare exceptions
            raise GrouperError(f"drgpy DRGEngine({drgpy_version!r}) failed: {e!r}")
        _ENGINE_CACHE[drgpy_version] = engine
        return engine


# ---------------------------------------------------------------------------
# Code normalization (parity with grouper._normalize_code)
# ---------------------------------------------------------------------------


def _normalize_code(code: str) -> str:
    """Upper-case, strip whitespace, strip decimal point. drgpy expects no dot."""
    return (code or "").strip().upper().replace(".", "")


# ---------------------------------------------------------------------------
# MDC + severity + OR-flag derivation
# ---------------------------------------------------------------------------


def _resolve_mdc(engine: object, ms_drg: str, features) -> str:
    """Return 2-digit zero-padded MDC for ``ms_drg``.

    Strategy: prefer drgpy's drgmap (Appendix A). When drgmap reports an
    empty mdc (pre-MDC DRGs 001-018, 981-989, allogeneic BMT 014, etc.), fall
    back to the FIRST ``_MDC*`` feature present in the feature counter --
    Phase 4 locked landmine #2 requires pre-MDC DRGs to report a non-"00"
    MDC, and the features capture the underlying anatomic MDC even when the
    Appendix A entry is blank (see heart-transplant 002 -> _MDC05).
    """
    drgmap = getattr(engine, "drgmap", {})
    entry = drgmap.get(ms_drg)
    if entry and entry.get("mdc"):
        mdc = str(entry["mdc"]).strip()
        if mdc:
            return mdc.zfill(2)
    # Fallback: scan features for _MDC** keys.
    for key in features:
        if isinstance(key, str) and key.startswith("_MDC") and len(key) >= 6:
            tail = key[4:6]
            if tail.isdigit():
                return tail.zfill(2)
    # Last resort -- we couldn't infer; surface "00" so downstream tests can
    # spot the gap. Phase 4 locked-contract test will fail if a pre-MDC DRG
    # reports "00" here, which is the correct loud-failure mode.
    return "00"


def _resolve_severity_tier(features) -> str:
    """Map drgpy's _MCC / _CC features to the locked severity_tier vocabulary."""
    if features.get("_MCC", 0) > 0:
        return "with_mcc"
    if features.get("_CC", 0) > 0:
        return "with_cc"
    return "without_cc_mcc"


def _resolve_or_flag(features) -> bool:
    return features.get("_ORPCS_ANY", 0) > 0


# ---------------------------------------------------------------------------
# Public API (mirrors src.grouper.grouper.assign signature)
# ---------------------------------------------------------------------------


def assign(
    record: ClaimRecord,
    fy: int,
    version: Optional[str] = None,
) -> GrouperResult:
    """Group a single claim via drgpy. Mirrors src.grouper.grouper.assign.

    Parameters
    ----------
    record:
        Same ``ClaimRecord`` accepted by the Java wrapper. POA, los,
        admit_date, age_days_* are all ignored (drgpy has no model for them).
    fy:
        Federal fiscal year (FY2019..FY2023). Unmapped FYs raise GrouperError.
    version:
        Optional drgpy version override (e.g. ``"v40"``). When None, resolved
        from ``_FY_TO_DRGPY_VERSION[fy]``.

    Raises
    ------
    GrouperError
        If FY out of drgpy coverage, drgpy not installed, version unknown,
        or pdx is empty after normalization.
    """
    if version is None:
        if fy not in _FY_TO_DRGPY_VERSION:
            raise GrouperError(
                f"FY {fy} not in drgpy coverage {sorted(_FY_TO_DRGPY_VERSION)}. "
                "drgpy supports v36..v40 (FY2019..FY2023). Pre-FY2019 cohorts "
                "have no grouper coverage; FY2024+ uses Java."
            )
        drgpy_version = _FY_TO_DRGPY_VERSION[fy]
    else:
        drgpy_version = version
    engine = _get_engine(drgpy_version)

    pdx = _normalize_code(record.pdx)
    if not pdx:
        raise GrouperError("ClaimRecord.pdx required (drgpy)")
    sdx = [_normalize_code(c) for c in (record.sdx or ()) if c and c.strip()]
    pcs = [_normalize_code(c) for c in (record.pcs or ()) if c and c.strip()]
    # drgpy doesn't enforce 7-char PCS; we filter to length-7 to mirror the
    # Java validation contract and avoid silently dropping malformed codes
    # without telling anyone. Out-of-shape codes will not appear in drgpy's
    # prmap anyway, so the result is equivalent.
    pcs = [p for p in pcs if len(p) == 7]
    dx_lst = [pdx, *sdx]

    sex = (record.sex or "U").strip().upper()
    gender = sex if sex in ("M", "F") else "F"  # drgpy default is "F"
    is_alive = (record.discharge_disp or "01").strip() != "20"

    try:
        # drgpy's get_features returns a Counter; reuse it for both DRG and
        # the MDC/severity/OR-flag derivation in one engine pass.
        features = engine.get_features(dx_lst, pcs, gender, is_alive)
        drg_list = engine.get_drg_all(dx_lst, pcs, gender, is_alive)
    except Exception as e:  # noqa: BLE001 -- drgpy raises bare exceptions
        raise GrouperError(f"drgpy {drgpy_version} failed for pdx={pdx}: {e!r}")

    if not drg_list:
        ms_drg = "000"
    else:
        ms_drg = str(drg_list[0]).strip().zfill(3)

    mdc = _resolve_mdc(engine, ms_drg, features)
    severity_tier = _resolve_severity_tier(features)
    or_flag = _resolve_or_flag(features)

    return GrouperResult(
        ms_drg=ms_drg,
        mdc=mdc,
        severity_tier=severity_tier,
        or_procedure_flag=or_flag,
        sdc_indicators={},  # drgpy is not an SDC/MCE pipeline
    )
