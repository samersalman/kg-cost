"""MS-DRG grouper subprocess wrapper. PHASES.md-locked API.

Phase 4 part 2 (Session 7): all FYs FY2008-FY2026 routed via
``GROUPER_VERSIONS_BY_FY[fy][0]`` (V*.0). Optional ``version`` override on
``assign()`` lets callers pin a specific version (e.g. V43.1 mid-year).

Strategy: ONE persistent Java subprocess, JVM held open across calls. Single
classpath spans EVERY vendored ``src/grouper/jars/V*_*/`` directory plus the
shared vendor JARs (gfc, protobuf, slf4j). Per-version ``MsdrgComponent`` is
constructed lazily by the driver on first use and cached for the life of the
JVM. Lazy javac compile of the driver. ``atexit`` cleanup.
"""

from __future__ import annotations

import atexit
import json
import os
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

from src import config
from src.config import grouper_jar_dir, grouper_vendor_dir, ROOT


_DRIVER_DIR = ROOT / "src" / "grouper" / "driver"
_DRIVER_SRC = _DRIVER_DIR / "KGCostGrouperDriver.java"
_DRIVER_CLASSES = _DRIVER_DIR / "_classes"
_DRIVER_CLASS = _DRIVER_CLASSES / "KGCostGrouperDriver.class"
_JARS_ROOT = ROOT / "src" / "grouper" / "jars"

_READY_TIMEOUT_SEC = 30.0
_REQUEST_TIMEOUT_SEC = 30.0
_SHUTDOWN_TIMEOUT_SEC = 5.0

# Shared vendor JARs (all 4 are mandatory across every grouper version).
_VENDOR_JARS: tuple[str, ...] = (
    "gfc-base-api-3.4.9.jar",
    "protobuf-java-3.25.5.jar",
    "slf4j-api-1.7.36.jar",
    "slf4j-nop-1.7.36.jar",
)

# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class ClaimRecord:
    """One inpatient claim ready for grouper input.

    All ICD codes upper-case, no decimal point, left-stripped of whitespace.
    PCS codes must be exactly 7 characters per ICD-10-PCS spec.
    ``poa`` aligns positionally with ``(pdx,) + sdx``; empty tuple = all-Y.
    """

    pdx: str
    sdx: tuple[str, ...]
    pcs: tuple[str, ...]
    sex: str  # "M" | "F" | "U"
    age: int
    discharge_disp: str  # CMS UB-04 2-digit code as string, e.g. "01", "20"
    los: int  # length of stay; passed through, not consumed by V43.1 API
    admit_date: Optional[date] = None  # FY routing helper
    age_days_admit: int = 0  # newborn-only, default 0
    age_days_discharge: int = 0  # newborn-only, default 0
    poa: tuple[str, ...] = ()  # per-dx POA aligned to (pdx,) + sdx; () = all-Y


@dataclass(frozen=True)
class GrouperResult:
    ms_drg: str  # 3-digit zero-padded, e.g. "001", "470", "871"
    mdc: str  # 2-digit zero-padded, e.g. "00" (Pre-MDC), "08", "18"
    severity_tier: str  # "with_mcc" | "with_cc" | "without_cc_mcc" | "none"
    or_procedure_flag: bool
    sdc_indicators: dict = field(default_factory=dict)


class GrouperError(RuntimeError):
    """Raised on any grouper failure: missing JARs, java unavailable, malformed code, driver error."""


# ----------------------------------------------------------------------------
# Module-level state (single persistent JVM subprocess)
# ----------------------------------------------------------------------------

_LOCK = threading.Lock()
_PROCESS: Optional[subprocess.Popen] = None
_JAVA_OK: Optional[bool] = None  # cached java -version probe result


def assign(
    record: ClaimRecord,
    fy: int,
    version: Optional[str] = None,
) -> GrouperResult:
    """Group a single claim. Lazily starts the JVM subprocess on first call.

    Dispatch:
      - FY in ``_DRGPY_FALLBACK_FYS`` (FY2019-FY2022) -> drgpy fallback.
        CMS never published a public Java grouper for V36-V39 (NTIS-paid /
        Windows-only / V38.1+V39 model-v1 driver-incompatible). Session 9.5
        FLAG 3 work package wires the pure-Python drgpy path here so the
        ICD->DRG enumeration in Phase 7b can cover those FYs.
      - FY in ``config.GROUPER_VERSIONS_BY_FY`` with non-empty versions
        (FY2023+) -> Java grouper subprocess.
      - All other FYs -> ``GrouperError`` via ``_resolve_version``.

    Pass ``version`` to override the default routing (e.g. ``"V43.1"`` to pin
    a mid-year Java refresh, or ``"v40"`` to force drgpy v40 for FY2023
    cross-checks). Raises ``GrouperError`` on any failure.
    """
    # drgpy fallback for FY2019-FY2022 (no Java grouper available; see
    # `project_phase4_scope_reduction.md` memory). Implemented as a kept-
    # single-public-entry-point dispatcher: the drgpy module imports lazily
    # so that pure-Java callers don't pay the drgpy import cost, and the
    # public ``assign`` signature/contract is the only thing callers see.
    if fy in _DRGPY_FALLBACK_FYS and version is None:
        from src.grouper import drgpy_wrapper
        return drgpy_wrapper.assign(record, fy, version=None)

    # Explicit drgpy override path (for FY2023+ cross-checks): caller passed
    # a lower-case "v36".."v40" string. We treat lower-case "v"-prefix as the
    # drgpy namespace; upper-case "V"-prefix as the Java namespace.
    if version is not None and version.startswith("v"):
        from src.grouper import drgpy_wrapper
        return drgpy_wrapper.assign(record, fy, version=version)

    resolved = _resolve_version(fy, version)
    req = _record_to_request(record)
    req["version"] = resolved
    resp = _send_request(req)
    if "error" in resp:
        raise GrouperError(f"grouper error: {resp['error']}")
    if "fatal" in resp:
        raise GrouperError(f"grouper fatal: {resp['fatal']}")
    return _response_to_result(resp)


# FYs routed to drgpy by default (no Java grouper available).
# FY2023 (V40) is intentionally NOT in this set: Java V40.0 is preferred when
# vendored. To force drgpy on FY2023, pass version="v40" explicitly.
_DRGPY_FALLBACK_FYS: frozenset[int] = frozenset({2019, 2020, 2021, 2022})


def _resolve_version(fy: int, version: Optional[str]) -> str:
    """Resolve fy + optional override to a vendored version string."""
    if fy not in config.GROUPER_VERSIONS_BY_FY:
        raise GrouperError(
            f"FY {fy} not in GROUPER_VERSIONS_BY_FY "
            f"(known: {sorted(config.GROUPER_VERSIONS_BY_FY)})"
        )
    versions = config.GROUPER_VERSIONS_BY_FY[fy]
    if not versions:
        raise GrouperError(
            f"FY {fy} has no Java MS-DRG grouper available. CMS only published "
            f"Java JARs starting V38.1 (May 2021); pre-V38.1 cohorts use the "
            f"drgpy fallback or formula-only Phase 7 path. See "
            f"`project_phase4_scope_reduction.md` memory."
        )
    if version is None:
        return versions[0]  # V*.0 routing per plan-v9.md A3
    if version not in versions:
        raise GrouperError(
            f"FY {fy} version {version!r} not in GROUPER_VERSIONS_BY_FY[{fy}] = {versions}"
        )
    return version


# ----------------------------------------------------------------------------
# Code normalization + request shaping
# ----------------------------------------------------------------------------


def _normalize_code(code: str) -> str:
    # ICD-10-CM/PCS codes must be dotless per the Java grouper contract
    # (V40+ rejects dotted input as invalid, returning DRG 999 "UNGROUPABLE").
    # The ClaimRecord docstring at line 60 already declares "no decimal
    # point"; this enforces it. NTDB delivers dotted codes; the modal index
    # and codes_master use dotless. See plans/Session 5.5 FLAG 1 root cause.md.
    return code.strip().upper().replace(".", "")


# CMS canonical POA value set: Y, N, U, W, 1, E.
_POA_VALID = frozenset({"Y", "N", "U", "W", "1", "E"})


def _normalize_poa(value: str) -> str:
    v = (value or "").strip().upper()
    if v not in _POA_VALID:
        raise GrouperError(f"invalid POA: {value}")
    return v


def _record_to_request(record: ClaimRecord) -> dict:
    pdx = _normalize_code(record.pdx)
    if not pdx:
        raise GrouperError("ClaimRecord.pdx required")
    sdx = [_normalize_code(c) for c in record.sdx if c and c.strip()]
    pcs = [_normalize_code(c) for c in record.pcs if c and c.strip()]
    for p in pcs:
        if len(p) != 7:
            raise GrouperError(f"ICD-10-PCS code must be 7 chars: got {p!r} (len={len(p)})")
    n_dx = 1 + len(sdx)
    if record.poa:
        if len(record.poa) != n_dx:
            raise GrouperError(
                f"poa length {len(record.poa)} does not match (pdx,)+sdx length {n_dx}"
            )
        poa_norm = [_normalize_poa(p) for p in record.poa]
    else:
        poa_norm = ["Y"] * n_dx
    diagnoses = [{"code": pdx, "poa": poa_norm[0]}]
    for i, code in enumerate(sdx, start=1):
        diagnoses.append({"code": code, "poa": poa_norm[i]})
    return {
        "diagnoses": diagnoses,
        "pcs": pcs,
        "sex": (record.sex or "U").strip().upper(),
        "age": int(record.age),
        "age_days_admit": int(record.age_days_admit),
        "age_days_discharge": int(record.age_days_discharge),
        "discharge_disp": (record.discharge_disp or "01").strip(),
    }


_SDC_KEYS = (
    "initial_drg",
    "initial_mdc",
    "initial_severity",
    "final_grc",
    "initial_grc",
    "hac_status",
    "num_hac_categories",
    "final_base_drg",
    "initial_base_drg",
)


def _response_to_result(resp: dict) -> GrouperResult:
    sdc = {k: resp.get(k) for k in _SDC_KEYS}
    return GrouperResult(
        ms_drg=str(resp.get("ms_drg", "000")),
        mdc=str(resp.get("mdc", "00")),
        severity_tier=str(resp.get("severity_tier", "none")),
        or_procedure_flag=bool(resp.get("or_procedure_flag", False)),
        sdc_indicators=sdc,
    )


# ----------------------------------------------------------------------------
# Subprocess lifecycle
# ----------------------------------------------------------------------------


def _check_java() -> None:
    global _JAVA_OK
    if _JAVA_OK is True:
        return
    try:
        r = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=10)
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        _JAVA_OK = False
        raise GrouperError(f"java not available: {e}. Need Java 17+ (`brew install --cask temurin@17`).")
    if r.returncode != 0:
        _JAVA_OK = False
        raise GrouperError(f"`java -version` failed (rc={r.returncode}): {r.stderr.strip() or r.stdout.strip()}")
    _JAVA_OK = True


def _vendored_version_dirs() -> list[Path]:
    """List src/grouper/jars/V*_* directories that contain at least one core JAR."""
    if not _JARS_ROOT.is_dir():
        return []
    out: list[Path] = []
    for d in sorted(_JARS_ROOT.iterdir()):
        if not d.is_dir() or not d.name.startswith("V"):
            continue
        # at least one msdrg-core-*.jar present (excluding sources)
        for jar in d.glob("msdrg-core-*.jar"):
            if "-sources" in jar.name:
                continue
            out.append(d)
            break
    return out


def _classpath() -> str:
    """Assemble classpath spanning every vendored grouper version + shared vendor JARs."""
    vendor_dir = grouper_vendor_dir()
    parts: list[Path] = []

    version_dirs = _vendored_version_dirs()
    if not version_dirs:
        raise GrouperError(
            "no grouper version directories found under src/grouper/jars/.\n"
            "Run: python3 -m src.extract.grouper --fy 2026 --version V43.1"
        )

    # Per-version JARs: skip *-sources.jar (compile-only artifacts).
    for vdir in version_dirs:
        for jar in sorted(vdir.glob("*.jar")):
            if "-sources" in jar.name:
                continue
            parts.append(jar)

    # Shared vendor JARs (mandatory).
    missing_vendor: list[str] = []
    for name in _VENDOR_JARS:
        p = vendor_dir / name
        if not p.exists():
            missing_vendor.append(str(p))
        else:
            parts.append(p)
    if missing_vendor:
        raise GrouperError(
            "Grouper vendor JARs missing:\n  "
            + "\n  ".join(missing_vendor)
            + "\nRun: python3 -m src.extract.grouper --fy 2026 --version V43.1"
        )

    return os.pathsep.join(str(p) for p in parts) + os.pathsep + str(_DRIVER_CLASSES)


def _ensure_compiled() -> None:
    """Lazy javac. Recompiles when source is newer than class file or class is missing."""
    if not _DRIVER_SRC.exists():
        raise GrouperError(f"Java driver source missing: {_DRIVER_SRC}")
    _DRIVER_CLASSES.mkdir(parents=True, exist_ok=True)
    if _DRIVER_CLASS.exists() and _DRIVER_CLASS.stat().st_mtime >= _DRIVER_SRC.stat().st_mtime:
        return
    r = subprocess.run(
        ["javac", "--release", "17", "-cp", _classpath(), "-d", str(_DRIVER_CLASSES), str(_DRIVER_SRC)],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        raise GrouperError(f"javac failed (rc={r.returncode}):\nSTDERR:\n{r.stderr}\nSTDOUT:\n{r.stdout}")


def _start_process() -> subprocess.Popen:
    _check_java()
    _ensure_compiled()
    cp = _classpath()
    proc = subprocess.Popen(
        ["java", "-cp", cp, "KGCostGrouperDriver"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    # Timeout-kill via watchdog.
    timer = threading.Timer(_READY_TIMEOUT_SEC, proc.kill)
    timer.daemon = True
    timer.start()
    pre: list[str] = []
    try:
        while True:
            line = proc.stdout.readline()  # type: ignore[union-attr]
            if not line:
                rc = proc.poll()
                err = (proc.stderr.read() if proc.stderr else "") or ""
                raise GrouperError(f"grouper exited before READY (rc={rc}). stderr:\n{err}")
            line = line.rstrip("\n")
            if line == "READY":
                return proc
            pre.append(line)
            if len(pre) > 50:
                raise GrouperError(f"too many lines before READY: {pre!r}")
    except Exception:
        try: proc.kill()
        except Exception: pass
        raise
    finally:
        timer.cancel()


def _ensure_started() -> subprocess.Popen:
    global _PROCESS
    if _PROCESS is not None and _PROCESS.poll() is None:
        return _PROCESS
    _PROCESS = _start_process()
    return _PROCESS


def _send_request(req: dict) -> dict:
    payload = json.dumps(req, separators=(",", ":")) + "\n"
    with _LOCK:
        proc = _ensure_started()
        try:
            proc.stdin.write(payload)  # type: ignore[union-attr]
            proc.stdin.flush()  # type: ignore[union-attr]
        except (BrokenPipeError, ValueError) as e:
            raise GrouperError(f"failed to write request: {e}")
        line = proc.stdout.readline()  # type: ignore[union-attr]
        if not line:
            rc = proc.poll()
            err = (proc.stderr.read() if proc.stderr else "") or ""
            raise GrouperError(f"grouper subprocess EOF (rc={rc}). stderr:\n{err}")
        line = line.strip()
        if not line:
            raise GrouperError("grouper returned empty response line")
        try:
            return json.loads(line)
        except json.JSONDecodeError as e:
            raise GrouperError(f"malformed grouper response: {e!r}: {line!r}")


def _shutdown() -> None:
    global _PROCESS
    proc, _PROCESS = _PROCESS, None
    if proc is None:
        return
    try:
        if proc.stdin is not None:
            try: proc.stdin.close()
            except Exception: pass
        try: proc.wait(timeout=_SHUTDOWN_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            proc.kill()
            try: proc.wait(timeout=2)
            except Exception: pass
    except Exception as e:
        sys.stderr.write(f"[grouper] shutdown warning: {e}\n")


atexit.register(_shutdown)


# ----------------------------------------------------------------------------
# CLI smoke (manual round-trip helper, NOT the test fixture)
# ----------------------------------------------------------------------------


def _main_smoke() -> int:
    r = ClaimRecord(
        pdx="A419", sdx=(), pcs=(), sex="M", age=70,
        discharge_disp="01", los=5,
    )
    res = assign(r, 2026, version="V43.1")
    print(f"smoke A419 sex=M age=70 -> ms_drg={res.ms_drg} mdc={res.mdc} "
          f"severity={res.severity_tier} or_flag={res.or_procedure_flag}")
    print(f"  sdc_indicators={res.sdc_indicators}")
    return 0


if __name__ == "__main__":
    sys.exit(_main_smoke())
