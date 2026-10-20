"""MS-DRG Java grouper bronze pull + JAR working-tree positioning.

Phase 4 Step 2 (Session 6). NOT a parser -- downloads CMS artifacts + 3
external runtime deps, demuxes the inner standalone-JARs ZIP, copies vendored
JARs into the working classpath dir, and (Maven permitting) builds
gfc-base-api from source. No silver/parquet output.

Bronze layout:
    data/bronze/grouper/fy{fy}_{vshort}/{download_date}/
        grouper_jar_{vshort}.zip          (cms.gov standalone JARs ZIP)
        grouper_definitions_{vshort}.zip  (cms.gov Definitions Manual ZIP)
        gfc_source_3_4_9.zip              (GitHub source ZIP)
        protobuf-java-3.25.5.jar          (Maven Central)
        slf4j-api-1.7.36.jar              (Maven Central)

Working tree (NOT bronze):
    src/grouper/jars/V{maj}_{min}/    (6 JARs + 2 PDFs from inner ZIP)
    src/grouper/vendor/               (3 runtime dep JARs)

Inner ZIP demux: cms.gov outer ZIP wraps an inner
``MSDRG.V*.R0.Java.Standalone.Jars.P.zip``. V43.1 wraps in a subdirectory;
V43.0 has the inner ZIPs at the root. Located by case-insensitive
``MSDRG.V*`` substring predicate.

CLI: ``python -m src.extract.grouper --fy 2026 --version V43.1 [--strict]``.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import requests

from src import config
from src.extract import _http

# V43.1 is the only cell with a hand-verified explicit JAR-member list.
# All other V40+ cells use ``_expected_members_default`` -> empty tuple, so
# the post-extraction ``_find_core_jar`` probe is the canonical invariant.
# (V43.0 was originally listed here under the same `msdrg-core-43.0.0.0.jar`
# pattern as V43.1, but the actual V43.0 ZIP ships
# `msdrg-v430-43.0.0.2.jar` -- the rename to `msdrg-core-*` started at
# V43.1, not V43.0. Per Session 7 empirical verification.)
_EXPECTED_JAR_MEMBERS: dict[tuple[int, str], tuple[str, ...]] = {
    (2026, "V43.1"): (
        "msdrg-binary-access-1.5.0.jar",
        "msdrg-binary-access-1.5.0-sources.jar",
        "msdrg-core-43.1.0.0.jar",
        "msdrg-core-43.1.0.0-sources.jar",
        "msdrg-model-v2-2.11.0.jar",
        "msdrg-model-v2-2.11.0-sources.jar",
        "pbl161_msdrg_java_api.pdf",
        "OpenSourceLicenseDisclosures.pdf",
    ),
}


def _expected_core_jar(version: str) -> str:
    """V43.1 -> msdrg-core-43.1.0.0.jar; V43.0 -> msdrg-core-43.0.0.0.jar.

    The "canonical" V43+ name is the V*.0/V*.1 hint used for idempotency
    checks. For V40-V42 the name varies per minor release (e.g. V41.0 ships
    `msdrg-v410-41.0.1.0.jar` per Section 7.3 of grouper-url-discovery.md);
    use ``_find_core_jar`` after extraction to actually locate the core.
    """
    bare = version.lstrip("V").lstrip("v")
    return f"msdrg-core-{bare}.0.0.jar"


def _find_core_jar(jar_dir: Path, version: str) -> Path | None:
    """Locate the MS-DRG core JAR in `jar_dir`, tolerating per-version naming
    drift across V40-V43. Returns None if no candidate matches.

    Per Section 7.3:
      - V43.0: msdrg-core-43.0.0.0.jar
      - V42.0: msdrg-v420-42.0.3.3.jar
      - V41.0: msdrg-v410-41.0.1.0.jar
      - V40.0: msdrg-40.0.0.3.jar (no `-core-`, no `v{maj}{min}-` prefix)
    """
    bare = version.lstrip("V").lstrip("v")
    try:
        maj, minor = bare.split(".", 1)
    except ValueError:
        return None
    excluded = ("-sources.jar", "-javadoc.jar")
    sub_excludes = ("model", "binary-access", "proto-generator")
    candidates: list[Path] = []
    for jar in jar_dir.glob("*.jar"):
        if any(jar.name.endswith(suf) for suf in excluded):
            continue
        name_lower = jar.name.lower()
        if any(s in name_lower for s in sub_excludes):
            continue
        # Core JAR carries the major version in the filename (e.g. "-43." or
        # "v43" or "-42." or "v42"). Filter to candidates with the major.
        maj_token = f"-{maj}."
        v_token = f"v{maj}{minor}"
        v_token_majonly = f"v{maj}-"
        if (
            maj_token in jar.name
            or v_token in name_lower
            or v_token_majonly in name_lower
        ):
            candidates.append(jar)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Tie-break: prefer the deepest version match (e.g. `msdrg-v410-41.0.1.0.jar`
    # over `binary-access-41.x.jar` if any) -- by score on token presence.
    def _score(p: Path) -> tuple[int, int]:
        n = p.name.lower()
        return (int(f"v{maj}{minor}" in n), len(p.name))
    return max(candidates, key=_score)


def _expected_members_default(version: str) -> tuple[str, ...]:
    """Fallback when ``_EXPECTED_JAR_MEMBERS`` has no explicit entry.

    For V40+ the model/binary-access JARs have version-specific filenames that
    Session 7's URL-discovery audit captured (Section 7.3) but did not enforce
    as build-time invariants. Returning an empty tuple here delegates to the
    `_find_core_jar` post-extraction probe -- which is the canonical
    invariant after Session 7's per-version naming drift was confirmed.
    """
    return ()


def _synthesized_basenames(version: str) -> dict[str, str]:
    """artifact-key -> bronze basename. Self-describing across re-pulls."""
    vshort = version.replace("V", "v").replace(".", "_").lower()
    return {
        "java_jar":           f"grouper_jar_{vshort}.zip",
        "definitions_manual": f"grouper_definitions_{vshort}.zip",
        "gfc_source":         "gfc_source_3_4_9.zip",
        "protobuf_jar":       "protobuf-java-3.25.5.jar",
        "slf4j_jar":          "slf4j-api-1.7.36.jar",
        "slf4j_nop_jar":      "slf4j-nop-1.7.36.jar",
    }


def _verify_java_available() -> None:
    try:
        proc = subprocess.run(
            ["java", "-version"], capture_output=True, text=True,
            timeout=10, check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "java not on PATH. Install Java 17+ "
            "(`brew install --cask temurin@17`) and retry."
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"`java -version` exit {proc.returncode}: {proc.stderr.strip()}"
        )
    print(f"[grouper] {proc.stderr.strip().splitlines()[0]}", file=sys.stderr)


def _check_mvn_available() -> bool:
    try:
        proc = subprocess.run(
            ["mvn", "-version"], capture_output=True, text=True,
            timeout=10, check=False,
        )
    except FileNotFoundError:
        print(
            "[grouper] mvn not on PATH; using stdlib javac fallback for GFC.",
            file=sys.stderr,
        )
        return False
    if proc.returncode != 0:
        print(
            f"[grouper] mvn -version exit {proc.returncode}; using javac",
            file=sys.stderr,
        )
        return False
    print(
        f"[grouper] {proc.stdout.strip().splitlines()[0]}", file=sys.stderr,
    )
    return True


def _find_inner_msdrg_zip(zf: zipfile.ZipFile) -> str:
    """Locate inner MSDRG.V*.zip by case-insensitive substring predicate."""
    candidates = [
        n for n in zf.namelist()
        if "msdrg.v" in n.lower()
        and n.lower().endswith(".zip")
        and not n.endswith("/")
    ]
    if not candidates:
        raise RuntimeError(
            f"no inner MSDRG.V*.zip found (members: {zf.namelist()[:8]}...)"
        )
    if len(candidates) > 1:
        msdrg = [
            c for c in candidates
            if Path(c).name.lower().startswith("msdrg.")
        ]
        if not msdrg:
            raise RuntimeError(f"multiple inner ZIPs, none MSDRG.: {candidates}")
        return msdrg[0]
    return candidates[0]


def _extract_inner_jars(
    outer_zip: Path, jar_dir: Path, *, fy: int, version: str
) -> None:
    """Stream-extract inner MSDRG ZIP into `jar_dir`. Idempotent."""
    jar_dir.mkdir(parents=True, exist_ok=True)
    existing_core = _find_core_jar(jar_dir, version)
    if existing_core is not None:
        print(
            f"[grouper] {existing_core.name} already present in {jar_dir.name}/ -- skip demux",
            file=sys.stderr,
        )
        return

    with zipfile.ZipFile(outer_zip) as outer:
        inner_member = _find_inner_msdrg_zip(outer)
        print(f"[grouper] inner ZIP: {inner_member}", file=sys.stderr)
        with tempfile.NamedTemporaryFile(
            suffix=".zip", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
            with outer.open(inner_member) as src:
                shutil.copyfileobj(src, tmp)
        try:
            with zipfile.ZipFile(tmp_path) as inner:
                for name in inner.namelist():
                    if name.endswith("/"):
                        continue
                    basename = Path(name).name
                    if not basename:
                        continue
                    dest = jar_dir / basename
                    with inner.open(name) as src, dest.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
        finally:
            tmp_path.unlink(missing_ok=True)


def _verify_jar_members(
    jar_dir: Path, *, fy: int, version: str, strict: bool
) -> None:
    core_path = _find_core_jar(jar_dir, version)
    if core_path is None:
        raise RuntimeError(
            f"no msdrg-core-* JAR matching version {version} in {jar_dir} -- "
            f"inner-ZIP demux failed (expected per-version naming patterns "
            f"per Section 7.3 of grouper-url-discovery.md)."
        )
    expected = _EXPECTED_JAR_MEMBERS.get(
        (fy, version), _expected_members_default(version),
    )
    missing = [m for m in expected if not (jar_dir / m).exists()]
    if missing:
        line = (
            f"[grouper] WARNING: missing expected members in "
            f"{jar_dir.name}/: {missing}"
        )
        if strict:
            raise RuntimeError(line)
        print(line, file=sys.stderr)


def _vendor_runtime_jars(
    *, bronze_dir: Path, vendor_dir: Path, basenames: dict[str, str]
) -> None:
    vendor_dir.mkdir(parents=True, exist_ok=True)
    for key in ("protobuf_jar", "slf4j_jar", "slf4j_nop_jar"):
        src = bronze_dir / basenames[key]
        dst = vendor_dir / src.name
        shutil.copy2(src, dst)
        print(
            f"[grouper] vendored {dst.name} ({dst.stat().st_size} B)",
            file=sys.stderr,
        )


def _unzip_gfc_source(gfc_zip: Path, work_root: Path) -> Path | None:
    """Extract GFC source ZIP into work_root; return repo root or None."""
    work_root.mkdir(parents=True, exist_ok=True)
    extract_root = work_root / "gfc_source_extract"
    if extract_root.exists():
        shutil.rmtree(extract_root)
    extract_root.mkdir(parents=True)
    with zipfile.ZipFile(gfc_zip) as zf:
        zf.extractall(extract_root)
    children = [c for c in extract_root.iterdir() if c.is_dir()]
    if not children:
        print("[grouper] WARNING: GFC source ZIP had no top-level dir",
              file=sys.stderr)
        return None
    return children[0]


def _build_gfc_with_mvn(gfc_root: Path, target_jar: Path) -> bool:
    """`mvn -DskipTests package` then copy the built JAR. Returns True on success."""
    print(f"[grouper] running `mvn -DskipTests package` in {gfc_root}",
          file=sys.stderr)
    try:
        proc = subprocess.run(
            ["mvn", "-DskipTests", "package"],
            cwd=gfc_root, capture_output=True, text=True,
            timeout=300, check=False,
        )
    except subprocess.TimeoutExpired:
        print("[grouper] WARNING: `mvn package` timed out (300s)",
              file=sys.stderr)
        return False
    if proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-50:])
        print(
            f"[grouper] WARNING: mvn exit {proc.returncode}; tail:\n{tail}",
            file=sys.stderr,
        )
        return False
    candidates = [
        c for c in gfc_root.rglob("gfc-base-api-3.4.9.jar")
        if "target" in c.parts
        and not c.name.endswith("-sources.jar")
        and not c.name.endswith("-javadoc.jar")
        and "original-" not in c.name
    ]
    if not candidates:
        print("[grouper] WARNING: mvn ok but no gfc-base-api-3.4.9.jar found",
              file=sys.stderr)
        return False
    shutil.copy2(candidates[0], target_jar)
    return True


def _build_gfc_with_javac(gfc_root: Path, target_jar: Path) -> bool:
    """Build gfc-base-api JAR via stdlib `javac` + `jar` (Maven-free fallback).

    GFC is a single-module project with all sources under `src/main/java`. JDK
    17 already ships javac/jar so no extra tooling is needed beyond what the
    Java runtime check at extractor entry has already verified. Tests are
    skipped (mirrors `mvn -DskipTests`).
    """
    src_root = gfc_root / "src" / "main" / "java"
    if not src_root.is_dir():
        print(f"[grouper] WARNING: GFC source layout unexpected at {src_root}",
              file=sys.stderr)
        return False
    sources = [str(p) for p in src_root.rglob("*.java")]
    if not sources:
        print("[grouper] WARNING: no GFC .java sources found",
              file=sys.stderr)
        return False
    classes_dir = gfc_root / "_classes"
    if classes_dir.exists():
        shutil.rmtree(classes_dir)
    classes_dir.mkdir(parents=True)
    print(f"[grouper] javac fallback: compiling {len(sources)} GFC sources",
          file=sys.stderr)
    proc = subprocess.run(
        ["javac", "-d", str(classes_dir), "--release", "17", *sources],
        capture_output=True, text=True, timeout=120, check=False,
    )
    if proc.returncode != 0:
        tail = "\n".join((proc.stderr or proc.stdout).splitlines()[-30:])
        print(f"[grouper] WARNING: javac exit {proc.returncode}; tail:\n{tail}",
              file=sys.stderr)
        return False
    proc = subprocess.run(
        ["jar", "cf", str(target_jar), "-C", str(classes_dir), "."],
        capture_output=True, text=True, timeout=60, check=False,
    )
    if proc.returncode != 0:
        print(f"[grouper] WARNING: jar exit {proc.returncode}: {proc.stderr}",
              file=sys.stderr)
        return False
    return True


def _build_gfc_base_api(
    *, gfc_zip: Path, vendor_dir: Path, work_root: Path, strict: bool = False,
) -> Path | None:
    """Vendor gfc-base-api-3.4.9.jar. Tries Maven first, then stdlib javac.

    Idempotent: returns the existing JAR if already vendored. The javac path
    is the canonical hermetic build for machines without Maven; Maven is
    preferred only when it's already on PATH (faster + matches upstream).

    Under ``strict=True``, raises ``RuntimeError`` if BOTH Maven AND javac
    fallback fail (rather than emitting a stderr warning and returning None).
    """
    target_jar = vendor_dir / "gfc-base-api-3.4.9.jar"
    if target_jar.exists() and target_jar.stat().st_size > 0:
        print(
            f"[grouper] {target_jar.name} already vendored -- skip build",
            file=sys.stderr,
        )
        return target_jar

    gfc_root = _unzip_gfc_source(gfc_zip, work_root)
    if gfc_root is None:
        msg = (
            "GFC source ZIP had no top-level dir; cannot build "
            "gfc-base-api-3.4.9.jar"
        )
        if strict:
            raise RuntimeError(f"GFC build failed under --strict: {msg}")
        return None

    if _check_mvn_available() and _build_gfc_with_mvn(gfc_root, target_jar):
        print(f"[grouper] built + vendored {target_jar.name} via mvn",
              file=sys.stderr)
        return target_jar
    if _build_gfc_with_javac(gfc_root, target_jar):
        print(f"[grouper] built + vendored {target_jar.name} via javac",
              file=sys.stderr)
        return target_jar
    msg = (
        "failed to build gfc-base-api-3.4.9.jar via either mvn or javac; "
        "Step 3 wrapper cannot run"
    )
    if strict:
        raise RuntimeError(f"GFC build failed under --strict: {msg}")
    print(f"[grouper] WARNING: {msg}", file=sys.stderr)
    return None


def extract_fy_version(
    *, fy: int, version: str, download_date: str | None, strict: bool,
) -> None:
    """Pull bronze, demux inner JAR ZIP, vendor runtime JARs, build GFC."""
    key = (fy, version)
    if key not in config.GROUPER_SOURCES:
        raise KeyError(
            f"grouper fy{fy} {version!r}: not in GROUPER_SOURCES "
            f"(known: {list(config.GROUPER_SOURCES.keys())})"
        )

    _verify_java_available()

    bronze = config.grouper_bronze_dir(fy, version, download_date)
    bronze.mkdir(parents=True, exist_ok=True)
    basenames = _synthesized_basenames(version)
    sources = config.GROUPER_SOURCES[key]
    deps = config.GROUPER_RUNTIME_DEPS
    session = _http.make_session()

    java_jar_path = bronze / basenames["java_jar"]
    _http.download(sources["java_jar"], java_jar_path, session=session)
    _http.download(
        sources["definitions_manual"],
        bronze / basenames["definitions_manual"], session=session,
    )

    gfc_zip_path = bronze / basenames["gfc_source"]
    _http.download(deps["gfc_source"], gfc_zip_path, session=session)
    _http.download(
        deps["protobuf_jar"], bronze / basenames["protobuf_jar"],
        session=session,
    )
    _http.download(
        deps["slf4j_jar"], bronze / basenames["slf4j_jar"], session=session,
    )
    _http.download(
        deps["slf4j_nop_jar"], bronze / basenames["slf4j_nop_jar"],
        session=session,
    )

    jar_dir = config.grouper_jar_dir(fy, version)
    _extract_inner_jars(java_jar_path, jar_dir, fy=fy, version=version)
    _verify_jar_members(jar_dir, fy=fy, version=version, strict=strict)

    vendor_dir = config.grouper_vendor_dir()
    _vendor_runtime_jars(
        bronze_dir=bronze, vendor_dir=vendor_dir, basenames=basenames,
    )

    with tempfile.TemporaryDirectory(prefix="kgcost_gfc_build_") as tmp:
        _build_gfc_base_api(
            gfc_zip=gfc_zip_path, vendor_dir=vendor_dir,
            work_root=Path(tmp), strict=strict,
        )

    print(
        f"[grouper] fy{fy} {version}: bronze + jar tree positioned",
        file=sys.stderr,
    )


def _resolve_targets(args: argparse.Namespace) -> list[tuple[int, str]]:
    """Translate CLI flags into a list of (fy, version) keys to extract.

    Resolution order:
      --all                        -> every key in GROUPER_SOURCES
      --fy-range LO-HI [+ --version OPT] -> all keys in [LO, HI]
                                            optionally filtered by version
      --fy + --version (legacy)    -> single (fy, version) pair
      --fy + --all-versions        -> every version for that fy
    """
    sources = config.GROUPER_SOURCES

    if args.all:
        return sorted(sources.keys())

    if args.fy_range:
        try:
            lo_str, hi_str = args.fy_range.split("-", 1)
            lo, hi = int(lo_str), int(hi_str)
        except ValueError as exc:
            raise SystemExit(
                f"--fy-range must look like '2008-2025'; got {args.fy_range!r}"
            ) from exc
        if lo > hi:
            raise SystemExit(
                f"--fy-range LO-HI requires LO<=HI; got {lo}-{hi}"
            )
        keys = [k for k in sorted(sources) if lo <= k[0] <= hi]
        if args.version:
            keys = [k for k in keys if k[1] == args.version]
        if not keys:
            raise SystemExit(
                f"--fy-range {lo}-{hi} (version={args.version!r}) "
                f"matched no GROUPER_SOURCES keys"
            )
        return keys

    if args.fy is None:
        raise SystemExit(
            "specify exactly one of --all, --fy-range LO-HI, or --fy + --version"
        )

    if args.all_versions:
        keys = [k for k in sorted(sources) if k[0] == args.fy]
        if not keys:
            raise SystemExit(
                f"--fy {args.fy} --all-versions matched no GROUPER_SOURCES keys"
            )
        return keys

    if not args.version:
        raise SystemExit("--fy requires --version (or --all-versions)")
    key = (args.fy, args.version)
    if key not in sources:
        raise SystemExit(
            f"({args.fy}, {args.version!r}) not in GROUPER_SOURCES; "
            f"known keys (truncated): {list(sources)[:8]}"
        )
    return [key]


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.extract.grouper",
        description="MS-DRG Java grouper bronze pull + JAR positioning.",
    )
    p.add_argument("--fy", type=int, default=None)
    p.add_argument("--version", type=str, default=None,
                   help="Grouper version, e.g. V43.1")
    p.add_argument(
        "--fy-range", type=str, default=None,
        help="LO-HI inclusive, e.g. 2008-2025 (filters GROUPER_SOURCES keys)",
    )
    p.add_argument(
        "--all", action="store_true",
        help="Iterate every (fy, version) in GROUPER_SOURCES",
    )
    p.add_argument(
        "--all-versions", action="store_true",
        help="With --fy: iterate every version for that fy",
    )
    p.add_argument("--download-date", type=str, default=None,
                   help="YYYY-MM-DD; defaults to today")
    p.add_argument("--strict", action="store_true",
                   help="Raise on member mismatch instead of stderr-warn")
    p.add_argument(
        "--continue-on-error", action="store_true",
        help="In bulk runs, log+continue rather than aborting on the first failure",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    targets = _resolve_targets(args)

    if len(targets) == 1:
        fy, version = targets[0]
        extract_fy_version(
            fy=fy, version=version,
            download_date=args.download_date, strict=args.strict,
        )
        return 0

    # Bulk mode -- ordered, deterministic, with optional log+continue.
    print(
        f"[grouper] bulk extract: {len(targets)} (fy, version) cells",
        file=sys.stderr,
    )
    failures: list[tuple[int, str, str]] = []
    for i, (fy, version) in enumerate(targets, 1):
        print(
            f"[grouper] [{i}/{len(targets)}] fy{fy} {version} ...",
            file=sys.stderr,
        )
        try:
            extract_fy_version(
                fy=fy, version=version,
                download_date=args.download_date, strict=args.strict,
            )
        except Exception as exc:  # noqa: BLE001 -- bulk-mode error capture
            failures.append((fy, version, repr(exc)))
            print(
                f"[grouper] FAIL fy{fy} {version}: {exc!r}",
                file=sys.stderr,
            )
            if not args.continue_on_error:
                raise
    if failures:
        print(
            f"[grouper] bulk extract complete with {len(failures)} failures:",
            file=sys.stderr,
        )
        for fy, version, msg in failures:
            print(f"  fy{fy} {version}: {msg}", file=sys.stderr)
        return 2
    print(
        f"[grouper] bulk extract complete: {len(targets)} cells, 0 failures",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
