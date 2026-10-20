"""ICD-10-CM extractor: CDC FTP ZIPs -> per-FY silver parquet.

For each fiscal year in scope this module:

    1. Downloads the four ICD-10-CM ZIPs published by CDC into the bronze
       layer (``data/bronze/icd10cm/fy{fy}/{download_date}/``):

         - ``table_and_index``: tabular XML + alphabetic index. Source of truth
           for ``code``, ``description`` (long title), and ``chapter``.
         - ``code_descriptions``: flat-file long descriptions
           (``icd10cm_codes_{fy}.txt``) plus a 5-column order file
           (``icd10cm_order_{fy}.txt``) carrying both short and long titles.
           Source of truth for ``short_description``.
         - ``addenda``: 3-4 flat files documenting code-level changes
           (added/revised/deleted). Mapped to ``addendum_action``.
         - ``poa_exempt``: a single flat list of POA-exempt codes. Drives the
           boolean ``poa_exempt`` column.

    2. Parses the artifacts and emits one parquet at
       ``data/silver/per_source/icd10cm_fy{fy}.parquet`` whose schema matches
       ``src.transform.codes_master._SCHEMA`` exactly (Phase-3 backfill columns
       emitted as nullable typed nulls).

CLI::

    python -m src.extract.icd10cm --fy 2026
    python -m src.extract.icd10cm --fy-range 2016-2026

Conventions (CLAUDE.md):

    - Codes are upper-case, dotless. CDC publishes ICD-10-CM dotless to begin
      with; this module never reintroduces a dot.
    - Bronze is write-once. The wrapper ``download_to_bronze`` short-circuits
      when the ZIP already exists, so re-runs are cheap.
    - URLs are composed via ``config.ICD10CM_SOURCES`` and
      ``config.ICD10CM_FILENAME_PATTERNS``; no URL is hardcoded here.

Defensive behavior:

    - Older CDC FYs occasionally use a "table and index" filename with spaces
      instead of hyphens. We try the canonical hyphen variant first; on 404 we
      retry with the space variant for FY <= 2017 only (plan-v1.md risk #4).
    - Every ``download_to_bronze`` call is wrapped to re-raise as
      ``RuntimeError`` with the FY tag and offending URL (plan-v1.md risk #1).
    - The tabular XML is parsed with ``iterparse`` so memory stays bounded
      across the 70k+ codes in recent FYs.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import etree

from src import config
from src.extract._http import download_to_bronze, make_session

_SOURCE_NAME: str = "icd10cm"

# Order in which we hand artifacts back from ``_download_all_zips``. Keep stable
# so callers can pattern-match positionally if they need to.
_ARTIFACT_KEYS: tuple[str, ...] = (
    "table_and_index",
    "code_descriptions",
    "addenda",
    "poa_exempt",
)

# Locked output schema. Column order and types must match
# ``src.transform.codes_master._SCHEMA`` exactly.
_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        ("code", pa.string()),
        ("code_system", pa.string()),
        ("fy_or_cy", pa.int32()),
        ("effective_start", pa.date32()),
        ("effective_end", pa.date32()),
        ("description", pa.string()),
        ("short_description", pa.string()),
        ("chapter", pa.string()),
        ("addendum_action", pa.string()),
        ("or_procedure_flag", pa.bool_()),
        ("mcc_flag", pa.bool_()),
        ("cc_flag", pa.bool_()),
        ("poa_exempt", pa.bool_()),
        ("source", pa.string()),
        ("n", pa.int32()),
    ]
)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _compose_url(fy: int, key: str) -> str:
    """Compose the canonical CDC URL for ``(fy, key)``.

    Falls back to the per-FY override in ``config.ICD10CM_FILENAME_OVERRIDES``
    when one is registered. Spaces in override filenames are URL-encoded here
    (the bronze on-disk filename is left raw).
    """
    base = config.ICD10CM_SOURCES[fy]
    override = _resolve_override(fy, key)
    if override is None:
        # Override sentinel meaning "no artifact for this FY". Caller should
        # check ``_resolve_override`` first; this branch only fires if the
        # override is explicitly None (key is registered but missing upstream).
        raise RuntimeError(
            f"icd10cm fy{fy}: no upstream artifact for key={key!r}"
        )
    if isinstance(override, list):
        # Loose-files mode is handled by callers, not _compose_url.
        raise RuntimeError(
            f"icd10cm fy{fy}: key={key!r} is loose-files; use _compose_loose_urls"
        )
    if override == _LOOSE_SENTINEL:
        # The artifact is part of a loose-files bundle resolved via
        # the table_and_index list — there's no standalone URL.
        raise RuntimeError(
            f"icd10cm fy{fy}: key={key!r} is consumed by loose-files mode"
        )
    if isinstance(override, str):
        return base + quote(override, safe="/-_.~")
    pattern = config.ICD10CM_FILENAME_PATTERNS[key]
    return base + pattern.format(fy=fy, yy=fy % 100)


_LOOSE_SENTINEL: str = "__LOOSE__"


def _resolve_override(fy: int, key: str) -> object:
    """Resolve the per-FY filename override for ``(fy, key)``.

    Returns:
        - ``str`` (raw filename) when an override is registered.
        - ``list[str]`` of loose filenames when in loose-files mode.
        - ``None`` when explicitly absent upstream (skip artifact).
        - ``"__LOOSE__"`` when this key is consumed by a sibling key's loose list.
        - The sentinel ``False`` when no override exists (fall back to canonical
          pattern). We use ``False`` rather than ``None`` because ``None`` is
          itself a meaningful override value.
    """
    fy_overrides = config.ICD10CM_FILENAME_OVERRIDES.get(fy)
    if fy_overrides is None or key not in fy_overrides:
        return False
    return fy_overrides[key]


def _compose_loose_urls(fy: int, filenames: list[str]) -> list[tuple[str, str]]:
    """Return ``[(filename, url), ...]`` for loose-files-mode FYs."""
    base = config.ICD10CM_SOURCES[fy]
    return [(fn, base + quote(fn, safe="/-_.~")) for fn in filenames]


def _safe_download(
    url: str,
    fy: int,
    filename: str,
    *,
    session: requests.Session,
    download_date: str | None,
) -> Path:
    """Wrap ``download_to_bronze`` so HTTP errors carry the FY tag."""
    try:
        return download_to_bronze(
            url,
            _SOURCE_NAME,
            fy,
            filename,
            download_date=download_date,
            session=session,
        )
    except requests.HTTPError as exc:
        raise RuntimeError(f"icd10cm fy{fy}: {url} failed: {exc}") from exc


# Hyphen<->space substring pairs that we know CDC has flipped between across
# FYs. When a canonical hyphenated filename 404s, we retry with the space
# (URL-encoded) variant. The empirical CDC FY2026 directory listing confirms
# both ``icd10cm-table and index-2026.zip`` and
# ``icd10cm-Code Descriptions-2026.zip`` use spaces, contradicting the
# hyphen-stable assumption baked into config's filename patterns. So we apply
# this fallback unconditionally (any FY), not just to old ones.
_HYPHEN_SPACE_VARIANTS: dict[str, str] = {
    "table-and-index": "table%20and%20index",
    "Code-Descriptions": "Code%20Descriptions",
}


def _try_download_with_variant(
    fy: int,
    key: str,
    *,
    session: requests.Session,
    download_date: str | None,
) -> tuple[Path, str]:
    """Download the ZIP for ``(fy, key)``. On 404 against the canonical
    hyphenated URL, retry with the matching space variant (URL-encoded).

    Returns ``(local_path, url_used)``. The URL used is the one that succeeded;
    the caller stores it in the parquet's ``source`` column.
    """
    url = _compose_url(fy, key)
    # Local filename: URL-decoded for sanity (strip %20, drop trailing .zip case).
    filename_from_url = url.rsplit("/", 1)[-1]
    local_filename = filename_from_url.replace("%20", "_")
    try:
        path = _safe_download(
            url, fy, local_filename, session=session, download_date=download_date
        )
        return path, url
    except RuntimeError as exc:
        if "404" not in str(exc):
            raise
        # Per-FY override should already be exact; only attempt the
        # hyphen->space fallback when we used the CANONICAL pattern (no
        # override registered for this key). Otherwise re-raise.
        override = _resolve_override(fy, key)
        if override is not False:
            raise
        # Try a hyphen->space variant if applicable.
        canonical_filename = config.ICD10CM_FILENAME_PATTERNS[key].format(
            fy=fy, yy=fy % 100
        )
        variant_filename = canonical_filename
        for hyphen, encoded_space in _HYPHEN_SPACE_VARIANTS.items():
            if hyphen in variant_filename:
                variant_filename = variant_filename.replace(hyphen, encoded_space)
                break
        if variant_filename == canonical_filename:
            # No known variant pattern applies — surface the original error.
            raise
        variant_url = config.ICD10CM_SOURCES[fy] + variant_filename
        # Local filename: keep on-disk name URL-decoded for sanity.
        local_filename = variant_filename.replace("%20", "_")
        print(
            f"[icd10cm] fy{fy}: canonical URL 404'd for {key}; "
            f"retrying space variant {variant_url}",
            file=sys.stderr,
        )
        path = _safe_download(
            variant_url,
            fy,
            local_filename,
            session=session,
            download_date=download_date,
        )
        return path, variant_url


def _download_loose_files(
    fy: int,
    filenames: list[str],
    *,
    session: requests.Session,
    download_date: str | None,
) -> tuple[list[Path], str]:
    """Download every file in a loose-files-mode FY into bronze.

    Returns ``(paths, source_url)`` where ``source_url`` is the URL of the
    tabular XML (used as the parquet's ``source`` column).
    """
    paths: list[Path] = []
    source_url: str | None = None
    for filename, url in _compose_loose_urls(fy, filenames):
        path = _safe_download(
            url, fy, filename, session=session, download_date=download_date
        )
        paths.append(path)
        if filename.endswith(".xml"):
            source_url = url
    if source_url is None:
        # No XML in the loose set — fall back to first URL.
        source_url = _compose_loose_urls(fy, filenames)[0][1]
    return paths, source_url


def _download_all_zips(
    fy: int,
    *,
    session: requests.Session,
    download_date: str | None,
) -> tuple[dict[str, object], str]:
    """Download all four CDC ZIPs for the given FY into bronze.

    Returns ``(paths_by_key, source_url)``. ``paths_by_key[key]`` is one of:
        - ``Path`` (single ZIP file or single loose file)
        - ``list[Path]`` (loose-files mode aggregated under table_and_index)
        - ``None`` (artifact does not exist for this FY)

    ``source_url`` is the URL of the table_and_index artifact, used as the
    parquet's ``source`` column.
    """
    paths: dict[str, object] = {}
    source_url: str | None = None
    for key in _ARTIFACT_KEYS:
        override = _resolve_override(fy, key)
        if override is None:
            # Artifact is explicitly absent for this FY (e.g. FY2016 addenda).
            paths[key] = None
            continue
        if isinstance(override, list):
            # Loose-files mode (only valid on table_and_index key).
            if key != "table_and_index":
                raise RuntimeError(
                    f"icd10cm fy{fy}: loose-files list set on key={key!r}; "
                    f"only table_and_index supports loose mode"
                )
            loose_paths, loose_url = _download_loose_files(
                fy, override, session=session, download_date=download_date
            )
            paths[key] = loose_paths
            source_url = loose_url
            continue
        if isinstance(override, str) and override == _LOOSE_SENTINEL:
            # Consumed by sibling key's loose list — point at the loose list.
            paths[key] = paths.get("table_and_index")
            continue
        # Either a registered string override OR no override (canonical fallback).
        path, url_used = _try_download_with_variant(
            fy, key, session=session, download_date=download_date
        )
        paths[key] = path
        if key == "table_and_index":
            source_url = url_used
    assert source_url is not None, f"icd10cm fy{fy}: failed to resolve source URL"
    return paths, source_url


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _open_zip_member(zip_path: Path, predicate) -> tuple[str, bytes] | None:
    """Open ``zip_path`` and return ``(member_name, raw_bytes)`` for the first
    member whose name satisfies ``predicate(name_lower) -> bool``.

    Returns None when no member matches.
    """
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if predicate(name.lower()):
                with zf.open(name) as fh:
                    return name, fh.read()
    return None


def _iter_zip_members(zip_path: Path):
    """Yield ``(member_name, raw_bytes)`` for every regular file in ``zip_path``."""
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            with zf.open(info) as fh:
                yield info.filename, fh.read()


def _decode_text(raw: bytes) -> str:
    """Decode CDC text artifacts. They are typically Latin-1 / Windows-1252."""
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def _read_tabular_xml_bytes(path_or_zip: Path) -> bytes:
    """Return the raw bytes of the tabular XML.

    Accepts either a ZIP containing ``*tabular*.xml`` (FY2018+), or a loose
    ``.xml`` path (FY2019/FY2020), or a ZIP that ships only one XML inside a
    nested ``Table and Index`` directory (older layouts). Raises
    ``RuntimeError`` if no tabular-shaped XML can be located.
    """
    if path_or_zip.suffix.lower() == ".xml":
        return path_or_zip.read_bytes()
    # ZIP: prefer "*tabular*.xml" first; fall back to any large XML file with
    # ``<diag>`` content (older FYs may not include "tabular" in the filename).
    member = _open_zip_member(
        path_or_zip, lambda n: n.endswith(".xml") and "tabular" in n
    )
    if member is not None:
        return member[1]
    # Fallback: pick the largest .xml member that contains a <diag tag.
    best: tuple[str, bytes] | None = None
    with zipfile.ZipFile(path_or_zip) as zf:
        for info in zf.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".xml"):
                continue
            with zf.open(info) as fh:
                raw = fh.read()
            if b"<diag" not in raw:
                continue
            if best is None or len(raw) > len(best[1]):
                best = (info.filename, raw)
    if best is None:
        raise RuntimeError(
            f"icd10cm: no tabular XML found in {path_or_zip} "
            f"(expected a *.xml member containing <diag>)"
        )
    return best[1]


def _parse_tabular_xml(path_or_zip: Path) -> list[dict]:
    """Parse the CDC tabular XML, returning one dict per ``<diag>``.

    Handles arbitrary nesting of ``<diag>`` (category and subcategory codes)
    and walks up to the enclosing ``<chapter>`` for the ``chapter`` field.
    Uses ``iterparse`` so memory stays bounded.

    The table-and-index ZIP ships several XMLs (index, drug, neoplasm, eindex,
    tabular). We need the tabular one specifically — anything else has a
    different schema and would yield zero ``<diag>`` rows.

    Output dict shape::

        {"code": "A000", "description": "Cholera due to ...", "chapter": "..."}

    Important: the tabular XML is **not** an exhaustive list of billable codes.
    Many billable codes are 6-7 chars long where the tabular only enumerates
    the 3-5 char parent (e.g. fracture codes get a 7th-character extender that
    is documented in the tabular's instructions but not enumerated as
    ``<diag>``). For an exhaustive code list use the order file
    (``_parse_master_codes``); the tabular is here only to supply
    ``chapter`` per 3-char category prefix and to enrich descriptions when
    available.
    """
    raw = _read_tabular_xml_bytes(path_or_zip)

    rows: list[dict] = []
    # iterparse on both 'start' and 'end' so we can track the current chapter
    # title from <chapter><desc>...</desc></chapter>. We grab the chapter desc
    # at <desc> end-events whose parent is <chapter> (depth tracking).
    context = etree.iterparse(
        _BytesReader(raw),
        events=("start", "end"),
        # Recover lets us tolerate minor schema drift between FYs.
        recover=True,
        huge_tree=True,
    )

    chapter_stack: list[str] = []
    current_chapter: str | None = None

    for event, elem in context:
        tag = elem.tag
        if event == "start":
            if tag == "chapter":
                # Push a placeholder; replaced when we see the chapter's <desc>.
                chapter_stack.append("")
        elif event == "end":
            if tag == "desc" and elem.getparent() is not None and elem.getparent().tag == "chapter":
                # First <desc> child of <chapter> is the chapter title.
                if chapter_stack:
                    chapter_stack[-1] = (elem.text or "").strip()
                current_chapter = chapter_stack[-1] if chapter_stack else None
            elif tag == "diag":
                name_el = elem.find("name")
                desc_el = elem.find("desc")
                if name_el is not None and desc_el is not None:
                    code_text = (name_el.text or "").strip().upper().replace(".", "")
                    desc_text = (desc_el.text or "").strip()
                    if code_text:
                        rows.append(
                            {
                                "code": code_text,
                                "description": desc_text,
                                "chapter": current_chapter,
                            }
                        )
                # Don't clear here — nested <diag> children are siblings of the
                # parent's name/desc; clearing the parent breaks getparent() on
                # already-yielded children. iterparse end-events fire deepest-
                # first, so we just clear the leaf <diag>.
                elem.clear()
            elif tag == "chapter":
                if chapter_stack:
                    chapter_stack.pop()
                current_chapter = chapter_stack[-1] if chapter_stack else None
                elem.clear()

    return rows


class _BytesReader:
    """Tiny file-like wrapper so lxml.iterparse can consume a ``bytes`` blob.

    lxml expects ``read(n)`` semantics; ``io.BytesIO`` would also work but this
    avoids one import and keeps the dependency surface minimal.
    """

    def __init__(self, raw: bytes) -> None:
        self._raw = raw
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            chunk = self._raw[self._pos :]
            self._pos = len(self._raw)
            return chunk
        chunk = self._raw[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk


def _read_order_file_bytes(path_or_zip: Path) -> bytes | None:
    """Return raw bytes of the order file, or None if not present.

    Accepts a loose ``.txt`` (FY2019/FY2020) or a ZIP containing
    ``*order*.txt`` (FY2018+). Excludes the per-FY addenda variants.
    """
    if path_or_zip.suffix.lower() == ".txt":
        # Loose mode: the caller passed us the order file directly.
        return path_or_zip.read_bytes()
    member = _open_zip_member(
        path_or_zip,
        lambda n: n.endswith(".txt") and "order" in n and "addenda" not in n,
    )
    if member is None:
        return None
    return member[1]


def _read_codes_file_bytes(path_or_zip: Path) -> bytes | None:
    """Return raw bytes of the simple ``icd10cm_codes_{fy}.txt`` flat file.

    This format ships in some FYs (e.g. FY2018) when the fixed-width order
    file isn't available. Layout::

        <code>   <long description>

    Whitespace-delimited (the code is space-padded to 7 chars then a few
    spaces then the description). Returns None when no codes file is found.
    """
    if path_or_zip.suffix.lower() == ".txt":
        return path_or_zip.read_bytes()
    member = _open_zip_member(
        path_or_zip,
        lambda n: (
            n.endswith(".txt")
            and "codes" in n
            and "addenda" not in n
            and "order" not in n
        ),
    )
    if member is None:
        return None
    return member[1]


def _parse_codes_flat(raw: bytes) -> list[dict]:
    """Parse the simple two-column ``icd10cm_codes_{fy}.txt`` format."""
    text = _decode_text(raw)
    out: list[dict] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue
        code = parts[0].strip().upper().replace(".", "")
        if not code:
            continue
        out.append(
            {
                "code": code,
                "short_description": "",
                "description": parts[1].strip(),
            }
        )
    return out


def _parse_master_codes(path_or_zip: Path) -> list[dict]:
    """Return the exhaustive list of billable ICD-10-CM codes for the FY.

    Reads the order file (``icd10cm-order-{fy}.txt``) and emits one row per
    leaf code (``valid_flag == "1"``). Each row carries::

        {"code": str, "short_description": str, "description": str}

    Column layout (1-indexed; verified against FY2026 hex dump)::

        cols 1-5    order number
        col  6      space
        cols 7-13   code (7-char window, code left-justified, no dot)
        col  14     space
        col  15     valid-flag (``0`` = header, ``1`` = leaf billable)
        col  16     space
        cols 17-76  short title (60 chars)
        col  77     space
        cols 78+    long title

    Tolerant of older FYs by using fixed slices and stripping whitespace per
    field.
    """
    raw = _read_order_file_bytes(path_or_zip)
    if raw is None:
        raise RuntimeError(
            f"icd10cm: no order file found in {path_or_zip} "
            f"(expected ``*order*{{fy}}*.txt`` excluding the addenda variants)"
        )
    text = _decode_text(raw)
    out: list[dict] = []
    for line in text.splitlines():
        if len(line) < 17:
            continue
        valid_flag = line[14:15]
        if valid_flag != "1":
            # Skip header / non-billable rows.
            continue
        code_field = line[6:13].strip().upper().replace(".", "")
        if not code_field:
            continue
        short_field = line[16:76].rstrip() if len(line) > 16 else ""
        long_field = line[77:].rstrip() if len(line) > 77 else ""
        out.append(
            {
                "code": code_field,
                "short_description": short_field.strip(),
                "description": long_field.strip(),
            }
        )
    return out


# Filename keyword -> addendum_action vocabulary. Ordered most-specific-first
# so substrings like "add" don't capture "added"-named files before "deleted"
# is checked.
_ADDENDA_FILENAME_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("delet", "deleted"),
    ("revis", "revised"),
    ("added", "added"),
    ("addition", "added"),
    # ``add`` last so ``addenda`` filenames don't claim it.
    ("add-", "added"),
)

# Line-prefix keyword -> action. CDC's ``icd10cm-codes-addenda-{fy}.txt`` file
# uses prefixes like ``Add:``, ``Delete:``, ``Revise``. Match
# case-insensitively.
_ADDENDA_LINE_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("delete", "deleted"),
    ("revise", "revised"),
    ("add", "added"),
)


def _classify_addenda_filename(filename: str) -> str | None:
    """Map an addenda member filename to an ``addendum_action`` value.

    Returns None for files that don't encode a clear action in the filename —
    callers fall back to per-line action prefixes for those.
    """
    lower = filename.lower()
    if "header" in lower:
        return None
    for kw, action in _ADDENDA_FILENAME_KEYWORDS:
        if kw in lower:
            return action
    return None


_ICD10CM_CODE_RE = re.compile(r"^[A-Z][A-Z0-9]{2,7}$")


def _looks_like_icd10cm_code(token: str) -> bool:
    """Heuristic: token is shaped like an ICD-10-CM code (already dotless,
    upper-cased)."""
    return bool(_ICD10CM_CODE_RE.match(token))


def _parse_addenda_lines(text: str, filename_action: str | None) -> dict[str, str]:
    """Parse a single addenda txt file into ``{code: action}``.

    If ``filename_action`` is set, every code on a recognized line is mapped
    to that action. Otherwise each line's leading token is classified against
    ``_ADDENDA_LINE_KEYWORDS`` (e.g. ``Add:    A001  Description``).
    """
    out: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        tokens = stripped.split()
        if not tokens:
            continue

        action: str | None
        code_idx: int
        if filename_action is not None:
            action = filename_action
            code_idx = 0
        else:
            head = tokens[0].lower().rstrip(":")
            action = None
            for kw, mapped in _ADDENDA_LINE_KEYWORDS:
                if head.startswith(kw):
                    action = mapped
                    break
            if action is None:
                continue
            code_idx = 1
            # Some files have ``Action:  <flag> <code> ...`` (the order-addenda
            # form has a single-digit valid flag between the action and the
            # code). Skip a one-character numeric token at code_idx.
            if (
                len(tokens) > code_idx + 1
                and len(tokens[code_idx]) == 1
                and tokens[code_idx].isdigit()
            ):
                code_idx += 1
            # ``Revise`` lines come as ``Revise from: <code> ...`` or
            # ``Revise to: <code> ...`` — skip the ``from:`` / ``to:`` token.
            if (
                code_idx < len(tokens)
                and tokens[code_idx].lower().rstrip(":") in ("from", "to")
            ):
                code_idx += 1

        if code_idx >= len(tokens):
            continue
        token = tokens[code_idx]
        code = token.upper().replace(".", "")
        if not _looks_like_icd10cm_code(code):
            continue
        out[code] = action
    return out


def _parse_addenda(paths: list[Path | None]) -> dict[str, str]:
    """Return ``{code: addendum_action}`` parsed from ZIPs and/or loose files.

    Accepts ``None`` entries (skipped). Loose ``.txt`` paths are parsed
    directly; ZIPs are scanned for addenda-shaped members. The CDC FY2026
    addenda ZIP ships PDFs only — the actual flat-file addenda live in the
    Code Descriptions ZIP, so callers pass both ZIPs.
    """
    out: dict[str, str] = {}
    for path in paths:
        if path is None or not path.exists():
            continue
        if path.suffix.lower() == ".txt":
            # Loose addenda flat file.
            lower = path.name.lower()
            if not any(
                kw in lower
                for kw in ("addenda", "addendum", "added", "deleted", "revised")
            ):
                continue
            text = _decode_text(path.read_bytes())
            file_action = _classify_addenda_filename(path.name)
            file_rows = _parse_addenda_lines(text, file_action)
            out.update(file_rows)
            continue
        if path.suffix.lower() != ".zip":
            continue
        for filename, raw in _iter_zip_members(path):
            lower = filename.lower()
            if not lower.endswith(".txt"):
                continue
            # Only consider files that look like addenda — name-checks 'addenda'
            # or 'addendum' substring, OR is one of the named delta files.
            if not any(
                kw in lower for kw in ("addenda", "addendum", "added", "deleted", "revised")
            ):
                continue
            text = _decode_text(raw)
            file_action = _classify_addenda_filename(filename)
            file_rows = _parse_addenda_lines(text, file_action)
            out.update(file_rows)
    return out


def _parse_poa_exempt(zip_path: Path) -> set[str]:
    """Return the set of POA-exempt codes from the POA-exempt ZIP.

    The CDC FY2026 POA-exempt files are tab-delimited with three columns::

        Order26\tPOAexemptCode\tDescription
        01238\tB90.0\tSequelae of central nervous system tuberculosis

    We pull the **second** column (the code), strip its dot, and skip any
    row whose code-column doesn't shape like an ICD-10-CM code (e.g. the
    header row whose second column is the literal ``POAexemptCode``).

    The ZIP contains both an ``xlsx`` and a ``txt`` form for each delta plus a
    full-list file (``POAexemptCodesFY{yy}.txt``); we only parse ``.txt``.

    Skipped: ``ReadMePOAexemptFY{yy}.txt`` (free-form notes, no code rows).
    """
    out: set[str] = set()
    for filename, raw in _iter_zip_members(zip_path):
        lower = filename.lower()
        if not lower.endswith(".txt"):
            continue
        if "readme" in lower:
            continue
        text = _decode_text(raw)
        for line in text.splitlines():
            stripped = line.rstrip("\r\n")
            if not stripped.strip():
                continue
            # The format is tab-delimited; fall back to whitespace if the line
            # has no tabs (older FYs may lack the header).
            parts = stripped.split("\t") if "\t" in stripped else stripped.split()
            if len(parts) < 2:
                continue
            token = parts[1].strip()
            code = token.upper().replace(".", "")
            if _looks_like_icd10cm_code(code):
                out.add(code)
    return out


# ---------------------------------------------------------------------------
# Assemble + write
# ---------------------------------------------------------------------------


def _master_codes_from_tabular(tabular_rows: list[dict]) -> list[dict]:
    """Derive a master-codes list from tabular XML rows when no order file is
    available (FY2017 ships only XML).

    Each tabular ``<diag>`` row becomes one master row. ``short_description``
    is left empty (the XML doesn't carry short titles); ``description`` is the
    full ``<desc>`` text. Result will under-count vs the official total because
    the XML enumerates 3-5 char categories but not all 6-7 char extender
    combinations — accept the gap (~46k vs ~71k for FY2017) and document.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for row in tabular_rows:
        code = row.get("code") or ""
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(
            {
                "code": code,
                "short_description": "",
                "description": row.get("description") or "",
            }
        )
    return out


def _build_chapter_lookup(tabular_rows: list[dict]) -> dict[str, str]:
    """Return ``{3-char category prefix: chapter}`` derived from tabular XML.

    Many billable codes (e.g. 7th-character-extended fracture codes) are
    enumerated only in the order file, not in the tabular XML. We derive
    chapter for those codes by 3-char prefix (the ICD-10-CM ``category``
    layer, which is always present in the tabular).
    """
    out: dict[str, str] = {}
    for row in tabular_rows:
        code = row.get("code") or ""
        chapter = row.get("chapter")
        if not chapter:
            continue
        prefix = code[:3]
        if not prefix:
            continue
        # First occurrence wins. Categories are unique to a chapter.
        out.setdefault(prefix, chapter)
    return out


def _build_table(
    fy: int,
    *,
    master_codes: list[dict],
    tabular_rows: list[dict],
    addenda: dict[str, str],
    poa_exempt: set[str],
    source_url: str,
    default_addendum_action: str = "unchanged",
) -> pa.Table:
    """Assemble the per-FY parquet table from parsed inputs.

    ``master_codes`` (from the order file) is the source of truth for the code
    list and for short/long descriptions. ``tabular_rows`` (from the tabular
    XML) supplies ``chapter`` via 3-char category prefix and may override the
    long description when the tabular text differs (per plan-v1.md note: "the
    FY's tabular XML is the authoritative description"); we apply the
    tabular-as-truth override only for codes the tabular actually enumerates.
    """
    if not master_codes:
        raise RuntimeError(
            f"icd10cm fy{fy}: order file yielded zero leaf codes; check parser"
        )

    fy_start, fy_end = config.fy_interval(fy)

    chapter_by_prefix = _build_chapter_lookup(tabular_rows)
    tabular_desc_by_code = {row["code"]: row["description"] for row in tabular_rows}

    # Order file is master; de-duplicate (last wins for descriptions).
    seen: dict[str, dict] = {}
    for row in master_codes:
        seen[row["code"]] = row

    codes = sorted(seen.keys())
    descriptions: list[str] = []
    short_descriptions: list[str | None] = []
    chapters: list[str | None] = []
    for c in codes:
        master_row = seen[c]
        # Tabular description preferred when present (plan-v1.md risk #6).
        desc = tabular_desc_by_code.get(c) or master_row["description"]
        descriptions.append(desc)
        short_descriptions.append(master_row["short_description"] or None)
        chapters.append(chapter_by_prefix.get(c[:3]))

    addendum_actions = [addenda.get(c, default_addendum_action) for c in codes]
    poa_flags = [c in poa_exempt for c in codes]
    n_rows = len(codes)

    arrays = {
        "code": pa.array(codes, type=pa.string()),
        "code_system": pa.array(["ICD-10-CM"] * n_rows, type=pa.string()),
        "fy_or_cy": pa.array([fy] * n_rows, type=pa.int32()),
        "effective_start": pa.array([fy_start] * n_rows, type=pa.date32()),
        "effective_end": pa.array([fy_end] * n_rows, type=pa.date32()),
        "description": pa.array(descriptions, type=pa.string()),
        "short_description": pa.array(short_descriptions, type=pa.string()),
        "chapter": pa.array(chapters, type=pa.string()),
        "addendum_action": pa.array(addendum_actions, type=pa.string()),
        "or_procedure_flag": pa.array([None] * n_rows, type=pa.bool_()),
        "mcc_flag": pa.array([None] * n_rows, type=pa.bool_()),
        "cc_flag": pa.array([None] * n_rows, type=pa.bool_()),
        "poa_exempt": pa.array(poa_flags, type=pa.bool_()),
        "source": pa.array([source_url] * n_rows, type=pa.string()),
        "n": pa.array([None] * n_rows, type=pa.int32()),
    }

    return pa.table(arrays, schema=_PARQUET_SCHEMA)


def extract_fy(
    fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Download + parse one FY of ICD-10-CM. Returns the per-source parquet path."""
    if fy not in config.ICD10CM_SOURCES:
        raise ValueError(
            f"icd10cm fy{fy}: not registered in config.ICD10CM_SOURCES "
            f"(known FYs: {sorted(config.ICD10CM_SOURCES)})"
        )

    sess = session or make_session()
    paths, table_url = _download_all_zips(
        fy, session=sess, download_date=download_date
    )

    # Resolve the inputs for each parser. Loose-files-mode FYs (FY2019/FY2020)
    # have a list of paths under "table_and_index"; everything else is a single
    # Path or None.
    table_input: Path
    order_input: Path | None
    addenda_inputs: list[Path | None]

    table_artifact = paths["table_and_index"]
    if isinstance(table_artifact, list):
        # Loose-files mode: separate the bundle by extension.
        xml_paths = [p for p in table_artifact if p.suffix.lower() == ".xml"]
        txt_paths = [p for p in table_artifact if p.suffix.lower() == ".txt"]
        if not xml_paths:
            raise RuntimeError(
                f"icd10cm fy{fy}: loose-files mode but no .xml in bundle"
            )
        table_input = xml_paths[0]
        # The "order" file contains both short and long titles; preferred.
        order_paths = [p for p in txt_paths if "order" in p.name.lower() and "addenda" not in p.name.lower()]
        order_input = order_paths[0] if order_paths else None
        # All loose .txts are candidates for addenda parsing (the helper filters).
        addenda_inputs = list(txt_paths)
    else:
        table_input = table_artifact  # type: ignore[assignment]
        cd = paths.get("code_descriptions")
        order_input = cd if isinstance(cd, Path) else None
        ad = paths.get("addenda")
        addenda_inputs = [ad if isinstance(ad, Path) else None,
                          cd if isinstance(cd, Path) else None]

    print(f"[icd10cm] fy{fy}: parsing tabular XML ({table_input})", file=sys.stderr)
    tabular_rows = _parse_tabular_xml(table_input)
    print(f"[icd10cm] fy{fy}: tabular yielded {len(tabular_rows)} <diag> rows", file=sys.stderr)

    master_codes: list[dict] = []
    if order_input is not None:
        print(f"[icd10cm] fy{fy}: parsing order file ({order_input})", file=sys.stderr)
        try:
            master_codes = _parse_master_codes(order_input)
            print(
                f"[icd10cm] fy{fy}: order file yielded {len(master_codes)} leaf codes",
                file=sys.stderr,
            )
        except RuntimeError as exc:
            # No order file inside the ZIP — fall back to the simple codes flat
            # file (FY2018 ships this layout).
            print(
                f"[icd10cm] fy{fy}: order-file parse failed ({exc}); "
                f"falling back to codes flat file",
                file=sys.stderr,
            )
            codes_raw = _read_codes_file_bytes(order_input)
            if codes_raw is not None:
                master_codes = _parse_codes_flat(codes_raw)
                print(
                    f"[icd10cm] fy{fy}: codes flat yielded {len(master_codes)} codes",
                    file=sys.stderr,
                )
    if not master_codes:
        # FY2017 path (or other XML-only FYs): derive codes from tabular XML
        # alone. Expected lower row count (~46k-50k vs ~71k official total) —
        # see landmines.
        print(
            f"[icd10cm] fy{fy}: no order/codes file; deriving codes from tabular XML",
            file=sys.stderr,
        )
        master_codes = _master_codes_from_tabular(tabular_rows)
        print(
            f"[icd10cm] fy{fy}: derived {len(master_codes)} codes from XML",
            file=sys.stderr,
        )

    # FY2016 (launch year, no addenda artifact): semantically every code is
    # "added". FY2017 also has no flat-file addenda but most codes carry over
    # from FY2016 — default those rows to "unchanged" instead. Other FYs have
    # addenda; default unmatched rows to "unchanged".
    fy_addenda_missing = paths.get("addenda") is None and not isinstance(
        paths.get("table_and_index"), list
    )
    if fy_addenda_missing:
        if fy == 2016:
            default_addendum_action = "added"
        else:
            default_addendum_action = "unchanged"
        print(
            f"[icd10cm] fy{fy}: addenda absent upstream; "
            f"defaulting to '{default_addendum_action}'",
            file=sys.stderr,
        )
        addenda = {}
    else:
        addenda = _parse_addenda(addenda_inputs)
        default_addendum_action = "unchanged"
        print(f"[icd10cm] fy{fy}: addenda map size = {len(addenda)}", file=sys.stderr)

    poa_artifact = paths.get("poa_exempt")
    if isinstance(poa_artifact, Path):
        poa = _parse_poa_exempt(poa_artifact)
    else:
        poa = set()
    print(f"[icd10cm] fy{fy}: poa-exempt set size = {len(poa)}", file=sys.stderr)

    table = _build_table(
        fy,
        master_codes=master_codes,
        tabular_rows=tabular_rows,
        addenda=addenda,
        poa_exempt=poa,
        source_url=table_url,
        default_addendum_action=default_addendum_action,
    )

    out_dir = output_dir if output_dir is not None else config.SILVER_PER_SOURCE
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"icd10cm_fy{fy}.parquet"
    pq.write_table(table, str(out_path))
    print(f"[icd10cm] fy{fy}: wrote {table.num_rows} rows to {out_path}", file=sys.stderr)
    return out_path


def extract_fy_range(
    start_fy: int,
    end_fy: int,
    *,
    session: requests.Session | None = None,
    download_date: str | None = None,
) -> list[Path]:
    """Loop ``extract_fy`` over inclusive ``[start_fy, end_fy]``.

    Per-FY failures are logged loudly and skipped — the bulk run is
    fault-tolerant so one bad FY doesn't abort the rest. The list of
    successfully written parquet paths is returned.
    """
    if end_fy < start_fy:
        raise ValueError(f"end_fy ({end_fy}) must be >= start_fy ({start_fy})")
    sess = session or make_session()
    out: list[Path] = []
    failures: list[tuple[int, str]] = []
    for fy in range(start_fy, end_fy + 1):
        try:
            path = extract_fy(fy, session=sess, download_date=download_date)
            out.append(path)
        except Exception as exc:  # noqa: BLE001 — bulk run swallows per-FY errors.
            print(
                f"[icd10cm] FY{fy} FAILED: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failures.append((fy, f"{type(exc).__name__}: {exc}"))
    if failures:
        print(
            f"[icd10cm] bulk run completed with {len(failures)} failure(s):",
            file=sys.stderr,
        )
        for fy, msg in failures:
            print(f"  FY{fy}: {msg}", file=sys.stderr)
    print(
        f"[icd10cm] bulk run summary: {len(out)} succeeded, {len(failures)} failed",
        file=sys.stderr,
    )
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_fy_range(arg: str) -> tuple[int, int]:
    """Parse ``"START-END"`` into ``(start, end)``. Raises argparse errors."""
    parts = arg.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--fy-range must be START-END (e.g. 2016-2026); got {arg!r}"
        )
    try:
        start, end = int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--fy-range parts must be integers; got {arg!r}"
        ) from exc
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.extract.icd10cm",
        description=(
            "Download and parse CDC ICD-10-CM artifacts for one or more "
            "fiscal years; emits data/silver/per_source/icd10cm_fy{FY}.parquet."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--fy",
        type=int,
        help="Single fiscal year to extract (e.g. 2026).",
    )
    group.add_argument(
        "--fy-range",
        type=_parse_fy_range,
        help="Inclusive FY range, START-END (e.g. 2016-2026).",
    )
    parser.add_argument(
        "--download-date",
        default=None,
        help=(
            "Override the bronze date subdir (YYYY-MM-DD). Default: today. "
            "Useful for replays."
        ),
    )
    args = parser.parse_args(argv)

    if args.fy is None and args.fy_range is None:
        parser.error("one of --fy or --fy-range is required")
        return 2  # pragma: no cover (argparse exits before this)

    session = make_session()
    if args.fy is not None:
        extract_fy(args.fy, session=session, download_date=args.download_date)
    else:
        start, end = args.fy_range
        extract_fy_range(
            start, end, session=session, download_date=args.download_date
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
