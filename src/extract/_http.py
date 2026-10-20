"""Thin requests wrapper for CDC/CMS file downloads.

Provides:
    - make_session(): pre-configured Session with retry adapter and User-Agent.
    - download(url, dest): idempotent streaming download to an explicit path.
    - download_to_bronze(url, source, fy, filename): convenience wrapper that
      composes the bronze path via src.config.bronze_dir.
    - head_etag(url): HEAD probe for upstream ETag (used to skip re-pulls).

Design notes:
    - Bronze is write-once per CLAUDE.md, so download() short-circuits when the
      destination file already exists with non-zero size.
    - Honors Retry-After on 429/503 with bounded exponential backoff (3 tries).
    - Sleeps `sleep_seconds` after a successful download to avoid hammering
      CDC FTP (plan risk #2).
    - No ICD-10-specific logic; reusable for ICD-9, GEMs, HCPCS, etc.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src import config

USER_AGENT: str = "kg-cost/0.1 (research; +https://github.com/samersalman/kg-cost)"
_CHUNK_SIZE: int = 64 * 1024  # 64 KB
_MAX_RETRIES: int = 3
_RETRY_STATUS: tuple[int, ...] = (429, 503)


def make_session() -> requests.Session:
    """Return a Session with retry adapter and a research User-Agent."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def _parse_retry_after(value: str | None) -> float:
    """Best-effort parse of a Retry-After header. Returns 0.0 on failure."""
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        # HTTP-date form is rare for CDC/CMS; ignore and fall back to backoff.
        return 0.0


def download(
    url: str,
    dest: Path,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
    sleep_seconds: float = 1.0,
) -> Path:
    """Stream a single file to `dest` idempotently.

    If `dest` already exists with non-zero size, returns it without re-downloading
    (bronze is write-once per CLAUDE.md). Creates parent dirs as needed.

    Retries 3 times on 429/503 with exponential backoff (1s, 2s, 4s base) plus
    any Retry-After hint from the server.

    Sleeps `sleep_seconds` after a successful download to rate-limit against
    CDC FTP.

    Raises:
        requests.HTTPError: on non-retryable HTTP errors or after exhausting
            retries. The URL is included in the message.
    """
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[_http] skipping existing {dest}", file=sys.stderr)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)

    sess = session or make_session()
    last_exc: requests.HTTPError | None = None

    for attempt in range(_MAX_RETRIES):
        try:
            resp = sess.get(url, stream=True, timeout=timeout)
            if resp.status_code in _RETRY_STATUS:
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                backoff = (2**attempt) + retry_after  # 1s, 2s, 4s base
                resp.close()
                if attempt == _MAX_RETRIES - 1:
                    # Final attempt: raise via raise_for_status path below.
                    resp.raise_for_status()
                print(
                    f"[_http] {resp.status_code} for {url}; "
                    f"sleeping {backoff:.1f}s (attempt {attempt + 1}/{_MAX_RETRIES})",
                    file=sys.stderr,
                )
                time.sleep(backoff)
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                # Embed URL in the message for caller diagnostics. Caller wraps
                # with FY context.
                raise requests.HTTPError(
                    f"{exc} (url={url})", response=resp, request=exc.request
                ) from exc

            bytes_written = 0
            with dest.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
                    if chunk:
                        fh.write(chunk)
                        bytes_written += len(chunk)
            resp.close()

            print(
                f"[_http] downloaded {bytes_written} bytes to {dest}",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
            return dest

        except requests.HTTPError as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES - 1:
                break
            backoff = 2**attempt
            print(
                f"[_http] HTTPError for {url}: {exc}; "
                f"sleeping {backoff}s (attempt {attempt + 1}/{_MAX_RETRIES})",
                file=sys.stderr,
            )
            time.sleep(backoff)

    assert last_exc is not None  # loop guarantees this on fall-through
    raise last_exc


def download_to_bronze(
    url: str,
    source: str,
    fy: int,
    filename: str,
    *,
    download_date: str | None = None,
    session: requests.Session | None = None,
    period: str = "fy",
) -> Path:
    """Download `url` into the bronze layout for (`source`, `fy`).

    Composes the destination as
    ``config.bronze_dir(source, fy, download_date, period=period) / filename``.
    Idempotent — see `download()`.

    Parameters
    ----------
    period:
        Period prefix passed through to ``config.bronze_dir`` — ``"fy"``
        (default) for fiscal-year sources or ``"cy"`` for calendar-year
        sources (HCPCS Level II).
    """
    dest = config.bronze_dir(source, fy, download_date, period=period) / filename
    return download(url, dest, session=session)


def head_etag(
    url: str,
    session: requests.Session | None = None,
    timeout: int = 30,
) -> str | None:
    """Return the upstream `ETag` header, or None if unavailable.

    Used by extractors to skip re-download when the upstream file hasn't changed
    across re-pulls. Returns None if HEAD is not supported, the header is absent,
    or the request fails — callers should treat None as "cannot determine,
    proceed with download".
    """
    sess = session or make_session()
    try:
        resp = sess.head(url, timeout=timeout, allow_redirects=True)
    except requests.RequestException:
        return None
    if not resp.ok:
        return None
    etag = resp.headers.get("ETag")
    return etag if etag else None
