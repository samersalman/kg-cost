"""Shared URL-ladder helper for Phase 5 PUF extractors.

Lifted verbatim from ``puf_outpatient._resolve_outpatient_url`` per
plan-v11 Session 8.5 resolution. Order of precedence:

    cms.gov canonical -> NBER mirror -> Wayback CDX -> None

A truthy first hit wins. Returns ``(url, source_host)`` where
``source_host`` is one of ``{"cms_gov", "nber", "wayback"}``. Returns
``None`` if every tier misses (caller is expected to log+skip).

No HTTP probing is performed -- each layer is pre-verified by the
extractor author via ``curl HEAD`` on the discovery date and codified
per-cell in the extractor's module-local URL dicts. This helper is a
pure precedence selector; it does not validate, fetch, or fall back on
HTTP errors at runtime.

This module deliberately imports nothing from ``src.extract.*`` to
avoid an import cycle when ``puf_outpatient`` (and the other PUF
extractors) import this helper.
"""

from __future__ import annotations

VALID_SOURCE_HOSTS: frozenset[str] = frozenset({"cms_gov", "nber", "wayback"})


def resolve_with_ladder(
    cms_url: str | None,
    nber_url: str | None = None,
    wayback_url: str | None = None,
) -> tuple[str, str] | None:
    """Try cms.gov -> NBER -> Wayback in order. Truthy first hit wins.

    Returns ``(url, source_host)`` for the first non-empty layer, or
    ``None`` when every layer is missing. ``source_host`` is one of the
    members of ``VALID_SOURCE_HOSTS``.
    """
    if cms_url:
        return (cms_url, "cms_gov")
    if nber_url:
        return (nber_url, "nber")
    if wayback_url:
        return (wayback_url, "wayback")
    return None
