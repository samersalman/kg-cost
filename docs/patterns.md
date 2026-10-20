# Cross-cutting extractor patterns

Reusable patterns across CMS / federal data extractors. Loaded when triaging
URL-resolution gaps or replication-package work in any phase.

## Patterns

- [Pattern] NBER mirror is a tier-1 source for retired CMS PUFs, NOT a tier-3 fallback. Claude Code repeatedly declared NBER-mirrored PUFs "unrecoverable" in Sessions 8 + Session 8.5 first pass because it only checked cms.gov canonical paths. URL-resolution ladder for ALL future PUF / charge-data extractors: cms.gov canonical → NBER mirror → Wayback CDX → log+skip. NBER MUST be probed BEFORE giving up. Shared helper: `src/extract/_url_resolver.py:resolve_with_ladder(cms_url, nber_url=None, wayback_url=None)`. Documented for non-PUF extractors (HCPCS pre-CY2017, OPPS/PFS pre-2017) — applied lazily only when those extractors are next touched (out of Session 8.5 scope).
- [Pattern] Socrata dataset IDs are stable replication-package seeds for data.cms.gov; the per-release UUIDs in URL paths (`sites/default/files/{YYYY-MM}/{uuid}/...`) rotate every wave. Hardcoded UUID URLs in `_PUF_*_SOURCES` capture a frozen snapshot for this analytic build (one-pass paradigm, PI verdict on DEV 3). Dataset IDs documented in `src/config.py` Phase 5 comment block as replication metadata: `puf_inpatient = 690ddc6c-2767-4618-b277-420ffb2bf27c`; `puf_outpatient = ccbc9a44-40d4-46b4-a709-5caa59212e50`; `puf_physician = 92396110-2aed-4d63-a6a2-5d6207d46a29`; `puf_inpatient_summary = 2941ab09-8cee-49d8-9703-f3c5b854e388`. Re-resolve via data.cms.gov data.json catalog at manuscript-supplementary-materials freeze. NOT consumed at extract time — runtime Socrata Discovery API resolver intentionally deferred (one-pass paradigm).

## Resolved questions

None.
