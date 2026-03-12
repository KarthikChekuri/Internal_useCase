## Why

The current V2 architecture uses Azure AI Search for candidate retrieval but relies on Python-side regex, normalized substring matching, and rapidfuzz for per-field PII detection. This means the system must read files from disk at search time and maintain a three-tier detection engine in Python. The V3 alternate route explores whether Azure AI Search alone — using Lucene queries, hit highlighting, PII Detection cognitive skill, and metadata filters — can replace the Python-side detection pipeline entirely. This provides a simpler architecture with fewer moving parts and no disk dependency at search time, at the cost of some detection granularity.

This is an **alternate route** alongside V2, not a replacement. Both approaches run on the same simulated data so results can be compared side by side.

## What Changes

- Create a new Azure AI Search index (`breach-file-index-v3`) with PII metadata fields (`has_ssn`, `has_name`, `has_dob`, `pii_types`) populated during indexing
- Add PII Detection cognitive skill to the indexing pipeline to auto-tag PII entity types per document
- Implement per-field Lucene query search: instead of one broad search + local detection, run separate targeted queries per PII field (SSN, Name, DOB, etc.) with hit highlighting for snippet extraction
- Create V3 search service that determines field matches based on query results (hit = found) and uses `@search.score` as confidence proxy
- Create V3 batch router under `/v3/batch/` prefix so both approaches coexist in the same FastAPI app
- No changes to existing V2 code — all V3 code is additive (new files only)

## Capabilities

### New Capabilities
- `indexing-v3`: Index with PII Detection cognitive skill, populate PII metadata fields for pre-filtering
- `search-v3`: Per-field Lucene queries with hit highlighting — Azure AI Search handles matching, snippets, and scoring
- `batch-v3`: Alternate batch processing route using V3 search (no disk reads, no regex, no rapidfuzz)

### Modified Capabilities
- None — V2 is untouched

## Impact

- **New API endpoints**: `POST /v3/batch/run`, `GET /v3/batch/{id}/status`, `GET /v3/batch/{id}/results`
- **New Azure AI Search index**: `breach-file-index-v3` with PII metadata fields and cognitive skill enrichment
- **Azure AI Language dependency**: PII Detection cognitive skill requires a billable Azure AI Language resource (free tier: 20 docs/day/indexer)
- **No new Python dependencies**: Uses only `azure-search-documents` (already installed)
- **No changes to**: SQL Server schema, existing V2 routes, existing V2 services, simulated data, test suite

## Delta from V2

| Aspect | V2 (current) | V3 (alternate) |
|---|---|---|
| File content source | Read from disk | Read from Azure AI Search `content` field |
| PII detection | Python regex + normalized + rapidfuzz | Azure AI Search Lucene queries per field |
| Snippet extraction | Python regex context window | Azure AI Search hit highlighting |
| Confidence scoring | 4 formulas (SSN+Name, SSN-only, Name-only, No-anchor) | `@search.score` normalized per field |
| PII type pre-filtering | None | `$filter=has_ssn eq true` on metadata fields |
| Fuzzy name matching | rapidfuzz token_set_ratio (threshold 75) | Lucene `~1`/`~2` + phonetic analyzer |
| Detection granularity | Per-field: exact/normalized/fuzzy with confidence tiers | Per-field: found/not-found with search score |
| Disk dependency at search time | Yes (read file for detection) | No |
| API calls per customer | 1 (multi-strategy union) + disk reads | 13 (one per PII field) |
| Index-time cost | None (push only) | PII Detection cognitive skill (Azure AI Language billing) |
