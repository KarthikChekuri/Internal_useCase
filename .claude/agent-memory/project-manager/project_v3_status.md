---
name: V3 Implementation Status
description: V3 Azure-only alternate route - phase structure, batch dependencies, and current status
type: project
---

## V3 Implementation Plan (Created 2026-03-12)

V3 is an Azure AI Search-only alternate approach running alongside V2. 9 phases across 5 batches.

### Phase Structure
| Phase | Goal | Batch | Effort | Depends On |
|-------|------|-------|--------|------------|
| V3-1.1 | Config Setting | 1 | S | None |
| V3-1.2 | Pydantic Schemas | 1 | S | None |
| V3-1.3 | Index Script | 1 | S | None |
| V3-2.1 | Indexing Service + PII Detection | 2 | M | V3-1.1, V3-1.3 |
| V3-2.2 | Search Query Builder + Field Execution | 2 | M | V3-1.1 |
| V3-3.1 | Search Merge + Confidence | 3 | M | V3-2.2 |
| V3-3.2 | Batch Service | 3 | M | V3-3.1, V3-1.2 |
| V3-4.1 | FastAPI Routes | 4 | M | V3-3.2, V3-1.2, V3-1.1 |
| V3-5.1 | Integration + Comparison | 5 | M | V3-4.1 |

### Key Design Decisions
- Additive only -- no V2 files modified
- Per-field Lucene queries (one per PII field) instead of multi-strategy broad search
- PII Detection API enrichment during indexing (best-effort, graceful fallback)
- Separate Azure AI Search index: breach-file-index-v3
- Results stored in same [Search].[results] table with strategy_name="v3_azure_only"
- V3 specs at: openspec/changes/v3-azure-only/specs/

### Max Parallelism
- Batch 1: 3 parallel agents (V3-1.1, V3-1.2, V3-1.3)
- Batch 2: 2 parallel agents (V3-2.1, V3-2.2) -- BUT V3-2.1 needs both V3-1.1 AND V3-1.3
- Batch 3: sequential (V3-3.1 must complete before V3-3.2 can start)
- Batch 4: single agent (V3-4.1)
- Batch 5: single agent (V3-5.1)
