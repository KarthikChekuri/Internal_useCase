# Breach Data PII Search — V3 System Design Document (Azure AI Search Only)

> **Status**: Draft — Under PM Review
> **Version**: 3.0
> **Last Updated**: 2026-03-12
> **Alternate to**: V2 (Python regex + fuzzy detection)
> **Branch**: `v3-ai-search`

---

## Table of Contents

1. [What Is V3?](#1-what-is-v3)
2. [V2 vs V3 — The Core Difference](#2-v2-vs-v3--the-core-difference)
3. [Why Explore This?](#3-why-explore-this)
4. [V3 Pipeline Overview](#4-v3-pipeline-overview)
5. [Phase 1: Indexing with PII Detection](#5-phase-1-indexing-with-pii-detection)
6. [Phase 2: Per-Field Lucene Query Search](#6-phase-2-per-field-lucene-query-search)
7. [Hit Highlighting for Snippets](#7-hit-highlighting-for-snippets)
8. [Metadata Pre-Filtering](#8-metadata-pre-filtering)
9. [V3 Confidence Scoring](#9-v3-confidence-scoring)
10. [Batch Processing](#10-batch-processing)
11. [Results & Output](#11-results--output)
12. [API Endpoints](#12-api-endpoints)
13. [V3 Index Schema](#13-v3-index-schema)
14. [Project Structure (Additive Only)](#14-project-structure-additive-only)
15. [What V3 Can and Cannot Do](#15-what-v3-can-and-cannot-do)
16. [Side-by-Side Comparison Plan](#16-side-by-side-comparison-plan)
17. [Key Decisions & Rationale](#17-key-decisions--rationale)
18. [Architecture Diagram](#18-architecture-diagram)
19. [Risks & Limitations](#19-risks--limitations)
20. [Open Questions](#20-open-questions)

---

## 1. What Is V3?

V3 is an **alternate approach** to PII detection that uses **only Azure AI Search capabilities** — no Python regex, no rapidfuzz, no disk reads at search time. It runs alongside V2 in the same app so results can be compared on the same data.

**V2 approach**: Azure AI Search finds candidates → Python reads files from disk → Python regex/fuzzy detects PII per field

**V3 approach**: Azure AI Search finds candidates → Azure AI Search tells us which fields matched → Azure AI Search provides snippets

Everything happens inside Azure AI Search. Python just orchestrates the queries and stores the results.

---

## 2. V2 vs V3 — The Core Difference

The best way to understand V3 is to see how it differs from V2 at each step:

### V2: One broad search + local detection

```
Customer: Karthik Chekuri, SSN 343-43-4343

Step 1: Build ONE Lucene query from strategy fields
        → "Karthik~1 Chekuri~1" OR "343-43-4343" OR "343434343"

Step 2: Send to Azure AI Search → get 27 candidate files

Step 3: For EACH candidate file:
        → Read file from disk
        → Run regex for SSN (Tier 1)
        → Run normalized match for name (Tier 2)
        → Run rapidfuzz for misspelled names (Tier 3)
        → Check all 13 PII fields
        → Compute confidence

Result: Per-field detection with method (exact/normalized/fuzzy) and confidence tiers
```

### V3: Separate queries per field, Azure does everything

```
Customer: Karthik Chekuri, SSN 343-43-4343

Step 1: Build SEPARATE Lucene query per PII field
        → Query 1 (SSN):  "343-43-4343" OR "343434343"
        → Query 2 (Name): Karthik~1 Chekuri~1
        → Query 3 (DOB):  "07/15/1992" OR "1992-07-15" OR ...
        → Query 4 (City): "Houston"
        → ... (one per non-null PII field)

Step 2: Send each query to Azure AI Search with hit highlighting
        → Each returns: which files matched + search score + highlighted snippet

Step 3: Merge results across queries per document
        → File abc123: SSN ✓ (score 12.5, snippet), Name ✓ (score 8.3, snippet), DOB ✗
        → Compute confidence from search scores

Result: Per-field found/not-found with search score and snippet from highlighting
```

### The trade-off in one sentence

V2 gives you **"SSN was found via exact regex with confidence 1.0"** — V3 gives you **"the SSN query matched this document with search score 12.5"**.

V3 is simpler but less granular. V2 is more precise but requires disk access and Python detection logic.

---

## 3. Why Explore This?

### Simpler architecture
- No `leak_detection_service.py` (300+ lines of regex)
- No `fuzzy.py` (sliding window logic)
- No `confidence.py` (4 formulas)
- No `text_extraction.py` dependency at search time
- No disk reads at search time

### Fewer moving parts
```
V2 depends on:
  Azure AI Search + Disk (file system) + Python regex + rapidfuzz + text extraction

V3 depends on:
  Azure AI Search only
```

### The question we're answering

> Can Azure AI Search's Lucene matching + hit highlighting deliver comparable PII detection accuracy to our custom Python regex + fuzzy pipeline?

Running both on the same 10 customers and 25 files answers this empirically.

---

## 4. V3 Pipeline Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                      V3 PIPELINE                                  │
│                                                                    │
│   Phase 1: INDEX (with PII Detection)                              │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │ Files → Extract text → PII Detection API → Tag metadata    │   │
│   │ → Push to breach-file-index-v3                             │   │
│   │                                                            │   │
│   │ Each doc gets: content + has_ssn + has_name + has_dob +    │   │
│   │                pii_types + pii_entity_count                │   │
│   └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│   Phase 2: SEARCH (per-field queries)                              │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │ For each customer:                                         │   │
│   │   For each PII field (SSN, Name, DOB, ...):                │   │
│   │     1. Build Lucene query for this field                   │   │
│   │     2. Pre-filter: $filter=has_ssn eq true (if applicable) │   │
│   │     3. Send query with hit highlighting                    │   │
│   │     4. Collect: matched docs, scores, snippets             │   │
│   │                                                            │   │
│   │   Merge results per document across all field queries      │   │
│   │   Compute confidence from normalized search scores         │   │
│   │   Persist to [Search].[results]                            │   │
│   └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│   Output: customer_id | md5 | leaked_fields | confidence           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. Phase 1: Indexing with PII Detection

### What's the same as V2

- Read file metadata from DLU table (MD5, file_path)
- Extract text using the same `extract_text()` function (.txt, .xlsx, .csv, .xls)
- Same three content fields with same analyzers (content, content_phonetic, content_lowercase)
- Same scoring profile (pii_boost)
- Same error handling (skip unsupported, log failures, continue)

### What's different from V2

Before pushing each document to the index, V3 calls the **Azure AI Language PII Detection API** on the extracted text. This auto-detects what PII types are in the file.

### How PII Detection works

```
Extracted text: "Employee Karthik Chekuri, SSN 343-43-4343, hired 07/15/1992"
                                    │
                    Azure AI Language PII API
                                    │
                                    ▼
Detected entities:
  [
    { "text": "Karthik Chekuri",  "type": "Person",                          "score": 0.95 },
    { "text": "343-43-4343",      "type": "USSocialSecurityNumber",          "score": 0.85 },
    { "text": "07/15/1992",       "type": "DateTime",                        "score": 0.90 }
  ]
```

### How entities map to metadata fields

The indexing code derives boolean flags and a type list from the detected entities:

| Entity type contains | Sets field |
|---|---|
| `"SocialSecurity"` | `has_ssn = true` |
| `"Person"` | `has_name = true` |
| `"DateTime"` or `"Date"` | `has_dob = true` |
| `"Address"` | `has_address = true` |
| `"PhoneNumber"` | `has_phone = true` |
| (all types) | `pii_types = ["Person", "USSocialSecurityNumber", "DateTime"]` |
| (count) | `pii_entity_count = 3` |

### The document pushed to the V3 index

```json
{
  "id": "c8578af0e239aaeb7e4030b346430ac3",
  "md5": "c8578af0e239aaeb7e4030b346430ac3",
  "file_path": "data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt",
  "content": "Employee Karthik Chekuri, SSN 343-43-4343, hired 07/15/1992...",
  "content_phonetic": "Employee Karthik Chekuri, SSN 343-43-4343, hired 07/15/1992...",
  "content_lowercase": "Employee Karthik Chekuri, SSN 343-43-4343, hired 07/15/1992...",
  "has_ssn": true,
  "has_name": true,
  "has_dob": true,
  "has_address": false,
  "has_phone": false,
  "pii_types": ["Person", "USSocialSecurityNumber", "DateTime"],
  "pii_entity_count": 3
}
```

### PII Detection is best-effort

If the Azure AI Language API is unreachable or returns an error, the document is still indexed — just with all `has_*` fields set to `false` and `pii_types` as empty. The content is still searchable. The metadata is a bonus for pre-filtering, not a requirement.

### Cost

- Free tier: 20 documents per indexer per day
- Our simulated data: 25 files → fits within free tier (run over 2 days, or use a paid tier)
- Production: requires a billable Azure AI Language resource

---

## 6. Phase 2: Per-Field Lucene Query Search

### The Big Idea

Instead of one broad search followed by local Python detection, V3 lets **Azure AI Search do the detection**. For each customer, we send a separate, targeted Lucene query per PII field. If a query returns results for a document, that field is "found" in that document.

### Query Construction Per Field Type

| PII Field | Lucene Query | search_mode | Why |
|---|---|---|---|
| **SSN** | `"343-43-4343" OR "343434343"` | `all` | Exact match — dashed and undashed |
| **Fullname** | `Karthik~1 Chekuri~1` | `any` | Fuzzy — each token allows 1 edit |
| **FirstName** | `Karthik~1` | `any` | Fuzzy — single token |
| **LastName** | `Chekuri~1` | `any` | Fuzzy — single token |
| **DOB** | `"07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"` | `all` | All 4 date formats |
| **ZipCode** | `"77001"` | `all` | Exact quoted |
| **DriversLicense** | `"TX12345678"` | `all` | Exact quoted |
| **State** | `"TX"` | `all` | Exact quoted |
| **City** | `"Houston"` | `all` | Exact phrase |
| **Address1** | `"123 Main Street"` | `all` | Exact phrase |
| **Address2** | (if not null) | `all` | Exact phrase |
| **Address3** | (if not null) | `all` | Exact phrase |
| **Country** | `"United States"` | `all` | Exact phrase |

### Query Parameters (same for all field queries)

```python
results = search_client.search(
    search_text=field_query,          # The Lucene query for this field
    query_type="full",                # Enable Lucene syntax
    search_mode="all" or "any",       # Depends on field type (see table)
    search_fields="content,content_phonetic,content_lowercase",
    scoring_profile="pii_boost",
    highlight_fields="content",       # Hit highlighting for snippets
    highlight_pre_tag="[[MATCH]]",
    highlight_post_tag="[[/MATCH]]",
    filter=metadata_filter,           # e.g., "has_ssn eq true" (optional)
    top=100,
)
```

### How Many Queries Per Customer?

A customer with all 13 PII fields populated → **13 queries**.
A customer with 3 null fields → **10 queries**.

For 10 customers: 10 × ~13 = ~130 API calls (Azure handles this in seconds).
For 200 customers: 200 × ~13 = ~2,600 API calls (still fast, Azure AI Search scales easily).

### Result from Each Query

For each per-field query, Azure AI Search returns:
- **Which documents matched** (by MD5)
- **Search score** (`@search.score`) — how relevant the match is
- **Highlighted snippet** (`@search.highlights`) — the matched text in context

### Merging Results Across Field Queries

After all per-field queries complete for a customer, V3 merges results:

```
SSN query  → matched: [doc_A (12.5), doc_B (10.0)]
Name query → matched: [doc_A (8.3), doc_C (6.1)]
DOB query  → matched: [doc_A (9.0), doc_D (7.2)]
City query → matched: [doc_A (3.1), doc_B (2.5)]

Merged result per document:
  doc_A: SSN ✓ (12.5), Name ✓ (8.3), DOB ✓ (9.0), City ✓ (3.1) → high confidence
  doc_B: SSN ✓ (10.0), City ✓ (2.5) → medium confidence
  doc_C: Name ✓ (6.1) → lower confidence
  doc_D: DOB ✓ (7.2) → lower confidence, needs_review
```

---

## 7. Hit Highlighting for Snippets

### What it is

Azure AI Search can return the **matched text with surrounding context**, wrapped in custom tags. This is called hit highlighting.

### How V3 uses it

Instead of V2's Python code that extracts ~100 chars around a regex match, V3 gets snippets directly from Azure:

```
Query: "343-43-4343"
Result highlight: "Employee SSN: [[MATCH]]343-43-4343[[/MATCH]] effective date 01/15/2024"
```

### Configuration

```python
highlight_fields="content",
highlight_pre_tag="[[MATCH]]",
highlight_post_tag="[[/MATCH]]",
```

We use `[[MATCH]]` tags instead of HTML `<em>` tags since results are JSON, not rendered HTML.

### Limitation: Fuzzy queries and highlighting

Azure AI Search has **limited highlighting support for fuzzy queries** (`~1`, `~2`). For exact queries (SSN, DOB, Zip), highlights work reliably. For fuzzy name queries, highlights may be missing.

When no highlight is returned, V3 sets the snippet to `null`. The field is still marked as "found" (the query did match), we just don't have a text snippet to show.

```json
{
  "SSN": { "found": true, "score": 12.5, "snippet": "...SSN: [[MATCH]]343-43-4343[[/MATCH]]..." },
  "Fullname": { "found": true, "score": 8.3, "snippet": null }
}
```

---

## 8. Metadata Pre-Filtering

### What it is

During indexing, the PII Detection skill tags each document with what types of PII it contains (`has_ssn`, `has_name`, etc.). At search time, V3 uses these tags to **pre-filter** documents before running the Lucene query.

### Why it helps

Without pre-filtering:
- SSN query `"343-43-4343"` searches all 25 documents
- Most don't contain any SSN at all → wasted work

With pre-filtering:
- `$filter=has_ssn eq true` narrows to, say, 8 documents that the PII skill tagged as containing SSNs
- SSN query only searches those 8 → faster, fewer false positives

### Filter mapping

| Per-field query | Filter expression |
|---|---|
| SSN | `has_ssn eq true` |
| Fullname, FirstName, LastName | `has_name eq true` |
| DOB | `has_dob eq true` |
| Address1/2/3 | `has_address eq true` |
| City, State, ZipCode, Country | No filter (no dedicated metadata field) |
| DriversLicense | No filter |

### Pre-filtering is optional

If the PII Detection skill didn't run (API unavailable), all `has_*` fields are `false`. In that case, V3 skips the pre-filter and searches all documents. The Lucene query still works — it's just not pre-filtered.

---

## 9. V3 Confidence Scoring

### The difference from V2

V2 has four detailed formulas using per-field match methods (exact=1.0, normalized=0.95, fuzzy=ratio/100).

V3 doesn't know the match method — it only knows "the query matched" and the search score. So V3 uses a simpler model based on **normalized search scores**.

### Per-field confidence

```
field_confidence = min(1.0, field_search_score / max_search_score)
```

Where `max_search_score` is the highest `@search.score` seen across ALL per-field queries for this customer. This normalizes all scores to a 0.0–1.0 range.

**Why this works**: Exact matches (SSN, DOB) produce higher search scores in Azure AI Search than fuzzy matches (names). So exact matches naturally get higher confidence, and fuzzy matches get lower confidence — similar to V2's tiers, but derived from search behavior rather than explicit rules.

### Overall confidence per document

```
SSN_conf  = SSN field confidence (or 0 if not found)
Name_conf = max(Fullname_conf, FirstName_conf, LastName_conf) (or 0)
Other_avg = average confidence of other found fields (DOB, Zip, DL, Address, City, State, Country)

Overall = 0.35 × SSN_conf + 0.30 × Name_conf + 0.20 × Other_avg + 0.15 × 0
```

The last term (document-level search score) is 0 in V3 because V3 doesn't do a broad search query — it only does per-field queries. The 0.15 weight effectively gets redistributed.

### needs_review flag

Set to `true` when:
- Overall confidence < 0.5
- OR only FirstName matched (without Fullname, LastName, or SSN)

---

## 10. Batch Processing

### Same model as V2

V3 batch processing follows the same pattern as V2:
- Create batch run with unique batch_id
- Process customers sequentially in customer_id order
- Update customer status (pending → searching → complete/failed)
- Persist results to `[Search].[results]` table
- Resumable on crash

### What's different

| Aspect | V2 batch | V3 batch |
|---|---|---|
| Strategy set | `["fullname_ssn", "lastname_dob", "unique_identifiers"]` | `["v3_azure_only"]` |
| Search per customer | 3 queries (one per strategy) | ~13 queries (one per PII field) |
| Detection per customer | Read files from disk, run regex/fuzzy | None — detection IS the search |
| strategy_name in results | `"fullname_ssn"`, `"lastname_dob"`, etc. | `"v3_azure_only"` (constant) |
| Console log prefix | None | `[V3]` prefix |

### Customer status transitions

Same as V2:
```
pending → searching → complete
                   → failed (on error)
```

In V3, "searching" means "sending per-field queries." There is no separate "detecting" phase because detection is built into the search.

---

## 11. Results & Output

### Same table as V2

V3 results go into the same `[Search].[results]` table. The `strategy_name` column distinguishes V2 from V3 results:

- V2 rows: `strategy_name = "fullname_ssn"`, `"lastname_dob"`, etc.
- V3 rows: `strategy_name = "v3_azure_only"`

### Example V3 result row

```json
{
  "batch_id": "v3-batch-id-...",
  "customer_id": 1,
  "md5": "c8578af0e239aaeb7e4030b346430ac3",
  "strategy_name": "v3_azure_only",
  "leaked_fields": ["SSN", "Fullname", "DOB"],
  "match_details": {
    "SSN":       { "found": true,  "score": 0.92, "snippet": "...SSN: [[MATCH]]343-43-4343[[/MATCH]]..." },
    "Fullname":  { "found": true,  "score": 0.67, "snippet": null },
    "FirstName": { "found": true,  "score": 0.55, "snippet": "...patient [[MATCH]]Karthik[[/MATCH]]..." },
    "LastName":  { "found": true,  "score": 0.61, "snippet": "...[[MATCH]]Chekuri[[/MATCH]], Karthik..." },
    "DOB":       { "found": true,  "score": 0.72, "snippet": "...DOB: [[MATCH]]07/15/1992[[/MATCH]]..." },
    "DriversLicense": { "found": false },
    "Address1":  { "found": false },
    "ZipCode":   { "found": false },
    "City":      { "found": false },
    "State":     { "found": false },
    "Country":   { "found": false }
  },
  "overall_confidence": 0.68,
  "azure_search_score": 12.5,
  "needs_review": false,
  "searched_at": "2026-03-12T14:30:00Z"
}
```

### Key differences from V2 result

| Field | V2 value | V3 value |
|---|---|---|
| `strategy_name` | `"fullname_ssn"` | `"v3_azure_only"` |
| `match_details.SSN.method` | `"exact_regex"` | Not present (V3 doesn't know the method) |
| `match_details.SSN.confidence` | `1.0` (exact) | `0.92` (normalized search score) |
| `match_details.Fullname.snippet` | `"...john smith indicate..."` | `null` (fuzzy highlight not available) |

---

## 12. API Endpoints

### V3 routes (all under `/v3` prefix)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v3/index/all` | Index all files to V3 index (with PII detection) |
| `POST` | `/v3/batch/run` | Start V3 batch run |
| `GET` | `/v3/batch/{id}/status` | V3 batch progress |
| `GET` | `/v3/batch/{id}/results` | V3 batch results |

### V2 routes (unchanged)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/index/all` | Index all files to V2 index |
| `POST` | `/batch/run` | Start V2 batch run |
| `GET` | `/batch/{id}/status` | V2 batch progress |
| `GET` | `/batch/{id}/results` | V2 batch results |

Both sets of routes work independently. Starting a V3 batch doesn't affect V2 and vice versa.

---

## 13. V3 Index Schema

### Index name: `breach-file-index-v3`

### Fields

| Field | Type | Searchable | Filterable | Analyzer |
|---|---|---|---|---|
| `id` | String (key) | No | Yes | — |
| `md5` | String | No | Yes | — |
| `file_path` | String | No | No | — |
| `content` | String | Yes | No | `standard.lucene` |
| `content_phonetic` | String | Yes | No | `phonetic_analyzer` |
| `content_lowercase` | String | Yes | No | `name_analyzer` |
| `has_ssn` | Boolean | No | Yes | — |
| `has_name` | Boolean | No | Yes | — |
| `has_dob` | Boolean | No | Yes | — |
| `has_address` | Boolean | No | Yes | — |
| `has_phone` | Boolean | No | Yes | — |
| `pii_types` | Collection(String) | No | Yes | — |
| `pii_entity_count` | Int32 | No | Yes | — |

### Custom analyzers (same as V2)

**`phonetic_analyzer`**: standard tokenizer → lowercase → asciifolding → Double Metaphone (`replace: false`)

**`name_analyzer`**: standard tokenizer → lowercase → asciifolding

### Scoring profile (same as V2)

`pii_boost`: content=3.0, content_lowercase=2.0, content_phonetic=1.5

---

## 14. Project Structure (Additive Only)

V3 adds new files only. **No existing V2 files are modified.**

```
breach-search/
├── app/
│   ├── services/
│   │   ├── search_service.py           # V2 (untouched)
│   │   ├── search_service_v3.py        # NEW — per-field Lucene queries
│   │   ├── batch_service.py            # V2 (untouched)
│   │   ├── batch_service_v3.py         # NEW — V3 batch orchestration
│   │   ├── indexing_service.py         # V2 (untouched)
│   │   ├── indexing_service_v3.py      # NEW — index with PII detection
│   │   ├── leak_detection_service.py   # V2 (untouched, NOT used by V3)
│   │   └── text_extraction.py          # Shared by V2 and V3
│   ├── routers/
│   │   ├── batch.py                    # V2 (untouched)
│   │   └── batch_v3.py                 # NEW — /v3/batch/* routes
│   ├── schemas/
│   │   ├── batch.py                    # V2 (untouched)
│   │   └── search_v3.py               # NEW — V3 response models
│   └── utils/
│       ├── fuzzy.py                    # V2 (untouched, NOT used by V3)
│       └── confidence.py              # V2 (untouched, NOT used by V3)
├── scripts/
│   ├── create_search_index.py          # V2 (untouched)
│   ├── create_search_index_v3.py       # NEW — V3 index + PII metadata
│   ├── run_indexing.py                 # V2 (untouched)
│   └── run_indexing_v3.py              # NEW — V3 indexing with PII detection
├── tests/
│   └── services/
│       ├── test_search_service_v3.py   # NEW
│       └── test_batch_service_v3.py    # NEW
└── V3_DESIGN.md                        # This file
```

### Configuration

New environment variable:
```
AZURE_SEARCH_INDEX_V3=breach-file-index-v3
```

Added to `app/config.py` settings. V2 continues using `AZURE_SEARCH_INDEX`, V3 uses `AZURE_SEARCH_INDEX_V3`.

---

## 15. What V3 Can and Cannot Do

### What V3 does well

| Capability | How |
|---|---|
| Find exact SSN in a file | Lucene exact query `"343-43-4343"` — same accuracy as regex |
| Find exact DOB in multiple formats | Lucene OR query with all 4 date formats |
| Find fuzzy name matches | Lucene `~1` fuzzy + phonetic analyzer |
| Extract snippets | Hit highlighting — Azure returns matched text in context |
| Pre-filter candidates | PII metadata (`has_ssn eq true`) narrows search before query |
| No disk dependency | All content is in the Azure AI Search index |
| Simple codebase | No regex engine, no fuzzy library, no detection tiers |

### What V3 cannot do (that V2 can)

| V2 capability | Why V3 can't |
|---|---|
| Distinguish exact vs normalized vs fuzzy match | V3 only knows "query matched" — not how |
| SSN last-4 partial matching with word boundaries | Lucene `"4343"` matches anywhere, not just word boundaries |
| Tier-based confidence (1.0/0.95/ratio) | V3 uses search score, not match method |
| Fullname as complete substring | Lucene tokenizes "John Smith" into two tokens — can't do substring |
| State word-boundary matching (`\bCA\b`) | Lucene may match "CA" inside "CABLE" |
| Disambiguation rule (FirstName + SSN = 0.70) | V3 has no equivalent logic |
| Guaranteed snippet for every match | Fuzzy query highlights are unreliable |

### Severity assessment

| Lost capability | Impact |
|---|---|
| Match method distinction | **Low** — for comparison purposes, found/not-found is sufficient |
| SSN last-4 partial | **Medium** — V3 will have more false positives on partial SSN |
| State word boundary | **Low** — state is a weak identifier regardless |
| Fullname substring | **Medium** — V3 may miss "John Smith" embedded in longer text without spaces around it |
| Disambiguation rule | **Low** — V3 uses `needs_review` flag instead |

---

## 16. Side-by-Side Comparison Plan

### How to compare V2 and V3

1. Run V2 batch: `POST /batch/run` → produces results with V2 strategy names
2. Run V3 batch: `POST /v3/batch/run` → produces results with `v3_azure_only`
3. Query both: `GET /batch/{v2_id}/results` and `GET /v3/batch/{v3_id}/results`
4. Compare per customer:

```
Customer 1:
  V2 found files: [A, B, C, D]     V3 found files: [A, B, C, E]
  V2 missed: —                     V3 missed: D (SSN last-4 only)
  V3 extra: E (false positive)     V2 extra: D (partial SSN catch)

  V2 confidence for A: 0.92        V3 confidence for A: 0.68
  V2 fields for A: SSN(1.0), Name(0.95), DOB(1.0)
  V3 fields for A: SSN(0.92), Name(0.67), DOB(0.72)
```

### Comparison script (Phase 7.3)

A script that:
- Queries both V2 and V3 results for the same customers
- Outputs a table showing: files found by both, files found by V2 only, files found by V3 only
- Shows per-field match differences
- Highlights where confidence scores diverge significantly

---

## 17. Key Decisions & Rationale

### Decision 1: Separate index, not modify V2

**Why**: V3 needs metadata fields that V2 doesn't. Modifying the V2 index could break V2. A separate index keeps both approaches independent.

### Decision 2: PII Detection via API call, not skillset pipeline

**Why**: We use push-mode indexing (our code extracts text and uploads). Azure's built-in skillset pipeline requires pull-mode indexing (indexer pulls from a data source). Since we already extract text in Python, we call the PII API directly and map results ourselves.

### Decision 3: Per-field queries, not one big query

**Why**: The whole point of V3 is to let Azure AI Search tell us which fields matched. One big OR query returns documents but doesn't tell us whether it was the SSN or the name that matched. Separate queries per field give us per-field found/not-found.

### Decision 4: search_mode varies by field type

**Why**: For exact fields (SSN, DOB), we use `search_mode="all"` so all tokens must match. For name fields, we use `search_mode="any"` so partial name matches are allowed (first name only, last name only).

### Decision 5: Snippet tags are `[[MATCH]]` not `<em>`

**Why**: Results are consumed as JSON by the API, not rendered as HTML. `<em>` tags would need HTML escaping. `[[MATCH]]` is unambiguous in JSON strings and easy to parse.

### Decision 6: Same results table for V2 and V3

**Why**: Storing in the same table with different `strategy_name` values makes side-by-side queries trivial:
```sql
SELECT * FROM [Search].[results]
WHERE customer_id = 1
ORDER BY strategy_name, overall_confidence DESC
```

---

## 18. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        V3 ARCHITECTURE                                │
│                                                                        │
│  ┌─────────────┐                                                       │
│  │  FastAPI     │         ┌───────────────────────────────────────┐    │
│  │  /v3/batch/* │────────►│        Azure AI Search                │    │
│  │  /v3/index/* │         │        (breach-file-index-v3)         │    │
│  └──────┬──────┘         │                                       │    │
│         │                 │  ┌─────────────────────────────────┐  │    │
│         │                 │  │ content (standard.lucene)       │  │    │
│         │  Per-field      │  │ content_phonetic (dbl metaphone)│  │    │
│         │  Lucene queries │  │ content_lowercase (ascii fold)  │  │    │
│         │  + highlighting │  ├─────────────────────────────────┤  │    │
│         │                 │  │ has_ssn     (filterable)        │  │    │
│         │                 │  │ has_name    (filterable)        │  │    │
│         │                 │  │ has_dob     (filterable)        │  │    │
│         │                 │  │ has_address (filterable)        │  │    │
│         │                 │  │ pii_types   (filterable)        │  │    │
│         │                 │  └─────────────────────────────────┘  │    │
│         │                 │                                       │    │
│         │                 │  Scoring: pii_boost (3.0/2.0/1.5)    │    │
│         │                 │  Highlighting: [[MATCH]]...[[/MATCH]] │    │
│         │                 └───────────────────────────────────────┘    │
│         │                                                              │
│         │                 ┌───────────────────────────────────────┐    │
│         │                 │  Azure AI Language                     │    │
│         └────────────────►│  PII Detection API                    │    │
│           (index time     │  (called during indexing only)        │    │
│            only)          │  → entity types, scores, offsets      │    │
│                           └───────────────────────────────────────┘    │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  SQL Server (shared with V2)                                    │   │
│  │                                                                  │   │
│  │  [PII].[master_data]        ← customer PII (read only)          │   │
│  │  [Batch].[batch_runs]       ← V3 batches (strategy="v3_azure")  │   │
│  │  [Batch].[customer_status]  ← per-customer progress             │   │
│  │  [Search].[results]         ← V3 results (strategy="v3_azure")  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  NO disk reads at search time                                          │
│  NO Python regex / rapidfuzz                                           │
│  NO leak_detection_service.py                                          │
└──────────────────────────────────────────────────────────────────────┘
```

### V2 vs V3 — Where the work happens

```
V2:                                         V3:
  Azure AI Search ──→ Candidates              Azure AI Search ──→ Per-field matches
        │                                           │
        ▼                                           ▼
  Disk ──→ File text                          (no disk read)
        │
        ▼
  Python regex ──→ Tier 1 match               (no Python regex)
  Python normalized ──→ Tier 2 match          (no Python normalized)
  Python rapidfuzz ──→ Tier 3 match           (no Python rapidfuzz)
        │                                           │
        ▼                                           ▼
  4 confidence formulas                       Search score normalization
        │                                           │
        ▼                                           ▼
  [Search].[results]                          [Search].[results]
```

---

## 19. Risks & Limitations

### Risk 1: 32KB field limit (content truncation)

**What**: Azure AI Search caps string fields at 32,768 characters. Files larger than this get silently truncated.

**Impact**: PII in the truncated portion is invisible to V3. No error, no warning.

**For simulated data**: Not a problem (files are 500–3,000 chars).

**For production**: Would need chunking (out of scope for V3 prototype).

### Risk 2: False positives on short values

**What**: Lucene query `"TX"` for State or `"4343"` for SSN last-4 matches many documents that contain these strings in unrelated contexts.

**Impact**: More false positive matches than V2 (which uses `\bCA\b` word-boundary regex).

**Mitigation**: Metadata pre-filtering (`has_ssn eq true`) reduces but doesn't eliminate this.

### Risk 3: Fuzzy highlighting gaps

**What**: Azure AI Search has limited highlighting support for fuzzy queries (`~1`, `~2`). Snippets may be empty.

**Impact**: For exact fields (SSN, DOB), snippets work. For fuzzy name matches, snippets may be `null`.

**Mitigation**: Acceptable — the match exists, we just can't show the exact text.

### Risk 4: API call volume

**What**: V3 sends ~13 queries per customer vs V2's ~3.

**Impact**: ~4x more Azure AI Search API calls.

**Mitigation**: Azure AI Search handles thousands of queries per second. Cost is negligible on existing tier.

### Risk 5: PII Detection billing

**What**: Azure AI Language PII Detection API has a free tier (20 docs/indexer/day). Beyond that, it's billed.

**Impact**: 25 simulated files fit in free tier (barely). Production would require paid tier.

**Mitigation**: PII detection is best-effort. If skipped, V3 still works — just without pre-filtering.

### Risk 6: Less precise confidence scores

**What**: V3 confidence is based on search score normalization, not match method. A fuzzy name match might get a higher score than expected, or vice versa.

**Impact**: Confidence numbers won't match V2's. Relative ranking should be similar.

**Mitigation**: This is expected. The comparison will show whether the ranking difference matters.

---

## 20. Open Questions

1. **Same table or separate table for V3 results?** Current design: same table with `strategy_name = "v3_azure_only"`. Alternative: separate `[Search].[results_v3]` table. Same table is simpler for comparison queries.

2. **PII Detection toggle?** Should PII Detection be configurable (on/off)? It adds billing cost and API dependency. V3 works without it (just no pre-filtering).

3. **Minimum precision for PII skill?** Default is 0.5. Lower catches more entities (fewer missed tags) but more noise. Higher is more precise but may miss some.

4. **search_mode for exact fields?** Current design uses `"all"` for exact fields (SSN, DOB). But `"all"` requires every token to match. For quoted exact phrases like `"343-43-4343"`, this should be fine. Need to verify behavior with OR queries.

5. **Should V3 support resumable batches?** V2 does. For the prototype, V3 could skip resumability to keep it simple. Or reuse V2's resumability logic since it shares the same status tables.

6. **How to handle the comparison output?** Simple console table? CSV export? API endpoint that returns both V2 and V3 results side by side?
