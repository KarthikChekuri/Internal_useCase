## Context

V3 is an alternate route that replaces Python-side PII detection (regex, normalized matching, rapidfuzz) with Azure AI Search-native capabilities (Lucene queries, hit highlighting, PII Detection cognitive skill, metadata filters). The goal is to evaluate whether Azure AI Search alone can deliver comparable accuracy to the Python-based approach, with a simpler architecture and no disk dependency at search time.

V3 runs alongside V2 in the same FastAPI app. Both use the same SQL Server tables, same simulated data, same customer records. The only differences are: (1) a separate Azure AI Search index with PII metadata, and (2) a different search/detection strategy at query time.

## Goals / Non-Goals

**Goals:**
- Prove that Azure AI Search can handle PII detection without Python regex/fuzzy logic
- Enable side-by-side comparison of V2 vs V3 results on the same dataset
- Leverage Azure AI Search PII Detection cognitive skill to auto-tag PII types during indexing
- Use hit highlighting for snippet extraction instead of Python context windows
- Use per-field Lucene queries to determine which PII fields are present in each file
- Pre-filter candidate files using PII metadata (e.g., only search files that contain SSN patterns)

**Non-Goals:**
- Replace V2 — this is an alternate route for comparison
- Achieve identical confidence scores to V2 (different scoring model)
- Support file chunking (simulated files fit in single documents)
- Modify any V2 code or specs
- Create a separate database schema for V3 results (reuse `[Search].[results]` with a `method` column or tag)

## Decisions

### Decision 1: Separate index (`breach-file-index-v3`)

V3 needs additional metadata fields (`has_ssn`, `has_name`, `pii_types`, etc.) and a cognitive skillset. Rather than modifying the V2 index, a separate index keeps both approaches independent.

**Index fields:**

| Field | Type | Analyzer | Purpose |
|---|---|---|---|
| `id` | String (key) | — | MD5 hash |
| `md5` | String (filterable) | — | File identifier |
| `file_path` | String | — | Source path |
| `content` | String (searchable) | `standard.lucene` | Full text, exact/fuzzy search |
| `content_phonetic` | String (searchable) | `phonetic_analyzer` | Phonetic name matching |
| `content_lowercase` | String (searchable) | `name_analyzer` | Accent/case-insensitive |
| `has_ssn` | Boolean (filterable) | — | PII skill detected SSN |
| `has_name` | Boolean (filterable) | — | PII skill detected person name |
| `has_dob` | Boolean (filterable) | — | PII skill detected date |
| `has_address` | Boolean (filterable) | — | PII skill detected address |
| `has_phone` | Boolean (filterable) | — | PII skill detected phone |
| `pii_types` | Collection(String) (filterable) | — | All PII categories found |
| `pii_entity_count` | Int32 (filterable) | — | Total PII entities detected |

**Analyzers and scoring profile:** Same as V2 (`phonetic_analyzer`, `name_analyzer`, `pii_boost`).

### Decision 2: PII Detection cognitive skill at index time

During indexing, each document's content is sent through Azure AI Language's PII Detection skill. The skill returns extracted entities with type, text, offset, and confidence. The indexer maps these outputs to the index metadata fields.

**Skillset definition:**
```json
{
  "@odata.type": "#Microsoft.Skills.Text.PIIDetectionSkill",
  "defaultLanguageCode": "en",
  "minimumPrecision": 0.5,
  "maskingMode": "none",
  "inputs": [{ "name": "text", "source": "/document/content" }],
  "outputs": [{ "name": "piiEntities" }]
}
```

**Mapping PII entities to metadata fields:**
After the skill runs, a custom mapping step derives boolean flags from the entity types:
- `has_ssn = true` if any entity has type containing "Social Security"
- `has_name = true` if any entity has type "Person"
- `has_dob = true` if any entity has type containing "DateTime" or "Date"
- `has_address = true` if any entity has type "Address"
- `pii_types = distinct list of entity type strings`
- `pii_entity_count = count of all entities`

**Note:** Since we use push-mode indexing (not an indexer with data source), the PII Detection skill is called **from our Python code** via the Azure AI Language API during indexing, not via a built-in skillset pipeline. The results are mapped to metadata fields before pushing documents to the index.

### Decision 3: Per-field Lucene queries for PII detection

Instead of one broad search query + Python detection, V3 sends separate Lucene queries per PII field:

```
For customer with SSN "343-43-4343", Name "Karthik Chekuri", DOB "1992-07-15":

Query 1 (SSN):       "343-43-4343" OR "343434343"
Query 2 (Fullname):  Karthik~1 Chekuri~1
Query 3 (FirstName): Karthik~1
Query 4 (LastName):  Chekuri~1
Query 5 (DOB):       "07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"
Query 6 (Zip):       "77001"
Query 7 (City):      "Houston"
Query 8 (State):     "TX"
Query 9 (Address):   "123 Main St"
Query 10 (DL):       "TX12345678"
Query 11 (Country):  "United States"
...
```

Each query is sent to Azure AI Search with:
- `query_type="full"` (Lucene syntax)
- `search_mode="all"` for exact fields (SSN, DOB, Zip) — all terms must match
- `search_mode="any"` for name fields — partial matches allowed
- `search_fields="content,content_phonetic,content_lowercase"`
- `scoring_profile="pii_boost"`
- `highlight_fields="content"` — for snippet extraction
- `top=100`
- Metadata pre-filter where applicable: `$filter=has_ssn eq true` for SSN queries

**Field match determination:** If a query returns results for a document, that PII field is considered "found" in that document.

### Decision 4: Hit highlighting for snippets

Azure AI Search's hit highlighting returns matched terms wrapped in tags within surrounding context. This replaces Python's regex-based context window extraction.

```python
results = search_client.search(
    search_text='"343-43-4343"',
    query_type="full",
    highlight_fields="content",
    highlight_pre_tag="[[MATCH]]",
    highlight_post_tag="[[/MATCH]]",
    top=100,
)

for doc in results:
    highlights = doc.get("@search.highlights", {})
    # highlights["content"] = ["...found SSN [[MATCH]]343-43-4343[[/MATCH]] in payroll..."]
```

**Limitation:** Hit highlighting has limited support for fuzzy queries (`~1`, `~2`). Exact queries highlight reliably; fuzzy queries may not always produce highlights. When highlights are missing, the snippet is left empty.

### Decision 5: Confidence scoring via search score normalization

V2 uses four composite formulas. V3 uses a simpler model based on Azure AI Search scores:

**Per-field confidence:**
- If a per-field query returns results with `@search.score > 0` → field found
- Field confidence = `min(1.0, field_search_score / max_score_in_batch)` (normalized 0-1)
- Exact queries (SSN, DOB, Zip) typically return higher scores → higher confidence
- Fuzzy queries (names) return lower scores → lower confidence

**Overall confidence per document:**
Weighted average of found-field confidences, using the same weight categories as V2 but with search scores instead of match-method tiers:

| Category | Weight |
|---|---|
| SSN field score | 0.35 |
| Name field score (max of Fullname, FirstName, LastName) | 0.30 |
| Other fields average score | 0.20 |
| Document-level search score (from V2 broad query) | 0.15 |

**`needs_review` flag:** Set when confidence < 0.5, or when only FirstName matches without SSN or LastName.

### Decision 6: V3 routes coexist with V2 in the same app

V3 routes are registered under a `/v3` prefix:

| V2 route | V3 route |
|---|---|
| `POST /batch/run` | `POST /v3/batch/run` |
| `GET /batch/{id}/status` | `GET /v3/batch/{id}/status` |
| `GET /batch/{id}/results` | `GET /v3/batch/{id}/results` |
| `POST /index/all` | `POST /v3/index/all` |

V3 batch results are stored in the same `[Search].[results]` table with `strategy_name = "v3_azure_only"` to distinguish them from V2 results.

### Decision 7: Project structure (additive only)

```
breach-search/
├── app/
│   ├── services/
│   │   ├── search_service_v3.py      # NEW: per-field Lucene queries
│   │   ├── batch_service_v3.py       # NEW: V3 batch orchestration
│   │   └── indexing_service_v3.py    # NEW: index with PII metadata
│   ├── routers/
│   │   └── batch_v3.py               # NEW: /v3/batch/* routes
│   └── schemas/
│       └── search_v3.py              # NEW: V3-specific response models
├── scripts/
│   ├── create_search_index_v3.py     # NEW: index + PII metadata fields
│   └── run_indexing_v3.py            # NEW: index with PII detection
└── tests/
    └── services/
        ├── test_search_service_v3.py
        └── test_batch_service_v3.py
```

No existing files are modified.

## Risks / Trade-offs

- **[Detection granularity loss]** V3 cannot distinguish "exact regex match" from "normalized substring match" from "fuzzy match" per field. It only knows "query returned results" or not. This means confidence scores are less precise than V2. → Mitigation: Acceptable for comparison purposes. The side-by-side results will show whether this matters in practice.

- **[API call volume]** V3 sends ~13 queries per customer (one per PII field) vs V2's ~3 queries. For 200 customers, that's 2,600 API calls vs 600. → Mitigation: Azure AI Search handles this easily; queries are fast (<50ms each). Cost is minimal on existing tier.

- **[Hit highlighting limits with fuzzy]** Lucene fuzzy queries (`~1`, `~2`) have limited highlighting support. Snippets may be empty for fuzzy name matches. → Mitigation: Snippets are nice-to-have, not critical. The field match (found/not-found) is what matters.

- **[PII skill billing]** PII Detection cognitive skill is free for 20 docs/indexer/day. Beyond that, it requires a billable Azure AI Language resource. → Mitigation: 25 simulated files fit within the free tier. Production would need billing.

- **[32KB field limit]** Content field truncation for large files. → Mitigation: Simulated files are 500-3000 chars. Production risk is documented but not addressed in V3 prototype.

- **[False positives on short fields]** Searching for a 2-letter state code "CA" or 4-digit SSN last-4 "4343" may return many false positives in Lucene. → Mitigation: Use `search_mode="all"` and metadata pre-filters for short-value fields. Accept that V3 may have higher false positive rate than V2's word-boundary regex.

## Open Questions

- Should V3 results be stored in the same `[Search].[results]` table or a separate table?
- Should the PII Detection cognitive skill be optional (config toggle) since it adds Azure billing?
- What `minimumPrecision` threshold should the PII skill use? (0.5 is the default — lower catches more, higher reduces noise)
- Should per-field queries use `search_mode="all"` or `search_mode="any"`? "all" is more precise but may miss partial matches.
