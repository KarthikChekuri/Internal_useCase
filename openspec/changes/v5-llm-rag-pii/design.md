## Context

V5 adds LLM-based PII detection to the breach search pipeline using a RAG approach. Vector embeddings handle file retrieval (replacing keyword search), and GPT-4o handles PII detection (replacing regex/fuzzy/Lucene). This runs alongside V2/V3 for comparison.

## Goals / Non-Goals

**Goals:**
- Use vector embeddings to retrieve all relevant breach files per customer
- Use GPT-4o to detect PII in retrieved files with per-field confidence scores
- Catch paraphrased, reformatted, and contextual PII references that V2/V3 miss
- Output to same `Search.results` table for side-by-side comparison with V2/V3
- Fixed batch size: 10 files per LLM call

**Non-Goals:**
- Replace V2/V3 — this is an alternate route for comparison
- Sub-file chunking (one chunk = one file for now)
- Fine-tuning or custom models
- Real-time/streaming detection
- Modifying any V2/V3/V4 code

## Decisions

### Decision 1: Azure AI Search index with vector field (`breach-file-index-v5`)

| Field | Type | Purpose |
|---|---|---|
| `id` | String (key) | MD5 hash |
| `md5` | String (filterable) | File identifier |
| `file_path` | String | Source path |
| `content` | String (searchable) | Raw text (for reference) |
| `content_vector` | Collection(Single), 1536 dims | Embedding vector |
| `file_name` | String (filterable) | Original file name |
| `file_type` | String (filterable) | Extension (txt, csv, xlsx, xls) |

Vector search configuration:
- Algorithm: HNSW (Hierarchical Navigable Small World)
- Metric: Cosine similarity
- Dimensions: 1536 (matches `text-embedding-3-small`)

### Decision 2: Embedding model — Azure OpenAI `text-embedding-3-small`

- 1536 dimensions, good quality, low cost
- Used for both file indexing (one-time) and customer PII query embedding (per search)
- Deployed on Azure OpenAI (same subscription as existing Azure AI Search)

### Decision 3: Vector-only retrieval (no hybrid)

Customer PII is embedded as a single string and used for vector search. No keyword/BM25 component.

```python
# Embed customer PII
query_text = f"{first_name} {last_name} {ssn} {dob} {address} {city} {state} {zip} {email} {phone}"
query_vector = embedding_client.embeddings.create(
    model="text-embedding-3-small",
    input=query_text
).data[0].embedding

# Vector search — return all relevant results
results = search_client.search(
    search_text=None,
    vector_queries=[
        VectorizedQuery(
            vector=query_vector,
            k_nearest_neighbors=1000,
            fields="content_vector"
        )
    ],
    select=["md5", "file_path", "content", "file_name", "file_type"]
)
```

All results above Azure AI Search's internal relevance threshold are returned. No hard cap — could be 5 or 200 depending on the customer and data.

### Decision 4: Fixed LLM batching — 10 files per call

Retrieved files are batched into groups of 10 and sent to GPT-4o:

```
Customer has 30 relevant files:
  Call 1: files 1-10  + customer PII → GPT-4o
  Call 2: files 11-20 + customer PII → GPT-4o
  Call 3: files 21-30 + customer PII → GPT-4o
```

Each call includes:
- System prompt defining the PII detection task
- Customer PII (all fields)
- 10 file contents with md5 and file_path metadata
- Expected JSON output schema

### Decision 5: LLM prompt and response schema

**Prompt structure:**
```
SYSTEM:
You are a PII detection agent. Given a customer's personal information
and breach file contents, determine which PII fields appear in each file.
Only report confirmed matches. Return confidence 0-100 for each found field.
If a PII field is not found in a file, omit it from the result.
If no PII is found in a file, omit the file entirely.

USER:
CUSTOMER PII:
  first_name: {first_name}
  last_name: {last_name}
  ssn: {ssn}
  dob: {dob}
  address: {address}
  city: {city}
  state: {state}
  zip: {zip}
  email: {email}
  phone: {phone}
  drivers_license: {drivers_license}

FILES:
[File 1 | md5: {md5} | path: {file_path}]
<content>{file_content}</content>

[File 2 | md5: {md5} | path: {file_path}]
<content>{file_content}</content>

... (up to 10 files)
```

**Expected JSON response:**
```json
[
  {
    "md5": "abc123",
    "file_path": "data/TEXT/abc/abc123.txt",
    "leaked_first_name": {"value": "John", "confidence": 95},
    "leaked_last_name": {"value": "Smith", "confidence": 95},
    "leaked_ssn": {"value": "123-45-6789", "confidence": 99},
    "leaked_dob": null,
    "leaked_address": {"value": "main street in springfield", "confidence": 75},
    "leaked_city": null,
    "leaked_state": null,
    "leaked_zip": null,
    "leaked_email": null,
    "leaked_phone": null,
    "leaked_drivers_license": null
  }
]
```

### Decision 6: Result persistence — same table, new strategy

Results are stored in `"Search"."results"` with `strategy_name = '["llm_v5"]'` (JSON list format, consistent with V3).

Mapping from LLM response to `Search.results` columns:
- `leaked_*` columns: value from LLM response (or NULL if not found)
- `confidence`: average of all found field confidences (0.0-1.0 scale, divided by 100)
- `azure_search_score`: vector similarity score from retrieval step
- `needs_review`: true if any field confidence < 50 or overall confidence < 0.5
- `strategy_name`: `'["llm_v5"]'`

### Decision 7: Configuration — new env vars

| Variable | Description | Default |
|---|---|---|
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI service endpoint | Required |
| `AZURE_OPENAI_KEY` | Azure OpenAI API key | Required |
| `AZURE_OPENAI_EMBEDDING_DEPLOYMENT` | Embedding model deployment name | `text-embedding-3-small` |
| `AZURE_OPENAI_CHAT_DEPLOYMENT` | Chat model deployment name | `gpt-4o` |
| `AZURE_SEARCH_INDEX_V5` | V5 search index name | `breach-file-index-v5` |
| `V5_LLM_BATCH_SIZE` | Files per LLM call | `10` |

### Decision 8: Project structure (additive only)

```
breach-search/
├── app/
│   ├── services/
│   │   ├── embedding_service.py     # NEW: embed files + customer PII
│   │   ├── retrieval_service_v5.py  # NEW: vector search
│   │   ├── detection_service_v5.py  # NEW: LLM PII detection
│   │   └── batch_service_v5.py      # NEW: V5 batch orchestration
│   └── cli.py                       # MODIFIED: add v5 commands
├── tests/
│   └── services/
│       ├── test_embedding_service.py
│       ├── test_retrieval_service_v5.py
│       ├── test_detection_service_v5.py
│       └── test_batch_service_v5.py
└── strategies_v5.yaml               # NOT NEEDED — LLM replaces strategies
```

### Decision 9: CLI commands

```bash
# Index with embeddings
poetry run breach-search index --v5

# Run V5 batch
poetry run breach-search run --v5

# Compare all versions
poetry run breach-search compare <V2_ID> <V3_ID> <V5_ID>
```

## Risks / Trade-offs

- **[Vector retrieval may miss exact matches]** Embeddings encode semantic meaning, not exact values. A file containing SSN "123-45-6789" may not rank high in vector search because numbers don't embed semantically. → Mitigation: The LLM is the safety net. Even if vector retrieval ranks some files lower, it returns all results above threshold. For production, hybrid retrieval (keyword + vector) could be added later.

- **[LLM cost]** GPT-4o costs ~$5/1M input tokens. 10 customers × ~30 files each × ~2KB per file = ~600KB = ~150K tokens per batch. Cost: ~$0.75 per batch. Manageable but scales with file count. → Mitigation: Fixed batch size (10 files/call) controls cost per call. Token budget monitoring can be added.

- **[LLM hallucination]** GPT-4o may report PII that doesn't actually exist in the file (false positive). → Mitigation: Prompt instructs "only report confirmed matches." Confidence threshold + `needs_review` flag catches low-confidence detections. V2/V3 comparison reveals discrepancies.

- **[Token limit]** 10 large files could exceed GPT-4o's context window (128K tokens). → Mitigation: Most breach files are small (<5KB). For large files, content can be truncated with a warning. Future: sub-file chunking.

- **[Non-deterministic]** LLM results may vary between runs (temperature > 0). → Mitigation: Use `temperature=0` for reproducibility.

## Open Questions

- Should V5 support sub-file chunking for very large files? (deferred — one file = one chunk for now)
- Should there be a similarity threshold for vector retrieval, or rely on Azure AI Search's default cutoff?
- Should the LLM response include the exact text snippet where PII was found? (useful for auditing but increases response size)
- Should V5 support a "verify" mode where it only checks files already flagged by V2/V3? (cheaper, useful for validation)
