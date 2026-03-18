## Why

V2 uses regex/fuzzy matching and V3 uses Azure AI Search Lucene queries for PII detection. Both approaches fail when PII appears in paraphrased or non-standard forms — e.g., "lives on main street in springfield" instead of "123 Main St, Springfield IL", or "March fifteen eighty-five" instead of "1985-03-15". These are real patterns found in breach files containing emails, chat logs, and narrative text.

V5 introduces LLM-based PII detection using a RAG (Retrieval-Augmented Generation) approach: vector embeddings retrieve relevant files per customer, then GPT-4o reads the file content alongside the customer's PII and returns structured detection results with per-field confidence scores. This catches paraphrased, reformatted, and contextual PII references that pattern-matching cannot.

V5 runs alongside V2 and V3 — same database, same simulated data, comparable output for side-by-side evaluation.

## What Changes

- Add vector field (`content_vector`) to a new Azure AI Search index (`breach-file-index-v5`) for embedding-based retrieval
- Embed each breach file's content using Azure OpenAI embedding model during indexing
- At search time, embed each customer's PII and perform vector search to retrieve all relevant files
- Batch retrieved files (10 per call) and send to GPT-4o with customer PII for structured PII detection
- Parse LLM JSON response and persist to `Search.results` table with `strategy_name = "llm_v5"`

## Capabilities

### New Capabilities
- `embedding-v5`: Embed breach file content using Azure OpenAI and store vectors in Azure AI Search
- `retrieval-v5`: Vector search to retrieve relevant files per customer (all results above similarity threshold)
- `detection-v5`: LLM-based PII detection — GPT-4o reads file content + customer PII, returns structured JSON with per-field confidence
- `batch-v5`: End-to-end batch processing: embed customer PII → vector retrieve → batch LLM calls → persist results

### Modified Capabilities
- None — V2, V3, V4 code is untouched

## Impact

- **New Azure OpenAI dependency**: Embedding model (`text-embedding-3-small`) + GPT-4o for detection
- **New Azure AI Search index**: `breach-file-index-v5` with vector field (1536 dimensions)
- **New Python dependency**: `openai` (Azure OpenAI SDK)
- **Cost**: ~$1-5 per batch run (embedding + GPT-4o calls)
- **No changes to**: PostgreSQL schema, existing V2/V3/V4 code, simulated data

## Delta from V2/V3

| Aspect | V2 (regex) | V3 (Azure Search) | V5 (LLM RAG) |
|---|---|---|---|
| File retrieval | Keyword (BM25) | Keyword + PII metadata filter | Vector (embedding similarity) |
| PII detection | Python regex + rapidfuzz | Lucene per-field queries | GPT-4o reads content + customer PII |
| Paraphrased PII | Misses | Misses | Catches ("lives on main street") |
| Format variants | Partial (regex patterns) | Partial (Lucene fuzzy) | Full (LLM understands all formats) |
| Confidence | Formula-based (0-1) | Search score normalized | LLM-assigned per field (0-100) |
| Contextual reasoning | None | None | Yes (understands "his social" + number = SSN) |
| Cost per batch | ~$0 (local compute) | ~$0 (Azure Search queries) | ~$1-5 (LLM API calls) |
| API calls per customer | 3 strategies | 13 per-field queries | 1 embed + N/10 LLM calls |
