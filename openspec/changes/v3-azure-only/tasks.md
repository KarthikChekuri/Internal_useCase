## 1. V3 Index Setup

- [ ] 1.1 Create `scripts/create_search_index_v3.py` — define `breach-file-index-v3` with same content fields and analyzers as V2, plus: `has_ssn` (Boolean, filterable), `has_name` (Boolean, filterable), `has_dob` (Boolean, filterable), `has_address` (Boolean, filterable), `has_phone` (Boolean, filterable), `pii_types` (Collection(String), filterable), `pii_entity_count` (Int32, filterable)
- [ ] 1.2 Add `AZURE_SEARCH_INDEX_V3` to `.env` and `app/config.py` settings (default: `breach-file-index-v3`)
- [ ] 1.3 Write tests for `create_search_index_v3.py` (index creation, field definitions, analyzer config)

## 2. V3 Indexing Pipeline (with PII Detection)

- [ ] 2.1 Create `app/services/indexing_service_v3.py` — reuse `extract_text()` from V2, add PII Detection API call per document, map entity types to metadata fields, push to V3 index
- [ ] 2.2 Implement PII entity type mapping: parse PII Detection API response, derive `has_ssn`, `has_name`, `has_dob`, `has_address`, `has_phone`, `pii_types`, `pii_entity_count` from entity categories
- [ ] 2.3 Implement graceful fallback: if PII Detection API is unavailable, set all metadata to defaults (false/empty) and continue indexing
- [ ] 2.4 Create `scripts/run_indexing_v3.py` — standalone script to trigger V3 indexing pipeline
- [ ] 2.5 Write tests for `indexing_service_v3.py` (PII detection mapping, fallback behavior, document building with metadata)

## 3. V3 Search Service (Per-Field Lucene Queries)

- [ ] 3.1 Create `app/services/search_service_v3.py` with `build_field_query(field_name, field_value)` — constructs Lucene query per field type (SSN: exact dashed+undashed, DOB: 4 formats, Names: fuzzy ~1, others: exact quoted)
- [ ] 3.2 Implement `execute_field_query(search_client, query, field_name, filter_expr)` — sends Lucene query to Azure AI Search with hit highlighting (`highlight_fields="content"`, custom tags `[[MATCH]]`/`[[/MATCH]]`), metadata pre-filter, and `top=100`
- [ ] 3.3 Implement `get_metadata_filter(field_name)` — returns appropriate `$filter` expression for PII metadata pre-filtering (SSN→`has_ssn eq true`, Name→`has_name eq true`, DOB→`has_dob eq true`, Address→`has_address eq true`, others→None)
- [ ] 3.4 Implement `search_customer_v3(search_client, customer)` — iterates over all non-null PII fields, sends per-field query for each, collects results
- [ ] 3.5 Implement `merge_field_results(field_results)` — merges per-field query results into per-document results: for each unique MD5, aggregate which fields were found, their scores, and snippets
- [ ] 3.6 Implement `compute_confidence_v3(doc_result, max_score)` — normalize search scores to 0-1, compute weighted overall confidence (SSN: 0.35, Name: 0.30, Others: 0.20, Doc: 0.15), set `needs_review` flag
- [ ] 3.7 Write tests for `build_field_query` (all field types, null fields, special characters in names)
- [ ] 3.8 Write tests for `execute_field_query` (mock Azure AI Search responses with highlights, empty results, filter expressions)
- [ ] 3.9 Write tests for `merge_field_results` (multiple fields, single field, no results)
- [ ] 3.10 Write tests for `compute_confidence_v3` (SSN+Name, SSN-only, Name-only, needs_review conditions)

## 4. V3 Batch Service

- [ ] 4.1 Create `app/services/batch_service_v3.py` — V3 batch orchestration: create batch run, iterate customers, call `search_customer_v3`, persist results, update status
- [ ] 4.2 Implement V3 batch run creation: insert into `[Batch].[batch_runs]` with `strategy_set = '["v3_azure_only"]'`
- [ ] 4.3 Implement per-customer processing: call `search_customer_v3`, merge results, compute confidence, insert into `[Search].[results]` with `strategy_name = "v3_azure_only"`
- [ ] 4.4 Implement customer status updates: pending → searching → complete (or failed)
- [ ] 4.5 Implement error handling: catch per-customer errors, mark failed, continue to next
- [ ] 4.6 Implement console logging with `[V3]` prefix for batch progress
- [ ] 4.7 Write tests for `batch_service_v3.py` (batch creation, customer processing, result persistence, error handling, logging)

## 5. V3 FastAPI Routes

- [ ] 5.1 Create `app/routers/batch_v3.py` — `POST /v3/batch/run`, `GET /v3/batch/{id}/status`, `GET /v3/batch/{id}/results`
- [ ] 5.2 Create `POST /v3/index/all` route in `app/routers/batch_v3.py` (or separate `indexing_v3.py` router)
- [ ] 5.3 Register V3 router in `app/main.py` with `/v3` prefix
- [ ] 5.4 Create V3 search client dependency pointing to `breach-file-index-v3`
- [ ] 5.5 Write tests for V3 routes (mock service layer, verify request/response shapes)

## 6. V3 Pydantic Schemas

- [ ] 6.1 Create `app/schemas/search_v3.py` — V3-specific response models: `V3FieldMatch` (field_name, found, score, snippet), `V3DocumentResult` (md5, fields_found, overall_confidence, needs_review, match_details), `V3BatchResponse`
- [ ] 6.2 Write tests for V3 schemas (serialization, validation)

## 7. Integration & Comparison

- [ ] 7.1 Write integration test: run V3 indexing on simulated data → verify documents in V3 index with PII metadata
- [ ] 7.2 Write integration test: run V3 batch on simulated data → verify results in `[Search].[results]` with `strategy_name = "v3_azure_only"`
- [ ] 7.3 Write comparison script: run same customers through V2 and V3, output side-by-side result comparison (which files matched, which fields found, confidence differences)
