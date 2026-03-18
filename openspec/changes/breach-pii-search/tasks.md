## 1. Project Scaffold & Configuration

- [ ] 1.1 Create project directory structure (app/, scripts/, data/, tests/ with all subdirectories) and `__init__.py` files for all Python packages (app/, app/models/, app/schemas/, app/services/, app/routers/, app/utils/)
- [ ] 1.2 Create `requirements.txt` with all dependencies (fastapi, uvicorn, sqlalchemy, psycopg2-binary, rapidfuzz, azure-search-documents, openpyxl, xlrd, pydantic-settings, pyyaml, pytest, pytest-mock)
- [ ] 1.3 Create `.env.example` with all required environment variables (DATABASE_URL, AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_KEY, AZURE_SEARCH_INDEX)
- [ ] 1.4 Create `app/config.py` with pydantic-settings BaseSettings loading from .env
- [ ] 1.5 Create `.gitignore` (Python defaults + .env, __pycache__, .pytest_cache, data/TEXT/, *.pyc)
- [ ] 1.6 Create `app/models/database.py` with SQLAlchemy 2.0 engine and session factory (postgresql+psycopg2)
- [ ] 1.7 Create default `strategies.yaml` with three default strategies (fullname_ssn, lastname_dob, unique_identifiers)

## 2. Database Models (SQLAlchemy ORM)

- [ ] 2.1 Create `app/models/dlu.py` — ORM model for `"DLU"."datalakeuniverse"` (MD5 as PK, file_path only)
- [ ] 2.2 Create `app/models/master_data.py` — ORM model for `"PII"."master_data"` (customer_id as PK, 13 PII fields)
- [ ] 2.3 Create `app/models/batch.py` — ORM models for `"Batch"."batch_runs"` (batch_id, strategy_set, status, timestamps, totals) and `"Batch"."customer_status"` (batch_id, customer_id, status, candidates_found, leaks_confirmed, strategies_matched, error_message, processed_at)
- [ ] 2.4 Create `app/models/result.py` — ORM model for `"Search"."results"` (batch_id, customer_id, md5, strategy_name, leaked_fields JSON, match_details JSON, overall_confidence, azure_search_score, needs_review, searched_at)
- [ ] 2.5 Create `app/models/file_status.py` — ORM model for `"Index"."file_status"` (md5 as PK, status, indexed_at, error_message)

## 3. Simulated Data Generation

- [ ] 3.1 Create `scripts/generate_simulated_data.py` — generate 10 diverse customer records to `data/seed/master_data.csv` (customer_id as PK, all 13 PII fields)
- [ ] 3.2 Generate ~25 breach files across .txt, .xlsx, .csv, .xls formats with PII from 1–4 customers per file embedded in realistic document context. Write each file to BOTH `data/simulated_files/{descriptive_name}.{ext}` (human browsing) AND `data/TEXT/{md5[:3]}/{md5}.{ext}` (indexing pipeline). Both copies must be identical.
- [ ] 3.3 Include intentional PII variations in generated files: name misspellings, SSN format changes, date format variations, reordered names, abbreviations
- [ ] 3.4 Generate `data/seed/dlu_metadata.csv` with MD5 and file_path (following data/TEXT/{md5[:3]}/{md5}.ext convention). No GUID, caseName, fileName, fileExtension, or isExclusion columns.
- [ ] 3.5 Create `scripts/seed_database.py` — read CSVs and insert into PostgreSQL tables, creating schemas/tables if needed, with idempotent seeding

## 4. Text Extraction Service

- [ ] 4.1 Create `app/services/text_extraction.py` with extract function dispatching by file extension (.txt, .xlsx, .csv, .xls)
- [ ] 4.2 Implement .txt extractor (UTF-8 read)
- [ ] 4.3 Implement .xlsx extractor (openpyxl — all sheets, all cells, concatenated)
- [ ] 4.4 Implement .xls extractor (xlrd — all sheets, all cells, concatenated)
- [ ] 4.5 Implement .csv extractor (csv module — all rows, all columns, concatenated)
- [ ] 4.6 Add error handling: `extract_text()` returns `str` on success, `None` on failure (unsupported extension, missing file, corrupt file, encoding error). Never raises — all errors caught internally and logged. Empty files return `""`.

## 5. Azure AI Search Index Setup

- [ ] 5.1 Create `scripts/create_search_index.py` — define index schema with three content fields, custom analyzers (phonetic_analyzer with Double Metaphone, name_analyzer with ASCII folding), and pii_boost scoring profile
- [ ] 5.2 Add metadata fields to index: id (MD5), md5, file_path

## 6. Indexing Pipeline

- [ ] 6.1 Create `app/services/indexing_service.py` — query DLU table, filter to supported extensions at runtime
- [ ] 6.2 Implement file path resolution: read file_path directly from DLU record
- [ ] 6.3 Implement document building: populate all index fields (same text in content, content_phonetic, content_lowercase; MD5 as id)
- [ ] 6.4 Implement batch upload to Azure AI Search (up to 1000 documents per batch) with progress logging. Documents with the same id are upserted (overwritten).
- [ ] 6.5 Implement `index_single_file(db, search_client, md5)` for single-file indexing by MD5
- [ ] 6.6 Implement resumable indexing: check `"Index"."file_status"` table before indexing each file, skip already-indexed MD5s, support `force=true` to re-index all
- [ ] 6.7 Update `"Index"."file_status"` table after each file (indexed/failed with error_message)
- [ ] 6.8 Return IndexResponse from indexing functions: `{ files_processed, files_succeeded, files_failed, files_skipped, errors: list[str] }`
- [ ] 6.9 Create `scripts/run_indexing.py` — standalone script to trigger full indexing pipeline

## 7. Strategy System

- [ ] 7.1 Create `app/utils/strategy_loader.py` — load and validate strategies from `strategies.yaml`
- [ ] 7.2 Validate strategy field names against allowed PII field list at load time
- [ ] 7.3 Implement Lucene query building from strategy fields: name fields get ~1 fuzzy, SSN gets dashed|undashed, DOB gets multi-format, others get exact/quoted. All combined with OR.
- [ ] 7.4 Handle null PII values in strategy: skip null fields in query, warn if all fields null
- [ ] 7.5 Implement Lucene special character escaping for names (`-`, `'`, `.` → escaped or quoted)

## 8. Leak Detection Engine

- [ ] 8.1 Create `app/services/leak_detection.py` with main function that takes file text + customer PII → returns per-field match results
- [ ] 8.2 Implement Tier 1 exact regex matching: SSN (dashed + undashed), DOB (ISO + US + European formats, generate all representations of customer DOB and match any), ZipCode, DriversLicense, State (`\bXX\b` word-boundary regex for 2-char codes) with word boundary patterns
- [ ] 8.3 Implement SSN last-4 partial matching with word boundaries (`\b{last4}\b`) and confidence 0.40
- [ ] 8.4 Implement Tier 2 normalized matching: lowercase + strip punctuation for Fullname (complete-string substring search, NOT token-by-token), FirstName, LastName, City, Address fields, Country. State is excluded from Tier 2 (handled by Tier 1 word-boundary regex only).
- [ ] 8.5 Implement Tier 3 fuzzy matching: rapidfuzz token_set_ratio with sliding window, threshold 75, confidence = ratio/100. Tier 3 applies ONLY to name fields (Fullname, FirstName, LastName). All other fields stop at Tier 2.
- [ ] 8.6 Implement three-tier cascade: Tier 1 → Tier 2 → Tier 3, first match wins
- [ ] 8.7 Implement null PII field handling: skip detection for null/empty fields, report as not found, exclude from OtherFields_avg denominator
- [ ] 8.8 Implement disambiguation rule: triggers ONLY when FirstName matches but Fullname.found == false AND LastName.found == false. With SSN → confidence 0.70; without SSN → confidence 0.30–0.50 + needs_review flag
- [ ] 8.9 Implement snippet extraction: ~100 characters of surrounding context for each match

## 9. Fuzzy Matching Utilities

- [ ] 9.1 Create `app/utils/fuzzy.py` with sliding window function for rapidfuzz token_set_ratio
- [ ] 9.2 Add helper for name token splitting and normalization (handle apostrophes, hyphens, whitespace)

## 10. Confidence Scoring

- [ ] 10.1 Create `app/utils/confidence.py` with search score normalization (divide by max score in result set)
- [ ] 10.2 Implement four-formula overall confidence: SSN+Name (0.40/0.30/0.15/0.15), SSN-only (0.60/0.15/0.25), Name-only (0.50/0.20/0.30), No-anchor fallback (0.50/0.50 OtherFields_avg + SearchScore_norm, needs_review=true). Name_conf = max(Fullname_conf, FirstName_conf, LastName_conf). OtherFields_avg denominator = count of evaluable (non-null) non-anchor fields.
- [ ] 10.3 Implement per-field confidence mapping (exact→1.0, normalized→0.95, fuzzy→ratio/100, partial→0.40)

## 11. Search Service (Strategy Query Execution)

- [ ] 11.1 Create `app/services/search_service.py` — execute strategy queries against Azure AI Search per customer
- [ ] 11.2 Implement Azure AI Search query execution (queryType=full, searchMode=any, all 3 content fields, pii_boost scoring profile, top 100)
- [ ] 11.3 Implement multi-strategy union: run all strategies, merge candidates by MD5, keep highest score, track which strategy found each file
- [ ] 11.4 Retrieve file text for leak detection by re-reading from file system using text_extraction service and DLU file_path (do not rely on Azure Search result content fields, which may truncate long text)

## 12. Batch Orchestration Service

- [ ] 12.1 Create `app/services/batch_service.py` — main batch function: create batch run → iterate customers → search + detect per customer → update status
- [ ] 12.2 Implement batch run creation: generate batch_id, load strategies, insert batch_runs row, initialize customer_status rows for all customers
- [ ] 12.3 Implement per-customer processing loop: search (all strategies) → detect (all candidates) → score → persist results → update customer status
- [ ] 12.4 Implement resumable batch: on resume, skip completed customers, retry failed, continue from last pending
- [ ] 12.5 Implement error handling: catch per-customer errors, mark failed, continue to next customer
- [ ] 12.6 Implement batch completion: update batch_runs status to "completed" when all customers processed
- [ ] 12.7 Implement result persistence: insert rows into `"Search"."results"` for each (customer, file) pair with leaks

## 13. Pydantic Schemas (Request/Response Models)

- [ ] 13.1 Create `app/schemas/batch.py` — BatchRunResponse (batch_id, status, total_customers), BatchStatusResponse (phase-level summary), CustomerStatusResponse (per-customer detail), BatchResultResponse (result rows)
- [ ] 13.2 Create `app/schemas/pii.py` — PII field models, FieldMatchResult (found, method, confidence, snippet)
- [ ] 13.3 Create `app/schemas/indexing.py` — IndexResponse (files_processed, files_succeeded, files_failed, files_skipped, errors)

## 14. FastAPI Routers & App

- [ ] 14.1 Create `app/routers/indexing.py` — POST /index/all (with optional force query param) and POST /index/{md5} endpoints
- [ ] 14.2 Create `app/routers/batch.py` — POST /batch/run, POST /batch/{id}/resume, GET /batch/{id}/status, GET /batch/{id}/customers (with optional status filter), GET /batch/{id}/results (with optional customer_id filter), GET /batches
- [ ] 14.3 Create `app/dependencies.py` — dependency injection for DB session and Azure Search client
- [ ] 14.4 Create `app/main.py` — FastAPI app with router registration, CORS middleware, and lifespan handler
- [ ] 14.5 Create `run_batch.py` — CLI script to trigger batch run (calls same service layer as API)

## 15. Test Coverage Verification & Integration Tests

> Per TDD (CLAUDE.md), unit tests are written during each phase. Phase 15 verifies coverage, adds missing edge cases, and creates integration tests.

- [ ] 15.1 Verify unit test coverage for text extraction (each format: txt, xlsx, xls, csv, unsupported, missing file, corrupt file, encoding error, empty file)
- [ ] 15.2 Verify unit test coverage for leak detection engine (Tier 1 exact, Tier 2 normalized incl. Country, Tier 3 fuzzy, cascade order, disambiguation rule guard, null PII fields)
- [ ] 15.3 Verify unit test coverage for confidence scoring (all four formulas with updated OtherFields_avg denominator, search score normalization, null field edge cases)
- [ ] 15.4 Write unit tests for fuzzy matching utilities (sliding window, name normalization)
- [ ] 15.5 Write unit tests for strategy loader (valid YAML, invalid field name, missing file, null PII fields in strategy)
- [ ] 15.6 Write unit tests for Lucene query building (each field type, OR combination, special character escaping)
- [ ] 15.7 Write unit tests for batch orchestration (batch creation, customer processing, resumability, error handling)
- [ ] 15.8 Write unit tests for status tracking (customer status transitions, batch status updates)
- [ ] 15.9 Write integration test: end-to-end batch run with simulated data — verify correct files returned with expected leaked fields and confidence ranges
- [ ] 15.10 Write negative test: batch with no matching files → all customers complete with 0 leaks
- [ ] 15.11 Write fuzzy test: verify phonetic/fuzzy matching finds files with misspelled names
- [ ] 15.12 Write resumability test: interrupt batch, resume, verify no duplicate results
