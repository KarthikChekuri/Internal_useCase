# Roadmap

---

## V1 Batches (All Complete)

> V1 implemented on-demand single-customer search via `POST /search`. All 6 batches are complete. V2 replaces V1's search mode with automated batch processing. See `plans/completed/roadmap-archive.md` for V1 phase details.

- **Batch 1:** Project scaffold, DB models, simulated data -- COMPLETE
- **Batch 2:** Text extraction, search index, fuzzy utils, confidence utils -- COMPLETE
- **Batch 3:** Indexing pipeline, leak detection engine -- COMPLETE
- **Batch 4:** Search orchestration, Pydantic schemas -- COMPLETE
- **Batch 5:** FastAPI routers and app wiring -- COMPLETE
- **Batch 6:** Test coverage verification and integration tests -- COMPLETE

---

## V2: Automated Batch Processing

> V2 replaces V1's on-demand single-customer search with automated batch processing using configurable strategies. The three-tier leak detection, confidence scoring, fuzzy utils, and text extraction carry over from V1 unchanged. Major new work: strategy system, batch orchestration, status tracking, and rewriting the DB models / indexing / API layer.

---

## V2 Batch 1 -- Foundation: Models, Config, Strategy Loader, Simulated Data

All four phases have zero dependencies on each other. Maximum parallelization.

### Phase V2-1.1: V2 Database Models (ORM Rewrite)
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Create `app/models/master_data.py` -- ORM for `[PII].[master_data]` with `customer_id` (INT PK, not Identity), 13 PII fields. Same field names/types as V1 MasterPII but with customer_id instead of auto-increment ID, schema `PII`, table `master_data`
  - [ ] Create `app/models/batch.py` -- ORM for `[Batch].[batch_runs]` (batch_id UNIQUEIDENTIFIER PK, strategy_set NVARCHAR(MAX), status VARCHAR(20), started_at DATETIME2, completed_at DATETIME2, total_customers INT, total_files INT) and `[Batch].[customer_status]` (id INT IDENTITY PK, batch_id FK, customer_id FK, status VARCHAR(20), candidates_found INT default 0, leaks_confirmed INT default 0, strategies_matched NVARCHAR(MAX), error_message NVARCHAR(MAX), processed_at DATETIME2)
  - [ ] Create `app/models/file_status.py` -- ORM for `[Index].[file_status]` (md5 VARCHAR(32) PK FK->DLU, status VARCHAR(20), indexed_at DATETIME2, error_message NVARCHAR(MAX))
  - [ ] Create `app/models/result.py` -- ORM for `[Search].[results]` (id INT IDENTITY PK, batch_id FK, customer_id FK, md5 FK, strategy_name VARCHAR(100), leaked_fields NVARCHAR(MAX), match_details NVARCHAR(MAX), overall_confidence FLOAT, azure_search_score FLOAT, needs_review BIT default 0, searched_at DATETIME2 default GETDATE())
  - [ ] Update `app/models/dlu.py` -- Rewrite DLU model: MD5 as VARCHAR(32) PK, file_path as NVARCHAR(500), remove GUID/TEXTPATH/fileName/fileExtension/caseName/isExclusion
  - [ ] Write tests first (TDD): test all model column types, PKs, FKs, defaults, schema names, table names. Tests go in `tests/models/test_v2_models.py`
- **V1 Files Affected:** `app/models/dlu.py` (rewrite), `app/models/search_result.py` (replace with `result.py`), `app/models/master_pii.py` (replace with `master_data.py`)
- **New Files:** `app/models/master_data.py`, `app/models/batch.py`, `app/models/file_status.py`, `app/models/result.py`, `tests/models/test_v2_models.py`
- **Spec Reference:** `V2_DESIGN.md` Section 14 (Data Model), `openspec/changes/breach-pii-search/design.md` Decision 4 (SQL schemas), `openspec/changes/breach-pii-search/specs/status-tracking/spec.md` (batch_runs + customer_status + file_status schemas)
- **Effort:** M
- **Done When:** All 5 ORM models (DLU, MasterData, BatchRun, CustomerStatus, FileStatus, Result) have correct column types, PKs, FKs, defaults, and schema names. All tests pass. V1 `master_pii.py` and `search_result.py` are removed or renamed.

---

### Phase V2-1.2: Config and Strategy Loader
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Update `app/config.py` -- Remove `FILE_BASE_PATH` and `CASE_NAME` settings. Add `STRATEGIES_FILE: str = "strategies.yaml"` setting. Keep DATABASE_URL, AZURE_SEARCH_* settings unchanged
  - [ ] Create `strategies.yaml` in project root with three default strategies: `fullname_ssn` (Fullname, SSN), `lastname_dob` (LastName, DOB), `unique_identifiers` (SSN, DriversLicense)
  - [ ] Create `app/utils/strategy_loader.py` with:
    - `load_strategies(path: str) -> list[Strategy]` -- reads YAML, validates, returns list of Strategy dataclasses/models
    - `Strategy` model/dataclass with name (str), description (str), fields (list[str])
    - Validation: reject unknown field names (allowed: Fullname, FirstName, LastName, DOB, SSN, DriversLicense, Address1, Address2, Address3, ZipCode, City, State, Country), reject empty strategies list, reject missing file, reject invalid YAML
    - Raise clear errors at load time with file path and specific validation message
  - [ ] Write tests first (TDD): test valid YAML loading, invalid field name rejection, missing file error, invalid YAML error, empty strategies list error, strategy with all valid fields. Tests in `tests/utils/test_strategy_loader.py`
- **V1 Files Affected:** `app/config.py` (modify)
- **New Files:** `strategies.yaml`, `app/utils/strategy_loader.py`, `tests/utils/test_strategy_loader.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/strategy-system/spec.md` (Strategy definition, YAML config, validation scenarios)
- **Effort:** M
- **Done When:** `load_strategies()` correctly reads the default YAML, validates field names, and returns typed Strategy objects. All error cases raise with clear messages. Config no longer requires FILE_BASE_PATH or CASE_NAME. All tests pass.

---

### Phase V2-1.3: Lucene Query Builder (Strategy-Driven)
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Create `app/services/query_builder.py` with `build_strategy_query(strategy: Strategy, customer_pii: dict) -> str` that builds a full Lucene query from strategy fields + customer PII values
  - [ ] Implement field-type-specific query formatting:
    - Name fields (Fullname, FirstName, LastName): tokenize, escape Lucene specials, add `~1` fuzzy operator per token, group with parentheses
    - SSN: both dashed and undashed formats, quoted
    - DOB: ISO (1990-05-15), US (05/15/1990), European with slash (15/05/1990) and dot (15.05.1990) -- four formats total, each quoted
    - DriversLicense: quoted exact string
    - Address1/2/3: quoted exact string
    - ZipCode: exact string (no quotes needed for numeric)
    - City: tokenize with `~1` fuzzy (like names)
    - State: exact 2-character string
    - Country: quoted exact string
  - [ ] Combine all field values within a strategy with OR logic
  - [ ] Handle null PII values: skip null fields, log warning if all fields are null (strategy produces empty query)
  - [ ] Reuse V1 helpers `_escape_lucene()` and `_tokenize_for_lucene()` from `app/services/search_service.py` -- extract into `query_builder.py`
  - [ ] Write tests first (TDD): test each field type query format, test OR combination, test null field skipping, test all-null warning, test special characters in names (apostrophes, hyphens). Tests in `tests/services/test_query_builder.py`
- **V1 Files Affected:** None directly (V1 search_service.py helpers are copied, not modified yet)
- **New Files:** `app/services/query_builder.py`, `tests/services/test_query_builder.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/strategy-system/spec.md` (Lucene query construction, all field type scenarios)
- **Effort:** M
- **Done When:** `build_strategy_query()` produces correct Lucene syntax for all field types. Name tokens are fuzzy (`~1`), SSN is dashed|undashed, DOB has 4 formats, null fields are skipped. All scenarios from spec have passing tests.

---

### Phase V2-1.4: V2 Simulated Data and Seed Script
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Rewrite `scripts/generate_simulated_data.py`:
    - Generate `data/seed/master_data.csv` (not `master_pii.csv`) with columns: customer_id, Fullname, FirstName, LastName, DOB, SSN, DriversLicense, Address1, Address2, Address3, ZipCode, City, State, Country. 10 customers with diverse names (apostrophe, hyphen, non-Western)
    - Generate `data/seed/dlu_metadata.csv` with only 2 columns: MD5, file_path (no GUID, caseName, fileName, fileExtension, isExclusion)
    - Dual-write files to `data/simulated_files/` and `data/TEXT/{md5[:3]}/`
    - ~25 files across .txt, .xlsx, .csv, .xls with PII variations (misspellings, SSN format changes, date format variations, reordered names)
  - [ ] Rewrite `scripts/seed_database.py`:
    - Read `master_data.csv` and insert into `[PII].[master_data]` (customer_id as PK)
    - Read `dlu_metadata.csv` and insert into `[DLU].[datalakeuniverse]` (MD5 as PK, file_path)
    - Create all V2 schemas and tables if they don't exist (DLU, PII, Batch, Index, Search)
    - Idempotent: skip or replace on re-run
  - [ ] Write tests first (TDD): test customer CSV has correct columns and 10 rows, test DLU CSV has only MD5+file_path, test dual-write produces identical files, test file diversity (4 formats), test PII variation presence. Tests in `tests/test_generate_simulated_data_v2.py`
- **V1 Files Affected:** `scripts/generate_simulated_data.py` (rewrite), `scripts/seed_database.py` (rewrite)
- **New Files:** `tests/test_generate_simulated_data_v2.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/simulated-data/spec.md` (all requirements)
- **Effort:** M
- **Done When:** Running the generation script produces `master_data.csv` with customer_id PK and 10 diverse customers, `dlu_metadata.csv` with only MD5+file_path, ~25 dual-written files across 4 formats with PII variations. Seed script creates all V2 tables and inserts data. All tests pass.

---

## V2 Batch 2 -- Services: Indexing Rewrite, Search Service, Batch Schemas

Depends on V2 Batch 1 (models must exist). Three independent phases.

### Phase V2-2.1: Indexing Service Rewrite
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-1.1 (DLU model with MD5 PK, FileStatus model)
- **Tasks:**
  - [ ] Rewrite `app/services/indexing_service.py`:
    - Remove `_query_eligible_files()` with caseName/isExclusion filtering. Replace with: query all DLU records, filter by supported extension (.txt, .csv, .xls, .xlsx) from file_path at runtime
    - Remove `_resolve_file_path(base_path, textpath)`. Replace with: use `file_path` directly from DLU record
    - Rewrite `_build_document()`: `id` = MD5 (not GUID), include `md5` and `file_path` metadata fields only, remove `file_guid`/`file_name`/`file_extension`/`case_name`
    - Add resumability: before indexing each file, check `[Index].[file_status]` table. Skip if status="indexed" (unless force=True). After indexing, insert/update file_status row with status="indexed" or "failed"
    - Add `files_skipped` to IndexResponse (files skipped due to already-indexed status)
    - Rewrite `index_single_file()`: accept `md5` parameter instead of `guid`. Look up DLU by MD5. Always force-index (no resumability check for single file)
    - Update `index_all_files()` signature: add optional `force: bool = False` parameter
  - [ ] Update `app/routers/indexing.py`:
    - Change `POST /index/{guid}` to `POST /index/{md5}`
    - Add `force: bool = False` query parameter to `POST /index/all`
  - [ ] Update `scripts/create_search_index.py`:
    - Remove metadata fields: `file_guid`, `file_name`, `file_extension`, `case_name`
    - Add `md5` field (filterable, not searchable)
    - Keep `id` field (now set to MD5 value)
    - Keep `file_path` field
    - Keep all three content fields and custom analyzers unchanged
  - [ ] Write tests first (TDD): test extension filtering from file_path, test direct file_path resolution, test document id=MD5, test resumability (skip indexed, retry failed, force re-index), test file_status updates, test files_skipped counting. Tests in `tests/services/test_indexing_service_v2.py` and `tests/routers/test_indexing_v2.py`
- **V1 Files Affected:** `app/services/indexing_service.py` (major rewrite), `app/routers/indexing.py` (modify), `scripts/create_search_index.py` (modify)
- **New Files:** `tests/services/test_indexing_service_v2.py`, `tests/routers/test_indexing_v2.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/file-indexing/spec.md` (all requirements)
- **Effort:** M
- **Done When:** Indexing queries DLU with MD5 PK, filters by extension from file_path, uses file_path directly (no base path), builds docs with id=MD5, supports resumability via file_status table, supports force re-index. Single-file indexing uses MD5 path param. IndexResponse includes files_skipped. All tests pass.

---

### Phase V2-2.2: Search Service (Strategy-Driven)
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-1.1 (DLU model, MasterData model), V2-1.2 (strategy_loader), V2-1.3 (query_builder)
- **Tasks:**
  - [ ] Create `app/services/search_service.py` (rewrite from V1) with:
    - `search_customer(db, search_client, customer: MasterData, strategies: list[Strategy]) -> SearchResult` that executes all strategies against Azure AI Search for one customer and returns the union of candidates
    - For each strategy: call `build_strategy_query()`, execute against Azure Search (queryType="full", searchMode="any", searchFields=3 content fields, scoringProfile="pii_boost", top=100)
    - Union candidates across strategies by MD5. Keep highest score per MD5. Track which strategy first found each file
    - Return list of candidates: `[{md5, file_path, azure_search_score, strategy_name}]`
  - [ ] Remove V1 functions: `_lookup_customer()`, `_validate_fullname()`, `_build_lucene_query()`, `_execute_search()`, `_lookup_dlu_record()`, `_resolve_file_path()`, `_process_file()`, `_persist_results()`, `search_customer_pii()`. These are replaced by the strategy-driven search + batch service
  - [ ] Remove V1 exceptions `CustomerNotFoundError`, `DataIntegrityError`, `FullnameMismatchError` (no more SSN-based lookup)
  - [ ] Write tests first (TDD): test single strategy execution, test multi-strategy union (dedup by MD5, highest score wins), test strategy_name tracking (first strategy), test empty results from a strategy, test all strategies empty. Tests in `tests/services/test_search_service_v2.py`
- **V1 Files Affected:** `app/services/search_service.py` (complete rewrite)
- **New Files:** `tests/services/test_search_service_v2.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/strategy-system/spec.md` (multi-strategy union, Azure Search execution params)
- **Effort:** M
- **Done When:** `search_customer()` runs all strategies, unions candidates by MD5, keeps highest score, tracks strategy origin. Azure Search is called with correct params per strategy. All tests pass.

---

### Phase V2-2.3: V2 Pydantic Schemas (Batch Request/Response)
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-1.1 (model schemas inform response shapes)
- **Tasks:**
  - [ ] Create `app/schemas/batch.py` with:
    - `BatchRunResponse` -- batch_id (UUID), status (str), total_customers (int)
    - `BatchStatusResponse` -- batch_id, status, started_at, completed_at, strategy_set (list[str]), indexing summary (total, indexed, failed, skipped), searching summary (total_customers, completed, failed, pending), detection summary (total_pairs_processed, leaks_found)
    - `CustomerStatusResponse` -- customer_id, status, candidates_found, leaks_confirmed, strategies_matched (list[str]), error_message (optional), processed_at (optional)
    - `BatchResultResponse` -- batch_id, customer_id, md5, strategy_name, leaked_fields (list[str]), match_details (dict), overall_confidence, azure_search_score, needs_review, searched_at
    - `BatchListItem` -- batch_id, status, started_at, completed_at, total_customers, strategy_count
  - [ ] Update `app/schemas/pii.py` -- Keep FieldMatchResult as-is (no change). Remove CustomerSummary (V1 only -- SSN masking for single-customer response). Or keep if useful for batch results.
  - [ ] Remove `app/schemas/search.py` -- V1-only schemas (SearchRequest, SearchResponse, FileResult). These are replaced by batch schemas
  - [ ] Create `app/schemas/indexing.py` -- Move/update IndexResponse from indexing_service.py into schemas: add `files_skipped: int = 0`
  - [ ] Write tests first (TDD): test all schema serialization, test UUID fields, test optional fields, test list fields. Tests in `tests/schemas/test_batch.py` and `tests/schemas/test_indexing.py`
- **V1 Files Affected:** `app/schemas/search.py` (remove), `app/schemas/pii.py` (minor update)
- **New Files:** `app/schemas/batch.py`, `app/schemas/indexing.py`, `tests/schemas/test_batch.py`, `tests/schemas/test_indexing.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/status-tracking/spec.md` (response formats), `openspec/changes/breach-pii-search/specs/batch-orchestration/spec.md` (BatchRunResponse), `openspec/changes/breach-pii-search/specs/file-indexing/spec.md` (IndexResponse with files_skipped)
- **Effort:** M
- **Done When:** All batch, status, and result response schemas serialize correctly. IndexResponse includes files_skipped. V1 search.py schemas removed. All tests pass.

---

## V2 Batch 3 -- Orchestration: Batch Service, Leak Detection Update, Status APIs

Depends on V2 Batch 2. Three independent phases.

### Phase V2-3.1: Batch Orchestration Service
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-2.2 (search_service), V2-1.1 (batch models), V2-1.2 (strategy_loader)
- **Tasks:**
  - [ ] Create `app/services/batch_service.py` with:
    - `start_batch(db, search_client, strategies_file: str) -> str` -- create batch_id (UUID), load strategies, insert batch_runs row (status="running", started_at=now), count customers, initialize customer_status rows (all "pending"), call `_process_all_customers()`, update batch_runs to "completed"
    - `resume_batch(db, search_client, batch_id: str) -> str` -- load existing batch, skip completed customers, retry failed, continue pending
    - `_process_all_customers(db, search_client, batch_id, strategies)` -- iterate customers in customer_id order, call `_process_single_customer()` for each, handle errors per customer (mark failed, log, continue)
    - `_process_single_customer(db, search_client, customer, batch_id, strategies)`:
      1. Update customer_status to "searching"
      2. Call `search_customer()` for all strategies -> candidates
      3. Update customer_status to "detecting", set candidates_found
      4. For each candidate: read file text from disk via DLU file_path + text_extraction, run `detect_leaks()`, compute confidence via `_compute_file_confidence()`, persist result row if leaks found
      5. Update customer_status to "complete" with leaks_confirmed, strategies_matched
    - Conflict detection: check batch_runs for any "running" batch before starting new one (return 409 if conflict)
    - Normalize search scores across the candidate set for each customer
  - [ ] Write tests first (TDD): test batch creation (batch_runs row inserted, customer_status rows initialized), test per-customer flow (status transitions: pending->searching->detecting->complete), test error handling (customer fails, marked failed, next customer processed), test resumability (skip completed, retry failed), test conflict detection (409 if running batch exists), test zero candidates (customer complete with 0 leaks), test result persistence. Tests in `tests/services/test_batch_service.py`
- **V1 Files Affected:** None (entirely new)
- **New Files:** `app/services/batch_service.py`, `tests/services/test_batch_service.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/batch-orchestration/spec.md` (all requirements), `openspec/changes/breach-pii-search/specs/status-tracking/spec.md` (status transitions)
- **Effort:** M
- **Done When:** `start_batch()` creates a batch, iterates all customers in order, runs search+detect per customer, persists results, updates status at each phase. `resume_batch()` skips completed customers and retries failed. Conflict detection prevents concurrent batches. All tests pass.

---

### Phase V2-3.2: Leak Detection V2 Adaptation
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-1.1 (MasterData model)
- **Tasks:**
  - [ ] Update `app/services/leak_detection_service.py`:
    - Change import from `MasterPII` to `MasterData` (from `app.models.master_data`)
    - Update type annotation on `detect_leaks()` parameter from `MasterPII` to `MasterData`
    - Verify all PII field attribute names are identical between MasterPII and MasterData (they are: Fullname, FirstName, LastName, DOB, SSN, DriversLicense, Address1-3, ZipCode, City, State, Country)
    - Add European DOB format with dot separator (15.05.1990) in addition to slash (15/05/1990) -- both should be checked in Tier 1
  - [ ] Update existing tests in `tests/services/test_leak_detection_service.py`:
    - Change MasterPII references to MasterData
    - Add test for DOB European format with dot separator
  - [ ] Verify all existing leak detection tests still pass after the import change
- **V1 Files Affected:** `app/services/leak_detection_service.py` (minor import + type changes, DOB format addition)
- **New Files:** None
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/leak-detection/spec.md`, `V2_DESIGN.md` Section 8 (European DOB with `.` separator)
- **Effort:** S
- **Done When:** `detect_leaks()` accepts MasterData instead of MasterPII. European DOB with both `/` and `.` separators are matched. All existing tests pass with updated imports. New DOB dot-separator test passes.

---

### Phase V2-3.3: Batch Router and Status APIs
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-2.3 (batch schemas), V2-3.1 (batch_service)
- **Tasks:**
  - [ ] Create `app/routers/batch.py` with:
    - `POST /batch/run` -- calls `batch_service.start_batch()` via BackgroundTasks, returns BatchRunResponse immediately with batch_id and status="running"
    - `POST /batch/{batch_id}/resume` -- calls `batch_service.resume_batch()`, returns BatchRunResponse
    - `GET /batch/{batch_id}/status` -- queries batch_runs + customer_status + file_status + results, returns BatchStatusResponse with phase-level summary
    - `GET /batch/{batch_id}/customers` -- queries customer_status for batch, optional `?status=` filter, returns list[CustomerStatusResponse]
    - `GET /batch/{batch_id}/results` -- queries results for batch, optional `?customer_id=` filter, returns list[BatchResultResponse] ordered by customer_id then confidence desc
    - `GET /batches` -- queries all batch_runs ordered by started_at desc, returns list[BatchListItem]
    - Error handling: 404 for invalid batch_id, 409 for concurrent batch, 400 for resuming completed batch
  - [ ] Update `app/main.py`:
    - Remove `from app.routers.search import router as search_router` (V1 endpoint removed)
    - Add `from app.routers.batch import router as batch_router`
    - Register batch_router instead of search_router
    - Keep indexing_router
  - [ ] Remove `app/routers/search.py` (V1 POST /search endpoint)
  - [ ] Update `app/dependencies.py` if needed (add strategy loader dependency?)
  - [ ] Create `run_batch.py` at project root -- CLI script that calls `batch_service.start_batch()` synchronously, logs progress to console, supports `--strategies` flag for custom YAML file path
  - [ ] Write tests first (TDD): test all 6 endpoints (POST run, POST resume, GET status, GET customers, GET results, GET batches), test query params (status filter, customer_id filter), test error codes (404, 409, 400), test main.py router registration. Tests in `tests/routers/test_batch.py`, `tests/test_main_v2.py`
- **V1 Files Affected:** `app/main.py` (modify -- swap routers), `app/routers/search.py` (remove), `app/dependencies.py` (possible minor update)
- **New Files:** `app/routers/batch.py`, `run_batch.py`, `tests/routers/test_batch.py`, `tests/test_main_v2.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/batch-orchestration/spec.md` (POST /batch/run, resume, CLI), `openspec/changes/breach-pii-search/specs/status-tracking/spec.md` (all GET endpoints, response formats)
- **Effort:** M
- **Done When:** All 6 batch/status endpoints are registered and functional. POST /batch/run triggers background processing and returns immediately. GET endpoints return correctly shaped responses. V1 POST /search is removed. CLI script works. All tests pass.

---

## V2 Batch 4 -- Integration: Console Logging, End-to-End Tests

Depends on V2 Batch 3. Two independent phases.

### Phase V2-4.1: Console Logging and Observability
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-3.1 (batch_service)
- **Tasks:**
  - [ ] Add structured console logging to `app/services/batch_service.py`:
    - Indexing progress: `"Indexing: 50/500 files processed (3 failed)"`
    - Customer processing: `"Customer 42/200: 5 candidates, 3 leaks confirmed (fullname_ssn, unique_identifiers)"`
    - Batch completion: `"Batch complete: 200 customers, 1500 total leaks across 180 files, 2 customers failed"`
    - Error logging: customer_id, phase (searching/detecting), error message
  - [ ] Configure Python logging format for console output in `app/main.py` or a logging config module
  - [ ] Write tests: verify log messages are emitted at correct points with correct format (capture log output in tests). Tests in `tests/services/test_batch_logging.py`
- **V1 Files Affected:** `app/services/batch_service.py` (add logging statements)
- **New Files:** `tests/services/test_batch_logging.py`
- **Spec Reference:** `openspec/changes/breach-pii-search/specs/status-tracking/spec.md` (Console logging scenarios)
- **Effort:** S
- **Done When:** Running a batch produces structured progress logs to stdout matching the spec format. Error logs include customer_id and phase context. Tests verify log output.

---

### Phase V2-4.2: V2 Integration Tests
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-3.1 (batch_service), V2-3.3 (batch router), V2-2.1 (indexing), V2-1.4 (simulated data)
- **Tasks:**
  - [ ] Create `tests/test_v2_integration.py` with end-to-end tests using simulated data:
    - Test full batch run: index files -> run batch -> verify results for all 10 customers
    - Test customer with matches: verify correct files returned with expected leaked fields and confidence ranges
    - Test customer with no matches: verify status "complete" with 0 leaks
    - Test fuzzy matching: customer with misspelled name in files -> verify detected
    - Test multi-strategy union: verify files found by different strategies are properly merged
    - Test resumability: interrupt batch (mock crash), resume, verify no duplicate results and all customers processed
    - Test concurrent batch rejection: start batch, try to start another -> 409
  - [ ] Update or replace `tests/test_integration.py` (V1 integration tests that test POST /search flow -- these are now invalid)
  - [ ] Remove or update V1-specific tests that reference removed code:
    - `tests/routers/test_search.py` (V1 POST /search router tests)
    - `tests/services/test_search_service.py` (V1 search_customer_pii tests)
    - `tests/models/test_models.py` (V1 model tests if they reference MasterPII/DLU GUID)
  - [ ] Write tests (TDD): all integration scenarios above, using mock Azure Search and in-memory DB
- **V1 Files Affected:** `tests/test_integration.py` (replace), `tests/routers/test_search.py` (remove), `tests/services/test_search_service.py` (remove or replace)
- **New Files:** `tests/test_v2_integration.py`
- **Spec Reference:** All 7 spec files (integration tests validate the complete pipeline)
- **Effort:** M
- **Done When:** Integration tests exercise the full V2 pipeline: batch run -> strategy search -> leak detection -> confidence -> results persistence -> status tracking. All tests pass with mocked Azure Search. No V1-specific dead test code remains.

---

## V2 Batch 5 -- Cleanup and Verification

Depends on V2 Batch 4.

### Phase V2-5.1: V1 Code Cleanup and Final Verification
- **Status:** :white_check_mark: Complete
- **Depends On:** V2-4.1, V2-4.2
- **Tasks:**
  - [ ] Remove all dead V1 code:
    - Verify `app/models/master_pii.py` is no longer imported anywhere; delete if so
    - Verify `app/models/search_result.py` is no longer imported anywhere; delete if so
    - Verify `app/routers/search.py` is deleted
    - Verify `app/schemas/search.py` is deleted
    - Clean up any remaining V1 imports or references across the codebase
  - [ ] Run full test suite: `pytest` -- all tests must pass
  - [ ] Verify `uvicorn app.main:app` starts without errors
  - [ ] Verify `/docs` endpoint shows all V2 routes (indexing + batch) and no V1 routes (POST /search)
  - [ ] Verify `python run_batch.py --help` works
  - [ ] Update `requirements.txt` if any new dependencies needed (pyyaml for strategies.yaml)
  - [ ] Update `.env.example` to remove `FILE_BASE_PATH` and `CASE_NAME`, add any new vars
- **V1 Files Affected:** Various (cleanup)
- **New Files:** None
- **Spec Reference:** All specs (verification)
- **Effort:** S
- **Done When:** No dead V1 code remains. Full test suite passes. App starts cleanly. All V2 endpoints visible in /docs. CLI script functional. requirements.txt and .env.example are current.

---

## Backlog

- [ ] File chunking support for production files exceeding 32KB
- [ ] Nickname detection and initial-pattern matching
- [ ] Parallel customer processing (multiple customers at once within a batch)
- [ ] Batch cancellation endpoint (POST /batch/{id}/cancel)
- [ ] Pagination on list endpoints (customers, results, batches)
- [ ] Confidence threshold filtering on results
- [ ] Incremental batch mode (process only new customers)
- [ ] Encrypted/password-protected file handling
- [ ] Image/video file processing
- [ ] UI/frontend for search results
- [ ] Production master_data data source integration