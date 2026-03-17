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

## V2 Backlog

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

---

## V3: Azure AI Search Only (Alternate Route)

> V3 is an alternate approach that uses ONLY Azure AI Search capabilities -- no Python regex, no rapidfuzz, no disk reads at search time. It runs alongside V2 in the same app so results can be compared on the same data. V3 adds new files only -- no existing V2 files are modified.
>
> Key architectural differences from V2:
> - Per-field Lucene queries (one per PII field) instead of multi-strategy broad search
> - Azure AI Search hit highlighting for snippets instead of Python context windows
> - PII Detection API enrichment during indexing for metadata pre-filtering
> - Confidence from normalized `@search.score` instead of match-method tiers
> - No disk reads, no regex, no rapidfuzz at search time

---

## V3 Batch 1 -- Foundation: Config, Schemas, Index Script

All three phases have zero dependencies on each other. Maximum parallelization.

### Phase V3-1.1: V3 Config Setting
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Write test: verify `Settings` has `AZURE_SEARCH_INDEX_V3` field with default `breach-file-index-v3`. Test in `tests/test_config.py` (append to existing)
  - [ ] Add `AZURE_SEARCH_INDEX_V3: str = "breach-file-index-v3"` to `Settings` class in `app/config.py`
  - [ ] Write test: verify `get_search_client_v3()` returns a `SearchClient` pointing to the V3 index name. Test in `tests/test_dependencies.py` (append to existing)
  - [ ] Add `get_search_client_v3()` to `app/dependencies.py` -- same as `get_search_client()` but uses `AZURE_SEARCH_INDEX_V3`
  - [ ] Add `AZURE_SEARCH_INDEX_V3=breach-file-index-v3` to `.env.example`
- **V1 Files Affected:** `app/config.py`, `app/dependencies.py`
- **New Files:** `tests/test_config.py`, `tests/test_dependencies.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/indexing-v3/spec.md`
- **Effort:** S
- **Done When:** `Settings().AZURE_SEARCH_INDEX_V3` returns `"breach-file-index-v3"`. `get_search_client_v3()` returns a SearchClient configured for the V3 index. All tests pass.

---

### Phase V3-1.2: V3 Pydantic Schemas
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/schemas/test_search_v3.py`:
    - Test `V3FieldMatch` serialization: `field_name`, `found` (bool), `score` (float, optional), `snippet` (str, optional) -- test found=true with score+snippet, found=true with null snippet (fuzzy highlight gap), found=false
    - Test `V3DocumentResult` serialization: `md5`, `file_path`, `fields_found` (list[str]), `overall_confidence` (float), `azure_search_score` (float), `needs_review` (bool), `match_details` (dict[str, V3FieldMatch])
    - Test `V3BatchRunResponse` serialization: `batch_id` (UUID str), `status`, `total_customers` (int), `method` = `"v3_azure_only"`
    - Test `V3BatchStatusResponse`: same as V2 `BatchStatusResponse` but with `method` field
    - Test `V3BatchResultResponse`: `batch_id`, `customer_id`, `md5`, `strategy_name` = `"v3_azure_only"`, `leaked_fields` (list[str]), `match_details` (dict), `overall_confidence`, `azure_search_score`, `needs_review`, `searched_at`
    - Test optional fields default to None, test list fields default to empty
  - [ ] Create `app/schemas/search_v3.py` with: `V3FieldMatch`, `V3DocumentResult`, `V3BatchRunResponse`, `V3BatchStatusResponse`, `V3BatchResultResponse`
- **V1 Files Affected:** None
- **New Files:** `app/schemas/search_v3.py`, `tests/schemas/test_search_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/schemas-v3/spec.md`, `openspec/changes/v3-azure-only/specs/search-v3/spec.md`, `openspec/changes/v3-azure-only/specs/batch-v3/spec.md`
- **Effort:** S
- **Done When:** All V3 Pydantic models serialize and validate correctly. `V3FieldMatch` handles found/not-found with optional score/snippet. `V3BatchRunResponse` has `method="v3_azure_only"`. All tests pass.

---

### Phase V3-1.3: V3 Search Index Script
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/test_create_search_index_v3.py`:
    - Test `build_v3_index_definition()` returns a `SearchIndex` with name `breach-file-index-v3`
    - Test index has all V2 content fields: `content` (standard.lucene), `content_phonetic` (phonetic_analyzer), `content_lowercase` (name_analyzer)
    - Test index has V2 metadata fields: `id` (key), `md5` (filterable), `file_path`
    - Test index has V3 PII metadata fields: `has_ssn` (Boolean, filterable), `has_name` (Boolean, filterable), `has_dob` (Boolean, filterable), `has_address` (Boolean, filterable), `has_phone` (Boolean, filterable)
    - Test index has `pii_types` (Collection(String), filterable) and `pii_entity_count` (Int32, filterable)
    - Test index includes `phonetic_analyzer` and `name_analyzer` with same config as V2
    - Test index includes `pii_boost` scoring profile with weights: content=3.0, content_lowercase=2.0, content_phonetic=1.5
  - [ ] Create `scripts/create_search_index_v3.py` with `build_v3_index_definition(index_name)` and `create_v3_index()` -- extends V2 index definition with PII metadata fields
- **V1 Files Affected:** None
- **New Files:** `scripts/create_search_index_v3.py`, `tests/test_create_search_index_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/indexing-v3/spec.md`
- **Effort:** S
- **Done When:** `build_v3_index_definition()` produces a SearchIndex with all V2 content/analyzer/scoring fields plus 7 PII metadata fields (5 Boolean filterable, 1 Collection(String) filterable, 1 Int32 filterable). All tests pass.

---

## V3 Batch 2 -- Services: Indexing and Search Query Building

Depends on V3 Batch 1 (config must exist). Two independent phases.

### Phase V3-2.1: V3 Indexing Service (with PII Detection)
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-1.1, Phase V3-1.3
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/services/test_indexing_service_v3.py`:
    - Test `_call_pii_detection(text)`: mock Azure AI Language API response with SSN+Person+DateTime entities, verify returns parsed entity list
    - Test `_map_pii_entities(entities)`: verify entity type "USSocialSecurityNumber" sets `has_ssn=True`, "Person" sets `has_name=True`, "DateTime" sets `has_dob=True`, "Address" sets `has_address=True`, "PhoneNumber" sets `has_phone=True`; verify `pii_types` is distinct list, `pii_entity_count` is total count
    - Test `_map_pii_entities` with no entities: all `has_*`=False, `pii_types`=[], `pii_entity_count`=0
    - Test `_build_v3_document(md5, file_path, content, pii_metadata)`: verify document dict has all V2 fields (id, md5, file_path, content, content_phonetic, content_lowercase) plus V3 PII metadata fields
    - Test PII Detection API fallback: when API call raises exception, returns default metadata (all false/empty), logs warning, indexing continues
    - Test `index_all_files_v3()`: mock DLU query, mock text extraction, mock PII detection, mock search index client upload -- verify correct number of documents uploaded with PII metadata
    - Test file extension filtering: only .txt, .csv, .xls, .xlsx are indexed (same as V2)
  - [ ] Create `app/services/indexing_service_v3.py` with:
    - `_call_pii_detection(text: str) -> list[dict]` -- calls Azure AI Language PII Detection API, returns list of entity dicts
    - `_map_pii_entities(entities: list[dict]) -> dict` -- maps entity types to `has_*` booleans, `pii_types` list, `pii_entity_count`
    - `_build_v3_document(md5, file_path, content, pii_metadata) -> dict` -- builds document dict for V3 index
    - `index_all_files_v3(db, search_client, language_client=None) -> IndexResponse` -- main entry point, queries DLU, extracts text, calls PII detection, uploads to V3 index
  - [ ] Create `scripts/run_indexing_v3.py` -- standalone CLI script that calls `index_all_files_v3()`
- **V1 Files Affected:** None
- **New Files:** `app/services/indexing_service_v3.py`, `scripts/run_indexing_v3.py`, `tests/services/test_indexing_service_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/indexing-v3/spec.md`
- **Effort:** M
- **Done When:** `index_all_files_v3()` reads DLU records, extracts text (reusing V2), calls PII Detection API per document, maps entities to metadata, builds V3 documents, and uploads to `breach-file-index-v3`. PII Detection failures are gracefully handled (defaults + warning). All tests pass.

---

### Phase V3-2.2: V3 Search Query Builder and Field Execution
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-1.1
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/services/test_search_service_v3.py`:
    - Test `build_field_query("SSN", "343-43-4343")` returns `'"343-43-4343" OR "343434343"'`
    - Test `build_field_query("SSN", "123-45-6789")` returns `'"123-45-6789" OR "123456789"'`
    - Test `build_field_query("Fullname", "Karthik Chekuri")` returns `'Karthik~1 Chekuri~1'`
    - Test `build_field_query("Fullname", "Robert O'Brien")` returns properly escaped query with `~1` fuzzy
    - Test `build_field_query("FirstName", "Karthik")` returns `'Karthik~1'`
    - Test `build_field_query("LastName", "Chekuri")` returns `'Chekuri~1'`
    - Test `build_field_query("DOB", "1992-07-15")` returns `'"07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"'`
    - Test `build_field_query("ZipCode", "77001")` returns `'"77001"'`
    - Test `build_field_query("DriversLicense", "TX12345678")` returns `'"TX12345678"'`
    - Test `build_field_query("State", "TX")` returns `'"TX"'`
    - Test `build_field_query("City", "Houston")` returns `'"Houston"'`
    - Test `build_field_query("City", "New York")` returns `'"New York"'`
    - Test `build_field_query("Address1", "123 Main Street")` returns `'"123 Main Street"'`
    - Test `build_field_query("Country", "United States")` returns `'"United States"'`
    - Test `build_field_query` with null value returns None (skip field)
    - Test `get_search_mode("SSN")` returns `"all"`, `get_search_mode("Fullname")` returns `"any"`
    - Test `get_metadata_filter("SSN")` returns `"has_ssn eq true"`, `get_metadata_filter("Fullname")` returns `"has_name eq true"`, `get_metadata_filter("DOB")` returns `"has_dob eq true"`, `get_metadata_filter("Address1")` returns `"has_address eq true"`, `get_metadata_filter("City")` returns None
    - Test `execute_field_query()`: mock SearchClient, verify called with correct params (query_type="full", search_fields, scoring_profile, highlight_fields, highlight_pre_tag="[[MATCH]]", highlight_post_tag="[[/MATCH]]", filter, top=100), verify returns list of (md5, score, snippet) tuples
    - Test `execute_field_query()` with empty results returns empty list
    - Test `execute_field_query()` with results but no highlights: snippet is None
  - [ ] Create `app/services/search_service_v3.py` with:
    - `build_field_query(field_name: str, field_value: str | None) -> str | None` -- constructs Lucene query per field type
    - `get_search_mode(field_name: str) -> str` -- returns "all" or "any" per field type
    - `get_metadata_filter(field_name: str) -> str | None` -- returns filter expression or None
    - `execute_field_query(search_client, field_name, field_value) -> list[FieldQueryResult]` -- sends query to Azure AI Search with highlighting and pre-filter
- **V1 Files Affected:** None
- **New Files:** `app/services/search_service_v3.py`, `tests/services/test_search_service_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/search-v3/spec.md`
- **Effort:** M
- **Done When:** `build_field_query()` produces correct Lucene syntax for all 13 PII field types: SSN (dashed+undashed), DOB (4 formats), names (fuzzy ~1), others (exact quoted). `get_metadata_filter()` returns correct `$filter` expressions. `execute_field_query()` calls Azure AI Search with correct params and extracts score+snippet from response. Null fields return None (skip). All tests pass.

---

## V3 Batch 3 -- Search Merge, Confidence, Batch Service

Depends on V3 Batch 2. Two independent phases.

### Phase V3-3.1: V3 Search Result Merging and Confidence
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-2.2
- **Tasks:**
  - [ ] Write tests first (TDD), appending to `tests/services/test_search_service_v3.py`:
    - Test `search_customer_v3(search_client, customer)`: mock customer with SSN+Fullname+DOB, mock `execute_field_query` for each field, verify returns merged per-document results
    - Test `search_customer_v3` with customer where all PII is null: returns empty results
    - Test `search_customer_v3` skips null fields (no query sent for null DriversLicense)
    - Test `merge_field_results(field_results_dict)`: given SSN results for [doc_A(12.5), doc_B(10.0)] and Name results for [doc_A(8.3), doc_C(6.1)], verify merged output has doc_A with both SSN+Name, doc_B with SSN only, doc_C with Name only
    - Test `merge_field_results` with single field: one doc appears with one field found
    - Test `merge_field_results` with empty results: returns empty dict
    - Test `compute_confidence_v3(doc_result, max_score)`:
      - SSN(12.5) + Name(8.3) + DOB(9.0), max=12.5: verify per-field normalization and weighted average (0.35*1.0 + 0.30*0.664 + 0.20*0.72 + 0.15*0.0 = 0.693)
      - SSN only (10.0), max=10.0: verify (0.35*1.0 + 0.30*0.0 + 0.20*0.0 + 0.15*0.0 = 0.35), needs_review=True (below 0.5)
      - FirstName only (6.0), no SSN, no LastName, max=6.0: verify needs_review=True regardless of score
    - Test `compute_confidence_v3` Name category: takes max of Fullname, FirstName, LastName confidence
    - Test `compute_confidence_v3` Other category: takes average of found non-name/non-SSN field confidences
  - [ ] Add to `app/services/search_service_v3.py`:
    - `search_customer_v3(search_client, customer: MasterData) -> list[V3DocumentResult]` -- iterates non-null PII fields, calls `execute_field_query` per field, merges results, computes confidence
    - `merge_field_results(field_results: dict[str, list[FieldQueryResult]]) -> dict[str, dict]` -- merges per-field query results into per-document aggregation
    - `compute_confidence_v3(doc_fields: dict, max_score: float) -> tuple[float, bool]` -- computes overall confidence and needs_review flag
- **V1 Files Affected:** None
- **New Files:** None (extending `app/services/search_service_v3.py` and `tests/services/test_search_service_v3.py`)
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/search-v3/spec.md`
- **Effort:** M
- **Done When:** `search_customer_v3()` runs per-field queries for all non-null PII fields, merges results per document, and computes V3 confidence. `merge_field_results()` correctly aggregates multi-field hits per document. `compute_confidence_v3()` implements the weighted formula (SSN: 0.35, Name: 0.30, Others: 0.20, Doc: 0.15) with proper normalization and needs_review logic. All tests pass.

---

### Phase V3-3.2: V3 Batch Service
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-3.1, Phase V3-1.2
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/services/test_batch_service_v3.py`:
    - Test `start_batch_v3()`: verify batch_runs row inserted with `strategy_set='["v3_azure_only"]'`, customer_status rows initialized as "pending"
    - Test per-customer processing: mock `search_customer_v3` returning results, verify customer status transitions (pending -> searching -> complete), verify `candidates_found` and `leaks_confirmed` updated
    - Test result persistence: verify rows inserted into `[Search].[results]` with `strategy_name="v3_azure_only"`, correct `leaked_fields`, `match_details`, `overall_confidence`, `azure_search_score`, `needs_review`
    - Test customer with no results: status = "complete", leaks_confirmed = 0, no result rows inserted
    - Test customer with all-null PII: skipped with status "complete", leaks_confirmed = 0
    - Test error handling: customer processing raises exception, status = "failed" with error_message, next customer continues
    - Test batch completion: batch_runs status updated to "completed", completed_at set
    - Test `[V3]` prefix in console logging: capture log output, verify `[V3]` prefix in batch start, customer progress, and batch complete messages
  - [ ] Create `app/services/batch_service_v3.py` with:
    - `start_batch_v3(db, search_client) -> str` -- creates batch run, iterates customers, calls `search_customer_v3`, persists results, updates status
    - `_process_customer_v3(db, search_client, customer, batch_id) -> None` -- per-customer logic
    - Console logging with `[V3]` prefix at batch start, per-customer progress, batch completion
- **V1 Files Affected:** None
- **New Files:** `app/services/batch_service_v3.py`, `tests/services/test_batch_service_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/batch-v3/spec.md`
- **Effort:** M
- **Done When:** `start_batch_v3()` creates a batch with `strategy_set=["v3_azure_only"]`, processes all customers sequentially, runs `search_customer_v3` per customer (no disk reads, no regex), persists results with `strategy_name="v3_azure_only"`, updates customer status at each transition, handles errors per customer, logs with `[V3]` prefix. All tests pass.

---

## V3 Batch 4 -- Routes and App Wiring

Depends on V3 Batch 3.

### Phase V3-4.1: V3 FastAPI Routes
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-3.2, Phase V3-1.2, Phase V3-1.1
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/routers/test_batch_v3.py`:
    - Test `POST /v3/index/all`: mock indexing_service_v3, verify returns IndexResponse, verify calls `index_all_files_v3()`
    - Test `POST /v3/batch/run`: mock batch_service_v3, verify returns `V3BatchRunResponse` with `method="v3_azure_only"`, verify triggers `start_batch_v3()` via BackgroundTasks
    - Test `GET /v3/batch/{batch_id}/status`: mock DB query, verify returns batch status in `V3BatchStatusResponse` format
    - Test `GET /v3/batch/{batch_id}/results`: mock DB query, verify returns list of `V3BatchResultResponse` with `strategy_name="v3_azure_only"`
    - Test `GET /v3/batch/{batch_id}/status` with invalid batch_id: returns 404
    - Test V3 routes do not conflict with V2 routes (both registered)
  - [ ] Create `app/routers/batch_v3.py` with:
    - `POST /v3/index/all` -- calls `index_all_files_v3()`, returns IndexResponse
    - `POST /v3/batch/run` -- calls `start_batch_v3()` via BackgroundTasks, returns V3BatchRunResponse
    - `GET /v3/batch/{batch_id}/status` -- queries batch_runs + customer_status, returns V3BatchStatusResponse
    - `GET /v3/batch/{batch_id}/results` -- queries results where strategy_name="v3_azure_only", returns list[V3BatchResultResponse]
  - [ ] Write test in `tests/test_main_v2.py` (append): verify V3 router registered at `/v3` prefix alongside V2 routes
  - [ ] Update `app/main.py`: import and register `batch_v3.router` with prefix `/v3`
- **V1 Files Affected:** None
- **New Files:** `app/routers/batch_v3.py`, `tests/routers/test_batch_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/batch-v3/spec.md`
- **Effort:** M
- **Done When:** All 4 V3 endpoints are registered and functional: `POST /v3/index/all`, `POST /v3/batch/run`, `GET /v3/batch/{id}/status`, `GET /v3/batch/{id}/results`. V3 routes coexist with V2 routes. Responses use V3 schema models. All tests pass.

---

## V3 Batch 5 -- Integration and Comparison

Depends on V3 Batch 4.

### Phase V3-5.1: V3 Integration Tests and Comparison Script
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V3-4.1
- **Tasks:**
  - [ ] Write integration tests in `tests/test_v3_integration.py`:
    - Test V3 indexing: index simulated data via `index_all_files_v3()`, verify documents uploaded with PII metadata (has_ssn, has_name, etc.)
    - Test V3 batch: run `start_batch_v3()` on simulated data with mock Azure Search, verify results in `[Search].[results]` with `strategy_name="v3_azure_only"`
    - Test per-field detection: verify correct fields found for customers with known PII in files (SSN match, name match, DOB match)
    - Test V3 no-match customer: customer with no PII in any file -> status "complete", 0 leaks
    - Test V3 confidence scoring: verify confidence values match expected formula output
    - Test V3 needs_review flag: verify flag set when confidence < 0.5, or FirstName-only match without SSN/LastName
    - Test V3 snippet extraction: verify snippets contain `[[MATCH]]` tags for exact matches, null for fuzzy
  - [ ] Create `scripts/compare_v2_v3.py` -- comparison script that:
    - Queries `[Search].[results]` for both V2 and V3 batch results
    - Outputs per-customer comparison: files found by both, V2-only, V3-only
    - Shows per-field match differences (V2 leaked_fields vs V3 leaked_fields)
    - Highlights confidence score divergence
    - Outputs to console table format
  - [ ] Write test for comparison script in `tests/test_compare_v2_v3.py`: mock DB results, verify comparison output format
- **V1 Files Affected:** None
- **New Files:** `tests/test_v3_integration.py`, `scripts/compare_v2_v3.py`, `tests/test_compare_v2_v3.py`
- **Spec Reference:** `openspec/changes/v3-azure-only/specs/indexing-v3/spec.md`, `openspec/changes/v3-azure-only/specs/search-v3/spec.md`, `openspec/changes/v3-azure-only/specs/batch-v3/spec.md`
- **Effort:** M
- **Done When:** Integration tests verify the full V3 pipeline: index with PII metadata -> per-field search -> merge -> confidence -> persist with `strategy_name="v3_azure_only"`. Comparison script queries both V2 and V3 results and outputs a side-by-side table. All tests pass.

---

## V3 Backlog

- [ ] V3 batch resumability (skip completed, retry failed customers)
- [ ] PII Detection toggle (config flag to enable/disable, reduces Azure billing)
- [ ] Configurable `minimumPrecision` for PII Detection skill (default 0.5)
- [ ] V3 comparison endpoint -- API endpoint returning V2 vs V3 side-by-side results

---

## V4: CLI + Poetry Migration

> V4 replaces the FastAPI REST API with a Click CLI, adds Poetry for dependency management, Docker for portable deployment, and cleans up the API layer. All service logic (batch processing, search, leak detection, scoring) is unchanged. The CLI calls the same service layer that the API routers previously called.

---

## V4 Batch 1 -- Foundation: Poetry, Domain Model Relocation, Batch Query Extraction

All three phases have zero dependencies on each other. Maximum parallelization.

### Phase V4-1.1: Poetry Setup
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Create `pyproject.toml` with all runtime deps (sqlalchemy, pyodbc, rapidfuzz, azure-search-documents, openpyxl, xlrd, xlwt, pydantic-settings, python-dotenv, pyyaml, click), dev deps (pytest, pytest-mock), entry point `breach-search = "app.cli:main"`, and pytest config under `[tool.pytest.ini_options]`
  - [ ] Delete `requirements.txt`
  - [ ] Run `poetry install` to verify deps resolve and lock file generates
- **V1 Files Affected:** `requirements.txt` (delete)
- **New Files:** `pyproject.toml`
- **Spec Reference:** `specs/packaging-deployment/spec.md`
- **Effort:** S
- **Done When:** `pyproject.toml` exists with all runtime and dev dependencies declared, `requirements.txt` is deleted, `poetry install` succeeds and generates `poetry.lock`. Neither `fastapi` nor `uvicorn` appear in any dependency section.

---

### Phase V4-1.2: Relocate Domain Model (pii.py)
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Move `app/schemas/pii.py` to `app/models/pii.py`
  - [ ] Update import in `app/services/leak_detection_service.py`: change `from app.schemas.pii import FieldMatchResult` to `from app.models.pii import FieldMatchResult`
  - [ ] Move `tests/schemas/test_pii.py` to `tests/models/test_pii.py` and update all internal imports from `app.schemas.pii` to `app.models.pii`
  - [ ] Ensure `tests/models/__init__.py` exists
  - [ ] Run relocated tests to verify they pass
- **V1 Files Affected:** `app/schemas/pii.py` (move to `app/models/pii.py`), `app/services/leak_detection_service.py` (update import), `tests/schemas/test_pii.py` (move to `tests/models/test_pii.py`)
- **New Files:** `app/models/pii.py`, `tests/models/test_pii.py`
- **Spec Reference:** `openspec/changes/v3-cli-poetry/design.md`
- **Effort:** S
- **Done When:** `app/models/pii.py` contains `FieldMatchResult` and `CustomerSummary`. `app/schemas/pii.py` no longer exists. `leak_detection_service.py` imports from `app.models.pii`. All relocated tests pass. Note: `app/schemas/batch.py` also imports from `app.schemas.pii` but that file is deleted in Phase V4-3.1, so do NOT update it here.

---

### Phase V4-1.3: Extract Batch Query Service
- **Status:** :white_check_mark: Complete
- **Depends On:** None
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/services/test_batch_query_service.py`:
    - Test `get_batch_status(db, batch_id)` with existing batch: returns dict with batch_id, status, started_at, completed_at, strategy_set, total_customers, completed_customers, failed_customers
    - Test `get_batch_status(db, batch_id)` with non-existent batch: returns None
    - Test `get_batch_status` includes correct customer counts (completed, failed)
    - Test `get_customer_statuses(db, batch_id)` without filter: returns list of dicts with customer_id, status, candidates_found, leaks_confirmed, error_message
    - Test `get_customer_statuses(db, batch_id, status_filter="failed")`: returns only failed customers
    - Test `get_customer_statuses(db, "nonexistent")`: returns None
    - Test `get_batch_results(db, batch_id)` without customer filter: returns list of result dicts
    - Test `get_batch_results(db, batch_id, customer_id=42)`: returns only that customer's results
    - Test `get_batch_results(db, batch_id)` with no results: returns empty list
    - Test `get_batch_results(db, "nonexistent")`: returns None
    - Test `list_all_batches(db)` with batches: returns list ordered by started_at descending
    - Test `list_all_batches(db)` with no batches: returns empty list
  - [ ] Create `app/services/batch_query_service.py` with functions extracted from `app/routers/batch.py` (lines 48-307):
    - `get_batch_status(db, batch_id) -> dict | None`
    - `get_customer_statuses(db, batch_id, status_filter=None) -> list[dict] | None`
    - `get_batch_results(db, batch_id, customer_id=None) -> list[dict] | None`
    - `list_all_batches(db) -> list[dict]`
  - [ ] All functions accept SQLAlchemy Session, return plain dicts, zero FastAPI dependency
  - [ ] Run batch query service tests
- **V1 Files Affected:** `app/routers/batch.py` (source of extraction -- read only, do not modify yet)
- **New Files:** `app/services/batch_query_service.py`, `tests/services/test_batch_query_service.py`
- **Spec Reference:** `specs/batch-query-service/spec.md`
- **Effort:** M
- **Done When:** All 4 query functions exist in `app/services/batch_query_service.py`, accept SQLAlchemy Session, return plain dicts (not ORM models or Pydantic). Zero imports from `fastapi`. All 12+ test cases pass.

---

## V4 Batch 2 -- CLI Entry Point

Depends on all of V4 Batch 1.

### Phase V4-2.1: Create CLI Entry Point
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V4-1.1, Phase V4-1.2, Phase V4-1.3
- **Tasks:**
  - [ ] Write tests first (TDD) in `tests/test_cli.py` using Click `CliRunner`:
    - Test `breach-search --help` displays all subcommands (generate, seed, index, run, status, compare)
    - Test `breach-search --verbose run` sets root logger to DEBUG
    - Test `generate` command calls `scripts.generate_simulated_data.main()`
    - Test `seed` command calls `scripts.seed_database.main()`
    - Test `index` command (without --v3) calls V2 indexing pipeline
    - Test `index --v3` command calls V3 indexing pipeline
    - Test `run` command calls `batch_service.start_batch()` with default strategies
    - Test `run --strategies custom.yaml` loads custom strategies file
    - Test `run --v3` calls `batch_service_v3.start_batch_v3()`
    - Test `run` when batch already running: prints error, exits code 1
    - Test `status BATCH_ID` calls `get_batch_status()`, prints JSON
    - Test `status BATCH_ID --customers` includes per-customer statuses
    - Test `status` with non-existent batch: prints error, exits code 1
    - Test `compare V2_ID V3_ID` calls comparison logic
    - Test `compare` with non-existent batch: prints error, exits code 1
    - Test DB connection failure: prints user-friendly error, exits code 1
    - Test Azure Search auth failure: prints user-friendly error, exits code 1
    - Test `--verbose` mode shows traceback on error
  - [ ] Create `app/cli.py` with Click group `main` and `--verbose` flag
  - [ ] Implement `_build_db_session()` helper (Settings -> engine -> session)
  - [ ] Implement `_build_search_client(settings, v3=False)` helper
  - [ ] Implement `generate` command (calls `scripts.generate_simulated_data.main()`)
  - [ ] Implement `seed` command (calls `scripts.seed_database.main()`)
  - [ ] Implement `index` command with `--v3` flag
  - [ ] Implement `run` command with `--v3` and `--strategies` flags
  - [ ] Implement `status` command with `BATCH_ID` argument and `--customers` flag (uses `batch_query_service`)
  - [ ] Implement `compare` command with `V2_BATCH_ID` and `V3_BATCH_ID` arguments
  - [ ] Create `app/__main__.py` with `from app.cli import main; main()` (enables `python -m app`)
  - [ ] Run CLI tests
- **V1 Files Affected:** None
- **New Files:** `app/cli.py`, `app/__main__.py`, `tests/test_cli.py`
- **Spec Reference:** `specs/cli-interface/spec.md`
- **Effort:** M
- **Done When:** All 6 CLI subcommands (generate, seed, index, run, status, compare) work via `breach-search <cmd>` and `python -m app <cmd>`. `--verbose` flag enables DEBUG logging. Error handling catches service exceptions and prints user-friendly messages. All 18+ test cases pass using Click CliRunner.

---

## V4 Batch 3 -- Delete API Layer

Depends on V4 Batch 2.

### Phase V4-3.1: Delete API Layer
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V4-2.1
- **Tasks:**
  - [ ] Delete `app/main.py` (FastAPI app instance)
  - [ ] Delete `app/dependencies.py` (FastAPI DI)
  - [ ] Delete `app/routers/__init__.py`, `app/routers/batch.py`, `app/routers/batch_v3.py`, `app/routers/indexing.py`
  - [ ] Delete `app/schemas/__init__.py`, `app/schemas/batch.py`, `app/schemas/indexing.py`, `app/schemas/search_v3.py`
  - [ ] Delete `run_batch.py`
  - [ ] Delete `tests/routers/` (entire directory: `test_batch.py`, `test_batch_v3.py`, `test_indexing.py`, `test_indexing_v2.py`, `test_search.py`, `__init__.py`)
  - [ ] Delete `tests/schemas/` (remaining files after pii.py relocation: `__init__.py`, `test_batch.py`, `test_indexing.py`, `test_search_v3.py`)
  - [ ] Delete `tests/test_dependencies.py`, `tests/test_main.py`, `tests/test_main_v2.py`
  - [ ] Delete `tests/test_integration.py`, `tests/test_v2_integration.py`, `tests/test_v3_integration.py`
  - [ ] Remove `get_db()` generator from `app/models/database.py` (lines 53-68)
  - [ ] Update `CLAUDE.md`: remove FastAPI/uvicorn from tech stack, add Click/Poetry, update project structure to remove routers/schemas directories, update orchestrator docs for V4
  - [ ] Run full test suite (`pytest`) to verify no remaining imports of `fastapi`, `uvicorn`, or deleted modules
- **V1 Files Affected:** `app/main.py` (delete), `app/dependencies.py` (delete), `app/routers/` (delete directory), `app/schemas/__init__.py` (delete), `app/schemas/batch.py` (delete), `app/schemas/indexing.py` (delete), `app/schemas/search_v3.py` (delete), `run_batch.py` (delete), `app/models/database.py` (remove `get_db`), `CLAUDE.md` (update), `tests/routers/` (delete directory), `tests/schemas/` (delete remaining), `tests/test_dependencies.py` (delete), `tests/test_main.py` (delete), `tests/test_main_v2.py` (delete), `tests/test_integration.py` (delete), `tests/test_v2_integration.py` (delete), `tests/test_v3_integration.py` (delete)
- **New Files:** None
- **Spec Reference:** `specs/cli-interface/spec.md`
- **Effort:** M
- **Done When:** No FastAPI-related files exist. `app/routers/` and `app/schemas/` directories are gone (except `app/models/pii.py` which was already relocated). `run_batch.py` is gone. `app/models/database.py` no longer has `get_db()`. No code imports `fastapi` or `uvicorn`. `CLAUDE.md` reflects the new CLI architecture. Full test suite passes.

---

## V4 Batch 4 -- Docker and Documentation

Depends on V4 Batch 3.

### Phase V4-4.1: Docker + README
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V4-3.1
- **Tasks:**
  - [ ] Create `Dockerfile` using `python:3.12-slim` base, install Poetry, copy project files, install dependencies (without dev), set entry point to `breach-search`
  - [ ] Create `docker-compose.yml` with two services: `sqlserver` (mcr.microsoft.com/mssql/server:2022-latest on port 1433) and `app` (breach-search CLI with `depends_on: sqlserver`, `./data` volume mount, env_file)
  - [ ] Create or update `.env.example` with all required environment variables: DATABASE_URL, AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_KEY, AZURE_SEARCH_INDEX, AZURE_SEARCH_INDEX_V3, FILE_BASE_PATH, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD
  - [ ] Create `README.md` with: prerequisites, Docker Quick Start (clone, cp .env, docker-compose up sqlserver, poetry install, breach-search seed/index/run), Local Quick Start (poetry install, cp .env, breach-search seed/index/run), Environment Variables table, CLI Command Reference (all 6 commands with examples), Testing instructions (poetry run pytest)
  - [ ] Verify `docker build -t breach-search .` succeeds
- **V1 Files Affected:** `.env.example` (update)
- **New Files:** `Dockerfile`, `docker-compose.yml`, `README.md`
- **Spec Reference:** `specs/packaging-deployment/spec.md`
- **Effort:** M
- **Done When:** `docker build -t breach-search .` succeeds. `docker-compose.yml` defines sqlserver + app services. `.env.example` lists all environment variables with placeholder values. `README.md` covers Docker setup, local setup, env vars, CLI reference, and testing. `docker run breach-search --help` shows CLI help.

---

## V4 Batch 5 -- Final Verification

Depends on all V4 work.

### Phase V4-5.1: Final Verification
- **Status:** :white_check_mark: Complete
- **Depends On:** Phase V4-2.1, Phase V4-3.1, Phase V4-4.1
- **Tasks:**
  - [ ] Run `poetry install` -- verify deps resolve and lock file is valid
  - [ ] Run `poetry run pytest` -- verify all remaining tests pass
  - [ ] Run `poetry run breach-search --help` -- verify CLI help text displays all 6 subcommands
  - [ ] Grep entire codebase for imports of `fastapi`, `uvicorn`, or any deleted module -- verify zero matches
  - [ ] Verify `requirements.txt` does not exist
  - [ ] Verify `app/routers/` directory does not exist
  - [ ] Verify `app/schemas/` directory does not exist (except `app/models/pii.py` is in its new location)
  - [ ] Verify `run_batch.py` does not exist
  - [ ] Verify Docker build succeeds
- **V1 Files Affected:** None
- **New Files:** None
- **Spec Reference:** `specs/packaging-deployment/spec.md`, `specs/cli-interface/spec.md`
- **Effort:** S
- **Done When:** `poetry install` succeeds, `poetry run pytest` all green, `poetry run breach-search --help` works, zero imports of fastapi/uvicorn remain, all deleted files confirmed gone, Docker build succeeds.

---

## V4 Backlog

- [ ] CLI `batch list` subcommand to list all batches (currently only `status BATCH_ID`)
- [ ] CLI `--json` flag for machine-readable output on all commands
- [ ] Docker health check for SQL Server readiness before app start