# V2 Spec Review

> Reviewed: 2026-03-11
> Reviewer: Project Manager Agent
> Scope: All 7 V2 spec files reviewed against V2_DESIGN.md, design.md, and existing V1 codebase

---

## 1. Strategy System (`specs/strategy-system/spec.md`)

### Completeness: GOOD
Covers strategy definition format, YAML configuration, Lucene query construction for all field types, multi-strategy union, Azure Search query params, null handling, and validation errors.

### Issues Found

**Issue S-1: Lucene query syntax inconsistency with V2_DESIGN.md**
The spec scenario for `fullname_ssn` builds the query as:
```
Karthik~1 Chekuri~1 OR "343-43-4343" OR "343434343"
```
But V2_DESIGN.md Section 6 shows:
```
"John~1 Smith~1" OR "343-43-4343" OR "343434343"
```
The V1 code groups name tokens with parentheses: `(Karthik~1 Chekuri~1)`. The spec omits grouping entirely. Without grouping or quoting, Lucene may parse `Chekuri~1 OR "343-43-4343"` differently than intended.

**Recommendation:** Clarify whether name tokens are grouped in parentheses and whether OR is explicit between field groups. The V1 implementation uses `(token1~1 token2~1)` grouping with implicit AND within the group, which is correct. The spec scenarios should match this format.

**Impact:** Low -- agents implementing this will read V1 code for reference and the parenthesized pattern is clear there.

**Issue S-2: City fuzzy operator `~1` may not be appropriate**
The spec says City is split into tokens with `~1` fuzzy operator (like names). However, city names like "El Paso" or "San Francisco" are multi-word but not prone to the same misspellings as personal names. The V1 code does not include City in search queries at all (it only uses Fullname + SSN). This is a new behavior.

**Recommendation:** Acceptable as designed -- City as a fuzzy search term makes sense in the strategy context. No change needed.

**Issue S-3: "First strategy that found it" tracking**
Scenario says `strategy_that_found_it: "fullname_ssn"` records the first strategy. This implies processing strategies in order and recording which found each file first. The impl needs to track this during the union step.

**Recommendation:** The spec is clear enough. Implementation should iterate strategies in YAML-defined order and record which found each MD5 first.

### Gaps

- **G-S1:** No spec coverage for what happens when `strategies.yaml` defines zero strategies (empty list). Should this be a validation error at startup?
- **G-S2:** No spec coverage for duplicate strategy names in YAML. Should the system reject duplicate names?
- **G-S3:** The spec does not specify where `strategies.yaml` is located. V2_DESIGN.md says "project root." The spec says "in the project root" in one place but the CLI script scenario says `--strategies custom_strategies.yaml` (suggesting it could be anywhere). Clarify default path.

### V1 to V2 Transition

The V1 `search_service.py` has `_build_lucene_query(fullname, ssn)` hardcoded for two fields only. This entire function needs to be replaced by a strategy-driven query builder. The V1 `_tokenize_for_lucene()` and `_escape_lucene()` helper functions can be reused directly. The new `strategy_loader.py` is entirely new code.

---

## 2. Batch Orchestration (`specs/batch-orchestration/spec.md`)

### Completeness: GOOD
Covers batch lifecycle, per-customer processing flow, API trigger, CLI trigger, resumability, result persistence, file text re-read, and result append semantics.

### Issues Found

**Issue B-1: `POST /batch/{batch_id}/resume` endpoint not in V2_DESIGN.md API table**
The spec defines `POST /batch/{batch_id}/resume` for resuming interrupted batches. V2_DESIGN.md Section 13 only lists `POST /batch/run`, not a separate resume endpoint. In V2_DESIGN.md Section 10, resumability is described as restarting "the same batch_id" but doesn't specify the API mechanism.

**Recommendation:** The resume endpoint is a good addition but should be formally added to the design doc API table. Alternatively, `POST /batch/run` could accept an optional `batch_id` parameter to resume an existing batch.

**Issue B-2: Background processing model unclear**
The spec says `POST /batch/run` "returns immediately with the batch_id and starts processing in the background." This implies async background processing. However, V2_DESIGN.md does not discuss async patterns (no mention of BackgroundTasks, Celery, or threading). The CLI script scenario says "the script exits when complete" -- suggesting synchronous processing in CLI mode.

**Recommendation:** Clarify the async model. For the FastAPI endpoint, use `fastapi.BackgroundTasks` to run the batch in the background. For the CLI script, run synchronously. Both share the same service layer. This is a significant implementation detail that agents need clarity on.

**Issue B-3: Conflict detection for concurrent batches**
The spec says calling `POST /batch/run` while another batch is running returns 409. The detection mechanism is not specified. Should it check `batch_runs` table for any row with `status = "running"`?

**Recommendation:** Specify that conflict detection queries `batch_runs` for any row with `status = 'running'`. This is simple and sufficient for a single-instance deployment.

**Issue B-4: `customer_status.strategies_matched` column**
The batch orchestration spec references storing `strategies_matched` per customer but does not detail when/how this is populated during the processing flow. Is it set after all strategies run for a customer?

**Recommendation:** It should be populated during the search phase -- after all strategies execute, the strategies that returned non-empty results are recorded. The status-tracking spec defines the column. No contradiction, just needs implementation clarity.

### Gaps

- **G-B1:** The spec mentions `run_batch.py` as a CLI script but places it at the project root. The V2_DESIGN.md project structure also shows it at the root (`run_batch.py`). However, V1 has `scripts/run_indexing.py` under scripts/. Should `run_batch.py` be in scripts/ or the project root? V2_DESIGN.md is explicit: project root.
- **G-B2:** No spec coverage for batch cancellation. What if a user wants to stop a running batch? Is there a `POST /batch/{id}/cancel` endpoint? Not in scope per V2_DESIGN.md, but worth noting.
- **G-B3:** The spec mentions `POST /batch/{batch_id}/resume` but the status-tracking spec does not list this endpoint. Cross-spec gap.
- **G-B4:** No mention of what happens to `customer_status` rows when a batch is resumed. Are failed customers' status reset to "pending" before retry?

### V1 to V2 Transition

This is entirely new functionality. V1 has no batch processing, no customer iteration, no status tracking. The `batch_service.py` is a brand-new file. However, the per-customer processing logic reuses the leak detection engine and confidence scoring directly.

---

## 3. Status Tracking (`specs/status-tracking/spec.md`)

### Completeness: GOOD
Covers all three new tables (batch_runs, customer_status, file_status), all four API endpoints, status transitions, console logging format.

### Issues Found

**Issue T-1: `customer_status.strategies_matched` column present in spec but absent from batch_orchestration result persistence**
The status-tracking spec defines `strategies_matched` as `NVARCHAR(MAX)` JSON array on the customer_status table. The batch orchestration spec's "Per-customer processing flow" does not explicitly include a step for populating this field.

**Recommendation:** The batch orchestration flow should include: after search phase completes, record which strategies returned results into `customer_status.strategies_matched`.

**Issue T-2: Phase-level status response includes "indexing" section**
The `GET /batch/{batch_id}/status` response includes an "indexing" section with total/indexed/failed/skipped counts. However, indexing is a separate operation from batch runs (Phase 1 vs Phase 2+3). The indexing counts come from `[Index].[file_status]` table which is batch-independent.

**Recommendation:** This is a convenience aggregation. The impl should query file_status for indexing counts and customer_status for searching/detecting counts. This is reasonable as designed -- it gives a complete picture in one API call.

**Issue T-3: `detection.total_pairs_processed` field**
The status response includes `detection.total_pairs_processed` (e.g., 3200) and `detection.leaks_found` (e.g., 450). These counts are not stored on any table. They would need to be computed at query time by counting rows in the results table for this batch_id and summing candidates_found from customer_status.

**Recommendation:** `total_pairs_processed` = sum of `candidates_found` across all customer_status rows for the batch. `leaks_found` = count of result rows for the batch. Agents need to compute these at query time.

**Issue T-4: `GET /batch/{batch_id}/status` response schema inconsistency**
The spec shows the status response with `strategy_set` as an array of strings `["fullname_ssn", "lastname_dob", "unique_identifiers"]`, but the batch_runs table stores it as JSON (`NVARCHAR(MAX)`). The strategy_set in the table would be the full strategy objects (name + description + fields), not just names.

**Recommendation:** The API response should return just strategy names for the summary view. The full strategy definitions are stored for audit trail. No contradiction -- just a projection difference.

### Gaps

- **G-T1:** The `POST /batch/{batch_id}/resume` endpoint is mentioned in batch-orchestration spec but not listed in the status-tracking spec's endpoint inventory. The status-tracking spec lists: `GET /batch/{id}/status`, `GET /batch/{id}/customers`, `GET /batch/{id}/results`, `GET /batches`. The resume endpoint is a mutation, not a status query, so arguably it belongs in the batch router, not the status section. But it should be documented somewhere complete.
- **G-T2:** No pagination on any list endpoint. `GET /batch/{id}/customers` for 200+ customers and `GET /batch/{id}/results` for thousands of results could be large. For V2 MVP with 10 customers this is fine. Note for future.
- **G-T3:** The `GET /batch/{id}/results` response format is not specified in the status-tracking spec. It says "all result rows" but doesn't define the response schema. The batch-orchestration spec defines the result row fields (batch_id, customer_id, md5, strategy_name, leaked_fields, match_details, overall_confidence, azure_search_score, needs_review, searched_at).

### V1 to V2 Transition

All new code. V1 has no status tracking at all. New tables, new models, new endpoints.

---

## 4. Simulated Data (`specs/simulated-data/spec.md`)

### Completeness: GOOD
Covers customer generation (10 with diversity), file generation (~25 across 4 formats), PII variations, dual-write, DLU metadata (V2 format), and database seeding.

### Issues Found

**Issue D-1: File path in DLU metadata uses relative path**
The spec says `file_path` is `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt` -- a relative path. The file-indexing spec says "absolute or relative to the working directory." This is consistent but means the application must be run from the project root directory for relative paths to resolve.

**Recommendation:** Acceptable. The V1 code uses `FILE_BASE_PATH` + `TEXTPATH`. V2 uses `file_path` directly. Relative paths work if the working directory is the project root. Document this assumption.

**Issue D-2: No mention of file_status seeding**
The seed script creates master_data and DLU tables. It does not mention creating the `[Index].[file_status]`, `[Batch].[batch_runs]`, `[Batch].[customer_status]`, or `[Search].[results]` tables. These need to be created (empty) for the application to work.

**Recommendation:** The seed script should either: (a) create all schemas/tables including the empty tracking tables, or (b) the application should use `Base.metadata.create_all()` at startup. Option (b) is cleaner. The V1 seed script likely already does this. Agents should ensure all tables are created.

### Gaps

- **G-D1:** The spec says `customer_id` (INT) as PK but doesn't specify how IDs are assigned. Auto-increment starting at 1? Manually assigned? The V1 `master_pii` uses `ID` with `Identity(start=1)`. V2 uses `customer_id` which is also INT but the spec doesn't say Identity. The tasks.md says "customer_id as PK" which could mean manually assigned integers.
- **G-D2:** No spec coverage for whether the generation script is deterministic (same output every time) or random. For reproducible testing, deterministic is better. The V1 script hardcodes the data.

### V1 to V2 Transition

The V1 script writes `master_pii.csv` with an auto-generated ID column. V2 needs `master_data.csv` with `customer_id` as an explicit column. The V1 script writes DLU metadata with GUID, TEXTPATH, fileName, fileExtension, caseName, isExclusion, MD5. V2 needs only MD5 and file_path.

**Files to modify:**
- `scripts/generate_simulated_data.py` -- rewrite customer CSV format, DLU CSV format
- `scripts/seed_database.py` -- update table references and column mappings
- `data/seed/master_pii.csv` -> `data/seed/master_data.csv` (rename + reformat)
- `data/seed/dlu_metadata.csv` -- simplify to 2 columns

---

## 5. File Indexing (`specs/file-indexing/spec.md`)

### Completeness: EXCELLENT
This is the most thorough spec. Covers text extraction (9 scenarios), error contract, file path resolution, DLU table structure, document building, batch upload, single-file indexing, resumable indexing, status tracking, response format, and custom analyzers.

### Issues Found

**Issue I-1: IndexResponse adds `files_skipped` field not in V1**
V1 `IndexResponse` has: `files_processed`, `files_succeeded`, `files_failed`, `errors`. V2 adds `files_skipped` for resumability. This is a schema change.

**Recommendation:** Add `files_skipped: int = 0` to IndexResponse. Non-breaking for V1 code since it defaults to 0.

**Issue I-2: `POST /index/{md5}` replaces `POST /index/{guid}`**
V1 uses GUID as the identifier for single-file indexing. V2 uses MD5. The router path parameter changes from `{guid}` to `{md5}`.

**Recommendation:** Clear V1-to-V2 change. The router and service function signatures both need updating.

**Issue I-3: Search index metadata fields simplified**
V1 index has: `id` (GUID), `file_guid`, `content` x3, `file_name`, `file_path`, `file_extension`, `case_name`.
V2 index has: `id` (MD5), `md5`, `content` x3, `file_path`.
Fields removed: `file_guid`, `file_name`, `file_extension`, `case_name`.

**Recommendation:** The `create_search_index.py` script needs updating to remove the dropped fields and change `id` to use MD5. This is clearly specified.

**Issue I-4: Unsupported extension counted as `files_failed` vs `files_skipped`**
The spec scenario says unsupported extensions are "skipped with a warning, counted in `files_failed` with an error message." But the IndexResponse has a separate `files_skipped` field. Which is it -- failed or skipped?

**Recommendation:** Unsupported extensions should be `files_skipped` (not errors, just not applicable). Files that fail extraction (corrupt, encoding error) should be `files_failed`. The spec wording is contradictory here. I recommend: unsupported = skipped, extraction failure = failed.

### Gaps

- **G-I1:** The spec defines `force=true` query parameter for `POST /index/all` to bypass resumability. V1 does not have this. It should be a query parameter: `POST /index/all?force=true`.
- **G-I2:** No spec coverage for what happens when a file is re-indexed via `POST /index/{md5}` and it already has a "indexed" status in file_status. Should it be re-indexed (force behavior) or skipped? I assume single-file indexing always indexes (force), since the user explicitly requested it.

### V1 to V2 Transition

**Files to modify:**
- `app/services/indexing_service.py` -- major rewrite: remove GUID/caseName/isExclusion filtering, add file_status table integration for resumability, change document id to MD5, simplify document schema
- `app/routers/indexing.py` -- change `{guid}` to `{md5}`, add `force` query param
- `scripts/create_search_index.py` -- update index schema (remove file_guid, file_name, file_extension, case_name; add md5 field)

---

## 6. Leak Detection (`specs/leak-detection/spec.md`)

### Completeness: EXCELLENT
Carries over all V1 requirements with no changes. All 13 fields, three-tier cascade, disambiguation rule, null handling, snippet extraction.

### Issues Found

**Issue L-1: Spec references `master_pii` instead of `master_data`**
In the null PII field handling section, the spec says "customer's master_pii record" in two places. V2 renames this to `master_data`. The spec should use V2 terminology.

**Recommendation:** Minor wording fix. Does not affect implementation since the ORM model attribute names for PII fields are identical.

**Issue L-2: DOB European format uses `/` but V2_DESIGN.md shows `.`**
The leak detection spec shows DOB European format as `15/05/1990` (slash). V2_DESIGN.md Section 8 Tier 1 table shows `15.05.1990` (dot). These are different formats.

**Recommendation:** The detection engine should check for BOTH formats: `15/05/1990` and `15.05.1990`. The V1 code should be checked to see which it implements. This is a gap in both the spec and the design doc -- they each mention only one European separator.

### Gaps

- **G-L1:** The DOB European format discrepancy above -- the spec and design doc disagree on separator. Both `/` and `.` should be supported.
- **G-L2:** No mention of what happens when FirstName or LastName match in Tier 2 (normalized) vs Tier 3 (fuzzy) for the disambiguation rule. The disambiguation rule checks `found` status, not the tier. If FirstName is found by any tier and Fullname/LastName are not found, disambiguation applies. This is clear from the logic but could be stated more explicitly.

### V1 to V2 Transition

**No code changes needed for the detection engine itself.** The `leak_detection_service.py` function signature is `detect_leaks(file_text: str, customer: MasterPII) -> LeakDetectionResult`. In V2, the customer ORM model changes from `MasterPII` to `MasterData`, but the PII field attribute names are identical. The only change is the import and type annotation.

The file `app/services/leak_detection_service.py` will need its import changed from `MasterPII` to `MasterData`. The function body is unchanged.

---

## 7. Confidence Scoring (`specs/confidence-scoring/spec.md`)

### Completeness: EXCELLENT
All four formulas, per-field confidence mapping, search score normalization, disambiguation rule, no-anchor fallback -- all confirmed identical to V1.

### Issues Found

No issues. This spec is clean and consistent with V2_DESIGN.md.

### Gaps

None. The confidence scoring logic is 100% carried over from V1 with no changes.

### V1 to V2 Transition

**No code changes needed.** `app/utils/confidence.py` is unchanged. All functions are pure (no ORM dependencies) and work identically in V2.

---

## Cross-Spec Consistency Review

### Finding CS-1: V1 `POST /search` endpoint -- keep or remove?

None of the 7 specs mention the V1 `POST /search` endpoint. V2_DESIGN.md Section 13 API table does not list it either. V2_DESIGN.md Section 17 explicitly shows the transition: "On-demand single customer (POST /search) -> Automated batch, all customers."

**Recommendation:** Remove `POST /search` endpoint and its associated `SearchRequest`/`SearchResponse` schemas. The V1 `search_service.py` with its `search_customer_pii()` function is entirely replaced by the batch flow. The batch results API (`GET /batch/{id}/results?customer_id=42`) serves the same purpose.

The V1 files to remove/replace:
- `app/routers/search.py` -- remove entirely (replaced by `app/routers/batch.py`)
- `app/schemas/search.py` -- remove entirely (replaced by `app/schemas/batch.py`)
- `app/services/search_service.py` -- major rewrite into strategy-driven search + batch service

### Finding CS-2: Results table schema change

V1 `SearchResult` model has:
- `SearchRunID` (maps to search_run_id UUID)
- `CustomerID` (string)
- `FileGUID` (FK to DLU.GUID)
- 13 individual `LeakedXxx` BIT columns
- `LeakedFieldsList` (JSON)
- `MatchDetails` (JSON)

V2 `Result` model needs:
- `batch_id` (UNIQUEIDENTIFIER, FK to batch_runs)
- `customer_id` (INT, FK to master_data)
- `md5` (VARCHAR(32), FK to DLU)
- `strategy_name` (VARCHAR(100))
- No individual LeakedXxx BIT columns (only JSON `leaked_fields` + `match_details`)
- `overall_confidence`, `azure_search_score`, `needs_review`, `searched_at`

This is a complete schema rewrite, not an incremental change.

### Finding CS-3: Config changes

V1 `Settings` has: `DATABASE_URL`, `AZURE_SEARCH_ENDPOINT`, `AZURE_SEARCH_KEY`, `AZURE_SEARCH_INDEX`, `FILE_BASE_PATH`, `CASE_NAME`.

V2 removes: `FILE_BASE_PATH` (file_path is read directly from DLU), `CASE_NAME` (no case filtering in V2).

V2 adds: `STRATEGIES_FILE` (path to strategies.yaml, default: `strategies.yaml`).

### Finding CS-4: `customer_status.strategies_matched` populated when?

The status-tracking spec defines the column. The batch-orchestration spec defines the processing flow but does not include an explicit step for populating `strategies_matched`. Implementation should set it after the search phase completes for each customer (recording which strategies returned non-zero results).

### Finding CS-5: `POST /batch/{batch_id}/resume` endpoint location

Mentioned in batch-orchestration spec but absent from status-tracking spec endpoint list and V2_DESIGN.md API table. Should be added to the batch router alongside `POST /batch/run`.

### Finding CS-6: Indexing response `files_skipped` vs `files_failed` for unsupported extensions

The file-indexing spec says unsupported extensions are "counted in `files_failed`" but the IndexResponse schema has a separate `files_skipped` field. These are contradictory. Recommend: unsupported = skipped, extraction errors = failed.

---

## Summary of Spec Quality

| Spec | Quality | Critical Issues | Changes from V1 |
|------|---------|-----------------|------------------|
| Strategy System | Good | 0 | New code (strategy_loader.py, query builder rewrite) |
| Batch Orchestration | Good | 1 (async model unclear) | New code (batch_service.py) |
| Status Tracking | Good | 0 | New code (3 tables, 4 endpoints) |
| Simulated Data | Good | 0 | Rewrite (CSV format, DLU columns) |
| File Indexing | Excellent | 1 (skipped vs failed ambiguity) | Major rewrite (indexing_service.py) |
| Leak Detection | Excellent | 0 | Minimal (import rename only) |
| Confidence Scoring | Excellent | 0 | None |

### Recommendations Before Starting V2 Implementation

1. **Decide on async model for `POST /batch/run`** -- use `fastapi.BackgroundTasks` for the API, synchronous for CLI
2. **Clarify `files_skipped` vs `files_failed`** for unsupported extensions in indexing
3. **Add `POST /batch/{id}/resume`** to the formal API inventory
4. **Support both `/` and `.` as European DOB separators** in leak detection
5. **Confirm V1 `POST /search` removal** -- the specs and design doc imply it, but it should be an explicit decision
