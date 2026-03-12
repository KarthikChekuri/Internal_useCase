## ADDED Requirements

### Requirement: V3 batch processing route
The system SHALL provide a `POST /v3/batch/run` endpoint that runs batch processing using the V3 Azure-only search approach. The V3 batch uses per-field Lucene queries instead of V2's multi-strategy search + Python leak detection. Results are stored in the same `[Search].[results]` table with `strategy_name = "v3_azure_only"`.

#### Scenario: Trigger V3 batch run
- **WHEN** `POST /v3/batch/run` is called
- **THEN** a new batch run is created with `strategy_set` containing `["v3_azure_only"]`, processing begins in a background task, and the response includes `{ "batch_id": "uuid-...", "status": "running", "total_customers": N, "method": "v3_azure_only" }` immediately (HTTP 202)

#### Scenario: V3 batch runs in background
- **WHEN** `POST /v3/batch/run` is called
- **THEN** the HTTP response returns immediately with the batch_id, and processing runs asynchronously via FastAPI `BackgroundTasks`. The background task creates its own DB session (not the request session).

#### Scenario: V3 batch run alongside V2
- **WHEN** a V3 batch run completes and a V2 batch was run previously on the same data
- **THEN** both sets of results exist in `[Search].[results]` — V2 results have strategy names like "fullname_ssn", V3 results have strategy name "v3_azure_only"

#### Scenario: Concurrent V3 batch prevention
- **WHEN** `POST /v3/batch/run` is called while another V3 batch has `status = "running"`
- **THEN** the request returns HTTP 409 Conflict with a message indicating a batch is already running

### Requirement: V3 per-customer processing loop
The V3 batch service SHALL process customers sequentially, following the same order as V2 (`customer_id` ascending). For each customer, it runs per-field Lucene queries, merges results, computes confidence, and persists to the results table.

#### Scenario: V3 processes all customers
- **WHEN** V3 batch processes 10 customers
- **THEN** each customer gets per-field queries sent, results merged per document, and rows inserted into `[Search].[results]` for each (customer, file) pair where at least one field was found

#### Scenario: V3 skips customers with no queryable PII
- **WHEN** a customer has all PII fields as null
- **THEN** the customer is skipped with status "complete" and `leaks_confirmed = 0`

### Requirement: V3 result persistence
The V3 batch service SHALL persist results in the same `[Search].[results]` table as V2, with the following field mappings:

| Column | V3 value |
|---|---|
| `batch_id` | V3 batch run ID |
| `customer_id` | Customer being processed |
| `md5` | Document MD5 from Azure AI Search result |
| `strategy_name` | `"v3_azure_only"` (constant) |
| `leaked_fields` | JSON array of field names where per-field query returned results |
| `match_details` | JSON object: per field → `{ "found": true, "score": float, "snippet": str or null }` for found fields, `{ "found": false }` for not-found fields |
| `overall_confidence` | Weighted score from V3 confidence formula |
| `azure_search_score` | Highest per-field search score for this document |
| `needs_review` | Boolean per V3 review rules |
| `searched_at` | Current timestamp |

#### Scenario: V3 result row structure
- **WHEN** V3 finds customer 1's SSN and name in document "abc123"
- **THEN** a row is inserted with `leaked_fields = ["SSN", "Fullname"]`, `match_details` containing per-field scores and snippets, `strategy_name = "v3_azure_only"`, and computed `overall_confidence`

#### Scenario: V3 no-match customer
- **WHEN** V3 per-field queries return no results for any document for customer 5
- **THEN** no result rows are inserted for customer 5, and customer_status shows `leaks_confirmed = 0`

### Requirement: V3 batch status tracking
The V3 batch service SHALL reuse the same `[Batch].[batch_runs]` and `[Batch].[customer_status]` tables as V2. V3 batches are distinguishable by their `strategy_set` containing `["v3_azure_only"]`.

#### Scenario: V3 batch status via API
- **WHEN** `GET /v3/batch/{batch_id}/status` is called
- **THEN** the response shows batch progress with customer-level status (pending, searching, complete, failed), identical format to V2

#### Scenario: V3 batch results via API
- **WHEN** `GET /v3/batch/{batch_id}/results` is called
- **THEN** all V3 result rows for that batch are returned, with `strategy_name = "v3_azure_only"` and per-field match details

### Requirement: V3 batch customer status updates
The V3 batch service SHALL update customer status through the same transitions as V2: `pending → searching → complete` (or `failed` on error).

#### Scenario: Customer status during V3 processing
- **WHEN** customer 1 is being processed by V3 batch
- **THEN** status transitions from "pending" to "searching" (while per-field queries run) to "complete" (after results persisted)

#### Scenario: Customer fails during V3 processing
- **WHEN** an error occurs while processing customer 3 (e.g., Azure AI Search API error)
- **THEN** customer 3's status is set to "failed" with error_message, and processing continues to customer 4

### Requirement: V3 batch completion lifecycle
The V3 batch service SHALL update the batch run status to reflect completion or failure of the overall batch.

#### Scenario: V3 batch completes successfully
- **WHEN** all customers have been processed (each either "complete" or "failed")
- **THEN** the batch run status is set to "completed", `completed_at` is set to the current timestamp, and a summary log is emitted: `"[V3] Batch {id} complete: N customers processed, M results"`

#### Scenario: V3 batch fails with unhandled error
- **WHEN** an unhandled exception occurs during batch processing (not a per-customer error)
- **THEN** the batch run status is set to "failed" and the error is logged

### Requirement: V3 batch resumability (deferred)
V3 batch resumability is **deferred** for the prototype. If a V3 batch fails partway through, it must be re-run from scratch via `POST /v3/batch/run`. This is acceptable for the 10-customer simulated dataset. A `POST /v3/batch/{id}/resume` endpoint is NOT implemented in V3.

#### Scenario: No resume endpoint for V3
- **WHEN** `POST /v3/batch/{id}/resume` is called
- **THEN** the response is HTTP 404 (endpoint does not exist)

### Requirement: V3 routes registered under /v3 prefix
All V3 batch routes SHALL be registered under the `/v3` prefix to coexist with V2 routes.

#### Scenario: V3 routes do not conflict with V2
- **WHEN** the FastAPI app starts
- **THEN** both `POST /batch/run` (V2) and `POST /v3/batch/run` (V3) are available and independent

#### Scenario: V3 index route
- **WHEN** `POST /v3/index/all` is called
- **THEN** files are indexed to `breach-file-index-v3` (not the V2 index)

### Requirement: V3 batch uses V3 search client
The V3 batch service SHALL create a separate Azure AI Search client pointing to the V3 index (`breach-file-index-v3`). The V2 search client (pointing to `breach-file-index`) is not used by V3.

#### Scenario: V3 queries go to V3 index
- **WHEN** V3 batch processing sends per-field queries
- **THEN** all queries are sent to the `breach-file-index-v3` index, not `breach-file-index`

### Requirement: Console logging during V3 batch processing
The V3 batch service SHALL log progress to the console during processing, including: batch start, per-customer progress, per-field query counts, documents found, and batch completion summary.

#### Scenario: V3 batch logging output
- **WHEN** V3 batch processes 10 customers
- **THEN** console logs include lines like:
  - `"[V3] Batch abc123 started: 10 customers"`
  - `"[V3] Customer 1/10 (id=1): 13 field queries, 27 unique documents found, 5 with leaks"`
  - `"[V3] Batch abc123 complete: 10 customers processed, 122 results"`

## MODIFIED Requirements

None — V2 batch is not changed.

## REMOVED Requirements

None.
