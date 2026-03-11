## ADDED Requirements

### Requirement: Batch run lifecycle
A batch run is the top-level unit of work that processes ALL customers in the master_data table against ALL indexed files. Each batch run has a unique `batch_id` (UUID), a strategy set (loaded from config at start time), and progresses through states: `pending` → `running` → `completed` (or `failed`).

#### Scenario: Start a new batch run
- **WHEN** `POST /batch/run` is called
- **THEN** a new batch_id (UUID) is generated, the strategy set is loaded from `strategies.yaml`, a row is inserted into `[Batch].[batch_runs]` with status "running", and processing begins

#### Scenario: Batch run completes successfully
- **WHEN** all customers have been processed (or failed)
- **THEN** the batch_runs row is updated to status "completed" with `completed_at` timestamp

#### Scenario: Batch run with strategy set recorded
- **WHEN** a batch run starts
- **THEN** the strategies used are stored as JSON in the `strategy_set` column of `batch_runs` for audit trail

### Requirement: Per-customer processing flow
For each customer in the master_data table, the system SHALL execute the following steps in sequence:
1. **Check resumability**: Skip if already completed in this batch
2. **Mark status**: Set customer status to "searching"
3. **Search**: Run all strategies against Azure AI Search, union candidates
4. **Mark status**: Set customer status to "detecting"
5. **Detect**: For each candidate file, run three-tier leak detection on all 13 PII fields
6. **Score**: Compute per-field and overall confidence for each (customer, file) pair
7. **Persist**: Insert result rows into `[Search].[results]` table
8. **Mark status**: Set customer status to "complete" with summary counts

Search and detection run together per customer (not as separate bulk phases).

#### Scenario: Process customer with matches
- **WHEN** customer 42 is processed and 3 strategies produce 5 unique candidate files
- **THEN** leak detection runs on all 5 files, results are persisted, and customer status is updated to "complete" with `candidates_found: 5` and `leaks_confirmed: N` (where N is files with at least one PII field found)

#### Scenario: Process customer with no matches
- **WHEN** customer 99 is processed and all strategies return zero results
- **THEN** customer status is updated to "complete" with `candidates_found: 0` and `leaks_confirmed: 0`

#### Scenario: Process customer with search error
- **WHEN** Azure AI Search returns a timeout error while processing customer 50
- **THEN** customer status is updated to "failed" with `error_message: "Azure Search timeout after 30s"`, and processing continues with the next customer

### Requirement: Customer processing order
The system SHALL process customers sequentially in `customer_id` order. This ensures deterministic behavior and predictable progress tracking.

#### Scenario: Customers processed in order
- **WHEN** the master_data table contains customer_ids [1, 2, 3, 5, 10]
- **THEN** they are processed in order: 1, 2, 3, 5, 10

### Requirement: Batch trigger via API
The system SHALL provide a `POST /batch/run` endpoint to start a new batch run. The endpoint returns immediately with the `batch_id` and starts processing in the background using `fastapi.BackgroundTasks`. The service layer is shared between API and CLI — the API calls it asynchronously via BackgroundTasks, the CLI calls it synchronously.

#### Scenario: Trigger batch via API
- **WHEN** `POST /batch/run` is called
- **THEN** the response includes `{ "batch_id": "uuid-...", "status": "running", "total_customers": 200 }` and processing begins asynchronously via `fastapi.BackgroundTasks`

#### Scenario: Trigger batch while another is running
- **WHEN** `POST /batch/run` is called while a batch is already running
- **THEN** the system returns a 409 Conflict with message "A batch is already running (batch_id: ...)"

### Requirement: Batch trigger via CLI script
The system SHALL provide a `run_batch.py` script that triggers a batch run from the command line. The script uses the same service layer as the API endpoint.

#### Scenario: Run batch from CLI
- **WHEN** `python run_batch.py` is executed
- **THEN** a new batch run starts, progress is logged to console, and the script exits when complete

#### Scenario: CLI script with custom strategy file
- **WHEN** `python run_batch.py --strategies custom_strategies.yaml` is executed
- **THEN** the batch uses strategies from the specified file instead of the default `strategies.yaml`

### Requirement: Resumable batch processing
If a batch run is interrupted (crash, timeout, manual stop), it SHALL be resumable. When a batch is resumed, the system:
1. Skips customers already marked "complete" in `[Batch].[customer_status]` for that batch_id
2. Retries customers marked "failed" (gives them another chance)
3. Continues processing customers not yet attempted

#### Scenario: Resume interrupted batch
- **WHEN** a batch was interrupted after processing 150 out of 200 customers (148 complete, 2 failed)
- **AND** `POST /batch/{batch_id}/resume` is called
- **THEN** the system retries the 2 failed customers and processes the remaining 50, without re-processing the 148 completed ones

#### Scenario: Resume a completed batch
- **WHEN** `POST /batch/{batch_id}/resume` is called on a batch with status "completed"
- **THEN** the system returns a 400 error with message "Batch already completed"

### Requirement: Result persistence
For each (customer, candidate file) pair where at least one PII field is detected, the system SHALL insert a row into `[Search].[results]` with:
- `batch_id`: the current batch run ID
- `customer_id`: FK to master_data
- `md5`: FK to datalakeuniverse
- `strategy_name`: name of the first strategy that found this file
- `leaked_fields`: JSON array of field names that were detected (e.g., `["SSN", "Fullname", "DOB"]`)
- `match_details`: JSON object with per-field detection results (found, method, confidence, snippet)
- `overall_confidence`: float computed by the confidence scoring formula
- `azure_search_score`: raw Azure AI Search score for this file (highest across strategies)
- `needs_review`: BIT flag (true for disambiguation cases, no-anchor matches)
- `searched_at`: timestamp

#### Scenario: Persist result for customer with leaks
- **WHEN** customer 42's data is found in file "abc123" with SSN (exact, 1.0) and Fullname (normalized, 0.95)
- **THEN** a row is inserted with `leaked_fields: ["SSN", "Fullname"]`, per-field match_details, and computed overall_confidence

#### Scenario: No results persisted when no leaks found
- **WHEN** customer 42 has 5 candidate files but leak detection finds no PII in any of them
- **THEN** no rows are inserted into the results table for this customer (candidates were false positives)

#### Scenario: Multiple files for same customer
- **WHEN** customer 42's data is found in 3 different files
- **THEN** 3 separate rows are inserted, each with the respective MD5, leaked fields, and confidence

### Requirement: Re-read file text from disk for leak detection
The system SHALL read file text from the file system for leak detection (using the text_extraction service and the DLU file_path), NOT from Azure AI Search result content fields. Azure Search may truncate long text content. The authoritative source is always the file on disk.

#### Scenario: File text read from disk during detection
- **WHEN** leak detection runs on a candidate file with MD5 "abc123"
- **THEN** the system resolves the file path from DLU, reads the file using text_extraction, and uses the full extracted text for detection

### Requirement: Batch results append across runs
When a new batch is run, results are appended with a new batch_id. Previous batch results are preserved. This allows comparing results across different runs or strategy sets.

#### Scenario: Two batch runs produce separate results
- **WHEN** batch A runs with `fullname_ssn` strategy and batch B runs with `ssn_only` strategy
- **THEN** both sets of results exist in the results table, distinguished by batch_id

#### Scenario: Query results by batch
- **WHEN** `GET /batch/{batch_id}/results` is called
- **THEN** only results for that specific batch are returned
