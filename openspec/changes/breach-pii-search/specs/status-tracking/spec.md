## ADDED Requirements

### Requirement: Batch run status table
The system SHALL maintain a `[Batch].[batch_runs]` table that tracks overall batch run metadata:
- `batch_id` (UNIQUEIDENTIFIER, PK): unique identifier for the run
- `strategy_set` (NVARCHAR(MAX)): JSON representation of the strategies used
- `status` (VARCHAR(20)): pending, running, completed, failed
- `started_at` (DATETIME2): when the batch started
- `completed_at` (DATETIME2): when the batch finished (null if still running)
- `total_customers` (INT): total customers to process
- `total_files` (INT): total files in the index

#### Scenario: Batch run row created at start
- **WHEN** a new batch run starts
- **THEN** a row is inserted with status "running", started_at set to now, and total_customers populated from the master_data count

#### Scenario: Batch run row updated on completion
- **WHEN** all customers are processed
- **THEN** the row is updated with status "completed" and completed_at set to now

### Requirement: Per-customer status table
The system SHALL maintain a `[Batch].[customer_status]` table that tracks per-customer progress within a batch:
- `id` (INT IDENTITY, PK)
- `batch_id` (UNIQUEIDENTIFIER, FK → batch_runs)
- `customer_id` (INT, FK → master_data)
- `status` (VARCHAR(20)): pending, searching, detecting, complete, failed
- `candidates_found` (INT): number of unique candidate files from search
- `leaks_confirmed` (INT): number of files with at least one PII field detected
- `strategies_matched` (NVARCHAR(MAX)): JSON array of strategy names that returned results
- `error_message` (NVARCHAR(MAX)): error details if status is "failed"
- `processed_at` (DATETIME2): when processing completed/failed

#### Scenario: Customer status initialized as pending
- **WHEN** a batch run starts
- **THEN** a customer_status row is created for each customer with status "pending"

#### Scenario: Customer status transitions through searching → detecting → complete
- **WHEN** customer 42 is being processed
- **THEN** status transitions: "pending" → "searching" (during strategy queries) → "detecting" (during leak detection) → "complete" (after all results persisted)

#### Scenario: Customer status set to failed on error
- **WHEN** an error occurs processing customer 50 (e.g., Azure Search timeout)
- **THEN** status is set to "failed" with error_message describing the failure, and processed_at is set

#### Scenario: Customer with zero candidates
- **WHEN** customer 99 is processed and all strategies return no results
- **THEN** status is "complete" with `candidates_found: 0`, `leaks_confirmed: 0`, and `strategies_matched: []`

#### Scenario: Customer with candidates but no leaks
- **WHEN** customer 88 has 5 candidate files but detection finds no PII in any
- **THEN** status is "complete" with `candidates_found: 5`, `leaks_confirmed: 0`

### Requirement: Indexing file status table
The system SHALL maintain an `[Index].[file_status]` table that tracks the indexing state of each file:
- `md5` (VARCHAR(32), PK, FK → datalakeuniverse)
- `status` (VARCHAR(20)): indexed, failed, skipped
- `indexed_at` (DATETIME2): when the file was indexed
- `error_message` (NVARCHAR(MAX)): error details if status is "failed"

This table is used for indexing resumability (skip already-indexed files).

#### Scenario: File indexed successfully
- **WHEN** file with MD5 "abc123" is indexed
- **THEN** file_status row has `status: "indexed"`, `indexed_at` set, `error_message: null`

#### Scenario: File failed to index
- **WHEN** file with MD5 "def456" fails (corrupt file)
- **THEN** file_status row has `status: "failed"`, `error_message: "Corrupt xlsx file: ..."`

### Requirement: Phase-level status API
The system SHALL provide a `GET /batch/{batch_id}/status` endpoint that returns a summary of the batch run's progress across all phases.

#### Scenario: Status of a running batch
- **WHEN** `GET /batch/{batch_id}/status` is called for a running batch
- **THEN** the response includes:
```json
{
  "batch_id": "a1b2c3d4-...",
  "status": "running",
  "started_at": "2026-03-11T10:00:00Z",
  "completed_at": null,
  "strategy_set": ["fullname_ssn", "lastname_dob", "unique_identifiers"],
  "indexing": {
    "total": 500,
    "indexed": 500,
    "failed": 3,
    "skipped": 0
  },
  "searching": {
    "total_customers": 200,
    "completed": 120,
    "failed": 1,
    "pending": 79
  },
  "detection": {
    "total_pairs_processed": 3200,
    "leaks_found": 450
  }
}
```

#### Scenario: Status of a completed batch
- **WHEN** `GET /batch/{batch_id}/status` is called for a completed batch
- **THEN** `status` is "completed" and `completed_at` is populated, all customer counts reflect final totals

#### Scenario: Status of a non-existent batch
- **WHEN** `GET /batch/{batch_id}/status` is called with an invalid batch_id
- **THEN** the system returns 404 with message "Batch not found"

### Requirement: Customer-level status API
The system SHALL provide a `GET /batch/{batch_id}/customers` endpoint that returns per-customer status within a batch.

#### Scenario: Customer status list
- **WHEN** `GET /batch/{batch_id}/customers` is called
- **THEN** the response is an array of customer status objects:
```json
[
  {
    "customer_id": 1,
    "status": "complete",
    "candidates_found": 5,
    "leaks_confirmed": 3,
    "strategies_matched": ["fullname_ssn", "unique_identifiers"],
    "processed_at": "2026-03-11T10:02:15Z"
  },
  {
    "customer_id": 2,
    "status": "complete",
    "candidates_found": 0,
    "leaks_confirmed": 0,
    "strategies_matched": [],
    "processed_at": "2026-03-11T10:02:18Z"
  },
  {
    "customer_id": 3,
    "status": "failed",
    "candidates_found": 0,
    "leaks_confirmed": 0,
    "strategies_matched": [],
    "error_message": "Azure Search timeout after 30s",
    "processed_at": "2026-03-11T10:02:45Z"
  },
  {
    "customer_id": 4,
    "status": "pending",
    "candidates_found": 0,
    "leaks_confirmed": 0,
    "strategies_matched": []
  }
]
```

#### Scenario: Filter customers by status
- **WHEN** `GET /batch/{batch_id}/customers?status=failed` is called
- **THEN** only customers with status "failed" are returned

### Requirement: Batch results API
The system SHALL provide a `GET /batch/{batch_id}/results` endpoint that returns all result rows for a batch.

#### Scenario: Get all results for a batch
- **WHEN** `GET /batch/{batch_id}/results` is called
- **THEN** all result rows for that batch are returned, ordered by customer_id then overall_confidence descending

#### Scenario: Filter results by customer
- **WHEN** `GET /batch/{batch_id}/results?customer_id=42` is called
- **THEN** only results for customer 42 in that batch are returned

### Requirement: Batch resume API
The system SHALL provide a `POST /batch/{batch_id}/resume` endpoint to resume an interrupted batch. It skips completed customers, retries failed customers, and continues with pending customers. Returns 400 if the batch is already completed.

#### Scenario: Resume an interrupted batch
- **WHEN** `POST /batch/{batch_id}/resume` is called on a batch with status "running" or "failed"
- **THEN** processing resumes: completed customers are skipped, failed customers are retried, pending customers are processed

#### Scenario: Resume a completed batch
- **WHEN** `POST /batch/{batch_id}/resume` is called on a batch with status "completed"
- **THEN** the system returns 400 with message "Batch already completed"

### Requirement: Batch list API
The system SHALL provide a `GET /batches` endpoint that lists all batch runs with their summary status.

#### Scenario: List all batches
- **WHEN** `GET /batches` is called
- **THEN** all batch runs are returned, ordered by `started_at` descending, each with batch_id, status, started_at, completed_at, total_customers, and strategy count

### Requirement: Console logging during batch processing
In addition to database status tracking, the system SHALL log progress to the console (stdout) during batch processing for real-time monitoring.

#### Scenario: Indexing progress logged
- **WHEN** files are being indexed
- **THEN** log messages include: `"Indexing: 50/500 files processed (3 failed)"`

#### Scenario: Customer processing logged
- **WHEN** each customer finishes processing
- **THEN** a log message includes: `"Customer 42/200: 5 candidates, 3 leaks confirmed (fullname_ssn, unique_identifiers)"`

#### Scenario: Batch completion logged
- **WHEN** the batch completes
- **THEN** a summary log includes: `"Batch complete: 200 customers, 1500 total leaks across 180 files, 2 customers failed"`

#### Scenario: Errors logged with context
- **WHEN** a customer fails during processing
- **THEN** the error is logged with customer_id, phase (searching/detecting), and the error message
