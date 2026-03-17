## ADDED Requirements

### Requirement: Batch status query
The system SHALL provide a `get_batch_status(db, batch_id)` function in `app/services/batch_query_service.py` that queries the `[Batch].[batch_runs]` table and returns batch metadata as a dictionary, or `None` if the batch does not exist.

#### Scenario: Query existing batch
- **WHEN** `get_batch_status(db, "abc123")` is called and batch "abc123" exists
- **THEN** the function returns a dictionary with keys: `batch_id`, `status`, `started_at`, `completed_at`, `strategy_set`, `total_customers`, `completed_customers`, `failed_customers`

#### Scenario: Query non-existent batch
- **WHEN** `get_batch_status(db, "nonexistent")` is called and no batch with that ID exists
- **THEN** the function returns `None`

#### Scenario: Batch status includes customer counts
- **WHEN** `get_batch_status(db, "abc123")` is called and the batch has 10 customers (8 complete, 1 failed, 1 pending)
- **THEN** the returned dictionary includes `total_customers: 10`, `completed_customers: 8`, `failed_customers: 1`

### Requirement: Customer statuses query
The system SHALL provide a `get_customer_statuses(db, batch_id, status_filter=None)` function that returns a list of per-customer status dictionaries for a given batch, or `None` if the batch does not exist. An optional `status_filter` parameter filters to only customers with that status (e.g., "complete", "failed").

#### Scenario: Get all customer statuses
- **WHEN** `get_customer_statuses(db, "abc123")` is called without a filter
- **THEN** the function returns a list of dictionaries, one per customer, each with: `customer_id`, `status`, `candidates_found`, `leaks_confirmed`, `error_message`

#### Scenario: Filter by failed status
- **WHEN** `get_customer_statuses(db, "abc123", status_filter="failed")` is called
- **THEN** only customers with `status = "failed"` are returned

#### Scenario: Batch does not exist
- **WHEN** `get_customer_statuses(db, "nonexistent")` is called
- **THEN** the function returns `None`

### Requirement: Batch results query
The system SHALL provide a `get_batch_results(db, batch_id, customer_id=None)` function that returns result rows from `[Search].[results]` for a given batch. An optional `customer_id` parameter filters to a specific customer. Returns `None` if the batch does not exist.

#### Scenario: Get all results for a batch
- **WHEN** `get_batch_results(db, "abc123")` is called without customer filter
- **THEN** the function returns a list of dictionaries, one per result row, each with: `customer_id`, `md5`, `strategy_name`, `leaked_fields`, `overall_confidence`, `needs_review`

#### Scenario: Get results for specific customer
- **WHEN** `get_batch_results(db, "abc123", customer_id=42)` is called
- **THEN** only result rows where `customer_id = 42` are returned

#### Scenario: No results for batch
- **WHEN** `get_batch_results(db, "abc123")` is called and no results exist for that batch
- **THEN** the function returns an empty list `[]`

#### Scenario: Batch does not exist
- **WHEN** `get_batch_results(db, "nonexistent")` is called
- **THEN** the function returns `None`

### Requirement: List all batches
The system SHALL provide a `list_all_batches(db)` function that returns a list of all batch runs ordered by `started_at` descending (most recent first).

#### Scenario: List batches with results
- **WHEN** `list_all_batches(db)` is called and 3 batch runs exist
- **THEN** the function returns a list of 3 dictionaries, each with `batch_id`, `status`, `started_at`, `completed_at`, `strategy_set`, ordered by `started_at` descending

#### Scenario: No batches exist
- **WHEN** `list_all_batches(db)` is called and no batch runs exist
- **THEN** the function returns an empty list `[]`

### Requirement: Zero FastAPI dependency
All functions in `batch_query_service.py` SHALL have zero dependency on FastAPI. They accept a SQLAlchemy `Session` as the `db` parameter and return plain Python dictionaries. They do NOT use `Depends()`, `Request`, `Response`, or any FastAPI types.

#### Scenario: Import without FastAPI installed
- **GIVEN** FastAPI is not installed in the environment
- **WHEN** `from app.services.batch_query_service import get_batch_status` is executed
- **THEN** the import succeeds without error
