## ADDED Requirements

### Requirement: CLI entry point via Click
The system SHALL provide a Click-based CLI entry point at `app/cli.py` with a top-level `main` group command. The CLI SHALL be invokable as `breach-search <command>` when installed via Poetry (`poetry run breach-search`) or as `python -m app <command>`. All commands SHALL share a `--verbose` flag that enables DEBUG-level logging.

#### Scenario: Display help text
- **WHEN** `breach-search --help` is executed
- **THEN** the CLI displays a list of all available subcommands (generate, seed, index, run, status, compare) with brief descriptions

#### Scenario: Enable verbose logging
- **WHEN** `breach-search --verbose run` is executed
- **THEN** the root logger is set to DEBUG level before the subcommand executes, and all service-layer log messages at DEBUG level are printed to stdout

#### Scenario: Invoke via python -m
- **WHEN** `python -m app --help` is executed
- **THEN** the same CLI help text is displayed as `breach-search --help` (via `app/__main__.py` calling `main()`)

### Requirement: generate command
The system SHALL provide a `breach-search generate` command that generates simulated breach files by calling the existing `scripts.generate_simulated_data.main()` function.

#### Scenario: Generate simulated data
- **WHEN** `breach-search generate` is executed
- **THEN** the system calls `scripts.generate_simulated_data.main()`, which creates simulated breach files in the `data/simulated_files/` directory, and prints a success message when complete

### Requirement: seed command
The system SHALL provide a `breach-search seed` command that seeds the database with master customer data and DLU metadata by calling the existing `scripts.seed_database.main()` function.

#### Scenario: Seed database
- **WHEN** `breach-search seed` is executed
- **THEN** the system calls `scripts.seed_database.main()`, which creates schemas/tables if needed and inserts rows from `data/seed/master_data.csv` and `data/seed/dlu_metadata.csv` into PostgreSQL

#### Scenario: Seed is idempotent
- **WHEN** `breach-search seed` is executed twice
- **THEN** the second run skips rows that already exist (matched by primary key) and reports 0 rows inserted

### Requirement: index command
The system SHALL provide a `breach-search index` command that creates the Azure AI Search index and indexes all eligible files from the DLU table. The command SHALL accept a `--v3` flag to use the V3 index and indexing pipeline instead of V2.

#### Scenario: Index files using V2 pipeline
- **WHEN** `breach-search index` is executed (without `--v3`)
- **THEN** the system creates the V2 search index if it does not exist, queries all eligible DLU records, extracts text from each file, and uploads documents to the `breach-file-index` Azure AI Search index

#### Scenario: Index files using V3 pipeline
- **WHEN** `breach-search index --v3` is executed
- **THEN** the system creates the V3 search index if it does not exist, queries all eligible DLU records, extracts text with PII metadata enrichment, and uploads documents to the `breach-file-index-v3` Azure AI Search index

#### Scenario: Resumable indexing
- **WHEN** `breach-search index` is executed after a previous interrupted run
- **THEN** files already marked as "indexed" in the `"Index"."file_status"` table are skipped, and only remaining files are processed

### Requirement: run command
The system SHALL provide a `breach-search run` command that executes a full batch processing run. The command SHALL accept `--v3` for V3 pipeline and `--strategies FILE` for a custom strategies YAML file. Without `--strategies`, the default `strategies.yaml` in the project root is used.

#### Scenario: Run V2 batch with default strategies
- **WHEN** `breach-search run` is executed
- **THEN** the system loads strategies from `strategies.yaml`, connects to PostgreSQL and Azure AI Search, calls `batch_service.start_batch(db, search_client, strategies)`, logs progress per customer to stdout, and prints the batch_id when complete

#### Scenario: Run V2 batch with custom strategies
- **WHEN** `breach-search run --strategies custom_strategies.yaml` is executed
- **THEN** the system loads strategies from `custom_strategies.yaml` instead of the default file

#### Scenario: Run V3 batch
- **WHEN** `breach-search run --v3` is executed
- **THEN** the system calls `batch_service_v3.start_batch_v3(db, search_client_v3)` using the V3 search client pointing at `breach-file-index-v3`

#### Scenario: Run batch when another is already running
- **WHEN** `breach-search run` is executed while a batch is already in "running" state
- **THEN** the system prints an error message "A batch is already running (batch_id: ...)" and exits with code 1

### Requirement: status command
The system SHALL provide a `breach-search status BATCH_ID` command that prints the status of a batch run as formatted JSON to stdout.

#### Scenario: Query existing batch status
- **WHEN** `breach-search status abc123-def456` is executed and that batch exists
- **THEN** the system queries `"Batch"."batch_runs"` and `"Batch"."customer_status"`, and prints a JSON object with `batch_id`, `status`, `started_at`, `completed_at`, `total_customers`, `completed_customers`, `failed_customers`

#### Scenario: Query non-existent batch
- **WHEN** `breach-search status nonexistent-id` is executed and no batch with that ID exists
- **THEN** the system prints "Batch not found: nonexistent-id" and exits with code 1

#### Scenario: Query batch with customer details
- **WHEN** `breach-search status abc123 --customers` is executed
- **THEN** the system also includes per-customer status entries (customer_id, status, candidates_found, leaks_confirmed) in the JSON output

### Requirement: compare command
The system SHALL provide a `breach-search compare V2_BATCH_ID V3_BATCH_ID` command that compares results from a V2 batch run against a V3 batch run and prints a comparison summary.

#### Scenario: Compare V2 and V3 batch results
- **WHEN** `breach-search compare batch-v2-id batch-v3-id` is executed
- **THEN** the system calls the existing `scripts.compare_v2_v3` comparison logic and prints a summary showing: files matched by V2 only, files matched by V3 only, files matched by both, and per-field agreement/disagreement

#### Scenario: Compare with non-existent batch
- **WHEN** `breach-search compare valid-id nonexistent-id` is executed
- **THEN** the system prints "Batch not found: nonexistent-id" and exits with code 1

### Requirement: DB session and search client construction
The CLI SHALL construct database sessions and Azure Search clients using helper functions that follow the same patterns as the existing `run_batch.py` (lines 66-83) and `app/dependencies.py` (lines 56-73). These helpers SHALL be private functions within `app/cli.py`, not exported.

#### Scenario: Build DB session from settings
- **WHEN** any CLI command needs a database session
- **THEN** `_build_db_session()` loads `Settings()`, creates an engine via `get_engine(settings.DATABASE_URL)`, creates a session via `get_session_factory(engine)()`, and returns the session

#### Scenario: Build V2 search client from settings
- **WHEN** a CLI command needs the V2 Azure Search client
- **THEN** `_build_search_client(settings)` creates an `AzureKeyCredential` from `settings.AZURE_SEARCH_KEY` and returns a `SearchClient` configured with `settings.AZURE_SEARCH_ENDPOINT` and `settings.AZURE_SEARCH_INDEX`

#### Scenario: Build V3 search client from settings
- **WHEN** a CLI command needs the V3 Azure Search client (via `--v3` flag)
- **THEN** `_build_search_client(settings, v3=True)` returns a `SearchClient` configured with `settings.AZURE_SEARCH_INDEX_V3` instead of `settings.AZURE_SEARCH_INDEX`

### Requirement: Error handling and exit codes
The CLI SHALL catch service-layer exceptions, print user-friendly error messages to stderr, and exit with appropriate codes. The CLI SHALL NOT print Python tracebacks unless `--verbose` is enabled.

#### Scenario: Database connection failure
- **WHEN** `breach-search run` is executed but the PostgreSQL server is unreachable
- **THEN** the system prints "Error: Could not connect to database. Check POSTGRES_SERVER and DATABASE_URL in your .env file." and exits with code 1

#### Scenario: Azure Search connection failure
- **WHEN** `breach-search index` is executed but Azure Search credentials are invalid
- **THEN** the system prints "Error: Azure Search authentication failed. Check AZURE_SEARCH_KEY and AZURE_SEARCH_ENDPOINT in your .env file." and exits with code 1

#### Scenario: Verbose mode shows traceback
- **WHEN** `breach-search --verbose run` is executed and a service error occurs
- **THEN** the full Python traceback is printed in addition to the user-friendly error message

## REMOVED Requirements

### Requirement: FastAPI REST API endpoints
The system SHALL NOT provide HTTP REST API endpoints. The following endpoints are removed:
- `POST /batch/run` — replaced by `breach-search run`
- `GET /batch/{batch_id}/status` — replaced by `breach-search status`
- `GET /batch/{batch_id}/results` — replaced by `breach-search status --customers`
- `GET /batch/` — replaced by `breach-search status` (list all)
- `POST /index/all` — replaced by `breach-search index`
- `POST /index/{md5}` — no CLI equivalent (not needed)
- `POST /v3/batch/run` — replaced by `breach-search run --v3`
- `GET /v3/batch/{id}/status` — replaced by `breach-search status`
- `GET /v3/batch/{id}/results` — replaced by `breach-search status --customers`

#### Scenario: No HTTP server
- **WHEN** the breach-search application is installed
- **THEN** there is no `uvicorn` or HTTP server component; all operations are invoked via CLI commands

### Requirement: FastAPI dependency injection
The system SHALL NOT use FastAPI dependency injection (`Depends(get_db)`, `Depends(get_search_client)`). Database sessions and search clients are constructed directly in CLI helper functions.

#### Scenario: No FastAPI dependencies module
- **WHEN** the application is installed
- **THEN** `app/dependencies.py` does not exist and no code imports from it

### Requirement: run_batch.py standalone script
The `run_batch.py` script is removed. Its functionality is replaced by `breach-search run`.

#### Scenario: No standalone batch script
- **WHEN** a user looks for `run_batch.py` in the project root
- **THEN** the file does not exist; the README directs users to `breach-search run` instead
