## 1. Poetry Setup

- [ ] 1.1 Create `pyproject.toml` with all runtime deps (sqlalchemy, pyodbc, rapidfuzz, azure-search-documents, openpyxl, xlrd, xlwt, pydantic-settings, python-dotenv, pyyaml, click), dev deps (pytest, pytest-mock), entry point `breach-search = "app.cli:main"`, and pytest config
- [ ] 1.2 Delete `requirements.txt`
- [ ] 1.3 Run `poetry install` to verify deps resolve and lock file generates

## 2. Relocate Domain Model

- [ ] 2.1 Move `app/schemas/pii.py` → `app/models/pii.py`
- [ ] 2.2 Update import in `app/services/leak_detection_service.py`: `from app.models.pii import FieldMatchResult`
- [ ] 2.3 Move `tests/schemas/test_pii.py` → `tests/models/test_pii.py` (update imports)
- [ ] 2.4 Run tests for relocated module

## 3. Extract Batch Query Service

- [ ] 3.1 Create `app/services/batch_query_service.py` with functions extracted from `app/routers/batch.py` (lines 48-307): `get_batch_status(db, batch_id)`, `get_customer_statuses(db, batch_id, status_filter)`, `get_batch_results(db, batch_id, customer_id)`, `list_all_batches(db)`
- [ ] 3.2 Write tests `tests/services/test_batch_query_service.py` — mock DB session, verify return shapes, test None/empty cases
- [ ] 3.3 Run batch query service tests

## 4. Create CLI Entry Point

- [ ] 4.1 Create `app/cli.py` with Click group `main` and `--verbose` flag
- [ ] 4.2 Implement `_build_db_session()` helper (Settings → engine → session)
- [ ] 4.3 Implement `_build_search_client(settings, v3=False)` helper (AzureKeyCredential → SearchClient)
- [ ] 4.4 Implement `generate` command (calls `scripts.generate_simulated_data.main()`)
- [ ] 4.5 Implement `seed` command (calls `scripts.seed_database.main()`)
- [ ] 4.6 Implement `index` command with `--v3` flag (create index + index files)
- [ ] 4.7 Implement `run` command with `--v3` and `--strategies` flags (batch processing)
- [ ] 4.8 Implement `status` command with `BATCH_ID` argument and `--customers` flag
- [ ] 4.9 Implement `compare` command with `V2_BATCH_ID` and `V3_BATCH_ID` arguments
- [ ] 4.10 Create `app/__main__.py` (enables `python -m app`)
- [ ] 4.11 Write tests `tests/test_cli.py` — use Click `CliRunner`, mock service calls, verify output and exit codes
- [ ] 4.12 Run CLI tests

## 5. Delete API Layer

- [ ] 5.1 Delete `app/main.py` (FastAPI app instance)
- [ ] 5.2 Delete `app/dependencies.py` (FastAPI DI)
- [ ] 5.3 Delete `app/routers/__init__.py`, `app/routers/batch.py`, `app/routers/batch_v3.py`, `app/routers/indexing.py`
- [ ] 5.4 Delete `app/schemas/__init__.py`, `app/schemas/batch.py`, `app/schemas/indexing.py`, `app/schemas/search_v3.py`
- [ ] 5.5 Delete `run_batch.py`
- [ ] 5.6 Delete `tests/routers/` (entire directory)
- [ ] 5.7 Delete `tests/schemas/` (remaining files after pii.py relocation)
- [ ] 5.8 Delete `tests/test_dependencies.py`, `tests/test_main.py`, `tests/test_main_v2.py`
- [ ] 5.9 Delete `tests/test_integration.py`, `tests/test_v2_integration.py`, `tests/test_v3_integration.py`
- [ ] 5.10 Remove `get_db()` generator from `app/models/database.py` (lines 53-68)
- [ ] 5.11 Update `CLAUDE.md` tech stack (remove FastAPI/uvicorn, add Click/Poetry)
- [ ] 5.12 Run full test suite to verify nothing is broken

## 6. Docker + README

- [ ] 6.1 Create `Dockerfile` (python:3.12-slim, Poetry install, entry point)
- [ ] 6.2 Create `docker-compose.yml` (SQL Server 2022 + app services)
- [ ] 6.3 Update `.env.example` with all required environment variables
- [ ] 6.4 Create `README.md` with Docker Quick Start, Local Quick Start, env vars, CLI reference, testing instructions
- [ ] 6.5 Verify Docker build succeeds

## 7. Verify

- [ ] 7.1 `poetry install` — deps resolve, lock file valid
- [ ] 7.2 `poetry run pytest` — all remaining tests pass
- [ ] 7.3 `poetry run breach-search --help` — CLI help text works
- [ ] 7.4 Verify no imports of `fastapi`, `uvicorn`, or deleted modules remain
