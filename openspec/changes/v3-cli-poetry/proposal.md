## Why

The breach-search app currently uses FastAPI with REST endpoints (`POST /batch/run`, `GET /batch/{id}/status`, etc.) but all operations are run locally by a single developer — there are no external consumers of the API. The HTTP layer adds unnecessary complexity (uvicorn server, FastAPI dependency injection, Pydantic request/response schemas, CORS, BackgroundTasks) without providing value. Meanwhile, the app lacks portable setup: no lock file, no containerization, no step-by-step instructions for running on a fresh machine.

The service layer (`batch_service`, `search_service`, `leak_detection_service`, etc.) is already well-separated — services accept `db` and `search_client` as parameters with zero FastAPI dependency. The API layer is a thin wrapper that can be replaced with CLI commands.

## What Changes

- **Remove FastAPI**: Delete all routers, API schemas, dependency injection, `main.py`, `run_batch.py`
- **Add Click CLI**: Create `app/cli.py` with subcommands (`generate`, `seed`, `index`, `run`, `status`, `compare`) that call existing services directly
- **Extract batch query helpers**: Move pure DB query functions from `app/routers/batch.py` into `app/services/batch_query_service.py` before deleting routers
- **Relocate domain model**: Move `app/schemas/pii.py` to `app/models/pii.py` (it's a domain model used by services, not an API schema)
- **Add Poetry**: Replace `requirements.txt` with `pyproject.toml` + `poetry.lock` for reproducible builds
- **Add Docker**: `Dockerfile` + `docker-compose.yml` (connects to Azure PostgreSQL) for portable setup
- **Add README**: Step-by-step setup for both Docker and local development

## Capabilities

### New Capabilities
- `cli-interface`: Click-based CLI with subcommands for all operations (generate, seed, index, run, status, compare)
- `batch-query-service`: Extracted DB query functions (batch status, customer statuses, batch results, list batches) usable from CLI
- `packaging-deployment`: Poetry dependency management, Docker containerization, README documentation

### Removed Capabilities
- FastAPI REST API endpoints (all `POST`/`GET` routes)
- FastAPI dependency injection (`get_db`, `get_search_client`, `get_settings`)
- API-specific Pydantic schemas (`BatchRunRequest`, `BatchStatusResponse`, `IndexRequest`, etc.)
- `run_batch.py` standalone script (replaced by `breach-search run`)

### Modified Capabilities
- `app/models/database.py`: Remove FastAPI-specific `get_db()` generator
- `app/services/leak_detection_service.py`: Update import path for relocated `pii.py`

## Impact

- **No new Python runtime dependencies** except `click` (CLI framework)
- **Removed dependencies**: `fastapi`, `uvicorn`
- **No changes to**: service layer, utils, models (except database.py import cleanup), scripts, strategies, SQL schema
- **Test impact**: Delete API/router/schema tests, add CLI tests and batch query service tests
- **Existing service tests**: Unchanged — they don't depend on the API layer

## Delta from Current

| Aspect | Current | After |
|---|---|---|
| Entry point | `uvicorn app.main:app` or `python run_batch.py` | `breach-search <command>` via Click |
| Dependency management | `requirements.txt` (no lock) | `pyproject.toml` + `poetry.lock` |
| Batch trigger | `POST /batch/run` or `python run_batch.py` | `breach-search run` |
| Batch status | `GET /batch/{id}/status` | `breach-search status <id>` |
| Indexing | `POST /index/all` | `breach-search index` |
| DB seeding | `python scripts/seed_database.py` | `breach-search seed` |
| Containerization | None | `docker-compose.yml` with Azure PostgreSQL |
| Setup docs | None | `README.md` with Docker + local instructions |
