## Context

The breach-search app's REST API layer (FastAPI, uvicorn) serves no external consumers — all operations are triggered locally. The service layer is already cleanly separated from the API, accepting `db` and `search_client` as parameters. This change removes the unnecessary HTTP layer and replaces it with a Click CLI, adds Poetry for reproducible dependency management, and adds Docker + README for portable setup.

## Goals / Non-Goals

**Goals:**
- Replace all API endpoints with CLI commands that call the same service layer
- Provide reproducible builds via Poetry (`pyproject.toml` + `poetry.lock`)
- Provide one-command setup via Docker Compose (SQL Server + app)
- Maintain all existing service functionality unchanged
- Keep spec-driven, test-driven development approach (tests for CLI commands)

**Non-Goals:**
- Change any service logic (batch processing, search, leak detection, scoring)
- Modify SQL schema or data models
- Add new features beyond CLI/Poetry/Docker
- Change the file path resolution system (`FILE_BASE_PATH` + relative path from DLU)

## Decisions

### Decision 1: Click for CLI framework

Click was chosen over alternatives:
- **vs argparse**: Click has better subcommand support, automatic `--help`, and cleaner API for multiple commands
- **vs Typer**: Typer wraps Click but adds a dependency on `typing-extensions` — no benefit for our simple command set
- **vs Fire**: Fire auto-generates CLI from functions but gives less control over argument validation

Click is lightweight (~80KB), has zero transitive dependencies, and is the de facto standard for Python CLIs.

### Decision 2: Poetry for dependency management

Poetry provides:
- `pyproject.toml` for all project metadata (replaces `requirements.txt` + `setup.py`)
- `poetry.lock` for reproducible installs across machines
- Virtual environment management built-in
- `poetry run breach-search` as the standard invocation

### Decision 3: Extract batch query helpers before deleting routers

`app/routers/batch.py` contains pure DB query functions (lines 48-307) that have zero FastAPI dependency. These are needed for the `breach-search status` CLI command. They are extracted to `app/services/batch_query_service.py` before the router is deleted.

### Decision 4: Relocate `pii.py` from schemas to models

`app/schemas/pii.py` contains `FieldMatchResult` and `CustomerSummary` — domain models used by the leak detection service, not API request/response schemas. Moving to `app/models/pii.py` reflects their actual purpose and prevents deletion when the `schemas/` directory is removed.

### Decision 5: Docker Compose with SQL Server

The `docker-compose.yml` includes `mcr.microsoft.com/mssql/server:2022-latest` so developers don't need a local SQL Server installation. The app container uses the same Poetry setup for consistency.

## File Changes

### Create
| File | Purpose |
|---|---|
| `pyproject.toml` | Poetry project configuration with all dependencies |
| `app/cli.py` | Click CLI entry point with all subcommands |
| `app/__main__.py` | Enables `python -m app` invocation |
| `app/services/batch_query_service.py` | Extracted DB query functions from batch router |
| `tests/test_cli.py` | Click CliRunner tests for all commands |
| `tests/services/test_batch_query_service.py` | Tests for extracted query functions |
| `Dockerfile` | Multi-stage build for the app |
| `docker-compose.yml` | SQL Server + app containers |
| `README.md` | Setup and usage documentation |

### Move
| From | To |
|---|---|
| `app/schemas/pii.py` | `app/models/pii.py` |
| `tests/schemas/test_pii.py` | `tests/models/test_pii.py` |

### Modify
| File | Change |
|---|---|
| `app/services/leak_detection_service.py` | Update import: `from app.models.pii import FieldMatchResult` |
| `app/models/database.py` | Remove `get_db()` generator (lines 53-68) |
| `CLAUDE.md` | Update tech stack description |

### Delete
| Category | Files |
|---|---|
| API layer | `app/main.py`, `app/dependencies.py` |
| Routers | `app/routers/__init__.py`, `app/routers/batch.py`, `app/routers/batch_v3.py`, `app/routers/indexing.py` |
| API schemas | `app/schemas/__init__.py`, `app/schemas/batch.py`, `app/schemas/indexing.py`, `app/schemas/search_v3.py` |
| Standalone script | `run_batch.py` |
| Old config | `requirements.txt` |
| Router tests | `tests/routers/` (entire directory) |
| Schema tests | `tests/schemas/` (except `test_pii.py` which is relocated) |
| API tests | `tests/test_dependencies.py`, `tests/test_main.py`, `tests/test_main_v2.py` |
| Integration tests | `tests/test_integration.py`, `tests/test_v2_integration.py`, `tests/test_v3_integration.py` |

### Unchanged
All services, models (except `database.py`), utils, scripts, strategies, `conftest.py`, service tests, util tests.
