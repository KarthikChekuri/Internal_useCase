## ADDED Requirements

### Requirement: Poetry project configuration
The system SHALL use Poetry for dependency management with a `pyproject.toml` file at the project root. The `pyproject.toml` SHALL define all runtime and development dependencies, a `breach-search` script entry point, and pytest configuration.

#### Scenario: Install dependencies with Poetry
- **WHEN** `poetry install` is executed in the project root
- **THEN** all runtime and development dependencies are installed into a virtual environment, and `poetry.lock` is generated (or verified against existing lock)

#### Scenario: Run CLI via Poetry
- **WHEN** `poetry run breach-search --help` is executed after `poetry install`
- **THEN** the Click CLI help text is displayed, confirming the entry point is correctly wired to `app.cli:main`

#### Scenario: Run tests via Poetry
- **WHEN** `poetry run pytest` is executed
- **THEN** pytest discovers and runs all tests in the `tests/` directory using the configuration from `[tool.pytest.ini_options]` in `pyproject.toml`

#### Scenario: Reproducible builds via lock file
- **WHEN** `poetry install` is executed on a different machine with the same `pyproject.toml` and `poetry.lock`
- **THEN** the exact same dependency versions are installed as on the original machine

### Requirement: Runtime dependencies
The `pyproject.toml` SHALL declare these runtime dependencies:
- `python ^3.12`
- `sqlalchemy ^2.0` (with asyncio extras)
- `psycopg2-binary ^2.9`
- `rapidfuzz ^3.0`
- `azure-search-documents ^11.4`
- `openpyxl ^3.1`
- `xlrd ^2.0`
- `xlwt ^1.3`
- `pydantic-settings ^2.0`
- `python-dotenv ^1.0`
- `pyyaml ^6.0`
- `click ^8.1`

#### Scenario: No FastAPI or uvicorn in dependencies
- **WHEN** the `pyproject.toml` is inspected
- **THEN** neither `fastapi` nor `uvicorn` appear in any dependency section

### Requirement: Development dependencies
The `pyproject.toml` SHALL declare these dev-only dependencies under `[tool.poetry.group.dev.dependencies]`:
- `pytest ^8.0`
- `pytest-mock ^3.12`

#### Scenario: Dev dependencies not installed in production
- **WHEN** `poetry install --without dev` is executed
- **THEN** pytest and pytest-mock are not installed

### Requirement: requirements.txt removal
The `requirements.txt` file SHALL be deleted from the project root. Poetry's `pyproject.toml` and `poetry.lock` replace it entirely.

#### Scenario: No requirements.txt exists
- **WHEN** the project root is listed
- **THEN** `requirements.txt` does not exist

### Requirement: Dockerfile for containerized execution
The system SHALL provide a `Dockerfile` at the project root that builds a container image capable of running all CLI commands. The image SHALL use `python:3.12-slim` as the base, install Poetry, copy project files, install dependencies, and set the entry point to `breach-search`.

#### Scenario: Build Docker image
- **WHEN** `docker build -t breach-search .` is executed
- **THEN** the image builds successfully with all Python dependencies installed

#### Scenario: Run CLI command in container
- **WHEN** `docker run breach-search --help` is executed
- **THEN** the Click CLI help text is displayed (same as running locally)

#### Scenario: Run seed in container
- **WHEN** `docker run --env-file .env breach-search seed` is executed with a reachable PostgreSQL server
- **THEN** the database is seeded with master data and DLU metadata

### Requirement: Docker Compose for full stack
The system SHALL provide a `docker-compose.yml` at the project root that defines the `app` service (the breach-search CLI) configured to connect to Azure PostgreSQL. The database is hosted externally on Azure PostgreSQL (datasense-prod-pg-restored.postgres.database.azure.com) and does not require a local container.

#### Scenario: App container connects to Azure PostgreSQL
- **WHEN** `docker-compose up app` is executed with correct `.env` values for `POSTGRES_SERVER`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`
- **THEN** the app container starts and connects to the Azure PostgreSQL database

#### Scenario: Data volume is mounted
- **WHEN** `docker-compose up app` is executed
- **THEN** the local `./data` directory is mounted to `/app/data` in the app container, making breach files and seed data accessible

### Requirement: README with setup instructions
The system SHALL provide a `README.md` at the project root with step-by-step instructions for both Docker-based and local development setup. The README SHALL include: prerequisites, Quick Start (Docker), Quick Start (Local), environment variables, CLI command reference, and how to run tests.

#### Scenario: Docker Quick Start works end-to-end
- **GIVEN** a machine with Docker and Poetry installed
- **WHEN** a user follows the Docker Quick Start steps in README (clone, copy .env, poetry install, breach-search seed, breach-search index, breach-search run)
- **THEN** the database is seeded, files are indexed, and a batch run completes successfully

#### Scenario: Local Quick Start works end-to-end
- **GIVEN** a machine with Python 3.12+ and Poetry installed, with access to Azure PostgreSQL
- **WHEN** a user follows the Local Quick Start steps in README (poetry install, copy .env, breach-search seed, breach-search index, breach-search run)
- **THEN** the database is seeded, files are indexed, and a batch run completes successfully

#### Scenario: Environment variables documented
- **WHEN** a user reads the Environment Variables section of README
- **THEN** they find documentation for: `DATABASE_URL`, `AZURE_SEARCH_ENDPOINT`, `AZURE_SEARCH_KEY`, `AZURE_SEARCH_INDEX`, `AZURE_SEARCH_INDEX_V3`, `FILE_BASE_PATH`, `POSTGRES_SERVER`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

#### Scenario: CLI commands documented
- **WHEN** a user reads the CLI Commands section of README
- **THEN** they find usage examples for all commands: `generate`, `seed`, `index`, `index --v3`, `run`, `run --v3`, `run --strategies FILE`, `status BATCH_ID`, `compare V2_ID V3_ID`

### Requirement: .env.example file
The system SHALL provide a `.env.example` file listing all required and optional environment variables with placeholder values and comments explaining each one.

#### Scenario: Copy .env.example to .env
- **WHEN** a user runs `cp .env.example .env` and fills in their Azure credentials and database URL
- **THEN** the application reads the `.env` file via pydantic-settings and connects successfully

## REMOVED Requirements

### Requirement: requirements.txt dependency management
The system SHALL NOT use `requirements.txt` for dependency management. Poetry replaces it entirely.

#### Scenario: pip install not required
- **WHEN** setting up the project
- **THEN** the README instructs users to use `poetry install`, not `pip install -r requirements.txt`
