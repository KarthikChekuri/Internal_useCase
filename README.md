# Breach PII Search

Search breach files for customer PII using Azure AI Search and fuzzy matching.

## Prerequisites

- Python 3.12+
- [Poetry](https://python-poetry.org/docs/#installation)

## Quick Start

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd breach-search
   ```

2. Copy `.env.example` to `.env` and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

3. Install Python dependencies:
   ```bash
   poetry install
   ```

4. Seed the database with master customer data:
   ```bash
   poetry run breach-search seed
   ```

5. Index breach files into Azure AI Search:
   ```bash
   poetry run breach-search index
   ```

6. Run a batch processing pass:
   ```bash
   poetry run breach-search run
   ```

## Database

Uses **Azure PostgreSQL** (`datasense` database on `datasense-prod-pg-restored.postgres.database.azure.com`).

Tables are organized across five schemas — `PII`, `DLU`, `Batch`, `Index`, `Search` — to keep breach search data isolated from other projects sharing the same database.

| Schema | Table | Description |
|---|---|---|
| `PII` | `master_data` | Customer PII records (customer_id INT PK) |
| `DLU` | `datalakeuniverse` | Breach file metadata (MD5 PK + file_path) |
| `Batch` | `batch_runs` | Top-level batch execution tracking |
| `Batch` | `customer_status` | Per-customer processing status within a batch |
| `Index` | `file_status` | Azure AI Search indexing status per file |
| `Search` | `results` | PII detection results per (customer, file) pair |

## Environment Variables

| Variable | Description | Required |
|---|---|---|
| `DATABASE_URL` | Full SQLAlchemy connection string (postgresql+psycopg2 dialect) | Required |
| `POSTGRES_SERVER` | PostgreSQL hostname | Required |
| `POSTGRES_PORT` | PostgreSQL port (default: 5432) | Required |
| `POSTGRES_DB` | Database name | Required |
| `POSTGRES_USER` | Database username | Required |
| `POSTGRES_PASSWORD` | Database password | Required |
| `AZURE_SEARCH_ENDPOINT` | Azure AI Search service endpoint URL | Required |
| `AZURE_SEARCH_KEY` | Azure AI Search admin key | Required |
| `AZURE_SEARCH_INDEX` | Name of the V2 search index | Required |
| `AZURE_SEARCH_INDEX_V3` | Name of the V3 search index | Required |
| `FILE_BASE_PATH` | Path to the directory containing breach text files | Optional |

## CLI Commands

All commands are invoked via `poetry run breach-search` or `python -m app`.

Add `--verbose` before any subcommand to enable DEBUG-level logging.

### generate

Generate simulated breach files for testing:

```bash
poetry run breach-search generate
```

### seed

Seed the database with master customer data and DLU metadata:

```bash
poetry run breach-search seed
```

### index

Create the Azure AI Search index and index all eligible breach files:

```bash
# V2 indexing (default)
poetry run breach-search index

# V3 indexing pipeline
poetry run breach-search index --v3
```

### run

Run a full batch processing pass against all customers:

```bash
# V2 batch (default)
poetry run breach-search run

# V3 batch (Azure AI Search only, no local strategies)
poetry run breach-search run --v3

# V2 batch with a custom strategies file
poetry run breach-search run --strategies path/to/strategies.yaml
```

### status

Query the status of a batch run (prints JSON):

```bash
# Summary status for a batch
poetry run breach-search status <BATCH_ID>

# Include per-customer status entries
poetry run breach-search status <BATCH_ID> --customers
```

### compare

Compare V2 and V3 batch results side-by-side:

```bash
poetry run breach-search compare <V2_BATCH_ID> <V3_BATCH_ID>
```

## Testing

Run the full test suite:

```bash
poetry run pytest
```

Tests live in `tests/` and mirror the `app/` structure. All external dependencies (PostgreSQL, Azure AI Search) are mocked in unit tests.

## Project Structure

```
breach-search/
├── app/
│   ├── cli.py          # Click CLI entry point
│   ├── config.py       # Pydantic settings
│   ├── models/         # SQLAlchemy models
│   ├── services/       # Business logic (batch, indexing, query)
│   └── utils/          # Helpers (fuzzy matching, strategy loader)
├── tests/              # Unit tests mirroring app/ structure
├── scripts/            # Data generation, seeding, index creation
├── data/
│   ├── seed/           # Master customer CSV files
│   ├── simulated_files/# Generated test breach files
│   └── TEXT/           # Actual breach text files
├── openspec/           # Feature specifications
├── plans/              # Roadmap and completed phase archive
├── strategies.yaml     # Default search strategies for V2 batch
├── pyproject.toml
└── .env.example
```
