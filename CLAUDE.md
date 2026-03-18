# Breach PII Search — Project Rules

## Development Approach: Test-Driven Development (TDD)

**This project uses strict TDD. No production code is written without a failing test first.**

### The TDD cycle for every task:
1. **Red** — Write a failing test that defines the expected behavior
2. **Green** — Write the minimum production code to make the test pass
3. **Refactor** — Clean up, keeping all tests green

### Rules
- Write tests BEFORE implementation code, not after
- Each spec scenario in `openspec/changes/breach-pii-search/specs/` maps directly to one or more tests — use the Given/When/Then scenarios as your test cases
- Tests live in `tests/` mirroring the `app/` structure:
  - `app/services/leak_detection_service.py` → `tests/services/test_leak_detection_service.py`
  - `app/utils/confidence.py` → `tests/utils/test_confidence.py`
  - etc.
- Run tests with `pytest` before marking a phase complete
- All tests must pass before a phase is archived

### What counts as a test
- Unit tests: one function/class at a time, mock external dependencies (PostgreSQL, Azure Search)
- Integration tests (Batch 6 only): real DB + real Azure Search, using simulated data

### Mocking rules
- Mock `azure.search.documents` — never hit real Azure AI Search in unit tests
- Mock `sqlalchemy` sessions — never hit real PostgreSQL in unit tests
- Use `pytest` fixtures for shared setup
- Use `pytest-mock` or `unittest.mock` for mocking

---

## Project Structure

```
breach-search/
├── app/
│   ├── cli.py          ← Click CLI entry point (replaces FastAPI)
│   ├── config.py
│   ├── models/
│   ├── services/
│   └── utils/
├── tests/
│   ├── conftest.py
│   ├── models/
│   ├── services/
│   └── utils/
├── scripts/
├── data/
│   ├── seed/
│   ├── simulated_files/
│   └── TEXT/
├── plans/
│   ├── roadmap.md
│   └── completed/
├── openspec/
├── orchestrator.sh
└── CLAUDE.md
```

## Specs

- **V2 specs:** `openspec/changes/breach-pii-search/specs/` — V2 batch processing with Python regex/fuzzy detection
- **V3 specs:** `openspec/changes/v3-azure-only/specs/` — V3 Azure-only alternate route (indexing-v3, search-v3, batch-v3)

Read the relevant spec before starting any phase. Each Given/When/Then scenario is a test case.

## Orchestrator

The orchestrator automates agent launch. It reads the roadmap, resolves dependencies, reads specs, and generates complete agent prompts. **Single entry point: `orchestrator.sh`** (no separate .py file).

```bash
bash orchestrator.sh status                    # Show work board
bash orchestrator.sh plan                      # Output JSON manifest with agent prompts
bash orchestrator.sh complete "Phase V2-X.Y"   # Mark done, unblock dependents
bash orchestrator.sh complete "Phase V3-X.Y"   # Also supports V3 phases
bash orchestrator.sh complete "Phase V4-X.Y"   # Also supports V4 phases
```

### Launch workflow (when user says "orchestrate" or "launch")
1. Run `bash orchestrator.sh plan`
2. Parse the JSON output
3. For each phase in `launchable_phases`, launch an Agent (background, isolated worktree) with the provided `prompt`
4. After each agent completes, run `bash orchestrator.sh complete "<phase_id>"`
5. Re-run `plan` to check for newly unblocked phases
6. Repeat until no more launchable phases

## Roadmap

Active work is tracked in `plans/roadmap.md`.

## Tech Stack

- Python 3.12+, Click, Poetry, SQLAlchemy 2.0, psycopg2-binary
- Azure AI Search (`azure-search-documents`)
- rapidfuzz, openpyxl, xlrd, pydantic-settings
- pytest, pytest-mock
- Azure PostgreSQL (datasense-prod-pg-restored.postgres.database.azure.com)

## CLI (V4)

The FastAPI/uvicorn HTTP layer has been removed. The application is now a
Click CLI invoked via `python -m app` or `breach-search` (Poetry script).

Key commands live in `app/cli.py`. No API endpoints exist — all interaction
is via CLI commands and direct Python calls.
