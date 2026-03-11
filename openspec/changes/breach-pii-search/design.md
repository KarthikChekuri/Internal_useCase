## Context

The organization needs to identify which customers' PII appears in breach data files discovered during investigations. Breach files (txt, xlsx, csv, xls) sit in a file system indexed by a DLU (Data Lake Universe) table in SQL Server with MD5 hash as the primary key. There is no automated system to search these files against known customer PII records. Azure AI Search is already provisioned and available. File volumes can reach 1TB with tens of thousands of files.

This is V2 — replacing V1's single-customer on-demand search with automated batch processing across all customers using configurable search strategies.

All data will be simulated (10 customers, ~25 files) for initial development and testing.

## Goals / Non-Goals

**Goals:**
- Automate PII detection across ALL breach files for ALL customers in a single batch run
- Configurable search strategies (which PII fields to search with) via YAML config file
- Multiple strategies per customer with union of results for maximum accuracy
- Per-field leak detection with confidence scores per file
- Index breach files into Azure AI Search with multiple analyzer configurations
- Resumable indexing and batch processing (crash recovery without re-doing completed work)
- Full status tracking at phase-level and customer-level with API endpoints
- Persist batch results with audit trail (batch_id, strategy used, timestamps)
- Generate realistic simulated data for end-to-end testing

**Non-Goals:**
- Real-time streaming or continuous monitoring (batch-only)
- Image/video file processing
- Nickname detection or initial-pattern matching (future enhancement)
- File chunking (simulated files are 500–3000 chars, well under 32KB Azure limit)
- UI/frontend (API-only)
- Encrypted file handling
- Customer notification letter generation (raw data output only)

## Decisions

### Decision 1: Three-field Azure AI Search index (same text, different analyzers)

Azure AI Search requires one analyzer per field. To support three matching strategies (exact/fuzzy, phonetic, accent-insensitive), the same extracted text is stored in three fields:

| Field | Analyzer | Purpose |
|---|---|---|
| `content` | `standard.lucene` | Exact keywords + Lucene fuzzy (~1, ~2) |
| `content_phonetic` | Custom `phonetic_analyzer` | Names that sound alike (Smith/Smyth) |
| `content_lowercase` | Custom `name_analyzer` | Accent/case-insensitive matching |

**Custom analyzer definitions:**
- `phonetic_analyzer`: standard tokenizer → lowercase filter → asciifolding filter → phonetic filter (Double Metaphone, `replace: false` to keep original tokens alongside phonetic codes)
- `name_analyzer`: standard tokenizer → lowercase filter → asciifolding filter (handles accented characters like é→e)

**Scoring profile** (`pii_boost`): `content` weight 3.0, `content_lowercase` 2.0, `content_phonetic` 1.5. Standard content gets highest weight because exact matches are most valuable.

### Decision 2: Three-tier leak detection engine (regex → normalized → fuzzy)

Post-search, each file's text is scanned for all 13 PII fields using a cascade:

```
Tier 1: Exact regex (SSN, DOB, zip, DL, State) → confidence 1.0
  ↓ miss
Tier 2: Normalized string (names, cities, address, country) → confidence 0.95
  ↓ miss
Tier 3: Fuzzy (rapidfuzz token_set_ratio, name fields ONLY) → confidence = ratio/100
```

This is the core of the system. Azure AI Search finds *candidate* files; leak detection confirms *which PII fields* are actually present and how closely they match.

**Why not use Azure OpenAI for detection?** Regex + rapidfuzz is deterministic, auditable, fast, and zero marginal cost per query.

### Decision 3: rapidfuzz over fuzzywuzzy

`rapidfuzz` is MIT-licensed and 5–10x faster than `fuzzywuzzy` (GPL) with the same API. `token_set_ratio` handles reordered tokens naturally ("Chekuri Karthik" matches "Karthik Chekuri").

Sliding window approach: split file text into overlapping windows of `len(search_term) * 1.5` characters with step size `max(1, len(search_term) // 2)` (50% overlap), compute `token_set_ratio` against each window, take the maximum score.

### Decision 4: SQLAlchemy 2.0 with pyodbc for SQL Server

SQLAlchemy 2.0 provides mature SQL Server support via `mssql+pyodbc` dialect. V2 uses a simplified schema:

**Input tables:**
```sql
[DLU].[datalakeuniverse] (
    MD5       VARCHAR(32) PRIMARY KEY,
    file_path NVARCHAR(500) NOT NULL
)

[PII].[master_data] (
    customer_id     INT PRIMARY KEY,
    Fullname        NVARCHAR(250),
    FirstName       NVARCHAR(100),
    LastName        NVARCHAR(100),
    DOB             DATE,
    SSN             VARCHAR(11),
    DriversLicense  VARCHAR(50),
    Address1        NVARCHAR(250),
    Address2        NVARCHAR(250),
    Address3        NVARCHAR(250),
    ZipCode         VARCHAR(10),
    City            NVARCHAR(100),
    State           VARCHAR(2),
    Country         VARCHAR(50)
)
```

**Batch and status tables:**
```sql
[Batch].[batch_runs] (
    batch_id        UNIQUEIDENTIFIER PRIMARY KEY,
    strategy_set    NVARCHAR(MAX),    -- JSON: strategies used
    status          VARCHAR(20),      -- pending, running, completed, failed
    started_at      DATETIME2,
    completed_at    DATETIME2,
    total_customers INT,
    total_files     INT
)

[Batch].[customer_status] (
    id              INT IDENTITY PRIMARY KEY,
    batch_id        UNIQUEIDENTIFIER FK → batch_runs,
    customer_id     INT FK → master_data,
    status          VARCHAR(20),     -- pending, searching, detecting, complete, failed
    candidates_found INT DEFAULT 0,
    leaks_confirmed  INT DEFAULT 0,
    strategies_matched NVARCHAR(MAX), -- JSON array
    error_message   NVARCHAR(MAX),
    processed_at    DATETIME2
)

[Index].[file_status] (
    md5             VARCHAR(32) PRIMARY KEY FK → datalakeuniverse,
    status          VARCHAR(20),    -- indexed, failed, skipped
    indexed_at      DATETIME2,
    error_message   NVARCHAR(MAX)
)
```

**Results table:**
```sql
[Search].[results] (
    id                  INT IDENTITY PRIMARY KEY,
    batch_id            UNIQUEIDENTIFIER FK → batch_runs,
    customer_id         INT FK → master_data,
    md5                 VARCHAR(32) FK → datalakeuniverse,
    strategy_name       VARCHAR(100),
    leaked_fields       NVARCHAR(MAX),   -- JSON array
    match_details       NVARCHAR(MAX),   -- JSON per-field details
    overall_confidence  FLOAT,
    azure_search_score  FLOAT,
    needs_review        BIT DEFAULT 0,
    searched_at         DATETIME2 DEFAULT GETDATE()
)
```

### Decision 5: One file = one search document (no chunking)

Simulated files are 500–3000 characters, well within Azure AI Search's ~32KB field limit. No chunking means:
- No split-PII risk (SSN won't be cut across chunks)
- No chunk merging or deduplication logic
- Simpler document-to-file mapping (1:1)

Chunking can be added later if real production files exceed 32KB.

### Decision 6: OR logic with searchMode "any" to maximize recall

Strategy field values are combined with OR in the Lucene query, and `searchMode: "any"` ensures files matching any term are returned. This maximizes recall — we'd rather have more candidates (some irrelevant) than miss real ones. Precision comes from the three-tier leak detection engine.

Azure Search also scores files higher when multiple OR terms match, so files with both name AND SSN naturally rank above files with only one match.

### Decision 7: Multiple strategies per customer for accuracy

No single search strategy covers all cases. By running multiple complementary strategies and unioning results:
- `fullname_ssn` catches the obvious matches
- `lastname_dob` catches files without SSN
- `unique_identifiers` (SSN + DriversLicense) catches files with hard identifiers but mangled names

The cost overhead is negligible (3 API calls per customer instead of 1), while the accuracy gain is significant.

### Decision 8: Configurable strategies via YAML file

Strategies are defined in `strategies.yaml`, not hardcoded. Users can add/remove/modify strategies without code changes. Different breach cases may need different strategies.

Default strategies:
```yaml
strategies:
  - name: fullname_ssn
    description: "Primary search using full name and SSN"
    fields: [Fullname, SSN]
  - name: lastname_dob
    description: "Catches files with last name + date of birth"
    fields: [LastName, DOB]
  - name: unique_identifiers
    description: "Safety net — hard identifiers only"
    fields: [SSN, DriversLicense]
```

### Decision 9: Batch processing with per-customer search+detect

Search and detection run **together** per customer (not as separate bulk phases). This is simpler, uses less memory, and enables per-customer status tracking without storing intermediate candidate lists.

```
For each customer:
    Search (all strategies) → candidates
    Detect (all candidates) → results
    Update status → next customer
```

### Decision 10: Resumable processing at every level

At 1TB scale, crashes are inevitable. Both indexing and batch processing are resumable:
- **Indexing**: `[Index].[file_status]` tracks which files are indexed; resume skips them
- **Batch**: `[Batch].[customer_status]` tracks which customers are done; resume skips completed, retries failed, continues from where it stopped

### Decision 11: FastAPI project structure

```
breach-search/
├── strategies.yaml              # Configurable strategy definitions
├── .env                         # Environment variables
├── run_batch.py                 # CLI entry point for batch runs
├── app/
│   ├── main.py                  # FastAPI app + CORS + lifespan
│   ├── config.py                # pydantic-settings BaseSettings (.env loader)
│   ├── dependencies.py          # Dependency injection (DB session, search client)
│   ├── models/                  # SQLAlchemy ORM models
│   │   ├── database.py          # Engine + session factory
│   │   ├── dlu.py               # DLU table (MD5, file_path)
│   │   ├── master_data.py       # Master data (customer_id, 13 PII fields)
│   │   ├── batch.py             # Batch runs + customer status
│   │   └── result.py            # Search results
│   ├── schemas/                 # Pydantic v2 request/response models
│   ├── services/                # Business logic
│   │   ├── indexing_service.py  # DLU → text extraction → Azure AI Search
│   │   ├── batch_service.py     # Batch orchestration
│   │   ├── search_service.py    # Strategy → Lucene query → Azure Search
│   │   ├── leak_detection.py    # Three-tier per-field detection
│   │   └── text_extraction.py   # File → plain text
│   ├── routers/                 # FastAPI routers
│   │   ├── indexing.py          # POST /index/all, /index/{md5}
│   │   └── batch.py             # POST /batch/run, GET /batch/{id}/*
│   └── utils/                   # Helpers
│       ├── fuzzy.py             # Sliding window fuzzy matching
│       ├── confidence.py        # Confidence score computation
│       └── strategy_loader.py   # YAML strategy parser + validator
├── scripts/
│   ├── generate_simulated_data.py
│   ├── create_search_index.py
│   └── seed_database.py
├── data/
│   ├── simulated_files/
│   ├── TEXT/
│   └── seed/
└── tests/
```

Configuration via `.env` file loaded by pydantic-settings `BaseSettings`:
- `DATABASE_URL`: SQL Server connection string
- `AZURE_SEARCH_ENDPOINT`: Azure AI Search endpoint
- `AZURE_SEARCH_KEY`: Azure AI Search admin key
- `AZURE_SEARCH_INDEX`: Index name (default: `breach-file-index`)

## Risks / Trade-offs

- **[Fuzzy false positives]** → Mitigation: 75 threshold on token_set_ratio is conservative; per-field confidence scores and disambiguation rule reduce false attribution. Results flagged with `needs_review` for manual verification.

- **[OR logic returns many candidates]** → Mitigation: OR with multiple strategies returns more candidates than a single AND query, but leak detection filters out false positives. The cost of extra detection runs is negligible compared to the risk of missing a real match.

- **[Single-document indexing may fail at scale]** → Mitigation: Acceptable for simulated data (25 files). Chunking function can be added as a future enhancement.

- **[Name-only searches have inherently lower confidence]** → Mitigation: Name-only matches use a separate confidence formula with higher weight on search score, and first-name-only matches are flagged for review.

- **[rapidfuzz sliding window performance at 1TB]** → Mitigation: Detection only runs on candidate files (50-80 per customer), not all files. For production, windows can be pre-computed or file text tokenized once and reused.

- **[Batch run duration at scale]** → Mitigation: Resumability ensures no lost progress. Status tracking provides visibility. Sequential processing is simpler and sufficient for batch workloads.

- **[SQL Server dependency for local development]** → Mitigation: Docker container for SQL Server. Connection string is configurable via .env.

## Open Questions

- What is the maximum expected file size in production to determine if chunking is needed?
- Should there be a confidence threshold below which results are filtered out, or always include everything?
- Should customers be processed in parallel (e.g., 5 at a time) or strictly sequential?
- Can the same file content appear under different MD5 hashes? Do we need content-based dedup?
- Should we support "process only new customers" mode for incremental batches?
