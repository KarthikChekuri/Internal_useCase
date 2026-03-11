## Why

When breach data files are discovered (payroll registers, HR forms, insurance claims, tax documents), the organization needs to determine which customers' personally identifiable information (PII) was exposed and what specific data types were leaked. Currently there is no automated way to search across heterogeneous file formats (txt, xlsx, csv, xls) and match file contents against a known customer PII database with fuzzy/phonetic name matching. This capability is needed now because breach notification timelines are tight and manual review of files is error-prone and slow.

## What Changes

- Build a new FastAPI application that accepts a customer's fullname + SSN and searches all indexed breach files for PII matches
- Create an indexing pipeline that extracts text from breach files (.txt, .xlsx, .csv, .xls) and pushes them to Azure AI Search with three analyzer configurations (standard, phonetic/Double Metaphone, ASCII-folding)
- Implement a three-tier leak detection engine: exact regex matching (SSN, DOB, zip), normalized string matching (names, cities), and fuzzy matching via rapidfuzz (misspellings, reordered tokens)
- Implement a weighted confidence scoring model that combines per-field match confidence with Azure AI Search relevance scores across three scenarios (SSN+Name, SSN-only, Name-only)
- Create a simulated dataset of 10 customers and ~25 breach files with intentional name variations, format differences, and multi-customer files for testing
- Set up SQL Server tables for customer PII storage (`[PII].[master_pii]`) and search result persistence (`[Search].[search_results]`)

## Capabilities

### New Capabilities
- `file-indexing`: Text extraction from breach files (.txt, .xlsx, .csv, .xls) and indexing into Azure AI Search with phonetic and ASCII-folding analyzers
- `pii-search`: Customer lookup by SSN, Lucene query construction with fuzzy operators, and Azure AI Search query orchestration across three content fields
- `leak-detection`: Three-tier per-field PII matching engine (exact regex, normalized, fuzzy) that checks all 13 PII fields against file content
- `confidence-scoring`: Weighted composite confidence scoring with three scenario formulas (SSN+Name, SSN-only, Name-only) and per-field confidence levels
- `simulated-data`: Generation of 10 diverse test customers and ~25 breach files with intentional name variations, format differences, and embedded PII for end-to-end testing

### Modified Capabilities

## Impact

- **New API endpoints**: `POST /search` (PII search), `POST /index/all` and `POST /index/{guid}` (indexing)
- **Database**: New `[PII].[master_pii]` and `[Search].[search_results]` tables in local SQL Server; reads from existing `[DLU].[datalakeuniverse]` table
- **Azure AI Search**: New index `breach-file-index` with custom phonetic and name analyzers, scoring profile `pii_boost`
- **Dependencies**: FastAPI, SQLAlchemy 2.0, pyodbc, rapidfuzz, azure-search-documents, openpyxl, xlrd, pydantic-settings
- **File system**: Reads breach files from configurable base path; generates simulated files to `data/simulated_files/`
