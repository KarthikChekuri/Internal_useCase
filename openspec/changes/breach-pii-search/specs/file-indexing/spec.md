## ADDED Requirements

### Requirement: Text extraction from supported file formats
The system SHALL extract plain text content from breach files in four supported formats: .txt, .xlsx, .csv, and .xls. The file type is inferred from the file path extension at runtime. Each extractor SHALL produce a single concatenated text string from the file's contents.

#### Scenario: Extract text from a plain text file
- **WHEN** the indexing pipeline processes a .txt file
- **THEN** the system reads the file contents as UTF-8 text and returns the full content as a single string

#### Scenario: Extract text from an Excel xlsx file
- **WHEN** the indexing pipeline processes a .xlsx file
- **THEN** the system reads all sheets and all cells using openpyxl, concatenating cell values into a single text string with whitespace separators

#### Scenario: Extract text from a legacy Excel xls file
- **WHEN** the indexing pipeline processes a .xls file
- **THEN** the system reads all sheets and all cells using xlrd, concatenating cell values into a single text string with whitespace separators

#### Scenario: Extract text from a CSV file
- **WHEN** the indexing pipeline processes a .csv file
- **THEN** the system reads all rows and columns using the csv module, concatenating all cell values into a single text string

#### Scenario: Unsupported file extension
- **WHEN** the indexing pipeline encounters a file with an extension not in (.txt, .xlsx, .csv, .xls)
- **THEN** the system skips the file and logs a warning with the file MD5 and extension

#### Scenario: File not found at resolved path
- **WHEN** the resolved file path does not exist on the file system
- **THEN** the system logs an error with the file MD5 and path and continues processing remaining files

#### Scenario: File with encoding error
- **WHEN** a .txt file cannot be decoded as UTF-8 (e.g., binary file with .txt extension)
- **THEN** the `extract_text` function returns `None`, logs a warning with the file path and error, and the indexing pipeline counts it as a failed file in the IndexResponse

#### Scenario: Corrupt or unreadable Excel file
- **WHEN** a .xlsx or .xls file is corrupt and cannot be opened by openpyxl/xlrd
- **THEN** the `extract_text` function returns `None`, logs a warning with the file path and error, and the indexing pipeline counts it as a failed file in the IndexResponse

#### Scenario: Empty file
- **WHEN** a file exists but contains no extractable text (0 bytes or only whitespace)
- **THEN** the `extract_text` function returns an empty string `""`, the file is indexed with empty content fields, and leak detection finds no matches for that file

### Requirement: extract_text error contract
The `extract_text(file_path: str) -> str | None` function SHALL return a `str` on success (including empty string for empty files) and `None` on failure (file not found, unsupported extension, corrupt file, encoding error). The caller (indexing pipeline) SHALL treat `None` as a failed file and skip indexing for that file. The function SHALL NOT raise exceptions — all errors are caught internally, logged, and result in a `None` return.

### Requirement: File path resolution from DLU metadata
The system SHALL resolve the file path directly from the `file_path` column in the `[DLU].[datalakeuniverse]` table. The `file_path` column contains the path to the file on disk (absolute or relative to the working directory).

#### Scenario: Resolve file path for a DLU record
- **WHEN** a DLU record has `file_path` = `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt`
- **THEN** the system uses this path directly to locate and read the file

### Requirement: DLU table structure (V2 simplified)
The `[DLU].[datalakeuniverse]` table contains only two columns: `MD5` (primary key, VARCHAR(32)) and `file_path` (NVARCHAR(500)). There is no caseName, isExclusion, fileExtension, fileName, or GUID column. File type is inferred from the file path extension at runtime. All files in the DLU table are eligible for indexing — filtering by supported extensions (.txt, .xlsx, .csv, .xls) is done at indexing time by checking the file path extension.

#### Scenario: Filter DLU records for indexing by extension
- **WHEN** the indexing pipeline reads the DLU table
- **THEN** only files whose path ends with a supported extension (.txt, .xls, .xlsx, .csv) are processed; all others are skipped with a warning

#### Scenario: Unsupported extension in DLU is skipped
- **WHEN** a DLU record has `file_path` ending in `.pdf`
- **THEN** the file is skipped and logged as unsupported, counted in `files_skipped` (not `files_failed` — unsupported extensions are skips, not errors)

### Requirement: Push documents to Azure AI Search index
The system SHALL build a search document for each file with fields: `id` (MD5 hash), `md5`, `content`, `content_phonetic`, `content_lowercase`, and `file_path`. The `content`, `content_phonetic`, and `content_lowercase` fields SHALL all contain the same extracted text. Documents SHALL be uploaded in batches of up to 1000.

#### Scenario: Index a single file
- **WHEN** the indexing pipeline processes one file successfully
- **THEN** a document is uploaded to Azure AI Search with `id` set to the MD5 hash, the same text in all three content fields, and `file_path` as metadata

#### Scenario: Batch indexing of all files
- **WHEN** `POST /index/all` is called
- **THEN** all eligible DLU records are processed and their documents are uploaded to Azure AI Search in batches, with progress logged per file

#### Scenario: Index a specific file by MD5
- **WHEN** `POST /index/{md5}` is called with a valid MD5 that exists in DLU
- **THEN** only the specified file is extracted and indexed to Azure AI Search

#### Scenario: Index a file with MD5 not found in DLU
- **WHEN** `POST /index/{md5}` is called with an MD5 that does not exist in DLU
- **THEN** the system returns a 404 error with a message indicating the MD5 was not found

#### Scenario: Re-indexing overwrites existing documents (upsert)
- **WHEN** `POST /index/all` is called and documents already exist in the index for the same MD5 hashes
- **THEN** existing documents with the same `id` are overwritten with the new content (Azure AI Search upsert behavior)

### Requirement: Resumable indexing
The indexing pipeline SHALL be resumable. Before indexing a file, the system checks if that MD5 is already indexed (via the `[Index].[file_status]` table). Already-indexed files are skipped. This allows the pipeline to be interrupted and restarted without re-processing completed files.

#### Scenario: Resume indexing after interruption
- **WHEN** indexing was interrupted after processing 480 out of 500 files, and `POST /index/all` is called again
- **THEN** the system skips the 480 already-indexed files and processes only the remaining 20

#### Scenario: Force re-index ignores previous status
- **WHEN** `POST /index/all?force=true` is called
- **THEN** all files are re-indexed regardless of previous status (upsert behavior)

#### Scenario: Failed files are retried on resume
- **WHEN** 3 files failed in a previous indexing run and `POST /index/all` is called again
- **THEN** the 3 previously-failed files are retried (their status is not "indexed")

### Requirement: Indexing status tracking
The system SHALL maintain an `[Index].[file_status]` table that tracks the indexing state of each file: MD5 (PK, FK to DLU), status ("indexed", "failed", "skipped"), indexed_at timestamp, and error_message (for failures).

#### Scenario: Successful indexing updates status
- **WHEN** a file is successfully indexed
- **THEN** a row is inserted/updated in `file_status` with `status = "indexed"` and `indexed_at` set to current timestamp

#### Scenario: Failed indexing records error
- **WHEN** a file fails to index (corrupt, missing, etc.)
- **THEN** a row is inserted/updated in `file_status` with `status = "failed"` and `error_message` describing the failure

### Requirement: Indexing response format
The system SHALL return a JSON response from indexing endpoints with the following structure:
- `files_processed` (int): total number of files attempted
- `files_succeeded` (int): number of files successfully indexed
- `files_failed` (int): number of files that failed extraction or upload
- `files_skipped` (int): number of files skipped (already indexed, resumability)
- `errors` (list[str]): error messages for each failed file (MD5 + reason)

#### Scenario: Successful bulk indexing response
- **WHEN** `POST /index/all` processes 25 files and all succeed
- **THEN** the response is `{ "files_processed": 25, "files_succeeded": 25, "files_failed": 0, "files_skipped": 0, "errors": [] }`

#### Scenario: Partial failure indexing response
- **WHEN** `POST /index/all` processes 25 files and 2 fail (missing files)
- **THEN** the response is `{ "files_processed": 25, "files_succeeded": 23, "files_failed": 2, "files_skipped": 0, "errors": ["MD5-xxx: file not found at path ...", "MD5-yyy: encoding error ..."] }`

#### Scenario: Resumed indexing with skipped files
- **WHEN** `POST /index/all` is called and 20 files are already indexed
- **THEN** the response includes `"files_skipped": 20` and only the remaining files are counted in `files_processed`

### Requirement: Azure AI Search index with custom analyzers
The system SHALL create an Azure AI Search index named `breach-file-index` with three content fields, each using a different analyzer:
- `content`: `standard.lucene` analyzer for exact/fuzzy keyword search
- `content_phonetic`: custom `phonetic_analyzer` (Double Metaphone, `replace: false`) for phonetic name matching
- `content_lowercase`: custom `name_analyzer` (standard tokenizer, lowercase filter, asciifolding filter) for accent/case-insensitive matching

The index SHALL include a scoring profile `pii_boost` with weights: `content` = 3.0, `content_lowercase` = 2.0, `content_phonetic` = 1.5.

#### Scenario: Create search index with custom analyzers
- **WHEN** the index creation script runs
- **THEN** an index named `breach-file-index` is created with `phonetic_analyzer` (Double Metaphone) and `name_analyzer` (ASCII folding) configured, and the `pii_boost` scoring profile is active

#### Scenario: Phonetic analyzer preserves original tokens
- **WHEN** text "Smith" is analyzed by `phonetic_analyzer`
- **THEN** both the original token "smith" and the Double Metaphone encoding are indexed (because `replace: false`)
