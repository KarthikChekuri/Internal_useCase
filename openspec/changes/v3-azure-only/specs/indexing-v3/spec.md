## ADDED Requirements

### Requirement: V3 Azure AI Search index with PII metadata fields
The system SHALL create a new Azure AI Search index named `breach-file-index-v3` with the same three content fields and analyzers as V2, plus additional metadata fields for PII type pre-filtering. The index uses the same `phonetic_analyzer`, `name_analyzer`, and `pii_boost` scoring profile as V2.

#### Scenario: Create V3 index with PII metadata fields
- **WHEN** the V3 index creation script runs
- **THEN** an index named `breach-file-index-v3` is created with fields: `id` (key), `md5`, `file_path`, `content` (standard.lucene), `content_phonetic` (phonetic_analyzer), `content_lowercase` (name_analyzer), `has_ssn` (Boolean, filterable), `has_name` (Boolean, filterable), `has_dob` (Boolean, filterable), `has_address` (Boolean, filterable), `has_phone` (Boolean, filterable), `pii_types` (Collection(String), filterable), `pii_entity_count` (Int32, filterable)

#### Scenario: V3 index uses same analyzers as V2
- **WHEN** the V3 index is created
- **THEN** it includes `phonetic_analyzer` (Double Metaphone, replace=false) and `name_analyzer` (ASCII folding) with the same definitions as V2

#### Scenario: V3 index uses same scoring profile as V2
- **WHEN** the V3 index is created
- **THEN** it includes the `pii_boost` scoring profile with weights: content=3.0, content_lowercase=2.0, content_phonetic=1.5

### Requirement: PII Detection during V3 indexing
The V3 indexing pipeline SHALL call the Azure AI Language PII Detection API for each document's content during indexing. The detected PII entity types are mapped to the index metadata fields before the document is pushed to the index.

#### Scenario: PII Detection detects SSN in file content
- **WHEN** a file contains the text "Employee SSN: 343-43-4343" and is indexed via V3
- **THEN** the document is uploaded with `has_ssn=true`, `pii_types` containing "USSocialSecurityNumber", and `pii_entity_count >= 1`

#### Scenario: PII Detection detects person name in file content
- **WHEN** a file contains "Patient: Karthik Chekuri" and is indexed via V3
- **THEN** the document is uploaded with `has_name=true` and `pii_types` containing "Person"

#### Scenario: PII Detection detects date in file content
- **WHEN** a file contains "DOB: 07/15/1992" and is indexed via V3
- **THEN** the document is uploaded with `has_dob=true` and `pii_types` containing "DateTime"

#### Scenario: PII Detection detects address in file content
- **WHEN** a file contains "Address: 123 Main St, Houston, TX 77001" and is indexed via V3
- **THEN** the document is uploaded with `has_address=true` and `pii_types` containing "Address"

#### Scenario: PII Detection detects phone number in file content
- **WHEN** a file contains "Phone: (713) 555-0142" and is indexed via V3
- **THEN** the document is uploaded with `has_phone=true` and `pii_types` containing "PhoneNumber"
- **NOTE**: `has_phone` is stored as metadata for completeness but is NOT used for search pre-filtering (no customer PII field maps to phone)

#### Scenario: File with multiple PII types
- **WHEN** a file contains "Employee Karthik Chekuri, SSN 343-43-4343, DOB 07/15/1992" and is indexed via V3
- **THEN** the document is uploaded with `has_ssn=true`, `has_name=true`, `has_dob=true`, `pii_types` containing all detected types, and `pii_entity_count >= 3`

#### Scenario: File with no detectable PII
- **WHEN** a file contains only generic text with no PII patterns and is indexed via V3
- **THEN** the document is uploaded with all `has_*` fields set to `false`, `pii_types` as empty collection, and `pii_entity_count=0`

#### Scenario: PII Detection API unavailable or errors
- **WHEN** the Azure AI Language PII Detection API is unreachable during V3 indexing
- **THEN** the document is still indexed with all `has_*` fields set to `false` and `pii_types` as empty collection, a warning is logged, and indexing continues (PII metadata is best-effort, not blocking)

### Requirement: V3 indexing reuses text extraction from V2
The V3 indexing pipeline SHALL reuse the same `extract_text()` function from V2 for reading file content from disk. The only difference is the additional PII Detection step and metadata fields before pushing to the V3 index.

#### Scenario: V3 indexes same files as V2
- **WHEN** V3 indexing runs on the same DLU records
- **THEN** the same files are extracted, the same content is pushed to `breach-file-index-v3`, with the addition of PII metadata fields

### Requirement: V3 indexing endpoint
The system SHALL provide a `POST /v3/index/all` endpoint that indexes all eligible DLU files into the `breach-file-index-v3` index with PII metadata enrichment.

#### Scenario: Trigger V3 indexing via API
- **WHEN** `POST /v3/index/all` is called
- **THEN** all eligible DLU files are extracted, PII-detected, and indexed to `breach-file-index-v3`, and the response follows the same `IndexResponse` format as V2

### Requirement: V3 index name configuration
The V3 index name SHALL be configurable via environment variable `AZURE_SEARCH_INDEX_V3` (default: `breach-file-index-v3`). This allows V2 and V3 to point at different indexes.

#### Scenario: V3 uses separate index from V2
- **WHEN** `AZURE_SEARCH_INDEX` is `breach-file-index` and `AZURE_SEARCH_INDEX_V3` is `breach-file-index-v3`
- **THEN** V2 operations use `breach-file-index` and V3 operations use `breach-file-index-v3`

## MODIFIED Requirements

None — V2 indexing is not changed.

## REMOVED Requirements

None.
