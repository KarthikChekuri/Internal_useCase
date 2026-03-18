## ADDED Requirements

### Requirement: Generate 10 diverse simulated customers
The system SHALL generate 10 simulated customer records with diverse names (including names with apostrophes, hyphens, and non-English origins like O'Brien, Patel, Hassan), different US states, realistic SSNs (XXX-XX-XXXX format), DOBs, driver's license numbers, and full address information across all 13 PII fields. The customer table uses `customer_id` (INT) as the primary key.

#### Scenario: Generate customer records
- **WHEN** the simulated data generation script runs
- **THEN** 10 customer records are created with all 13 PII fields populated and written to `data/seed/master_data.csv`

#### Scenario: Customer name diversity
- **WHEN** the 10 customers are generated
- **THEN** the set includes at least one name with an apostrophe, one with a hyphen, and one with a non-Western name pattern

### Requirement: Generate ~25 simulated breach files
The system SHALL generate approximately 25 breach files across four formats (.txt, .xlsx, .csv, .xls) representing realistic document types: appointment notes, HR onboarding forms, payroll registers, insurance claims, tax W-2s, client intake forms, and benefits enrollment forms. Each file SHALL contain PII from 1–4 customers embedded naturally in document context.

#### Scenario: Generate breach files
- **WHEN** the simulated data generation script runs
- **THEN** approximately 25 files are created in `data/simulated_files/` across .txt, .xlsx, .csv, and .xls formats

#### Scenario: Files contain embedded PII in natural context
- **WHEN** a payroll register file is generated
- **THEN** customer names, SSNs, and addresses appear within realistic payroll text (not as isolated PII values)

#### Scenario: Multi-customer files
- **WHEN** a file is generated with PII from 3 customers
- **THEN** all three customers' PII is interspersed throughout the file content naturally

### Requirement: Intentional PII variations in simulated files
The system SHALL include intentional variations in how PII appears in breach files to test fuzzy and phonetic matching:
- Name abbreviations: "J. Smith" for "John Smith", "Bob O'Brien" for "Robert O'Brien"
- Misspellings: "Rodgriguez" for "Rodriguez"
- SSN format variations: with and without dashes
- Date format variations: ISO (1990-05-15), US (05/15/1990), European (15/05/1990)
- Reordered names: "Chekuri, Karthik" for "Karthik Chekuri"

#### Scenario: Name misspelling in file
- **WHEN** a file references customer "Maria Rodriguez"
- **THEN** the file may contain "Maria Rodgriguez" (intentional misspelling) to test fuzzy matching

#### Scenario: SSN without dashes
- **WHEN** a file contains a customer's SSN
- **THEN** some files use dashed format "343-43-4343" and others use undashed "343434343"

#### Scenario: Name reordering in file
- **WHEN** a file references customer "Karthik Chekuri"
- **THEN** some files contain "Chekuri, Karthik" (last-first order) to test token reordering

### Requirement: Dual-write simulated files for human browsing and indexing pipeline
The generation script SHALL write each simulated file to TWO locations:
1. `data/simulated_files/{descriptive_name}.{ext}` -- for human browsing and inspection (e.g., `data/simulated_files/payroll_register_q1.txt`)
2. `data/TEXT/{md5[:3]}/{md5}.{ext}` -- under the TEXTPATH directory structure used by the indexing pipeline (e.g., `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt`)

Both copies contain identical content.

#### Scenario: File written to both locations
- **WHEN** the generation script creates a payroll register file with MD5 hash "c8578af0e239aaeb7e4030b346430ac3"
- **THEN** the file exists at both `data/simulated_files/payroll_register_q1.txt` and `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt` with identical content

#### Scenario: TEXTPATH directory structure created automatically
- **WHEN** the generation script writes files to the TEXTPATH structure
- **THEN** subdirectories under `data/TEXT/` are created as needed (e.g., `data/TEXT/c85/`)

### Requirement: Generate DLU metadata for simulated files (V2 simplified)
The system SHALL generate a DLU metadata CSV (`data/seed/dlu_metadata.csv`) with one row per simulated file, containing only two columns: `MD5` (hash of the file content) and `file_path` (path to the file in the TEXT directory structure, e.g., `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt`). There is no GUID, caseName, fileName, fileExtension, or isExclusion column in V2.

#### Scenario: Generate DLU metadata
- **WHEN** the simulated data generation script runs
- **THEN** a `dlu_metadata.csv` is created with one row per file containing MD5 and file_path

#### Scenario: File path follows MD5 directory convention
- **WHEN** a file has MD5 hash "c8578af0e239aaeb7e4030b346430ac3"
- **THEN** the file_path is `data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt`

### Requirement: Seed database from CSV files
The system SHALL provide a script that reads `master_data.csv` and `dlu_metadata.csv` and inserts the data into `"PII"."master_data"` and `"DLU"."datalakeuniverse"` tables in PostgreSQL, creating the tables and schemas if they do not exist.

#### Scenario: Seed master data table
- **WHEN** the seed database script runs with `master_data.csv`
- **THEN** 10 customer records are inserted into `"PII"."master_data"` with all fields mapped correctly, using `customer_id` as the primary key

#### Scenario: Seed DLU table
- **WHEN** the seed database script runs with `dlu_metadata.csv`
- **THEN** all file metadata records are inserted into `"DLU"."datalakeuniverse"` with MD5 as primary key and file_path

#### Scenario: Idempotent seeding
- **WHEN** the seed script runs twice
- **THEN** data is not duplicated (existing records are skipped or replaced)
