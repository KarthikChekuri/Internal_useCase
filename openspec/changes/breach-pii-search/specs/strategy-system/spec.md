## ADDED Requirements

### Requirement: Strategy definition format
A strategy defines which PII fields to use as search terms when querying Azure AI Search. Each strategy SHALL have a `name` (unique identifier), a `description` (human-readable purpose), and a `fields` list (PII field names from the master_data table). The strategy does NOT control which fields are checked during leak detection — it only controls the search query.

#### Scenario: Strategy with two fields
- **GIVEN** a strategy defined as `{ name: "fullname_ssn", fields: ["Fullname", "SSN"] }`
- **WHEN** the system builds a search query for a customer
- **THEN** the Lucene query is constructed using only the customer's Fullname and SSN values

#### Scenario: Strategy with three fields
- **GIVEN** a strategy defined as `{ name: "lastname_dob", fields: ["LastName", "DOB"] }`
- **WHEN** the system builds a search query for a customer
- **THEN** the Lucene query is constructed using the customer's LastName and DOB values

#### Scenario: Strategy field references a null PII value
- **GIVEN** a strategy with fields `["SSN", "DriversLicense"]` and the customer's DriversLicense is NULL
- **WHEN** the system builds the search query
- **THEN** the DriversLicense term is omitted from the query (only SSN is searched)

#### Scenario: All strategy fields are null for a customer
- **GIVEN** a strategy with fields `["DriversLicense"]` and the customer's DriversLicense is NULL
- **WHEN** the system builds the search query
- **THEN** the strategy produces no query terms and is effectively skipped for this customer (logged as a warning)

### Requirement: Strategy set configuration via YAML file
The system SHALL load strategies from a `strategies.yaml` configuration file in the project root. The file defines a list of strategies, each with `name`, `description`, and `fields`. The user can modify this file to add, remove, or change strategies without code changes.

#### Scenario: Load default strategy set
- **WHEN** the system starts a batch run and reads `strategies.yaml`
- **THEN** all strategies defined in the file are loaded and used for the batch

#### Scenario: Default strategy set contents
- **WHEN** the system ships with the default `strategies.yaml`
- **THEN** it contains three strategies:
  1. `fullname_ssn` — fields: [Fullname, SSN]
  2. `lastname_dob` — fields: [LastName, DOB]
  3. `unique_identifiers` — fields: [SSN, DriversLicense]

#### Scenario: Custom strategy override
- **WHEN** the user edits `strategies.yaml` to contain a single strategy `{ name: "ssn_only", fields: ["SSN"] }`
- **THEN** the system uses only that one strategy for the batch run

#### Scenario: Invalid strategy file
- **WHEN** `strategies.yaml` is missing or contains invalid YAML
- **THEN** the system raises a clear error at startup with the file path and parse error

#### Scenario: Strategy references invalid field name
- **WHEN** a strategy contains `fields: ["Fullname", "InvalidField"]`
- **THEN** the system raises a validation error at startup listing the invalid field name and the valid field names

### Requirement: Lucene query construction from strategy fields
The system SHALL build a full Lucene query by combining all strategy field values with OR logic. Each field type is formatted according to its nature:

- **Name fields** (Fullname, FirstName, LastName): Split into tokens, each token gets a `~1` fuzzy operator. Lucene special characters in names (`-`, `'`, `.`) are escaped or quoted.
- **SSN**: Included in both dashed ("343-43-4343") and undashed ("343434343") formats, quoted.
- **DOB**: Included in multiple date format representations — ISO (1990-05-15), US (05/15/1990), European (15/05/1990).
- **DriversLicense**: Included as a quoted exact string.
- **Address fields**: Included as quoted exact strings.
- **ZipCode**: Included as an exact string.
- **City**: Split into tokens with `~1` fuzzy operator (like names).
- **State**: Included as an exact 2-character string.
- **Country**: Included as a quoted exact string.

All field values within a single strategy are combined with OR.

#### Scenario: Build query for fullname_ssn strategy
- **GIVEN** strategy `fullname_ssn` with fields `[Fullname, SSN]`
- **WHEN** customer has Fullname "Karthik Chekuri" and SSN "343-43-4343"
- **THEN** the Lucene query is: `Karthik~1 Chekuri~1 OR "343-43-4343" OR "343434343"`

#### Scenario: Build query for lastname_dob strategy
- **GIVEN** strategy `lastname_dob` with fields `[LastName, DOB]`
- **WHEN** customer has LastName "Chekuri" and DOB 1990-05-15
- **THEN** the Lucene query includes: `Chekuri~1 OR "1990-05-15" OR "05/15/1990" OR "15/05/1990"`

#### Scenario: Build query for unique_identifiers strategy
- **GIVEN** strategy `unique_identifiers` with fields `[SSN, DriversLicense]`
- **WHEN** customer has SSN "343-43-4343" and DriversLicense "D1234567"
- **THEN** the Lucene query is: `"343-43-4343" OR "343434343" OR "D1234567"`

#### Scenario: Build query for customer with hyphenated name
- **GIVEN** strategy `fullname_ssn` with fields `[Fullname, SSN]`
- **WHEN** customer has Fullname "Mary O'Brien-Smith" and SSN "123-45-6789"
- **THEN** the query handles the apostrophe and hyphen properly, applying fuzzy operators to each token

#### Scenario: OR logic — file matches any term
- **WHEN** a file contains "343-43-4343" but NOT "Karthik" or "Chekuri"
- **THEN** the file IS returned by Azure AI Search (OR means any term match is sufficient)

### Requirement: Multiple strategies produce union of candidates
When multiple strategies are defined, the system SHALL run each strategy as a separate Azure AI Search query for each customer and merge (union) the results. Duplicate files (same MD5 returned by multiple strategies) are deduplicated. The highest Azure Search score across strategies is kept for scoring. Each result tracks which strategy first found it.

#### Scenario: Three strategies produce overlapping results
- **GIVEN** three strategies are configured
- **WHEN** strategy 1 returns [file_a, file_b], strategy 2 returns [file_a, file_d], strategy 3 returns [file_a, file_e]
- **THEN** the union of candidates is [file_a, file_b, file_d, file_e] (deduplicated by MD5)

#### Scenario: File found by multiple strategies records first match
- **WHEN** file_a is returned by both `fullname_ssn` (score 12.5) and `unique_identifiers` (score 9.0)
- **THEN** the result for file_a records `strategy_that_found_it: "fullname_ssn"` (first strategy that found it) and `azure_search_score: 12.5` (highest score)

#### Scenario: Strategies that return no results
- **WHEN** strategy `lastname_dob` returns zero results for a customer
- **THEN** the other strategies' results still form the candidate set; the empty strategy is logged but not an error

### Requirement: Azure AI Search query execution per strategy
Each strategy query SHALL be executed against Azure AI Search with:
- `queryType: "full"` (enables Lucene syntax — fuzzy ~1, OR, quoted phrases)
- `searchMode: "any"` (file matches if ANY term matches — maximizes recall)
- `searchFields: content, content_phonetic, content_lowercase` (search all 3 analyzer fields)
- `scoringProfile: "pii_boost"` (apply field weights: content 3.0, content_lowercase 2.0, content_phonetic 1.5)
- `top: 100` (return up to 100 candidates per strategy query)

#### Scenario: Execute strategy query
- **WHEN** the system executes a strategy query for a customer
- **THEN** Azure AI Search returns up to 100 matching documents with their search scores, ordered by relevance

#### Scenario: No matches from any strategy
- **WHEN** all strategies return zero results for a customer
- **THEN** the customer is marked as complete with 0 candidates found and 0 leaks confirmed
