## ADDED Requirements

### Requirement: Per-field Lucene query parameters
Every per-field query SHALL use the following Azure AI Search parameters: `query_type="full"` (Lucene syntax), `search_fields="content,content_phonetic,content_lowercase"`, `scoring_profile="pii_boost"`, and `top=100`. The `search_mode` varies by field type (see query construction requirement).

#### Scenario: All per-field queries use full Lucene syntax
- **WHEN** any per-field query is sent to Azure AI Search
- **THEN** the query uses `query_type="full"`, `search_fields="content,content_phonetic,content_lowercase"`, `scoring_profile="pii_boost"`, and `top=100`

### Requirement: Per-field Lucene query search
The V3 search service SHALL determine PII field matches by sending a separate Lucene query per PII field to Azure AI Search. If a query returns results for a document, that PII field is considered "found" in that document.

#### Scenario: SSN exact match via Lucene query
- **WHEN** a customer's SSN is "343-43-4343" and a per-field query `"343-43-4343" OR "343434343"` is sent
- **THEN** documents containing the exact SSN string are returned with a search score, and the SSN field is marked as "found" for those documents

#### Scenario: Name fuzzy match via Lucene query
- **WHEN** a customer's Fullname is "Karthik Chekuri" and a per-field query `Karthik~1 Chekuri~1` is sent
- **THEN** documents containing "Karthik Chekuri", "Kerthik Chekuri", or phonetically similar names are returned, and the Fullname field is marked as "found" for those documents

#### Scenario: DOB multi-format match via Lucene query
- **WHEN** a customer's DOB is 1992-07-15 and a per-field query `"07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"` is sent
- **THEN** documents containing any of those date formats are returned, and the DOB field is marked as "found"

#### Scenario: Field query returns no results
- **WHEN** a per-field query for ZipCode "77001" returns no documents
- **THEN** the ZipCode field is marked as "not found" for all documents in this batch

#### Scenario: Null PII field is skipped
- **WHEN** a customer has `DriversLicense = null`
- **THEN** no query is sent for the DriversLicense field, and it is excluded from the results

### Requirement: Query construction per field type
The V3 search service SHALL build appropriate Lucene queries based on the field type:

| Field | Query construction | search_mode |
|---|---|---|
| SSN | Quoted exact dashed + OR + quoted undashed | all |
| DOB | Quoted exact in 4 date formats joined by OR | all |
| Fullname | Space-separated tokens with `~1` fuzzy | any |
| FirstName | Single token with `~1` fuzzy | any |
| LastName | Single token with `~1` fuzzy | any |
| ZipCode | Quoted exact | all |
| DriversLicense | Quoted exact | all |
| State | Quoted exact | all |
| City | Quoted exact (multi-word quoted phrase) | all |
| Address1/2/3 | Quoted exact phrase | all |
| Country | Quoted exact phrase | all |

#### Scenario: SSN query includes both formats
- **WHEN** the customer's SSN is "343-43-4343"
- **THEN** the Lucene query is `"343-43-4343" OR "343434343"`

#### Scenario: Name query uses fuzzy operator
- **WHEN** the customer's Fullname is "Robert O'Brien"
- **THEN** the Lucene query is `Robert~1 O'Brien~1` (with special characters escaped appropriately)

#### Scenario: DOB query covers all date formats
- **WHEN** the customer's DOB is 1992-07-15
- **THEN** the Lucene query is `"07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"`

#### Scenario: Multi-word city is quoted
- **WHEN** the customer's City is "New York"
- **THEN** the Lucene query is `"New York"`

### Requirement: Metadata pre-filtering on per-field queries
The V3 search service SHALL use Azure AI Search `$filter` to pre-filter documents using PII metadata fields when available, reducing the candidate set before the Lucene query runs.

#### Scenario: SSN query pre-filters by has_ssn
- **WHEN** a per-field SSN query is sent
- **THEN** the query includes `filter="has_ssn eq true"` to only search documents that the PII skill tagged as containing an SSN

#### Scenario: Name query pre-filters by has_name
- **WHEN** a per-field name query is sent
- **THEN** the query includes `filter="has_name eq true"` to only search documents tagged as containing a person name

#### Scenario: DOB query pre-filters by has_dob
- **WHEN** a per-field DOB query is sent
- **THEN** the query includes `filter="has_dob eq true"`

#### Scenario: Address query pre-filters by has_address
- **WHEN** a per-field query is sent for Address1, Address2, or Address3
- **THEN** the query includes `filter="has_address eq true"` to only search documents tagged as containing an address

#### Scenario: No pre-filter for fields without metadata mapping
- **WHEN** a per-field query is sent for City, State, ZipCode, Country, or DriversLicense (no dedicated metadata field)
- **THEN** no `$filter` is applied; the Lucene query runs against all documents

### Requirement: Hit highlighting for snippet extraction
The V3 search service SHALL request hit highlighting on the `content` field for every per-field query. The highlighted text serves as the snippet for that field match.

#### Scenario: SSN match returns highlighted snippet
- **WHEN** a per-field SSN query matches a document
- **THEN** the result includes `@search.highlights` with the SSN wrapped in highlight tags, e.g. `"Employee SSN: [[MATCH]]343-43-4343[[/MATCH]] effective..."`

#### Scenario: Name match returns highlighted snippet
- **WHEN** a per-field name query matches a document
- **THEN** the result includes `@search.highlights` with the matched name tokens highlighted

#### Scenario: Fuzzy query with no highlight
- **WHEN** a per-field fuzzy name query matches but Azure AI Search does not produce a highlight (known limitation of fuzzy queries)
- **THEN** the snippet for that field is set to `null` (no snippet available), but the field is still marked as "found"

#### Scenario: Custom highlight tags
- **WHEN** hit highlighting is requested
- **THEN** the pre-tag is `[[MATCH]]` and post-tag is `[[/MATCH]]` (not HTML `<em>` tags, since results are JSON not HTML)

### Requirement: Result merging across per-field queries
The V3 search service SHALL merge results from all per-field queries into a unified per-document result. For each document that appeared in any per-field query result, the merged result includes: which fields were found, the search score per field, and the snippet per field.

#### Scenario: Document appears in multiple field queries
- **WHEN** document MD5 "abc123" appears in the SSN query (score 12.5) and the Fullname query (score 8.3) but not the DOB query
- **THEN** the merged result for "abc123" shows: SSN found (score 12.5, snippet), Fullname found (score 8.3, snippet), DOB not found

#### Scenario: Document appears in only one field query
- **WHEN** document MD5 "def456" appears only in the City query (score 3.1)
- **THEN** the merged result for "def456" shows: City found (score 3.1), all other fields not found

#### Scenario: Not-found field shape in match_details
- **WHEN** a field was queried but not found in a document
- **THEN** the match_details entry for that field is `{ "found": false }` (no score or snippet keys)

### Requirement: V3 confidence scoring
The V3 search service SHALL compute an overall confidence score per document using normalized search scores from per-field queries.

#### Scenario: Per-field confidence from search score
- **WHEN** a per-field SSN query returns a document with `@search.score = 12.5` and the maximum score across all per-field queries for this customer is 15.0
- **THEN** the SSN field confidence for this document is `min(1.0, 12.5 / 15.0) = 0.833`

#### Scenario: Overall confidence weighted average
- **WHEN** a document has SSN confidence 0.83, Name confidence 0.55, and average other-field confidence 0.30
- **THEN** the overall confidence is `0.35 * 0.83 + 0.30 * 0.55 + 0.20 * 0.30 + 0.15 * 0.0 = 0.516` (document-level score is 0 since V3 doesn't do a broad query)

#### Scenario: Needs review flag
- **WHEN** the overall confidence for a document is below 0.5
- **THEN** the result has `needs_review = true`

#### Scenario: FirstName-only match without SSN
- **WHEN** a document matches only on FirstName (not Fullname, not LastName, not SSN)
- **THEN** the result has `needs_review = true` regardless of confidence score

### Requirement: Search score normalization
The V3 search service SHALL normalize `@search.score` values to a 0.0–1.0 range by dividing each score by the maximum score observed across all per-field queries for the current customer.

#### Scenario: Normalize scores across fields
- **WHEN** the SSN query returns scores [12.5, 10.0, 8.0] and the Name query returns scores [9.0, 6.5]
- **THEN** the maximum score is 12.5, and all scores are divided by 12.5 to produce normalized values

## MODIFIED Requirements

None — V2 search is not changed.

## REMOVED Requirements

None.
