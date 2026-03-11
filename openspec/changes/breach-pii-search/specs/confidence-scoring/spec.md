## ADDED Requirements

### Requirement: Per-field confidence values
The system SHALL assign a confidence score between 0.0 and 1.0 to each PII field match based on the detection method:
- Exact string: 1.0
- Normalized exact: 0.95
- Fuzzy match (ratio >= 75): confidence = ratio / 100 (e.g., ratio 87 produces confidence 0.87)
- First name only + SSN in same file: 0.70
- First name only, no SSN: 0.30–0.50
- SSN exact: 1.0
- SSN last 4 only: 0.40
- No match: 0.0

#### Scenario: Exact SSN match confidence
- **WHEN** the SSN is found as an exact match
- **THEN** per-field confidence is 1.0

#### Scenario: Normalized name match confidence
- **WHEN** the fullname is found via normalized matching (case-insensitive)
- **THEN** per-field confidence is 0.95

#### Scenario: Fuzzy name match confidence at ratio 87
- **WHEN** the fullname is matched by rapidfuzz with token_set_ratio of 87
- **THEN** per-field confidence is 0.87

#### Scenario: First name only with SSN disambiguation
- **WHEN** only the first name matches and SSN is also found in the same file
- **THEN** per-field confidence for the name is 0.70

### Requirement: Overall file confidence with three scenario formulas
The system SHALL compute an overall confidence score per file using one of three weighted formulas based on which anchor fields (SSN, Name) matched:

**SSN + Name both match:**
`0.40 x SSN_conf + 0.30 x Name_conf + 0.15 x OtherFields_avg + 0.15 x SearchScore_norm`

**SSN only (no name in file):**
`0.60 x SSN_conf + 0.15 x OtherFields_avg + 0.25 x SearchScore_norm`

**Name only (no SSN in file):**
`0.50 x Name_conf + 0.20 x OtherFields_avg + 0.30 x SearchScore_norm`

Where:
- `SSN_conf` is the per-field confidence of the SSN match (0.0 if no match).
- `Name_conf` is the maximum confidence among the Fullname, FirstName, and LastName fields (0.0 if none match).
- `OtherFields_avg` is computed as: **sum of confidence scores for all evaluable non-anchor fields / count of evaluable non-anchor fields**. A field is "evaluable" if it is non-null in the customer's master_pii record. Unmatched but evaluable fields contribute 0.0 to the numerator. Fields that are null in master_pii are excluded from both numerator and denominator. If no non-anchor fields are evaluable (all null), OtherFields_avg = 0.0. The non-anchor fields are: DOB, DriversLicense, Address1, Address2, Address3, ZipCode, City, State, Country (up to 9 fields, but typically fewer are populated per customer).
- `SearchScore_norm` is the Azure AI Search score normalized to the 0.0–1.0 range.

#### Scenario: SSN and name both found in file
- **GIVEN** the customer has 3 evaluable non-anchor fields: DOB, ZipCode, City
- **WHEN** SSN matches with confidence 1.0 and Fullname matches with confidence 0.95 and DOB matches with confidence 1.0 (ZipCode and City unmatched = 0.0) and normalized search score is 0.8
- **THEN** OtherFields_avg = (1.0 + 0.0 + 0.0) / 3 = 0.333 and overall confidence is `0.40(1.0) + 0.30(0.95) + 0.15(0.333) + 0.15(0.8)` = 0.40 + 0.285 + 0.050 + 0.12 = **0.855**

#### Scenario: SSN found but no name in file
- **GIVEN** the customer has 4 evaluable non-anchor fields: DOB, ZipCode, City, State
- **WHEN** SSN matches with confidence 1.0 and no name field matches and ZipCode matches with confidence 1.0 (DOB, City, State unmatched = 0.0) and normalized search score is 0.6
- **THEN** OtherFields_avg = (0.0 + 1.0 + 0.0 + 0.0) / 4 = 0.25 and overall confidence is `0.60(1.0) + 0.15(0.25) + 0.25(0.6)` = 0.60 + 0.0375 + 0.15 = **0.7875**

#### Scenario: Name found but no SSN in file
- **GIVEN** the customer has 3 evaluable non-anchor fields: DOB, ZipCode, City
- **WHEN** Fullname matches with confidence 0.85 (fuzzy) and no SSN field matches and City matches with confidence 0.95 (DOB and ZipCode unmatched = 0.0) and normalized search score is 0.5
- **THEN** OtherFields_avg = (0.0 + 0.0 + 0.95) / 3 = 0.317 and overall confidence is `0.50(0.85) + 0.20(0.317) + 0.30(0.5)` = 0.425 + 0.063 + 0.15 = **0.638**

#### Scenario: No other fields matched besides anchors
- **WHEN** only SSN and Name match but no other fields match
- **THEN** `OtherFields_avg` is 0.0 and the formula uses 0.0 for that component

### Requirement: Search score normalization
The system SHALL normalize the Azure AI Search score to a 0.0–1.0 range by dividing each file's score by the maximum score in the result set. The highest-scoring file gets normalized score 1.0.

#### Scenario: Normalize scores across result set
- **WHEN** search returns files with scores 12.5, 8.3, and 4.1
- **THEN** normalized scores are 1.0, 0.664, and 0.328 respectively

### Requirement: Disambiguation rule for first-name-only matches
The system SHALL apply a disambiguation rule when **only FirstName matches and both Fullname and LastName are not found** (i.e., Fullname.found == false AND LastName.found == false). If the SSN is also found in the same file, the system treats the name as belonging to the customer with confidence 0.70 for the FirstName field. If only the first name matches with NO SSN in the file, confidence is 0.30–0.50 and the result is flagged for manual review. This rule does NOT apply when Fullname or LastName also matched — in those cases, standard per-field confidence applies.

#### Scenario: First name plus SSN confirms identity
- **WHEN** a file contains "Karthik" and "343-43-4343" but not "Chekuri"
- **THEN** FirstName is attributed to the customer with confidence 0.70 and the overall formula uses SSN+Name scenario

#### Scenario: First name only without SSN is low confidence
- **WHEN** a file contains "Karthik" but no SSN match
- **THEN** FirstName confidence is 0.30–0.50, the overall formula uses Name-only scenario, and the result is flagged with `needs_review: true`

### Requirement: Fallback formula when neither SSN nor name matches
If a file is returned by Azure AI Search but leak detection finds neither an SSN match nor any name field match (only other fields like ZipCode, DOB, or Address matched), the system SHALL compute overall confidence using:

**No anchor fields:**
`0.50 x OtherFields_avg + 0.50 x SearchScore_norm`

The result SHALL be flagged with `needs_review: true`.

#### Scenario: Only non-anchor fields matched
- **GIVEN** the customer has 4 evaluable non-anchor fields: DOB, ZipCode, City, State
- **WHEN** a file matches with ZipCode confidence 1.0 and DOB confidence 1.0 (City and State unmatched = 0.0) and no SSN or name fields match and normalized search score is 0.4
- **THEN** OtherFields_avg = (1.0 + 1.0 + 0.0 + 0.0) / 4 = 0.50 and overall confidence is `0.50(0.50) + 0.50(0.4)` = 0.25 + 0.20 = **0.45**, and the result is flagged with `needs_review: true`
