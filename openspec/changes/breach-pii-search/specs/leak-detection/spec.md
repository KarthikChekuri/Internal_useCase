## ADDED Requirements

### Requirement: Tier 1 exact regex matching for structured PII
The system SHALL check for exact matches of structured PII fields using regex patterns with word boundaries. Tier 1 applies to: SSN (dashed and undashed), DOB (multiple date formats), ZipCode, DriversLicense, and State (2-character code with word boundaries, e.g., `\bCA\b`). A Tier 1 match SHALL have confidence 1.0.

#### Scenario: Exact SSN match with dashes
- **WHEN** the file text contains "343-43-4343" and the customer's SSN is "343-43-4343"
- **THEN** the SSN field is detected with method "exact", confidence 1.0, and a snippet showing the match in context

#### Scenario: Exact SSN match without dashes
- **WHEN** the file text contains "343434343" and the customer's SSN is "343-43-4343"
- **THEN** the SSN field is detected with method "exact", confidence 1.0

#### Scenario: DOB match in ISO format
- **WHEN** the file text contains "1990-05-15" and the customer's DOB is 1990-05-15
- **THEN** the DOB field is detected with method "exact", confidence 1.0

#### Scenario: DOB match in US date format
- **WHEN** the file text contains "05/15/1990" and the customer's DOB is 1990-05-15
- **THEN** the DOB field is detected with method "exact", confidence 1.0

#### Scenario: DOB match in European date format (slash)
- **WHEN** the file text contains "15/05/1990" and the customer's DOB is 1990-05-15
- **THEN** the DOB field is detected with method "exact", confidence 1.0

#### Scenario: DOB match in European date format (dot)
- **WHEN** the file text contains "15.05.1990" and the customer's DOB is 1990-05-15
- **THEN** the DOB field is detected with method "exact", confidence 1.0

#### Scenario: DOB ambiguous date disambiguation
- **WHEN** the customer's DOB is 1990-03-05 (March 5) and the file text contains "05/03/1990"
- **THEN** the system generates all format representations of the customer's known DOB (ISO: "1990-03-05", US: "03/05/1990", European slash: "05/03/1990", European dot: "05.03.1990") and matches against any of them, detecting the DOB with method "exact", confidence 1.0

#### Scenario: State exact match with word boundary
- **WHEN** the file text contains "CA" as a standalone token and the customer's State is "CA"
- **THEN** the State field is detected with method "exact", confidence 1.0 (matched via `\bCA\b`)

#### Scenario: State substring does not match
- **WHEN** the file text contains "CABLE" and the customer's State is "CA"
- **THEN** the State field is NOT detected (word boundary prevents substring match)

#### Scenario: Last 4 of SSN only
- **WHEN** the file text contains "4343" as a standalone token (matched via `\b4343\b`) but not the full SSN, and the customer's SSN ends in "4343"
- **THEN** the SSN field is detected with method "partial", confidence 0.40

#### Scenario: ZipCode exact match
- **WHEN** the file text contains "90210" and the customer's ZipCode is "90210"
- **THEN** the ZipCode field is detected with method "exact", confidence 1.0

### Requirement: Tier 2 normalized string matching for names and locations
The system SHALL check for normalized matches by lowercasing text and stripping punctuation, then performing substring search. Tier 2 applies to: Fullname, FirstName, LastName, City, Address fields, and Country. State is excluded from Tier 2 because its 2-character code is too short for safe substring search (it is handled by Tier 1 exact regex with word boundaries only). A Tier 2 match SHALL have confidence 0.95.

For the Fullname field, Tier 2 performs a complete-string normalized substring search -- the entire normalized fullname must appear as a contiguous substring in the normalized file text. Reordered names (e.g., "Chekuri Karthik" when searching for "Karthik Chekuri") will NOT match at Tier 2 and will fall through to Tier 3 fuzzy matching. FirstName and LastName are checked individually as separate fields.

#### Scenario: Full name case-insensitive match
- **WHEN** the file text contains "karthik chekuri" (lowercase) and the customer's Fullname is "Karthik Chekuri"
- **THEN** the Fullname field is detected with method "normalized", confidence 0.95

#### Scenario: Name with extra whitespace
- **WHEN** the file text contains "Karthik  Chekuri" (double space) and the customer's Fullname is "Karthik Chekuri"
- **THEN** the Fullname field is detected with method "normalized", confidence 0.95

#### Scenario: Name with apostrophe variation
- **WHEN** the file text contains "OBrien" and the customer's LastName is "O'Brien"
- **THEN** the LastName field is detected with method "normalized", confidence 0.95

#### Scenario: City name case-insensitive match
- **WHEN** the file text contains "new york" and the customer's City is "New York"
- **THEN** the City field is detected with method "normalized", confidence 0.95

#### Scenario: Reordered fullname does not match at Tier 2
- **WHEN** the file text contains "Chekuri Karthik" and the customer's Fullname is "Karthik Chekuri"
- **THEN** the Fullname field is NOT detected at Tier 2 (complete-string substring "karthik chekuri" is not found) and evaluation falls through to Tier 3 fuzzy matching

### Requirement: Tier 3 fuzzy matching via rapidfuzz
The system SHALL use `rapidfuzz.fuzz.token_set_ratio` with a sliding window approach to detect fuzzy matches when Tier 1 and Tier 2 miss. Tier 3 applies ONLY to name fields: Fullname, FirstName, and LastName. All other fields (DOB, SSN, DriversLicense, ZipCode, City, State, Address1-3, Country) stop evaluation at Tier 2 — if Tier 1 and Tier 2 miss, those fields are marked as not found. The minimum threshold SHALL be 75. Confidence SHALL be ratio / 100 (e.g., ratio 82 produces confidence 0.82). The sliding window step size SHALL be `max(1, len(search_term) // 2)` characters (50% overlap between consecutive windows).

#### Scenario: Misspelled name fuzzy match
- **WHEN** the file text contains "Kerthik Chekuri" and the customer's Fullname is "Karthik Chekuri"
- **THEN** the Fullname field is detected with method "fuzzy", confidence between 0.80 and 0.90

#### Scenario: Reordered name tokens
- **WHEN** the file text contains "Chekuri Karthik" and the customer's Fullname is "Karthik Chekuri"
- **THEN** the Fullname field is detected with method "fuzzy", confidence 1.0 (token_set_ratio returns 100 for reordered tokens, confidence = 100/100)

#### Scenario: Severely misspelled name below threshold
- **WHEN** the file text contains "Zxywq Abcde" and the customer's Fullname is "Karthik Chekuri"
- **THEN** no match is detected for the Fullname field (ratio below 75)

#### Scenario: First name only with SSN in same file (disambiguation)
- **WHEN** the file text contains "Karthik" (first name only) AND neither Fullname nor LastName were detected (Fullname.found == false, LastName.found == false) AND the customer's SSN is also found in the same file
- **THEN** the FirstName field is detected with method "fuzzy", confidence 0.70 (disambiguation rule: SSN confirms identity)

#### Scenario: First name only without SSN in same file (disambiguation)
- **WHEN** the file text contains "Karthik" (first name only) AND neither Fullname nor LastName were detected AND the customer's SSN is NOT found in the file
- **THEN** the FirstName field is detected with method "fuzzy", confidence between 0.30 and 0.50 (low confidence, flagged for review)

#### Scenario: First name matches but Fullname also matches (no disambiguation)
- **WHEN** the file text contains "Karthik Chekuri" and the customer's Fullname is "Karthik Chekuri"
- **THEN** the disambiguation rule does NOT apply — Fullname gets its standard confidence (normalized 0.95 or fuzzy ratio/100) and FirstName is evaluated independently via the standard three-tier cascade

#### Scenario: Country normalized match
- **WHEN** the file text contains "united states" and the customer's Country is "United States"
- **THEN** the Country field is detected with method "normalized", confidence 0.95

### Requirement: Three-tier cascade evaluation order
The system SHALL evaluate each PII field against the file text in order: Tier 1 (exact) first, then Tier 2 (normalized) only if Tier 1 misses, then Tier 3 (fuzzy) only if Tier 2 misses. The first tier that matches wins.

#### Scenario: Exact match short-circuits normalized and fuzzy
- **WHEN** the file text contains the exact SSN "343-43-4343"
- **THEN** Tier 1 returns confidence 1.0 and Tiers 2 and 3 are not evaluated for that field

#### Scenario: Normalized match short-circuits fuzzy
- **WHEN** the file text contains "karthik chekuri" (case difference only)
- **THEN** Tier 2 returns confidence 0.95 and Tier 3 is not evaluated for that field

### Requirement: Null PII field handling
When a PII field is null or empty in the customer's master_data record, the system SHALL skip detection for that field entirely. The field SHALL be reported as `found: false`, `method: "none"`, `confidence: 0.0`, `snippet: null`. Null fields are excluded from the OtherFields_avg calculation (see confidence-scoring spec). This commonly applies to Address2, Address3, and Country, which are often unpopulated.

#### Scenario: Customer has null Address2 and Address3
- **WHEN** the customer's Address2 and Address3 are null in master_data
- **THEN** Address2 and Address3 are reported as `found: false, method: "none", confidence: 0.0` without scanning the file text, and they are excluded from the OtherFields_avg denominator

#### Scenario: Customer has all non-anchor fields populated
- **WHEN** all 9 non-anchor fields (DOB, DriversLicense, Address1-3, ZipCode, City, State, Country) are non-null
- **THEN** all 9 fields are evaluated through the three-tier cascade and all 9 are included in the OtherFields_avg denominator

### Requirement: Per-field match output
For each of the 13 PII fields, leak detection SHALL output: `found` (boolean), `method` (exact | normalized | fuzzy | partial | none), `confidence` (0.0–1.0), and `snippet` (surrounding text context, up to 100 characters around the match).

#### Scenario: Field found with snippet
- **WHEN** the SSN "343-43-4343" is found at position 150 in a 500-character file
- **THEN** the output includes `found: true`, `method: "exact"`, `confidence: 1.0`, and a snippet of ~100 characters centered on the match

#### Scenario: Field not found
- **WHEN** the customer's DriversLicense is not found in the file by any tier
- **THEN** the output includes `found: false`, `method: "none"`, `confidence: 0.0`, and `snippet: null`
