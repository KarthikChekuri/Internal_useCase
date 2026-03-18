# LLM Detection Service Spec (V5)

## Overview

The detection service sends batches of 10 files + customer PII to GPT-4o and parses the structured JSON response into per-file PII detection results.

## Scenarios

### Scenario 1: Detect PII in a batch of files

**Given** 10 file chunks with content, md5, and file_path
**And** customer PII (first_name, last_name, ssn, dob, address, city, state, zip, email, phone, drivers_license)
**When** `detect_pii_batch(customer, file_chunks)` is called
**Then** a prompt is constructed with customer PII + 10 file contents
**And** the prompt is sent to GPT-4o with `temperature=0` and `response_format=json`
**And** the JSON response is parsed into a list of detection results
**And** each result contains: md5, file_path, leaked_* fields with value + confidence

### Scenario 2: File with no PII detected

**Given** a batch of 10 files where file 3 contains no customer PII
**When** `detect_pii_batch(customer, file_chunks)` is called
**Then** file 3 is omitted from the response
**And** only files with detected PII are returned

### Scenario 3: Paraphrased PII detection

**Given** a file containing "The individual lives on main street in springfield"
**And** customer address is "123 Main St, Springfield, IL"
**When** `detect_pii_batch(customer, [file])` is called
**Then** `leaked_address` is returned with the paraphrased value
**And** confidence reflects the indirect match (e.g., 70-80)

### Scenario 4: Format variant detection

**Given** a file containing "Date of birth: March 15, 1985"
**And** customer DOB is "1985-03-15"
**When** `detect_pii_batch(customer, [file])` is called
**Then** `leaked_dob` is returned with value "March 15, 1985"
**And** confidence is high (e.g., 90+) since the date matches

### Scenario 5: Contextual PII detection

**Given** a file containing "his social security number was compromised" followed by "343-43-4343" two lines later
**And** customer SSN is "343-43-4343"
**When** `detect_pii_batch(customer, [file])` is called
**Then** `leaked_ssn` is returned with value "343-43-4343" and high confidence
**And** the LLM understands the contextual reference

### Scenario 6: Batch with fewer than 10 files

**Given** only 3 files to process (last batch for a customer)
**When** `detect_pii_batch(customer, file_chunks)` is called with 3 files
**Then** the prompt includes only 3 files
**And** detection works correctly

### Scenario 7: LLM returns invalid JSON

**Given** GPT-4o returns malformed or unexpected JSON
**When** the response is parsed
**Then** a warning is logged
**And** the batch is marked as failed
**And** processing continues with the next batch

### Scenario 8: LLM API error

**Given** Azure OpenAI endpoint returns a rate limit or server error
**When** `detect_pii_batch()` is called
**Then** the error is logged
**And** the batch is retried once after a short delay
**And** if retry fails, the batch is marked as failed

### Scenario 9: Detection result structure

**Given** a successful LLM detection
**When** the result is parsed
**Then** each file result contains:
  - `md5` (str)
  - `file_path` (str)
  - For each PII field (leaked_first_name, leaked_last_name, leaked_ssn, leaked_dob, leaked_address, leaked_city, leaked_state, leaked_zip, leaked_email, leaked_phone, leaked_drivers_license):
    - `value` (str or null) — the text found in the file
    - `confidence` (int 0-100) — LLM's confidence in the match
