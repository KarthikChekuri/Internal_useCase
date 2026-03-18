# Batch Service V5 Spec

## Overview

The V5 batch service orchestrates the full pipeline per customer: embed PII → vector retrieve → batch LLM detection → persist results. Uses the same batch tracking infrastructure as V2/V3.

## Scenarios

### Scenario 1: Full batch run (happy path)

**Given** 10 customers in `PII.master_data`
**And** 1000+ files indexed in `breach-file-index-v5` with embeddings
**When** `poetry run breach-search run --v5` is called
**Then** a new batch run is created in `Batch.batch_runs` with method "v5_llm_rag"
**And** for each customer:
  1. Customer PII is embedded via Azure OpenAI
  2. Vector search retrieves all relevant files
  3. Files are batched (10 per group)
  4. Each batch is sent to GPT-4o for PII detection
  5. Results are parsed and inserted into `Search.results`
  6. Customer status is updated in `Batch.customer_status`
**And** batch status is set to "completed" when all customers are processed

### Scenario 2: Per-customer processing

**Given** customer "John Smith" with known PII
**And** vector search returns 30 relevant files
**When** `_process_customer_v5(customer, batch_id, db)` is called
**Then** 3 LLM calls are made (30 files ÷ 10 per call)
**And** results from all 3 calls are merged
**And** duplicate md5s are deduplicated (keep highest confidence)
**And** results are inserted into `Search.results` with strategy_name = '["llm_v5"]'

### Scenario 3: Result persistence

**Given** LLM detection found PII in file "abc123" for customer 1
**With** leaked_ssn = "123-45-6789" (confidence: 99), leaked_first_name = "John" (confidence: 95)
**When** the result is persisted
**Then** a row is inserted into `Search.results`:
  - `customer_id`: 1
  - `md5`: "abc123"
  - `file_path`: from retrieval result
  - `strategy_name`: '["llm_v5"]'
  - `leaked_ssn`: "123-45-6789"
  - `leaked_first_name`: "John"
  - `confidence`: 0.97 (average of 99 and 95, divided by 100)
  - `azure_search_score`: vector similarity score from retrieval
  - `needs_review`: false (confidence > 0.5)
  - All other `leaked_*` fields: NULL

### Scenario 4: No relevant files for a customer

**Given** customer "Jane Doe" has no matching files in vector search
**When** `_process_customer_v5(customer, batch_id, db)` is called
**Then** no LLM calls are made
**And** customer status is set to "completed" with 0 results
**And** processing continues to next customer

### Scenario 5: LLM call fails for one batch

**Given** customer has 30 relevant files (3 batches)
**And** LLM call for batch 2 fails after retry
**When** `_process_customer_v5()` processes all batches
**Then** batches 1 and 3 results are persisted successfully
**And** batch 2 failure is logged
**And** customer status is set to "partial" (some results missing)

### Scenario 6: Customer processing fails entirely

**Given** embedding the customer PII fails (Azure OpenAI error)
**When** `_process_customer_v5(customer, batch_id, db)` is called
**Then** customer status is set to "failed" with error message
**And** batch run continues with the next customer

### Scenario 7: Batch status tracking

**Given** a V5 batch run in progress
**When** `poetry run breach-search status <BATCH_ID>` is called
**Then** the status shows:
  - Total customers: 10
  - Completed: N
  - Failed: M
  - Method: "v5_llm_rag"

### Scenario 8: Compare V2 vs V3 vs V5

**Given** completed batch runs for V2, V3, and V5 on the same data
**When** `poetry run breach-search compare <V2_ID> <V3_ID> <V5_ID>` is called
**Then** a side-by-side comparison is shown
**And** differences in detection (V5 found but V2/V3 missed) are highlighted
**And** per-field confidence is compared across versions

### Scenario 9: CLI integration

**Given** V5 dependencies are configured (Azure OpenAI endpoint, key, deployments)
**When** `poetry run breach-search index --v5` is called
**Then** all DLU files are embedded and indexed into `breach-file-index-v5`

**When** `poetry run breach-search run --v5` is called
**Then** a full V5 batch run is executed

### Scenario 10: Deduplication across batches

**Given** customer has 30 files, split into 3 LLM batches
**And** file "abc123" appears in batch 1 results AND batch 3 results (edge case)
**When** results are merged
**Then** only one row is inserted for "abc123"
**And** the row with the highest average confidence is kept
