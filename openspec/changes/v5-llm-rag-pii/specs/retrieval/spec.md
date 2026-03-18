# Retrieval Service Spec (V5)

## Overview

The retrieval service performs vector search against `breach-file-index-v5` using embedded customer PII as the query vector. Returns all relevant files above the similarity threshold — no hard cap.

## Scenarios

### Scenario 1: Retrieve relevant files for a customer

**Given** `breach-file-index-v5` contains 1000+ indexed files with embeddings
**And** customer "John Smith" has PII: SSN 123-45-6789, DOB 1985-03-15, etc.
**When** `retrieve_files_v5(customer_vector)` is called with the embedded customer PII
**Then** Azure AI Search vector query is executed on `content_vector` field
**And** all results above the relevance threshold are returned
**And** each result contains: md5, file_path, content, file_name, file_type, `@search.score`

### Scenario 2: No relevant files found

**Given** `breach-file-index-v5` contains files
**And** customer PII has no match in any file
**When** `retrieve_files_v5(customer_vector)` is called
**Then** an empty list is returned
**And** no error is raised

### Scenario 3: Large result set

**Given** customer PII appears in 200+ files
**When** `retrieve_files_v5(customer_vector)` is called
**Then** all 200+ results are returned (no hard cap)
**And** results are ordered by similarity score (descending)

### Scenario 4: Result structure

**Given** a successful vector search
**When** results are returned
**Then** each result is a dict with keys:
  - `md5` (str)
  - `file_path` (str)
  - `content` (str)
  - `file_name` (str)
  - `file_type` (str)
  - `search_score` (float) — the `@search.score` from Azure AI Search

### Scenario 5: Azure Search unavailable

**Given** Azure AI Search endpoint is unreachable
**When** `retrieve_files_v5(customer_vector)` is called
**Then** an exception is raised with a descriptive error message
**And** the caller (batch service) handles the error per customer
