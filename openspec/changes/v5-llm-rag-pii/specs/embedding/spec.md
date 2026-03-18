# Embedding Service Spec

## Overview

The embedding service handles two responsibilities:
1. **Indexing**: Embed breach file content and store vectors in Azure AI Search (`breach-file-index-v5`)
2. **Query**: Embed customer PII for vector search at retrieval time

## Index Schema

The V5 index (`breach-file-index-v5`) contains:

| Field | Type | Purpose |
|---|---|---|
| `id` | Edm.String (key) | MD5 hash |
| `md5` | Edm.String (filterable) | File identifier |
| `file_path` | Edm.String | Source path |
| `content` | Edm.String (searchable) | Raw text |
| `content_vector` | Collection(Edm.Single), 1536 dims | Embedding |
| `file_name` | Edm.String (filterable) | Original file name |
| `file_type` | Edm.String (filterable) | Extension |

Vector configuration: HNSW algorithm, cosine metric, 1536 dimensions.

## Scenarios

### Scenario 1: Create V5 index

**Given** Azure AI Search is accessible
**When** `create_v5_index()` is called
**Then** index `breach-file-index-v5` is created with all fields including vector field
**And** vector search profile is configured with HNSW + cosine

### Scenario 2: Embed and index a single file

**Given** a breach file exists at a known path with known MD5
**And** Azure OpenAI embedding endpoint is accessible
**When** `embed_and_index_file(md5, file_path, content)` is called
**Then** the content is sent to Azure OpenAI `text-embedding-3-small`
**And** a document is uploaded to `breach-file-index-v5` with all fields populated
**And** `content_vector` contains a 1536-dimension float array

### Scenario 3: Batch index all files from DLU

**Given** DLU table contains N records with file paths
**And** all files are readable
**When** `index_all_v5(db)` is called
**Then** each file is embedded and indexed
**And** `Index.file_status` is updated per file (indexed/failed)
**And** files already indexed (status = "indexed" for index `breach-file-index-v5`) are skipped

### Scenario 4: Embed customer PII for query

**Given** a customer record with PII fields
**When** `embed_customer_pii(customer)` is called
**Then** all PII fields are concatenated into a single string
**And** the string is embedded using `text-embedding-3-small`
**And** a 1536-dimension float array is returned

### Scenario 5: Handle embedding API error

**Given** Azure OpenAI endpoint is unreachable or returns an error
**When** `embed_and_index_file()` is called
**Then** the file is marked as "failed" in `Index.file_status`
**And** the error is logged
**And** processing continues with the next file

### Scenario 6: Skip already-indexed files

**Given** file with MD5 "abc123" already has status "indexed" for `breach-file-index-v5`
**When** `index_all_v5(db)` is called
**Then** "abc123" is skipped
**And** only un-indexed files are processed
