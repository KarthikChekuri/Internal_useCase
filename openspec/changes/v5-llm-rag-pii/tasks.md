# V5 LLM RAG PII — Tasks

## Phase V5-1: Configuration & Embedding Service
**Depends on:** nothing
**Spec:** `specs/embedding/spec.md`

- [ ] Add V5 env vars to `config.py` (AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, deployment names, AZURE_SEARCH_INDEX_V5, V5_LLM_BATCH_SIZE)
- [ ] Add `openai` dependency to `pyproject.toml`
- [ ] Create `app/services/embedding_service.py`:
  - `create_v5_index()` — creates `breach-file-index-v5` with vector field
  - `embed_text(text)` — calls Azure OpenAI embedding model, returns 1536-dim vector
  - `embed_and_index_file(md5, file_path, content)` — embeds + uploads to index
  - `index_all_v5(db)` — batch index all DLU files, skip already-indexed
  - `embed_customer_pii(customer)` — embeds concatenated PII string
- [ ] Write tests: `tests/services/test_embedding_service.py`
  - Mock Azure OpenAI and Azure AI Search clients
  - Test index creation, single file embed+index, batch indexing, skip logic, error handling
- [ ] Update `.env.example` with V5 variables

## Phase V5-2: Retrieval Service
**Depends on:** V5-1
**Spec:** `specs/retrieval/spec.md`

- [ ] Create `app/services/retrieval_service_v5.py`:
  - `retrieve_files_v5(search_client, customer_vector)` — vector search, returns all relevant results
- [ ] Write tests: `tests/services/test_retrieval_service_v5.py`
  - Mock Azure AI Search client
  - Test result structure, empty results, large result sets, error handling

## Phase V5-3: LLM Detection Service
**Depends on:** nothing (can parallel with V5-1 and V5-2)
**Spec:** `specs/detection/spec.md`

- [ ] Create `app/services/detection_service_v5.py`:
  - `build_detection_prompt(customer, file_chunks)` — constructs the LLM prompt
  - `detect_pii_batch(openai_client, customer, file_chunks)` — sends to GPT-4o, parses JSON response
  - `parse_llm_response(response_json)` — validates and normalizes LLM output
- [ ] Write tests: `tests/services/test_detection_service_v5.py`
  - Mock Azure OpenAI chat client
  - Test prompt construction, response parsing, paraphrased detection, format variants, contextual detection, invalid JSON handling, retry logic

## Phase V5-4: Batch Service V5
**Depends on:** V5-1, V5-2, V5-3
**Spec:** `specs/batch-v5/spec.md`

- [ ] Create `app/services/batch_service_v5.py`:
  - `run_batch_v5(db)` — orchestrates full batch run
  - `_process_customer_v5(customer, batch_id, db)` — per-customer pipeline (embed → retrieve → detect → persist)
  - `_persist_v5_results(db, customer_id, batch_id, results, search_scores)` — insert into Search.results
  - `_merge_and_deduplicate(batch_results)` — merge across LLM batches, keep highest confidence
- [ ] Write tests: `tests/services/test_batch_service_v5.py`
  - Mock embedding, retrieval, and detection services
  - Test full pipeline, deduplication, partial failures, no-results, persistence

## Phase V5-5: CLI Integration
**Depends on:** V5-4
**Spec:** `specs/batch-v5/spec.md` (Scenario 9)

- [ ] Add `--v5` flag to `index` command in `app/cli.py`
- [ ] Add `--v5` flag to `run` command in `app/cli.py`
- [ ] Update `compare` command to support 3-way comparison (V2 vs V3 vs V5)
- [ ] Write tests: update `tests/test_cli.py` with V5 command tests
