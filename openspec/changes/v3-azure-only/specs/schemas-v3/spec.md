## ADDED Requirements

### Requirement: V3 field match response model
The system SHALL define a `V3FieldMatch` Pydantic model representing a single PII field's match result from a per-field query.

#### Scenario: Found field serialization
- **WHEN** a V3FieldMatch is created with `found=True, score=0.83, snippet="...[[MATCH]]343-43-4343[[/MATCH]]..."`
- **THEN** it serializes to `{ "found": true, "score": 0.83, "snippet": "...[[MATCH]]343-43-4343[[/MATCH]]..." }`

#### Scenario: Not-found field serialization
- **WHEN** a V3FieldMatch is created with `found=False`
- **THEN** it serializes to `{ "found": false }` (score and snippet are excluded)

#### Scenario: Found field with no snippet
- **WHEN** a V3FieldMatch is created with `found=True, score=0.67, snippet=None` (fuzzy match without highlight)
- **THEN** it serializes to `{ "found": true, "score": 0.67, "snippet": null }`

### Requirement: V3 document result response model
The system SHALL define a `V3DocumentResult` Pydantic model representing a single document's merged search results across all per-field queries.

#### Scenario: Document result with multiple matched fields
- **WHEN** a V3DocumentResult has `md5="abc123"`, `leaked_fields=["SSN", "Fullname"]`, `overall_confidence=0.72`, `needs_review=False`
- **THEN** it serializes with all fields including `match_details` as a dict of field_name -> V3FieldMatch

#### Scenario: Document result validation
- **WHEN** a V3DocumentResult is created
- **THEN** it requires: `md5` (str), `leaked_fields` (list[str]), `match_details` (dict[str, V3FieldMatch]), `overall_confidence` (float, 0.0-1.0), `azure_search_score` (float), `needs_review` (bool)

### Requirement: V3 batch run response model
The system SHALL define a `V3BatchRunResponse` Pydantic model for the `POST /v3/batch/run` endpoint response.

#### Scenario: V3 batch start response
- **WHEN** a V3 batch is started
- **THEN** the response includes `{ "batch_id": "uuid", "status": "running", "total_customers": N, "method": "v3_azure_only" }`

### Requirement: V3 batch status response model
The system SHALL define a `V3BatchStatusResponse` Pydantic model for the `GET /v3/batch/{id}/status` endpoint response. It follows the same structure as V2's batch status response.

#### Scenario: V3 batch status response
- **WHEN** `GET /v3/batch/{id}/status` returns
- **THEN** the response includes `batch_id`, `status`, `total_customers`, `customers_completed`, `customers_failed`, and `customer_details` (list of per-customer status)

### Requirement: V3 batch results response model
The system SHALL define a `V3BatchResultsResponse` Pydantic model for the `GET /v3/batch/{id}/results` endpoint response.

#### Scenario: V3 batch results response
- **WHEN** `GET /v3/batch/{id}/results` returns
- **THEN** the response includes `batch_id`, `total_results`, and `results` (list of V3DocumentResult)

## MODIFIED Requirements

None.

## REMOVED Requirements

None.
