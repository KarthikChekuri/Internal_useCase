"""Pydantic schemas for V3 Azure-only batch request/response models.

V3 uses per-field Lucene queries against Azure AI Search. Detection IS the
search — no Python regex or rapidfuzz. Results carry search scores and optional
hit-highlighting snippets instead of match methods.

Covers:
- V3FieldMatch: per-field found/score/snippet (score+snippet excluded when found=False)
- V3DocumentResult: merged per-document result across all field queries
- V3BatchRunResponse: response for POST /v3/batch/run
- V3BatchStatusResponse: response for GET /v3/batch/{id}/status
- V3BatchResultResponse: single result row from GET /v3/batch/{id}/results
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# V3 field match
# ---------------------------------------------------------------------------


class V3FieldMatch(BaseModel):
    """Result of a single per-field Lucene query in V3.

    When found=False, score and snippet are excluded from serialization.
    When found=True, score and snippet are included (snippet may still be
    None when Azure AI Search cannot produce a fuzzy highlight).

    Attributes:
        found: Whether the per-field query matched this document.
        score: Normalized search score (only present when found=True).
        snippet: Hit-highlighted snippet from Azure (only present when found=True,
                 may be None for fuzzy queries where highlighting is unavailable).
    """

    found: bool
    score: Optional[float] = None
    snippet: Optional[str] = None

    def model_dump(self, **kwargs) -> dict:
        """Serialize, excluding score and snippet when found=False."""
        if not self.found:
            # When not found, always exclude score and snippet
            kwargs.setdefault("exclude_none", True)
            data = super().model_dump(**kwargs)
            data.pop("score", None)
            data.pop("snippet", None)
            return data
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs) -> str:
        """Serialize to JSON string, excluding score and snippet when found=False."""
        if not self.found:
            kwargs.setdefault("exclude_none", True)
            # Build a minimal dict and re-serialize
            import json

            return json.dumps({"found": False})
        return super().model_dump_json(**kwargs)


# ---------------------------------------------------------------------------
# V3 document result
# ---------------------------------------------------------------------------


class V3DocumentResult(BaseModel):
    """Merged result for a single document across all per-field queries.

    Attributes:
        md5: MD5 hash of the file (FK to datalakeuniverse).
        file_path: Optional path to the file on disk.
        leaked_fields: Field names where the per-field query returned results.
        match_details: Per-field results keyed by field name.
        overall_confidence: Weighted confidence score (0.0–1.0).
        azure_search_score: Highest per-field raw search score for this document.
        needs_review: True when confidence < 0.5 or only first name matched.
    """

    md5: str
    file_path: Optional[str] = None
    leaked_fields: list[str]
    match_details: dict[str, V3FieldMatch]
    overall_confidence: float
    azure_search_score: float
    needs_review: bool


# ---------------------------------------------------------------------------
# V3 batch run response
# ---------------------------------------------------------------------------


class V3BatchRunResponse(BaseModel):
    """Response for POST /v3/batch/run.

    Attributes:
        batch_id: UUID string identifying the new V3 batch run.
        status: Current status (always 'running' at creation time).
        total_customers: Total number of customers to process.
        method: Always 'v3_azure_only'.
    """

    batch_id: str
    status: str
    total_customers: int
    method: str


# ---------------------------------------------------------------------------
# V3 batch status response
# ---------------------------------------------------------------------------


class V3BatchStatusResponse(BaseModel):
    """Response for GET /v3/batch/{id}/status.

    Follows the same high-level shape as the V2 batch status but with a
    simpler per-customer tracking model (no separate indexing/detection
    sub-objects since V3 has no separate detection phase).

    Attributes:
        batch_id: UUID string of the batch run.
        status: Overall batch status (pending/running/completed/failed).
        total_customers: Total customers in this batch.
        customers_completed: Customers with status='complete'.
        customers_failed: Customers with status='failed'.
        customer_details: List of per-customer status dicts.
        method: Always 'v3_azure_only'.
    """

    batch_id: str
    status: str
    total_customers: int
    customers_completed: int
    customers_failed: int
    customer_details: list[Any]
    method: str = "v3_azure_only"


# ---------------------------------------------------------------------------
# V3 batch result response
# ---------------------------------------------------------------------------


class V3BatchResultResponse(BaseModel):
    """A single (customer, file) result row from GET /v3/batch/{id}/results.

    Attributes:
        batch_id: UUID string of the batch run.
        customer_id: FK to master_data.
        md5: MD5 hash of the file.
        strategy_name: Always 'v3_azure_only'.
        leaked_fields: Field names where per-field query returned results.
        match_details: Per-field V3 results as a plain dict (for flexible storage).
        overall_confidence: Weighted confidence score (0.0–1.0).
        azure_search_score: Highest per-field raw search score.
        needs_review: True for low-confidence or single-name matches.
        searched_at: Timestamp when this result was written.
    """

    batch_id: str
    customer_id: int
    md5: str
    strategy_name: str
    leaked_fields: list[str]
    match_details: dict[str, Any]
    overall_confidence: float
    azure_search_score: float
    needs_review: bool
    searched_at: datetime
