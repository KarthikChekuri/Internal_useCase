"""Pydantic schemas for V2 batch request/response models.

Covers:
- Batch run lifecycle (start, status, list, resume)
- Per-customer status tracking
- Batch result items
- Sub-schemas for indexing/searching/detection progress
"""

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from app.schemas.pii import FieldMatchResult


# ---------------------------------------------------------------------------
# Batch run trigger
# ---------------------------------------------------------------------------


class BatchRunResponse(BaseModel):
    """Response for POST /batch/run.

    Attributes:
        batch_id: UUID identifying the new batch run.
        status: Current status (always 'running' at creation time).
        total_customers: Total number of customers to process.
    """

    batch_id: uuid.UUID
    status: str
    total_customers: int


class BatchConflictResponse(BaseModel):
    """Response body for 409 Conflict when a batch is already running.

    Attributes:
        detail: Human-readable description of the conflict.
    """

    detail: str


# ---------------------------------------------------------------------------
# Batch status sub-schemas
# ---------------------------------------------------------------------------


class IndexingStatus(BaseModel):
    """Indexing progress sub-object within BatchStatusResponse.

    Attributes:
        total: Total number of files in the DLU eligible for indexing.
        indexed: Files successfully indexed.
        failed: Files that failed extraction or upload.
        skipped: Files already indexed (resumability).
    """

    total: int
    indexed: int
    failed: int
    skipped: int


class SearchingStatus(BaseModel):
    """Per-customer search progress sub-object within BatchStatusResponse.

    Attributes:
        total_customers: Total number of customers in the batch.
        completed: Customers fully processed (status='complete').
        failed: Customers that failed processing (status='failed').
        pending: Customers not yet processed (status='pending' or 'searching'/'detecting').
    """

    total_customers: int
    completed: int
    failed: int
    pending: int


class DetectionStatus(BaseModel):
    """Detection progress sub-object within BatchStatusResponse.

    Attributes:
        total_pairs_processed: Total (customer, file) pairs evaluated.
        leaks_found: Pairs where at least one PII field was detected.
    """

    total_pairs_processed: int
    leaks_found: int


# ---------------------------------------------------------------------------
# Batch status response
# ---------------------------------------------------------------------------


class BatchStatusResponse(BaseModel):
    """Response for GET /batch/{batch_id}/status.

    Attributes:
        batch_id: UUID of the batch run.
        status: Overall batch status (pending/running/completed/failed).
        started_at: When the batch started.
        completed_at: When the batch finished (None if still running).
        strategy_set: List of strategy names used for this run.
        indexing: Indexing progress breakdown.
        searching: Per-customer search progress breakdown.
        detection: Leak detection progress breakdown.
    """

    batch_id: uuid.UUID
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    strategy_set: list[str]
    indexing: IndexingStatus
    searching: SearchingStatus
    detection: DetectionStatus


# ---------------------------------------------------------------------------
# Per-customer status
# ---------------------------------------------------------------------------


class CustomerStatusItem(BaseModel):
    """A single customer's status within a batch run.

    Used in GET /batch/{batch_id}/customers response.

    Attributes:
        customer_id: FK to master_data.
        status: Customer processing status (pending/searching/detecting/complete/failed).
        candidates_found: Number of unique candidate files from search.
        leaks_confirmed: Number of files with at least one PII field detected.
        strategies_matched: List of strategy names that returned results.
        error_message: Error details if status is 'failed'.
        processed_at: When processing completed/failed (None if still pending).
    """

    customer_id: int
    status: str
    candidates_found: int
    leaks_confirmed: int
    strategies_matched: list[str]
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Batch results
# ---------------------------------------------------------------------------


class BatchResultItem(BaseModel):
    """A single (customer, file) result row in a batch run.

    Used in GET /batch/{batch_id}/results response.

    Attributes:
        batch_id: UUID of the batch run.
        customer_id: FK to master_data.
        md5: MD5 hash of the file (FK to datalakeuniverse).
        strategy_name: Name of the first strategy that found this file.
        leaked_fields: List of PII field names that were detected.
        match_details: Per-field detection results keyed by field name.
        overall_confidence: Computed confidence score for the pair.
        azure_search_score: Raw Azure AI Search score (highest across strategies).
        needs_review: True for disambiguation cases or no-anchor matches.
        searched_at: Timestamp when this result was written.
    """

    batch_id: uuid.UUID
    customer_id: int
    md5: str
    strategy_name: str
    leaked_fields: list[str]
    match_details: dict[str, FieldMatchResult]
    overall_confidence: float
    azure_search_score: float
    needs_review: bool
    searched_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Batch list
# ---------------------------------------------------------------------------


class BatchSummaryItem(BaseModel):
    """Summary row in the GET /batches response.

    Attributes:
        batch_id: UUID of the batch run.
        status: Overall batch status.
        started_at: When the batch started.
        completed_at: When the batch finished (None if still running).
        total_customers: Total customers in this batch.
        strategy_count: Number of strategies used.
    """

    batch_id: uuid.UUID
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    total_customers: int
    strategy_count: int


# ---------------------------------------------------------------------------
# Resume
# ---------------------------------------------------------------------------


class ResumeResponse(BaseModel):
    """Response for POST /batch/{batch_id}/resume.

    Attributes:
        batch_id: UUID of the resumed batch.
        status: Current status after resumption (typically 'running').
        message: Human-readable description of the action taken.
    """

    batch_id: uuid.UUID
    status: str
    message: str
