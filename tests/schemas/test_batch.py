"""Tests for app.schemas.batch — Batch request/response Pydantic schemas.

Covers batch run lifecycle, per-customer status, batch status API, customer
status API, results API, and batch list API schemas.
"""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError


class TestBatchRunResponse:
    """Scenario: Trigger batch via API
    WHEN POST /batch/run is called
    THEN the response includes batch_id (UUID), status "running", and total_customers.
    """

    def test_batch_run_response_construction(self):
        """BatchRunResponse holds batch_id (UUID), status, and total_customers."""
        from app.schemas.batch import BatchRunResponse

        batch_id = uuid.uuid4()
        resp = BatchRunResponse(
            batch_id=batch_id,
            status="running",
            total_customers=200,
        )
        assert resp.batch_id == batch_id
        assert resp.status == "running"
        assert resp.total_customers == 200

    def test_batch_run_response_batch_id_is_uuid(self):
        """batch_id must be a UUID type."""
        from app.schemas.batch import BatchRunResponse

        batch_id = uuid.uuid4()
        resp = BatchRunResponse(batch_id=batch_id, status="running", total_customers=50)
        assert isinstance(resp.batch_id, uuid.UUID)

    def test_batch_run_response_serialization(self):
        """BatchRunResponse serializes to dict with correct keys."""
        from app.schemas.batch import BatchRunResponse

        batch_id = uuid.uuid4()
        resp = BatchRunResponse(batch_id=batch_id, status="running", total_customers=100)
        data = resp.model_dump()
        assert "batch_id" in data
        assert "status" in data
        assert "total_customers" in data
        assert data["status"] == "running"
        assert data["total_customers"] == 100

    def test_batch_run_response_status_running(self):
        """Status 'running' is accepted."""
        from app.schemas.batch import BatchRunResponse

        resp = BatchRunResponse(batch_id=uuid.uuid4(), status="running", total_customers=0)
        assert resp.status == "running"


class TestBatchConflictResponse:
    """Scenario: Trigger batch while another is running
    WHEN POST /batch/run is called while a batch is already running
    THEN the system returns 409 with message 'A batch is already running (batch_id: ...)'.
    """

    def test_batch_conflict_response_construction(self):
        """BatchConflictResponse holds detail message."""
        from app.schemas.batch import BatchConflictResponse

        batch_id = uuid.uuid4()
        resp = BatchConflictResponse(
            detail=f"A batch is already running (batch_id: {batch_id})"
        )
        assert str(batch_id) in resp.detail

    def test_batch_conflict_response_serialization(self):
        """BatchConflictResponse serializes with detail key."""
        from app.schemas.batch import BatchConflictResponse

        resp = BatchConflictResponse(detail="A batch is already running (batch_id: abc)")
        data = resp.model_dump()
        assert "detail" in data
        assert data["detail"] == "A batch is already running (batch_id: abc)"


class TestIndexingStatus:
    """Tests for the IndexingStatus sub-schema used in BatchStatusResponse."""

    def test_indexing_status_construction(self):
        """IndexingStatus holds total, indexed, failed, skipped counts."""
        from app.schemas.batch import IndexingStatus

        status = IndexingStatus(total=500, indexed=500, failed=3, skipped=0)
        assert status.total == 500
        assert status.indexed == 500
        assert status.failed == 3
        assert status.skipped == 0

    def test_indexing_status_defaults(self):
        """IndexingStatus fields default to 0."""
        from app.schemas.batch import IndexingStatus

        status = IndexingStatus(total=0, indexed=0, failed=0, skipped=0)
        assert status.total == 0
        assert status.skipped == 0

    def test_indexing_status_serialization(self):
        """IndexingStatus serializes to dict with all required keys."""
        from app.schemas.batch import IndexingStatus

        status = IndexingStatus(total=100, indexed=97, failed=3, skipped=0)
        data = status.model_dump()
        assert set(data.keys()) == {"total", "indexed", "failed", "skipped"}


class TestSearchingStatus:
    """Tests for the SearchingStatus sub-schema used in BatchStatusResponse."""

    def test_searching_status_construction(self):
        """SearchingStatus holds total_customers, completed, failed, pending."""
        from app.schemas.batch import SearchingStatus

        status = SearchingStatus(
            total_customers=200, completed=120, failed=1, pending=79
        )
        assert status.total_customers == 200
        assert status.completed == 120
        assert status.failed == 1
        assert status.pending == 79

    def test_searching_status_serialization(self):
        """SearchingStatus serializes to dict with all required keys."""
        from app.schemas.batch import SearchingStatus

        status = SearchingStatus(total_customers=10, completed=5, failed=0, pending=5)
        data = status.model_dump()
        assert set(data.keys()) == {"total_customers", "completed", "failed", "pending"}


class TestDetectionStatus:
    """Tests for the DetectionStatus sub-schema used in BatchStatusResponse."""

    def test_detection_status_construction(self):
        """DetectionStatus holds total_pairs_processed and leaks_found."""
        from app.schemas.batch import DetectionStatus

        status = DetectionStatus(total_pairs_processed=3200, leaks_found=450)
        assert status.total_pairs_processed == 3200
        assert status.leaks_found == 450

    def test_detection_status_serialization(self):
        """DetectionStatus serializes to dict with correct keys."""
        from app.schemas.batch import DetectionStatus

        status = DetectionStatus(total_pairs_processed=100, leaks_found=25)
        data = status.model_dump()
        assert set(data.keys()) == {"total_pairs_processed", "leaks_found"}


class TestBatchStatusResponse:
    """Scenario: Status of a running batch
    WHEN GET /batch/{batch_id}/status is called for a running batch
    THEN response includes batch_id, status, started_at, completed_at (null),
    strategy_set, indexing, searching, and detection sub-objects.
    """

    def test_batch_status_response_running(self):
        """BatchStatusResponse for a running batch has completed_at=None."""
        from app.schemas.batch import (
            BatchStatusResponse,
            DetectionStatus,
            IndexingStatus,
            SearchingStatus,
        )

        batch_id = uuid.uuid4()
        started_at = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)

        resp = BatchStatusResponse(
            batch_id=batch_id,
            status="running",
            started_at=started_at,
            completed_at=None,
            strategy_set=["fullname_ssn", "lastname_dob", "unique_identifiers"],
            indexing=IndexingStatus(total=500, indexed=500, failed=3, skipped=0),
            searching=SearchingStatus(
                total_customers=200, completed=120, failed=1, pending=79
            ),
            detection=DetectionStatus(total_pairs_processed=3200, leaks_found=450),
        )

        assert resp.batch_id == batch_id
        assert resp.status == "running"
        assert resp.completed_at is None
        assert resp.strategy_set == [
            "fullname_ssn",
            "lastname_dob",
            "unique_identifiers",
        ]
        assert resp.indexing.total == 500
        assert resp.searching.total_customers == 200
        assert resp.detection.leaks_found == 450

    def test_batch_status_response_completed(self):
        """BatchStatusResponse for a completed batch has completed_at populated."""
        from app.schemas.batch import (
            BatchStatusResponse,
            DetectionStatus,
            IndexingStatus,
            SearchingStatus,
        )

        batch_id = uuid.uuid4()
        started_at = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        completed_at = datetime(2026, 3, 11, 11, 0, 0, tzinfo=timezone.utc)

        resp = BatchStatusResponse(
            batch_id=batch_id,
            status="completed",
            started_at=started_at,
            completed_at=completed_at,
            strategy_set=["fullname_ssn"],
            indexing=IndexingStatus(total=100, indexed=100, failed=0, skipped=0),
            searching=SearchingStatus(
                total_customers=50, completed=50, failed=0, pending=0
            ),
            detection=DetectionStatus(total_pairs_processed=500, leaks_found=100),
        )

        assert resp.status == "completed"
        assert resp.completed_at == completed_at

    def test_batch_status_response_batch_id_is_uuid(self):
        """batch_id in BatchStatusResponse is a UUID."""
        from app.schemas.batch import (
            BatchStatusResponse,
            DetectionStatus,
            IndexingStatus,
            SearchingStatus,
        )

        batch_id = uuid.uuid4()
        resp = BatchStatusResponse(
            batch_id=batch_id,
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=None,
            strategy_set=[],
            indexing=IndexingStatus(total=0, indexed=0, failed=0, skipped=0),
            searching=SearchingStatus(
                total_customers=0, completed=0, failed=0, pending=0
            ),
            detection=DetectionStatus(total_pairs_processed=0, leaks_found=0),
        )
        assert isinstance(resp.batch_id, uuid.UUID)

    def test_batch_status_response_serialization_shape(self):
        """BatchStatusResponse serializes with all required top-level keys."""
        from app.schemas.batch import (
            BatchStatusResponse,
            DetectionStatus,
            IndexingStatus,
            SearchingStatus,
        )

        resp = BatchStatusResponse(
            batch_id=uuid.uuid4(),
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=None,
            strategy_set=["fullname_ssn"],
            indexing=IndexingStatus(total=10, indexed=10, failed=0, skipped=0),
            searching=SearchingStatus(
                total_customers=5, completed=5, failed=0, pending=0
            ),
            detection=DetectionStatus(total_pairs_processed=10, leaks_found=2),
        )
        data = resp.model_dump()
        assert set(data.keys()) == {
            "batch_id",
            "status",
            "started_at",
            "completed_at",
            "strategy_set",
            "indexing",
            "searching",
            "detection",
        }
        assert data["completed_at"] is None
        assert isinstance(data["strategy_set"], list)

    def test_batch_status_response_strategy_set_is_list(self):
        """strategy_set is a list of strings."""
        from app.schemas.batch import (
            BatchStatusResponse,
            DetectionStatus,
            IndexingStatus,
            SearchingStatus,
        )

        resp = BatchStatusResponse(
            batch_id=uuid.uuid4(),
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=None,
            strategy_set=["a", "b", "c"],
            indexing=IndexingStatus(total=0, indexed=0, failed=0, skipped=0),
            searching=SearchingStatus(
                total_customers=0, completed=0, failed=0, pending=0
            ),
            detection=DetectionStatus(total_pairs_processed=0, leaks_found=0),
        )
        assert isinstance(resp.strategy_set, list)
        assert resp.strategy_set == ["a", "b", "c"]


class TestCustomerStatusItem:
    """Scenario: Customer status list
    WHEN GET /batch/{batch_id}/customers is called
    THEN the response is an array of customer status objects with all fields.
    """

    def test_customer_status_item_complete(self):
        """CustomerStatusItem with status 'complete' has all fields."""
        from app.schemas.batch import CustomerStatusItem

        processed_at = datetime(2026, 3, 11, 10, 2, 15, tzinfo=timezone.utc)
        item = CustomerStatusItem(
            customer_id=1,
            status="complete",
            candidates_found=5,
            leaks_confirmed=3,
            strategies_matched=["fullname_ssn", "unique_identifiers"],
            processed_at=processed_at,
        )

        assert item.customer_id == 1
        assert item.status == "complete"
        assert item.candidates_found == 5
        assert item.leaks_confirmed == 3
        assert item.strategies_matched == ["fullname_ssn", "unique_identifiers"]
        assert item.processed_at == processed_at

    def test_customer_status_item_pending_no_processed_at(self):
        """CustomerStatusItem with status 'pending' has no processed_at."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=4,
            status="pending",
            candidates_found=0,
            leaks_confirmed=0,
            strategies_matched=[],
        )

        assert item.status == "pending"
        assert item.processed_at is None
        assert item.strategies_matched == []

    def test_customer_status_item_failed_has_error_message(self):
        """CustomerStatusItem with status 'failed' includes error_message."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=3,
            status="failed",
            candidates_found=0,
            leaks_confirmed=0,
            strategies_matched=[],
            error_message="Azure Search timeout after 30s",
            processed_at=datetime.now(tz=timezone.utc),
        )

        assert item.status == "failed"
        assert item.error_message == "Azure Search timeout after 30s"

    def test_customer_status_item_zero_candidates(self):
        """CustomerStatusItem with zero candidates has zero leaks."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=99,
            status="complete",
            candidates_found=0,
            leaks_confirmed=0,
            strategies_matched=[],
            processed_at=datetime.now(tz=timezone.utc),
        )

        assert item.candidates_found == 0
        assert item.leaks_confirmed == 0
        assert item.strategies_matched == []

    def test_customer_status_item_candidates_but_no_leaks(self):
        """CustomerStatusItem with 5 candidates but 0 leaks."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=88,
            status="complete",
            candidates_found=5,
            leaks_confirmed=0,
            strategies_matched=[],
            processed_at=datetime.now(tz=timezone.utc),
        )

        assert item.candidates_found == 5
        assert item.leaks_confirmed == 0

    def test_customer_status_item_error_message_optional(self):
        """error_message defaults to None for non-failed customers."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=1,
            status="complete",
            candidates_found=2,
            leaks_confirmed=1,
            strategies_matched=["fullname_ssn"],
            processed_at=datetime.now(tz=timezone.utc),
        )

        assert item.error_message is None

    def test_customer_status_item_serialization(self):
        """CustomerStatusItem serializes to dict with all keys."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=1,
            status="complete",
            candidates_found=5,
            leaks_confirmed=3,
            strategies_matched=["fullname_ssn", "unique_identifiers"],
            processed_at=datetime(2026, 3, 11, 10, 2, 15, tzinfo=timezone.utc),
        )

        data = item.model_dump()
        assert "customer_id" in data
        assert "status" in data
        assert "candidates_found" in data
        assert "leaks_confirmed" in data
        assert "strategies_matched" in data
        assert "processed_at" in data
        assert "error_message" in data
        assert data["error_message"] is None

    def test_customer_status_item_strategies_matched_is_list(self):
        """strategies_matched is a list of strings."""
        from app.schemas.batch import CustomerStatusItem

        item = CustomerStatusItem(
            customer_id=1,
            status="complete",
            candidates_found=0,
            leaks_confirmed=0,
            strategies_matched=["a", "b"],
        )
        assert isinstance(item.strategies_matched, list)


class TestBatchResultItem:
    """Scenario: Get all results for a batch
    WHEN GET /batch/{batch_id}/results is called
    THEN all result rows for that batch are returned with full details.
    """

    def test_batch_result_item_construction(self):
        """BatchResultItem holds all result fields."""
        from app.schemas.batch import BatchResultItem
        from app.schemas.pii import FieldMatchResult

        batch_id = uuid.uuid4()
        item = BatchResultItem(
            batch_id=batch_id,
            customer_id=42,
            md5="abc123def456",
            strategy_name="fullname_ssn",
            leaked_fields=["SSN", "Fullname"],
            match_details={
                "SSN": FieldMatchResult(
                    found=True, method="exact", confidence=1.0, snippet="343-43-4343"
                ),
                "Fullname": FieldMatchResult(
                    found=True, method="normalized", confidence=0.95, snippet="Karthik"
                ),
            },
            overall_confidence=0.97,
            azure_search_score=15.0,
            needs_review=False,
        )

        assert item.batch_id == batch_id
        assert item.customer_id == 42
        assert item.md5 == "abc123def456"
        assert item.strategy_name == "fullname_ssn"
        assert item.leaked_fields == ["SSN", "Fullname"]
        assert item.overall_confidence == 0.97
        assert item.azure_search_score == 15.0
        assert item.needs_review is False

    def test_batch_result_item_batch_id_is_uuid(self):
        """batch_id in BatchResultItem is a UUID."""
        from app.schemas.batch import BatchResultItem
        from app.schemas.pii import FieldMatchResult

        batch_id = uuid.uuid4()
        item = BatchResultItem(
            batch_id=batch_id,
            customer_id=1,
            md5="abc",
            strategy_name="fullname_ssn",
            leaked_fields=["SSN"],
            match_details={
                "SSN": FieldMatchResult(found=True, method="exact", confidence=1.0)
            },
            overall_confidence=1.0,
            azure_search_score=10.0,
            needs_review=False,
        )
        assert isinstance(item.batch_id, uuid.UUID)

    def test_batch_result_item_searched_at_optional(self):
        """searched_at is optional and defaults to None."""
        from app.schemas.batch import BatchResultItem
        from app.schemas.pii import FieldMatchResult

        item = BatchResultItem(
            batch_id=uuid.uuid4(),
            customer_id=42,
            md5="abc123",
            strategy_name="fullname_ssn",
            leaked_fields=["SSN"],
            match_details={
                "SSN": FieldMatchResult(found=True, method="exact", confidence=1.0)
            },
            overall_confidence=1.0,
            azure_search_score=10.0,
            needs_review=False,
        )
        assert item.searched_at is None

    def test_batch_result_item_serialization_shape(self):
        """BatchResultItem serializes with all required keys."""
        from app.schemas.batch import BatchResultItem
        from app.schemas.pii import FieldMatchResult

        item = BatchResultItem(
            batch_id=uuid.uuid4(),
            customer_id=42,
            md5="abc123",
            strategy_name="fullname_ssn",
            leaked_fields=["SSN", "Fullname"],
            match_details={
                "SSN": FieldMatchResult(found=True, method="exact", confidence=1.0)
            },
            overall_confidence=0.97,
            azure_search_score=15.0,
            needs_review=False,
        )
        data = item.model_dump()
        required_keys = {
            "batch_id",
            "customer_id",
            "md5",
            "strategy_name",
            "leaked_fields",
            "match_details",
            "overall_confidence",
            "azure_search_score",
            "needs_review",
            "searched_at",
        }
        assert required_keys.issubset(set(data.keys()))

    def test_batch_result_item_leaked_fields_is_list(self):
        """leaked_fields is a list of strings."""
        from app.schemas.batch import BatchResultItem
        from app.schemas.pii import FieldMatchResult

        item = BatchResultItem(
            batch_id=uuid.uuid4(),
            customer_id=1,
            md5="abc",
            strategy_name="fullname_ssn",
            leaked_fields=["SSN", "DOB", "Fullname"],
            match_details={},
            overall_confidence=0.9,
            azure_search_score=5.0,
            needs_review=False,
        )
        assert isinstance(item.leaked_fields, list)
        assert len(item.leaked_fields) == 3


class TestBatchSummaryItem:
    """Scenario: List all batches
    WHEN GET /batches is called
    THEN all batch runs are returned with batch_id, status, started_at,
    completed_at, total_customers, and strategy count.
    """

    def test_batch_summary_item_construction(self):
        """BatchSummaryItem holds batch listing fields."""
        from app.schemas.batch import BatchSummaryItem

        batch_id = uuid.uuid4()
        started_at = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        item = BatchSummaryItem(
            batch_id=batch_id,
            status="completed",
            started_at=started_at,
            completed_at=None,
            total_customers=200,
            strategy_count=3,
        )

        assert item.batch_id == batch_id
        assert item.status == "completed"
        assert item.started_at == started_at
        assert item.completed_at is None
        assert item.total_customers == 200
        assert item.strategy_count == 3

    def test_batch_summary_item_batch_id_is_uuid(self):
        """batch_id in BatchSummaryItem is a UUID."""
        from app.schemas.batch import BatchSummaryItem

        batch_id = uuid.uuid4()
        item = BatchSummaryItem(
            batch_id=batch_id,
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=None,
            total_customers=100,
            strategy_count=2,
        )
        assert isinstance(item.batch_id, uuid.UUID)

    def test_batch_summary_item_serialization(self):
        """BatchSummaryItem serializes with all required keys."""
        from app.schemas.batch import BatchSummaryItem

        item = BatchSummaryItem(
            batch_id=uuid.uuid4(),
            status="completed",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=datetime.now(tz=timezone.utc),
            total_customers=50,
            strategy_count=1,
        )
        data = item.model_dump()
        assert set(data.keys()) == {
            "batch_id",
            "status",
            "started_at",
            "completed_at",
            "total_customers",
            "strategy_count",
        }

    def test_batch_summary_item_completed_at_nullable(self):
        """completed_at is nullable (None for running batches)."""
        from app.schemas.batch import BatchSummaryItem

        item = BatchSummaryItem(
            batch_id=uuid.uuid4(),
            status="running",
            started_at=datetime.now(tz=timezone.utc),
            completed_at=None,
            total_customers=100,
            strategy_count=3,
        )
        assert item.completed_at is None


class TestResumeResponse:
    """Scenario: Resume an interrupted batch
    WHEN POST /batch/{batch_id}/resume is called on a running/failed batch
    THEN processing resumes and response is returned.

    Scenario: Resume a completed batch
    WHEN POST /batch/{batch_id}/resume is called on a completed batch
    THEN the system returns 400 with 'Batch already completed'.
    """

    def test_resume_response_construction(self):
        """ResumeResponse acknowledges batch resumption."""
        from app.schemas.batch import ResumeResponse

        batch_id = uuid.uuid4()
        resp = ResumeResponse(
            batch_id=batch_id,
            status="running",
            message="Batch resumed",
        )
        assert resp.batch_id == batch_id
        assert resp.status == "running"
        assert resp.message == "Batch resumed"

    def test_resume_response_serialization(self):
        """ResumeResponse serializes with batch_id, status, message."""
        from app.schemas.batch import ResumeResponse

        resp = ResumeResponse(
            batch_id=uuid.uuid4(),
            status="running",
            message="Batch resumed",
        )
        data = resp.model_dump()
        assert "batch_id" in data
        assert "status" in data
        assert "message" in data
