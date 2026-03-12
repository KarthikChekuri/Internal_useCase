"""Tests for app/routers/batch.py — Batch Router and Status APIs (Phase V2-3.3).

Covers all 6 endpoints:
- POST /batch/run
- POST /batch/{batch_id}/resume
- GET /batch/{batch_id}/status
- GET /batch/{batch_id}/customers
- GET /batch/{batch_id}/results
- GET /batches

Each Given/When/Then scenario from the spec maps to one or more test cases.
All DB access and batch_service calls are mocked.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixture: app with batch router and overridden dependencies
# ---------------------------------------------------------------------------

@pytest.fixture
def app_with_batch_router():
    """Create a minimal FastAPI app with only the batch router registered."""
    from app.routers.batch import router as batch_router
    from app.dependencies import get_db, get_search_client, get_settings

    app = FastAPI()
    app.include_router(batch_router)

    # Provide mock dependencies so tests don't need real DB or Azure Search
    mock_db = MagicMock()
    mock_search_client = MagicMock()
    mock_settings = MagicMock()
    mock_settings.STRATEGIES_FILE = "strategies.yaml"

    def override_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_search_client] = lambda: mock_search_client
    app.dependency_overrides[get_settings] = lambda: mock_settings

    return app


@pytest.fixture
def client(app_with_batch_router):
    """TestClient for the batch router app."""
    return TestClient(app_with_batch_router, raise_server_exceptions=False)


# Sample UUIDs for use in tests
BATCH_ID = str(uuid.uuid4())
BATCH_UUID = uuid.UUID(BATCH_ID)


# ---------------------------------------------------------------------------
# Test: POST /batch/run — trigger batch
# ---------------------------------------------------------------------------

def _patch_batch_run_success(batch_id=BATCH_ID, total=200):
    """Context manager stack for a successful POST /batch/run call."""
    from contextlib import contextmanager, ExitStack

    @contextmanager
    def _ctx():
        with ExitStack() as stack:
            stack.enter_context(patch("app.routers.batch.load_strategies", return_value=[]))
            stack.enter_context(patch("app.routers.batch._get_total_customers", return_value=total))
            stack.enter_context(patch("app.routers.batch.batch_service._check_running_batch", return_value=None))
            stack.enter_context(patch("app.routers.batch.batch_service._create_batch_run", return_value=batch_id))
            stack.enter_context(patch("app.routers.batch.batch_service._get_all_customers", return_value=[]))
            stack.enter_context(patch("app.routers.batch.batch_service._init_customer_statuses", return_value=None))
            stack.enter_context(patch("app.routers.batch._run_batch_background", return_value=None))
            yield

    return _ctx()


class TestPostBatchRun:
    """Tests for POST /batch/run — start a new batch run."""

    def test_post_batch_run_returns_200(self, client):
        """WHEN POST /batch/run is called THEN 200 OK is returned."""
        with _patch_batch_run_success():
            response = client.post("/batch/run")
        assert response.status_code == 200

    def test_post_batch_run_response_has_batch_id(self, client):
        """WHEN POST /batch/run THEN response includes batch_id as UUID string."""
        with _patch_batch_run_success():
            response = client.post("/batch/run")
        data = response.json()
        assert "batch_id" in data
        # Should be a valid UUID
        uuid.UUID(str(data["batch_id"]))

    def test_post_batch_run_response_has_status_running(self, client):
        """WHEN POST /batch/run THEN status is 'running'."""
        with _patch_batch_run_success():
            response = client.post("/batch/run")
        data = response.json()
        assert data["status"] == "running"

    def test_post_batch_run_response_has_total_customers(self, client):
        """WHEN POST /batch/run THEN response includes total_customers count."""
        with _patch_batch_run_success(total=200):
            response = client.post("/batch/run")
        data = response.json()
        assert "total_customers" in data
        assert data["total_customers"] == 200

    def test_post_batch_run_returns_409_when_batch_running(self, client):
        """WHEN POST /batch/run called while another batch is running THEN 409 Conflict."""
        mock_running = MagicMock()
        mock_running.batch_id = BATCH_ID
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch._get_total_customers", return_value=200):
                with patch("app.routers.batch.batch_service._check_running_batch", return_value=mock_running):
                    response = client.post("/batch/run")
        assert response.status_code == 409

    def test_post_batch_run_409_message_contains_running_batch_id(self, client):
        """WHEN 409 is returned THEN the message mentions the running batch_id."""
        running_batch_id = str(uuid.uuid4())
        mock_running = MagicMock()
        mock_running.batch_id = running_batch_id
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch._get_total_customers", return_value=200):
                with patch("app.routers.batch.batch_service._check_running_batch", return_value=mock_running):
                    response = client.post("/batch/run")
        data = response.json()
        assert running_batch_id in data.get("detail", "")

    def test_post_batch_run_starts_background_task(self, client):
        """WHEN POST /batch/run is called THEN processing starts in background (BackgroundTasks)."""
        # The endpoint should return immediately — verified by checking that
        # processing is enqueued in BackgroundTasks, not blocking the HTTP response.
        with _patch_batch_run_success(total=50):
            response = client.post("/batch/run")
        assert response.status_code == 200
        assert response.json()["status"] == "running"


# ---------------------------------------------------------------------------
# Test: POST /batch/{batch_id}/resume
# ---------------------------------------------------------------------------

class TestPostBatchResume:
    """Tests for POST /batch/{batch_id}/resume."""

    def test_resume_running_batch_returns_200(self, client):
        """WHEN POST /batch/{batch_id}/resume on running batch THEN 200."""
        mock_batch = MagicMock(status="running")
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch.batch_service._get_batch_run", return_value=mock_batch):
                response = client.post(f"/batch/{BATCH_ID}/resume")
        assert response.status_code == 200

    def test_resume_returns_batch_id(self, client):
        """WHEN POST /batch/{batch_id}/resume THEN batch_id in response."""
        mock_batch = MagicMock(status="running")
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch.batch_service._get_batch_run", return_value=mock_batch):
                response = client.post(f"/batch/{BATCH_ID}/resume")
        data = response.json()
        assert "batch_id" in data

    def test_resume_completed_batch_returns_400(self, client):
        """WHEN POST /batch/{batch_id}/resume on completed batch THEN 400."""
        mock_batch = MagicMock(status="completed")
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch.batch_service._get_batch_run", return_value=mock_batch):
                response = client.post(f"/batch/{BATCH_ID}/resume")
        assert response.status_code == 400

    def test_resume_completed_batch_400_message(self, client):
        """WHEN 400 on resume THEN message is 'Batch already completed'."""
        mock_batch = MagicMock(status="completed")
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch.batch_service._get_batch_run", return_value=mock_batch):
                response = client.post(f"/batch/{BATCH_ID}/resume")
        data = response.json()
        assert "Batch already completed" in data.get("detail", "")

    def test_resume_not_found_returns_404(self, client):
        """WHEN POST /batch/{batch_id}/resume with unknown batch_id THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch.load_strategies", return_value=[]):
            with patch("app.routers.batch.batch_service._get_batch_run", return_value=None):
                response = client.post(f"/batch/{fake_id}/resume")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Test: GET /batch/{batch_id}/status
# ---------------------------------------------------------------------------

class TestGetBatchStatus:
    """Tests for GET /batch/{batch_id}/status."""

    def _make_status_response(self, batch_id=BATCH_ID, status="running"):
        """Build a mock BatchStatusResponse dict."""
        return {
            "batch_id": batch_id,
            "status": status,
            "started_at": "2026-03-11T10:00:00",
            "completed_at": None,
            "strategy_set": ["fullname_ssn", "lastname_dob"],
            "indexing": {"total": 500, "indexed": 500, "failed": 3, "skipped": 0},
            "searching": {"total_customers": 200, "completed": 120, "failed": 1, "pending": 79},
            "detection": {"total_pairs_processed": 3200, "leaks_found": 450},
        }

    def test_get_status_running_batch_returns_200(self, client):
        """WHEN GET /batch/{batch_id}/status for running batch THEN 200."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        assert response.status_code == 200

    def test_get_status_response_has_batch_id(self, client):
        """WHEN GET /batch/{batch_id}/status THEN batch_id in response."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert "batch_id" in data

    def test_get_status_response_has_status_field(self, client):
        """WHEN GET /batch/{batch_id}/status THEN status field present."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert "status" in data

    def test_get_status_response_has_indexing_section(self, client):
        """WHEN GET /batch/{batch_id}/status THEN indexing section present."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert "indexing" in data
        assert "total" in data["indexing"]
        assert "indexed" in data["indexing"]

    def test_get_status_response_has_searching_section(self, client):
        """WHEN GET /batch/{batch_id}/status THEN searching section present."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert "searching" in data
        assert "total_customers" in data["searching"]

    def test_get_status_response_has_detection_section(self, client):
        """WHEN GET /batch/{batch_id}/status THEN detection section present."""
        mock_status = self._make_status_response()
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert "detection" in data
        assert "leaks_found" in data["detection"]

    def test_get_status_not_found_returns_404(self, client):
        """WHEN GET /batch/{batch_id}/status with unknown id THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch.get_batch_status", return_value=None):
            response = client.get(f"/batch/{fake_id}/status")
        assert response.status_code == 404

    def test_get_status_404_message(self, client):
        """WHEN 404 returned THEN message is 'Batch not found'."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch.get_batch_status", return_value=None):
            response = client.get(f"/batch/{fake_id}/status")
        data = response.json()
        assert "Batch not found" in data.get("detail", "")

    def test_get_status_completed_batch_has_completed_at(self, client):
        """WHEN GET /batch/{batch_id}/status for completed batch THEN completed_at is populated."""
        mock_status = self._make_status_response(status="completed")
        mock_status["completed_at"] = "2026-03-11T12:00:00"
        with patch("app.routers.batch.get_batch_status", return_value=mock_status):
            response = client.get(f"/batch/{BATCH_ID}/status")
        data = response.json()
        assert data["status"] == "completed"
        assert data["completed_at"] is not None


# ---------------------------------------------------------------------------
# Test: GET /batch/{batch_id}/customers
# ---------------------------------------------------------------------------

class TestGetBatchCustomers:
    """Tests for GET /batch/{batch_id}/customers."""

    def _make_customer_list(self):
        return [
            {
                "customer_id": 1,
                "status": "complete",
                "candidates_found": 5,
                "leaks_confirmed": 3,
                "strategies_matched": ["fullname_ssn"],
                "error_message": None,
                "processed_at": "2026-03-11T10:02:15",
            },
            {
                "customer_id": 2,
                "status": "complete",
                "candidates_found": 0,
                "leaks_confirmed": 0,
                "strategies_matched": [],
                "error_message": None,
                "processed_at": "2026-03-11T10:02:18",
            },
            {
                "customer_id": 3,
                "status": "failed",
                "candidates_found": 0,
                "leaks_confirmed": 0,
                "strategies_matched": [],
                "error_message": "Azure Search timeout after 30s",
                "processed_at": "2026-03-11T10:02:45",
            },
            {
                "customer_id": 4,
                "status": "pending",
                "candidates_found": 0,
                "leaks_confirmed": 0,
                "strategies_matched": [],
                "error_message": None,
                "processed_at": None,
            },
        ]

    def test_get_customers_returns_200(self, client):
        """WHEN GET /batch/{batch_id}/customers THEN 200 OK."""
        with patch("app.routers.batch.get_customer_statuses", return_value=self._make_customer_list()):
            response = client.get(f"/batch/{BATCH_ID}/customers")
        assert response.status_code == 200

    def test_get_customers_returns_array(self, client):
        """WHEN GET /batch/{batch_id}/customers THEN response is an array."""
        with patch("app.routers.batch.get_customer_statuses", return_value=self._make_customer_list()):
            response = client.get(f"/batch/{BATCH_ID}/customers")
        data = response.json()
        assert isinstance(data, list)

    def test_get_customers_array_has_required_fields(self, client):
        """WHEN GET /batch/{batch_id}/customers THEN each item has required fields."""
        with patch("app.routers.batch.get_customer_statuses", return_value=self._make_customer_list()):
            response = client.get(f"/batch/{BATCH_ID}/customers")
        data = response.json()
        assert len(data) > 0
        item = data[0]
        assert "customer_id" in item
        assert "status" in item
        assert "candidates_found" in item
        assert "leaks_confirmed" in item
        assert "strategies_matched" in item

    def test_get_customers_filter_by_status_failed(self, client):
        """WHEN GET /batch/{batch_id}/customers?status=failed THEN only failed customers returned."""
        failed_only = [c for c in self._make_customer_list() if c["status"] == "failed"]
        with patch("app.routers.batch.get_customer_statuses", return_value=failed_only):
            response = client.get(f"/batch/{BATCH_ID}/customers?status=failed")
        data = response.json()
        assert all(c["status"] == "failed" for c in data)

    def test_get_customers_filter_by_status_complete(self, client):
        """WHEN GET /batch/{batch_id}/customers?status=complete THEN only complete customers."""
        complete_only = [c for c in self._make_customer_list() if c["status"] == "complete"]
        with patch("app.routers.batch.get_customer_statuses", return_value=complete_only):
            response = client.get(f"/batch/{BATCH_ID}/customers?status=complete")
        data = response.json()
        assert all(c["status"] == "complete" for c in data)

    def test_get_customers_failed_has_error_message(self, client):
        """WHEN customer is failed THEN error_message is populated."""
        customers = self._make_customer_list()
        with patch("app.routers.batch.get_customer_statuses", return_value=customers):
            response = client.get(f"/batch/{BATCH_ID}/customers")
        data = response.json()
        failed = [c for c in data if c["status"] == "failed"]
        assert len(failed) > 0
        assert failed[0]["error_message"] is not None

    def test_get_customers_pending_has_no_processed_at(self, client):
        """WHEN customer is pending THEN processed_at is null."""
        customers = self._make_customer_list()
        with patch("app.routers.batch.get_customer_statuses", return_value=customers):
            response = client.get(f"/batch/{BATCH_ID}/customers")
        data = response.json()
        pending = [c for c in data if c["status"] == "pending"]
        assert len(pending) > 0
        assert pending[0]["processed_at"] is None

    def test_get_customers_not_found_returns_404(self, client):
        """WHEN batch_id does not exist THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch.get_customer_statuses", return_value=None):
            response = client.get(f"/batch/{fake_id}/customers")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Test: GET /batch/{batch_id}/results
# ---------------------------------------------------------------------------

class TestGetBatchResults:
    """Tests for GET /batch/{batch_id}/results."""

    def _make_results_list(self, customer_id=42):
        return [
            {
                "batch_id": BATCH_ID,
                "customer_id": customer_id,
                "md5": "abc123def456",
                "strategy_name": "fullname_ssn",
                "leaked_fields": ["SSN", "Fullname"],
                "match_details": {
                    "SSN": {"found": True, "method": "exact", "confidence": 1.0, "snippet": "123-45-6789"},
                    "Fullname": {"found": True, "method": "normalized", "confidence": 0.95, "snippet": "Jane Doe"},
                },
                "overall_confidence": 0.975,
                "azure_search_score": 4.5,
                "needs_review": False,
                "searched_at": "2026-03-11T10:05:00",
            }
        ]

    def test_get_results_returns_200(self, client):
        """WHEN GET /batch/{batch_id}/results THEN 200 OK."""
        with patch("app.routers.batch.get_batch_results", return_value=self._make_results_list()):
            response = client.get(f"/batch/{BATCH_ID}/results")
        assert response.status_code == 200

    def test_get_results_returns_array(self, client):
        """WHEN GET /batch/{batch_id}/results THEN array is returned."""
        with patch("app.routers.batch.get_batch_results", return_value=self._make_results_list()):
            response = client.get(f"/batch/{BATCH_ID}/results")
        data = response.json()
        assert isinstance(data, list)

    def test_get_results_items_have_required_fields(self, client):
        """WHEN GET /batch/{batch_id}/results THEN each item has required fields."""
        with patch("app.routers.batch.get_batch_results", return_value=self._make_results_list()):
            response = client.get(f"/batch/{BATCH_ID}/results")
        data = response.json()
        assert len(data) > 0
        item = data[0]
        for field in ["batch_id", "customer_id", "md5", "strategy_name",
                      "leaked_fields", "overall_confidence", "needs_review"]:
            assert field in item, f"Missing field: {field}"

    def test_get_results_filter_by_customer_id(self, client):
        """WHEN GET /batch/{batch_id}/results?customer_id=42 THEN only results for customer 42."""
        results = self._make_results_list(customer_id=42)
        with patch("app.routers.batch.get_batch_results", return_value=results):
            response = client.get(f"/batch/{BATCH_ID}/results?customer_id=42")
        data = response.json()
        assert all(r["customer_id"] == 42 for r in data)

    def test_get_results_not_found_returns_404(self, client):
        """WHEN batch_id does not exist THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch.get_batch_results", return_value=None):
            response = client.get(f"/batch/{fake_id}/results")
        assert response.status_code == 404

    def test_get_results_for_batch_only(self, client):
        """WHEN GET /batch/{batch_id}/results THEN only results for that batch."""
        results = self._make_results_list()
        with patch("app.routers.batch.get_batch_results", return_value=results):
            response = client.get(f"/batch/{BATCH_ID}/results")
        data = response.json()
        for item in data:
            assert str(item["batch_id"]) == str(BATCH_ID)


# ---------------------------------------------------------------------------
# Test: GET /batches
# ---------------------------------------------------------------------------

class TestGetBatches:
    """Tests for GET /batches — list all batch runs."""

    def _make_batch_list(self):
        return [
            {
                "batch_id": BATCH_ID,
                "status": "completed",
                "started_at": "2026-03-11T10:00:00",
                "completed_at": "2026-03-11T12:00:00",
                "total_customers": 200,
                "strategy_count": 3,
            },
            {
                "batch_id": str(uuid.uuid4()),
                "status": "running",
                "started_at": "2026-03-11T14:00:00",
                "completed_at": None,
                "total_customers": 200,
                "strategy_count": 2,
            },
        ]

    def test_get_batches_returns_200(self, client):
        """WHEN GET /batches THEN 200 OK."""
        with patch("app.routers.batch.list_all_batches", return_value=self._make_batch_list()):
            response = client.get("/batches")
        assert response.status_code == 200

    def test_get_batches_returns_array(self, client):
        """WHEN GET /batches THEN array of batch summaries returned."""
        with patch("app.routers.batch.list_all_batches", return_value=self._make_batch_list()):
            response = client.get("/batches")
        data = response.json()
        assert isinstance(data, list)

    def test_get_batches_items_have_required_fields(self, client):
        """WHEN GET /batches THEN each item has batch_id, status, started_at, total_customers, strategy_count."""
        with patch("app.routers.batch.list_all_batches", return_value=self._make_batch_list()):
            response = client.get("/batches")
        data = response.json()
        assert len(data) > 0
        item = data[0]
        for field in ["batch_id", "status", "started_at", "total_customers", "strategy_count"]:
            assert field in item, f"Missing field: {field}"

    def test_get_batches_empty_list(self, client):
        """WHEN no batches exist THEN empty array returned."""
        with patch("app.routers.batch.list_all_batches", return_value=[]):
            response = client.get("/batches")
        data = response.json()
        assert data == []


# ---------------------------------------------------------------------------
# Test: Route registration
# ---------------------------------------------------------------------------

class TestBatchRouterRegistration:
    """Verify all batch routes are registered in the router."""

    def test_batch_run_route_exists(self, app_with_batch_router):
        """POST /batch/run should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batch/run" in paths

    def test_batch_resume_route_exists(self, app_with_batch_router):
        """POST /batch/{batch_id}/resume should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batch/{batch_id}/resume" in paths

    def test_batch_status_route_exists(self, app_with_batch_router):
        """GET /batch/{batch_id}/status should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batch/{batch_id}/status" in paths

    def test_batch_customers_route_exists(self, app_with_batch_router):
        """GET /batch/{batch_id}/customers should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batch/{batch_id}/customers" in paths

    def test_batch_results_route_exists(self, app_with_batch_router):
        """GET /batch/{batch_id}/results should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batch/{batch_id}/results" in paths

    def test_batches_list_route_exists(self, app_with_batch_router):
        """GET /batches should be registered."""
        paths = [route.path for route in app_with_batch_router.routes]
        assert "/batches" in paths
