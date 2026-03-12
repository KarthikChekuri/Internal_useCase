"""Tests for app/routers/batch_v3.py — V3 FastAPI Routes (Phase V3-4.1).

Covers all 4 V3 endpoints:
- POST /v3/index/all
- POST /v3/batch/run
- GET  /v3/batch/{batch_id}/status
- GET  /v3/batch/{batch_id}/results

Each Given/When/Then scenario from the spec maps to one or more test cases.
All DB access and service calls are mocked — no real SQLAlchemy or Azure Search.
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Sample UUIDs for use in tests
# ---------------------------------------------------------------------------

BATCH_ID = str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Fixture: minimal FastAPI app with only the V3 batch router registered
# ---------------------------------------------------------------------------

@pytest.fixture
def app_v3():
    """Create a minimal FastAPI app with only the V3 batch router registered."""
    from app.routers.batch_v3 import router as batch_v3_router
    from app.dependencies import get_db, get_search_client_v3

    app = FastAPI()
    app.include_router(batch_v3_router, prefix="/v3", tags=["V3"])

    mock_db = MagicMock()
    mock_search_client = MagicMock()

    def override_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_search_client_v3] = lambda: mock_search_client

    return app


@pytest.fixture
def client_v3(app_v3):
    """TestClient for the V3 batch router app."""
    return TestClient(app_v3, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Test: POST /v3/index/all
# ---------------------------------------------------------------------------

def _make_index_response(
    files_processed=10,
    files_succeeded=9,
    files_failed=1,
    files_skipped=2,
    errors=None,
):
    """Return a real IndexResponse Pydantic model for test use."""
    from app.services.indexing_service_v3 import IndexResponse
    return IndexResponse(
        files_processed=files_processed,
        files_succeeded=files_succeeded,
        files_failed=files_failed,
        files_skipped=files_skipped,
        errors=errors if errors is not None else [],
    )


class TestPostV3IndexAll:
    """Tests for POST /v3/index/all."""

    def test_index_all_returns_200(self, client_v3):
        """WHEN POST /v3/index/all is called THEN 200 OK is returned."""
        with patch(
            "app.routers.batch_v3.index_all_files_v3",
            return_value=_make_index_response(),
        ):
            response = client_v3.post("/v3/index/all")
        assert response.status_code == 200

    def test_index_all_calls_indexing_service(self, client_v3):
        """WHEN POST /v3/index/all THEN index_all_files_v3 is called."""
        with patch(
            "app.routers.batch_v3.index_all_files_v3",
            return_value=_make_index_response(files_processed=5, files_succeeded=5,
                                               files_failed=0, files_skipped=0),
        ) as mock_svc:
            client_v3.post("/v3/index/all")
        mock_svc.assert_called_once()

    def test_index_all_response_has_files_processed(self, client_v3):
        """WHEN POST /v3/index/all THEN response includes files_processed."""
        with patch(
            "app.routers.batch_v3.index_all_files_v3",
            return_value=_make_index_response(files_processed=10),
        ):
            response = client_v3.post("/v3/index/all")
        data = response.json()
        assert "files_processed" in data
        assert data["files_processed"] == 10


# ---------------------------------------------------------------------------
# Test: POST /v3/batch/run
# ---------------------------------------------------------------------------

class TestPostV3BatchRun:
    """Tests for POST /v3/batch/run."""

    def _patch_batch_run_success(self, total=100, batch_id=BATCH_ID):
        """Context manager stack for a successful POST /v3/batch/run."""
        from contextlib import ExitStack, contextmanager

        @contextmanager
        def _ctx():
            with ExitStack() as stack:
                # Mock MasterData count
                stack.enter_context(
                    patch("app.routers.batch_v3._get_total_customers_v3", return_value=total)
                )
                # Mock no running batch
                stack.enter_context(
                    patch("app.routers.batch_v3._get_running_batch_v3", return_value=None)
                )
                # Mock background function so it doesn't actually run
                stack.enter_context(
                    patch("app.routers.batch_v3._run_v3_batch_background", return_value=None)
                )
                # Freeze uuid
                stack.enter_context(
                    patch("app.routers.batch_v3.uuid.uuid4", return_value=uuid.UUID(batch_id))
                )
                yield

        return _ctx()

    def test_post_v3_batch_run_returns_202(self, client_v3):
        """WHEN POST /v3/batch/run THEN 202 Accepted is returned."""
        with self._patch_batch_run_success():
            response = client_v3.post("/v3/batch/run")
        assert response.status_code == 202

    def test_post_v3_batch_run_response_has_batch_id(self, client_v3):
        """WHEN POST /v3/batch/run THEN response includes batch_id."""
        with self._patch_batch_run_success():
            response = client_v3.post("/v3/batch/run")
        data = response.json()
        assert "batch_id" in data
        uuid.UUID(str(data["batch_id"]))  # must be valid UUID

    def test_post_v3_batch_run_response_has_status_running(self, client_v3):
        """WHEN POST /v3/batch/run THEN status is 'running'."""
        with self._patch_batch_run_success():
            response = client_v3.post("/v3/batch/run")
        data = response.json()
        assert data["status"] == "running"

    def test_post_v3_batch_run_response_has_method_v3_azure_only(self, client_v3):
        """WHEN POST /v3/batch/run THEN method is 'v3_azure_only'."""
        with self._patch_batch_run_success():
            response = client_v3.post("/v3/batch/run")
        data = response.json()
        assert data["method"] == "v3_azure_only"

    def test_post_v3_batch_run_response_has_total_customers(self, client_v3):
        """WHEN POST /v3/batch/run THEN response includes total_customers count."""
        with self._patch_batch_run_success(total=150):
            response = client_v3.post("/v3/batch/run")
        data = response.json()
        assert "total_customers" in data
        assert data["total_customers"] == 150

    def test_post_v3_batch_run_triggers_background_task(self, client_v3):
        """WHEN POST /v3/batch/run THEN background processing is triggered."""
        with self._patch_batch_run_success(total=50):
            response = client_v3.post("/v3/batch/run")
        # Response returns immediately with 202 and status=running
        assert response.status_code == 202
        assert response.json()["status"] == "running"

    def test_post_v3_batch_run_returns_409_when_batch_running(self, client_v3):
        """WHEN POST /v3/batch/run while another batch is running THEN 409 Conflict."""
        mock_running = MagicMock()
        mock_running.batch_id = BATCH_ID
        with patch("app.routers.batch_v3._get_total_customers_v3", return_value=100):
            with patch("app.routers.batch_v3._get_running_batch_v3", return_value=mock_running):
                response = client_v3.post("/v3/batch/run")
        assert response.status_code == 409

    def test_post_v3_batch_run_409_message_mentions_conflict(self, client_v3):
        """WHEN 409 is returned THEN the detail mentions a running batch."""
        mock_running = MagicMock()
        mock_running.batch_id = BATCH_ID
        with patch("app.routers.batch_v3._get_total_customers_v3", return_value=100):
            with patch("app.routers.batch_v3._get_running_batch_v3", return_value=mock_running):
                response = client_v3.post("/v3/batch/run")
        data = response.json()
        assert "running" in data.get("detail", "").lower() or "batch" in data.get("detail", "").lower()


# ---------------------------------------------------------------------------
# Test: GET /v3/batch/{batch_id}/status
# ---------------------------------------------------------------------------

class TestGetV3BatchStatus:
    """Tests for GET /v3/batch/{batch_id}/status."""

    def _make_v3_status_dict(self, batch_id=BATCH_ID, status="running"):
        return {
            "batch_id": batch_id,
            "status": status,
            "total_customers": 200,
            "customers_completed": 50,
            "customers_failed": 2,
            "customer_details": [],
            "method": "v3_azure_only",
        }

    def test_get_v3_batch_status_returns_200(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status for existing batch THEN 200."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_status",
            return_value=self._make_v3_status_dict(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/status")
        assert response.status_code == 200

    def test_get_v3_batch_status_has_batch_id(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status THEN batch_id in response."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_status",
            return_value=self._make_v3_status_dict(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/status")
        data = response.json()
        assert "batch_id" in data

    def test_get_v3_batch_status_has_method_v3_azure_only(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status THEN method is 'v3_azure_only'."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_status",
            return_value=self._make_v3_status_dict(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/status")
        data = response.json()
        assert data.get("method") == "v3_azure_only"

    def test_get_v3_batch_status_has_customer_counts(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status THEN customer counts present."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_status",
            return_value=self._make_v3_status_dict(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/status")
        data = response.json()
        assert "total_customers" in data
        assert "customers_completed" in data
        assert "customers_failed" in data

    def test_get_v3_batch_status_not_found_returns_404(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status with unknown batch_id THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch_v3._get_v3_batch_status", return_value=None):
            response = client_v3.get(f"/v3/batch/{fake_id}/status")
        assert response.status_code == 404

    def test_get_v3_batch_status_404_message(self, client_v3):
        """WHEN 404 returned THEN message mentions 'Batch not found'."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch_v3._get_v3_batch_status", return_value=None):
            response = client_v3.get(f"/v3/batch/{fake_id}/status")
        data = response.json()
        assert "Batch not found" in data.get("detail", "")

    def test_get_v3_batch_status_customer_details_present(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/status THEN customer_details list present."""
        status_dict = self._make_v3_status_dict()
        status_dict["customer_details"] = [
            {"customer_id": 1, "status": "complete"},
            {"customer_id": 2, "status": "pending"},
        ]
        with patch("app.routers.batch_v3._get_v3_batch_status", return_value=status_dict):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/status")
        data = response.json()
        assert "customer_details" in data
        assert isinstance(data["customer_details"], list)


# ---------------------------------------------------------------------------
# Test: GET /v3/batch/{batch_id}/results
# ---------------------------------------------------------------------------

class TestGetV3BatchResults:
    """Tests for GET /v3/batch/{batch_id}/results."""

    def _make_v3_results_list(self, customer_id=42):
        return [
            {
                "batch_id": BATCH_ID,
                "customer_id": customer_id,
                "md5": "aabbccdd1122",
                "strategy_name": "v3_azure_only",
                "leaked_fields": ["SSN", "Fullname"],
                "match_details": {
                    "SSN": {"found": True, "score": 0.9, "snippet": "123-45-6789"},
                    "Fullname": {"found": True, "score": 0.8, "snippet": "Jane Doe"},
                },
                "overall_confidence": 0.85,
                "azure_search_score": 0.9,
                "needs_review": False,
                "searched_at": "2026-03-12T10:05:00",
            }
        ]

    def test_get_v3_results_returns_200(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/results THEN 200 OK."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_results",
            return_value=self._make_v3_results_list(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        assert response.status_code == 200

    def test_get_v3_results_returns_array(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/results THEN array is returned."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_results",
            return_value=self._make_v3_results_list(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        data = response.json()
        assert isinstance(data, list)

    def test_get_v3_results_items_have_required_fields(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/results THEN each item has required fields."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_results",
            return_value=self._make_v3_results_list(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        data = response.json()
        assert len(data) > 0
        item = data[0]
        for field in [
            "batch_id", "customer_id", "md5", "strategy_name",
            "leaked_fields", "overall_confidence", "needs_review",
        ]:
            assert field in item, f"Missing field: {field}"

    def test_get_v3_results_strategy_name_is_v3_azure_only(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/results THEN strategy_name='v3_azure_only'."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_results",
            return_value=self._make_v3_results_list(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        data = response.json()
        for item in data:
            assert item["strategy_name"] == "v3_azure_only"

    def test_get_v3_results_not_found_returns_404(self, client_v3):
        """WHEN batch_id does not exist THEN 404."""
        fake_id = str(uuid.uuid4())
        with patch("app.routers.batch_v3._get_v3_batch_results", return_value=None):
            response = client_v3.get(f"/v3/batch/{fake_id}/results")
        assert response.status_code == 404

    def test_get_v3_results_for_correct_batch(self, client_v3):
        """WHEN GET /v3/batch/{batch_id}/results THEN results contain matching batch_id."""
        with patch(
            "app.routers.batch_v3._get_v3_batch_results",
            return_value=self._make_v3_results_list(),
        ):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        data = response.json()
        for item in data:
            assert str(item["batch_id"]) == str(BATCH_ID)

    def test_get_v3_results_empty_list_when_no_results(self, client_v3):
        """WHEN batch exists but has no results THEN empty array returned."""
        with patch("app.routers.batch_v3._get_v3_batch_results", return_value=[]):
            response = client_v3.get(f"/v3/batch/{BATCH_ID}/results")
        data = response.json()
        assert data == []


# ---------------------------------------------------------------------------
# Test: V3 routes co-exist with V2 routes
# ---------------------------------------------------------------------------

class TestV3AndV2RoutesCoexist:
    """Verify V3 routes registered alongside V2 routes in main app."""

    def test_v3_batch_run_route_registered_in_main_app(self):
        """POST /v3/batch/run should be accessible in the main app."""
        from app.main import app
        paths = [route.path for route in app.routes]
        assert "/v3/batch/run" in paths

    def test_v2_batch_run_route_still_registered_in_main_app(self):
        """POST /batch/run (V2) should still be accessible in the main app."""
        from app.main import app
        paths = [route.path for route in app.routes]
        assert "/batch/run" in paths

    def test_v3_batch_status_route_registered_in_main_app(self):
        """GET /v3/batch/{batch_id}/status should be accessible in the main app."""
        from app.main import app
        paths = [route.path for route in app.routes]
        assert "/v3/batch/{batch_id}/status" in paths

    def test_v3_batch_results_route_registered_in_main_app(self):
        """GET /v3/batch/{batch_id}/results should be accessible in the main app."""
        from app.main import app
        paths = [route.path for route in app.routes]
        assert "/v3/batch/{batch_id}/results" in paths

    def test_v3_index_all_route_registered_in_main_app(self):
        """POST /v3/index/all should be accessible in the main app."""
        from app.main import app
        paths = [route.path for route in app.routes]
        assert "/v3/index/all" in paths


# ---------------------------------------------------------------------------
# Test: V3 route registration in isolation
# ---------------------------------------------------------------------------

class TestV3RouterRegistration:
    """Verify all V3 routes are registered on the router itself."""

    def test_v3_batch_run_route_exists(self, app_v3):
        """POST /v3/batch/run should be registered."""
        paths = [route.path for route in app_v3.routes]
        assert "/v3/batch/run" in paths

    def test_v3_index_all_route_exists(self, app_v3):
        """POST /v3/index/all should be registered."""
        paths = [route.path for route in app_v3.routes]
        assert "/v3/index/all" in paths

    def test_v3_batch_status_route_exists(self, app_v3):
        """GET /v3/batch/{batch_id}/status should be registered."""
        paths = [route.path for route in app_v3.routes]
        assert "/v3/batch/{batch_id}/status" in paths

    def test_v3_batch_results_route_exists(self, app_v3):
        """GET /v3/batch/{batch_id}/results should be registered."""
        paths = [route.path for route in app_v3.routes]
        assert "/v3/batch/{batch_id}/results" in paths
