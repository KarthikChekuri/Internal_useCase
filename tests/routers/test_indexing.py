"""Tests for app/routers/indexing.py — POST /index/all and POST /index/{guid}.

Tests cover spec scenarios:
- POST /index/all triggers full indexing -> 200 with IndexResponse
- POST /index/{guid} indexes single file -> 200 with IndexResponse
- POST /index/{guid} with GUID not found -> 404
- Successful bulk indexing response format
- Partial failure indexing response format
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a TestClient with mocked dependencies."""
    from app.main import app
    from app.dependencies import get_db, get_search_client, get_settings

    mock_db = MagicMock()
    mock_search_client = MagicMock()
    mock_settings = MagicMock()
    mock_settings.FILE_BASE_PATH = "C:/test/data"
    mock_settings.CASE_NAME = "test-case"

    def override_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_search_client] = lambda: mock_search_client
    app.dependency_overrides[get_settings] = lambda: mock_settings

    yield TestClient(app), mock_db, mock_search_client, mock_settings

    app.dependency_overrides.clear()


class TestIndexAllEndpoint:
    """Tests for POST /index/all."""

    def test_index_all_returns_200(self, client):
        """WHEN POST /index/all -> THEN 200 with IndexResponse."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=25,
            files_succeeded=25,
            files_failed=0,
            errors=[],
        )

        with patch("app.routers.indexing.index_all_files", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 25
            assert data["files_succeeded"] == 25
            assert data["files_failed"] == 0
            assert data["errors"] == []

    def test_index_all_partial_failure(self, client):
        """WHEN POST /index/all with some failures -> THEN 200 with errors listed."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=25,
            files_succeeded=23,
            files_failed=2,
            errors=[
                "GUID-xxx: file not found at path ...",
                "GUID-yyy: file not found at path ...",
            ],
        )

        with patch("app.routers.indexing.index_all_files", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 25
            assert data["files_succeeded"] == 23
            assert data["files_failed"] == 2
            assert len(data["errors"]) == 2

    def test_index_all_calls_service_correctly(self, client):
        """POST /index/all should pass db, search_client, settings to service."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=0, files_succeeded=0, files_failed=0, errors=[]
        )

        with patch("app.routers.indexing.index_all_files", return_value=mock_result) as mock_svc:
            test_client.post("/index/all")
            mock_svc.assert_called_once()


class TestIndexSingleEndpoint:
    """Tests for POST /index/{guid}."""

    def test_index_single_returns_200(self, client):
        """WHEN POST /index/{guid} with valid GUID -> THEN 200 with IndexResponse."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1,
            files_succeeded=1,
            files_failed=0,
            errors=[],
        )

        with patch("app.routers.indexing.index_single_file", return_value=mock_result):
            response = test_client.post("/index/test-guid-123")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 1
            assert data["files_succeeded"] == 1

    def test_index_single_guid_not_found_returns_404(self, client):
        """WHEN POST /index/{guid} with GUID not in DLU -> THEN 404."""
        test_client, mock_db, mock_search_client, mock_settings = client

        with patch("app.routers.indexing.index_single_file", return_value=None):
            response = test_client.post("/index/nonexistent-guid")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()

    def test_index_single_passes_guid_to_service(self, client):
        """POST /index/{guid} should pass the GUID to index_single_file."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1, files_succeeded=1, files_failed=0, errors=[]
        )

        test_guid = "abc-123-def-456"

        with patch("app.routers.indexing.index_single_file", return_value=mock_result) as mock_svc:
            test_client.post(f"/index/{test_guid}")
            mock_svc.assert_called_once()
            # Verify the guid was passed
            call_args = mock_svc.call_args
            assert test_guid in str(call_args)

    def test_index_single_extraction_failure(self, client):
        """WHEN POST /index/{guid} but extraction fails -> THEN 200 with failure counts."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1,
            files_succeeded=0,
            files_failed=1,
            errors=["test-guid: extraction failed for /some/path.txt"],
        )

        with patch("app.routers.indexing.index_single_file", return_value=mock_result):
            response = test_client.post("/index/test-guid")
            assert response.status_code == 200
            data = response.json()
            assert data["files_failed"] == 1
