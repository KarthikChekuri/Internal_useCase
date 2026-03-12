"""Tests for V2 indexing router (Phase V2-2.1).

V2 changes from V1:
- POST /index/all?force=true for force re-index
- POST /index/{md5} instead of /index/{guid}
- IndexResponse now includes files_skipped
- 404 when MD5 not found in DLU

Spec scenarios covered:
- POST /index/all triggers full indexing -> 200 with IndexResponse (including files_skipped)
- POST /index/all?force=true forces re-index
- POST /index/{md5} indexes single file -> 200 with IndexResponse
- POST /index/{md5} with MD5 not found -> 404
- Partial failure response format
- Resumed indexing with files_skipped
"""

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

    def override_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_search_client] = lambda: mock_search_client
    app.dependency_overrides[get_settings] = lambda: mock_settings

    yield TestClient(app), mock_db, mock_search_client, mock_settings

    app.dependency_overrides.clear()


# ===========================================================================
# Test: POST /index/all
# ===========================================================================

class TestIndexAllV2Endpoint:
    """Tests for POST /index/all V2."""

    def test_index_all_returns_200_with_files_skipped(self, client):
        """POST /index/all -> 200 with IndexResponse including files_skipped."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=25,
            files_succeeded=25,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )

        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 25
            assert data["files_succeeded"] == 25
            assert data["files_failed"] == 0
            assert data["files_skipped"] == 0
            assert data["errors"] == []

    def test_index_all_force_false_by_default(self, client):
        """POST /index/all without force param -> force=False passed to service."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=0, files_succeeded=0, files_failed=0, files_skipped=0, errors=[]
        )

        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result) as mock_svc:
            test_client.post("/index/all")
            call_kwargs = mock_svc.call_args.kwargs
            # force should be False (default)
            assert call_kwargs.get("force", False) is False

    def test_index_all_force_true_passed_to_service(self, client):
        """POST /index/all?force=true -> force=True passed to service."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=5, files_succeeded=5, files_failed=0, files_skipped=0, errors=[]
        )

        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result) as mock_svc:
            test_client.post("/index/all?force=true")
            call_kwargs = mock_svc.call_args.kwargs
            assert call_kwargs.get("force") is True

    def test_index_all_partial_failure(self, client):
        """POST /index/all with some failures -> 200 with error list."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=25,
            files_succeeded=23,
            files_failed=2,
            files_skipped=0,
            errors=[
                "MD5-xxx: file not found at path ...",
                "MD5-yyy: file not found at path ...",
            ],
        )

        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 25
            assert data["files_succeeded"] == 23
            assert data["files_failed"] == 2
            assert data["files_skipped"] == 0
            assert len(data["errors"]) == 2

    def test_index_all_resumed_with_skipped(self, client):
        """POST /index/all when 20 already indexed -> files_skipped=20 in response."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=5,
            files_succeeded=5,
            files_failed=0,
            files_skipped=20,
            errors=[],
        )

        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code == 200
            data = response.json()
            assert data["files_skipped"] == 20
            assert data["files_processed"] == 5


# ===========================================================================
# Test: POST /index/{md5}
# ===========================================================================

class TestIndexSingleV2Endpoint:
    """Tests for POST /index/{md5} V2."""

    def test_index_single_md5_returns_200(self, client):
        """POST /index/{md5} with valid MD5 -> 200 with IndexResponse."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1,
            files_succeeded=1,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )

        with patch("app.routers.indexing.index_single_file_v2", return_value=mock_result):
            response = test_client.post("/index/c8578af0e239aaeb7e4030b346430ac3")
            assert response.status_code == 200
            data = response.json()
            assert data["files_processed"] == 1
            assert data["files_succeeded"] == 1
            assert data["files_skipped"] == 0

    def test_index_single_md5_not_found_returns_404(self, client):
        """POST /index/{md5} with MD5 not in DLU -> 404."""
        test_client, mock_db, mock_search_client, mock_settings = client

        with patch("app.routers.indexing.index_single_file_v2", return_value=None):
            response = test_client.post("/index/nonexistentmd5hash00000000000000")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()

    def test_index_single_passes_md5_to_service(self, client):
        """POST /index/{md5} passes MD5 to index_single_file_v2."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1, files_succeeded=1, files_failed=0, files_skipped=0, errors=[]
        )

        test_md5 = "abcdef1234567890abcdef1234567890"

        with patch("app.routers.indexing.index_single_file_v2", return_value=mock_result) as mock_svc:
            test_client.post(f"/index/{test_md5}")
            mock_svc.assert_called_once()
            call_args = mock_svc.call_args
            assert test_md5 in str(call_args)

    def test_index_single_extraction_failure_returns_200(self, client):
        """POST /index/{md5} when extraction fails -> 200 with failure counts (not 500)."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=1,
            files_succeeded=0,
            files_failed=1,
            files_skipped=0,
            errors=["md5abc: extraction failed for /data/corrupt.xlsx"],
        )

        with patch("app.routers.indexing.index_single_file_v2", return_value=mock_result):
            response = test_client.post("/index/md5abc00000000000000000000000000")
            assert response.status_code == 200
            data = response.json()
            assert data["files_failed"] == 1
            assert len(data["errors"]) == 1

    def test_index_single_unsupported_ext_returns_200_with_skipped(self, client):
        """POST /index/{md5} for unsupported extension -> 200 with files_skipped=1."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=1,
            errors=[],
        )

        with patch("app.routers.indexing.index_single_file_v2", return_value=mock_result):
            response = test_client.post("/index/pdf_md5_000000000000000000000000000")
            assert response.status_code == 200
            data = response.json()
            assert data["files_skipped"] == 1
            assert data["files_failed"] == 0
