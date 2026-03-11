"""Tests for app/routers/search.py — POST /search endpoint.

Tests cover all spec scenarios:
- Customer found by SSN -> 200 with results
- Customer not found by SSN -> 404
- SSN format validation -> 422
- Fullname validation mismatch -> 409
- Fullname not provided (skips validation) -> 200
- Duplicate SSN in master_pii -> 409
- No matching files found -> 200 with empty results
- Response with multiple file matches ordered by confidence
- SSN masking in response
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


class TestSearchEndpointStatusCodes:
    """Test that POST /search returns correct HTTP status codes."""

    def test_customer_found_returns_200(self, client):
        """WHEN POST /search with valid SSN that exists -> THEN 200."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse
        from app.schemas.pii import CustomerSummary

        mock_response = SearchResponse(
            search_run_id=uuid.uuid4(),
            customer=CustomerSummary(fullname="Test User", ssn="123-45-6789"),
            results=[],
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response):
            response = test_client.post(
                "/search",
                json={"ssn": "123-45-6789"},
            )
            assert response.status_code == 200

    def test_customer_not_found_returns_404(self, client):
        """WHEN POST /search with SSN not in master_pii -> THEN 404."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.search_service import CustomerNotFoundError

        with patch(
            "app.routers.search.search_customer_pii",
            side_effect=CustomerNotFoundError("Customer not found"),
        ):
            response = test_client.post(
                "/search",
                json={"ssn": "999-99-9999"},
            )
            assert response.status_code == 404
            assert "Customer not found" in response.json()["detail"]

    def test_invalid_ssn_format_returns_422(self, client):
        """WHEN POST /search with invalid SSN format -> THEN 422."""
        test_client, *_ = client

        response = test_client.post(
            "/search",
            json={"ssn": "12-34-5678"},
        )
        assert response.status_code == 422

    def test_fullname_mismatch_returns_409(self, client):
        """WHEN POST /search with mismatched fullname -> THEN 409."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.search_service import FullnameMismatchError

        with patch(
            "app.routers.search.search_customer_pii",
            side_effect=FullnameMismatchError(
                "Provided fullname does not match customer record"
            ),
        ):
            response = test_client.post(
                "/search",
                json={"ssn": "343-43-4343", "fullname": "John Doe"},
            )
            assert response.status_code == 409
            assert "fullname does not match" in response.json()["detail"]

    def test_duplicate_ssn_returns_409(self, client):
        """WHEN POST /search with SSN matching multiple records -> THEN 409."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.services.search_service import DataIntegrityError

        with patch(
            "app.routers.search.search_customer_pii",
            side_effect=DataIntegrityError(
                "Multiple customers found with this SSN -- data integrity error"
            ),
        ):
            response = test_client.post(
                "/search",
                json={"ssn": "343-43-4343"},
            )
            assert response.status_code == 409
            assert "Multiple customers" in response.json()["detail"]

    def test_fullname_not_provided_skips_validation(self, client):
        """WHEN POST /search with no fullname -> THEN proceeds with search."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse
        from app.schemas.pii import CustomerSummary

        mock_response = SearchResponse(
            search_run_id=uuid.uuid4(),
            customer=CustomerSummary(fullname="Karthik Chekuri", ssn="343-43-4343"),
            results=[],
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response):
            response = test_client.post(
                "/search",
                json={"ssn": "343-43-4343"},
            )
            assert response.status_code == 200


class TestSearchEndpointResponse:
    """Test the response body from POST /search."""

    def test_no_matches_returns_empty_results(self, client):
        """WHEN no matching files -> THEN 200 with empty results and message."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse
        from app.schemas.pii import CustomerSummary

        run_id = uuid.uuid4()
        mock_response = SearchResponse(
            search_run_id=run_id,
            customer=CustomerSummary(fullname="Test User", ssn="123-45-6789"),
            results=[],
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response):
            response = test_client.post(
                "/search",
                json={"ssn": "123-45-6789"},
            )
            data = response.json()
            assert data["results"] == []
            assert data["search_run_id"] == str(run_id)

    def test_ssn_masked_in_response(self, client):
        """WHEN response returned -> THEN SSN is masked as XXX-XX-{last4}."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse
        from app.schemas.pii import CustomerSummary

        mock_response = SearchResponse(
            search_run_id=uuid.uuid4(),
            customer=CustomerSummary(fullname="Karthik Chekuri", ssn="343-43-4343"),
            results=[],
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response):
            response = test_client.post(
                "/search",
                json={"ssn": "343-43-4343"},
            )
            data = response.json()
            assert data["customer"]["ssn_masked"] == "XXX-XX-4343"
            # Raw SSN should NOT be in the response
            assert "343-43-4343" not in str(data["customer"])

    def test_results_ordered_by_confidence_descending(self, client):
        """WHEN multiple file matches -> THEN ordered by overall_confidence desc."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse, FileResult
        from app.schemas.pii import CustomerSummary, FieldMatchResult

        # Results are pre-sorted by confidence descending (sorting is
        # the search_service's responsibility, not the router's).
        results = [
            FileResult(
                file_name="high.txt",
                file_guid="guid-high",
                leaked_fields=["SSN", "Fullname"],
                overall_confidence=0.95,
                azure_search_score=2.0,
                needs_review=False,
                match_details={
                    "SSN": FieldMatchResult(found=True, method="regex", confidence=1.0),
                    "Fullname": FieldMatchResult(found=True, method="normalized", confidence=0.95),
                },
            ),
            FileResult(
                file_name="low.txt",
                file_guid="guid-low",
                leaked_fields=["SSN"],
                overall_confidence=0.5,
                azure_search_score=1.0,
                needs_review=False,
                match_details={"SSN": FieldMatchResult(found=True, method="regex", confidence=0.5)},
            ),
        ]

        mock_response = SearchResponse(
            search_run_id=uuid.uuid4(),
            customer=CustomerSummary(fullname="Test User", ssn="123-45-6789"),
            results=results,
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response):
            response = test_client.post(
                "/search",
                json={"ssn": "123-45-6789"},
            )
            data = response.json()
            assert len(data["results"]) == 2
            # Verify descending order by confidence (spec requirement)
            assert data["results"][0]["overall_confidence"] >= data["results"][1]["overall_confidence"]

    def test_search_calls_service_with_correct_args(self, client):
        """POST /search should pass ssn, fullname, db, search_client, settings to service."""
        test_client, mock_db, mock_search_client, mock_settings = client

        from app.schemas.search import SearchResponse
        from app.schemas.pii import CustomerSummary

        mock_response = SearchResponse(
            search_run_id=uuid.uuid4(),
            customer=CustomerSummary(fullname="Test User", ssn="123-45-6789"),
            results=[],
        )

        with patch("app.routers.search.search_customer_pii", return_value=mock_response) as mock_svc:
            test_client.post(
                "/search",
                json={"ssn": "123-45-6789", "fullname": "Test User"},
            )
            mock_svc.assert_called_once()
            call_kwargs = mock_svc.call_args
            # Should pass ssn and fullname
            assert call_kwargs.kwargs.get("ssn") == "123-45-6789"
            assert call_kwargs.kwargs.get("fullname") == "Test User"
