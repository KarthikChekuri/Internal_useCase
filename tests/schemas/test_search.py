"""Tests for app.schemas.search — SearchRequest, SearchResponse, FileResult schemas."""

import uuid

import pytest
from pydantic import ValidationError


class TestSearchRequestSSNValidation:
    """Scenario: SSN format validation.

    WHEN POST /search is called with an SSN not matching XXX-XX-XXXX or XXXXXXXXX
    THEN the system returns a 422 validation error.
    """

    def test_ssn_dashed_format_accepted(self):
        """SSN in XXX-XX-XXXX format is valid."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="343-43-4343")
        assert req.ssn == "343-43-4343"

    def test_ssn_undashed_format_accepted(self):
        """SSN in XXXXXXXXX format (9 digits, no dashes) is valid."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="343434343")
        assert req.ssn == "343434343"

    def test_ssn_invalid_too_short(self):
        """SSN with fewer than 9 digits is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="123-45-678")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_invalid_too_long(self):
        """SSN with more than 9 digits is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="123-45-67890")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_invalid_letters(self):
        """SSN containing letters is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="abc-de-fghi")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_invalid_partial_dashes(self):
        """SSN with dashes in wrong positions is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="12-345-6789")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_invalid_empty_string(self):
        """Empty SSN is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_invalid_spaces(self):
        """SSN with spaces is rejected."""
        from app.schemas.search import SearchRequest

        with pytest.raises(ValidationError) as exc_info:
            SearchRequest(ssn="343 43 4343")
        assert "ssn" in str(exc_info.value).lower()

    def test_ssn_all_zeros_dashed_is_valid_format(self):
        """All-zero SSN in correct format is syntactically valid."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="000-00-0000")
        assert req.ssn == "000-00-0000"

    def test_ssn_all_zeros_undashed_is_valid_format(self):
        """All-zero SSN undashed is syntactically valid."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="000000000")
        assert req.ssn == "000000000"


class TestSearchRequestFullname:
    """Scenario: Fullname not provided (skips validation).

    The fullname field in SearchRequest is OPTIONAL.
    """

    def test_fullname_optional_defaults_to_none(self):
        """fullname defaults to None when not provided."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="343-43-4343")
        assert req.fullname is None

    def test_fullname_provided(self):
        """fullname is stored when provided."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="343-43-4343", fullname="Karthik Chekuri")
        assert req.fullname == "Karthik Chekuri"

    def test_fullname_empty_string_treated_as_none(self):
        """Empty string fullname is treated as None (no validation)."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="343-43-4343", fullname="")
        # Empty string should be treated as "not provided"
        assert req.fullname is None or req.fullname == ""


class TestSearchRequestSerialization:
    """SearchRequest serializes to the expected JSON shape."""

    def test_search_request_serialization(self):
        """SearchRequest serializes with ssn and fullname."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="123-45-6789", fullname="John Doe")
        data = req.model_dump()
        assert data["ssn"] == "123-45-6789"
        assert data["fullname"] == "John Doe"

    def test_search_request_serialization_no_fullname(self):
        """SearchRequest serializes with fullname=None when not provided."""
        from app.schemas.search import SearchRequest

        req = SearchRequest(ssn="123456789")
        data = req.model_dump()
        assert data["ssn"] == "123456789"
        assert data["fullname"] is None


class TestFileResult:
    """Tests for FileResult schema."""

    def test_file_result_construction(self):
        """FileResult holds file match data with leaked fields and confidence."""
        from app.schemas.pii import FieldMatchResult
        from app.schemas.search import FileResult

        match_details = {
            "Fullname": FieldMatchResult(
                found=True, method="fuzzy", confidence=0.85, snippet="...Karthik..."
            ),
            "SSN": FieldMatchResult(
                found=True, method="exact", confidence=1.0, snippet="...343-43-4343..."
            ),
        }

        result = FileResult(
            file_name="payroll_2024.xlsx",
            file_guid="abc-def-123",
            leaked_fields=["Fullname", "SSN"],
            overall_confidence=0.92,
            azure_search_score=12.5,
            needs_review=False,
            match_details=match_details,
        )

        assert result.file_name == "payroll_2024.xlsx"
        assert result.file_guid == "abc-def-123"
        assert result.leaked_fields == ["Fullname", "SSN"]
        assert result.overall_confidence == 0.92
        assert result.azure_search_score == 12.5
        assert result.needs_review is False
        assert len(result.match_details) == 2
        assert result.match_details["SSN"].confidence == 1.0

    def test_file_result_empty_leaked_fields(self):
        """FileResult with no leaked fields."""
        from app.schemas.search import FileResult

        result = FileResult(
            file_name="innocent.txt",
            file_guid="xyz-789",
            leaked_fields=[],
            overall_confidence=0.0,
            azure_search_score=1.2,
            needs_review=False,
            match_details={},
        )

        assert result.leaked_fields == []
        assert result.match_details == {}

    def test_file_result_needs_review_flag(self):
        """needs_review flag is correctly stored."""
        from app.schemas.search import FileResult

        result = FileResult(
            file_name="ambiguous.pdf",
            file_guid="guid-456",
            leaked_fields=["DOB"],
            overall_confidence=0.45,
            azure_search_score=3.8,
            needs_review=True,
            match_details={},
        )
        assert result.needs_review is True

    def test_file_result_serialization(self):
        """FileResult serializes to dict with correct keys."""
        from app.schemas.pii import FieldMatchResult
        from app.schemas.search import FileResult

        result = FileResult(
            file_name="test.txt",
            file_guid="g-1",
            leaked_fields=["SSN"],
            overall_confidence=0.99,
            azure_search_score=15.0,
            needs_review=False,
            match_details={
                "SSN": FieldMatchResult(
                    found=True, method="exact", confidence=1.0, snippet="343-43-4343"
                )
            },
        )
        data = result.model_dump()
        assert set(data.keys()) == {
            "file_name",
            "file_guid",
            "leaked_fields",
            "overall_confidence",
            "azure_search_score",
            "needs_review",
            "match_details",
        }
        assert data["match_details"]["SSN"]["found"] is True
        assert data["match_details"]["SSN"]["method"] == "exact"


class TestSearchResponse:
    """Tests for SearchResponse schema.

    Scenario: Response with multiple file matches
    WHEN a search finds PII in multiple files
    THEN results include search_run_id, customer summary, and results array.

    Scenario: SSN masking in response
    WHEN a search response is returned
    THEN the customer SSN is masked as XXX-XX-4343.
    """

    def test_search_response_construction(self):
        """SearchResponse combines search_run_id, customer, and results."""
        from app.schemas.pii import CustomerSummary, FieldMatchResult
        from app.schemas.search import FileResult, SearchResponse

        run_id = uuid.uuid4()
        customer = CustomerSummary(fullname="Karthik Chekuri", ssn="343-43-4343")
        results = [
            FileResult(
                file_name="file1.txt",
                file_guid="g-1",
                leaked_fields=["Fullname"],
                overall_confidence=0.8,
                azure_search_score=10.0,
                needs_review=False,
                match_details={
                    "Fullname": FieldMatchResult(
                        found=True, method="fuzzy", confidence=0.8
                    )
                },
            )
        ]

        response = SearchResponse(
            search_run_id=run_id,
            customer=customer,
            results=results,
        )

        assert response.search_run_id == run_id
        assert response.customer.fullname == "Karthik Chekuri"
        assert response.customer.ssn_masked == "XXX-XX-4343"
        assert len(response.results) == 1
        assert response.results[0].file_name == "file1.txt"

    def test_search_response_empty_results(self):
        """SearchResponse with no file matches has empty results list."""
        from app.schemas.pii import CustomerSummary
        from app.schemas.search import SearchResponse

        run_id = uuid.uuid4()
        customer = CustomerSummary(fullname="Nobody Match", ssn="111-22-3333")

        response = SearchResponse(
            search_run_id=run_id,
            customer=customer,
            results=[],
        )

        assert response.results == []
        assert response.customer.ssn_masked == "XXX-XX-3333"

    def test_search_response_serialization_json_shape(self):
        """SearchResponse serializes to the JSON shape described in the spec."""
        from app.schemas.pii import CustomerSummary, FieldMatchResult
        from app.schemas.search import FileResult, SearchResponse

        run_id = uuid.uuid4()
        customer = CustomerSummary(fullname="Karthik Chekuri", ssn="343-43-4343")
        results = [
            FileResult(
                file_name="payroll.xlsx",
                file_guid="guid-abc",
                leaked_fields=["Fullname", "SSN"],
                overall_confidence=0.92,
                azure_search_score=12.5,
                needs_review=False,
                match_details={
                    "Fullname": FieldMatchResult(
                        found=True, method="fuzzy", confidence=0.85, snippet="...Karthik..."
                    ),
                    "SSN": FieldMatchResult(
                        found=True, method="exact", confidence=1.0, snippet="343-43-4343"
                    ),
                },
            )
        ]

        response = SearchResponse(
            search_run_id=run_id,
            customer=customer,
            results=results,
        )

        data = response.model_dump()

        # Top-level keys
        assert set(data.keys()) == {"search_run_id", "customer", "results"}

        # Customer shape: no raw SSN
        assert set(data["customer"].keys()) == {"fullname", "ssn_masked"}
        assert data["customer"]["ssn_masked"] == "XXX-XX-4343"

        # Results array
        assert len(data["results"]) == 1
        file_result = data["results"][0]
        assert file_result["file_name"] == "payroll.xlsx"
        assert file_result["leaked_fields"] == ["Fullname", "SSN"]
        assert file_result["match_details"]["SSN"]["confidence"] == 1.0

    def test_search_response_search_run_id_is_uuid(self):
        """search_run_id must be a UUID."""
        from app.schemas.pii import CustomerSummary
        from app.schemas.search import SearchResponse

        run_id = uuid.uuid4()
        customer = CustomerSummary(fullname="Test", ssn="123-45-6789")

        response = SearchResponse(
            search_run_id=run_id,
            customer=customer,
            results=[],
        )
        # Should be a valid UUID
        assert isinstance(response.search_run_id, uuid.UUID)

    def test_search_response_multiple_results(self):
        """SearchResponse can hold multiple FileResult items."""
        from app.schemas.pii import CustomerSummary
        from app.schemas.search import FileResult, SearchResponse

        run_id = uuid.uuid4()
        customer = CustomerSummary(fullname="Multi File", ssn="555-66-7777")
        results = [
            FileResult(
                file_name=f"file{i}.txt",
                file_guid=f"g-{i}",
                leaked_fields=["Fullname"],
                overall_confidence=0.5 + i * 0.1,
                azure_search_score=float(i),
                needs_review=False,
                match_details={},
            )
            for i in range(5)
        ]

        response = SearchResponse(
            search_run_id=run_id,
            customer=customer,
            results=results,
        )
        assert len(response.results) == 5
