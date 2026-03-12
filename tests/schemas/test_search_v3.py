"""Tests for app.schemas.search_v3 — V3 Pydantic schemas.

Covers V3FieldMatch, V3DocumentResult, V3BatchRunResponse,
V3BatchStatusResponse, and V3BatchResultResponse models.
"""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError


class TestV3FieldMatch:
    """Scenario: V3 field match serialization.

    GIVEN a V3FieldMatch model
    WHEN serialized in various configurations
    THEN found/score/snippet behave per the not-found exclusion rule.
    """

    def test_found_true_with_score_and_snippet(self):
        """Found field with score and snippet serializes all three fields."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(
            found=True,
            score=0.83,
            snippet="...[[MATCH]]343-43-4343[[/MATCH]]...",
        )
        data = match.model_dump(exclude_none=False)
        assert data["found"] is True
        assert data["score"] == 0.83
        assert data["snippet"] == "...[[MATCH]]343-43-4343[[/MATCH]]..."

    def test_found_true_with_null_snippet(self):
        """Found field with score but no snippet (fuzzy highlight gap) serializes found+score+null snippet."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=True, score=0.67, snippet=None)
        data = match.model_dump(exclude_none=False)
        assert data["found"] is True
        assert data["score"] == 0.67
        assert data["snippet"] is None

    def test_found_false_serializes_to_found_only(self):
        """Not-found field serializes to just { 'found': false } — score and snippet excluded."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=False)
        data = match.model_dump(exclude_none=True)
        assert data == {"found": False}
        assert "score" not in data
        assert "snippet" not in data

    def test_found_false_json_serialization(self):
        """Not-found field JSON string excludes score and snippet keys."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=False)
        json_str = match.model_dump_json(exclude_none=True)
        assert '"found":false' in json_str or '"found": false' in json_str
        assert "score" not in json_str
        assert "snippet" not in json_str

    def test_score_is_optional_defaults_to_none(self):
        """score defaults to None when not provided."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=True)
        assert match.score is None

    def test_snippet_is_optional_defaults_to_none(self):
        """snippet defaults to None when not provided."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=True, score=0.5)
        assert match.snippet is None

    def test_found_true_score_is_float(self):
        """score is stored as float."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=True, score=12.5)
        assert isinstance(match.score, float)

    def test_found_false_attributes(self):
        """V3FieldMatch with found=False has score=None and snippet=None."""
        from app.schemas.search_v3 import V3FieldMatch

        match = V3FieldMatch(found=False)
        assert match.found is False
        assert match.score is None
        assert match.snippet is None


class TestV3DocumentResult:
    """Scenario: V3 document result serialization.

    GIVEN a V3DocumentResult model
    WHEN serialized
    THEN all required fields are present including match_details as dict[str, V3FieldMatch].
    """

    def test_document_result_construction_with_matched_fields(self):
        """V3DocumentResult with multiple matched fields constructs correctly."""
        from app.schemas.search_v3 import V3DocumentResult, V3FieldMatch

        result = V3DocumentResult(
            md5="abc123",
            leaked_fields=["SSN", "Fullname"],
            match_details={
                "SSN": V3FieldMatch(
                    found=True, score=0.92, snippet="...[[MATCH]]343-43-4343[[/MATCH]]..."
                ),
                "Fullname": V3FieldMatch(found=True, score=0.67, snippet=None),
                "DOB": V3FieldMatch(found=False),
            },
            overall_confidence=0.72,
            azure_search_score=12.5,
            needs_review=False,
        )

        assert result.md5 == "abc123"
        assert result.leaked_fields == ["SSN", "Fullname"]
        assert result.overall_confidence == 0.72
        assert result.azure_search_score == 12.5
        assert result.needs_review is False
        assert result.match_details["SSN"].found is True
        assert result.match_details["DOB"].found is False

    def test_document_result_file_path_optional(self):
        """file_path is optional and defaults to None."""
        from app.schemas.search_v3 import V3DocumentResult

        result = V3DocumentResult(
            md5="abc123",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
        )
        assert result.file_path is None

    def test_document_result_file_path_set(self):
        """file_path can be set to a string path."""
        from app.schemas.search_v3 import V3DocumentResult

        result = V3DocumentResult(
            md5="abc123",
            file_path="data/TEXT/abc/abc123.txt",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
        )
        assert result.file_path == "data/TEXT/abc/abc123.txt"

    def test_document_result_leaked_fields_default_empty(self):
        """leaked_fields defaults to empty list."""
        from app.schemas.search_v3 import V3DocumentResult

        result = V3DocumentResult(
            md5="abc123",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
        )
        assert result.leaked_fields == []

    def test_document_result_serialization_shape(self):
        """V3DocumentResult serializes with all required keys."""
        from app.schemas.search_v3 import V3DocumentResult, V3FieldMatch

        result = V3DocumentResult(
            md5="abc123def456",
            leaked_fields=["SSN"],
            match_details={
                "SSN": V3FieldMatch(found=True, score=0.9, snippet="...snip...")
            },
            overall_confidence=0.72,
            azure_search_score=10.0,
            needs_review=False,
        )
        data = result.model_dump()
        assert "md5" in data
        assert "file_path" in data
        assert "leaked_fields" in data
        assert "match_details" in data
        assert "overall_confidence" in data
        assert "azure_search_score" in data
        assert "needs_review" in data

    def test_document_result_match_details_is_dict(self):
        """match_details is a dict keyed by field name."""
        from app.schemas.search_v3 import V3DocumentResult, V3FieldMatch

        result = V3DocumentResult(
            md5="xyz",
            leaked_fields=["SSN"],
            match_details={"SSN": V3FieldMatch(found=True, score=1.0)},
            overall_confidence=1.0,
            azure_search_score=15.0,
            needs_review=False,
        )
        assert isinstance(result.match_details, dict)
        assert "SSN" in result.match_details

    def test_document_result_requires_md5(self):
        """ValidationError raised when md5 is missing."""
        from app.schemas.search_v3 import V3DocumentResult

        with pytest.raises(ValidationError):
            V3DocumentResult(
                leaked_fields=[],
                match_details={},
                overall_confidence=0.0,
                azure_search_score=0.0,
                needs_review=False,
            )

    def test_document_result_needs_review_bool(self):
        """needs_review is a bool."""
        from app.schemas.search_v3 import V3DocumentResult

        result = V3DocumentResult(
            md5="abc",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.3,
            azure_search_score=2.0,
            needs_review=True,
        )
        assert result.needs_review is True


class TestV3BatchRunResponse:
    """Scenario: V3 batch start response.

    WHEN POST /v3/batch/run is called
    THEN the response includes batch_id, status "running", total_customers, and method "v3_azure_only".
    """

    def test_batch_run_response_construction(self):
        """V3BatchRunResponse holds batch_id, status, total_customers, and method."""
        from app.schemas.search_v3 import V3BatchRunResponse

        batch_id = str(uuid.uuid4())
        resp = V3BatchRunResponse(
            batch_id=batch_id,
            status="running",
            total_customers=10,
            method="v3_azure_only",
        )
        assert resp.batch_id == batch_id
        assert resp.status == "running"
        assert resp.total_customers == 10
        assert resp.method == "v3_azure_only"

    def test_batch_run_response_method_is_v3_azure_only(self):
        """method field always carries 'v3_azure_only'."""
        from app.schemas.search_v3 import V3BatchRunResponse

        resp = V3BatchRunResponse(
            batch_id=str(uuid.uuid4()),
            status="running",
            total_customers=5,
            method="v3_azure_only",
        )
        assert resp.method == "v3_azure_only"

    def test_batch_run_response_serialization(self):
        """V3BatchRunResponse serializes to dict with correct keys."""
        from app.schemas.search_v3 import V3BatchRunResponse

        resp = V3BatchRunResponse(
            batch_id=str(uuid.uuid4()),
            status="running",
            total_customers=200,
            method="v3_azure_only",
        )
        data = resp.model_dump()
        assert set(data.keys()) == {"batch_id", "status", "total_customers", "method"}
        assert data["method"] == "v3_azure_only"
        assert data["status"] == "running"
        assert data["total_customers"] == 200

    def test_batch_run_response_batch_id_is_str(self):
        """batch_id is a str (UUID string)."""
        from app.schemas.search_v3 import V3BatchRunResponse

        batch_id = str(uuid.uuid4())
        resp = V3BatchRunResponse(
            batch_id=batch_id,
            status="running",
            total_customers=0,
            method="v3_azure_only",
        )
        assert isinstance(resp.batch_id, str)
        assert resp.batch_id == batch_id


class TestV3BatchStatusResponse:
    """Scenario: V3 batch status response.

    WHEN GET /v3/batch/{id}/status returns
    THEN response includes batch_id, status, total_customers, customers_completed,
    customers_failed, and customer_details list.
    """

    def test_batch_status_response_construction(self):
        """V3BatchStatusResponse holds all required status fields."""
        from app.schemas.search_v3 import V3BatchStatusResponse

        batch_id = str(uuid.uuid4())
        resp = V3BatchStatusResponse(
            batch_id=batch_id,
            status="running",
            total_customers=10,
            customers_completed=7,
            customers_failed=1,
            customer_details=[],
        )
        assert resp.batch_id == batch_id
        assert resp.status == "running"
        assert resp.total_customers == 10
        assert resp.customers_completed == 7
        assert resp.customers_failed == 1
        assert resp.customer_details == []

    def test_batch_status_response_method_default(self):
        """V3BatchStatusResponse has method='v3_azure_only' by default."""
        from app.schemas.search_v3 import V3BatchStatusResponse

        resp = V3BatchStatusResponse(
            batch_id=str(uuid.uuid4()),
            status="running",
            total_customers=5,
            customers_completed=0,
            customers_failed=0,
            customer_details=[],
        )
        assert resp.method == "v3_azure_only"

    def test_batch_status_response_customer_details_list(self):
        """customer_details is a list (can hold per-customer status objects)."""
        from app.schemas.search_v3 import V3BatchStatusResponse

        detail = {
            "customer_id": 1,
            "status": "complete",
            "candidates_found": 5,
            "leaks_confirmed": 3,
        }
        resp = V3BatchStatusResponse(
            batch_id=str(uuid.uuid4()),
            status="running",
            total_customers=1,
            customers_completed=1,
            customers_failed=0,
            customer_details=[detail],
        )
        assert len(resp.customer_details) == 1
        assert resp.customer_details[0]["customer_id"] == 1

    def test_batch_status_response_serialization_shape(self):
        """V3BatchStatusResponse serializes with all required keys."""
        from app.schemas.search_v3 import V3BatchStatusResponse

        resp = V3BatchStatusResponse(
            batch_id=str(uuid.uuid4()),
            status="completed",
            total_customers=10,
            customers_completed=9,
            customers_failed=1,
            customer_details=[],
        )
        data = resp.model_dump()
        expected_keys = {
            "batch_id",
            "status",
            "total_customers",
            "customers_completed",
            "customers_failed",
            "customer_details",
            "method",
        }
        assert expected_keys.issubset(set(data.keys()))

    def test_batch_status_response_customer_details_default_empty(self):
        """customer_details defaults to empty list."""
        from app.schemas.search_v3 import V3BatchStatusResponse

        resp = V3BatchStatusResponse(
            batch_id=str(uuid.uuid4()),
            status="pending",
            total_customers=5,
            customers_completed=0,
            customers_failed=0,
            customer_details=[],
        )
        assert resp.customer_details == []


class TestV3BatchResultResponse:
    """Scenario: V3 batch results response for a single result row.

    WHEN the V3 batch results endpoint returns
    THEN each result has batch_id, customer_id, md5, strategy_name='v3_azure_only',
    leaked_fields, match_details, overall_confidence, azure_search_score,
    needs_review, searched_at.
    """

    def test_batch_result_response_construction(self):
        """V3BatchResultResponse holds all V3-specific result fields."""
        from app.schemas.search_v3 import V3BatchResultResponse

        batch_id = str(uuid.uuid4())
        searched_at = datetime(2026, 3, 12, 14, 30, 0, tzinfo=timezone.utc)

        result = V3BatchResultResponse(
            batch_id=batch_id,
            customer_id=1,
            md5="c8578af0e239aaeb7e4030b346430ac3",
            strategy_name="v3_azure_only",
            leaked_fields=["SSN", "Fullname"],
            match_details={
                "SSN": {"found": True, "score": 0.92, "snippet": "...snip..."},
                "Fullname": {"found": True, "score": 0.67, "snippet": None},
                "DOB": {"found": False},
            },
            overall_confidence=0.68,
            azure_search_score=12.5,
            needs_review=False,
            searched_at=searched_at,
        )

        assert result.batch_id == batch_id
        assert result.customer_id == 1
        assert result.md5 == "c8578af0e239aaeb7e4030b346430ac3"
        assert result.strategy_name == "v3_azure_only"
        assert result.leaked_fields == ["SSN", "Fullname"]
        assert result.overall_confidence == 0.68
        assert result.azure_search_score == 12.5
        assert result.needs_review is False
        assert result.searched_at == searched_at

    def test_batch_result_response_strategy_name_is_v3_azure_only(self):
        """strategy_name is always 'v3_azure_only'."""
        from app.schemas.search_v3 import V3BatchResultResponse

        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=1,
            md5="abc",
            strategy_name="v3_azure_only",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
            searched_at=datetime.now(tz=timezone.utc),
        )
        assert result.strategy_name == "v3_azure_only"

    def test_batch_result_response_leaked_fields_list(self):
        """leaked_fields is a list of strings."""
        from app.schemas.search_v3 import V3BatchResultResponse

        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=2,
            md5="abc",
            strategy_name="v3_azure_only",
            leaked_fields=["SSN", "DOB", "City"],
            match_details={},
            overall_confidence=0.5,
            azure_search_score=5.0,
            needs_review=True,
            searched_at=datetime.now(tz=timezone.utc),
        )
        assert isinstance(result.leaked_fields, list)
        assert len(result.leaked_fields) == 3

    def test_batch_result_response_leaked_fields_default_empty(self):
        """leaked_fields defaults to empty list when no fields found."""
        from app.schemas.search_v3 import V3BatchResultResponse

        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=99,
            md5="xyz",
            strategy_name="v3_azure_only",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
            searched_at=datetime.now(tz=timezone.utc),
        )
        assert result.leaked_fields == []

    def test_batch_result_response_match_details_is_dict(self):
        """match_details is a dict keyed by field name."""
        from app.schemas.search_v3 import V3BatchResultResponse

        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=1,
            md5="abc",
            strategy_name="v3_azure_only",
            leaked_fields=["SSN"],
            match_details={"SSN": {"found": True, "score": 0.9}},
            overall_confidence=0.9,
            azure_search_score=9.0,
            needs_review=False,
            searched_at=datetime.now(tz=timezone.utc),
        )
        assert isinstance(result.match_details, dict)
        assert "SSN" in result.match_details

    def test_batch_result_response_serialization_shape(self):
        """V3BatchResultResponse serializes with all required keys."""
        from app.schemas.search_v3 import V3BatchResultResponse

        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=1,
            md5="abc123",
            strategy_name="v3_azure_only",
            leaked_fields=["SSN"],
            match_details={"SSN": {"found": True, "score": 0.9, "snippet": None}},
            overall_confidence=0.9,
            azure_search_score=9.0,
            needs_review=False,
            searched_at=datetime.now(tz=timezone.utc),
        )
        data = result.model_dump()
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

    def test_batch_result_response_searched_at_is_datetime(self):
        """searched_at is a datetime field."""
        from app.schemas.search_v3 import V3BatchResultResponse

        searched_at = datetime(2026, 3, 12, 0, 0, 0, tzinfo=timezone.utc)
        result = V3BatchResultResponse(
            batch_id=str(uuid.uuid4()),
            customer_id=5,
            md5="def456",
            strategy_name="v3_azure_only",
            leaked_fields=[],
            match_details={},
            overall_confidence=0.0,
            azure_search_score=0.0,
            needs_review=False,
            searched_at=searched_at,
        )
        assert result.searched_at == searched_at
        assert isinstance(result.searched_at, datetime)
