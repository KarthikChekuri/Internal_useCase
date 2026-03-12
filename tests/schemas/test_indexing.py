"""Tests for app.schemas.indexing — IndexResponse Pydantic schema.

The IndexResponse schema lives in app/schemas/indexing.py (moved from
indexing_service.py) and adds the files_skipped field to support resumable
indexing.
"""

import pytest
from pydantic import ValidationError


class TestIndexResponse:
    """Scenario: Successful bulk indexing response
    WHEN POST /index/all processes 25 files and all succeed
    THEN response is { files_processed: 25, files_succeeded: 25, files_failed: 0,
                       files_skipped: 0, errors: [] }

    Scenario: Partial failure indexing response
    WHEN POST /index/all processes 25 files and 2 fail
    THEN response includes files_failed: 2 and errors list with messages.

    Scenario: Resumed indexing with skipped files
    WHEN POST /index/all is called and 20 files are already indexed
    THEN the response includes files_skipped: 20.
    """

    def test_index_response_all_succeed(self):
        """IndexResponse with all files succeeded."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=25,
            files_succeeded=25,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )
        assert resp.files_processed == 25
        assert resp.files_succeeded == 25
        assert resp.files_failed == 0
        assert resp.files_skipped == 0
        assert resp.errors == []

    def test_index_response_partial_failure(self):
        """IndexResponse with some files failed."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=25,
            files_succeeded=23,
            files_failed=2,
            files_skipped=0,
            errors=[
                "MD5-xxx: file not found at path ...",
                "MD5-yyy: encoding error ...",
            ],
        )
        assert resp.files_processed == 25
        assert resp.files_succeeded == 23
        assert resp.files_failed == 2
        assert resp.files_skipped == 0
        assert len(resp.errors) == 2
        assert "MD5-xxx" in resp.errors[0]

    def test_index_response_with_skipped_files(self):
        """IndexResponse for resumed indexing includes files_skipped count."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=5,
            files_succeeded=5,
            files_failed=0,
            files_skipped=20,
            errors=[],
        )
        assert resp.files_skipped == 20
        assert resp.files_processed == 5

    def test_index_response_files_skipped_defaults_to_zero(self):
        """files_skipped defaults to 0 when not provided."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=10,
            files_succeeded=10,
            files_failed=0,
            errors=[],
        )
        assert resp.files_skipped == 0

    def test_index_response_serialization_shape(self):
        """IndexResponse serializes to dict with all required keys."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=100,
            files_succeeded=97,
            files_failed=3,
            files_skipped=0,
            errors=["abc: failed"],
        )
        data = resp.model_dump()
        assert set(data.keys()) == {
            "files_processed",
            "files_succeeded",
            "files_failed",
            "files_skipped",
            "errors",
        }

    def test_index_response_errors_is_list_of_strings(self):
        """errors is a list of strings."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=5,
            files_succeeded=3,
            files_failed=2,
            files_skipped=0,
            errors=["err1", "err2"],
        )
        assert isinstance(resp.errors, list)
        assert all(isinstance(e, str) for e in resp.errors)

    def test_index_response_empty_run(self):
        """IndexResponse with zero files processed."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )
        assert resp.files_processed == 0
        assert resp.errors == []

    def test_index_response_all_skipped(self):
        """IndexResponse where all files are already indexed (skipped)."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=480,
            errors=[],
        )
        assert resp.files_skipped == 480
        assert resp.files_processed == 0

    def test_index_response_errors_default_empty_list(self):
        """errors defaults to empty list when not provided."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=10,
            files_succeeded=10,
            files_failed=0,
            files_skipped=0,
        )
        assert resp.errors == []

    def test_index_response_serialization_values(self):
        """IndexResponse serializes values correctly."""
        from app.schemas.indexing import IndexResponse

        resp = IndexResponse(
            files_processed=25,
            files_succeeded=25,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )
        data = resp.model_dump()
        assert data == {
            "files_processed": 25,
            "files_succeeded": 25,
            "files_failed": 0,
            "files_skipped": 0,
            "errors": [],
        }
