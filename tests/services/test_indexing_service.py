"""Tests for app/services/indexing_service.py — Phase 3.1.

Covers:
- DLU query filtering (supported extensions, isExclusion, caseName)
- File path resolution from DLU metadata
- Document building with all required fields
- Batch upload to Azure AI Search
- index_all_files orchestration
- index_single_file for single-GUID indexing
- IndexResponse format
- Error handling (missing files, extraction failures, GUID not found)
- Re-indexing upserts existing documents
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# Prevent sqlalchemy from being imported (it can hang on this machine).
# We mock it at the module level so the DLU model import doesn't trigger a real
# sqlalchemy import during tests.

# We need to make sure the models can be imported without triggering
# real SQLAlchemy connections. We'll mock the necessary pieces.


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for Settings / DLU records
# ---------------------------------------------------------------------------

def _make_settings(**overrides):
    """Return a fake Settings object."""
    defaults = {
        "DATABASE_URL": "mssql+pyodbc://fake",
        "AZURE_SEARCH_ENDPOINT": "https://fake.search.windows.net",
        "AZURE_SEARCH_KEY": "fake-key",
        "AZURE_SEARCH_INDEX": "breach-file-index",
        "FILE_BASE_PATH": r"C:\data\breach",
        "CASE_NAME": "TestCase",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_dlu_record(guid="abc-123", textpath=r"TEXT\c85\file.txt",
                     file_name="file.txt", file_extension=".txt",
                     case_name="TestCase", is_exclusion=False):
    """Return a fake DLU row object."""
    rec = SimpleNamespace(
        GUID=guid,
        TEXTPATH=textpath,
        fileName=file_name,
        fileExtension=file_extension,
        caseName=case_name,
        isExclusion=is_exclusion,
    )
    return rec


# ===========================================================================
# Test: DLU query filtering
# ===========================================================================

@pytest.mark.skip(reason="V1 DLU columns (fileExtension, isExclusion, caseName) removed in V2 model rewrite")
class TestQueryDLURecords:
    """DLU query should filter by extension, isExclusion, and caseName."""

    def test_filter_supported_extensions_and_exclusion_and_case(self):
        """Only records with supported extensions, isExclusion=0, and matching caseName are returned."""
        from app.services.indexing_service import _query_eligible_files

        # Build a mock session whose query chain returns controlled results
        txt_rec = _make_dlu_record(guid="1", file_extension=".txt")
        xls_rec = _make_dlu_record(guid="2", file_extension=".xls")
        xlsx_rec = _make_dlu_record(guid="3", file_extension=".xlsx")
        csv_rec = _make_dlu_record(guid="4", file_extension=".csv")
        pdf_rec = _make_dlu_record(guid="5", file_extension=".pdf")  # unsupported
        excluded_rec = _make_dlu_record(guid="6", file_extension=".txt", is_exclusion=True)
        wrong_case = _make_dlu_record(guid="7", file_extension=".txt", case_name="OtherCase")

        all_records = [txt_rec, xls_rec, xlsx_rec, csv_rec, pdf_rec, excluded_rec, wrong_case]

        db = MagicMock()
        # We mock _query_eligible_files to be called with db and case_name.
        # Since _query_eligible_files does the actual SQL query, we need to test
        # its filtering logic.  We'll mock the db.query chain.
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = [txt_rec, xls_rec, xlsx_rec, csv_rec]

        result = _query_eligible_files(db, "TestCase")

        assert len(result) == 4
        guids = [r.GUID for r in result]
        assert "1" in guids
        assert "2" in guids
        assert "3" in guids
        assert "4" in guids

    def test_excluded_files_are_skipped(self):
        """Records with isExclusion=1 must not be returned."""
        from app.services.indexing_service import _query_eligible_files

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = []  # no eligible records

        result = _query_eligible_files(db, "TestCase")
        assert result == []


# ===========================================================================
# Test: File path resolution
# ===========================================================================

@pytest.mark.skip(reason="V1 function _resolve_file_path removed in V2; see test_indexing_service_v2.py")
class TestFilePathResolution:
    """File path resolution combines FILE_BASE_PATH + TEXTPATH."""

    def test_resolve_file_path(self):
        """Construct full path from base path and TEXTPATH."""
        from app.services.indexing_service import _resolve_file_path

        base = r"C:\data\breach"
        textpath = r"TEXT\c85\c8578af0e239aaeb7e4030b346430ac3.txt"
        result = _resolve_file_path(base, textpath)
        expected = r"C:\data\breach\TEXT\c85\c8578af0e239aaeb7e4030b346430ac3.txt"
        assert result == expected

    def test_resolve_file_path_strips_leading_separator(self):
        """If TEXTPATH starts with a separator, don't double up."""
        from app.services.indexing_service import _resolve_file_path

        base = r"C:\data\breach"
        textpath = r"\TEXT\c85\file.txt"
        result = _resolve_file_path(base, textpath)
        # os.path.join handles this correctly
        assert "breach" in result
        assert "TEXT" in result


# ===========================================================================
# Test: Document building
# ===========================================================================

@pytest.mark.skip(reason="V1 function _build_document removed in V2; see test_indexing_service_v2.py")
class TestBuildDocument:
    """Build a search document from a DLU record + extracted text."""

    def test_build_document_has_all_required_fields(self):
        """Document must include id, file_guid, content, content_phonetic,
        content_lowercase, file_name, file_path, file_extension, case_name."""
        from app.services.indexing_service import _build_document

        record = _make_dlu_record(guid="my-guid-123", file_name="report.txt",
                                  file_extension=".txt", case_name="TestCase")
        text = "John Smith 123-45-6789"
        full_path = r"C:\data\breach\TEXT\report.txt"

        doc = _build_document(record, text, full_path)

        assert doc["id"] == "my-guid-123"
        assert doc["file_guid"] == "my-guid-123"
        assert doc["content"] == text
        assert doc["content_phonetic"] == text
        assert doc["content_lowercase"] == text
        assert doc["file_name"] == "report.txt"
        assert doc["file_path"] == full_path
        assert doc["file_extension"] == ".txt"
        assert doc["case_name"] == "TestCase"

    def test_build_document_all_content_fields_same(self):
        """content, content_phonetic, content_lowercase must all be identical."""
        from app.services.indexing_service import _build_document

        record = _make_dlu_record()
        text = "Jane Doe SSN 987-65-4321"
        doc = _build_document(record, text, "/some/path")
        assert doc["content"] == doc["content_phonetic"] == doc["content_lowercase"]

    def test_build_document_empty_text(self):
        """Empty file content should produce empty string in all content fields."""
        from app.services.indexing_service import _build_document

        record = _make_dlu_record()
        doc = _build_document(record, "", "/some/path")
        assert doc["content"] == ""
        assert doc["content_phonetic"] == ""
        assert doc["content_lowercase"] == ""


# ===========================================================================
# Test: Batch upload to Azure AI Search
# ===========================================================================

class TestBatchUpload:
    """Documents are uploaded in batches of up to 1000."""

    def test_single_batch_under_1000(self):
        """When documents <= 1000, one upload call is made."""
        from app.services.indexing_service import _upload_documents

        docs = [{"id": str(i)} for i in range(50)]
        search_client = MagicMock()
        search_client.upload_documents.return_value = [
            SimpleNamespace(succeeded=True, key=str(i)) for i in range(50)
        ]

        failed = _upload_documents(search_client, docs)
        search_client.upload_documents.assert_called_once()
        assert failed == []

    def test_multiple_batches_over_1000(self):
        """When documents > 1000, upload is called in batches."""
        from app.services.indexing_service import _upload_documents

        docs = [{"id": str(i)} for i in range(2500)]
        search_client = MagicMock()

        # Each call returns success for all docs in that batch
        def mock_upload(documents):
            return [SimpleNamespace(succeeded=True, key=d["id"]) for d in documents]

        search_client.upload_documents.side_effect = mock_upload

        failed = _upload_documents(search_client, docs)
        assert search_client.upload_documents.call_count == 3  # 1000+1000+500
        assert failed == []

    def test_upload_reports_failed_documents(self):
        """Failed documents in the upload response are returned."""
        from app.services.indexing_service import _upload_documents

        docs = [{"id": "ok-1"}, {"id": "fail-1"}, {"id": "ok-2"}]
        search_client = MagicMock()
        search_client.upload_documents.return_value = [
            SimpleNamespace(succeeded=True, key="ok-1"),
            SimpleNamespace(succeeded=False, key="fail-1",
                            error_message="Upload error"),
            SimpleNamespace(succeeded=True, key="ok-2"),
        ]

        failed = _upload_documents(search_client, docs)
        assert len(failed) == 1
        assert "fail-1" in failed[0]


# ===========================================================================
# Test: index_all_files
# ===========================================================================

@pytest.mark.skip(reason="V1 function index_all_files removed in V2; see test_indexing_service_v2.py")
class TestIndexAllFiles:
    """index_all_files queries DLU, extracts text, builds docs, uploads."""

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service._query_eligible_files")
    def test_successful_bulk_indexing(self, mock_query, mock_upload, mock_extract):
        """All 3 files processed successfully -> 3 succeeded, 0 failed."""
        from app.services.indexing_service import index_all_files

        records = [
            _make_dlu_record(guid="g1", textpath=r"TEXT\a.txt",
                             file_name="a.txt", file_extension=".txt"),
            _make_dlu_record(guid="g2", textpath=r"TEXT\b.csv",
                             file_name="b.csv", file_extension=".csv"),
            _make_dlu_record(guid="g3", textpath=r"TEXT\c.xlsx",
                             file_name="c.xlsx", file_extension=".xlsx"),
        ]
        mock_query.return_value = records
        mock_extract.side_effect = ["text from a", "text from b", "text from c"]
        mock_upload.return_value = []  # no failures

        config = _make_settings()
        db = MagicMock()
        search_client = MagicMock()

        result = index_all_files(db, search_client, config)

        assert result.files_processed == 3
        assert result.files_succeeded == 3
        assert result.files_failed == 0
        assert result.errors == []

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service._query_eligible_files")
    def test_partial_failure(self, mock_query, mock_upload, mock_extract):
        """2 out of 3 files fail extraction -> 1 succeeded, 2 failed."""
        from app.services.indexing_service import index_all_files

        records = [
            _make_dlu_record(guid="g1", textpath=r"TEXT\a.txt",
                             file_name="a.txt", file_extension=".txt"),
            _make_dlu_record(guid="g2", textpath=r"TEXT\b.csv",
                             file_name="b.csv", file_extension=".csv"),
            _make_dlu_record(guid="g3", textpath=r"TEXT\c.xlsx",
                             file_name="c.xlsx", file_extension=".xlsx"),
        ]
        mock_query.return_value = records
        # g1 succeeds, g2 and g3 fail (return None)
        mock_extract.side_effect = ["text from a", None, None]
        mock_upload.return_value = []  # no upload failures

        config = _make_settings()
        db = MagicMock()
        search_client = MagicMock()

        result = index_all_files(db, search_client, config)

        assert result.files_processed == 3
        assert result.files_succeeded == 1
        assert result.files_failed == 2
        assert len(result.errors) == 2
        assert "g2" in result.errors[0]
        assert "g3" in result.errors[1]

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service._query_eligible_files")
    def test_upload_failure_counted(self, mock_query, mock_upload, mock_extract):
        """If upload reports failures, they're reflected in the response."""
        from app.services.indexing_service import index_all_files

        records = [
            _make_dlu_record(guid="g1", textpath=r"TEXT\a.txt",
                             file_name="a.txt", file_extension=".txt"),
            _make_dlu_record(guid="g2", textpath=r"TEXT\b.txt",
                             file_name="b.txt", file_extension=".txt"),
        ]
        mock_query.return_value = records
        mock_extract.side_effect = ["text a", "text b"]
        mock_upload.return_value = ["g2: upload failed"]

        config = _make_settings()
        db = MagicMock()
        search_client = MagicMock()

        result = index_all_files(db, search_client, config)

        assert result.files_processed == 2
        assert result.files_succeeded == 1
        assert result.files_failed == 1
        assert "g2: upload failed" in result.errors

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service._query_eligible_files")
    def test_no_eligible_files(self, mock_query, mock_upload, mock_extract):
        """When no files are eligible, 0 processed, 0 succeeded."""
        from app.services.indexing_service import index_all_files

        mock_query.return_value = []
        config = _make_settings()
        db = MagicMock()
        search_client = MagicMock()

        result = index_all_files(db, search_client, config)

        assert result.files_processed == 0
        assert result.files_succeeded == 0
        assert result.files_failed == 0
        assert result.errors == []
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service._query_eligible_files")
    def test_reindexing_upserts(self, mock_query, mock_upload, mock_extract):
        """Re-indexing uses the same id (GUID), relying on Azure upsert behavior."""
        from app.services.indexing_service import index_all_files

        records = [
            _make_dlu_record(guid="g1", textpath=r"TEXT\a.txt",
                             file_name="a.txt", file_extension=".txt"),
        ]
        mock_query.return_value = records
        mock_extract.return_value = "updated text"
        mock_upload.return_value = []

        config = _make_settings()
        db = MagicMock()
        search_client = MagicMock()

        result = index_all_files(db, search_client, config)

        # Verify the document id equals the GUID (enables upsert)
        uploaded_docs = mock_upload.call_args[0][1]
        assert uploaded_docs[0]["id"] == "g1"
        assert result.files_succeeded == 1


# ===========================================================================
# Test: index_single_file
# ===========================================================================

@pytest.mark.skip(reason="V1 DLU column GUID removed in V2 model rewrite; use test_indexing_service_v2.py")
class TestIndexSingleFile:
    """index_single_file handles the single-GUID case."""

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    def test_index_single_file_success(self, mock_upload, mock_extract):
        """Single file indexed successfully."""
        from app.services.indexing_service import index_single_file

        record = _make_dlu_record(guid="single-guid", textpath=r"TEXT\single.txt",
                                  file_name="single.txt", file_extension=".txt")

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = record

        mock_extract.return_value = "single file text"
        mock_upload.return_value = []

        config = _make_settings()
        search_client = MagicMock()

        result = index_single_file(db, search_client, config, "single-guid")

        assert result.files_processed == 1
        assert result.files_succeeded == 1
        assert result.files_failed == 0
        assert result.errors == []

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    def test_index_single_file_guid_not_found(self, mock_upload, mock_extract):
        """GUID not found in DLU returns None (caller raises 404)."""
        from app.services.indexing_service import index_single_file

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None

        config = _make_settings()
        search_client = MagicMock()

        result = index_single_file(db, search_client, config, "nonexistent-guid")

        assert result is None
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    def test_index_single_file_extraction_failure(self, mock_upload, mock_extract):
        """Extraction failure for single file -> 1 processed, 0 succeeded, 1 failed."""
        from app.services.indexing_service import index_single_file

        record = _make_dlu_record(guid="bad-guid", textpath=r"TEXT\corrupt.xlsx",
                                  file_name="corrupt.xlsx", file_extension=".xlsx")

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = record

        mock_extract.return_value = None  # extraction failed

        config = _make_settings()
        search_client = MagicMock()

        result = index_single_file(db, search_client, config, "bad-guid")

        assert result.files_processed == 1
        assert result.files_succeeded == 0
        assert result.files_failed == 1
        assert len(result.errors) == 1
        assert "bad-guid" in result.errors[0]
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._upload_documents")
    def test_index_single_file_upload_failure(self, mock_upload, mock_extract):
        """Upload failure for single file -> 1 processed, 0 succeeded, 1 failed."""
        from app.services.indexing_service import index_single_file

        record = _make_dlu_record(guid="up-fail", textpath=r"TEXT\ok.txt",
                                  file_name="ok.txt", file_extension=".txt")

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = record

        mock_extract.return_value = "some text"
        mock_upload.return_value = ["up-fail: upload error"]

        config = _make_settings()
        search_client = MagicMock()

        result = index_single_file(db, search_client, config, "up-fail")

        assert result.files_processed == 1
        assert result.files_succeeded == 0
        assert result.files_failed == 1
        assert "up-fail: upload error" in result.errors


# ===========================================================================
# Test: IndexResponse schema
# ===========================================================================

class TestIndexResponse:
    """IndexResponse must have the right fields and types."""

    def test_successful_response_format(self):
        """Full success: 25 processed, 25 succeeded, 0 failed, empty errors."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=25, files_succeeded=25, files_failed=0, errors=[]
        )
        assert resp.files_processed == 25
        assert resp.files_succeeded == 25
        assert resp.files_failed == 0
        assert resp.errors == []

    def test_partial_failure_response_format(self):
        """Partial failure: 25 processed, 23 succeeded, 2 failed, 2 errors."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=25, files_succeeded=23, files_failed=2,
            errors=["GUID-xxx: file not found at path ...",
                    "GUID-yyy: file not found at path ..."]
        )
        assert resp.files_processed == 25
        assert resp.files_succeeded == 23
        assert resp.files_failed == 2
        assert len(resp.errors) == 2

    def test_response_serializes_to_dict(self):
        """IndexResponse can be serialized to dict (for JSON response)."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=5, files_succeeded=4, files_failed=1,
            errors=["g1: failed"]
        )
        d = resp.model_dump()
        assert d == {
            "files_processed": 5,
            "files_succeeded": 4,
            "files_failed": 1,
            "files_skipped": 0,
            "errors": ["g1: failed"],
        }
