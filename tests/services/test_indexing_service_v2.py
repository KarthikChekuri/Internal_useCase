"""Tests for V2 Indexing Service (Phase V2-2.1).

V2 changes from V1:
- DLU table: MD5 (PK) + file_path only (no GUID/TEXTPATH/fileName/fileExtension/caseName/isExclusion)
- Extension filtering done at runtime from file_path extension
- file_path used directly (no FILE_BASE_PATH join)
- Document id = MD5 hash
- Resumability via [Index].[file_status] table
- files_skipped counted in IndexResponse
- force=True re-indexes all regardless of status

Covers spec scenarios from openspec/changes/breach-pii-search/specs/file-indexing/spec.md:
- Extension filtering from file_path
- Direct file_path resolution
- Document id=MD5
- Resumability: skip indexed, retry failed, force re-index
- file_status table updates (indexed/failed)
- files_skipped counting
- IndexResponse format with files_skipped
- Error cases (not found, encoding error, corrupt file, unsupported ext)
"""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for DLU records and FileStatus rows
# ---------------------------------------------------------------------------

def _make_dlu_v2(md5="abc123", file_path="data/TEXT/c85/abc123.txt"):
    """Return a fake DLU V2 row object (MD5 + file_path only)."""
    return SimpleNamespace(MD5=md5, file_path=file_path)


def _make_file_status(md5="abc123", status="indexed", error_message=None):
    """Return a fake FileStatus row object."""
    return SimpleNamespace(
        md5=md5,
        status=status,
        indexed_at=datetime.datetime(2025, 1, 1),
        error_message=error_message,
    )


def _make_settings_v2(**overrides):
    """Return a fake Settings object for V2 (no FILE_BASE_PATH or CASE_NAME needed)."""
    defaults = {
        "DATABASE_URL": "mssql+pyodbc://fake",
        "AZURE_SEARCH_ENDPOINT": "https://fake.search.windows.net",
        "AZURE_SEARCH_KEY": "fake-key",
        "AZURE_SEARCH_INDEX": "breach-file-index",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ===========================================================================
# Test: Extension filtering from file_path
# ===========================================================================

class TestExtensionFilteringFromFilePath:
    """V2 filters by extension from file_path at runtime."""

    def test_txt_file_is_supported(self):
        """WHEN DLU record has file_path ending in .txt THEN it is eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("data/TEXT/c85/abc123.txt") is True

    def test_xlsx_file_is_supported(self):
        """WHEN DLU record has file_path ending in .xlsx THEN it is eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("data/TEXT/file.xlsx") is True

    def test_xls_file_is_supported(self):
        """WHEN DLU record has file_path ending in .xls THEN it is eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("data/TEXT/file.xls") is True

    def test_csv_file_is_supported(self):
        """WHEN DLU record has file_path ending in .csv THEN it is eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("reports/data.csv") is True

    def test_pdf_file_is_not_supported(self):
        """WHEN DLU record has file_path ending in .pdf THEN it is NOT eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("documents/invoice.pdf") is False

    def test_mp4_file_is_not_supported(self):
        """WHEN DLU record has file_path ending in .mp4 THEN it is NOT eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("videos/clip.mp4") is False

    def test_no_extension_is_not_supported(self):
        """WHEN file_path has no extension THEN it is NOT eligible."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("data/TEXT/somefile") is False

    def test_case_insensitive_extension(self):
        """Extension check is case-insensitive (.TXT is supported)."""
        from app.services.indexing_service import _is_supported_extension
        assert _is_supported_extension("data/REPORT.TXT") is True
        assert _is_supported_extension("data/REPORT.XLSX") is True


# ===========================================================================
# Test: V2 document building — id=MD5, uses file_path
# ===========================================================================

class TestBuildDocumentV2:
    """V2 document: id=MD5, md5, content, content_phonetic, content_lowercase, file_path."""

    def test_build_document_v2_id_is_md5(self):
        """Document id must equal the MD5 hash."""
        from app.services.indexing_service import _build_document_v2

        record = _make_dlu_v2(md5="abc123md5hash", file_path="data/TEXT/c85/abc123.txt")
        text = "John Smith SSN 123-45-6789"

        doc = _build_document_v2(record, text)

        assert doc["id"] == "abc123md5hash"

    def test_build_document_v2_has_md5_field(self):
        """Document must include md5 field."""
        from app.services.indexing_service import _build_document_v2

        record = _make_dlu_v2(md5="mymd5", file_path="data/file.txt")
        doc = _build_document_v2(record, "some text")

        assert doc["md5"] == "mymd5"

    def test_build_document_v2_content_fields_all_same(self):
        """content, content_phonetic, content_lowercase must all be identical."""
        from app.services.indexing_service import _build_document_v2

        record = _make_dlu_v2(md5="md5abc", file_path="data/file.csv")
        text = "Jane Doe 1990-01-01"

        doc = _build_document_v2(record, text)

        assert doc["content"] == text
        assert doc["content_phonetic"] == text
        assert doc["content_lowercase"] == text

    def test_build_document_v2_file_path_from_record(self):
        """Document file_path comes from DLU record directly."""
        from app.services.indexing_service import _build_document_v2

        fp = "data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt"
        record = _make_dlu_v2(md5="c8578af0e239aaeb7e4030b346430ac3", file_path=fp)
        doc = _build_document_v2(record, "hello world")

        assert doc["file_path"] == fp

    def test_build_document_v2_required_fields_present(self):
        """All required fields (id, md5, content, content_phonetic, content_lowercase, file_path) are present."""
        from app.services.indexing_service import _build_document_v2

        record = _make_dlu_v2()
        doc = _build_document_v2(record, "test content")

        required = {"id", "md5", "content", "content_phonetic", "content_lowercase", "file_path"}
        assert required.issubset(doc.keys())

    def test_build_document_v2_empty_text(self):
        """Empty text produces empty strings in all content fields."""
        from app.services.indexing_service import _build_document_v2

        record = _make_dlu_v2()
        doc = _build_document_v2(record, "")

        assert doc["content"] == ""
        assert doc["content_phonetic"] == ""
        assert doc["content_lowercase"] == ""


# ===========================================================================
# Test: V2 query — DLU has only MD5 and file_path
# ===========================================================================

class TestQueryDLUV2:
    """V2 queries DLU and returns all records (extension filter done at runtime)."""

    def test_query_all_dlu_records(self):
        """_query_all_dlu_records returns all rows from DLU."""
        from app.services.indexing_service import _query_all_dlu_records

        records = [
            _make_dlu_v2(md5="md5a", file_path="data/a.txt"),
            _make_dlu_v2(md5="md5b", file_path="data/b.pdf"),
        ]

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.all.return_value = records

        result = _query_all_dlu_records(db)

        assert len(result) == 2
        assert result[0].MD5 == "md5a"
        assert result[1].MD5 == "md5b"

    def test_query_single_dlu_record_by_md5(self):
        """_query_dlu_by_md5 returns the record matching MD5."""
        from app.services.indexing_service import _query_dlu_by_md5

        record = _make_dlu_v2(md5="target123", file_path="data/target.txt")

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = record

        result = _query_dlu_by_md5(db, "target123")

        assert result is not None
        assert result.MD5 == "target123"

    def test_query_single_dlu_record_not_found(self):
        """_query_dlu_by_md5 returns None when MD5 not in DLU."""
        from app.services.indexing_service import _query_dlu_by_md5

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None

        result = _query_dlu_by_md5(db, "nonexistent")

        assert result is None


# ===========================================================================
# Test: Resumability via file_status table
# ===========================================================================

class TestResumability:
    """Resumability: skip indexed, retry failed, force re-index."""

    def test_get_indexed_md5s_returns_indexed_set(self):
        """_get_indexed_md5s returns set of MD5s with status='indexed'."""
        from app.services.indexing_service import _get_indexed_md5s

        rows = [
            _make_file_status(md5="md5a", status="indexed"),
            _make_file_status(md5="md5b", status="indexed"),
        ]

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = rows

        result = _get_indexed_md5s(db)

        assert "md5a" in result
        assert "md5b" in result
        assert isinstance(result, set)

    def test_already_indexed_files_are_skipped(self):
        """WHEN file is already indexed THEN it is skipped (not re-indexed)."""
        from app.services.indexing_service import index_all_files_v2

        records = [
            _make_dlu_v2(md5="md5already", file_path="data/a.txt"),
            _make_dlu_v2(md5="md5new", file_path="data/b.txt"),
        ]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value={"md5already"}), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="some text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=False)

        # md5already is skipped, md5new is processed
        assert result.files_skipped == 1
        assert result.files_processed == 1  # only md5new was processed
        assert result.files_succeeded == 1

    def test_resume_after_interruption(self):
        """WHEN 480 of 500 files already indexed THEN only remaining 20 are processed."""
        from app.services.indexing_service import index_all_files_v2

        # 480 already indexed, 20 new
        already_indexed = {f"md5_{i}" for i in range(480)}
        new_records = [_make_dlu_v2(md5=f"md5_new_{i}", file_path=f"data/file_{i}.txt") for i in range(20)]
        old_records = [_make_dlu_v2(md5=m, file_path=f"data/old_{m}.txt") for m in already_indexed]
        all_records = old_records + new_records

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=all_records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=already_indexed), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_skipped == 480
        assert result.files_processed == 20
        assert result.files_succeeded == 20

    def test_force_reindex_ignores_status(self):
        """WHEN force=True THEN all files are re-indexed regardless of status."""
        from app.services.indexing_service import index_all_files_v2

        records = [
            _make_dlu_v2(md5="md5a", file_path="data/a.txt"),
            _make_dlu_v2(md5="md5b", file_path="data/b.txt"),
        ]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value={"md5a", "md5b"}), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=True)

        # force=True: no files skipped, both processed
        assert result.files_skipped == 0
        assert result.files_processed == 2
        assert result.files_succeeded == 2

    def test_failed_files_are_retried(self):
        """WHEN files failed in previous run THEN they are retried (not skipped)."""
        from app.services.indexing_service import index_all_files_v2

        records = [
            _make_dlu_v2(md5="md5_failed", file_path="data/a.txt"),
        ]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        # Only "indexed" status is skipped; "failed" status is retried
        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=set()), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="recovered text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=False)

        # md5_failed was not in indexed set, so it gets processed
        assert result.files_skipped == 0
        assert result.files_processed == 1
        assert result.files_succeeded == 1


# ===========================================================================
# Test: file_status updates
# ===========================================================================

class TestFileStatusUpdates:
    """file_status table is updated for each processed file."""

    def test_upsert_file_status_indexed(self):
        """_upsert_file_status sets status='indexed' for successful file."""
        from app.services.indexing_service import _upsert_file_status

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None  # no existing row

        _upsert_file_status(db, "md5abc", status="indexed")

        # Should add a new row and commit
        db.add.assert_called_once()
        db.commit.assert_called_once()
        # Verify the added object has correct fields
        added_obj = db.add.call_args[0][0]
        assert added_obj.md5 == "md5abc"
        assert added_obj.status == "indexed"

    def test_upsert_file_status_failed(self):
        """_upsert_file_status sets status='failed' with error_message."""
        from app.services.indexing_service import _upsert_file_status

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None

        _upsert_file_status(db, "md5xyz", status="failed", error_message="file not found")

        db.add.assert_called_once()
        added_obj = db.add.call_args[0][0]
        assert added_obj.status == "failed"
        assert added_obj.error_message == "file not found"

    def test_upsert_file_status_updates_existing_row(self):
        """_upsert_file_status updates an existing row if MD5 already present."""
        from app.services.indexing_service import _upsert_file_status

        existing = _make_file_status(md5="md5abc", status="failed", error_message="old error")

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = existing

        _upsert_file_status(db, "md5abc", status="indexed")

        # Should NOT call db.add (just update existing)
        db.add.assert_not_called()
        db.commit.assert_called_once()
        assert existing.status == "indexed"

    def test_successful_indexing_updates_file_status(self):
        """WHEN file indexed successfully THEN file_status is updated to 'indexed'."""
        from app.services.indexing_service import index_all_files_v2

        records = [_make_dlu_v2(md5="md5ok", file_path="data/a.txt")]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=set()), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="some text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status") as mock_upsert:

            result = index_all_files_v2(db, search_client, config, force=False)

        # Verify _upsert_file_status was called with "indexed"
        mock_upsert.assert_called()
        call_args = mock_upsert.call_args_list
        # At least one call with status="indexed"
        statuses = [c.kwargs.get("status") or c.args[2] for c in call_args]
        assert "indexed" in statuses

    def test_failed_indexing_updates_file_status(self):
        """WHEN file fails to extract THEN file_status is updated to 'failed'."""
        from app.services.indexing_service import index_all_files_v2

        records = [_make_dlu_v2(md5="md5fail", file_path="data/corrupt.xlsx")]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=set()), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value=None), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status") as mock_upsert:

            result = index_all_files_v2(db, search_client, config, force=False)

        # Verify _upsert_file_status was called with "failed"
        mock_upsert.assert_called()
        call_args = mock_upsert.call_args_list
        statuses = [c.kwargs.get("status") or c.args[2] for c in call_args]
        assert "failed" in statuses


# ===========================================================================
# Test: files_skipped counting
# ===========================================================================

class TestFilesSkippedCounting:
    """files_skipped reflects unsupported extensions and already-indexed files."""

    def test_unsupported_extension_counted_in_skipped(self):
        """WHEN file_path has unsupported extension THEN counted in files_skipped (not files_failed)."""
        from app.services.indexing_service import index_all_files_v2

        records = [
            _make_dlu_v2(md5="md5pdf", file_path="data/report.pdf"),
            _make_dlu_v2(md5="md5txt", file_path="data/file.txt"),
        ]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        def fake_is_supported(fp):
            return fp.endswith(".txt")

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=set()), \
             patch("app.services.indexing_service._is_supported_extension", side_effect=fake_is_supported), \
             patch("app.services.indexing_service.extract_text", return_value="text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_skipped == 1  # pdf file
        assert result.files_failed == 0   # not a failure
        assert result.files_processed == 1  # only txt
        assert result.files_succeeded == 1

    def test_resumed_indexing_skipped_count(self):
        """WHEN 20 already indexed THEN files_skipped=20 in response."""
        from app.services.indexing_service import index_all_files_v2

        already_indexed_set = {f"md5_{i}" for i in range(20)}
        records = [
            *[_make_dlu_v2(md5=m, file_path=f"data/{m}.txt") for m in already_indexed_set],
            _make_dlu_v2(md5="md5_new", file_path="data/new.txt"),
        ]

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        with patch("app.services.indexing_service._query_all_dlu_records", return_value=records), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=already_indexed_set), \
             patch("app.services.indexing_service._is_supported_extension", return_value=True), \
             patch("app.services.indexing_service.extract_text", return_value="new text"), \
             patch("app.services.indexing_service._upload_documents", return_value=[]), \
             patch("app.services.indexing_service._upsert_file_status"):

            result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_skipped == 20
        assert result.files_processed == 1  # only md5_new
        assert result.files_succeeded == 1


# ===========================================================================
# Test: IndexResponse V2 format
# ===========================================================================

class TestIndexResponseV2:
    """IndexResponse V2 includes files_skipped."""

    def test_index_response_v2_has_files_skipped(self):
        """IndexResponse V2 must have files_skipped field."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=25,
            files_succeeded=25,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )
        assert resp.files_skipped == 0

    def test_successful_bulk_indexing_response(self):
        """Full success: 25 processed, 25 succeeded, 0 failed, 0 skipped."""
        from app.services.indexing_service import IndexResponse

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

    def test_partial_failure_indexing_response(self):
        """25 processed, 2 fail -> 23 succeeded, 2 failed, 0 skipped."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=25,
            files_succeeded=23,
            files_failed=2,
            files_skipped=0,
            errors=["MD5-xxx: file not found at path ...", "MD5-yyy: encoding error ..."],
        )
        assert resp.files_processed == 25
        assert resp.files_succeeded == 23
        assert resp.files_failed == 2
        assert resp.files_skipped == 0
        assert len(resp.errors) == 2

    def test_resumed_indexing_response_with_skipped(self):
        """When 20 already indexed -> files_skipped=20."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=5,
            files_succeeded=5,
            files_failed=0,
            files_skipped=20,
            errors=[],
        )
        assert resp.files_skipped == 20
        assert resp.files_processed == 5

    def test_index_response_v2_serializes(self):
        """IndexResponse V2 serializes to dict with files_skipped."""
        from app.services.indexing_service import IndexResponse

        resp = IndexResponse(
            files_processed=10,
            files_succeeded=8,
            files_failed=1,
            files_skipped=1,
            errors=["md5bad: extraction failed"],
        )
        d = resp.model_dump()
        assert "files_skipped" in d
        assert d["files_skipped"] == 1
        assert d["files_processed"] == 10


# ===========================================================================
# Test: index_all_files_v2 full orchestration
# ===========================================================================

class TestIndexAllFilesV2:
    """index_all_files_v2 orchestrates query -> filter -> extract -> upload."""

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._get_indexed_md5s")
    @patch("app.services.indexing_service._query_all_dlu_records")
    def test_successful_bulk_indexing(self, mock_query, mock_get_indexed, mock_extract, mock_upload, mock_upsert):
        """25 files processed successfully -> 25 succeeded, 0 failed, 0 skipped."""
        from app.services.indexing_service import index_all_files_v2

        records = [_make_dlu_v2(md5=f"md5_{i}", file_path=f"data/file_{i}.txt") for i in range(25)]
        mock_query.return_value = records
        mock_get_indexed.return_value = set()
        mock_extract.return_value = "text content"
        mock_upload.return_value = []

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_processed == 25
        assert result.files_succeeded == 25
        assert result.files_failed == 0
        assert result.files_skipped == 0
        assert result.errors == []

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._get_indexed_md5s")
    @patch("app.services.indexing_service._query_all_dlu_records")
    def test_partial_failure_response(self, mock_query, mock_get_indexed, mock_extract, mock_upload, mock_upsert):
        """25 files, 2 fail extraction -> 23 succeeded, 2 failed."""
        from app.services.indexing_service import index_all_files_v2

        records = [_make_dlu_v2(md5=f"md5_{i}", file_path=f"data/file_{i}.txt") for i in range(25)]
        mock_query.return_value = records
        mock_get_indexed.return_value = set()
        # First 23 succeed, last 2 fail
        mock_extract.side_effect = ["text"] * 23 + [None, None]
        mock_upload.return_value = []

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_processed == 25
        assert result.files_succeeded == 23
        assert result.files_failed == 2
        assert result.files_skipped == 0
        assert len(result.errors) == 2

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._get_indexed_md5s")
    @patch("app.services.indexing_service._query_all_dlu_records")
    def test_empty_dlu_returns_zero_counts(self, mock_query, mock_get_indexed, mock_extract, mock_upload, mock_upsert):
        """Empty DLU -> 0 processed, 0 succeeded, 0 failed, 0 skipped."""
        from app.services.indexing_service import index_all_files_v2

        mock_query.return_value = []
        mock_get_indexed.return_value = set()

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_all_files_v2(db, search_client, config, force=False)

        assert result.files_processed == 0
        assert result.files_succeeded == 0
        assert result.files_failed == 0
        assert result.files_skipped == 0
        assert result.errors == []

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._get_indexed_md5s")
    @patch("app.services.indexing_service._query_all_dlu_records")
    def test_document_id_equals_md5(self, mock_query, mock_get_indexed, mock_extract, mock_upload, mock_upsert):
        """Uploaded documents have id=MD5."""
        from app.services.indexing_service import index_all_files_v2

        records = [_make_dlu_v2(md5="c8578af0e239aaeb7e4030b346430ac3", file_path="data/a.txt")]
        mock_query.return_value = records
        mock_get_indexed.return_value = set()
        mock_extract.return_value = "content"
        mock_upload.return_value = []

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        index_all_files_v2(db, search_client, config, force=False)

        # Verify the document passed to _upload_documents has id=MD5
        call_args = mock_upload.call_args
        docs = call_args[0][1]  # second positional arg is the documents list
        assert docs[0]["id"] == "c8578af0e239aaeb7e4030b346430ac3"


# ===========================================================================
# Test: index_single_file_v2
# ===========================================================================

class TestIndexSingleFileV2:
    """index_single_file_v2 handles single MD5 indexing."""

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._query_dlu_by_md5")
    def test_index_single_file_v2_success(self, mock_query, mock_extract, mock_upload, mock_upsert):
        """Single file indexed successfully -> 1 processed, 1 succeeded."""
        from app.services.indexing_service import index_single_file_v2

        record = _make_dlu_v2(md5="mymd5", file_path="data/target.txt")
        mock_query.return_value = record
        mock_extract.return_value = "some text"
        mock_upload.return_value = []

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_single_file_v2(db, search_client, config, "mymd5")

        assert result is not None
        assert result.files_processed == 1
        assert result.files_succeeded == 1
        assert result.files_failed == 0
        assert result.files_skipped == 0
        assert result.errors == []

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._query_dlu_by_md5")
    def test_index_single_file_v2_md5_not_found(self, mock_query, mock_extract, mock_upload, mock_upsert):
        """MD5 not found in DLU -> returns None (caller raises 404)."""
        from app.services.indexing_service import index_single_file_v2

        mock_query.return_value = None

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_single_file_v2(db, search_client, config, "nonexistent")

        assert result is None
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._query_dlu_by_md5")
    def test_index_single_file_v2_extraction_failure(self, mock_query, mock_extract, mock_upload, mock_upsert):
        """Extraction failure -> 1 processed, 0 succeeded, 1 failed."""
        from app.services.indexing_service import index_single_file_v2

        record = _make_dlu_v2(md5="bad_md5", file_path="data/corrupt.xlsx")
        mock_query.return_value = record
        mock_extract.return_value = None

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_single_file_v2(db, search_client, config, "bad_md5")

        assert result.files_processed == 1
        assert result.files_succeeded == 0
        assert result.files_failed == 1
        assert "bad_md5" in result.errors[0]
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._query_dlu_by_md5")
    def test_index_single_file_v2_unsupported_extension(self, mock_query, mock_extract, mock_upload, mock_upsert):
        """Unsupported extension -> 0 processed, 1 skipped."""
        from app.services.indexing_service import index_single_file_v2

        record = _make_dlu_v2(md5="pdf_md5", file_path="data/doc.pdf")
        mock_query.return_value = record

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        result = index_single_file_v2(db, search_client, config, "pdf_md5")

        assert result.files_processed == 0
        assert result.files_skipped == 1
        mock_extract.assert_not_called()
        mock_upload.assert_not_called()

    @patch("app.services.indexing_service._upsert_file_status")
    @patch("app.services.indexing_service._upload_documents")
    @patch("app.services.indexing_service.extract_text")
    @patch("app.services.indexing_service._query_dlu_by_md5")
    def test_index_single_file_v2_document_id_is_md5(self, mock_query, mock_extract, mock_upload, mock_upsert):
        """Single-file index document has id=MD5."""
        from app.services.indexing_service import index_single_file_v2

        md5 = "c8578af0e239aaeb7e4030b346430ac3"
        record = _make_dlu_v2(md5=md5, file_path="data/file.txt")
        mock_query.return_value = record
        mock_extract.return_value = "content"
        mock_upload.return_value = []

        db = MagicMock()
        config = _make_settings_v2()
        search_client = MagicMock()

        index_single_file_v2(db, search_client, config, md5)

        call_args = mock_upload.call_args
        docs = call_args[0][1]
        assert docs[0]["id"] == md5
