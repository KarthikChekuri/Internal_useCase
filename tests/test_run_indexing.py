"""Tests for scripts/run_indexing.py — Phase 3.1.

The run_indexing script is a standalone trigger that wires up config, DB session,
search client, and calls index_all_files.  All external dependencies are mocked.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _make_index_response(processed=10, succeeded=10, failed=0, errors=None):
    """Return a fake IndexResponse-like object."""
    return SimpleNamespace(
        files_processed=processed,
        files_succeeded=succeeded,
        files_failed=failed,
        errors=errors or [],
    )


class TestRunIndexing:
    """run_indexing.py wires dependencies and calls index_all_files."""

    @patch("scripts.run_indexing.index_all_files")
    @patch("scripts.run_indexing.SearchClient")
    @patch("scripts.run_indexing.AzureKeyCredential")
    @patch("scripts.run_indexing.get_settings")
    @patch("scripts.run_indexing.get_session_factory")
    def test_main_calls_index_all_files(
        self, mock_session_factory, mock_get_settings, mock_cred,
        mock_search_client_cls, mock_index_all
    ):
        """main() should create a DB session, search client, and call index_all_files."""
        from scripts.run_indexing import main

        fake_settings = SimpleNamespace(
            DATABASE_URL="mssql+pyodbc://fake",
            AZURE_SEARCH_ENDPOINT="https://fake.search.windows.net",
            AZURE_SEARCH_KEY="fake-key",
            AZURE_SEARCH_INDEX="breach-file-index",
            FILE_BASE_PATH=r"C:\data",
            CASE_NAME="TestCase",
        )
        mock_get_settings.return_value = fake_settings

        fake_session = MagicMock()
        mock_factory = MagicMock()
        mock_factory.return_value = fake_session
        mock_session_factory.return_value = mock_factory

        mock_search_client = MagicMock()
        mock_search_client_cls.return_value = mock_search_client

        mock_index_all.return_value = _make_index_response()

        main()

        mock_index_all.assert_called_once()
        # First arg should be the db session, second the search client
        call_args = mock_index_all.call_args
        assert call_args[0][0] is fake_session  # db
        assert call_args[0][1] is mock_search_client  # search_client
        assert call_args[0][2] is fake_settings  # config

    @patch("scripts.run_indexing.index_all_files")
    @patch("scripts.run_indexing.SearchClient")
    @patch("scripts.run_indexing.AzureKeyCredential")
    @patch("scripts.run_indexing.get_settings")
    @patch("scripts.run_indexing.get_session_factory")
    def test_main_logs_result(
        self, mock_session_factory, mock_get_settings, mock_cred,
        mock_search_client_cls, mock_index_all
    ):
        """main() should return the IndexResponse from index_all_files."""
        from scripts.run_indexing import main

        fake_settings = SimpleNamespace(
            DATABASE_URL="mssql+pyodbc://fake",
            AZURE_SEARCH_ENDPOINT="https://fake.search.windows.net",
            AZURE_SEARCH_KEY="fake-key",
            AZURE_SEARCH_INDEX="breach-file-index",
            FILE_BASE_PATH=r"C:\data",
            CASE_NAME="TestCase",
        )
        mock_get_settings.return_value = fake_settings

        fake_session = MagicMock()
        mock_factory = MagicMock()
        mock_factory.return_value = fake_session
        mock_session_factory.return_value = mock_factory

        mock_search_client = MagicMock()
        mock_search_client_cls.return_value = mock_search_client

        expected_result = _make_index_response(processed=5, succeeded=4, failed=1,
                                               errors=["g1: bad"])
        mock_index_all.return_value = expected_result

        result = main()
        assert result.files_processed == 5
        assert result.files_succeeded == 4
        assert result.files_failed == 1

    @patch("scripts.run_indexing.index_all_files")
    @patch("scripts.run_indexing.SearchClient")
    @patch("scripts.run_indexing.AzureKeyCredential")
    @patch("scripts.run_indexing.get_settings")
    @patch("scripts.run_indexing.get_session_factory")
    def test_main_closes_session(
        self, mock_session_factory, mock_get_settings, mock_cred,
        mock_search_client_cls, mock_index_all
    ):
        """main() should close the DB session after indexing."""
        from scripts.run_indexing import main

        fake_settings = SimpleNamespace(
            DATABASE_URL="mssql+pyodbc://fake",
            AZURE_SEARCH_ENDPOINT="https://fake.search.windows.net",
            AZURE_SEARCH_KEY="fake-key",
            AZURE_SEARCH_INDEX="breach-file-index",
            FILE_BASE_PATH=r"C:\data",
            CASE_NAME="TestCase",
        )
        mock_get_settings.return_value = fake_settings

        fake_session = MagicMock()
        mock_factory = MagicMock()
        mock_factory.return_value = fake_session
        mock_session_factory.return_value = mock_factory

        mock_search_client_cls.return_value = MagicMock()
        mock_index_all.return_value = _make_index_response()

        main()

        fake_session.close.assert_called_once()
