"""Tests for app/cli.py — Phase V4-2.1.

TDD: all tests written before implementation.

Uses Click CliRunner to invoke CLI commands in-process.
All DB and Azure service calls are mocked — never hit real infrastructure.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from app.cli import main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def invoke(args, catch_exceptions=False, **kwargs):
    """Invoke the CLI with the given args list via CliRunner."""
    runner = CliRunner()
    return runner.invoke(main, args, catch_exceptions=catch_exceptions, **kwargs)


# ---------------------------------------------------------------------------
# 1. --help displays all subcommands
# ---------------------------------------------------------------------------

def test_help_shows_all_subcommands():
    """breach-search --help lists all 6 subcommands."""
    result = invoke(["--help"])
    assert result.exit_code == 0
    for cmd in ("generate", "seed", "index", "run", "status", "compare"):
        assert cmd in result.output


# ---------------------------------------------------------------------------
# 2. --verbose sets root logger to DEBUG
# ---------------------------------------------------------------------------

def test_verbose_sets_debug_logging():
    """breach-search --verbose run sets root logger to DEBUG before subcommand runs."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.load_strategies") as mock_ls, \
         patch("app.cli.start_batch") as mock_batch, \
         patch("app.cli.list_all_batches") as mock_list:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_ls.return_value = [MagicMock(name="s1")]
        mock_list.return_value = []  # no running batches
        mock_batch.return_value = "test-batch-id"

        result = invoke(["--verbose", "run"])

    assert result.exit_code == 0
    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG


# ---------------------------------------------------------------------------
# 3. generate command calls generate_simulated_data entry point
# ---------------------------------------------------------------------------

def test_generate_calls_generate_all():
    """breach-search generate calls scripts.generate_simulated_data.generate_all."""
    with patch("app.cli.generate_simulated_data_main") as mock_gen:
        result = invoke(["generate"])

    assert result.exit_code == 0
    mock_gen.assert_called_once()


# ---------------------------------------------------------------------------
# 4. seed command calls seed_database.main
# ---------------------------------------------------------------------------

def test_seed_calls_seed_database_main():
    """breach-search seed calls scripts.seed_database.main."""
    with patch("app.cli.seed_database_main") as mock_seed:
        result = invoke(["seed"])

    assert result.exit_code == 0
    mock_seed.assert_called_once()


# ---------------------------------------------------------------------------
# 5. index command (without --v3) calls V2 indexing pipeline
# ---------------------------------------------------------------------------

def test_index_v2_calls_index_all_files_v2():
    """breach-search index (no --v3) calls indexing_service.index_all_files_v2."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.create_index") as mock_create_idx, \
         patch("app.cli.index_all_files_v2") as mock_idx:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_idx.return_value = MagicMock(
            files_processed=1, files_succeeded=1, files_failed=0, files_skipped=0, errors=[]
        )

        result = invoke(["index"])

    assert result.exit_code == 0
    mock_idx.assert_called_once()
    # Should NOT call v3 create index
    mock_create_idx.assert_called_once()


# ---------------------------------------------------------------------------
# 6. index --v3 calls V3 indexing pipeline
# ---------------------------------------------------------------------------

def test_index_v3_calls_index_all_files_v3():
    """breach-search index --v3 calls indexing_service_v3.index_all_files_v3."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.create_v3_index") as mock_create_v3, \
         patch("app.cli.index_all_files_v3") as mock_idx_v3:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_idx_v3.return_value = MagicMock(
            files_processed=1, files_succeeded=1, files_failed=0, files_skipped=0, errors=[]
        )

        result = invoke(["index", "--v3"])

    assert result.exit_code == 0
    mock_idx_v3.assert_called_once()
    mock_create_v3.assert_called_once()


# ---------------------------------------------------------------------------
# 7. run command calls batch_service.start_batch with default strategies
# ---------------------------------------------------------------------------

def test_run_calls_start_batch_with_default_strategies():
    """breach-search run calls batch_service.start_batch with loaded strategies."""
    fake_strategies = [MagicMock()]

    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.load_strategies") as mock_ls, \
         patch("app.cli.list_all_batches") as mock_list, \
         patch("app.cli.start_batch") as mock_batch:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_ls.return_value = fake_strategies
        mock_list.return_value = []  # no running batch
        mock_batch.return_value = "batch-abc-123"

        result = invoke(["run"])

    assert result.exit_code == 0
    mock_batch.assert_called_once()
    assert "batch-abc-123" in result.output


# ---------------------------------------------------------------------------
# 8. run --strategies loads custom strategies file
# ---------------------------------------------------------------------------

def test_run_with_custom_strategies_loads_file():
    """breach-search run --strategies custom.yaml loads strategies from that file."""
    fake_strategies = [MagicMock()]

    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.load_strategies") as mock_ls, \
         patch("app.cli.list_all_batches") as mock_list, \
         patch("app.cli.start_batch") as mock_batch:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_ls.return_value = fake_strategies
        mock_list.return_value = []
        mock_batch.return_value = "batch-custom-999"

        result = invoke(["run", "--strategies", "custom.yaml"])

    assert result.exit_code == 0
    # load_strategies should have been called with the custom file path
    called_path = mock_ls.call_args[0][0]
    assert "custom.yaml" in called_path


# ---------------------------------------------------------------------------
# 9. run --v3 calls batch_service_v3.start_batch_v3
# ---------------------------------------------------------------------------

def test_run_v3_calls_start_batch_v3():
    """breach-search run --v3 calls batch_service_v3.start_batch_v3."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.list_all_batches") as mock_list, \
         patch("app.cli.start_batch_v3") as mock_batch_v3:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_list.return_value = []
        mock_batch_v3.return_value = "batch-v3-xyz"

        result = invoke(["run", "--v3"])

    assert result.exit_code == 0
    mock_batch_v3.assert_called_once()
    assert "batch-v3-xyz" in result.output


# ---------------------------------------------------------------------------
# 10. run when batch already running: prints error, exits code 1
# ---------------------------------------------------------------------------

def test_run_when_batch_already_running_exits_1():
    """breach-search run prints error and exits 1 if a batch is running."""
    running_batch = {"batch_id": "running-batch-001", "status": "running"}

    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.load_strategies") as mock_ls, \
         patch("app.cli.list_all_batches") as mock_list, \
         patch("app.cli.start_batch") as mock_batch:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_ls.return_value = [MagicMock()]
        mock_list.return_value = [running_batch]

        result = invoke(["run"])

    assert result.exit_code == 1
    assert "already running" in result.output.lower() or "already running" in (result.output + "").lower()
    mock_batch.assert_not_called()


# ---------------------------------------------------------------------------
# 11. status BATCH_ID calls get_batch_status, prints JSON
# ---------------------------------------------------------------------------

def test_status_prints_batch_json():
    """breach-search status BATCH_ID prints JSON with batch info."""
    fake_status = {
        "batch_id": "abc-123",
        "status": "completed",
        "started_at": "2025-01-01T00:00:00",
        "completed_at": "2025-01-01T01:00:00",
        "total_customers": 10,
        "completed_customers": 10,
        "failed_customers": 0,
    }

    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli.get_batch_status") as mock_status:

        mock_db.return_value = MagicMock()
        mock_status.return_value = fake_status

        result = invoke(["status", "abc-123"])

    assert result.exit_code == 0
    mock_status.assert_called_once()
    output_data = json.loads(result.output)
    assert output_data["batch_id"] == "abc-123"


# ---------------------------------------------------------------------------
# 12. status BATCH_ID --customers includes per-customer statuses
# ---------------------------------------------------------------------------

def test_status_with_customers_flag_includes_customer_statuses():
    """breach-search status BATCH_ID --customers includes customer_statuses in output."""
    fake_status = {
        "batch_id": "abc-123",
        "status": "completed",
        "started_at": "2025-01-01T00:00:00",
        "completed_at": "2025-01-01T01:00:00",
        "total_customers": 2,
        "completed_customers": 2,
        "failed_customers": 0,
    }
    fake_customers = [
        {"customer_id": 1, "status": "complete", "candidates_found": 3, "leaks_confirmed": 1},
        {"customer_id": 2, "status": "complete", "candidates_found": 5, "leaks_confirmed": 2},
    ]

    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli.get_batch_status") as mock_status, \
         patch("app.cli.get_customer_statuses") as mock_cust:

        mock_db.return_value = MagicMock()
        mock_status.return_value = fake_status
        mock_cust.return_value = fake_customers

        result = invoke(["status", "abc-123", "--customers"])

    assert result.exit_code == 0
    mock_cust.assert_called_once()
    output_data = json.loads(result.output)
    assert "customer_statuses" in output_data
    assert len(output_data["customer_statuses"]) == 2


# ---------------------------------------------------------------------------
# 13. status with non-existent batch: prints error, exits code 1
# ---------------------------------------------------------------------------

def test_status_nonexistent_batch_exits_1():
    """breach-search status nonexistent-id prints error and exits 1."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli.get_batch_status") as mock_status:

        mock_db.return_value = MagicMock()
        mock_status.return_value = None  # batch not found

        result = invoke(["status", "nonexistent-id"])

    assert result.exit_code == 1
    assert "not found" in result.output.lower()


# ---------------------------------------------------------------------------
# 14. compare V2_ID V3_ID calls comparison logic
# ---------------------------------------------------------------------------

def test_compare_calls_comparison_logic():
    """breach-search compare V2_ID V3_ID calls compare_v2_v3 logic."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli.compare_get_batch_results") as mock_get_results, \
         patch("app.cli.compare_results") as mock_compare, \
         patch("app.cli.format_comparison") as mock_format:

        mock_db.return_value = MagicMock()
        mock_get_results.return_value = []
        mock_compare.return_value = {}
        mock_format.return_value = "Comparison output"

        result = invoke(["compare", "v2-batch-id", "v3-batch-id"])

    assert result.exit_code == 0
    assert mock_get_results.call_count == 2  # called for V2 and V3
    mock_compare.assert_called_once()


# ---------------------------------------------------------------------------
# 15. DB connection failure: prints user-friendly error, exits code 1
# ---------------------------------------------------------------------------

def test_db_connection_failure_prints_friendly_error():
    """When DB connection fails, CLI prints user-friendly error and exits 1."""
    with patch("app.cli._build_db_session") as mock_db:
        mock_db.side_effect = Exception("ODBC connection refused")

        result = invoke(["run"])

    assert result.exit_code == 1
    assert "database" in result.output.lower()


# ---------------------------------------------------------------------------
# 16. Azure Search auth failure: prints user-friendly error, exits code 1
# ---------------------------------------------------------------------------

def test_azure_search_auth_failure_prints_friendly_error():
    """When Azure Search auth fails during index, CLI prints user-friendly error and exits 1."""
    with patch("app.cli._build_db_session") as mock_db, \
         patch("app.cli._build_search_client") as mock_sc, \
         patch("app.cli.create_index") as mock_create_idx:

        mock_db.return_value = MagicMock()
        mock_sc.return_value = MagicMock()
        mock_create_idx.side_effect = Exception("AuthorizationFailure: 403")

        result = invoke(["index"])

    assert result.exit_code == 1
    assert "azure search" in result.output.lower()


# ---------------------------------------------------------------------------
# 17. --verbose mode shows traceback on error
# ---------------------------------------------------------------------------

def test_verbose_mode_shows_traceback_on_error():
    """breach-search --verbose run shows traceback when a service error occurs."""
    with patch("app.cli._build_db_session") as mock_db:
        mock_db.side_effect = RuntimeError("Unexpected DB failure")

        result = invoke(["--verbose", "run"])

    assert result.exit_code == 1
    # In verbose mode, traceback should appear
    assert "Traceback" in result.output or "RuntimeError" in result.output


# ---------------------------------------------------------------------------
# 18. _build_db_session helper: loads settings, creates engine and session
# ---------------------------------------------------------------------------

def test_build_db_session_uses_settings_database_url():
    """_build_db_session loads Settings, creates engine from DATABASE_URL, returns session."""
    from app.cli import _build_db_session

    with patch("app.cli.Settings") as mock_settings_cls, \
         patch("app.cli.get_engine") as mock_get_engine, \
         patch("app.cli.get_session_factory") as mock_factory:

        mock_settings = MagicMock()
        mock_settings.DATABASE_URL = "mssql+pyodbc://test"
        mock_settings_cls.return_value = mock_settings

        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_session_factory = MagicMock()
        mock_session = MagicMock()
        mock_session_factory.return_value = mock_session
        mock_factory.return_value = mock_session_factory

        session = _build_db_session()

    mock_settings_cls.assert_called_once()
    mock_get_engine.assert_called_once_with(mock_settings.DATABASE_URL)
    mock_factory.assert_called_once_with(mock_engine)
    assert session == mock_session


# ---------------------------------------------------------------------------
# 19. _build_search_client helper: creates AzureKeyCredential and SearchClient
# ---------------------------------------------------------------------------

def test_build_search_client_v2():
    """_build_search_client(settings) creates SearchClient pointing at V2 index."""
    from app.cli import _build_search_client

    with patch("app.cli.AzureKeyCredential") as mock_cred, \
         patch("app.cli.SearchClient") as mock_sc_cls:

        mock_settings = MagicMock()
        mock_settings.AZURE_SEARCH_KEY = "key-abc"
        mock_settings.AZURE_SEARCH_ENDPOINT = "https://test.search.windows.net"
        mock_settings.AZURE_SEARCH_INDEX = "breach-file-index"
        mock_settings.AZURE_SEARCH_INDEX_V3 = "breach-file-index-v3"

        mock_credential = MagicMock()
        mock_cred.return_value = mock_credential

        client = _build_search_client(mock_settings, v3=False)

    mock_cred.assert_called_once_with("key-abc")
    mock_sc_cls.assert_called_once_with(
        endpoint="https://test.search.windows.net",
        index_name="breach-file-index",
        credential=mock_credential,
    )


def test_build_search_client_v3():
    """_build_search_client(settings, v3=True) creates SearchClient pointing at V3 index."""
    from app.cli import _build_search_client

    with patch("app.cli.AzureKeyCredential") as mock_cred, \
         patch("app.cli.SearchClient") as mock_sc_cls:

        mock_settings = MagicMock()
        mock_settings.AZURE_SEARCH_KEY = "key-abc"
        mock_settings.AZURE_SEARCH_ENDPOINT = "https://test.search.windows.net"
        mock_settings.AZURE_SEARCH_INDEX = "breach-file-index"
        mock_settings.AZURE_SEARCH_INDEX_V3 = "breach-file-index-v3"

        mock_credential = MagicMock()
        mock_cred.return_value = mock_credential

        client = _build_search_client(mock_settings, v3=True)

    mock_sc_cls.assert_called_once_with(
        endpoint="https://test.search.windows.net",
        index_name="breach-file-index-v3",
        credential=mock_credential,
    )
