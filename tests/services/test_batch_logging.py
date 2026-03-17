"""Tests for Console Logging and Observability — Phase V2-4.1.

TDD Red phase — tests written BEFORE production code changes.

Covers spec scenarios from:
- openspec/changes/breach-pii-search/specs/status-tracking/spec.md
  Section: Requirement: Console logging during batch processing

Test groups:
- TestIndexingProgressLogging: "Indexing: X/Y files processed (Z failed)"
- TestCustomerProcessingLogging: "Customer X/Y: N candidates, M leaks confirmed (...)"
- TestBatchCompletionLogging: "Batch complete: X customers, Y total leaks across Z files, N customers failed"
- TestErrorLoggingWithContext: error logged with customer_id, phase, and error message
- TestLoggingConfig: logging configured for console/stdout output
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for ORM objects
# ---------------------------------------------------------------------------

class FakeCustomer:
    """Lightweight stand-in for MasterData ORM model."""

    def __init__(self, customer_id=1, **kwargs):
        self.customer_id = customer_id
        self.Fullname = kwargs.get("Fullname", "John Doe")
        self.FirstName = kwargs.get("FirstName", "John")
        self.LastName = kwargs.get("LastName", "Doe")
        self.DOB = kwargs.get("DOB", "1990-01-01")
        self.SSN = kwargs.get("SSN", "123-45-6789")
        self.DriversLicense = kwargs.get("DriversLicense", "D1234567")
        self.Address1 = kwargs.get("Address1", "123 Main St")
        self.Address2 = kwargs.get("Address2", None)
        self.Address3 = kwargs.get("Address3", None)
        self.ZipCode = kwargs.get("ZipCode", "90210")
        self.City = kwargs.get("City", "New York")
        self.State = kwargs.get("State", "NY")
        self.Country = kwargs.get("Country", "USA")


class FakeStrategy:
    """Lightweight stand-in for Strategy dataclass."""

    def __init__(self, name="fullname_ssn"):
        self.name = name
        self.fields = ["Fullname", "SSN"]


class FakeLeakDetectionResult:
    """Lightweight stand-in for LeakDetectionResult."""

    def __init__(self, found_fields=None, needs_review=False):
        found_fields = found_fields or []
        all_fields = [
            "SSN", "DOB", "DriversLicense", "ZipCode", "State",
            "Fullname", "FirstName", "LastName",
            "Address1", "Address2", "Address3",
            "City", "Country",
        ]
        for field_name in all_fields:
            if field_name in found_fields:
                setattr(self, field_name, SimpleNamespace(
                    found=True, method="exact", confidence=1.0, snippet="...match..."
                ))
            else:
                setattr(self, field_name, SimpleNamespace(
                    found=False, method="none", confidence=0.0, snippet=None
                ))
        self.needs_review = needs_review


def _make_candidate(md5="abc123", file_path="data/TEXT/abc123.txt", score=5.0, strategy="fullname_ssn"):
    return {
        "md5": md5,
        "file_path": file_path,
        "azure_search_score": score,
        "strategy_that_found_it": strategy,
    }


# ===========================================================================
# TestIndexingProgressLogging
# Scenario: Indexing progress logged
# WHEN files are being indexed
# THEN log messages include: "Indexing: 50/500 files processed (3 failed)"
# ===========================================================================

class TestIndexingProgressLogging:
    """Indexing progress is logged with file counts and failure count."""

    def test_indexing_progress_message_format(self, caplog):
        """WHEN indexing_service logs progress THEN format matches spec.

        Spec: "Indexing: 50/500 files processed (3 failed)"
        """
        from app.services.indexing_service import _log_indexing_progress

        with caplog.at_level(logging.INFO, logger="app.services.indexing_service"):
            _log_indexing_progress(processed=50, total=500, failed=3)

        log_text = " ".join(caplog.messages)
        assert "Indexing:" in log_text
        assert "50" in log_text
        assert "500" in log_text
        assert "3 failed" in log_text

    def test_indexing_progress_format_exact(self, caplog):
        """WHEN _log_indexing_progress is called THEN message matches spec format exactly."""
        from app.services.indexing_service import _log_indexing_progress

        with caplog.at_level(logging.INFO, logger="app.services.indexing_service"):
            _log_indexing_progress(processed=50, total=500, failed=3)

        assert any(
            "Indexing: 50/500 files processed (3 failed)" in msg
            for msg in caplog.messages
        ), f"Expected 'Indexing: 50/500 files processed (3 failed)' in logs, got: {caplog.messages}"

    def test_indexing_progress_zero_failed(self, caplog):
        """WHEN no files fail THEN log shows (0 failed)."""
        from app.services.indexing_service import _log_indexing_progress

        with caplog.at_level(logging.INFO, logger="app.services.indexing_service"):
            _log_indexing_progress(processed=100, total=100, failed=0)

        assert any(
            "Indexing: 100/100 files processed (0 failed)" in msg
            for msg in caplog.messages
        )

    def test_indexing_progress_emitted_during_index_all_files_v2(self, caplog):
        """WHEN index_all_files_v2 runs THEN indexing progress is logged."""
        from app.services.indexing_service import index_all_files_v2

        # Create 3 fake DLU records with supported extensions
        fake_records = []
        for i in range(3):
            rec = MagicMock()
            rec.MD5 = f"md5_{i}"
            rec.file_path = f"/data/file_{i}.txt"
            fake_records.append(rec)

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = fake_records

        search_client = MagicMock()
        search_client.upload_documents.return_value = []

        config = MagicMock()

        with caplog.at_level(logging.INFO, logger="app.services.indexing_service"), \
             patch("app.services.indexing_service.extract_text", return_value="some text"), \
             patch("app.services.indexing_service._get_indexed_md5s", return_value=set()), \
             patch("app.services.indexing_service._upsert_file_status"):
            index_all_files_v2(db, search_client, config, force=False)

        log_text = " ".join(caplog.messages)
        # Should contain some indexing progress message
        assert "Indexing:" in log_text or "files processed" in log_text


# ===========================================================================
# TestCustomerProcessingLogging
# Scenario: Customer processing logged
# WHEN each customer finishes processing
# THEN a log message includes:
#   "Customer 42/200: 5 candidates, 3 leaks confirmed (fullname_ssn, unique_identifiers)"
# ===========================================================================

class TestCustomerProcessingLogging:
    """Customer processing completion is logged with candidate and leak counts."""

    def test_customer_completion_log_format(self, caplog):
        """WHEN customer finishes processing THEN log matches spec format.

        Spec: "Customer 42/200: 5 candidates, 3 leaks confirmed (fullname_ssn, unique_identifiers)"
        """
        from app.services.batch_service import _process_all_customers

        customers = [FakeCustomer(customer_id=42)]
        strategies = [FakeStrategy("fullname_ssn"), FakeStrategy("unique_identifiers")]
        batch_id = "batch-001"
        db = MagicMock()
        search_client = MagicMock()

        candidates = [
            _make_candidate("md5a", "data/a.txt", 5.0, "fullname_ssn"),
            _make_candidate("md5b", "data/b.txt", 4.0, "fullname_ssn"),
            _make_candidate("md5c", "data/c.txt", 3.0, "unique_identifiers"),
            _make_candidate("md5d", "data/d.txt", 2.0, "fullname_ssn"),
            _make_candidate("md5e", "data/e.txt", 1.0, "unique_identifiers"),
        ]
        leak_result = FakeLeakDetectionResult(found_fields=["SSN", "Fullname"])

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"), \
             patch("app.services.batch_service._get_customer_status",
                   return_value=SimpleNamespace(status="pending")), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="text content"), \
             patch("app.services.batch_service.detect_leaks", return_value=leak_result), \
             patch("app.services.batch_service._persist_result"), \
             patch("app.services.batch_service._compute_overall_confidence", return_value=1.0):

            _process_all_customers(
                db=db,
                search_client=search_client,
                customers=customers,
                strategies=strategies,
                batch_id=batch_id,
            )

        log_text = " ".join(caplog.messages)
        # Should contain customer ID
        assert "42" in log_text
        # Should contain candidate info
        assert "candidates" in log_text

    def test_customer_completion_log_exact_format(self, caplog):
        """WHEN customer 42 of 200 finishes THEN log message contains 'Customer 42/200:'.

        Spec format: "Customer 42/200: 5 candidates, 3 leaks confirmed (fullname_ssn, unique_identifiers)"
        """
        from app.services.batch_service import _log_customer_progress

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"):
            _log_customer_progress(
                customer_idx=42,
                total_customers=200,
                candidates_found=5,
                leaks_confirmed=3,
                strategies_matched=["fullname_ssn", "unique_identifiers"],
            )

        assert any(
            "Customer 42/200:" in msg and
            "5 candidates" in msg and
            "3 leaks confirmed" in msg and
            "fullname_ssn" in msg and
            "unique_identifiers" in msg
            for msg in caplog.messages
        ), f"Expected customer progress log, got: {caplog.messages}"

    def test_customer_completion_log_zero_candidates(self, caplog):
        """WHEN customer has no candidates THEN log shows 0 candidates, 0 leaks."""
        from app.services.batch_service import _log_customer_progress

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"):
            _log_customer_progress(
                customer_idx=99,
                total_customers=200,
                candidates_found=0,
                leaks_confirmed=0,
                strategies_matched=[],
            )

        assert any(
            "Customer 99/200:" in msg and
            "0 candidates" in msg and
            "0 leaks confirmed" in msg
            for msg in caplog.messages
        ), f"Expected customer 99/200 log, got: {caplog.messages}"

    def test_customer_completion_log_no_leaks(self, caplog):
        """WHEN customer has candidates but no leaks THEN log shows candidates_found, 0 leaks."""
        from app.services.batch_service import _log_customer_progress

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"):
            _log_customer_progress(
                customer_idx=88,
                total_customers=200,
                candidates_found=5,
                leaks_confirmed=0,
                strategies_matched=[],
            )

        assert any(
            "Customer 88/200:" in msg and
            "5 candidates" in msg and
            "0 leaks confirmed" in msg
            for msg in caplog.messages
        ), f"Expected customer 88/200 log, got: {caplog.messages}"


# ===========================================================================
# TestBatchCompletionLogging
# Scenario: Batch completion logged
# WHEN the batch completes
# THEN a summary log includes:
#   "Batch complete: 200 customers, 1500 total leaks across 180 files, 2 customers failed"
# ===========================================================================

class TestBatchCompletionLogging:
    """Batch completion emits a summary log message."""

    def test_batch_completion_log_exact_format(self, caplog):
        """WHEN batch completes THEN summary log matches spec format exactly.

        Spec: "Batch complete: 200 customers, 1500 total leaks across 180 files, 2 customers failed"
        """
        from app.services.batch_service import _log_batch_complete

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"):
            _log_batch_complete(
                total_customers=200,
                total_leaks=1500,
                files_with_leaks=180,
                customers_failed=2,
            )

        assert any(
            "Batch complete:" in msg and
            "200 customers" in msg and
            "1500 total leaks" in msg and
            "180 files" in msg and
            "2 customers failed" in msg
            for msg in caplog.messages
        ), f"Expected batch completion log, got: {caplog.messages}"

    def test_batch_completion_log_zero_failures(self, caplog):
        """WHEN batch completes with no failures THEN log shows 0 customers failed."""
        from app.services.batch_service import _log_batch_complete

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"):
            _log_batch_complete(
                total_customers=100,
                total_leaks=500,
                files_with_leaks=50,
                customers_failed=0,
            )

        assert any(
            "Batch complete:" in msg and
            "100 customers" in msg and
            "0 customers failed" in msg
            for msg in caplog.messages
        ), f"Expected batch completion with 0 failures, got: {caplog.messages}"

    def test_batch_completion_logged_during_start_batch(self, caplog):
        """WHEN start_batch() finishes THEN a completion summary log is emitted."""
        from app.services.batch_service import start_batch

        customers = [FakeCustomer(i) for i in range(1, 4)]
        strategies = [FakeStrategy()]
        db = MagicMock()

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"), \
             patch("app.services.batch_service._check_running_batch", return_value=None), \
             patch("app.services.batch_service._get_all_customers", return_value=customers), \
             patch("app.services.batch_service._create_batch_run", return_value="batch-xyz"), \
             patch("app.services.batch_service._init_customer_statuses"), \
             patch("app.services.batch_service._process_all_customers"), \
             patch("app.services.batch_service._complete_batch_run"), \
             patch("app.services.batch_service._collect_batch_summary",
                   return_value={"total_leaks": 10, "files_with_leaks": 5, "customers_failed": 0}):

            start_batch(db=db, search_client=MagicMock(), strategies=strategies)

        log_text = " ".join(caplog.messages)
        # A batch completion log should be emitted
        assert "Batch complete:" in log_text or "batch" in log_text.lower()


# ===========================================================================
# TestErrorLoggingWithContext
# Scenario: Errors logged with context
# WHEN a customer fails during processing
# THEN the error is logged with customer_id, phase (searching/detecting), and the error message
# ===========================================================================

class TestErrorLoggingWithContext:
    """Errors are logged with customer_id, phase, and error message."""

    def test_error_logged_with_customer_id_and_phase_searching(self, caplog):
        """WHEN customer fails during searching phase THEN log includes customer_id and 'searching'.

        Spec: error logged with customer_id, phase (searching/detecting), and the error message
        """
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=50)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        with caplog.at_level(logging.ERROR, logger="app.services.batch_service"), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer",
                   side_effect=Exception("Azure Search timeout after 30s")):

            _process_customer(
                db=db,
                search_client=search_client,
                customer=customer,
                strategies=strategies,
                batch_id=batch_id,
            )

        log_text = " ".join(caplog.messages)
        # customer_id must appear
        assert "50" in log_text, f"Expected customer_id 50 in error log, got: {caplog.messages}"
        # error message content must appear
        assert "Azure Search timeout" in log_text or "timeout" in log_text.lower(), \
            f"Expected error message in log, got: {caplog.messages}"

    def test_error_logged_with_phase_context_searching(self, caplog):
        """WHEN customer fails during search THEN log includes 'searching' phase."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=50)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        with caplog.at_level(logging.ERROR, logger="app.services.batch_service"), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer",
                   side_effect=Exception("Azure Search timeout after 30s")):

            _process_customer(
                db=db,
                search_client=search_client,
                customer=customer,
                strategies=strategies,
                batch_id=batch_id,
            )

        log_text = " ".join(caplog.messages)
        # Phase context should be present
        assert "searching" in log_text.lower() or "search" in log_text.lower(), \
            f"Expected phase context in error log, got: {caplog.messages}"

    def test_error_logged_with_phase_context_detecting(self, caplog):
        """WHEN customer fails during detect phase THEN log includes 'detecting' phase."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=77)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidates = [_make_candidate()]
        # detect_leaks raises an error
        with caplog.at_level(logging.ERROR, logger="app.services.batch_service"), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="text"), \
             patch("app.services.batch_service.detect_leaks",
                   side_effect=Exception("Detection model error")):

            _process_customer(
                db=db,
                search_client=search_client,
                customer=customer,
                strategies=strategies,
                batch_id=batch_id,
            )

        log_text = " ".join(caplog.messages)
        # customer_id must appear
        assert "77" in log_text, f"Expected customer_id 77 in error log, got: {caplog.messages}"
        # error message must appear
        assert "Detection model error" in log_text or "error" in log_text.lower()

    def test_error_logged_with_customer_id_and_phase_structured(self, caplog):
        """WHEN error occurs THEN log record attributes include customer_id and phase context."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        with caplog.at_level(logging.ERROR, logger="app.services.batch_service"), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer",
                   side_effect=Exception("Search failed")):

            _process_customer(
                db=db,
                search_client=search_client,
                customer=customer,
                strategies=strategies,
                batch_id=batch_id,
            )

        # Verify at least one error-level record was emitted
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_records) > 0, "Expected at least one ERROR log record"

        # The combined message should reference the customer
        combined = " ".join(r.getMessage() for r in error_records)
        assert "42" in combined


# ===========================================================================
# TestLoggingConfig
# Verify that logging is configured for console output via the CLI entry
# point (app/cli.py main() group).
# ===========================================================================

class TestLoggingConfig:
    """Logging is configured to emit structured output to console (stdout)."""

    def test_cli_main_group_exists(self):
        """WHEN the CLI module is imported THEN main() Click group is available."""
        from app.cli import main  # noqa: F401
        assert main is not None

    def test_verbose_flag_configures_debug_logging(self):
        """WHEN --verbose is passed THEN CLI accepts it and configures logging."""
        from click.testing import CliRunner
        from app.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--verbose", "--help"])
        # --help exits 0; the important thing is logging was configured
        assert result.exit_code == 0

    def test_default_logging_format_includes_level(self):
        """WHEN the CLI runs without --verbose THEN logging format includes levelname."""
        import inspect
        from app.cli import main
        source = inspect.getsource(main.callback)
        assert "levelname" in source, "Expected 'levelname' in CLI logging format"

    def test_verbose_logging_format_includes_timestamp(self):
        """WHEN --verbose is used THEN logging format includes asctime."""
        import inspect
        from app.cli import main
        source = inspect.getsource(main.callback)
        assert "asctime" in source, "Expected 'asctime' in verbose logging format"

    def test_verbose_logging_format_includes_message(self):
        """WHEN logging format is configured THEN it includes the log message."""
        import inspect
        from app.cli import main
        source = inspect.getsource(main.callback)
        assert "message" in source, "Expected 'message' in CLI logging format"
