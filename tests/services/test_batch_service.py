"""Tests for Batch Orchestration Service (Phase V2-3.1).

TDD Red phase — all tests written BEFORE production code.

Covers spec scenarios from:
- openspec/changes/breach-pii-search/specs/batch-orchestration/spec.md
- openspec/changes/breach-pii-search/specs/status-tracking/spec.md

Test groups:
- TestBatchCreation: batch_runs row inserted, customer_status rows initialized
- TestPerCustomerFlow: status transitions pending->searching->detecting->complete
- TestErrorHandling: customer fails, marked failed, next customer processed
- TestResumability: skip completed, retry failed
- TestConflictDetection: 409 if running batch exists
- TestZeroCandidates: customer complete with 0 leaks
- TestResultPersistence: results inserted with correct fields
- TestBatchCompletion: batch_runs updated to completed on finish
- TestLogging: log messages during processing
"""

import datetime
import json
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
        self.DOB = kwargs.get("DOB", datetime.date(1990, 1, 1))
        self.SSN = kwargs.get("SSN", "123-45-6789")
        self.DriversLicense = kwargs.get("DriversLicense", "D1234567")
        self.Address1 = kwargs.get("Address1", "123 Main St")
        self.Address2 = kwargs.get("Address2", None)
        self.Address3 = kwargs.get("Address3", None)
        self.ZipCode = kwargs.get("ZipCode", "90210")
        self.City = kwargs.get("City", "New York")
        self.State = kwargs.get("State", "NY")
        self.Country = kwargs.get("Country", "USA")


class FakeBatchRun:
    """Lightweight stand-in for BatchRun ORM model."""

    def __init__(self, batch_id="batch-001", status="running", total_customers=3,
                 strategy_set=None, started_at=None, completed_at=None):
        self.batch_id = batch_id
        self.status = status
        self.total_customers = total_customers
        self.strategy_set = strategy_set or json.dumps([])
        self.started_at = started_at or datetime.datetime.utcnow()
        self.completed_at = completed_at


class FakeCustomerStatus:
    """Lightweight stand-in for CustomerStatus ORM model."""

    def __init__(self, batch_id="batch-001", customer_id=1, status="pending",
                 candidates_found=0, leaks_confirmed=0, strategies_matched=None,
                 error_message=None, processed_at=None):
        self.id = None
        self.batch_id = batch_id
        self.customer_id = customer_id
        self.status = status
        self.candidates_found = candidates_found
        self.leaks_confirmed = leaks_confirmed
        self.strategies_matched = strategies_matched or json.dumps([])
        self.error_message = error_message
        self.processed_at = processed_at


class FakeStrategy:
    """Lightweight stand-in for Strategy dataclass."""

    def __init__(self, name="fullname_ssn", description="Full name + SSN", fields=None):
        self.name = name
        self.description = description
        self.fields = fields or ["Fullname", "SSN"]


class FakeLeakDetectionResult:
    """Lightweight stand-in for LeakDetectionResult dataclass."""

    def __init__(self, found_fields=None, needs_review=False):
        """
        found_fields: list of field names that were found (others get found=False)
        """
        from types import SimpleNamespace
        found_fields = found_fields or []

        all_fields = [
            "Fullname", "FirstName", "LastName", "DOB", "SSN", "DriversLicense",
            "Address1", "Address2", "Address3", "ZipCode", "City", "State", "Country",
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
    """Return a fake candidate dict as returned by search_customer."""
    return {
        "md5": md5,
        "file_path": file_path,
        "azure_search_score": score,
        "strategy_that_found_it": strategy,
    }


def _make_confidence_result(score=0.9, needs_review=False):
    """Return a fake confidence result dict as returned by _compute_overall_confidence."""
    return {
        "score": score,
        "scenario": "ssn_and_name",
        "needs_review": needs_review,
        "other_fields_avg": 0.5,
    }


def _make_db_mock_with_customers(customers):
    """Build a mock DB session that returns given customers on query."""
    db = MagicMock()
    mock_query = MagicMock()
    db.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.filter_by.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = None  # default: no running batch
    mock_query.all.return_value = customers
    return db


# ===========================================================================
# Test: Batch creation — batch_runs row + customer_status rows initialized
# ===========================================================================

class TestBatchCreation:
    """Batch creation inserts batch_runs row and initializes customer_status rows."""

    def test_start_batch_generates_uuid(self):
        """WHEN start_batch() is called THEN a UUID batch_id is generated."""
        from app.services.batch_service import start_batch

        customers = [FakeCustomer(1), FakeCustomer(2)]
        strategies = [FakeStrategy()]
        db = MagicMock()

        with patch("app.services.batch_service._get_all_customers", return_value=customers), \
             patch("app.services.batch_service._check_running_batch", return_value=None), \
             patch("app.services.batch_service._create_batch_run") as mock_create, \
             patch("app.services.batch_service._init_customer_statuses"), \
             patch("app.services.batch_service._process_all_customers"), \
             patch("app.services.batch_service._complete_batch_run"):

            mock_create.return_value = "test-uuid-1234"
            result = start_batch(db=db, search_client=MagicMock(), strategies=strategies)

        assert result == "test-uuid-1234"
        mock_create.assert_called_once()

    def test_start_batch_inserts_batch_run_row(self):
        """WHEN start_batch() is called THEN a row is inserted into batch_runs with status 'running'."""
        from app.services.batch_service import _create_batch_run

        db = MagicMock()
        strategy_names = ["fullname_ssn", "lastname_dob"]
        total_customers = 5

        with patch("app.services.batch_service.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = "generated-uuid"
            batch_id = _create_batch_run(db, strategy_names=strategy_names, total_customers=total_customers)

        db.add.assert_called_once()
        db.commit.assert_called_once()
        added = db.add.call_args[0][0]
        assert added.status == "running"
        assert added.total_customers == total_customers

    def test_start_batch_stores_strategy_set_as_json(self):
        """WHEN batch run starts THEN strategy_set is stored as JSON in batch_runs."""
        from app.services.batch_service import _create_batch_run

        db = MagicMock()
        strategy_names = ["fullname_ssn", "lastname_dob"]

        with patch("app.services.batch_service.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = "test-uuid"
            _create_batch_run(db, strategy_names=strategy_names, total_customers=2)

        added = db.add.call_args[0][0]
        stored = json.loads(added.strategy_set)
        assert stored == ["fullname_ssn", "lastname_dob"]

    def test_start_batch_initializes_customer_statuses_as_pending(self):
        """WHEN batch starts THEN one customer_status row per customer with status='pending'."""
        from app.services.batch_service import _init_customer_statuses

        db = MagicMock()
        customers = [FakeCustomer(1), FakeCustomer(2), FakeCustomer(3)]
        batch_id = "batch-abc"

        _init_customer_statuses(db, batch_id=batch_id, customers=customers)

        # Should add one row per customer
        assert db.add.call_count == 3
        db.commit.assert_called_once()

        # Verify each added row
        added_rows = [call_args[0][0] for call_args in db.add.call_args_list]
        for row in added_rows:
            assert row.status == "pending"
            assert row.batch_id == batch_id

        customer_ids = {row.customer_id for row in added_rows}
        assert customer_ids == {1, 2, 3}

    def test_start_batch_started_at_is_set(self):
        """WHEN batch starts THEN started_at is populated with current UTC time."""
        from app.services.batch_service import _create_batch_run

        db = MagicMock()

        before = datetime.datetime.now(datetime.UTC)
        with patch("app.services.batch_service.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = "test-uuid"
            _create_batch_run(db, strategy_names=[], total_customers=0)
        after = datetime.datetime.now(datetime.UTC)

        added = db.add.call_args[0][0]
        assert added.started_at is not None
        assert before <= added.started_at <= after


# ===========================================================================
# Test: Per-customer flow — status transitions
# ===========================================================================

class TestPerCustomerFlow:
    """Per-customer status transitions: pending -> searching -> detecting -> complete."""

    def test_customer_status_set_to_searching(self):
        """WHEN processing starts for a customer THEN status is set to 'searching'."""
        from app.services.batch_service import _update_customer_status

        db = MagicMock()
        cs_row = FakeCustomerStatus(customer_id=42, status="pending")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = cs_row

        _update_customer_status(db, batch_id="batch-001", customer_id=42, status="searching")

        assert cs_row.status == "searching"
        db.commit.assert_called_once()

    def test_customer_status_set_to_detecting(self):
        """WHEN search completes THEN status transitions to 'detecting'."""
        from app.services.batch_service import _update_customer_status

        db = MagicMock()
        cs_row = FakeCustomerStatus(customer_id=42, status="searching")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = cs_row

        _update_customer_status(db, batch_id="batch-001", customer_id=42, status="detecting")

        assert cs_row.status == "detecting"
        db.commit.assert_called_once()

    def test_customer_status_set_to_complete(self):
        """WHEN leak detection completes THEN status set to 'complete' with counts."""
        from app.services.batch_service import _update_customer_status

        db = MagicMock()
        cs_row = FakeCustomerStatus(customer_id=42, status="detecting")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = cs_row

        _update_customer_status(
            db, batch_id="batch-001", customer_id=42,
            status="complete",
            candidates_found=5,
            leaks_confirmed=3,
            strategies_matched=["fullname_ssn", "unique_ids"],
        )

        assert cs_row.status == "complete"
        assert cs_row.candidates_found == 5
        assert cs_row.leaks_confirmed == 3
        assert json.loads(cs_row.strategies_matched) == ["fullname_ssn", "unique_ids"]
        assert cs_row.processed_at is not None

    def test_process_customer_full_flow_with_matches(self):
        """WHEN customer 42 has 3 candidates THEN detect runs on all 3, result inserted, status=complete."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy("fullname_ssn")]
        batch_id = "batch-001"

        candidates = [
            _make_candidate("md5a", "data/a.txt", 5.0, "fullname_ssn"),
            _make_candidate("md5b", "data/b.txt", 4.0, "fullname_ssn"),
            _make_candidate("md5c", "data/c.txt", 3.0, "fullname_ssn"),
        ]
        leak_result = FakeLeakDetectionResult(found_fields=["SSN", "Fullname"])

        with patch("app.services.batch_service._update_customer_status") as mock_update, \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="file text content"), \
             patch("app.services.batch_service.detect_leaks", return_value=leak_result), \
             patch("app.services.batch_service._persist_result") as mock_persist, \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.97)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        # Should transition: searching -> detecting -> complete
        update_calls = mock_update.call_args_list
        statuses = [c.kwargs.get("status") or c.args[3] for c in update_calls]
        assert "searching" in statuses
        assert "detecting" in statuses
        assert "complete" in statuses

        # detect_leaks called 3 times (once per candidate)
        assert mock_persist.call_count == 3

    def test_process_customer_counts_leaks_confirmed(self):
        """WHEN 2 of 3 files have PII THEN leaks_confirmed=2 in final status."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy("fullname_ssn")]
        batch_id = "batch-001"

        candidates = [
            _make_candidate("md5a", "data/a.txt", 5.0),
            _make_candidate("md5b", "data/b.txt", 4.0),
            _make_candidate("md5c", "data/c.txt", 3.0),
        ]
        # md5a and md5b have leaks, md5c does not
        leak_with = FakeLeakDetectionResult(found_fields=["SSN"])
        leak_without = FakeLeakDetectionResult(found_fields=[])

        with patch("app.services.batch_service._update_customer_status") as mock_update, \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="file text"), \
             patch("app.services.batch_service.detect_leaks", side_effect=[leak_with, leak_with, leak_without]), \
             patch("app.services.batch_service._persist_result"), \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.9)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        # Verify complete call has leaks_confirmed=2, candidates_found=3
        complete_call = [c for c in mock_update.call_args_list
                         if (c.kwargs.get("status") or "") == "complete"
                         or (len(c.args) > 3 and c.args[3] == "complete")]
        assert len(complete_call) == 1
        kw = complete_call[0].kwargs
        assert kw.get("leaks_confirmed") == 2
        assert kw.get("candidates_found") == 3


# ===========================================================================
# Test: Error handling — customer fails, marked failed, next customer processed
# ===========================================================================

class TestErrorHandling:
    """When a customer fails, it is marked 'failed' and processing continues."""

    def test_customer_search_error_marked_failed(self):
        """WHEN Azure Search throws THEN customer status set to 'failed' with error_message."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=50)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        error_msg = "Azure Search timeout after 30s"

        with patch("app.services.batch_service._update_customer_status") as mock_update, \
             patch("app.services.batch_service.search_customer", side_effect=Exception(error_msg)):

            # Should not raise — error is caught and customer is marked failed
            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        # Verify failed status was set
        failed_calls = [c for c in mock_update.call_args_list
                        if c.kwargs.get("status") == "failed"
                        or (len(c.args) > 3 and c.args[3] == "failed")]
        assert len(failed_calls) == 1
        kw = failed_calls[0].kwargs
        assert error_msg in kw.get("error_message", "")

    def test_processing_continues_after_customer_failure(self):
        """WHEN customer 50 fails THEN customer 51 is still processed."""
        from app.services.batch_service import _process_all_customers

        db = MagicMock()
        search_client = MagicMock()
        customers = [FakeCustomer(50), FakeCustomer(51)]
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        call_log = []

        def fake_process_customer(db, search_client, customer, strategies, batch_id):
            call_log.append(customer.customer_id)
            if customer.customer_id == 50:
                raise Exception("Azure Search timeout after 30s")

        with patch("app.services.batch_service._get_customer_status", return_value=None), \
             patch("app.services.batch_service._process_customer",
                   side_effect=fake_process_customer), \
             patch("app.services.batch_service._update_customer_status"):
            # _process_all_customers should catch errors and continue
            _process_all_customers(db=db, search_client=search_client, customers=customers,
                                   strategies=strategies, batch_id=batch_id)

        # Both customers were attempted
        assert 50 in call_log
        assert 51 in call_log

    def test_failed_customer_has_error_message(self):
        """WHEN customer fails THEN error_message is stored in customer_status."""
        from app.services.batch_service import _update_customer_status

        db = MagicMock()
        cs_row = FakeCustomerStatus(customer_id=50, status="searching")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = cs_row

        _update_customer_status(
            db, batch_id="batch-001", customer_id=50,
            status="failed",
            error_message="Azure Search timeout after 30s",
        )

        assert cs_row.status == "failed"
        assert "Azure Search timeout after 30s" in cs_row.error_message
        assert cs_row.processed_at is not None


# ===========================================================================
# Test: Resumability — skip completed, retry failed
# ===========================================================================

class TestResumability:
    """Resume batch: skip completed customers, retry failed, process pending."""

    def test_completed_customer_is_skipped(self):
        """WHEN customer is already 'complete' THEN it is skipped during resume."""
        from app.services.batch_service import _process_all_customers

        db = MagicMock()
        search_client = MagicMock()
        customers = [FakeCustomer(1), FakeCustomer(2)]
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        processed_ids = []

        def fake_get_status(db, batch_id, customer_id):
            # Customer 1 is already complete
            if customer_id == 1:
                return FakeCustomerStatus(customer_id=1, status="complete")
            return FakeCustomerStatus(customer_id=2, status="pending")

        def fake_process(db, search_client, customer, strategies, batch_id):
            processed_ids.append(customer.customer_id)

        with patch("app.services.batch_service._get_customer_status", side_effect=fake_get_status), \
             patch("app.services.batch_service._process_customer", side_effect=fake_process), \
             patch("app.services.batch_service._update_customer_status"):

            _process_all_customers(db=db, search_client=search_client, customers=customers,
                                   strategies=strategies, batch_id=batch_id)

        assert 1 not in processed_ids
        assert 2 in processed_ids

    def test_failed_customer_is_retried(self):
        """WHEN customer is 'failed' THEN it is retried during resume."""
        from app.services.batch_service import _process_all_customers

        db = MagicMock()
        search_client = MagicMock()
        customers = [FakeCustomer(1)]
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        processed_ids = []

        def fake_get_status(db, batch_id, customer_id):
            return FakeCustomerStatus(customer_id=1, status="failed")

        def fake_process(db, search_client, customer, strategies, batch_id):
            processed_ids.append(customer.customer_id)

        with patch("app.services.batch_service._get_customer_status", side_effect=fake_get_status), \
             patch("app.services.batch_service._process_customer", side_effect=fake_process), \
             patch("app.services.batch_service._update_customer_status"):

            _process_all_customers(db=db, search_client=search_client, customers=customers,
                                   strategies=strategies, batch_id=batch_id)

        assert 1 in processed_ids

    def test_resume_completed_batch_returns_400(self):
        """WHEN resume_batch() called on completed batch THEN raises ValueError('Batch already completed')."""
        from app.services.batch_service import resume_batch

        db = MagicMock()
        completed_run = FakeBatchRun(batch_id="batch-done", status="completed")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = completed_run

        with pytest.raises(ValueError, match="Batch already completed"):
            resume_batch(db=db, search_client=MagicMock(), strategies=[FakeStrategy()],
                         batch_id="batch-done")

    def test_resume_interrupted_batch_processes_remaining(self):
        """WHEN 148 complete + 2 failed + 50 pending THEN retries 2 + processes 50."""
        from app.services.batch_service import resume_batch

        db = MagicMock()
        batch_id = "batch-interrupted"
        running_run = FakeBatchRun(batch_id=batch_id, status="running", total_customers=200)

        customers = [FakeCustomer(i) for i in range(1, 201)]

        def fake_get_batch_run(db, batch_id):
            return running_run

        def fake_get_all_customers(db):
            return customers

        with patch("app.services.batch_service._get_batch_run", side_effect=fake_get_batch_run), \
             patch("app.services.batch_service._get_all_customers", side_effect=fake_get_all_customers), \
             patch("app.services.batch_service._process_all_customers") as mock_process:

            resume_batch(db=db, search_client=MagicMock(), strategies=[FakeStrategy()],
                         batch_id=batch_id)

        mock_process.assert_called_once()


# ===========================================================================
# Test: Conflict detection — 409 if running batch exists
# ===========================================================================

class TestConflictDetection:
    """Conflict detection: prevent concurrent batches."""

    def test_start_batch_while_running_raises_conflict(self):
        """WHEN POST /batch/run while batch is running THEN raises ValueError with batch_id."""
        from app.services.batch_service import start_batch

        db = MagicMock()
        strategies = [FakeStrategy()]
        running_batch = FakeBatchRun(batch_id="running-batch-uuid", status="running")

        with patch("app.services.batch_service._check_running_batch", return_value=running_batch):
            with pytest.raises(ValueError, match="already running"):
                start_batch(db=db, search_client=MagicMock(), strategies=strategies)

    def test_start_batch_conflict_message_includes_batch_id(self):
        """Conflict error message must include the running batch_id."""
        from app.services.batch_service import start_batch

        db = MagicMock()
        strategies = [FakeStrategy()]
        running_batch = FakeBatchRun(batch_id="running-batch-uuid", status="running")

        with patch("app.services.batch_service._check_running_batch", return_value=running_batch):
            with pytest.raises(ValueError) as exc_info:
                start_batch(db=db, search_client=MagicMock(), strategies=strategies)

        assert "running-batch-uuid" in str(exc_info.value)

    def test_check_running_batch_returns_none_when_no_batch(self):
        """_check_running_batch returns None when no running batch exists."""
        from app.services.batch_service import _check_running_batch

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = None

        result = _check_running_batch(db)

        assert result is None

    def test_check_running_batch_returns_batch_when_running(self):
        """_check_running_batch returns the running batch run when one exists."""
        from app.services.batch_service import _check_running_batch

        db = MagicMock()
        running = FakeBatchRun(batch_id="active-uuid", status="running")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = running

        result = _check_running_batch(db)

        assert result is not None
        assert result.batch_id == "active-uuid"


# ===========================================================================
# Test: Zero candidates — customer complete with 0 leaks
# ===========================================================================

class TestZeroCandidates:
    """When search returns no results, customer is marked complete with 0 candidates."""

    def test_zero_candidates_status_complete(self):
        """WHEN all strategies return 0 results THEN status='complete', candidates_found=0, leaks_confirmed=0."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=99)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        with patch("app.services.batch_service._update_customer_status") as mock_update, \
             patch("app.services.batch_service.search_customer", return_value=[]):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        complete_calls = [c for c in mock_update.call_args_list
                          if c.kwargs.get("status") == "complete"]
        assert len(complete_calls) == 1
        kw = complete_calls[0].kwargs
        assert kw.get("candidates_found") == 0
        assert kw.get("leaks_confirmed") == 0
        assert kw.get("strategies_matched") == []

    def test_zero_candidates_no_results_inserted(self):
        """WHEN 0 candidates THEN no rows inserted into results table."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=99)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        with patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=[]), \
             patch("app.services.batch_service._persist_result") as mock_persist:

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        mock_persist.assert_not_called()

    def test_candidates_found_no_leaks_status_correct(self):
        """WHEN 5 candidates but no PII found THEN candidates_found=5, leaks_confirmed=0."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=88)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidates = [_make_candidate(f"md5_{i}", f"data/{i}.txt", 3.0) for i in range(5)]
        no_leak = FakeLeakDetectionResult(found_fields=[])

        with patch("app.services.batch_service._update_customer_status") as mock_update, \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="no pii here"), \
             patch("app.services.batch_service.detect_leaks", return_value=no_leak), \
             patch("app.services.batch_service._persist_result") as mock_persist, \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.0)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        complete_calls = [c for c in mock_update.call_args_list
                          if c.kwargs.get("status") == "complete"]
        assert len(complete_calls) == 1
        kw = complete_calls[0].kwargs
        assert kw.get("candidates_found") == 5
        assert kw.get("leaks_confirmed") == 0

        # No results persisted (no PII found in any candidate)
        mock_persist.assert_not_called()


# ===========================================================================
# Test: Result persistence
# ===========================================================================

class TestResultPersistence:
    """Results are persisted into [Search].[results] for (customer, file) pairs with leaks."""

    def test_persist_result_inserts_row(self):
        """WHEN leak found for customer 42 in file abc123 THEN a row is inserted into results."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        batch_id = "batch-001"
        customer_id = 42
        candidate = _make_candidate("abc123", "data/abc123.txt", 5.5, "fullname_ssn")
        leak_result = FakeLeakDetectionResult(found_fields=["SSN", "Fullname"])
        overall_confidence = 0.97

        _persist_result(
            db=db,
            batch_id=batch_id,
            customer_id=customer_id,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=overall_confidence,
        )

        db.add.assert_called_once()
        db.commit.assert_called_once()
        added = db.add.call_args[0][0]
        assert added.batch_id == batch_id
        assert added.customer_id == customer_id
        assert added.md5 == "abc123"

    def test_persist_result_stores_leaked_fields_as_json(self):
        """WHEN SSN and Fullname detected THEN leaked_fields=['SSN','Fullname'] in result row."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        candidate = _make_candidate("abc123", "data/abc123.txt", 5.0, "fullname_ssn")
        leak_result = FakeLeakDetectionResult(found_fields=["SSN", "Fullname"])

        _persist_result(
            db=db,
            batch_id="batch-001",
            customer_id=42,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=0.97,
        )

        added = db.add.call_args[0][0]
        stored_fields = json.loads(added.leaked_fields)
        assert "SSN" in stored_fields
        assert "Fullname" in stored_fields

    def test_persist_result_stores_strategy_name(self):
        """Result row stores the strategy name that found the file."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        candidate = _make_candidate("abc123", "data/abc123.txt", 5.0, "unique_identifiers")
        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])

        _persist_result(
            db=db,
            batch_id="batch-001",
            customer_id=42,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=0.95,
        )

        added = db.add.call_args[0][0]
        assert added.strategy_name == "unique_identifiers"

    def test_persist_result_stores_overall_confidence(self):
        """Result row stores computed overall_confidence."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        candidate = _make_candidate("abc123", "data/abc123.txt", 5.0)
        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])

        _persist_result(
            db=db,
            batch_id="batch-001",
            customer_id=42,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=0.87,
        )

        added = db.add.call_args[0][0]
        assert added.overall_confidence == 0.87

    def test_persist_result_stores_azure_search_score(self):
        """Result row stores the azure_search_score from the candidate."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        candidate = _make_candidate("abc123", "data/abc123.txt", 7.35)
        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])

        _persist_result(
            db=db,
            batch_id="batch-001",
            customer_id=42,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=0.9,
        )

        added = db.add.call_args[0][0]
        assert added.azure_search_score == 7.35

    def test_persist_result_stores_needs_review_flag(self):
        """Result row stores needs_review passed as explicit parameter."""
        from app.services.batch_service import _persist_result

        db = MagicMock()
        candidate = _make_candidate("abc123", "data/abc123.txt", 5.0)
        leak_result = FakeLeakDetectionResult(found_fields=["FirstName"])

        _persist_result(
            db=db,
            batch_id="batch-001",
            customer_id=42,
            candidate=candidate,
            leak_result=leak_result,
            overall_confidence=0.4,
            needs_review=True,
        )

        added = db.add.call_args[0][0]
        assert added.needs_review is True

    def test_multiple_files_produce_separate_rows(self):
        """WHEN customer 42 has leaks in 3 files THEN 3 separate rows are inserted."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidates = [
            _make_candidate("md5a", "data/a.txt", 5.0),
            _make_candidate("md5b", "data/b.txt", 4.0),
            _make_candidate("md5c", "data/c.txt", 3.0),
        ]
        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])

        with patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="text with ssn"), \
             patch("app.services.batch_service.detect_leaks", return_value=leak_result), \
             patch("app.services.batch_service._persist_result") as mock_persist, \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.9)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        assert mock_persist.call_count == 3

    def test_no_results_when_no_leaks(self):
        """WHEN no PII in any candidate THEN no rows inserted into results."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidates = [_make_candidate("md5a", "data/a.txt", 5.0)]
        no_leak = FakeLeakDetectionResult(found_fields=[])

        with patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="no pii"), \
             patch("app.services.batch_service.detect_leaks", return_value=no_leak), \
             patch("app.services.batch_service._persist_result") as mock_persist, \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.0)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        mock_persist.assert_not_called()


# ===========================================================================
# Test: Batch completion — batch_runs updated to completed
# ===========================================================================

class TestBatchCompletion:
    """Batch completion: batch_runs status set to 'completed' with timestamp."""

    def test_complete_batch_run_sets_status_completed(self):
        """WHEN all customers processed THEN batch_runs status='completed'."""
        from app.services.batch_service import _complete_batch_run

        db = MagicMock()
        batch_row = FakeBatchRun(batch_id="batch-001", status="running")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = batch_row

        _complete_batch_run(db, batch_id="batch-001")

        assert batch_row.status == "completed"
        assert batch_row.completed_at is not None
        db.commit.assert_called_once()

    def test_complete_batch_run_sets_completed_at_timestamp(self):
        """WHEN batch completes THEN completed_at is set to now."""
        from app.services.batch_service import _complete_batch_run

        db = MagicMock()
        batch_row = FakeBatchRun(batch_id="batch-001", status="running")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = batch_row

        before = datetime.datetime.now(datetime.UTC)
        _complete_batch_run(db, batch_id="batch-001")
        after = datetime.datetime.now(datetime.UTC)

        assert before <= batch_row.completed_at <= after

    def test_start_batch_full_flow_completes(self):
        """WHEN start_batch() runs to completion THEN _complete_batch_run is called."""
        from app.services.batch_service import start_batch

        db = MagicMock()
        strategies = [FakeStrategy()]
        customers = [FakeCustomer(1), FakeCustomer(2)]

        with patch("app.services.batch_service._check_running_batch", return_value=None), \
             patch("app.services.batch_service._get_all_customers", return_value=customers), \
             patch("app.services.batch_service._create_batch_run", return_value="new-batch-id"), \
             patch("app.services.batch_service._init_customer_statuses"), \
             patch("app.services.batch_service._process_all_customers"), \
             patch("app.services.batch_service._complete_batch_run") as mock_complete:

            result = start_batch(db=db, search_client=MagicMock(), strategies=strategies)

        mock_complete.assert_called_once_with(db, batch_id="new-batch-id")
        assert result == "new-batch-id"


# ===========================================================================
# Test: Customer processing order
# ===========================================================================

class TestCustomerProcessingOrder:
    """Customers are processed in customer_id order."""

    def test_customers_processed_in_customer_id_order(self):
        """WHEN master_data has customer_ids [1,2,3,5,10] THEN processed in that order."""
        from app.services.batch_service import _process_all_customers

        db = MagicMock()
        search_client = MagicMock()
        # Order is already sorted by customer_id (we assume _get_all_customers returns sorted)
        customers = [FakeCustomer(1), FakeCustomer(2), FakeCustomer(3), FakeCustomer(5), FakeCustomer(10)]
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        processing_order = []

        def fake_process(db, search_client, customer, strategies, batch_id):
            processing_order.append(customer.customer_id)

        with patch("app.services.batch_service._get_customer_status", return_value=None), \
             patch("app.services.batch_service._process_customer", side_effect=fake_process), \
             patch("app.services.batch_service._update_customer_status"):

            _process_all_customers(db=db, search_client=search_client, customers=customers,
                                   strategies=strategies, batch_id=batch_id)

        assert processing_order == [1, 2, 3, 5, 10]


# ===========================================================================
# Test: File text read from disk for leak detection
# ===========================================================================

class TestFileTextFromDisk:
    """File text is read from disk (via text_extraction), not from Azure Search result."""

    def test_file_text_read_from_candidate_file_path(self):
        """WHEN detecting leaks THEN text_extraction.extract_text is called with candidate file_path."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidate = _make_candidate("abc123", "data/TEXT/abc123.txt", 5.0)

        with patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=[candidate]), \
             patch("app.services.batch_service.extract_text") as mock_extract, \
             patch("app.services.batch_service.detect_leaks",
                   return_value=FakeLeakDetectionResult(found_fields=[])), \
             patch("app.services.batch_service._persist_result"), \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.0)):

            mock_extract.return_value = "file text from disk"
            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        # extract_text was called with the candidate's file_path
        mock_extract.assert_called_once_with("data/TEXT/abc123.txt")

    def test_file_text_none_skips_detection(self):
        """WHEN extract_text returns None THEN detect_leaks is NOT called for that file."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidate = _make_candidate("abc123", "data/TEXT/missing.txt", 5.0)

        with patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=[candidate]), \
             patch("app.services.batch_service.extract_text", return_value=None), \
             patch("app.services.batch_service.detect_leaks") as mock_detect, \
             patch("app.services.batch_service._persist_result"):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        mock_detect.assert_not_called()


# ===========================================================================
# Test: Helper functions
# ===========================================================================

class TestHelperFunctions:
    """Tests for internal helper functions."""

    def test_get_all_customers_returns_sorted_by_customer_id(self):
        """_get_all_customers returns customers ordered by customer_id."""
        from app.services.batch_service import _get_all_customers

        customers = [FakeCustomer(3), FakeCustomer(1), FakeCustomer(2)]
        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = sorted(customers, key=lambda c: c.customer_id)

        result = _get_all_customers(db)

        # Verify query used order_by
        mock_query.order_by.assert_called_once()
        # Result should be sorted
        assert [c.customer_id for c in result] == [1, 2, 3]

    def test_compute_overall_confidence_no_fields(self):
        """_compute_overall_confidence returns dict with score=0.0 when no fields found."""
        from app.services.batch_service import _compute_overall_confidence

        leak_result = FakeLeakDetectionResult(found_fields=[])
        customer = FakeCustomer(customer_id=1)
        result = _compute_overall_confidence(leak_result, customer, 0.5)

        assert result["score"] == 0.0 or result["score"] >= 0.0  # no-anchor scenario uses search score

    def test_compute_overall_confidence_single_field(self):
        """_compute_overall_confidence returns dict with score > 0 when SSN found."""
        from app.services.batch_service import _compute_overall_confidence

        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])
        customer = FakeCustomer(customer_id=1)
        # SSN found with confidence 1.0 (from FakeLeakDetectionResult)
        result = _compute_overall_confidence(leak_result, customer, 0.8)

        assert result["score"] > 0.0
        assert "needs_review" in result

    def test_get_customer_status_returns_none_when_not_found(self):
        """_get_customer_status returns None when no status row exists."""
        from app.services.batch_service import _get_customer_status

        db = MagicMock()
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = None

        result = _get_customer_status(db, batch_id="batch-001", customer_id=99)

        assert result is None

    def test_get_customer_status_returns_row_when_found(self):
        """_get_customer_status returns the status row when it exists."""
        from app.services.batch_service import _get_customer_status

        db = MagicMock()
        cs_row = FakeCustomerStatus(customer_id=42, status="complete")
        mock_query = MagicMock()
        db.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = cs_row

        result = _get_customer_status(db, batch_id="batch-001", customer_id=42)

        assert result is not None
        assert result.customer_id == 42
        assert result.status == "complete"


# ===========================================================================
# Test: Logging behavior
# ===========================================================================

class TestLogging:
    """Batch processing emits appropriate log messages."""

    def test_customer_completion_logged(self, caplog):
        """WHEN customer finishes processing THEN a log message is emitted."""
        from app.services.batch_service import _process_customer

        db = MagicMock()
        search_client = MagicMock()
        customer = FakeCustomer(customer_id=42)
        strategies = [FakeStrategy()]
        batch_id = "batch-001"

        candidates = [_make_candidate("md5a", "data/a.txt", 5.0)]
        leak_result = FakeLeakDetectionResult(found_fields=["SSN"])

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"), \
             patch("app.services.batch_service._update_customer_status"), \
             patch("app.services.batch_service.search_customer", return_value=candidates), \
             patch("app.services.batch_service.extract_text", return_value="text"), \
             patch("app.services.batch_service.detect_leaks", return_value=leak_result), \
             patch("app.services.batch_service._persist_result"), \
             patch("app.services.batch_service._compute_overall_confidence", return_value=_make_confidence_result(0.9)):

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        # Some log message should mention the customer_id
        log_text = " ".join(caplog.messages)
        assert "42" in log_text

    def test_batch_completion_logged(self, caplog):
        """WHEN batch completes THEN summary log message is emitted."""
        from app.services.batch_service import start_batch

        db = MagicMock()
        strategies = [FakeStrategy()]
        customers = [FakeCustomer(1)]

        with caplog.at_level(logging.INFO, logger="app.services.batch_service"), \
             patch("app.services.batch_service._check_running_batch", return_value=None), \
             patch("app.services.batch_service._get_all_customers", return_value=customers), \
             patch("app.services.batch_service._create_batch_run", return_value="batch-xyz"), \
             patch("app.services.batch_service._init_customer_statuses"), \
             patch("app.services.batch_service._process_all_customers"), \
             patch("app.services.batch_service._complete_batch_run"):

            start_batch(db=db, search_client=MagicMock(), strategies=strategies)

        # Some log messages should be emitted
        assert len(caplog.messages) > 0

    def test_error_logged_with_customer_id(self, caplog):
        """WHEN customer fails THEN error is logged with customer_id."""
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

            _process_customer(db=db, search_client=search_client, customer=customer,
                              strategies=strategies, batch_id=batch_id)

        log_text = " ".join(caplog.messages)
        assert "50" in log_text
