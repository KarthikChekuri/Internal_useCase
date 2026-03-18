"""Tests for V3 Batch Service — Phase V3-3.2 (strategy-driven).

TDD Red phase — all tests written BEFORE production code.

Covers:
- TestBatchCreation: batch_runs row inserted with strategy_set from strategies.yaml,
  customer_status rows initialized as "pending", returns batch_id
- TestPerCustomerFlow: status transitions pending->searching->complete,
  candidates_found and leaks_confirmed updated
- TestResultPersistence: rows inserted into [Search].[results] with strategy_name
  from strategies.yaml (e.g. "fullname_ssn"), correct leaked_fields, match_details,
  overall_confidence, azure_search_score, needs_review
- TestNoResults: customer with no results -> status="complete", leaks_confirmed=0, no result rows
- TestAllNullPII: customer with all-null PII -> skipped with status="complete", leaks_confirmed=0
- TestErrorHandling: customer raises exception -> status="failed", error_message set, next customer continues
- TestBatchCompletion: batch_runs status updated to "completed", completed_at set
- TestV3Logging: [V3] prefix in batch start, customer progress, and batch complete log messages
- TestConcurrentBatchPrevention: if running batch exists, raise ValueError
"""

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from app.services.search_service import Strategy


# ---------------------------------------------------------------------------
# Fake strategies (matching strategies.yaml shape)
# ---------------------------------------------------------------------------

FAKE_STRATEGIES = [
    Strategy(name="fullname_ssn", description="Full name + SSN", fields=["Fullname", "SSN"]),
    Strategy(name="lastname_dob", description="Last name + DOB", fields=["LastName", "DOB"]),
    Strategy(name="unique_identifiers", description="SSN + DL", fields=["SSN", "DriversLicense"]),
]

FAKE_STRATEGY_NAMES = [s.name for s in FAKE_STRATEGIES]


def _patch_load_strategies():
    """Return a patch context for load_strategies that returns FAKE_STRATEGIES."""
    return patch(
        "app.services.batch_service_v3.load_strategies",
        return_value=FAKE_STRATEGIES,
    )


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for ORM objects (no real SQLAlchemy imports)
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


class FakeCustomerAllNull:
    """Stand-in for a customer with all PII fields None."""

    def __init__(self, customer_id=99):
        self.customer_id = customer_id
        self.Fullname = None
        self.FirstName = None
        self.LastName = None
        self.DOB = None
        self.SSN = None
        self.DriversLicense = None
        self.Address1 = None
        self.Address2 = None
        self.Address3 = None
        self.ZipCode = None
        self.City = None
        self.State = None
        self.Country = None


class FakeBatchRun:
    """Lightweight stand-in for BatchRun ORM model."""

    def __init__(self, batch_id="batch-v3-001", status="running", total_customers=1,
                 strategy_set=None, started_at=None, completed_at=None):
        self.batch_id = batch_id
        self.status = status
        self.total_customers = total_customers
        self.strategy_set = strategy_set or json.dumps(FAKE_STRATEGY_NAMES)
        self.started_at = started_at
        self.completed_at = completed_at


class FakeCustomerStatus:
    """Lightweight stand-in for CustomerStatus ORM model."""

    def __init__(self, batch_id="batch-v3-001", customer_id=1, status="pending",
                 candidates_found=0, leaks_confirmed=0, error_message=None):
        self.id = None
        self.batch_id = batch_id
        self.customer_id = customer_id
        self.status = status
        self.candidates_found = candidates_found
        self.leaks_confirmed = leaks_confirmed
        self.error_message = error_message
        self.processed_at = None


# ---------------------------------------------------------------------------
# Mock DB builder
# ---------------------------------------------------------------------------

def make_db(customers=None, running_batch=None, batch_run_to_update=None,
            customer_status_for_batch=None):
    """Build a MagicMock db session with controlled query returns."""
    db = MagicMock()
    db.add = MagicMock()
    db.commit = MagicMock()
    db.flush = MagicMock()

    if customers is None:
        customers = []

    # Track added objects so we can inspect them
    db._added_objects = []
    original_add = db.add

    def tracking_add(obj):
        db._added_objects.append(obj)

    db.add.side_effect = tracking_add

    # Chain: db.query(MasterData).order_by(...).all() -> customers
    master_query = MagicMock()
    master_query.order_by.return_value.all.return_value = customers

    # Chain: db.query(BatchRun).filter_by(status="running").first() -> running_batch
    batch_run_query = MagicMock()
    batch_run_query.filter_by.return_value.first.return_value = running_batch

    # Support db.query(BatchRun).filter_by(batch_id=...).first() -> batch_run_to_update
    def batch_run_filter_by(**kwargs):
        mock = MagicMock()
        if "status" in kwargs:
            mock.first.return_value = running_batch
        elif "batch_id" in kwargs:
            mock.first.return_value = batch_run_to_update
        else:
            mock.first.return_value = None
        return mock

    batch_run_query.filter_by.side_effect = batch_run_filter_by

    # Chain: db.query(CustomerStatus).filter_by(...).first() -> per-customer status
    cs_query = MagicMock()

    def cs_filter_by(**kwargs):
        mock = MagicMock()
        if customer_status_for_batch is not None:
            cid = kwargs.get("customer_id")
            bid = kwargs.get("batch_id")
            row = customer_status_for_batch.get((bid, cid))
            mock.first.return_value = row
        else:
            mock.first.return_value = None
        return mock

    cs_query.filter_by.side_effect = cs_filter_by

    # Route db.query(X) to the right mock based on repr of the arg
    def query_router(model):
        model_name = getattr(model, "__name__", None) or repr(model)
        if "MasterData" in model_name:
            return master_query
        if "BatchRun" in model_name:
            return batch_run_query
        if "CustomerStatus" in model_name:
            return cs_query
        return MagicMock()

    db.query.side_effect = query_router
    return db


# ---------------------------------------------------------------------------
# Sample V3 search results
# ---------------------------------------------------------------------------

SAMPLE_RESULT_WITH_LEAKS = {
    "md5": "abc123def456",
    "fields": {
        "SSN": {"found": True, "score": 15.5, "snippet": "***-**-6789"},
        "Fullname": {"found": True, "score": 10.2, "snippet": "John Doe"},
        "City": {"found": False},
    },
    "confidence": 0.82,
    "needs_review": False,
}

SAMPLE_RESULT_NO_LEAKS_ALL_FIELDS_FALSE = {
    "md5": "deadbeef0000",
    "fields": {
        "SSN": {"found": False},
        "Fullname": {"found": False},
    },
    "confidence": 0.0,
    "needs_review": True,
}


# ---------------------------------------------------------------------------
# TestBatchCreation
# ---------------------------------------------------------------------------

class TestBatchCreation:
    """Test that start_batch_v3 creates the correct DB rows."""

    def test_batch_run_inserted_with_v3_strategy_set(self):
        """BatchRun row must be inserted with strategy_set from strategies.yaml."""
        customers = [FakeCustomer(customer_id=1)]
        batch_run_obj = FakeBatchRun()
        db = make_db(customers=customers, running_batch=None,
                     batch_run_to_update=batch_run_obj)
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
        ) as MockBatchRun, patch(
            "app.services.batch_service_v3.CustomerStatus",
        ) as MockCustomerStatus:
            mock_batch_instance = MagicMock()
            MockBatchRun.return_value = mock_batch_instance

            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        # BatchRun constructor must have been called
        assert MockBatchRun.called
        _, kwargs = MockBatchRun.call_args
        assert kwargs.get("status") == "running"
        strategy_set = kwargs.get("strategy_set")
        assert strategy_set is not None
        assert json.loads(strategy_set) == FAKE_STRATEGY_NAMES

    def test_batch_run_inserted_with_total_customers(self):
        """BatchRun row must have total_customers matching actual customer count."""
        customers = [FakeCustomer(customer_id=1), FakeCustomer(customer_id=2)]
        batch_run_obj = FakeBatchRun()
        db = make_db(customers=customers, running_batch=None,
                     batch_run_to_update=batch_run_obj)
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
        ) as MockBatchRun, patch(
            "app.services.batch_service_v3.CustomerStatus",
        ):
            mock_batch_instance = MagicMock()
            MockBatchRun.return_value = mock_batch_instance

            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        _, kwargs = MockBatchRun.call_args
        assert kwargs.get("total_customers") == 2

    def test_customer_status_rows_initialized_as_pending(self):
        """CustomerStatus rows must be created with status='pending' for each customer."""
        customers = [FakeCustomer(customer_id=1), FakeCustomer(customer_id=2)]
        batch_run_obj = FakeBatchRun()
        db = make_db(customers=customers, running_batch=None,
                     batch_run_to_update=batch_run_obj)
        search_client = MagicMock()

        created_statuses = []

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
        ) as MockCustomerStatus:
            def capture_status(**kwargs):
                obj = FakeCustomerStatus(**kwargs)
                created_statuses.append(obj)
                return obj

            MockCustomerStatus.side_effect = capture_status

            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        pending_statuses = [s for s in created_statuses if s.status == "pending"]
        assert len(pending_statuses) == 2
        customer_ids = {s.customer_id for s in pending_statuses}
        assert customer_ids == {1, 2}

    def test_start_batch_v3_returns_batch_id_string(self):
        """start_batch_v3 must return a non-empty string batch_id."""
        customers = [FakeCustomer(customer_id=1)]
        batch_run_obj = FakeBatchRun()
        db = make_db(customers=customers, running_batch=None,
                     batch_run_to_update=batch_run_obj)
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            result = start_batch_v3(db, search_client)

        assert isinstance(result, str)
        assert len(result) > 0

    def test_db_add_and_commit_called_for_batch_run(self):
        """db.add() and db.commit() must be called at least once."""
        customers = [FakeCustomer(customer_id=1)]
        batch_run_obj = FakeBatchRun()
        db = make_db(customers=customers, running_batch=None,
                     batch_run_to_update=batch_run_obj)
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        assert db.add.call_count >= 1
        assert db.commit.call_count >= 1


# ---------------------------------------------------------------------------
# TestPerCustomerFlow
# ---------------------------------------------------------------------------

class TestPerCustomerFlow:
    """Test per-customer status transitions."""

    def test_customer_status_transitions_to_searching_then_complete(self):
        """Status must move from pending -> searching -> complete."""
        customer = FakeCustomer(customer_id=1)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="batch-001", customer_id=1, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("batch-001", 1): cs_row},
        )
        search_client = MagicMock()

        status_transitions = []

        def track_status_update(obj):
            if hasattr(obj, "status") and hasattr(obj, "customer_id"):
                status_transitions.append(obj.status)
            db._added_objects.append(obj)

        db.add.side_effect = track_status_update

        result_with_leak = {
            "md5": "abc123",
            "fields": {
                "SSN": {"found": True, "score": 10.0, "snippet": "snip"},
            },
            "confidence": 0.75,
            "needs_review": False,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_with_leak],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
        ) as MockBatchRun, patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
            return_value=MagicMock(),
        ):
            mock_batch = MagicMock()
            mock_batch.batch_id = "batch-001"
            MockBatchRun.return_value = mock_batch

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "batch-001", FAKE_STRATEGIES, 1, 1)

        # The cs_row status should have been updated to "searching" then "complete"
        assert cs_row.status == "complete"

    def test_customer_candidates_found_and_leaks_confirmed_updated(self):
        """candidates_found and leaks_confirmed must be set when results exist."""
        customer = FakeCustomer(customer_id=5)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-001", customer_id=5, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-001", 5): cs_row},
        )
        search_client = MagicMock()

        results = [
            {
                "md5": "file_a",
                "fields": {"SSN": {"found": True, "score": 12.0, "snippet": "s1"}},
                "confidence": 0.80,
                "needs_review": False,
            },
            {
                "md5": "file_b",
                "fields": {"Fullname": {"found": True, "score": 8.0, "snippet": "s2"}},
                "confidence": 0.60,
                "needs_review": False,
            },
        ]

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=results,
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-001", FAKE_STRATEGIES, 1, 1)

        assert cs_row.leaks_confirmed == 2
        assert cs_row.candidates_found == 2

    def test_searching_status_set_before_search_call(self):
        """Customer status must be set to 'searching' before search_customer_strategy_v3 is called."""
        customer = FakeCustomer(customer_id=10)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-searching", customer_id=10, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-searching", 10): cs_row},
        )
        search_client = MagicMock()
        search_called_with_status = []

        def check_status_before_search(sc, cust, fields):
            # At the moment search is called, status should be "searching"
            search_called_with_status.append(cs_row.status)
            return []

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            side_effect=check_status_before_search,
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-searching", FAKE_STRATEGIES, 1, 1)

        assert all(s == "searching" for s in search_called_with_status)
        assert len(search_called_with_status) == len(FAKE_STRATEGIES)


# ---------------------------------------------------------------------------
# TestResultPersistence
# ---------------------------------------------------------------------------

class TestResultPersistence:
    """Test that result rows are inserted with the correct fields."""

    def test_result_row_inserted_with_v3_azure_only_strategy(self):
        """SearchResult rows must have strategy_name from strategies.yaml (e.g. "fullname_ssn")."""
        customer = FakeCustomer(customer_id=7)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-persist", customer_id=7, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-persist", 7): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash001",
            "fields": {
                "SSN": {"found": True, "score": 14.0, "snippet": "snip1"},
                "City": {"found": False},
            },
            "confidence": 0.77,
            "needs_review": False,
        }

        # Only the first strategy returns a hit; others return empty so the
        # md5 is attributed to a single strategy.
        call_count = {"n": 0}

        def _search_side_effect(*args, **kwargs):
            call_count["n"] += 1
            return [result_dict] if call_count["n"] == 1 else []

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            side_effect=_search_side_effect,
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-persist", FAKE_STRATEGIES, 1, 1)

        assert len(captured_results) >= 1
        row = captured_results[0]
        assert json.loads(row.strategy_name) == ["fullname_ssn"]

    def test_result_row_leaked_fields_contains_only_found_fields(self):
        """leaked_fields JSON must only include fields where found=True."""
        customer = FakeCustomer(customer_id=8)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-lf", customer_id=8, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-lf", 8): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash002",
            "fields": {
                "SSN": {"found": True, "score": 14.0, "snippet": "snip"},
                "Fullname": {"found": True, "score": 9.0, "snippet": "name"},
                "City": {"found": False},
                "State": {"found": False},
            },
            "confidence": 0.80,
            "needs_review": False,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-lf", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        leaked = json.loads(row.leaked_fields)
        assert "SSN" in leaked
        assert "Fullname" in leaked
        assert "City" not in leaked
        assert "State" not in leaked

    def test_result_row_match_details_contains_all_fields(self):
        """match_details JSON must contain all fields from the result dict."""
        customer = FakeCustomer(customer_id=9)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-md", customer_id=9, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-md", 9): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash003",
            "fields": {
                "SSN": {"found": True, "score": 11.0, "snippet": "snip"},
                "DOB": {"found": False},
            },
            "confidence": 0.70,
            "needs_review": True,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-md", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        details = json.loads(row.match_details)
        # All fields from result["fields"] should be in match_details
        assert "SSN" in details
        assert "DOB" in details

    def test_result_row_overall_confidence_set(self):
        """overall_confidence must be set from result dict."""
        customer = FakeCustomer(customer_id=11)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-conf", customer_id=11, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-conf", 11): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash004",
            "fields": {
                "SSN": {"found": True, "score": 20.0, "snippet": "snip"},
            },
            "confidence": 0.91,
            "needs_review": False,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-conf", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        assert row.overall_confidence == pytest.approx(0.91, abs=1e-6)

    def test_result_row_azure_search_score_is_max_found_field_score(self):
        """azure_search_score must be the highest score among found fields."""
        customer = FakeCustomer(customer_id=12)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-score", customer_id=12, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-score", 12): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash005",
            "fields": {
                "SSN": {"found": True, "score": 15.0, "snippet": "snip"},
                "Fullname": {"found": True, "score": 9.5, "snippet": "name"},
                "City": {"found": False},
            },
            "confidence": 0.75,
            "needs_review": False,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-score", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        # max score among found fields: SSN=15.0 > Fullname=9.5
        assert row.azure_search_score == pytest.approx(15.0, abs=1e-6)

    def test_result_row_needs_review_flag_preserved(self):
        """needs_review flag must be copied from the result dict."""
        customer = FakeCustomer(customer_id=13)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-review", customer_id=13, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-review", 13): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hash006",
            "fields": {
                "FirstName": {"found": True, "score": 6.0, "snippet": "John"},
            },
            "confidence": 0.30,
            "needs_review": True,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-review", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        assert row.needs_review is True

    def test_result_row_correct_batch_id_and_customer_id(self):
        """Result rows must have the correct batch_id and customer_id."""
        customer = FakeCustomer(customer_id=20)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="batch-xyz", customer_id=20, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("batch-xyz", 20): cs_row},
        )
        search_client = MagicMock()

        captured_results = []

        result_dict = {
            "md5": "md5hashABC",
            "fields": {
                "SSN": {"found": True, "score": 10.0, "snippet": "snip"},
            },
            "confidence": 0.70,
            "needs_review": False,
        }

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[result_dict],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture_result(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured_results.append(obj)
                return obj

            MockSearchResult.side_effect = capture_result

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "batch-xyz", FAKE_STRATEGIES, 1, 1)

        row = captured_results[0]
        assert row.batch_id == "batch-xyz"
        assert row.customer_id == 20
        assert row.md5 == "md5hashABC"


# ---------------------------------------------------------------------------
# TestNoResults
# ---------------------------------------------------------------------------

class TestNoResults:
    """Test that customers with no search results are handled correctly."""

    def test_customer_with_no_results_status_complete_leaks_zero(self):
        """Customer with no search results must end with status='complete', leaks_confirmed=0."""
        customer = FakeCustomer(customer_id=30)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-nores", customer_id=30, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-nores", 30): cs_row},
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-nores", FAKE_STRATEGIES, 1, 1)

        # No result rows should be inserted
        assert not MockSearchResult.called
        # Status should be complete
        assert cs_row.status == "complete"
        assert cs_row.leaks_confirmed == 0

    def test_customer_with_no_results_no_search_result_rows_inserted(self):
        """When search returns empty list, no SearchResult rows must be added."""
        customer = FakeCustomer(customer_id=31)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-nores2", customer_id=31, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-nores2", 31): cs_row},
        )
        search_client = MagicMock()

        inserted_results = []

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            MockSearchResult.side_effect = lambda **kw: inserted_results.append(kw) or MagicMock()

            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-nores2", FAKE_STRATEGIES, 1, 1)

        assert len(inserted_results) == 0


# ---------------------------------------------------------------------------
# TestAllNullPII
# ---------------------------------------------------------------------------

class TestAllNullPII:
    """Test that customers with all-null PII fields are handled gracefully."""

    def test_all_null_pii_customer_skipped_no_search_call(self):
        """If all PII fields are None, search_customer_strategy_v3 should return [] (no real queries)."""
        customer = FakeCustomerAllNull(customer_id=50)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-null", customer_id=50, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-null", 50): cs_row},
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ) as mock_search, _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ):
            from app.services.batch_service_v3 import _process_customer_v3
            _process_customer_v3(db, search_client, customer, "b-null", FAKE_STRATEGIES, 1, 1)

        # Status should be complete, leaks = 0
        assert cs_row.status == "complete"
        assert cs_row.leaks_confirmed == 0

    def test_all_null_pii_start_batch_v3_completes(self):
        """start_batch_v3 with all-null-PII customers must complete without error."""
        customer = FakeCustomerAllNull(customer_id=51)
        batch_run_obj = FakeBatchRun()
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            batch_id = start_batch_v3(db, search_client)

        assert isinstance(batch_id, str)


# ---------------------------------------------------------------------------
# TestErrorHandling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    """Test that per-customer errors are contained and processing continues."""

    def test_customer_exception_sets_status_to_failed_with_message(self):
        """When search_customer_strategy_v3 raises, status must be 'failed' with error_message."""
        customer = FakeCustomer(customer_id=60)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-err", customer_id=60, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-err", 60): cs_row},
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            side_effect=RuntimeError("Azure connection failed"),
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ):
            from app.services.batch_service_v3 import _process_customer_v3
            # Should not raise — error is captured internally
            _process_customer_v3(db, search_client, customer, "b-err", FAKE_STRATEGIES, 1, 1)

        assert cs_row.status == "failed"
        assert "Azure connection failed" in (cs_row.error_message or "")

    def test_error_in_one_customer_does_not_stop_next_customer(self):
        """Exception for customer 1 must not prevent customer 2 from being processed."""
        customers = [FakeCustomer(customer_id=70), FakeCustomer(customer_id=71)]

        # We need a batch_run_obj that will be returned when start_batch_v3 creates BatchRun().
        # The service will set batch_run.status = "completed" on this same object at the end.
        batch_run_obj = FakeBatchRun(status="running")

        cs_row_70 = FakeCustomerStatus(customer_id=70, status="pending")
        cs_row_71 = FakeCustomerStatus(customer_id=71, status="pending")

        # Build a db whose CustomerStatus query matches on customer_id regardless of batch_id
        db = MagicMock()
        db.add = MagicMock()
        db.commit = MagicMock()
        db.flush = MagicMock()

        def cs_filter_any(**kwargs):
            mock = MagicMock()
            cid = kwargs.get("customer_id")
            if cid == 70:
                mock.first.return_value = cs_row_70
            elif cid == 71:
                mock.first.return_value = cs_row_71
            else:
                mock.first.return_value = None
            return mock

        cs_query_mock = MagicMock()
        cs_query_mock.filter_by.side_effect = cs_filter_any

        master_query = MagicMock()
        master_query.order_by.return_value.all.return_value = customers

        def query_router(model):
            model_name = getattr(model, "__name__", None) or repr(model)
            if "MasterData" in model_name:
                return master_query
            if "BatchRun" in model_name:
                # no running batch; filter_by(batch_id=...) returns batch_run_obj
                m = MagicMock()
                def brf(**kw):
                    inner = MagicMock()
                    inner.first.return_value = None if "status" in kw else batch_run_obj
                    return inner
                m.filter_by.side_effect = brf
                return m
            if "CustomerStatus" in model_name:
                return cs_query_mock
            return MagicMock()

        db.query.side_effect = query_router

        search_client = MagicMock()
        call_count = {"n": 0}

        def failing_first(sc, cust, fields):
            call_count["n"] += 1
            if cust.customer_id == 70:
                raise RuntimeError("fail for 70")
            return []

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            side_effect=failing_first,
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=batch_run_obj,
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.SearchResult",
        ):
            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        # Both customers must have been attempted (customer 70 fails on 1st strategy,
        # customer 71 succeeds for all 3 strategies → 1 + 3 = 4 calls)
        assert call_count["n"] >= 2  # at least both customers were called
        assert cs_row_70.status == "failed"
        assert cs_row_71.status == "complete"


# ---------------------------------------------------------------------------
# TestBatchCompletion
# ---------------------------------------------------------------------------

class TestBatchCompletion:
    """Test that batch_runs is updated to 'completed' when all customers are processed."""

    def test_batch_run_status_updated_to_completed(self):
        """After all customers, batch_run.status must be set to 'completed'.

        We patch BatchRun so the constructor returns batch_run_obj directly.
        start_batch_v3 mutates batch_run.status = "completed", so we verify
        that same object was mutated.
        """
        customers = [FakeCustomer(customer_id=80)]
        batch_run_obj = FakeBatchRun(batch_id="b-done", status="running")
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=batch_run_obj,  # constructor returns the same obj we inspect
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        assert batch_run_obj.status == "completed"

    def test_batch_run_completed_at_is_set(self):
        """After all customers, batch_run.completed_at must be a non-None datetime."""
        customers = [FakeCustomer(customer_id=81)]
        batch_run_obj = FakeBatchRun(batch_id="b-done2", status="running",
                                     completed_at=None)
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=batch_run_obj,  # constructor returns the same obj we inspect
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            start_batch_v3(db, search_client)

        assert batch_run_obj.completed_at is not None


# ---------------------------------------------------------------------------
# TestV3Logging
# ---------------------------------------------------------------------------

class TestV3Logging:
    """Test that log messages contain the [V3] prefix."""

    def test_batch_start_log_contains_v3_prefix(self, caplog):
        """Batch start log message must contain '[V3]'."""
        customers = [FakeCustomer(customer_id=90)]
        batch_run_obj = FakeBatchRun()
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            with caplog.at_level(logging.INFO, logger="app.services.batch_service_v3"):
                from app.services.batch_service_v3 import start_batch_v3
                start_batch_v3(db, search_client)

        v3_messages = [r.message for r in caplog.records if "[V3]" in r.message]
        assert len(v3_messages) >= 1, "Expected at least one log message with [V3] prefix"

    def test_customer_progress_log_contains_v3_prefix(self, caplog):
        """Per-customer progress log must contain '[V3]'."""
        customer = FakeCustomer(customer_id=91)
        batch_run_obj = FakeBatchRun()
        cs_row = FakeCustomerStatus(batch_id="b-log", customer_id=91, status="pending")
        db = make_db(
            customers=[customer],
            running_batch=None,
            batch_run_to_update=batch_run_obj,
            customer_status_for_batch={("b-log", 91): cs_row},
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            with caplog.at_level(logging.INFO, logger="app.services.batch_service_v3"):
                from app.services.batch_service_v3 import _process_customer_v3
                _process_customer_v3(db, search_client, customer, "b-log", FAKE_STRATEGIES, 1, 1)

        v3_messages = [r.message for r in caplog.records if "[V3]" in r.message]
        assert len(v3_messages) >= 1, "Expected at least one [V3] log during customer processing"

    def test_batch_complete_log_contains_v3_prefix(self, caplog):
        """Batch completion log must contain '[V3]'."""
        customers = [FakeCustomer(customer_id=92)]
        batch_run_obj = FakeBatchRun()
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            with caplog.at_level(logging.INFO, logger="app.services.batch_service_v3"):
                from app.services.batch_service_v3 import start_batch_v3
                start_batch_v3(db, search_client)

        all_messages = " ".join(r.message for r in caplog.records)
        assert "[V3]" in all_messages, f"No [V3] prefix found. Messages: {all_messages!r}"

    def test_batch_complete_log_includes_batch_id_and_customer_count(self, caplog):
        """Batch complete log must mention the batch_id and customer count."""
        customers = [FakeCustomer(customer_id=93), FakeCustomer(customer_id=94)]
        batch_run_obj = FakeBatchRun()
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            with caplog.at_level(logging.INFO, logger="app.services.batch_service_v3"):
                from app.services.batch_service_v3 import start_batch_v3
                start_batch_v3(db, search_client)

        # Find a log message mentioning batch completion
        completion_msgs = [r.message for r in caplog.records
                           if "[V3]" in r.message and "complete" in r.message.lower()]
        assert len(completion_msgs) >= 1


# ---------------------------------------------------------------------------
# TestConcurrentBatchPrevention
# ---------------------------------------------------------------------------

class TestConcurrentBatchPrevention:
    """Test that starting a second batch while one is running raises an error."""

    def test_raises_value_error_if_running_batch_exists(self):
        """If a batch with status='running' exists, start_batch_v3 must raise ValueError."""
        existing_batch = FakeBatchRun(batch_id="running-batch", status="running")
        customers = [FakeCustomer(customer_id=100)]
        db = make_db(
            customers=customers,
            running_batch=existing_batch,
            batch_run_to_update=None,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            with pytest.raises(ValueError, match="already running"):
                start_batch_v3(db, search_client)

    def test_no_error_when_no_running_batch(self):
        """If no running batch exists, start_batch_v3 must not raise."""
        customers = [FakeCustomer(customer_id=101)]
        batch_run_obj = FakeBatchRun()
        db = make_db(
            customers=customers,
            running_batch=None,
            batch_run_to_update=batch_run_obj,
        )
        search_client = MagicMock()

        with patch(
            "app.services.batch_service_v3.search_customer_strategy_v3",
            return_value=[],
        ), _patch_load_strategies() as mock_load_strats, patch(
            "app.services.batch_service_v3.BatchRun",
            return_value=MagicMock(),
        ), patch(
            "app.services.batch_service_v3.CustomerStatus",
            return_value=MagicMock(),
        ):
            from app.services.batch_service_v3 import start_batch_v3
            batch_id = start_batch_v3(db, search_client)

        assert batch_id is not None


# ---------------------------------------------------------------------------
# TestPersistV3Result (unit test for _persist_v3_result helper directly)
# ---------------------------------------------------------------------------

class TestPersistV3Result:
    """Unit tests for the _persist_v3_result helper function."""

    def test_persist_v3_result_inserts_correct_row(self):
        """_persist_v3_result must insert a SearchResult with all expected fields."""
        db = MagicMock()
        captured = []

        with patch(
            "app.services.batch_service_v3.SearchResult",
        ) as MockSearchResult:
            def capture(**kwargs):
                obj = SimpleNamespace(**kwargs)
                captured.append(obj)
                return obj

            MockSearchResult.side_effect = capture

            from app.services.batch_service_v3 import _persist_v3_result

            result_dict = {
                "md5": "persist_md5",
                "fields": {
                    "SSN": {"found": True, "score": 12.5, "snippet": "snip"},
                    "DOB": {"found": False},
                },
                "confidence": 0.85,
                "needs_review": False,
            }

            _persist_v3_result(db, batch_id="b-ptest", customer_id=200, result_dict=result_dict,
                               strategy_name="fullname_ssn")

        assert len(captured) == 1
        row = captured[0]
        assert row.batch_id == "b-ptest"
        assert row.customer_id == 200
        assert row.md5 == "persist_md5"
        assert row.strategy_name == "fullname_ssn"
        leaked = json.loads(row.leaked_fields)
        assert "SSN" in leaked
        assert "DOB" not in leaked
        details = json.loads(row.match_details)
        assert "SSN" in details
        assert "DOB" in details
        assert row.overall_confidence == pytest.approx(0.85)
        assert row.azure_search_score == pytest.approx(12.5)
        assert row.needs_review is False
        assert db.add.called
        assert db.commit.called
