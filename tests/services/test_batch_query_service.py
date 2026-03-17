"""Tests for Batch Query Service (Phase V4-1.3).

TDD Red phase — all tests written BEFORE production code.

Covers spec scenarios from:
- openspec: batch-query-service spec

Functions under test (app/services/batch_query_service.py):
- get_batch_status(db, batch_id) -> dict | None
- get_customer_statuses(db, batch_id, status_filter=None) -> list[dict] | None
- get_batch_results(db, batch_id, customer_id=None) -> list[dict] | None
- list_all_batches(db) -> list[dict]

ALL DB access is mocked via MagicMock — no real SQLAlchemy imports triggered.
"""

import datetime
import json
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Fake ORM objects (stand-ins, no SQLAlchemy needed)
# ---------------------------------------------------------------------------

class FakeBatchRun:
    def __init__(
        self,
        batch_id="batch-001",
        status="running",
        total_customers=3,
        strategy_set=None,
        started_at=None,
        completed_at=None,
        total_files=None,
    ):
        self.batch_id = batch_id
        self.status = status
        self.total_customers = total_customers
        self.strategy_set = strategy_set or json.dumps(["fuzzy", "regex"])
        self.started_at = started_at or datetime.datetime(2025, 1, 10, 9, 0, 0)
        self.completed_at = completed_at
        self.total_files = total_files


class FakeCustomerStatus:
    def __init__(
        self,
        customer_id=1,
        batch_id="batch-001",
        status="complete",
        candidates_found=5,
        leaks_confirmed=2,
        strategies_matched=None,
        error_message=None,
        processed_at=None,
    ):
        self.customer_id = customer_id
        self.batch_id = batch_id
        self.status = status
        self.candidates_found = candidates_found
        self.leaks_confirmed = leaks_confirmed
        self.strategies_matched = strategies_matched or json.dumps(["fuzzy"])
        self.error_message = error_message
        self.processed_at = processed_at or datetime.datetime(2025, 1, 10, 10, 0, 0)


class FakeResult:
    def __init__(
        self,
        batch_id="batch-001",
        customer_id=1,
        md5="abc123",
        strategy_name="fuzzy",
        leaked_fields=None,
        match_details=None,
        overall_confidence=0.85,
        azure_search_score=1.2,
        needs_review=False,
        searched_at=None,
    ):
        self.batch_id = batch_id
        self.customer_id = customer_id
        self.md5 = md5
        self.strategy_name = strategy_name
        self.leaked_fields = leaked_fields or json.dumps(["SSN", "DOB"])
        self.match_details = match_details or json.dumps({"SSN": "matched"})
        self.overall_confidence = overall_confidence
        self.azure_search_score = azure_search_score
        self.needs_review = needs_review
        self.searched_at = searched_at or datetime.datetime(2025, 1, 10, 11, 0, 0)


# ---------------------------------------------------------------------------
# Helper: build a mock DB session with chained .query().filter_by().xxx()
# ---------------------------------------------------------------------------

def _make_db(batch_run=None, customer_statuses=None, results=None,
             complete_count=0, failed_count=0):
    """Return a MagicMock session configured for typical query patterns."""
    db = MagicMock()

    # We set up query returns based on what the real code does via filter_by chains.
    # The service uses deferred imports and db.query(Model).filter_by(...).first() etc.
    # We capture calls by configuring the mock generically — the service imports
    # models inside functions, so db.query() receives the model class itself.
    # We accept any call and route by inspection in side_effect below.

    # Default: track query results per model type
    db._batch_run = batch_run
    db._customer_statuses = customer_statuses or []
    db._results = results or []
    db._complete_count = complete_count
    db._failed_count = failed_count

    return db


# ---------------------------------------------------------------------------
# TestGetBatchStatus
# ---------------------------------------------------------------------------

class TestGetBatchStatus:
    """Tests for get_batch_status(db, batch_id)."""

    def _import_func(self):
        from app.services.batch_query_service import get_batch_status
        return get_batch_status

    def _build_db(self, batch_run=None, complete_count=0, failed_count=0,
                  file_total=0, file_indexed=0, file_failed=0, file_skipped=0,
                  result_count=0):
        """Build a mock DB that handles the query chains in get_batch_status."""
        db = MagicMock()

        # query(BatchRun).filter_by(batch_id=...).first() -> batch_run
        # query(CustomerStatus).filter_by(batch_id=..., status="complete").count() -> complete_count
        # query(CustomerStatus).filter_by(batch_id=..., status="failed").count() -> failed_count
        # query(FileStatus).count() -> file_total
        # query(FileStatus).filter_by(status="indexed").count() -> file_indexed
        # etc.

        # We use a side_effect on db.query to dispatch based on the model class name
        def query_side_effect(model_class):
            class_name = getattr(model_class, "__name__", str(model_class))

            mock_query = MagicMock()

            if class_name == "BatchRun":
                mock_query.filter_by.return_value.first.return_value = batch_run

            elif class_name == "CustomerStatus":
                # Two chained filter_by calls: first for batch_id, then status
                # The real code does: db.query(CustomerStatus).filter_by(batch_id=batch_id, status="complete").count()
                def cs_filter_by(**kwargs):
                    status = kwargs.get("status")
                    inner = MagicMock()
                    if status == "complete":
                        inner.count.return_value = complete_count
                    elif status == "failed":
                        inner.count.return_value = failed_count
                    else:
                        inner.count.return_value = 0
                        inner.all.return_value = []
                    return inner
                mock_query.filter_by.side_effect = cs_filter_by

            elif class_name == "FileStatus":
                mock_query.count.return_value = file_total

                def fs_filter_by(**kwargs):
                    status = kwargs.get("status")
                    inner = MagicMock()
                    if status == "indexed":
                        inner.count.return_value = file_indexed
                    elif status == "failed":
                        inner.count.return_value = file_failed
                    elif status == "skipped":
                        inner.count.return_value = file_skipped
                    else:
                        inner.count.return_value = 0
                    return inner
                mock_query.filter_by.side_effect = fs_filter_by

            elif class_name == "Result":
                mock_query.filter_by.return_value.count.return_value = result_count

            return mock_query

        db.query.side_effect = query_side_effect
        return db

    def test_returns_dict_for_existing_batch(self):
        """get_batch_status returns a dict when batch exists."""
        get_batch_status = self._import_func()
        batch_run = FakeBatchRun(batch_id="batch-001", status="running", total_customers=5)
        db = self._build_db(batch_run=batch_run, complete_count=3, failed_count=1)

        result = get_batch_status(db, "batch-001")

        assert result is not None
        assert isinstance(result, dict)

    def test_returns_none_for_nonexistent_batch(self):
        """get_batch_status returns None when batch_id not found."""
        get_batch_status = self._import_func()
        db = self._build_db(batch_run=None)

        result = get_batch_status(db, "nonexistent-batch")

        assert result is None

    def test_result_contains_required_keys(self):
        """Result dict contains batch_id, status, started_at, completed_at, strategy_set,
        total_customers, completed_customers, failed_customers."""
        get_batch_status = self._import_func()
        batch_run = FakeBatchRun(
            batch_id="batch-001",
            status="running",
            total_customers=10,
            strategy_set=json.dumps(["fuzzy", "regex"]),
            started_at=datetime.datetime(2025, 6, 1, 8, 0, 0),
            completed_at=None,
        )
        db = self._build_db(batch_run=batch_run, complete_count=4, failed_count=2)

        result = get_batch_status(db, "batch-001")

        assert result["batch_id"] == "batch-001"
        assert result["status"] == "running"
        assert result["started_at"] == datetime.datetime(2025, 6, 1, 8, 0, 0)
        assert result["completed_at"] is None
        assert result["strategy_set"] == ["fuzzy", "regex"]
        assert result["total_customers"] == 10

    def test_correct_customer_counts(self):
        """completed_customers and failed_customers reflect DB counts."""
        get_batch_status = self._import_func()
        batch_run = FakeBatchRun(batch_id="batch-002", total_customers=20)
        db = self._build_db(batch_run=batch_run, complete_count=7, failed_count=3)

        result = get_batch_status(db, "batch-002")

        assert result["completed_customers"] == 7
        assert result["failed_customers"] == 3

    def test_strategy_set_parsed_from_json(self):
        """strategy_set is deserialized from JSON string."""
        get_batch_status = self._import_func()
        batch_run = FakeBatchRun(strategy_set=json.dumps(["exact", "phonetic"]))
        db = self._build_db(batch_run=batch_run)

        result = get_batch_status(db, batch_run.batch_id)

        assert result["strategy_set"] == ["exact", "phonetic"]

    def test_strategy_set_empty_on_invalid_json(self):
        """strategy_set defaults to [] when JSON is invalid."""
        get_batch_status = self._import_func()
        batch_run = FakeBatchRun(strategy_set="not-valid-json{{{")
        db = self._build_db(batch_run=batch_run)

        result = get_batch_status(db, batch_run.batch_id)

        assert result["strategy_set"] == []


# ---------------------------------------------------------------------------
# TestGetCustomerStatuses
# ---------------------------------------------------------------------------

class TestGetCustomerStatuses:
    """Tests for get_customer_statuses(db, batch_id, status_filter=None)."""

    def _import_func(self):
        from app.services.batch_query_service import get_customer_statuses
        return get_customer_statuses

    def _build_db(self, batch_run=None, customer_rows=None, filtered_rows=None):
        db = MagicMock()

        def query_side_effect(model_class):
            class_name = getattr(model_class, "__name__", str(model_class))
            mock_query = MagicMock()

            if class_name == "BatchRun":
                mock_query.filter_by.return_value.first.return_value = batch_run

            elif class_name == "CustomerStatus":
                # Simulate: db.query(CustomerStatus).filter_by(batch_id=...) -> query
                # Then optionally .filter_by(status=...) -> query
                # Then .all() -> rows

                initial_filter = MagicMock()
                all_rows = customer_rows or []
                fail_rows = filtered_rows if filtered_rows is not None else [
                    r for r in all_rows if r.status == "failed"
                ]

                def second_filter_by(**kwargs):
                    status = kwargs.get("status")
                    inner = MagicMock()
                    if status == "failed":
                        inner.all.return_value = fail_rows
                    else:
                        inner.all.return_value = all_rows
                    return inner

                initial_filter.filter_by.side_effect = second_filter_by
                initial_filter.all.return_value = all_rows

                mock_query.filter_by.return_value = initial_filter

            return mock_query

        db.query.side_effect = query_side_effect
        return db

    def test_returns_list_for_existing_batch(self):
        """get_customer_statuses returns list of dicts for existing batch."""
        get_customer_statuses = self._import_func()
        batch_run = FakeBatchRun()
        rows = [
            FakeCustomerStatus(customer_id=1, status="complete"),
            FakeCustomerStatus(customer_id=2, status="failed"),
        ]
        db = self._build_db(batch_run=batch_run, customer_rows=rows)

        result = get_customer_statuses(db, "batch-001")

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 2

    def test_result_dicts_have_required_keys(self):
        """Each result dict contains customer_id, status, candidates_found,
        leaks_confirmed, error_message."""
        get_customer_statuses = self._import_func()
        batch_run = FakeBatchRun()
        row = FakeCustomerStatus(
            customer_id=42,
            status="complete",
            candidates_found=10,
            leaks_confirmed=3,
            error_message=None,
        )
        db = self._build_db(batch_run=batch_run, customer_rows=[row])

        result = get_customer_statuses(db, "batch-001")

        assert len(result) == 1
        item = result[0]
        assert item["customer_id"] == 42
        assert item["status"] == "complete"
        assert item["candidates_found"] == 10
        assert item["leaks_confirmed"] == 3
        assert item["error_message"] is None

    def test_status_filter_returns_only_matching(self):
        """status_filter='failed' returns only failed customer rows."""
        get_customer_statuses = self._import_func()
        batch_run = FakeBatchRun()
        all_rows = [
            FakeCustomerStatus(customer_id=1, status="complete"),
            FakeCustomerStatus(customer_id=2, status="failed", error_message="timeout"),
            FakeCustomerStatus(customer_id=3, status="complete"),
        ]
        failed_rows = [r for r in all_rows if r.status == "failed"]
        db = self._build_db(batch_run=batch_run, customer_rows=all_rows,
                            filtered_rows=failed_rows)

        result = get_customer_statuses(db, "batch-001", status_filter="failed")

        assert result is not None
        assert len(result) == 1
        assert result[0]["customer_id"] == 2
        assert result[0]["status"] == "failed"

    def test_returns_none_for_nonexistent_batch(self):
        """get_customer_statuses returns None when batch_id not found."""
        get_customer_statuses = self._import_func()
        db = self._build_db(batch_run=None)

        result = get_customer_statuses(db, "nonexistent")

        assert result is None

    def test_returns_empty_list_when_no_customers(self):
        """Returns empty list when batch exists but has no customer rows."""
        get_customer_statuses = self._import_func()
        batch_run = FakeBatchRun()
        db = self._build_db(batch_run=batch_run, customer_rows=[])

        result = get_customer_statuses(db, "batch-001")

        assert result == []


# ---------------------------------------------------------------------------
# TestGetBatchResults
# ---------------------------------------------------------------------------

class TestGetBatchResults:
    """Tests for get_batch_results(db, batch_id, customer_id=None)."""

    def _import_func(self):
        from app.services.batch_query_service import get_batch_results
        return get_batch_results

    def _build_db(self, batch_run=None, result_rows=None, filtered_rows=None):
        db = MagicMock()

        def query_side_effect(model_class):
            class_name = getattr(model_class, "__name__", str(model_class))
            mock_query = MagicMock()

            if class_name == "BatchRun":
                mock_query.filter_by.return_value.first.return_value = batch_run

            elif class_name == "Result":
                all_rows = result_rows or []

                # Simulate chained: .filter_by(batch_id=...).filter_by(customer_id=...).order_by(...).all()
                batch_filter = MagicMock()

                def customer_filter_by(**kwargs):
                    cid = kwargs.get("customer_id")
                    inner = MagicMock()
                    rows = filtered_rows if (filtered_rows is not None and cid is not None) else all_rows
                    inner.order_by.return_value.all.return_value = rows
                    return inner

                batch_filter.filter_by.side_effect = customer_filter_by
                batch_filter.order_by.return_value.all.return_value = all_rows

                mock_query.filter_by.return_value = batch_filter

            return mock_query

        db.query.side_effect = query_side_effect
        return db

    def test_returns_list_for_existing_batch(self):
        """get_batch_results returns list of dicts for existing batch."""
        get_batch_results = self._import_func()
        batch_run = FakeBatchRun()
        rows = [
            FakeResult(customer_id=1, md5="aaa"),
            FakeResult(customer_id=2, md5="bbb"),
        ]
        db = self._build_db(batch_run=batch_run, result_rows=rows)

        result = get_batch_results(db, "batch-001")

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 2

    def test_result_dicts_have_required_keys(self):
        """Each result dict contains customer_id, md5, strategy_name,
        leaked_fields, overall_confidence, needs_review."""
        get_batch_results = self._import_func()
        batch_run = FakeBatchRun()
        row = FakeResult(
            customer_id=5,
            md5="deadbeef",
            strategy_name="fuzzy",
            leaked_fields=json.dumps(["SSN", "DOB"]),
            overall_confidence=0.9,
            needs_review=True,
        )
        db = self._build_db(batch_run=batch_run, result_rows=[row])

        result = get_batch_results(db, "batch-001")

        assert len(result) == 1
        item = result[0]
        assert item["customer_id"] == 5
        assert item["md5"] == "deadbeef"
        assert item["strategy_name"] == "fuzzy"
        assert item["leaked_fields"] == ["SSN", "DOB"]
        assert item["overall_confidence"] == 0.9
        assert item["needs_review"] is True

    def test_customer_id_filter_returns_only_that_customer(self):
        """customer_id=42 filter returns only that customer's results."""
        get_batch_results = self._import_func()
        batch_run = FakeBatchRun()
        all_rows = [
            FakeResult(customer_id=42, md5="aaa"),
            FakeResult(customer_id=99, md5="bbb"),
        ]
        cust_42_rows = [r for r in all_rows if r.customer_id == 42]
        db = self._build_db(batch_run=batch_run, result_rows=all_rows,
                            filtered_rows=cust_42_rows)

        result = get_batch_results(db, "batch-001", customer_id=42)

        assert result is not None
        assert len(result) == 1
        assert result[0]["customer_id"] == 42
        assert result[0]["md5"] == "aaa"

    def test_returns_empty_list_when_no_results(self):
        """Returns empty list when batch exists but has no result rows."""
        get_batch_results = self._import_func()
        batch_run = FakeBatchRun()
        db = self._build_db(batch_run=batch_run, result_rows=[])

        result = get_batch_results(db, "batch-001")

        assert result == []

    def test_returns_none_for_nonexistent_batch(self):
        """Returns None when batch_id does not exist."""
        get_batch_results = self._import_func()
        db = self._build_db(batch_run=None)

        result = get_batch_results(db, "nonexistent")

        assert result is None

    def test_leaked_fields_deserialized_from_json(self):
        """leaked_fields is parsed from JSON string to list."""
        get_batch_results = self._import_func()
        batch_run = FakeBatchRun()
        row = FakeResult(leaked_fields=json.dumps(["FirstName", "LastName", "SSN"]))
        db = self._build_db(batch_run=batch_run, result_rows=[row])

        result = get_batch_results(db, "batch-001")

        assert result[0]["leaked_fields"] == ["FirstName", "LastName", "SSN"]


# ---------------------------------------------------------------------------
# TestListAllBatches
# ---------------------------------------------------------------------------

class TestListAllBatches:
    """Tests for list_all_batches(db)."""

    def _import_func(self):
        from app.services.batch_query_service import list_all_batches
        return list_all_batches

    def _build_db(self, batch_rows=None):
        db = MagicMock()

        def query_side_effect(model_class):
            class_name = getattr(model_class, "__name__", str(model_class))
            mock_query = MagicMock()

            if class_name == "BatchRun":
                rows = batch_rows or []
                mock_query.order_by.return_value.all.return_value = rows

            return mock_query

        db.query.side_effect = query_side_effect
        return db

    def test_returns_list_ordered_by_started_at_desc(self):
        """list_all_batches returns all batches ordered newest first."""
        list_all_batches = self._import_func()
        rows = [
            FakeBatchRun(batch_id="newest", started_at=datetime.datetime(2025, 6, 2)),
            FakeBatchRun(batch_id="older", started_at=datetime.datetime(2025, 6, 1)),
        ]
        db = self._build_db(batch_rows=rows)

        result = list_all_batches(db)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["batch_id"] == "newest"
        assert result[1]["batch_id"] == "older"

    def test_batch_dicts_have_required_keys(self):
        """Each batch dict contains batch_id, status, started_at, completed_at,
        total_customers."""
        list_all_batches = self._import_func()
        row = FakeBatchRun(
            batch_id="batch-x",
            status="completed",
            total_customers=50,
            started_at=datetime.datetime(2025, 5, 1),
            completed_at=datetime.datetime(2025, 5, 2),
        )
        db = self._build_db(batch_rows=[row])

        result = list_all_batches(db)

        assert len(result) == 1
        item = result[0]
        assert item["batch_id"] == "batch-x"
        assert item["status"] == "completed"
        assert item["total_customers"] == 50
        assert item["started_at"] == datetime.datetime(2025, 5, 1)
        assert item["completed_at"] == datetime.datetime(2025, 5, 2)

    def test_returns_empty_list_when_no_batches(self):
        """list_all_batches returns empty list when table is empty."""
        list_all_batches = self._import_func()
        db = self._build_db(batch_rows=[])

        result = list_all_batches(db)

        assert result == []

    def test_strategy_count_included(self):
        """Each batch dict includes strategy_count from strategy_set JSON."""
        list_all_batches = self._import_func()
        row = FakeBatchRun(strategy_set=json.dumps(["fuzzy", "regex", "exact"]))
        db = self._build_db(batch_rows=[row])

        result = list_all_batches(db)

        assert result[0]["strategy_count"] == 3
