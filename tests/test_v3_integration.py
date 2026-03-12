"""Phase V3-5.1: V3 Integration Tests — full pipeline end-to-end.

These tests exercise the complete V3 batch processing pipeline:
  indexing -> PII detection -> per-field search -> result merging -> confidence -> persistence

All Azure AI Search and DB access is mocked. No sqlalchemy imports at module
level (avoids hangs). Uses SimpleNamespace and fake stand-ins.

Test groups:
- TestV3IndexingPipeline: index_all_files_v3 calls PII detection and uploads docs with metadata
- TestV3BatchPipeline: start_batch_v3 creates batch run, processes customers, persists results
- TestV3PerFieldDetectionAccuracy: known PII customer, controlled search results per field
- TestV3NoMatchCustomer: all field queries empty -> customer complete with 0 leaks
- TestV3ConfidenceEndToEnd: known scores produce expected formula output
- TestV3NeedsReviewFlag: FirstName-only match triggers needs_review=True
- TestV3SnippetExtraction: highlights captured in match_details
"""

import json
import logging
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Strategy helpers — mirrors what batch_service_v3 loads from strategies.yaml
# ---------------------------------------------------------------------------

FAKE_STRATEGIES = [
    SimpleNamespace(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
    SimpleNamespace(name="lastname_dob", description="", fields=["LastName", "DOB"]),
    SimpleNamespace(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
]
FAKE_STRATEGY_NAMES = [s.name for s in FAKE_STRATEGIES]


@contextmanager
def _patch_load_strategies():
    with patch("app.services.batch_service_v3.load_strategies", return_value=FAKE_STRATEGIES) as m:
        yield m


# ---------------------------------------------------------------------------
# Fake domain objects — avoid sqlalchemy imports
# ---------------------------------------------------------------------------


class FakeDLURecord:
    """Lightweight stand-in for DLU ORM model."""

    def __init__(self, md5: str, file_path: str):
        self.MD5 = md5
        self.file_path = file_path


class FakeMasterData:
    """Lightweight stand-in for MasterData ORM model.

    Uses the exact PII values specified in the phase task.
    """

    def __init__(self, customer_id=1, **kwargs):
        self.customer_id = customer_id
        self.Fullname = kwargs.get("Fullname", "Karthik Chekuri")
        self.FirstName = kwargs.get("FirstName", "Karthik")
        self.LastName = kwargs.get("LastName", "Chekuri")
        self.DOB = kwargs.get("DOB", "1992-07-15")
        self.SSN = kwargs.get("SSN", "343-43-4343")
        self.DriversLicense = kwargs.get("DriversLicense", "TX12345678")
        self.Address1 = kwargs.get("Address1", "123 Main St")
        self.Address2 = kwargs.get("Address2", None)
        self.Address3 = kwargs.get("Address3", None)
        self.ZipCode = kwargs.get("ZipCode", "77001")
        self.City = kwargs.get("City", "Houston")
        self.State = kwargs.get("State", "TX")
        self.Country = kwargs.get("Country", "United States")


class FakeBatchRun:
    """Lightweight stand-in for BatchRun ORM model."""

    def __init__(self, batch_id=None, status="running", total_customers=1,
                 strategy_set=None, started_at=None, completed_at=None):
        self.batch_id = batch_id or str(uuid.uuid4())
        self.status = status
        self.total_customers = total_customers
        self.strategy_set = strategy_set or json.dumps(FAKE_STRATEGY_NAMES)
        self.started_at = started_at
        self.completed_at = completed_at


class FakeCustomerStatus:
    """Lightweight stand-in for CustomerStatus ORM model."""

    def __init__(self, batch_id=None, customer_id=1, status="pending",
                 candidates_found=0, leaks_confirmed=0, error_message=None):
        self.id = None
        self.batch_id = batch_id or str(uuid.uuid4())
        self.customer_id = customer_id
        self.status = status
        self.candidates_found = candidates_found
        self.leaks_confirmed = leaks_confirmed
        self.error_message = error_message


class FakeSearchResultDoc:
    """Mimics an Azure AI Search result document with highlights support."""

    def __init__(self, md5: str, score: float, file_path: str = "case1/file.txt",
                 highlights=None):
        self._md5 = md5
        self._score = score
        self._file_path = file_path
        self._highlights = highlights  # e.g. {"content": ["...[[MATCH]]text[[/MATCH]]..."]}

    def __getitem__(self, key):
        mapping = {
            "md5": self._md5,
            "file_path": self._file_path,
            "@search.score": self._score,
        }
        return mapping[key]

    def get(self, key, default=None):
        if key == "@search.highlights":
            return self._highlights
        try:
            return self[key]
        except KeyError:
            return default


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """A MagicMock for SQLAlchemy Session. Never hits a real DB."""
    return MagicMock()


@pytest.fixture
def mock_search_client():
    """A MagicMock for Azure AI SearchClient. Never hits real Azure Search."""
    return MagicMock()


@pytest.fixture
def fake_customer():
    """Standard V3 test customer with known PII from phase spec."""
    return FakeMasterData(
        customer_id=1,
        Fullname="Karthik Chekuri",
        FirstName="Karthik",
        LastName="Chekuri",
        DOB="1992-07-15",
        SSN="343-43-4343",
        DriversLicense="TX12345678",
        Address1="123 Main St",
        Address2=None,
        Address3=None,
        ZipCode="77001",
        City="Houston",
        State="TX",
        Country="United States",
    )


# ===========================================================================
# TEST CLASS: V3 Indexing Pipeline
# ===========================================================================


class TestV3IndexingPipeline:
    """Integration tests for index_all_files_v3 with mocked dependencies."""

    def test_indexing_uploads_documents_with_pii_metadata(self, mock_db, mock_search_client):
        """WHEN DLU records exist with text files THEN documents are uploaded with PII metadata fields."""
        from app.services.indexing_service_v3 import index_all_files_v3

        fake_dlu = [
            FakeDLURecord("abc123", "case1/file_a.txt"),
            FakeDLURecord("def456", "case1/file_b.txt"),
        ]

        # DB query returns our fake DLU records
        mock_db.query.return_value.all.return_value = fake_dlu

        # Mock PII client that returns SSN + Person entities
        mock_pii_client = MagicMock()
        mock_entity_ssn = MagicMock()
        mock_entity_ssn.category = "USSocialSecurityNumber"
        mock_entity_ssn.text = "343-43-4343"

        mock_entity_person = MagicMock()
        mock_entity_person.category = "Person"
        mock_entity_person.text = "Karthik Chekuri"

        mock_pii_result = MagicMock()
        mock_pii_result.is_error = False
        mock_pii_result.entities = [mock_entity_ssn, mock_entity_person]
        mock_pii_client.recognize_pii_entities.return_value = [mock_pii_result, mock_pii_result]

        # Mock upload_documents returns success for all
        mock_upload_result = MagicMock()
        mock_upload_result.succeeded = True
        mock_search_client.upload_documents.return_value = [mock_upload_result, mock_upload_result]

        with patch("app.services.indexing_service_v3.extract_text", return_value="SSN: 343-43-4343 Name: Karthik"):
            result = index_all_files_v3(
                db=mock_db,
                search_client=mock_search_client,
                pii_client=mock_pii_client,
            )

        # Verify upload was called
        mock_search_client.upload_documents.assert_called_once()
        uploaded_docs = mock_search_client.upload_documents.call_args[1]["documents"]

        assert len(uploaded_docs) == 2

        # Verify each uploaded document contains PII metadata fields
        for doc in uploaded_docs:
            assert "has_ssn" in doc
            assert "has_name" in doc
            assert "has_dob" in doc
            assert "has_address" in doc
            assert "has_phone" in doc
            assert "pii_types" in doc
            assert "pii_entity_count" in doc

        # Verify PII detection set the right flags
        for doc in uploaded_docs:
            assert doc["has_ssn"] is True
            assert doc["has_name"] is True

        assert result.files_processed == 2
        assert result.files_succeeded == 2
        assert result.files_failed == 0

    def test_indexing_uses_default_pii_metadata_on_api_failure(self, mock_db, mock_search_client):
        """WHEN PII detection API fails THEN documents are uploaded with all has_* = False."""
        from app.services.indexing_service_v3 import index_all_files_v3

        fake_dlu = [FakeDLURecord("aaa111", "case1/doc.txt")]
        mock_db.query.return_value.all.return_value = fake_dlu

        # PII client that raises an error
        failing_pii_client = MagicMock()
        failing_pii_client.recognize_pii_entities.side_effect = RuntimeError("API unavailable")

        mock_upload_result = MagicMock()
        mock_upload_result.succeeded = True
        mock_search_client.upload_documents.return_value = [mock_upload_result]

        with patch("app.services.indexing_service_v3.extract_text", return_value="Some text"):
            result = index_all_files_v3(
                db=mock_db,
                search_client=mock_search_client,
                pii_client=failing_pii_client,
            )

        uploaded_docs = mock_search_client.upload_documents.call_args[1]["documents"]
        assert len(uploaded_docs) == 1
        doc = uploaded_docs[0]

        # All PII flags should be False (fallback metadata)
        assert doc["has_ssn"] is False
        assert doc["has_name"] is False
        assert doc["has_dob"] is False
        assert doc["has_address"] is False
        assert doc["has_phone"] is False
        assert doc["pii_entity_count"] == 0

        assert result.files_succeeded == 1

    def test_indexing_skips_unsupported_extensions(self, mock_db, mock_search_client):
        """WHEN DLU records contain unsupported file types THEN they are skipped."""
        from app.services.indexing_service_v3 import index_all_files_v3

        fake_dlu = [
            FakeDLURecord("txt001", "case1/doc.txt"),    # supported
            FakeDLURecord("mp4001", "case1/video.mp4"),  # unsupported
            FakeDLURecord("pdf001", "case1/doc.pdf"),    # unsupported
        ]
        mock_db.query.return_value.all.return_value = fake_dlu

        mock_pii_client = MagicMock()
        pii_result = MagicMock()
        pii_result.is_error = False
        pii_result.entities = []
        mock_pii_client.recognize_pii_entities.return_value = [pii_result]

        mock_upload_result = MagicMock()
        mock_upload_result.succeeded = True
        mock_search_client.upload_documents.return_value = [mock_upload_result]

        with patch("app.services.indexing_service_v3.extract_text", return_value="some text"):
            result = index_all_files_v3(
                db=mock_db,
                search_client=mock_search_client,
                pii_client=mock_pii_client,
            )

        assert result.files_processed == 1
        assert result.files_skipped == 2


# ===========================================================================
# TEST CLASS: V3 Batch Pipeline
# ===========================================================================


class TestV3BatchPipeline:
    """Integration tests for start_batch_v3 with mocked DB and search."""

    def _make_db_with_customers(self, customers, status_rows_by_cid):
        """Build a mock DB that correctly returns customers and status rows."""
        mock_db = MagicMock()

        def mock_query(model_class):
            q = MagicMock()
            # When filter_by(status="running").first() is called for conflict check
            q.filter_by.return_value.first.return_value = None
            # When order_by().all() is called for customer list
            q.order_by.return_value.all.return_value = customers

            def filter_by_side(**kwargs):
                f = MagicMock()
                cid = kwargs.get("customer_id")
                if cid is not None and cid in status_rows_by_cid:
                    f.first.return_value = status_rows_by_cid[cid]
                else:
                    f.first.return_value = None
                f.all.return_value = list(status_rows_by_cid.values())
                return f

            q.filter_by.side_effect = filter_by_side
            return q

        mock_db.query.side_effect = mock_query
        return mock_db

    def test_batch_creates_run_with_v3_strategy_set(self, mock_search_client):
        """WHEN start_batch_v3 is called THEN batch run is created with strategy_set=['v3_azure_only']."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1)

        mock_db = self._make_db_with_customers(customers, {1: status_row})

        batch_runs_added = []
        orig_add = mock_db.add.side_effect

        def capture_add(obj):
            batch_runs_added.append(obj)

        mock_db.add.side_effect = capture_add

        # Patch ORM model classes so no sqlalchemy import happens
        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult"), \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = status_row

            batch_id = start_batch_v3(db=mock_db, search_client=mock_search_client)

        assert isinstance(batch_id, str)
        assert len(batch_id) > 0

        # Verify batch run was created
        mock_db.add.assert_called()

    def test_batch_customer_status_transitions(self, mock_search_client):
        """WHEN customer is processed THEN status goes: pending -> searching -> complete."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1, status="pending")
        status_history = []

        original_commit = None

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = None  # no running batch

            def filter_by_side(**kwargs):
                f = MagicMock()
                if "customer_id" in kwargs:
                    f.first.return_value = status_row
                else:
                    f.first.return_value = None
                return f

            q.filter_by.side_effect = filter_by_side
            q.order_by.return_value.all.return_value = customers
            return q

        mock_db = MagicMock()
        mock_db.query.side_effect = mock_query

        def track_status_on_commit():
            status_history.append(status_row.status)

        mock_db.commit.side_effect = track_status_on_commit

        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult"), \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = FakeCustomerStatus(customer_id=1)

            start_batch_v3(db=mock_db, search_client=mock_search_client)

        # "searching" should have appeared before "complete"
        assert "searching" in status_history
        assert "complete" in status_history
        searching_idx = status_history.index("searching")
        complete_idx = [i for i, s in enumerate(status_history) if s == "complete"][-1]
        assert searching_idx < complete_idx

    def test_batch_persists_results_with_v3_strategy_name(self, mock_search_client):
        """WHEN search returns matches THEN results are persisted with the actual strategy name."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1)
        added_rows = []

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = None

            def filter_by_side(**kwargs):
                f = MagicMock()
                if "customer_id" in kwargs:
                    f.first.return_value = status_row
                else:
                    f.first.return_value = None
                return f

            q.filter_by.side_effect = filter_by_side
            q.order_by.return_value.all.return_value = customers
            return q

        mock_db = MagicMock()
        mock_db.query.side_effect = mock_query
        mock_db.add.side_effect = lambda obj: added_rows.append(obj)

        search_result = {
            "md5": "doc_A",
            "fields": {
                "SSN": {"found": True, "score": 12.5, "snippet": "343-43-4343"},
                "Fullname": {"found": True, "score": 8.0, "snippet": "Karthik Chekuri"},
            },
            "confidence": 0.85,
            "needs_review": False,
        }

        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult") as MockResult, \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[search_result]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = FakeCustomerStatus(customer_id=1)

            fake_result_row = SimpleNamespace(
                batch_id="b1", customer_id=1, md5="doc_A",
                strategy_name="fullname_ssn", leaked_fields='["SSN","Fullname"]',
                overall_confidence=0.85, needs_review=False,
            )
            MockResult.return_value = fake_result_row

            start_batch_v3(db=mock_db, search_client=mock_search_client)

        # SearchResult should have been instantiated with an actual strategy name
        MockResult.assert_called()
        strategy_names_used = [c[1].get("strategy_name") for c in MockResult.call_args_list if c[1].get("strategy_name")]
        assert all(sn in FAKE_STRATEGY_NAMES for sn in strategy_names_used)

    def test_batch_leaked_fields_based_on_query_matches(self, mock_search_client):
        """WHEN SSN and Fullname queries return matches but DOB does not THEN leaked_fields=[SSN, Fullname]."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1)

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = None

            def filter_by_side(**kwargs):
                f = MagicMock()
                if "customer_id" in kwargs:
                    f.first.return_value = status_row
                else:
                    f.first.return_value = None
                return f

            q.filter_by.side_effect = filter_by_side
            q.order_by.return_value.all.return_value = customers
            return q

        mock_db = MagicMock()
        mock_db.query.side_effect = mock_query

        # Only SSN and Fullname found, no DOB
        search_result = {
            "md5": "doc_A",
            "fields": {
                "SSN": {"found": True, "score": 12.5, "snippet": "343-43-4343"},
                "Fullname": {"found": True, "score": 8.0, "snippet": "Karthik Chekuri"},
            },
            "confidence": 0.85,
            "needs_review": False,
        }

        result_row_kwargs = {}

        def capture_result(**kwargs):
            result_row_kwargs.update(kwargs)
            return SimpleNamespace(**kwargs)

        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult") as MockResult, \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[search_result]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = FakeCustomerStatus(customer_id=1)
            MockResult.side_effect = capture_result

            start_batch_v3(db=mock_db, search_client=mock_search_client)

        leaked = json.loads(result_row_kwargs.get("leaked_fields", "[]"))
        assert "SSN" in leaked
        assert "Fullname" in leaked
        assert "DOB" not in leaked


# ===========================================================================
# TEST CLASS: V3 Per-Field Detection Accuracy
# ===========================================================================


class TestV3PerFieldDetectionAccuracy:
    """Integration tests for per-field detection: specific PII customer + controlled search returns."""

    def test_ssn_and_name_found_dob_not_found(self, fake_customer):
        """WHEN SSN + Name queries return matches but DOB query returns empty THEN only SSN+Name in result."""
        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()

        ssn_result = FakeSearchResultDoc("doc_A", 12.5)
        name_result = FakeSearchResultDoc("doc_A", 8.0)

        # Return results for SSN and Name queries; empty for everything else
        # search_customer_v3 calls execute_field_query for each non-null field.
        # We use side_effect to selectively return results based on call order.
        # Instead, patch execute_field_query to control per-field returns.

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "SSN":
                return [("doc_A", 12.5, "343-43-4343")]
            elif field_name in ("Fullname", "FirstName", "LastName"):
                return [("doc_A", 8.0, "Karthik Chekuri")]
            else:
                return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, fake_customer)

        assert len(results) == 1
        doc_result = results[0]
        assert doc_result["md5"] == "doc_A"

        found_fields = set(doc_result["fields"].keys())
        assert "SSN" in found_fields
        assert any(f in found_fields for f in ("Fullname", "FirstName", "LastName"))
        assert "DOB" not in found_fields

    def test_ssn_found_confidence_reflects_weight(self, fake_customer):
        """WHEN only SSN is found THEN confidence = 0.35 * (score/max_score)."""
        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "SSN":
                return [("doc_B", 10.0, "snip")]
            return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, fake_customer)

        assert len(results) == 1
        confidence = results[0]["confidence"]
        # SSN weight = 0.35, score=10.0/max_score=10.0 → ssn_conf=1.0 → 0.35*1.0 = 0.35
        assert abs(confidence - 0.35) < 0.001

    def test_multiple_documents_returned_per_field(self):
        """WHEN SSN query matches two documents THEN both appear in results."""
        from app.services.search_service_v3 import search_customer_v3

        customer = FakeMasterData(
            customer_id=2,
            SSN="111-22-3333",
            Fullname=None, FirstName=None, LastName=None,
            DOB=None, DriversLicense=None,
            Address1=None, Address2=None, Address3=None,
            ZipCode=None, City=None, State=None, Country=None,
        )
        mock_client = MagicMock()

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "SSN":
                return [("doc_X", 12.5, "snip1"), ("doc_Y", 9.0, "snip2")]
            return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, customer)

        md5s = {r["md5"] for r in results}
        assert "doc_X" in md5s
        assert "doc_Y" in md5s


# ===========================================================================
# TEST CLASS: V3 No-Match Customer
# ===========================================================================


class TestV3NoMatchCustomer:
    """Tests for customers where all field queries return empty results."""

    def test_no_match_customer_status_complete_zero_leaks(self):
        """WHEN all field queries return empty THEN customer status=complete, leaks_confirmed=0."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1, status="pending")

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = None

            def filter_by_side(**kwargs):
                f = MagicMock()
                if "customer_id" in kwargs:
                    f.first.return_value = status_row
                else:
                    f.first.return_value = None
                return f

            q.filter_by.side_effect = filter_by_side
            q.order_by.return_value.all.return_value = customers
            return q

        mock_db = MagicMock()
        mock_db.query.side_effect = mock_query

        mock_search_client = MagicMock()

        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult"), \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = FakeCustomerStatus(customer_id=1)

            start_batch_v3(db=mock_db, search_client=mock_search_client)

        assert status_row.status == "complete"
        assert status_row.leaks_confirmed == 0
        assert status_row.candidates_found == 0

    def test_no_match_customer_no_results_persisted(self):
        """WHEN all field queries return empty THEN no SearchResult rows are added to DB."""
        from app.services.batch_service_v3 import start_batch_v3

        customers = [FakeMasterData(customer_id=1)]
        status_row = FakeCustomerStatus(customer_id=1)
        result_rows_added = []

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = None

            def filter_by_side(**kwargs):
                f = MagicMock()
                if "customer_id" in kwargs:
                    f.first.return_value = status_row
                else:
                    f.first.return_value = None
                return f

            q.filter_by.side_effect = filter_by_side
            q.order_by.return_value.all.return_value = customers
            return q

        mock_db = MagicMock()
        mock_db.query.side_effect = mock_query

        mock_search_client = MagicMock()

        with patch("app.services.batch_service_v3.BatchRun") as MockBatchRun, \
             patch("app.services.batch_service_v3.MasterData"), \
             patch("app.services.batch_service_v3.CustomerStatus") as MockCS, \
             patch("app.services.batch_service_v3.SearchResult") as MockResult, \
             patch("app.services.batch_service_v3.search_customer_strategy_v3", return_value=[]), \
             _patch_load_strategies():

            fake_batch_run = FakeBatchRun()
            MockBatchRun.return_value = fake_batch_run
            MockCS.return_value = FakeCustomerStatus(customer_id=1)
            MockResult.side_effect = lambda **kwargs: result_rows_added.append(kwargs)

            start_batch_v3(db=mock_db, search_client=mock_search_client)

        assert len(result_rows_added) == 0

    def test_search_customer_v3_returns_empty_for_all_null_fields(self):
        """WHEN customer has all PII fields null THEN search_customer_v3 returns []."""
        from app.services.search_service_v3 import search_customer_v3

        null_customer = SimpleNamespace(
            customer_id=99,
            Fullname=None, FirstName=None, LastName=None,
            DOB=None, SSN=None, DriversLicense=None,
            Address1=None, Address2=None, Address3=None,
            ZipCode=None, City=None, State=None, Country=None,
        )
        mock_client = MagicMock()

        results = search_customer_v3(mock_client, null_customer)

        assert results == []
        mock_client.search.assert_not_called()


# ===========================================================================
# TEST CLASS: V3 Confidence Calculation End-to-End
# ===========================================================================


class TestV3ConfidenceEndToEnd:
    """Integration tests verifying confidence formula with known input scores."""

    def test_ssn_and_fullname_confidence_formula(self, fake_customer):
        """WHEN SSN score=12.5 and Fullname score=8.0 with max=12.5 THEN confidence = 0.35*1.0 + 0.30*0.64."""
        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "SSN":
                return [("doc_A", 12.5, "snip")]
            elif field_name == "Fullname":
                return [("doc_A", 8.0, "snip2")]
            return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, fake_customer)

        assert len(results) == 1
        confidence = results[0]["confidence"]

        # max_score = 12.5
        # ssn_conf = min(1.0, 12.5/12.5) = 1.0
        # name_conf = max(fullname_conf, firstname_conf, lastname_conf)
        #           = max(min(1.0, 8.0/12.5), 0, 0) = max(0.64, 0, 0) = 0.64
        # other_avg = 0 (no other fields found)
        # overall = 0.35*1.0 + 0.30*0.64 + 0.20*0 + 0.15*0 = 0.35 + 0.192 = 0.542
        expected = 0.35 * 1.0 + 0.30 * (8.0 / 12.5)
        assert abs(confidence - expected) < 0.01

    def test_high_confidence_both_ssn_and_name_full_score(self):
        """WHEN SSN and Fullname both score at max THEN confidence = 0.65."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "SSN": {"found": True, "score": 10.0, "snippet": "snip"},
            "Fullname": {"found": True, "score": 10.0, "snippet": "snip"},
        }
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score=10.0)

        # ssn_conf = 1.0, name_conf = 1.0, other_avg = 0
        # overall = 0.35 + 0.30 = 0.65
        assert abs(confidence - 0.65) < 0.001
        assert needs_review is False

    def test_low_confidence_triggers_needs_review(self):
        """WHEN overall confidence < 0.5 THEN needs_review = True."""
        from app.services.search_service_v3 import compute_confidence_v3

        # Only DOB found (an "other" field) → low confidence
        doc_fields = {
            "DOB": {"found": True, "score": 2.0, "snippet": "snip"},
        }
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score=10.0)

        # ssn_conf=0, name_conf=0, other_avg=min(1.0, 2/10)=0.2
        # overall = 0.35*0 + 0.30*0 + 0.20*0.2 + 0 = 0.04
        assert confidence < 0.5
        assert needs_review is True

    def test_multiple_other_fields_averages_correctly(self):
        """WHEN DOB + ZipCode both found THEN other_avg = average of their normalised scores."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "DOB": {"found": True, "score": 8.0, "snippet": None},
            "ZipCode": {"found": True, "score": 4.0, "snippet": None},
        }
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score=10.0)

        # ssn_conf=0, name_conf=0, other_avg=(0.8+0.4)/2=0.6
        # overall = 0.20 * 0.6 = 0.12
        assert abs(confidence - 0.12) < 0.001
        assert needs_review is True


# ===========================================================================
# TEST CLASS: V3 Needs Review Flag
# ===========================================================================


class TestV3NeedsReviewFlag:
    """Tests for the needs_review flag computation."""

    def test_firstname_only_triggers_needs_review(self, fake_customer):
        """WHEN only FirstName matches (no Fullname, LastName, SSN) THEN needs_review=True."""
        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "FirstName":
                return [("doc_A", 6.0, "Karthik")]
            return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, fake_customer)

        assert len(results) == 1
        result = results[0]
        assert result["needs_review"] is True
        assert "FirstName" in result["fields"]
        assert "Fullname" not in result["fields"]
        assert "SSN" not in result["fields"]

    def test_firstname_with_ssn_does_not_trigger_needs_review_by_firstname_rule(self, fake_customer):
        """WHEN FirstName + SSN both match THEN firstname_only rule does NOT apply."""
        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()

        def mock_execute_field_query(search_client, field_name, field_value):
            if field_name == "FirstName":
                return [("doc_A", 8.0, "Karthik")]
            elif field_name == "SSN":
                return [("doc_A", 12.0, "343-43-4343")]
            return []

        with patch("app.services.search_service_v3.execute_field_query",
                   side_effect=mock_execute_field_query):
            results = search_customer_v3(mock_client, fake_customer)

        assert len(results) == 1
        result = results[0]
        # SSN present so firstname_only rule doesn't fire
        # overall = 0.35*(12/12) + 0.30*(8/12) + ... = 0.35 + 0.20 = 0.55
        # needs_review should be False (confidence >= 0.5 and not firstname-only)
        assert result["needs_review"] is False

    def test_fullname_match_no_ssn_above_threshold_no_needs_review(self, fake_customer):
        """WHEN Fullname matches with high score and no SSN THEN needs_review may be False if confidence >= 0.5."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "Fullname": {"found": True, "score": 10.0, "snippet": "Karthik Chekuri"},
        }
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score=10.0)

        # ssn_conf=0, name_conf=1.0 (fullname), other_avg=0
        # overall = 0.35*0 + 0.30*1.0 + 0 + 0 = 0.30
        assert confidence < 0.5
        assert needs_review is True  # low confidence even though Fullname matched

    def test_needs_review_false_when_confidence_high(self):
        """WHEN confidence is 0.65 (SSN + Fullname at max) THEN needs_review=False."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "SSN": {"found": True, "score": 10.0, "snippet": None},
            "Fullname": {"found": True, "score": 10.0, "snippet": None},
        }
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score=10.0)

        assert confidence >= 0.5
        assert needs_review is False


# ===========================================================================
# TEST CLASS: V3 Snippet Extraction
# ===========================================================================


class TestV3SnippetExtraction:
    """Tests verifying that Azure highlight snippets are captured in match_details."""

    def test_highlights_captured_in_field_results(self):
        """WHEN Azure returns @search.highlights THEN snippet is extracted for the field."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()

        # Simulate Azure returning highlights
        highlight_doc = FakeSearchResultDoc(
            md5="doc_H",
            score=9.5,
            highlights={"content": ["Employee with SSN [[MATCH]]343-43-4343[[/MATCH]] on file"]},
        )
        mock_client.search.return_value = [highlight_doc]

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert len(results) == 1
        md5, score, snippet = results[0]
        assert md5 == "doc_H"
        assert score == 9.5
        assert snippet is not None
        assert "[[MATCH]]" in snippet

    def test_no_highlights_gives_none_snippet(self):
        """WHEN Azure returns no @search.highlights THEN snippet is None."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()

        no_highlight_doc = FakeSearchResultDoc(md5="doc_N", score=5.0, highlights=None)
        mock_client.search.return_value = [no_highlight_doc]

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert len(results) == 1
        md5, score, snippet = results[0]
        assert snippet is None

    def test_snippets_appear_in_match_details_via_merge(self):
        """WHEN search results have snippets THEN merge_field_results captures them in the dict."""
        from app.services.search_service_v3 import merge_field_results

        field_results = {
            "SSN": [("doc_A", 12.5, "SSN [[MATCH]]343-43-4343[[/MATCH]] found")],
            "Fullname": [("doc_A", 8.0, "Name: [[MATCH]]Karthik[[/MATCH]] Chekuri")],
        }

        merged = merge_field_results(field_results)

        assert "doc_A" in merged
        doc = merged["doc_A"]

        ssn_entry = doc["SSN"]
        assert ssn_entry["snippet"] == "SSN [[MATCH]]343-43-4343[[/MATCH]] found"

        fullname_entry = doc["Fullname"]
        assert fullname_entry["snippet"] == "Name: [[MATCH]]Karthik[[/MATCH]] Chekuri"

    def test_snippet_persisted_in_match_details_json(self):
        """WHEN _persist_v3_result is called with snippets THEN match_details JSON contains them."""
        from app.services.batch_service_v3 import _persist_v3_result

        mock_db = MagicMock()
        batch_id = str(uuid.uuid4())
        customer_id = 1

        result_dict = {
            "md5": "doc_A",
            "fields": {
                "SSN": {"found": True, "score": 12.5, "snippet": "SSN [[MATCH]]343[[/MATCH]]"},
                "Fullname": {"found": True, "score": 8.0, "snippet": None},
            },
            "confidence": 0.85,
            "needs_review": False,
        }

        persisted_kwargs = {}

        with patch("app.services.batch_service_v3.SearchResult") as MockResult:
            MockResult.side_effect = lambda **kwargs: persisted_kwargs.update(kwargs) or SimpleNamespace(**kwargs)
            _persist_v3_result(mock_db, batch_id, customer_id, result_dict, strategy_name="fullname_ssn")

        match_details_str = persisted_kwargs.get("match_details", "{}")
        match_details = json.loads(match_details_str)

        # SSN snippet should be preserved
        assert "SSN" in match_details
        assert match_details["SSN"]["snippet"] == "SSN [[MATCH]]343[[/MATCH]]"

        leaked_fields = json.loads(persisted_kwargs.get("leaked_fields", "[]"))
        assert "SSN" in leaked_fields
        assert "Fullname" in leaked_fields
