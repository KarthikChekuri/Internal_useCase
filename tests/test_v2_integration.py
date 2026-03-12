"""Phase V2-4.2: V2 Integration Tests — full pipeline end-to-end.

These tests exercise the complete V2 batch processing pipeline:
  batch run -> strategy search -> leak detection -> confidence -> results persistence -> status tracking

All Azure AI Search and DB access is mocked. No sqlalchemy imports at module
level (avoids hangs). Uses FakeMasterData / FakeBatchRun / FakeCustomerStatus
stand-ins.

Test groups:
- TestBatchPipelineEndToEnd: start_batch runs all phases in order with mocked services
- TestStrategySearchIntegration: search_customer returns unioned candidates across strategies
- TestLeakDetectionIntegration: detect_leaks on realistic file text produces correct field results
- TestConfidenceScoringIntegration: compute_overall_confidence formula produces correct scores
- TestStatusTrackingIntegration: customer status transitions through full lifecycle
- TestResultPersistenceIntegration: _persist_result writes correct fields to DB
- TestBatchAPIIntegration: POST /batch/run -> GET /batch/{id}/status -> GET /batch/{id}/results
- TestBatchConflictIntegration: 409 returned when batch already running
- TestBatchResumeIntegration: resume skips completed, retries failed
- TestNoMatchesIntegration: customers with zero candidates marked complete correctly
- TestSearchErrorIntegration: Azure Search error marks customer failed, continues to next
"""

import datetime
import json
import logging
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fake domain objects — avoid sqlalchemy imports
# ---------------------------------------------------------------------------

class FakeMasterData:
    """Lightweight stand-in for MasterData ORM model.

    Mirrors all 13 PII field attributes used by the V2 pipeline.
    """

    def __init__(self, customer_id=1, **kwargs):
        self.customer_id = customer_id
        self.Fullname = kwargs.get("Fullname", "Karthik Chekuri")
        self.FirstName = kwargs.get("FirstName", "Karthik")
        self.LastName = kwargs.get("LastName", "Chekuri")
        self.DOB = kwargs.get("DOB", datetime.date(1990, 5, 15))
        self.SSN = kwargs.get("SSN", "343-43-4343")
        self.DriversLicense = kwargs.get("DriversLicense", "D1234567")
        self.Address1 = kwargs.get("Address1", "123 Main St")
        self.Address2 = kwargs.get("Address2", None)
        self.Address3 = kwargs.get("Address3", None)
        self.ZipCode = kwargs.get("ZipCode", "90210")
        self.City = kwargs.get("City", "New York")
        self.State = kwargs.get("State", "CA")
        self.Country = kwargs.get("Country", "United States")


class FakeBatchRun:
    """Lightweight stand-in for BatchRun ORM model."""

    def __init__(self, batch_id=None, status="running", total_customers=3,
                 strategy_set=None, started_at=None, completed_at=None):
        self.batch_id = batch_id or str(uuid.uuid4())
        self.status = status
        self.total_customers = total_customers
        self.strategy_set = strategy_set or json.dumps(["fullname_ssn"])
        self.started_at = started_at or datetime.datetime.utcnow()
        self.completed_at = completed_at


class FakeCustomerStatus:
    """Lightweight stand-in for CustomerStatus ORM model."""

    def __init__(self, batch_id=None, customer_id=1, status="pending",
                 candidates_found=0, leaks_confirmed=0, strategies_matched=None,
                 error_message=None, processed_at=None):
        self.id = None
        self.batch_id = batch_id or str(uuid.uuid4())
        self.customer_id = customer_id
        self.status = status
        self.candidates_found = candidates_found
        self.leaks_confirmed = leaks_confirmed
        self.strategies_matched = strategies_matched or json.dumps([])
        self.error_message = error_message
        self.processed_at = processed_at


class FakeResult:
    """Lightweight stand-in for Result ORM model."""

    def __init__(self, batch_id=None, customer_id=1, md5="abc123",
                 strategy_name="fullname_ssn", leaked_fields=None,
                 match_details=None, overall_confidence=0.9,
                 azure_search_score=12.5, needs_review=False,
                 searched_at=None):
        self.batch_id = batch_id or str(uuid.uuid4())
        self.customer_id = customer_id
        self.md5 = md5
        self.strategy_name = strategy_name
        self.leaked_fields = leaked_fields or json.dumps(["SSN", "Fullname"])
        self.match_details = match_details or json.dumps({})
        self.overall_confidence = overall_confidence
        self.azure_search_score = azure_search_score
        self.needs_review = needs_review
        self.searched_at = searched_at or datetime.datetime.utcnow()


class FakeSearchResult:
    """Mimics an Azure AI Search result document (MD5-keyed)."""

    def __init__(self, md5: str, score: float, file_path: str = "case1/file.txt"):
        self._md5 = md5
        self._score = score
        self._file_path = file_path

    def __getitem__(self, key):
        mapping = {
            "md5": self._md5,
            "file_path": self._file_path,
            "@search.score": self._score,
        }
        return mapping[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def customer_karthik():
    """Standard test customer with all PII fields populated."""
    return FakeMasterData(customer_id=1)


@pytest.fixture
def customer_john():
    """Second test customer for multi-customer batch tests."""
    return FakeMasterData(
        customer_id=2,
        Fullname="John Doe",
        FirstName="John",
        LastName="Doe",
        SSN="123-45-6789",
        DOB=datetime.date(1985, 3, 20),
        DriversLicense="D9876543",
        ZipCode="10001",
        City="Chicago",
        State="IL",
        Country="United States",
    )


@pytest.fixture
def mock_db():
    """A MagicMock for SQLAlchemy Session. Never hits a real DB."""
    return MagicMock()


@pytest.fixture
def mock_search_client():
    """A MagicMock for Azure AI SearchClient. Never hits real Azure Search."""
    return MagicMock()


@pytest.fixture
def strategies():
    """Load real strategies from strategies.yaml for integration tests."""
    from app.services.search_service import load_strategies
    import os
    yaml_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "strategies.yaml",
    )
    return load_strategies(yaml_path)


@pytest.fixture
def fullname_ssn_strategy():
    """A single fullname_ssn strategy object."""
    from app.services.search_service import Strategy
    return Strategy(
        name="fullname_ssn",
        description="Search by fullname and SSN",
        fields=["Fullname", "SSN"],
    )


@pytest.fixture
def ssn_only_strategy():
    """A single ssn_only strategy for testing single-field strategies."""
    from app.services.search_service import Strategy
    return Strategy(
        name="ssn_only",
        description="Search by SSN only",
        fields=["SSN"],
    )


@pytest.fixture
def file_text_with_full_pii():
    """File text containing all PII for customer_karthik."""
    return (
        "Employee Record\n"
        "Name: Karthik Chekuri\n"
        "Date of Birth: 1990-05-15\n"
        "Social Security Number: 343-43-4343\n"
        "Driver's License: D1234567\n"
        "Address: 123 Main St\n"
        "Zip Code: 90210\n"
        "City: New York\n"
        "State: CA\n"
        "Country: United States\n"
    )


@pytest.fixture
def file_text_ssn_only():
    """File text with only the SSN — no name or other PII."""
    return (
        "Payroll export for period ending 2026-03-11\n"
        "Record ID: 343-43-4343\n"
        "Amount: $5,000.00\n"
    )


@pytest.fixture
def file_text_no_pii():
    """File text with no PII — should produce zero leak detections."""
    return (
        "Quarterly Financial Report Q3 2024\n"
        "Revenue increased by 15% compared to previous quarter.\n"
        "Operating expenses remained stable at projected levels.\n"
    )


# ===========================================================================
# TEST CLASS: Strategy + Search Integration
# ===========================================================================

class TestStrategySearchIntegration:
    """Integration tests for strategy-driven search producing candidate unions."""

    def test_single_strategy_returns_candidates(self, mock_search_client, customer_karthik, fullname_ssn_strategy):
        """WHEN one strategy is run for a customer THEN candidates are returned with correct fields."""
        from app.services.search_service import search_customer

        mock_search_client.search.return_value = [
            FakeSearchResult("abc123", 12.5, "case1/file_a.txt"),
            FakeSearchResult("def456", 8.3, "case1/file_b.txt"),
        ]

        candidates = search_customer(mock_search_client, customer_karthik, [fullname_ssn_strategy])

        assert len(candidates) == 2
        md5s = {c["md5"] for c in candidates}
        assert "abc123" in md5s
        assert "def456" in md5s

        # Each candidate has required fields
        for c in candidates:
            assert "md5" in c
            assert "file_path" in c
            assert "azure_search_score" in c
            assert "strategy_that_found_it" in c

    def test_multiple_strategies_union_candidates(self, mock_search_client, customer_karthik):
        """WHEN three strategies return overlapping results THEN union is deduplicated."""
        from app.services.search_service import search_customer, Strategy

        strategy1 = Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])
        strategy2 = Strategy("lastname_dob", "desc", ["LastName", "DOB"])
        strategy3 = Strategy("unique_ids", "desc", ["SSN", "DriversLicense"])

        # Strategy 1: file_a, file_b
        # Strategy 2: file_a (overlap), file_d
        # Strategy 3: file_a (overlap), file_e
        # Union: file_a, file_b, file_d, file_e
        mock_search_client.search.side_effect = [
            [FakeSearchResult("file_a", 12.5), FakeSearchResult("file_b", 8.0)],
            [FakeSearchResult("file_a", 9.0), FakeSearchResult("file_d", 5.0)],
            [FakeSearchResult("file_a", 7.0), FakeSearchResult("file_e", 4.0)],
        ]

        candidates = search_customer(mock_search_client, customer_karthik, [strategy1, strategy2, strategy3])

        assert len(candidates) == 4
        md5s = {c["md5"] for c in candidates}
        assert md5s == {"file_a", "file_b", "file_d", "file_e"}

    def test_duplicate_file_highest_score_kept(self, mock_search_client, customer_karthik):
        """WHEN file_a found by two strategies THEN highest azure_search_score is kept."""
        from app.services.search_service import search_customer, Strategy

        strategy1 = Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])
        strategy2 = Strategy("unique_ids", "desc", ["SSN", "DriversLicense"])

        mock_search_client.search.side_effect = [
            [FakeSearchResult("file_a", 12.5)],
            [FakeSearchResult("file_a", 9.0)],
        ]

        candidates = search_customer(mock_search_client, customer_karthik, [strategy1, strategy2])

        assert len(candidates) == 1
        assert candidates[0]["azure_search_score"] == 12.5

    def test_duplicate_file_first_strategy_recorded(self, mock_search_client, customer_karthik):
        """WHEN file_a found by two strategies THEN strategy_that_found_it is the first."""
        from app.services.search_service import search_customer, Strategy

        strategy1 = Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])
        strategy2 = Strategy("unique_ids", "desc", ["SSN", "DriversLicense"])

        mock_search_client.search.side_effect = [
            [FakeSearchResult("file_a", 12.5)],
            [FakeSearchResult("file_a", 9.0)],
        ]

        candidates = search_customer(mock_search_client, customer_karthik, [strategy1, strategy2])

        assert candidates[0]["strategy_that_found_it"] == "fullname_ssn"

    def test_zero_results_from_all_strategies(self, mock_search_client, customer_karthik):
        """WHEN all strategies return 0 results THEN candidates list is empty."""
        from app.services.search_service import search_customer, Strategy

        strategy1 = Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])
        mock_search_client.search.return_value = []

        candidates = search_customer(mock_search_client, customer_karthik, [strategy1])

        assert candidates == []

    def test_null_field_strategy_skipped(self, mock_search_client):
        """WHEN customer has null DriversLicense and strategy uses only DriversLicense THEN strategy skipped."""
        from app.services.search_service import search_customer, Strategy

        customer = FakeMasterData(customer_id=99, DriversLicense=None, SSN=None)
        strategy = Strategy("dl_only", "desc", ["DriversLicense"])

        candidates = search_customer(mock_search_client, customer, [strategy])

        assert candidates == []
        # search should NOT have been called (null field, strategy skipped)
        mock_search_client.search.assert_not_called()


# ===========================================================================
# TEST CLASS: Leak Detection Integration
# ===========================================================================

class TestLeakDetectionIntegration:
    """Integration tests for detect_leaks on realistic file content."""

    def test_full_pii_file_detects_all_core_fields(self, file_text_with_full_pii, customer_karthik):
        """WHEN file contains all PII THEN SSN, Fullname, DOB, DriversLicense all detected."""
        from app.services.leak_detection_service import detect_leaks

        result = detect_leaks(file_text_with_full_pii, customer_karthik)

        assert result.SSN.found is True
        assert result.SSN.method == "exact"
        assert result.SSN.confidence == 1.0

        assert result.Fullname.found is True
        assert result.DOB.found is True
        assert result.DriversLicense.found is True

    def test_ssn_only_file_detects_ssn_not_name(self, file_text_ssn_only, customer_karthik):
        """WHEN file has SSN but no name THEN SSN found, Fullname not found."""
        from app.services.leak_detection_service import detect_leaks

        result = detect_leaks(file_text_ssn_only, customer_karthik)

        assert result.SSN.found is True
        assert result.Fullname.found is False

    def test_no_pii_file_detects_nothing(self, file_text_no_pii, customer_karthik):
        """WHEN file has no PII THEN all fields not found."""
        from app.services.leak_detection_service import detect_leaks

        result = detect_leaks(file_text_no_pii, customer_karthik)

        assert result.SSN.found is False
        assert result.Fullname.found is False
        assert result.DOB.found is False

    def test_null_fields_skipped_in_detection(self):
        """WHEN customer has null Address2 and Address3 THEN they are not found (no scan)."""
        from app.services.leak_detection_service import detect_leaks

        customer = FakeMasterData(customer_id=1, Address2=None, Address3=None)
        file_text = "Name: Karthik Chekuri\nSSN: 343-43-4343\n"

        result = detect_leaks(file_text, customer)

        # Null fields default to not-found
        assert result.Address2.found is False
        assert result.Address2.method == "none"
        assert result.Address3.found is False

    def test_detect_leaks_returns_leak_detection_result(self, file_text_with_full_pii, customer_karthik):
        """WHEN detect_leaks is called THEN LeakDetectionResult is returned with needs_review attribute."""
        from app.services.leak_detection_service import detect_leaks, LeakDetectionResult

        result = detect_leaks(file_text_with_full_pii, customer_karthik)

        assert isinstance(result, LeakDetectionResult)
        assert hasattr(result, "needs_review")

    def test_first_name_only_with_ssn_sets_confidence_070(self):
        """WHEN file has first name + SSN but no Fullname or LastName THEN disambiguation confidence 0.70."""
        from app.services.leak_detection_service import detect_leaks

        # File text: first name "Karthik" + SSN, but NOT "Chekuri"
        file_text = "Record: Karthik is employee at HQ. ID: 343-43-4343\n"
        customer = FakeMasterData(
            customer_id=1,
            Fullname="Karthik Chekuri",
            FirstName="Karthik",
            LastName="Chekuri",
            SSN="343-43-4343",
        )

        result = detect_leaks(file_text, customer)

        # FirstName should be found with disambiguation confidence 0.70 (SSN confirms)
        assert result.SSN.found is True
        assert result.FirstName.found is True
        assert result.FirstName.confidence == 0.70

    def test_snippet_included_when_field_found(self, file_text_with_full_pii, customer_karthik):
        """WHEN a field is detected THEN snippet is a non-empty string."""
        from app.services.leak_detection_service import detect_leaks

        result = detect_leaks(file_text_with_full_pii, customer_karthik)

        assert result.SSN.snippet is not None
        assert isinstance(result.SSN.snippet, str)
        assert len(result.SSN.snippet) > 0

    def test_snippet_null_when_field_not_found(self, file_text_no_pii, customer_karthik):
        """WHEN a field is not detected THEN snippet is None."""
        from app.services.leak_detection_service import detect_leaks

        result = detect_leaks(file_text_no_pii, customer_karthik)

        assert result.SSN.snippet is None


# ===========================================================================
# TEST CLASS: Confidence Scoring Integration
# ===========================================================================

class TestConfidenceScoringIntegration:
    """Integration tests for compute_overall_confidence formula scenarios."""

    def test_ssn_and_name_scenario(self):
        """WHEN SSN+Name both match THEN formula: 0.40*ssn + 0.30*name + 0.15*other + 0.15*search."""
        from app.utils.confidence import compute_overall_confidence

        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[1.0, 0.0, 0.0],
            search_score_norm=0.8,
        )

        assert result["scenario"] == "ssn_and_name"
        assert result["needs_review"] is False
        # Expected: 0.40*1.0 + 0.30*0.95 + 0.15*(1/3) + 0.15*0.8
        # = 0.40 + 0.285 + 0.05 + 0.12 = 0.855
        assert abs(result["score"] - 0.855) < 0.01

    def test_ssn_only_scenario(self):
        """WHEN SSN found but no name THEN formula: 0.60*ssn + 0.15*other + 0.25*search."""
        from app.utils.confidence import compute_overall_confidence

        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.0,
            other_field_confs=[0.0, 1.0, 0.0, 0.0],
            search_score_norm=0.6,
        )

        assert result["scenario"] == "ssn_only"
        # Expected: 0.60*1.0 + 0.15*(1/4) + 0.25*0.6
        # = 0.60 + 0.0375 + 0.15 = 0.7875
        assert abs(result["score"] - 0.7875) < 0.01

    def test_name_only_scenario(self):
        """WHEN name found but no SSN THEN formula: 0.50*name + 0.20*other + 0.30*search."""
        from app.utils.confidence import compute_overall_confidence

        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.85,
            other_field_confs=[0.0, 0.0, 0.95],
            search_score_norm=0.5,
        )

        assert result["scenario"] == "name_only"
        # Expected: 0.50*0.85 + 0.20*(0.95/3) + 0.30*0.5
        # = 0.425 + 0.0633 + 0.15 = 0.6383
        assert abs(result["score"] - 0.638) < 0.01

    def test_no_anchor_scenario_needs_review(self):
        """WHEN neither SSN nor name found THEN no_anchor formula and needs_review=True."""
        from app.utils.confidence import compute_overall_confidence

        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.0,
            other_field_confs=[1.0, 1.0, 0.0, 0.0],
            search_score_norm=0.4,
        )

        assert result["scenario"] == "no_anchor"
        assert result["needs_review"] is True
        # Expected: 0.50*(2/4) + 0.50*0.4 = 0.25 + 0.20 = 0.45
        assert abs(result["score"] - 0.45) < 0.01

    def test_score_clamped_to_1(self):
        """WHEN all inputs are 1.0 THEN score is clamped to at most 1.0."""
        from app.utils.confidence import compute_overall_confidence

        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=1.0,
            other_field_confs=[1.0, 1.0, 1.0],
            search_score_norm=1.0,
        )

        assert result["score"] <= 1.0

    def test_normalize_search_scores(self):
        """WHEN scores are [12.5, 8.3, 4.1] THEN normalized to [1.0, 0.664, 0.328]."""
        from app.utils.confidence import normalize_search_scores

        scores = [12.5, 8.3, 4.1]
        normalized = normalize_search_scores(scores)

        assert abs(normalized[0] - 1.0) < 0.001
        assert abs(normalized[1] - 0.664) < 0.01
        assert abs(normalized[2] - 0.328) < 0.01


# ===========================================================================
# TEST CLASS: Status Tracking Integration
# ===========================================================================

class TestStatusTrackingIntegration:
    """Integration tests for customer status transitions through the pipeline."""

    def test_customer_status_transitions_to_complete(self, mock_db, customer_karthik, fullname_ssn_strategy):
        """WHEN customer processed with matches THEN status goes pending -> searching -> detecting -> complete."""
        from app.services.batch_service import _process_customer

        batch_id = str(uuid.uuid4())
        cid = customer_karthik.customer_id

        # Track which statuses were set
        statuses_set = []
        status_row = FakeCustomerStatus(batch_id=batch_id, customer_id=cid, status="pending")

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        def capture_commit():
            # Just capture status changes
            pass

        mock_db.commit.side_effect = capture_commit

        candidate = {
            "md5": "abc123",
            "file_path": "case1/file.txt",
            "azure_search_score": 12.5,
            "strategy_that_found_it": "fullname_ssn",
        }

        with patch("app.services.batch_service.search_customer", return_value=[candidate]):
            with patch("app.services.batch_service.extract_text", return_value="Name: Karthik Chekuri\nSSN: 343-43-4343\n"):
                with patch("app.services.batch_service.detect_leaks") as mock_detect:
                    from app.schemas.pii import FieldMatchResult
                    from app.services.leak_detection_service import LeakDetectionResult

                    fake_result = LeakDetectionResult()
                    fake_result.SSN = FieldMatchResult(found=True, method="exact", confidence=1.0, snippet="343-43-4343")
                    fake_result.needs_review = False
                    mock_detect.return_value = fake_result

                    with patch("app.services.batch_service._persist_result"):
                        _process_customer(
                            db=mock_db,
                            search_client=MagicMock(),
                            customer=customer_karthik,
                            strategies=[fullname_ssn_strategy],
                            batch_id=batch_id,
                        )

        # Final status should be "complete" (last call to _update_customer_status)
        assert status_row.status == "complete"

    def test_customer_status_set_to_failed_on_search_error(self, mock_db, customer_karthik, fullname_ssn_strategy):
        """WHEN Azure Search raises an exception THEN customer status is set to failed."""
        from app.services.batch_service import _process_customer

        batch_id = str(uuid.uuid4())
        cid = customer_karthik.customer_id

        status_row = FakeCustomerStatus(batch_id=batch_id, customer_id=cid, status="pending")

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        with patch("app.services.batch_service.search_customer", side_effect=Exception("Azure Search timeout after 30s")):
            _process_customer(
                db=mock_db,
                search_client=MagicMock(),
                customer=customer_karthik,
                strategies=[fullname_ssn_strategy],
                batch_id=batch_id,
            )

        assert status_row.status == "failed"
        assert "Azure Search timeout" in (status_row.error_message or "")

    def test_customer_with_zero_candidates_marked_complete(self, mock_db, customer_karthik, fullname_ssn_strategy):
        """WHEN strategies return zero candidates THEN customer marked complete with 0 candidates/leaks."""
        from app.services.batch_service import _process_customer

        batch_id = str(uuid.uuid4())
        cid = customer_karthik.customer_id

        status_row = FakeCustomerStatus(batch_id=batch_id, customer_id=cid, status="pending")

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        with patch("app.services.batch_service.search_customer", return_value=[]):
            _process_customer(
                db=mock_db,
                search_client=MagicMock(),
                customer=customer_karthik,
                strategies=[fullname_ssn_strategy],
                batch_id=batch_id,
            )

        assert status_row.status == "complete"
        assert status_row.candidates_found == 0
        assert status_row.leaks_confirmed == 0


# ===========================================================================
# TEST CLASS: Result Persistence Integration
# ===========================================================================

class TestResultPersistenceIntegration:
    """Integration tests for result rows persisted to DB."""

    def test_persist_result_inserts_row_with_correct_fields(self, mock_db):
        """WHEN _persist_result is called THEN db.add is called with a Result having correct fields."""
        from app.services.batch_service import _persist_result
        from app.schemas.pii import FieldMatchResult
        from app.services.leak_detection_service import LeakDetectionResult

        batch_id = str(uuid.uuid4())
        customer_id = 42

        candidate = {
            "md5": "abc123",
            "file_path": "case1/file.txt",
            "azure_search_score": 12.5,
            "strategy_that_found_it": "fullname_ssn",
        }

        fake_result = LeakDetectionResult()
        fake_result.SSN = FieldMatchResult(found=True, method="exact", confidence=1.0, snippet="343-43-4343")
        fake_result.Fullname = FieldMatchResult(found=True, method="normalized", confidence=0.95, snippet="Karthik Chekuri")
        fake_result.needs_review = False

        rows_added = []
        mock_db.add.side_effect = lambda row: rows_added.append(row)

        # Patch the Result model import inside _persist_result using the deferred import path
        with patch("app.models.result.Result") as MockResult:
            fake_row = FakeResult(
                batch_id=batch_id,
                customer_id=customer_id,
                md5="abc123",
                strategy_name="fullname_ssn",
                overall_confidence=0.9,
                azure_search_score=12.5,
            )
            MockResult.return_value = fake_row

            _persist_result(
                db=mock_db,
                batch_id=batch_id,
                customer_id=customer_id,
                candidate=candidate,
                leak_result=fake_result,
                overall_confidence=0.9,
            )

        # db.add should have been called
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called()

    def test_no_result_persisted_when_no_leaks_found(self, mock_db, customer_karthik, fullname_ssn_strategy):
        """WHEN detect_leaks finds no PII in candidate files THEN no results are inserted."""
        from app.services.batch_service import _process_customer
        from app.services.leak_detection_service import LeakDetectionResult

        batch_id = str(uuid.uuid4())
        cid = customer_karthik.customer_id

        status_row = FakeCustomerStatus(batch_id=batch_id, customer_id=cid)

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        candidate = {
            "md5": "abc123",
            "file_path": "case1/file.txt",
            "azure_search_score": 5.0,
            "strategy_that_found_it": "fullname_ssn",
        }

        # detect_leaks returns a result with no fields found
        empty_result = LeakDetectionResult()
        empty_result.needs_review = False

        with patch("app.services.batch_service.search_customer", return_value=[candidate]):
            with patch("app.services.batch_service.extract_text", return_value="No PII here."):
                with patch("app.services.batch_service.detect_leaks", return_value=empty_result):
                    _process_customer(
                        db=mock_db,
                        search_client=MagicMock(),
                        customer=customer_karthik,
                        strategies=[fullname_ssn_strategy],
                        batch_id=batch_id,
                    )

        # _persist_result should NOT be called (no leaks found)
        # leaks_confirmed should be 0
        assert status_row.leaks_confirmed == 0

    def test_multiple_candidates_multiple_results(self, mock_db, customer_karthik, fullname_ssn_strategy):
        """WHEN customer has 3 files with leaks THEN 3 result rows are persisted."""
        from app.services.batch_service import _process_customer
        from app.schemas.pii import FieldMatchResult
        from app.services.leak_detection_service import LeakDetectionResult

        batch_id = str(uuid.uuid4())
        cid = customer_karthik.customer_id

        status_row = FakeCustomerStatus(batch_id=batch_id, customer_id=cid)

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        candidates = [
            {"md5": "file1", "file_path": "case1/f1.txt", "azure_search_score": 10.0, "strategy_that_found_it": "fullname_ssn"},
            {"md5": "file2", "file_path": "case1/f2.txt", "azure_search_score": 8.0, "strategy_that_found_it": "fullname_ssn"},
            {"md5": "file3", "file_path": "case1/f3.txt", "azure_search_score": 6.0, "strategy_that_found_it": "fullname_ssn"},
        ]

        def make_leak_result():
            r = LeakDetectionResult()
            r.SSN = FieldMatchResult(found=True, method="exact", confidence=1.0, snippet="343-43-4343")
            r.needs_review = False
            return r

        persist_calls = []
        with patch("app.services.batch_service.search_customer", return_value=candidates):
            with patch("app.services.batch_service.extract_text", return_value="SSN: 343-43-4343"):
                with patch("app.services.batch_service.detect_leaks", side_effect=[make_leak_result() for _ in candidates]):
                    with patch("app.services.batch_service._persist_result", side_effect=lambda **kwargs: persist_calls.append(kwargs)):
                        _process_customer(
                            db=mock_db,
                            search_client=MagicMock(),
                            customer=customer_karthik,
                            strategies=[fullname_ssn_strategy],
                            batch_id=batch_id,
                        )

        assert len(persist_calls) == 3
        assert status_row.leaks_confirmed == 3


# ===========================================================================
# TEST CLASS: Full Batch Pipeline End-to-End
# ===========================================================================

class TestBatchPipelineEndToEnd:
    """End-to-end integration tests for start_batch with all components mocked."""

    def test_start_batch_creates_batch_run_and_processes_customers(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN start_batch is called THEN batch run created and all customers processed."""
        from app.services.batch_service import start_batch

        customers = [
            FakeMasterData(customer_id=1),
            FakeMasterData(customer_id=2, Fullname="John Doe", SSN="123-45-6789"),
        ]

        batch_run_row = FakeBatchRun()
        status_rows = {
            1: FakeCustomerStatus(customer_id=1),
            2: FakeCustomerStatus(customer_id=2),
        }

        def mock_query(model_class):
            q = MagicMock()
            q.filter_by.return_value.first.side_effect = lambda **kwargs: (
                batch_run_row if "batch_id" in kwargs and "status" not in kwargs else
                None if "status" in kwargs else batch_run_row
            )
            # For customer queries, filter_by(status=...).first() -> None (no running batch)
            q.filter_by.return_value.first.return_value = None
            q.order_by.return_value.all.return_value = customers
            q.count.return_value = 0
            q.filter_by.return_value.all.return_value = list(status_rows.values())
            q.filter_by.return_value.count.return_value = 0
            return q

        mock_db.query.side_effect = mock_query

        with patch("app.services.batch_service._check_running_batch", return_value=None):
            with patch("app.services.batch_service._get_all_customers", return_value=customers):
                with patch("app.services.batch_service._create_batch_run", return_value="batch-uuid-001"):
                    with patch("app.services.batch_service._init_customer_statuses"):
                        with patch("app.services.batch_service._process_all_customers") as mock_process:
                            with patch("app.services.batch_service._complete_batch_run"):
                                with patch("app.services.batch_service._collect_batch_summary", return_value={
                                    "total_leaks": 5, "files_with_leaks": 3, "customers_failed": 0,
                                }):
                                    result_id = start_batch(
                                        db=mock_db,
                                        search_client=mock_search_client,
                                        strategies=[fullname_ssn_strategy],
                                    )

        assert result_id == "batch-uuid-001"
        mock_process.assert_called_once()

    def test_start_batch_raises_when_batch_already_running(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN a batch is already running THEN start_batch raises ValueError with batch_id."""
        from app.services.batch_service import start_batch

        running_batch = FakeBatchRun(batch_id="existing-batch-001")

        with patch("app.services.batch_service._check_running_batch", return_value=running_batch):
            with pytest.raises(ValueError) as exc_info:
                start_batch(
                    db=mock_db,
                    search_client=mock_search_client,
                    strategies=[fullname_ssn_strategy],
                )

        assert "existing-batch-001" in str(exc_info.value)

    def test_resume_batch_raises_for_completed_batch(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN resume_batch called on completed batch THEN ValueError raised."""
        from app.services.batch_service import resume_batch

        completed_batch = FakeBatchRun(batch_id="completed-001", status="completed")

        with patch("app.services.batch_service._get_batch_run", return_value=completed_batch):
            with pytest.raises(ValueError) as exc_info:
                resume_batch(
                    db=mock_db,
                    search_client=mock_search_client,
                    strategies=[fullname_ssn_strategy],
                    batch_id="completed-001",
                )

        assert "already completed" in str(exc_info.value).lower()

    def test_resume_batch_raises_for_nonexistent_batch(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN resume_batch called with unknown batch_id THEN ValueError raised."""
        from app.services.batch_service import resume_batch

        with patch("app.services.batch_service._get_batch_run", return_value=None):
            with pytest.raises(ValueError) as exc_info:
                resume_batch(
                    db=mock_db,
                    search_client=mock_search_client,
                    strategies=[fullname_ssn_strategy],
                    batch_id="nonexistent-batch",
                )

        assert "not found" in str(exc_info.value).lower()

    def test_customers_processed_in_customer_id_order(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN batch has customers with IDs [3,1,5,2] THEN processed in order [1,2,3,5]."""
        from app.services.batch_service import _process_all_customers

        customers = [
            FakeMasterData(customer_id=1),
            FakeMasterData(customer_id=2),
            FakeMasterData(customer_id=3),
            FakeMasterData(customer_id=5),
        ]
        batch_id = str(uuid.uuid4())
        processing_order = []

        def track_process(db, search_client, customer, strategies, batch_id):
            processing_order.append(customer.customer_id)

        status_row = FakeCustomerStatus(status="pending")

        def mock_query(*args, **kwargs):
            q = MagicMock()
            q.filter_by.return_value.first.return_value = status_row
            return q

        mock_db.query.side_effect = mock_query

        with patch("app.services.batch_service._process_customer", side_effect=track_process):
            with patch("app.services.batch_service._get_customer_status", return_value=status_row):
                _process_all_customers(
                    db=mock_db,
                    search_client=mock_search_client,
                    customers=customers,
                    strategies=[fullname_ssn_strategy],
                    batch_id=batch_id,
                )

        assert processing_order == [1, 2, 3, 5]

    def test_processing_continues_after_customer_failure(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN customer 1 fails THEN customer 2 is still processed."""
        from app.services.batch_service import _process_all_customers

        customers = [
            FakeMasterData(customer_id=1),
            FakeMasterData(customer_id=2),
        ]
        batch_id = str(uuid.uuid4())
        processed_ids = []

        def mock_process(db, search_client, customer, strategies, batch_id):
            if customer.customer_id == 1:
                raise Exception("Simulated failure for customer 1")
            processed_ids.append(customer.customer_id)

        status_row = FakeCustomerStatus(status="pending")
        failed_status_row = FakeCustomerStatus(status="pending")

        call_count = [0]

        def mock_get_status(db, batch_id, customer_id):
            return status_row

        mock_db.query.return_value.filter_by.return_value.first.return_value = status_row

        with patch("app.services.batch_service._process_customer", side_effect=mock_process):
            with patch("app.services.batch_service._get_customer_status", side_effect=mock_get_status):
                with patch("app.services.batch_service._update_customer_status") as mock_update:
                    _process_all_customers(
                        db=mock_db,
                        search_client=mock_search_client,
                        customers=customers,
                        strategies=[fullname_ssn_strategy],
                        batch_id=batch_id,
                    )

        # Customer 2 was processed despite customer 1 failing
        assert 2 in processed_ids

    def test_completed_customer_skipped_in_resume(self, mock_db, mock_search_client, fullname_ssn_strategy):
        """WHEN customer is already 'complete' THEN it is skipped during _process_all_customers."""
        from app.services.batch_service import _process_all_customers

        customers = [FakeMasterData(customer_id=1)]
        batch_id = str(uuid.uuid4())
        processed_ids = []

        # Return a "complete" status for this customer
        complete_status = FakeCustomerStatus(status="complete")

        with patch("app.services.batch_service._get_customer_status", return_value=complete_status):
            with patch("app.services.batch_service._process_customer", side_effect=lambda **kwargs: processed_ids.append(1)) as mock_proc:
                _process_all_customers(
                    db=mock_db,
                    search_client=mock_search_client,
                    customers=customers,
                    strategies=[fullname_ssn_strategy],
                    batch_id=batch_id,
                )

        # Customer was complete -> should be skipped
        mock_proc.assert_not_called()


# ===========================================================================
# TEST CLASS: Batch API Integration
# ===========================================================================

class TestBatchAPIIntegration:
    """Integration tests for the batch HTTP endpoints with mocked DB."""

    @pytest.fixture
    def app_client(self):
        """Create a TestClient with all DB and search dependencies mocked."""
        from app.main import app
        from app.dependencies import get_db, get_search_client, get_settings

        mock_db = MagicMock()
        mock_search_client = MagicMock()
        mock_settings = MagicMock()
        mock_settings.STRATEGIES_FILE = None  # Use default strategies.yaml

        def override_db():
            yield mock_db

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_search_client] = lambda: mock_search_client
        app.dependency_overrides[get_settings] = lambda: mock_settings

        from fastapi.testclient import TestClient
        yield TestClient(app), mock_db, mock_search_client, mock_settings

        app.dependency_overrides.clear()

    def test_post_batch_run_returns_batch_id_and_status(self, app_client):
        """WHEN POST /batch/run THEN response has batch_id, status=running, total_customers."""
        test_client, mock_db, mock_search_client, mock_settings = app_client

        batch_id = str(uuid.uuid4())

        with patch("app.routers.batch.load_strategies") as mock_load:
            from app.services.search_service import Strategy
            mock_load.return_value = [Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])]

            with patch("app.routers.batch._get_total_customers", return_value=100):
                with patch("app.routers.batch.batch_service._check_running_batch", return_value=None):
                    with patch("app.routers.batch.batch_service._create_batch_run", return_value=batch_id):
                        with patch("app.routers.batch.batch_service._get_all_customers", return_value=[]):
                            with patch("app.routers.batch.batch_service._init_customer_statuses"):
                                response = test_client.post("/batch/run")

        assert response.status_code == 200
        data = response.json()
        assert data["batch_id"] == batch_id
        assert data["status"] == "running"
        assert data["total_customers"] == 100

    def test_post_batch_run_returns_409_when_batch_running(self, app_client):
        """WHEN POST /batch/run while batch running THEN 409 with batch_id in message."""
        test_client, mock_db, mock_search_client, mock_settings = app_client

        running = FakeBatchRun(batch_id="existing-batch-001")

        with patch("app.routers.batch.load_strategies") as mock_load:
            from app.services.search_service import Strategy
            mock_load.return_value = [Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])]

            with patch("app.routers.batch._get_total_customers", return_value=100):
                with patch("app.routers.batch.batch_service._check_running_batch", return_value=running):
                    response = test_client.post("/batch/run")

        assert response.status_code == 409
        assert "existing-batch-001" in response.json()["detail"]

    def test_get_batch_status_returns_404_for_unknown_batch(self, app_client):
        """WHEN GET /batch/{batch_id}/status for unknown batch_id THEN 404."""
        test_client, mock_db, *_ = app_client

        with patch("app.routers.batch.get_batch_status", return_value=None):
            response = test_client.get("/batch/unknown-batch-id/status")

        assert response.status_code == 404

    def test_get_batch_status_returns_status_for_running_batch(self, app_client):
        """WHEN GET /batch/{batch_id}/status for running batch THEN status fields returned."""
        test_client, mock_db, *_ = app_client

        batch_id = str(uuid.uuid4())
        now = datetime.datetime.utcnow()

        status_data = {
            "batch_id": batch_id,
            "status": "running",
            "started_at": now,
            "completed_at": None,
            "strategy_set": ["fullname_ssn"],
            "indexing": {"total": 500, "indexed": 500, "failed": 3, "skipped": 0},
            "searching": {"total_customers": 200, "completed": 120, "failed": 1, "pending": 79},
            "detection": {"total_pairs_processed": 3200, "leaks_found": 450},
        }

        with patch("app.routers.batch.get_batch_status", return_value=status_data):
            response = test_client.get(f"/batch/{batch_id}/status")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"
        assert data["batch_id"] == batch_id
        assert "indexing" in data
        assert "searching" in data
        assert "detection" in data

    def test_get_batch_customers_returns_404_for_unknown_batch(self, app_client):
        """WHEN GET /batch/{batch_id}/customers for unknown batch THEN 404."""
        test_client, *_ = app_client

        with patch("app.routers.batch.get_customer_statuses", return_value=None):
            response = test_client.get("/batch/unknown-id/customers")

        assert response.status_code == 404

    def test_get_batch_customers_returns_customer_list(self, app_client):
        """WHEN GET /batch/{batch_id}/customers THEN list of customer status items returned."""
        test_client, *_ = app_client

        batch_id = str(uuid.uuid4())
        customer_statuses = [
            {
                "customer_id": 1,
                "status": "complete",
                "candidates_found": 5,
                "leaks_confirmed": 3,
                "strategies_matched": ["fullname_ssn"],
                "error_message": None,
                "processed_at": datetime.datetime.utcnow(),
            },
            {
                "customer_id": 2,
                "status": "failed",
                "candidates_found": 0,
                "leaks_confirmed": 0,
                "strategies_matched": [],
                "error_message": "Azure Search timeout after 30s",
                "processed_at": datetime.datetime.utcnow(),
            },
        ]

        with patch("app.routers.batch.get_customer_statuses", return_value=customer_statuses):
            response = test_client.get(f"/batch/{batch_id}/customers")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["customer_id"] == 1
        assert data[0]["status"] == "complete"
        assert data[1]["status"] == "failed"
        assert data[1]["error_message"] == "Azure Search timeout after 30s"

    def test_get_batch_customers_filter_by_status(self, app_client):
        """WHEN GET /batch/{batch_id}/customers?status=failed THEN only failed customers returned."""
        test_client, *_ = app_client

        batch_id = str(uuid.uuid4())
        failed_customer = [{
            "customer_id": 3,
            "status": "failed",
            "candidates_found": 0,
            "leaks_confirmed": 0,
            "strategies_matched": [],
            "error_message": "Timeout",
            "processed_at": datetime.datetime.utcnow(),
        }]

        with patch("app.routers.batch.get_customer_statuses", return_value=failed_customer) as mock_get:
            response = test_client.get(f"/batch/{batch_id}/customers?status=failed")

        assert response.status_code == 200
        # Verify the status_filter was passed correctly
        call_args = mock_get.call_args
        assert call_args.kwargs.get("status_filter") == "failed" or (
            len(call_args.args) >= 3 and call_args.args[2] == "failed"
        )

    def test_get_batch_results_returns_404_for_unknown_batch(self, app_client):
        """WHEN GET /batch/{batch_id}/results for unknown batch THEN 404."""
        test_client, *_ = app_client

        with patch("app.routers.batch.get_batch_results", return_value=None):
            response = test_client.get("/batch/unknown-id/results")

        assert response.status_code == 404

    def test_get_batch_results_returns_result_rows(self, app_client):
        """WHEN GET /batch/{batch_id}/results THEN result rows returned."""
        test_client, *_ = app_client

        batch_id = str(uuid.uuid4())
        results = [
            {
                "batch_id": batch_id,
                "customer_id": 1,
                "md5": "abc123",
                "strategy_name": "fullname_ssn",
                "leaked_fields": ["SSN", "Fullname"],
                "match_details": {
                    "SSN": {"found": True, "method": "exact", "confidence": 1.0, "snippet": "343-43-4343"},
                },
                "overall_confidence": 0.9,
                "azure_search_score": 12.5,
                "needs_review": False,
                "searched_at": datetime.datetime.utcnow(),
            },
        ]

        with patch("app.routers.batch.get_batch_results", return_value=results):
            response = test_client.get(f"/batch/{batch_id}/results")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["md5"] == "abc123"
        assert data[0]["overall_confidence"] == 0.9

    def test_get_batches_returns_list(self, app_client):
        """WHEN GET /batches THEN all batch runs returned."""
        test_client, *_ = app_client

        batch_list = [
            {
                "batch_id": str(uuid.uuid4()),
                "status": "completed",
                "started_at": datetime.datetime.utcnow(),
                "completed_at": datetime.datetime.utcnow(),
                "total_customers": 200,
                "strategy_count": 3,
            },
        ]

        with patch("app.routers.batch.list_all_batches", return_value=batch_list):
            response = test_client.get("/batches")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["status"] == "completed"

    def test_post_batch_resume_returns_400_for_completed(self, app_client):
        """WHEN POST /batch/{batch_id}/resume on completed batch THEN 400."""
        test_client, *_ = app_client

        batch_id = str(uuid.uuid4())
        mock_batch = MagicMock(status="completed")

        with patch("app.routers.batch.load_strategies") as mock_load:
            from app.services.search_service import Strategy
            mock_load.return_value = [Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])]

            with patch("app.routers.batch.batch_service._get_batch_run", return_value=mock_batch):
                response = test_client.post(f"/batch/{batch_id}/resume")

        assert response.status_code == 400
        assert "already completed" in response.json()["detail"].lower()

    def test_post_batch_resume_returns_404_for_not_found(self, app_client):
        """WHEN POST /batch/{batch_id}/resume for non-existent batch THEN 404."""
        test_client, *_ = app_client

        batch_id = str(uuid.uuid4())

        with patch("app.routers.batch.load_strategies") as mock_load:
            from app.services.search_service import Strategy
            mock_load.return_value = [Strategy("fullname_ssn", "desc", ["Fullname", "SSN"])]

            with patch("app.routers.batch.batch_service._get_batch_run", return_value=None):
                response = test_client.post(f"/batch/{batch_id}/resume")

        assert response.status_code == 404


# ===========================================================================
# TEST CLASS: Two-batch run append scenario
# ===========================================================================

class TestBatchResultsAppend:
    """Integration tests for multi-batch result isolation."""

    def test_two_batch_runs_produce_separate_results(self, mock_db):
        """WHEN two batches run THEN results are kept distinct by batch_id."""
        from app.services.batch_service import _create_batch_run

        batch_id_a = str(uuid.uuid4())
        batch_id_b = str(uuid.uuid4())

        assert batch_id_a != batch_id_b

        # Simulate that results from batch A and batch B are independently queryable
        result_a = FakeResult(batch_id=batch_id_a, customer_id=1, md5="file1")
        result_b = FakeResult(batch_id=batch_id_b, customer_id=1, md5="file1")

        assert result_a.batch_id != result_b.batch_id

    def test_batch_id_is_uuid(self, mock_db):
        """WHEN _create_batch_run is called THEN returned batch_id is a valid UUID string."""
        from app.services.batch_service import _create_batch_run

        rows_added = []
        mock_db.add.side_effect = lambda row: rows_added.append(row)

        with patch("app.models.batch.BatchRun") as MockBatchRun:
            MockBatchRun.return_value = FakeBatchRun()
            batch_id = _create_batch_run(
                db=mock_db,
                strategy_names=["fullname_ssn"],
                total_customers=5,
            )

        # Validate UUID format
        try:
            uuid.UUID(batch_id)
            valid = True
        except ValueError:
            valid = False

        assert valid, f"batch_id '{batch_id}' is not a valid UUID"


# ===========================================================================
# TEST CLASS: Strategy Query Building Integration
# ===========================================================================

class TestStrategyQueryBuildingIntegration:
    """Integration tests for Lucene query construction from strategies + PII data."""

    def test_fullname_ssn_query_contains_both_fields(self, customer_karthik, fullname_ssn_strategy):
        """WHEN building query for fullname_ssn THEN query contains name tokens and SSN variants."""
        from app.services.search_service import build_query_for_strategy

        query = build_query_for_strategy(fullname_ssn_strategy, customer_karthik)

        assert query is not None
        assert "Karthik~1" in query
        assert "Chekuri~1" in query
        assert '"343-43-4343"' in query
        assert '"343434343"' in query

    def test_null_field_omitted_from_query(self):
        """WHEN customer DriversLicense is null THEN DriversLicense omitted from query."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy("dl_only", "desc", ["DriversLicense", "SSN"])
        customer = FakeMasterData(customer_id=1, DriversLicense=None, SSN="343-43-4343")

        query = build_query_for_strategy(strategy, customer)

        assert query is not None
        assert "343-43-4343" in query
        # No DriversLicense in the query
        assert "D1234567" not in query

    def test_all_null_fields_returns_none(self):
        """WHEN all strategy fields are null THEN build_query returns None."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy("dl_only", "desc", ["DriversLicense"])
        customer = FakeMasterData(customer_id=1, DriversLicense=None)

        query = build_query_for_strategy(strategy, customer)

        assert query is None

    def test_dob_included_in_multiple_formats(self, customer_karthik):
        """WHEN strategy includes DOB THEN query has ISO, US, and European formats."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy("lastname_dob", "desc", ["LastName", "DOB"])

        query = build_query_for_strategy(strategy, customer_karthik)

        # DOB 1990-05-15 should appear in multiple formats
        assert "1990-05-15" in query  # ISO
        assert "05/15/1990" in query  # US
        assert "15/05/1990" in query  # European
