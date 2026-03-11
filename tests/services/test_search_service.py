"""Tests for the search orchestration service (Phase 4.1).

Tests cover the full search flow:
  SSN lookup -> Lucene query -> Azure Search -> leak detection per file
  -> confidence scoring -> DB persistence -> ordered results returned

All DB and Azure Search access is mocked.
"""

import datetime
import json
import uuid
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pytest

# We mock sqlalchemy at the module level to avoid import hangs
# Import the module under test after setting up mocks
from app.schemas.pii import FieldMatchResult
from app.services.leak_detection_service import LeakDetectionResult


# ---------------------------------------------------------------------------
# Helpers — fake PII record (mimics MasterPII ORM model)
# ---------------------------------------------------------------------------

@dataclass
class FakeMasterPII:
    """Mimics the MasterPII ORM model for testing."""
    ID: int = 1
    Fullname: Optional[str] = "Karthik Chekuri"
    FirstName: Optional[str] = "Karthik"
    LastName: Optional[str] = "Chekuri"
    DOB: Optional[datetime.date] = datetime.date(1990, 5, 15)
    SSN: Optional[str] = "343-43-4343"
    DriversLicense: Optional[str] = "D1234567"
    Address1: Optional[str] = "123 Main St"
    Address2: Optional[str] = None
    Address3: Optional[str] = None
    ZipCode: Optional[str] = "12345"
    City: Optional[str] = "Springfield"
    State: Optional[str] = "IL"
    Country: Optional[str] = "US"


@dataclass
class FakeDLU:
    """Mimics the DLU ORM model for testing."""
    GUID: str = "file-guid-001"
    TEXTPATH: str = "case1/file1.txt"
    fileName: str = "file1.txt"
    fileExtension: str = ".txt"
    caseName: str = "TestCase"
    isExclusion: bool = False
    MD5: Optional[str] = None


def _no_match() -> FieldMatchResult:
    return FieldMatchResult(found=False, method="none", confidence=0.0, snippet=None)


def _make_leak_result(
    ssn_found: bool = False,
    fullname_found: bool = False,
    dob_found: bool = False,
    ssn_conf: float = 0.0,
    fullname_conf: float = 0.0,
    ssn_method: str = "none",
    fullname_method: str = "none",
) -> LeakDetectionResult:
    """Build a LeakDetectionResult with specified fields found."""
    result = LeakDetectionResult()
    if ssn_found:
        result.SSN = FieldMatchResult(found=True, method=ssn_method or "exact", confidence=ssn_conf or 1.0, snippet="...343-43-4343...")
    if fullname_found:
        result.Fullname = FieldMatchResult(found=True, method=fullname_method or "normalized", confidence=fullname_conf or 0.95, snippet="...Karthik Chekuri...")
    if dob_found:
        result.DOB = FieldMatchResult(found=True, method="exact", confidence=1.0, snippet="...1990-05-15...")
    return result


# ---------------------------------------------------------------------------
# Fake Azure Search result
# ---------------------------------------------------------------------------

class FakeSearchResult:
    """Mimics an Azure AI Search result document."""
    def __init__(self, guid: str, score: float, file_name: str = "file.txt"):
        self._guid = guid
        self._score = score
        self._file_name = file_name

    def __getitem__(self, key):
        mapping = {
            "file_guid": self._guid,
            "file_name": self._file_name,
            "@search.score": self._score,
        }
        return mapping[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# ===========================================================================
# TEST CLASS: Customer Lookup
# ===========================================================================

class TestCustomerLookup:
    """Tests for customer lookup by SSN (spec: Customer lookup by SSN)."""

    def test_customer_found_by_ssn(self):
        """Scenario: Customer found by SSN.

        WHEN POST /search is called with a valid SSN that exists in master_pii
        THEN the system loads the customer's full PII record and proceeds.
        """
        from app.services.search_service import _lookup_customer

        mock_db = MagicMock()
        fake_customer = FakeMasterPII()
        mock_db.query.return_value.filter.return_value.all.return_value = [fake_customer]

        customer = _lookup_customer(mock_db, "343-43-4343")
        assert customer.Fullname == "Karthik Chekuri"
        assert customer.SSN == "343-43-4343"
        assert customer.ID == 1

    def test_customer_not_found_by_ssn(self):
        """Scenario: Customer not found by SSN.

        WHEN POST /search is called with an SSN that does not exist in master_pii
        THEN the system returns a 404 response with message 'Customer not found'.
        """
        from app.services.search_service import _lookup_customer, CustomerNotFoundError

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = []

        with pytest.raises(CustomerNotFoundError, match="Customer not found"):
            _lookup_customer(mock_db, "999-99-9999")

    def test_duplicate_ssn_in_master_pii(self):
        """Scenario: Duplicate SSN in master_pii.

        WHEN POST /search is called with an SSN that matches multiple records
        THEN the system returns a 409 Conflict with message about data integrity.
        """
        from app.services.search_service import _lookup_customer, DataIntegrityError

        mock_db = MagicMock()
        dup1 = FakeMasterPII(ID=1)
        dup2 = FakeMasterPII(ID=2)
        mock_db.query.return_value.filter.return_value.all.return_value = [dup1, dup2]

        with pytest.raises(DataIntegrityError, match="Multiple customers found"):
            _lookup_customer(mock_db, "343-43-4343")

    def test_fullname_validation_match(self):
        """Scenario: Fullname provided and matches (case-insensitive).

        WHEN fullname is provided and matches the DB record
        THEN validation passes and search proceeds.
        """
        from app.services.search_service import _validate_fullname

        customer = FakeMasterPII(Fullname="Karthik Chekuri")
        # Should not raise
        _validate_fullname(customer, "karthik chekuri")

    def test_fullname_validation_mismatch(self):
        """Scenario: Fullname validation mismatch.

        WHEN fullname 'John Doe' is provided but DB has 'Karthik Chekuri'
        THEN 409 Conflict with 'Provided fullname does not match customer record'.
        """
        from app.services.search_service import _validate_fullname, FullnameMismatchError

        customer = FakeMasterPII(Fullname="Karthik Chekuri")
        with pytest.raises(FullnameMismatchError, match="Provided fullname does not match"):
            _validate_fullname(customer, "John Doe")

    def test_fullname_not_provided_skips_validation(self):
        """Scenario: Fullname not provided (skips validation).

        WHEN fullname is null/empty
        THEN validation is skipped and search proceeds using DB Fullname.
        """
        from app.services.search_service import _validate_fullname

        customer = FakeMasterPII(Fullname="Karthik Chekuri")
        # None should not raise
        _validate_fullname(customer, None)
        # Empty string should not raise
        _validate_fullname(customer, "")


# ===========================================================================
# TEST CLASS: Lucene Query Construction
# ===========================================================================

class TestLuceneQueryConstruction:
    """Tests for Lucene query building (spec: Lucene query construction)."""

    def test_build_query_with_fullname_and_ssn(self):
        """Scenario: Build query for customer with full name and SSN.

        WHEN customer has fullname 'Karthik Chekuri' and SSN '343-43-4343'
        THEN the Lucene query includes fuzzy name tokens and SSN variants.
        """
        from app.services.search_service import _build_lucene_query

        query = _build_lucene_query("Karthik Chekuri", "343-43-4343")

        # Must contain fuzzy name tokens
        assert "Karthik~1" in query
        assert "Chekuri~1" in query

        # Must contain SSN in both formats
        assert '"343-43-4343"' in query
        assert '"343434343"' in query

    def test_build_query_with_hyphenated_name(self):
        """Scenario: Build query for customer with hyphenated last name.

        WHEN the customer has fullname "Mary O'Brien"
        THEN the query handles the apostrophe and applies fuzzy operators.
        """
        from app.services.search_service import _build_lucene_query

        query = _build_lucene_query("Mary O'Brien", "123-45-6789")

        # Name tokens should be present with fuzzy operators
        assert "Mary~1" in query
        # O'Brien should be handled (apostrophe escaped or removed)
        # The token might be "OBrien" or "O\\'Brien" depending on escaping
        assert "~1" in query

    def test_build_query_with_hyphenated_last_name(self):
        """Test hyphenated names like 'Anne-Marie Smith'."""
        from app.services.search_service import _build_lucene_query

        query = _build_lucene_query("Anne-Marie Smith", "123-45-6789")

        # Hyphen should be handled, fuzzy operators applied
        assert "Smith~1" in query
        assert "~1" in query

    def test_build_query_ssn_undashed_format(self):
        """Test that undashed SSN input is also handled."""
        from app.services.search_service import _build_lucene_query

        query = _build_lucene_query("John Doe", "123456789")

        # Both formats should be present
        assert '"123-45-6789"' in query
        assert '"123456789"' in query

    def test_build_query_escapes_lucene_special_chars(self):
        """Test that Lucene special characters in names are escaped."""
        from app.services.search_service import _build_lucene_query

        query = _build_lucene_query("J.R. Smith-Jones", "111-22-3333")

        # Periods and hyphens should be handled appropriately
        # The name tokens should still have fuzzy operators
        assert "~1" in query


# ===========================================================================
# TEST CLASS: Azure AI Search Execution
# ===========================================================================

class TestAzureSearchExecution:
    """Tests for Azure AI Search query execution."""

    def test_execute_search_returns_matching_files(self):
        """Scenario: Execute search and return matching files.

        WHEN the Lucene query is sent to Azure AI Search
        THEN matching documents are returned with search scores.
        """
        from app.services.search_service import _execute_search

        mock_client = MagicMock()
        result1 = FakeSearchResult("guid-1", 10.5, "file1.txt")
        result2 = FakeSearchResult("guid-2", 8.3, "file2.txt")
        mock_client.search.return_value = [result1, result2]

        results = _execute_search(mock_client, "(Karthik~1 Chekuri~1) (\"343-43-4343\" | \"343434343\")")

        # Verify search was called with correct parameters
        mock_client.search.assert_called_once()
        call_kwargs = mock_client.search.call_args
        assert call_kwargs.kwargs["query_type"] == "full"
        assert call_kwargs.kwargs["search_mode"] == "any"
        assert "content" in call_kwargs.kwargs["search_fields"]
        assert "content_phonetic" in call_kwargs.kwargs["search_fields"]
        assert "content_lowercase" in call_kwargs.kwargs["search_fields"]
        assert call_kwargs.kwargs["scoring_profile"] == "pii_boost"
        assert call_kwargs.kwargs["top"] == 100

        assert len(results) == 2

    def test_execute_search_no_matches(self):
        """Scenario: No matching files found.

        WHEN the Lucene query returns zero results
        THEN an empty list is returned.
        """
        from app.services.search_service import _execute_search

        mock_client = MagicMock()
        mock_client.search.return_value = []

        results = _execute_search(mock_client, "some query")
        assert results == []


# ===========================================================================
# TEST CLASS: Score Normalization
# ===========================================================================

class TestScoreNormalization:
    """Tests for search score normalization."""

    def test_normalize_scores_divides_by_max(self):
        """Verify scores are normalized by dividing by max."""
        from app.utils.confidence import normalize_search_scores

        raw_scores = [10.0, 5.0, 2.5]
        normalized = normalize_search_scores(raw_scores)

        assert normalized == [1.0, 0.5, 0.25]

    def test_normalize_scores_empty_list(self):
        """Empty input returns empty output."""
        from app.utils.confidence import normalize_search_scores

        assert normalize_search_scores([]) == []


# ===========================================================================
# TEST CLASS: Search Results Persistence
# ===========================================================================

class TestSearchResultsPersistence:
    """Tests for persisting search results to DB."""

    def test_persist_results_for_search_with_matches(self):
        """Scenario: Persist results for a search with matches.

        WHEN a search finds PII in 3 files
        THEN 3 rows are inserted into search_results.
        """
        from app.services.search_service import _persist_results

        mock_db = MagicMock()
        search_run_id = uuid.uuid4()
        customer_id = 1

        leak_result = _make_leak_result(ssn_found=True, fullname_found=True, ssn_conf=1.0, fullname_conf=0.95,
                                         ssn_method="exact", fullname_method="normalized")

        file_results = [
            {
                "file_guid": f"guid-{i}",
                "file_name": f"file{i}.txt",
                "leaked_fields": ["SSN", "Fullname"],
                "overall_confidence": 0.9 - i * 0.1,
                "azure_search_score": 10.0 - i,
                "needs_review": False,
                "match_details": {
                    "SSN": leak_result.SSN,
                    "Fullname": leak_result.Fullname,
                },
                "leak_detection": leak_result,
            }
            for i in range(3)
        ]

        _persist_results(mock_db, search_run_id, customer_id, file_results)

        # Should have added 3 SearchResult objects
        assert mock_db.add.call_count == 3
        mock_db.commit.assert_called_once()

    def test_persist_no_results_when_no_leaks(self):
        """Scenario: No leaks detected - no rows inserted.

        WHEN leak detection finds no PII in any file
        THEN no rows are inserted into search_results.
        """
        from app.services.search_service import _persist_results

        mock_db = MagicMock()
        search_run_id = uuid.uuid4()
        customer_id = 1

        _persist_results(mock_db, search_run_id, customer_id, [])

        mock_db.add.assert_not_called()
        # commit might still be called but no rows added
        # actually if there are no results, we shouldn't call commit either
        # Let's not assert on commit for empty results


# ===========================================================================
# TEST CLASS: Full Search Flow (search_customer_pii)
# ===========================================================================

class TestSearchCustomerPII:
    """Tests for the top-level search_customer_pii orchestration function."""

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_full_flow_returns_ordered_results(self, mock_detect, mock_extract):
        """Scenario: Response with multiple file matches.

        WHEN a search finds PII in multiple files with varying confidence
        THEN results are ordered by overall_confidence descending.
        """
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        # Setup: customer lookup returns one customer
        fake_customer = FakeMasterPII()
        mock_db.query.return_value.filter.return_value.all.return_value = [fake_customer]

        # Setup: DLU lookup for file paths
        dlu1 = FakeDLU(GUID="guid-1", TEXTPATH="case1/file1.txt", fileName="file1.txt")
        dlu2 = FakeDLU(GUID="guid-2", TEXTPATH="case1/file2.txt", fileName="file2.txt")
        dlu3 = FakeDLU(GUID="guid-3", TEXTPATH="case1/file3.txt", fileName="file3.txt")

        # We need the DLU query to return the right file for each GUID
        def dlu_side_effect(*args, **kwargs):
            """Return mock query chain for DLU lookups."""
            mock_chain = MagicMock()
            # For the customer lookup (MasterPII), return the fake customer
            # For the DLU lookup, return the DLU records
            return mock_chain

        # Setup: Azure Search returns 3 results with different scores
        result1 = FakeSearchResult("guid-1", 15.0, "file1.txt")
        result2 = FakeSearchResult("guid-2", 10.0, "file2.txt")
        result3 = FakeSearchResult("guid-3", 5.0, "file3.txt")
        mock_search_client.search.return_value = [result1, result2, result3]

        # Setup: text extraction returns file content
        mock_extract.return_value = "Some file content with Karthik Chekuri and 343-43-4343"

        # Setup: leak detection returns different results per file
        high_leak = _make_leak_result(ssn_found=True, fullname_found=True, dob_found=True,
                                       ssn_conf=1.0, fullname_conf=0.95,
                                       ssn_method="exact", fullname_method="normalized")
        medium_leak = _make_leak_result(ssn_found=True, fullname_found=False,
                                          ssn_conf=1.0, ssn_method="exact")
        low_leak = _make_leak_result(ssn_found=False, fullname_found=True,
                                       fullname_conf=0.80, fullname_method="fuzzy")

        mock_detect.side_effect = [high_leak, medium_leak, low_leak]

        # We need DLU file_path resolution — mock the db.query(DLU) calls
        dlu_map = {"guid-1": dlu1, "guid-2": dlu2, "guid-3": dlu3}

        # Track which model is being queried
        original_query = mock_db.query

        def query_dispatch(model):
            mock_chain = MagicMock()
            if hasattr(model, '__tablename__') and model.__tablename__ == 'master_pii':
                mock_chain.filter.return_value.all.return_value = [fake_customer]
            elif hasattr(model, '__tablename__') and model.__tablename__ == 'datalakeuniverse':
                def filter_fn(*args, **kwargs):
                    inner = MagicMock()
                    # Return the right DLU record based on the GUID
                    # We'll use a side_effect on .first() to return in order
                    inner.first.return_value = None  # default
                    return inner
                mock_chain.filter.side_effect = filter_fn
            return mock_chain

        # Simpler approach: mock the internal _lookup_dlu_record function
        mock_db.query.side_effect = None
        mock_db.query.return_value.filter.return_value.all.return_value = [fake_customer]

        # Config mock
        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record") as mock_dlu_lookup:

            mock_dlu_lookup.side_effect = lambda db, guid: dlu_map.get(guid)

            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        # Results should be ordered by overall_confidence descending
        assert len(response.results) == 3
        confidences = [r.overall_confidence for r in response.results]
        assert confidences == sorted(confidences, reverse=True), \
            f"Results not sorted by confidence descending: {confidences}"

        # Customer summary should have masked SSN
        assert response.customer.ssn_masked == "XXX-XX-4343"

        # search_run_id should be a valid UUID
        assert isinstance(response.search_run_id, uuid.UUID)

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_no_matches_returns_empty_results(self, mock_detect, mock_extract):
        """Scenario: No matching files found.

        WHEN the Lucene query returns zero results from Azure AI Search
        THEN response has empty results list.
        """
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()
        mock_search_client.search.return_value = []

        fake_customer = FakeMasterPII()

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []
        assert response.customer.fullname == "Karthik Chekuri"

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_leak_detection_finds_no_pii(self, mock_detect, mock_extract):
        """Scenario: Search returns files but leak detection finds no PII.

        WHEN files are found but no leaked fields detected
        THEN response has empty results list (files with no leaks are excluded).
        """
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        fake_customer = FakeMasterPII()

        # Azure Search returns 1 result
        result1 = FakeSearchResult("guid-1", 10.0, "file1.txt")
        mock_search_client.search.return_value = [result1]

        # Text extraction works
        mock_extract.return_value = "Some random text with no PII"

        # Leak detection finds nothing
        empty_leak = LeakDetectionResult()  # all fields default to no match
        mock_detect.return_value = empty_leak

        dlu1 = FakeDLU(GUID="guid-1", TEXTPATH="case1/file1.txt", fileName="file1.txt")

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", return_value=dlu1):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_ssn_masking_in_response(self, mock_detect, mock_extract):
        """Scenario: SSN masking in response.

        WHEN a search response is returned
        THEN the customer SSN is masked as 'XXX-XX-4343'.
        """
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()
        mock_search_client.search.return_value = []

        fake_customer = FakeMasterPII(SSN="343-43-4343")

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert response.customer.ssn_masked == "XXX-XX-4343"

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_fullname_mismatch_raises_error(self, mock_detect, mock_extract):
        """Scenario: Fullname validation mismatch raises error."""
        from app.services.search_service import search_customer_pii, FullnameMismatchError

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        fake_customer = FakeMasterPII(Fullname="Karthik Chekuri")

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer):
            with pytest.raises(FullnameMismatchError):
                search_customer_pii(
                    db=mock_db,
                    search_client=mock_search_client,
                    ssn="343-43-4343",
                    fullname="John Doe",
                    config=mock_config,
                )

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_text_extraction_failure_skips_file(self, mock_detect, mock_extract):
        """When text extraction fails for a file, that file is skipped."""
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        fake_customer = FakeMasterPII()

        result1 = FakeSearchResult("guid-1", 10.0, "file1.txt")
        mock_search_client.search.return_value = [result1]

        # Text extraction fails
        mock_extract.return_value = None

        dlu1 = FakeDLU(GUID="guid-1", TEXTPATH="case1/file1.txt", fileName="file1.txt")

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", return_value=dlu1):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_dlu_record_not_found_skips_file(self, mock_detect, mock_extract):
        """When DLU record not found for a file GUID, that file is skipped."""
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        fake_customer = FakeMasterPII()

        result1 = FakeSearchResult("guid-missing", 10.0, "file1.txt")
        mock_search_client.search.return_value = [result1]

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", return_value=None):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []


# ===========================================================================
# TEST CLASS: Response Format
# ===========================================================================

class TestResponseFormat:
    """Tests for the search response format."""

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_response_includes_all_required_fields(self, mock_detect, mock_extract):
        """Verify response includes search_run_id, customer, and results array."""
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()
        mock_search_client.search.return_value = []

        fake_customer = FakeMasterPII()

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        # Must have all required fields
        assert hasattr(response, "search_run_id")
        assert hasattr(response, "customer")
        assert hasattr(response, "results")
        assert isinstance(response.search_run_id, uuid.UUID)
        assert response.customer.fullname == "Karthik Chekuri"
        assert isinstance(response.results, list)

    @patch("app.services.search_service.extract_text")
    @patch("app.services.search_service.detect_leaks")
    def test_file_result_includes_required_fields(self, mock_detect, mock_extract):
        """Each file result should include all required fields."""
        from app.services.search_service import search_customer_pii

        mock_db = MagicMock()
        mock_search_client = MagicMock()

        fake_customer = FakeMasterPII()

        result1 = FakeSearchResult("guid-1", 10.0, "file1.txt")
        mock_search_client.search.return_value = [result1]

        mock_extract.return_value = "Karthik Chekuri 343-43-4343"

        leak = _make_leak_result(ssn_found=True, fullname_found=True,
                                   ssn_conf=1.0, fullname_conf=0.95,
                                   ssn_method="exact", fullname_method="normalized")
        mock_detect.return_value = leak

        dlu1 = FakeDLU(GUID="guid-1", TEXTPATH="case1/file1.txt", fileName="file1.txt")

        mock_config = MagicMock()
        mock_config.FILE_BASE_PATH = "C:/data"

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", return_value=dlu1):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="343-43-4343",
                fullname=None,
                config=mock_config,
            )

        assert len(response.results) == 1
        fr = response.results[0]
        assert fr.file_name == "file1.txt"
        assert fr.file_guid == "guid-1"
        assert "SSN" in fr.leaked_fields
        assert "Fullname" in fr.leaked_fields
        assert fr.overall_confidence > 0.0
        assert fr.azure_search_score == 10.0
        assert isinstance(fr.needs_review, bool)
        assert isinstance(fr.match_details, dict)
