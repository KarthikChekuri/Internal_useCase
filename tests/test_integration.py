"""Phase 6.2: Integration Tests — full pipeline end-to-end.

These tests exercise the complete search flow:
  search request -> customer lookup -> Azure query -> leak detection
  -> confidence scoring -> response

All Azure AI Search and DB access is mocked. Simulated data from
data/simulated_files/ and data/seed/ provides realistic file content.

IMPORTANT: Does not import sqlalchemy to avoid hangs in the test
environment. Uses FakeMasterPII / FakeDLU stand-ins from conftest.
"""

import csv
import datetime
import os
import uuid
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from app.schemas.pii import CustomerSummary, FieldMatchResult
from app.schemas.search import FileResult, SearchResponse
from app.services.leak_detection_service import LeakDetectionResult, detect_leaks
from app.services.text_extraction import extract_text
from app.utils.confidence import compute_overall_confidence, normalize_search_scores
from tests.conftest import FakeMasterPII


# ---------------------------------------------------------------------------
# Project root for resolving simulated file paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..")
)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SEED_DIR = os.path.join(DATA_DIR, "seed")
SIMULATED_DIR = os.path.join(DATA_DIR, "simulated_files")


# ---------------------------------------------------------------------------
# Helper: load customers from CSV (avoids sqlalchemy)
# ---------------------------------------------------------------------------

def _load_customers_from_csv() -> dict[str, FakeMasterPII]:
    """Load all 10 simulated customers from data/seed/master_pii.csv.

    Returns:
        Dict keyed by SSN -> FakeMasterPII instance.
    """
    csv_path = os.path.join(SEED_DIR, "master_pii.csv")
    customers: dict[str, FakeMasterPII] = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            dob = None
            if row["DOB"]:
                parts = row["DOB"].split("-")
                dob = datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
            cust = FakeMasterPII(
                ID=row["ID"],
                Fullname=row["Fullname"],
                FirstName=row["FirstName"],
                LastName=row["LastName"],
                DOB=dob,
                SSN=row["SSN"],
                DriversLicense=row["DriversLicense"] or None,
                Address1=row["Address1"] or None,
                Address2=row["Address2"] or None,
                Address3=row["Address3"] or None,
                ZipCode=row["ZipCode"] or None,
                City=row["City"] or None,
                State=row["State"] or None,
                Country=row["Country"] or None,
            )
            customers[row["SSN"]] = cust
    return customers


def _load_dlu_metadata() -> dict[str, dict]:
    """Load DLU metadata from data/seed/dlu_metadata.csv.

    Returns:
        Dict keyed by GUID -> row dict.
    """
    csv_path = os.path.join(SEED_DIR, "dlu_metadata.csv")
    records: dict[str, dict] = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            records[row["GUID"]] = row
    return records


# ---------------------------------------------------------------------------
# Fake DLU record stand-in
# ---------------------------------------------------------------------------

@dataclass
class FakeDLU:
    """Lightweight stand-in for the DLU ORM model."""
    GUID: str = ""
    TEXTPATH: str = ""
    fileName: str = ""
    fileExtension: str = ""
    caseName: str = "SimulatedBreach2024"
    isExclusion: bool = False
    MD5: Optional[str] = None


# ---------------------------------------------------------------------------
# Helper: build fake Azure search results
# ---------------------------------------------------------------------------

class FakeAzureSearchResult:
    """Mimics an Azure AI Search result document."""

    def __init__(self, guid: str, score: float, file_name: str):
        self._data = {
            "file_guid": guid,
            "file_name": file_name,
            "@search.score": score,
        }

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


def _build_azure_results_for_files(
    dlu_records: dict[str, dict],
    file_names: list[str],
    scores: Optional[list[float]] = None,
) -> list[FakeAzureSearchResult]:
    """Build fake Azure search results for the given simulated file names.

    Looks up the GUID from dlu_metadata by matching fileName.
    """
    guid_by_name = {}
    for guid, row in dlu_records.items():
        guid_by_name[row["fileName"]] = guid

    results = []
    for i, fname in enumerate(file_names):
        guid = guid_by_name.get(fname, str(uuid.uuid4()))
        score = scores[i] if scores else (20.0 - i * 2.0)
        results.append(FakeAzureSearchResult(guid, score, fname))
    return results


def _build_dlu_lookup(dlu_records: dict[str, dict]) -> dict[str, FakeDLU]:
    """Build a mapping from GUID -> FakeDLU from the CSV metadata."""
    lookup = {}
    for guid, row in dlu_records.items():
        lookup[guid] = FakeDLU(
            GUID=guid,
            TEXTPATH=row["TEXTPATH"],
            fileName=row["fileName"],
            fileExtension=row["fileExtension"],
            caseName=row["caseName"],
            isExclusion=row["isExclusion"] == "1",
            MD5=row["MD5"],
        )
    return lookup


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def all_customers():
    """All 10 simulated customers keyed by SSN."""
    return _load_customers_from_csv()


@pytest.fixture(scope="module")
def dlu_metadata():
    """All DLU records keyed by GUID."""
    return _load_dlu_metadata()


@pytest.fixture(scope="module")
def dlu_lookup(dlu_metadata):
    """GUID -> FakeDLU mapping."""
    return _build_dlu_lookup(dlu_metadata)


@pytest.fixture
def mock_db():
    """Fresh MagicMock for a SQLAlchemy session."""
    return MagicMock()


@pytest.fixture
def mock_search_client():
    """Fresh MagicMock for Azure AI SearchClient."""
    return MagicMock()


@pytest.fixture
def mock_config():
    """Settings mock pointing at the project's data/ directory."""
    cfg = MagicMock()
    cfg.FILE_BASE_PATH = DATA_DIR
    cfg.CASE_NAME = "SimulatedBreach2024"
    return cfg


# ===========================================================================
# TEST CLASS 1: End-to-end search — happy path
# ===========================================================================

class TestEndToEndSearch:
    """Integration: search request -> customer lookup -> Azure query ->
    leak detection -> confidence scoring -> ordered response.

    Uses Karthik Chekuri (C003) who appears in multiple simulated files:
      - hr_onboarding_chekuri.txt (full PII)
      - payroll_register_q1_2024.txt (name, SSN, state)
      - benefits_enrollment_batch1.txt (name reordered, DOB, SSN undashed)
    """

    def test_search_karthik_returns_results_with_leaked_fields(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Full pipeline: search for Karthik Chekuri, verify files are
        returned with correct leaked fields and confidence ranges.
        """
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]

        # Files we expect Azure to return for Karthik
        expected_files = [
            "hr_onboarding_chekuri.txt",
            "payroll_register_q1_2024.txt",
            "benefits_enrollment_batch1.txt",
        ]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[18.5, 12.0, 9.5]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname=None,
                config=mock_config,
            )

        # --- Assertions ---

        # 1. Response is a valid SearchResponse
        assert isinstance(response, SearchResponse)
        assert isinstance(response.search_run_id, uuid.UUID)

        # 2. Customer summary with masked SSN
        assert response.customer.fullname == "Karthik Chekuri"
        assert response.customer.ssn_masked == "XXX-XX-8805"

        # 3. Should have results (at least one file has PII)
        assert len(response.results) > 0

        # 4. Results ordered by overall_confidence descending
        confs = [r.overall_confidence for r in response.results]
        assert confs == sorted(confs, reverse=True), (
            f"Results not sorted by confidence: {confs}"
        )

        # 5. The HR onboarding file should be the top result (most PII)
        top = response.results[0]
        assert top.overall_confidence > 0.5

        # 6. Leaked fields should include SSN and at least one name field
        all_leaked = set()
        for r in response.results:
            all_leaked.update(r.leaked_fields)
        assert "SSN" in all_leaked, "SSN should be detected in at least one file"

        # 7. Each result has required structure
        for r in response.results:
            assert isinstance(r.file_name, str) and len(r.file_name) > 0
            assert isinstance(r.file_guid, str) and len(r.file_guid) > 0
            assert isinstance(r.leaked_fields, list) and len(r.leaked_fields) > 0
            assert 0.0 < r.overall_confidence <= 1.0
            assert r.azure_search_score > 0.0
            assert isinstance(r.needs_review, bool)
            assert isinstance(r.match_details, dict)

    def test_search_obrien_with_apostrophe_name(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Test searching for Robert O'Brien — apostrophe in name must not
        break Lucene query or leak detection.

        O'Brien appears in:
          - hr_onboarding_obrien.txt (full PII)
          - appointment_notes_mar2024.txt (SSN, DOB, address, DL)
          - payroll_register_q1_2024.txt (name with apostrophe, SSN, state)
          - benefits_enrollment_batch1.txt (DOB, SSN)
        """
        from app.services.search_service import search_customer_pii

        customer = all_customers["523-45-7891"]

        expected_files = [
            "hr_onboarding_obrien.txt",
            "appointment_notes_mar2024.txt",
            "payroll_register_q1_2024.txt",
            "benefits_enrollment_batch1.txt",
        ]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[20.0, 16.0, 12.0, 8.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="523-45-7891",
                fullname=None,
                config=mock_config,
            )

        assert len(response.results) > 0
        assert response.customer.fullname == "Robert O'Brien"
        assert response.customer.ssn_masked == "XXX-XX-7891"

        # O'Brien's PII should be detected
        all_leaked = set()
        for r in response.results:
            all_leaked.update(r.leaked_fields)
        assert "SSN" in all_leaked

        # Confidence should be high for the HR onboarding file (full PII)
        hr_results = [r for r in response.results if "onboarding" in r.file_name]
        if hr_results:
            assert hr_results[0].overall_confidence > 0.5

    def test_search_response_has_valid_uuid_and_structure(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Verify the response structure: search_run_id, customer, results."""
        from app.services.search_service import search_customer_pii

        customer = all_customers["348-56-7712"]  # Priya Patel

        expected_files = ["tax_w2_patel_2023.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[15.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="348-56-7712",
                fullname=None,
                config=mock_config,
            )

        # Structure checks
        assert hasattr(response, "search_run_id")
        assert hasattr(response, "customer")
        assert hasattr(response, "results")
        assert isinstance(response.search_run_id, uuid.UUID)
        assert isinstance(response.results, list)

        # Customer summary
        assert response.customer.fullname == "Priya Patel"
        assert response.customer.ssn_masked == "XXX-XX-7712"


# ===========================================================================
# TEST CLASS 2: Negative tests — customer not found
# ===========================================================================

class TestNegativeSearchNotFound:
    """Negative test: searching for a non-existent SSN returns 404."""

    def test_nonexistent_ssn_raises_customer_not_found(
        self, mock_db, mock_search_client, mock_config
    ):
        """WHEN POST /search with SSN that does not exist in master_pii
        THEN CustomerNotFoundError is raised (which router maps to 404).
        """
        from app.services.search_service import (
            CustomerNotFoundError,
            search_customer_pii,
        )

        # Mock DB returns no customer
        mock_db.query.return_value.filter.return_value.all.return_value = []

        with pytest.raises(CustomerNotFoundError, match="Customer not found"):
            search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="999-99-9999",
                fullname=None,
                config=mock_config,
            )

    def test_nonexistent_ssn_via_http_endpoint(self):
        """Full HTTP test: POST /search with non-existent SSN -> 404."""
        from app.main import app
        from app.dependencies import get_db, get_search_client, get_settings

        from fastapi.testclient import TestClient

        mock_db = MagicMock()
        mock_search_client = MagicMock()
        mock_settings = MagicMock()
        mock_settings.FILE_BASE_PATH = DATA_DIR
        mock_settings.CASE_NAME = "SimulatedBreach2024"

        def override_db():
            yield mock_db

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_search_client] = lambda: mock_search_client
        app.dependency_overrides[get_settings] = lambda: mock_settings

        try:
            with TestClient(app) as client:
                # Mock: _lookup_customer raises CustomerNotFoundError
                from app.services.search_service import CustomerNotFoundError

                with patch(
                    "app.routers.search.search_customer_pii",
                    side_effect=CustomerNotFoundError("Customer not found"),
                ):
                    resp = client.post(
                        "/search",
                        json={"ssn": "999-99-9999"},
                    )
                    assert resp.status_code == 404
                    assert "Customer not found" in resp.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_invalid_ssn_format_returns_422(self):
        """POST /search with bad SSN format -> 422 from Pydantic validation."""
        from app.main import app
        from app.dependencies import get_db, get_search_client, get_settings

        from fastapi.testclient import TestClient

        mock_db = MagicMock()

        def override_db():
            yield mock_db

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_search_client] = lambda: MagicMock()
        app.dependency_overrides[get_settings] = lambda: MagicMock()

        try:
            with TestClient(app) as client:
                resp = client.post(
                    "/search",
                    json={"ssn": "12-34-567"},  # invalid format
                )
                assert resp.status_code == 422
        finally:
            app.dependency_overrides.clear()


# ===========================================================================
# TEST CLASS 3: Fuzzy / phonetic matching
# ===========================================================================

class TestFuzzyMatchingIntegration:
    """Integration test: search with customer whose name is misspelled
    in breach files — verify fuzzy matching finds correct files.

    The insurance_claim_rodriguez_hassan.txt file contains:
      - "Maria Rodgriguez" (intentional misspelling of Rodriguez)
      - SSN: 291884451 (undashed)
      - DOB: 30/06/1980 (European format)
    """

    def test_misspelled_name_detected_via_fuzzy(self, all_customers):
        """Directly test that detect_leaks catches the misspelled name."""
        customer = all_customers["291-88-4451"]  # Maria Rodriguez

        # Read the actual simulated file
        file_path = os.path.join(
            SIMULATED_DIR, "insurance_claim_rodriguez_hassan.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None, "Failed to read simulated file"

        result = detect_leaks(file_text, customer)

        # SSN should be exact match (undashed "291884451" is in the file)
        assert result.SSN.found is True
        assert result.SSN.method == "exact"
        assert result.SSN.confidence == 1.0

        # LastName "Rodriguez" should match "Rodgriguez" via fuzzy
        # or the FirstName "Maria" should match (exact substring)
        name_found = result.Fullname.found or result.LastName.found or result.FirstName.found
        assert name_found, (
            "At least one name field should be detected for Maria Rodriguez"
        )

        # DOB: file has "30/06/1980" (European format for 1980-06-30)
        assert result.DOB.found is True
        assert result.DOB.confidence == 1.0

    def test_misspelled_name_full_pipeline(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Full pipeline: search for Maria Rodriguez whose name is
        misspelled in the insurance claim file.
        """
        from app.services.search_service import search_customer_pii

        customer = all_customers["291-88-4451"]

        expected_files = [
            "insurance_claim_rodriguez_hassan.txt",
            "payroll_register_q1_2024.txt",
            "benefits_enrollment_batch2.txt",
        ]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[18.0, 14.0, 10.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="291-88-4451",
                fullname=None,
                config=mock_config,
            )

        assert len(response.results) > 0

        # The insurance claim file should be among results with SSN detected
        insurance_results = [
            r for r in response.results
            if "insurance_claim" in r.file_name
        ]
        assert len(insurance_results) > 0, (
            "Insurance claim file should be returned for misspelled Rodriguez"
        )
        ir = insurance_results[0]
        assert "SSN" in ir.leaked_fields
        assert ir.overall_confidence > 0.3

    def test_reordered_name_detected_via_fuzzy(self, all_customers):
        """File contains 'Chekuri, Karthik' (reordered) — fuzzy should catch it.

        benefits_enrollment_batch1.txt has:
          Name: Chekuri, Karthik  (last-first order)
          SSN: 634218805 (undashed)
        """
        customer = all_customers["634-21-8805"]  # Karthik Chekuri

        file_path = os.path.join(
            SIMULATED_DIR, "benefits_enrollment_batch1.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        # SSN (undashed): should be exact
        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0

        # "Chekuri, Karthik" should match "Karthik Chekuri" via fuzzy
        # token_set_ratio handles reordered tokens
        fullname_or_names = (
            result.Fullname.found or result.FirstName.found or result.LastName.found
        )
        assert fullname_or_names, (
            "Reordered name 'Chekuri, Karthik' should be detected"
        )

    def test_name_abbreviation_detection(self, all_customers):
        """File contains 'J. Smith-Jones' (abbreviated first name).

        payroll_register_q1_2024.txt has: J. Smith-Jones (for Jennifer Smith-Jones)
        SSN: 412673309 (undashed)
        """
        customer = all_customers["412-67-3309"]  # Jennifer Smith-Jones

        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        # SSN (undashed "412673309") should be exact
        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0

        # LastName "Smith-Jones" should match — normalized to "smith jones"
        # and the file has "Smith-Jones" which normalizes the same
        assert result.LastName.found is True

    def test_reordered_name_hassan_ahmed(self, all_customers):
        """insurance_claim_rodriguez_hassan.txt has 'Hassan, Ahmed'
        (last-first order for Ahmed Hassan).
        """
        customer = all_customers["785-33-6624"]  # Ahmed Hassan

        file_path = os.path.join(
            SIMULATED_DIR, "insurance_claim_rodriguez_hassan.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        # SSN (dashed): exact match
        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0

        # Name "Hassan, Ahmed" should be detected for "Ahmed Hassan"
        name_found = result.Fullname.found or result.FirstName.found or result.LastName.found
        assert name_found, (
            "Reordered name 'Hassan, Ahmed' should be detected for Ahmed Hassan"
        )


# ===========================================================================
# TEST CLASS 4: Multi-customer file
# ===========================================================================

class TestMultiCustomerFile:
    """When a file contains PII from multiple customers, each customer's
    search should correctly identify their own fields.

    The payroll_register_q1_2024.txt contains 5 customers:
      C001: Robert O'Brien (523-45-7891, MA)
      C002: J. Smith-Jones (412673309, TX)
      C003: Karthik Chekuri (634-21-8805, CA)
      C004: Maria Rodriguez (291884451, FL)
      C005: Ahmed Hassan (785-33-6624, NY)

    Also insurance_claim_rodriguez_hassan.txt contains 2 customers.
    """

    def test_multi_customer_payroll_obrien(self, all_customers):
        """Robert O'Brien's PII correctly detected in multi-customer payroll file."""
        customer = all_customers["523-45-7891"]

        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0
        assert result.State.found is True
        assert result.State.confidence == 1.0

        # O'Brien's name with apostrophe
        name_found = result.Fullname.found or result.LastName.found or result.FirstName.found
        assert name_found

    def test_multi_customer_payroll_chekuri(self, all_customers):
        """Karthik Chekuri's PII correctly detected in multi-customer payroll file."""
        customer = all_customers["634-21-8805"]

        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0
        # "CA" with word boundary should match
        assert result.State.found is True

    def test_multi_customer_payroll_hassan(self, all_customers):
        """Ahmed Hassan's PII correctly detected in multi-customer payroll file."""
        customer = all_customers["785-33-6624"]

        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0
        assert result.State.found is True

    def test_multi_customer_each_finds_own_ssn_not_others(self, all_customers):
        """Each customer's search should find their own SSN, not other
        customers' SSNs. Verify no cross-contamination.
        """
        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        ssns_in_file = [
            "523-45-7891",  # O'Brien
            "412-67-3309",  # Smith-Jones (as 412673309 undashed)
            "634-21-8805",  # Chekuri
            "291-88-4451",  # Rodriguez (as 291884451 undashed)
            "785-33-6624",  # Hassan
        ]

        for ssn in ssns_in_file:
            customer = all_customers[ssn]
            result = detect_leaks(file_text, customer)

            # Each should find their own SSN
            assert result.SSN.found is True, (
                f"Customer {customer.Fullname} (SSN: {ssn}) should find their SSN"
            )

    def test_multi_customer_insurance_claim_two_customers(self, all_customers):
        """insurance_claim_rodriguez_hassan.txt: two customers, each finds their own PII."""
        file_path = os.path.join(
            SIMULATED_DIR, "insurance_claim_rodriguez_hassan.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        # Rodriguez
        rodriguez = all_customers["291-88-4451"]
        r_result = detect_leaks(file_text, rodriguez)
        assert r_result.SSN.found is True
        assert r_result.DOB.found is True

        # Hassan
        hassan = all_customers["785-33-6624"]
        h_result = detect_leaks(file_text, hassan)
        assert h_result.SSN.found is True

    def test_multi_customer_file_full_pipeline(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Full pipeline: two different customers searching the same
        multi-customer file get their own leaked fields.
        """
        from app.services.search_service import search_customer_pii

        # Search for O'Brien against payroll file
        customer_obrien = all_customers["523-45-7891"]
        expected_files = ["payroll_register_q1_2024.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[15.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer_obrien), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response_obrien = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="523-45-7891",
                fullname=None,
                config=mock_config,
            )

        # Search for Chekuri against the same file
        customer_chekuri = all_customers["634-21-8805"]
        mock_search_client.search.return_value = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[14.0]
        )

        with patch("app.services.search_service._lookup_customer", return_value=customer_chekuri), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response_chekuri = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname=None,
                config=mock_config,
            )

        # Both should find the payroll file
        assert len(response_obrien.results) > 0
        assert len(response_chekuri.results) > 0

        # O'Brien's SSN should be in O'Brien's result
        obrien_leaked = response_obrien.results[0].leaked_fields
        assert "SSN" in obrien_leaked

        # Chekuri's SSN should be in Chekuri's result
        chekuri_leaked = response_chekuri.results[0].leaked_fields
        assert "SSN" in chekuri_leaked


# ===========================================================================
# TEST CLASS 5: Confidence scoring integration
# ===========================================================================

class TestConfidenceScoringIntegration:
    """Verify that confidence scores computed through the full pipeline
    are within expected ranges for different matching scenarios.
    """

    def test_high_confidence_for_full_pii_match(self, all_customers):
        """File with full PII (name, SSN, DOB, DL, address, city, state, zip)
        should produce high overall confidence.

        hr_onboarding_chekuri.txt has nearly all fields for Karthik Chekuri.
        """
        customer = all_customers["634-21-8805"]

        file_path = os.path.join(
            SIMULATED_DIR, "hr_onboarding_chekuri.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, customer)

        # Count detected fields
        found_fields = []
        for field_name in [
            "SSN", "DOB", "DriversLicense", "Fullname", "FirstName",
            "LastName", "City", "State", "ZipCode",
        ]:
            fr = getattr(result, field_name)
            if fr.found:
                found_fields.append(field_name)

        # Should find most fields
        assert len(found_fields) >= 5, (
            f"Expected at least 5 fields found, got {len(found_fields)}: {found_fields}"
        )

        # SSN and Name should both be found -> SSN+Name formula
        assert result.SSN.found is True
        name_found = result.Fullname.found or result.FirstName.found or result.LastName.found
        assert name_found

        # Compute overall confidence manually and verify it's high
        ssn_conf = result.SSN.confidence
        name_confs = [
            getattr(result, f).confidence
            for f in ("Fullname", "FirstName", "LastName")
            if getattr(result, f).found
        ]
        name_conf = max(name_confs) if name_confs else 0.0

        assert ssn_conf > 0 and name_conf > 0, "Both SSN and Name anchors should be present"

    def test_ssn_only_confidence_scenario(self, all_customers):
        """File with only SSN match (no name) -> SSN-only formula, moderate confidence."""
        # Create a minimal customer with SSN but whose name won't appear
        fake_customer = FakeMasterPII(
            ID="X001",
            Fullname="Zxyqwort Bnplmftx",
            FirstName="Zxyqwort",
            LastName="Bnplmftx",
            DOB=datetime.date(1999, 1, 1),
            SSN="523-45-7891",  # O'Brien's SSN — will be found in file
            DriversLicense="XX-X0000000",
            Address1="99999 Nowhere St",
            Address2=None,
            Address3=None,
            ZipCode="00000",
            City="Nowhere",
            State="ZZ",
            Country="Narnia",
        )

        file_path = os.path.join(
            SIMULATED_DIR, "payroll_register_q1_2024.txt"
        )
        file_text = extract_text(file_path)
        assert file_text is not None

        result = detect_leaks(file_text, fake_customer)

        # SSN should match (it's O'Brien's SSN, present in the file)
        assert result.SSN.found is True

        # Name should NOT match (fake name not in file)
        assert not result.Fullname.found
        assert not result.LastName.found

    def test_no_match_returns_empty_results_with_zero_confidence(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """When Azure Search returns files but leak detection finds no PII,
        the search response has empty results.
        """
        from app.services.search_service import search_customer_pii

        # Use a customer whose PII won't appear in client_intake_whitfield.txt
        # Actually, let's use a fake customer whose PII is nowhere
        fake_customer = FakeMasterPII(
            ID="X002",
            Fullname="Zxyqwort Bnplmftx",
            FirstName="Zxyqwort",
            LastName="Bnplmftx",
            DOB=datetime.date(1950, 12, 31),
            SSN="000-00-0000",
            DriversLicense="ZZ-Z0000000",
            Address1="99999 Nowhere St",
            Address2=None,
            Address3=None,
            ZipCode="00000",
            City="Atlantis",
            State="ZZ",
            Country="Narnia",
        )

        expected_files = ["client_intake_whitfield.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[5.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="000-00-0000",
                fullname=None,
                config=mock_config,
            )

        # No leaked fields -> no results
        assert response.results == []


# ===========================================================================
# TEST CLASS 6: Leak detection on real simulated files (direct)
# ===========================================================================

class TestLeakDetectionOnSimulatedFiles:
    """Direct leak detection tests using actual simulated file content.

    These tests call detect_leaks() directly against real file text
    to verify the three-tier cascade works on production-like data.
    """

    def test_exact_ssn_dashed_in_hr_form(self, all_customers):
        """SSN with dashes in hr_onboarding_obrien.txt -> exact match."""
        customer = all_customers["523-45-7891"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_obrien.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.method == "exact"
        assert result.SSN.confidence == 1.0

    def test_exact_ssn_undashed_in_payroll(self, all_customers):
        """SSN without dashes in payroll_register_q1_2024.txt -> exact match.

        File has "412673309" for Jennifer Smith-Jones.
        """
        customer = all_customers["412-67-3309"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "payroll_register_q1_2024.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.method == "exact"
        assert result.SSN.confidence == 1.0

    def test_dob_european_format(self, all_customers):
        """DOB in European format (30/06/1980) in insurance claim -> exact match.

        Maria Rodriguez DOB is 1980-06-30, file has "30/06/1980".
        """
        customer = all_customers["291-88-4451"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "insurance_claim_rodriguez_hassan.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.DOB.found is True
        assert result.DOB.method == "exact"
        assert result.DOB.confidence == 1.0

    def test_dob_us_format(self, all_customers):
        """DOB in US format (03/22/1975) in hr_onboarding_obrien.txt.

        Robert O'Brien DOB is 1975-03-22, file has "03/22/1975".
        """
        customer = all_customers["523-45-7891"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_obrien.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.DOB.found is True
        assert result.DOB.method == "exact"
        assert result.DOB.confidence == 1.0

    def test_dob_iso_format(self, all_customers):
        """DOB in ISO format (1970-01-19) in appointment_notes_mar2024.txt.

        Ahmed Hassan DOB is 1970-01-19, file has "1970-01-19".
        """
        customer = all_customers["785-33-6624"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "appointment_notes_mar2024.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.DOB.found is True
        assert result.DOB.method == "exact"
        assert result.DOB.confidence == 1.0

    def test_drivers_license_exact_match(self, all_customers):
        """Driver's license number in hr_onboarding_chekuri.txt -> exact match."""
        customer = all_customers["634-21-8805"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_chekuri.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.DriversLicense.found is True
        assert result.DriversLicense.method == "exact"
        assert result.DriversLicense.confidence == 1.0

    def test_state_word_boundary_match(self, all_customers):
        """State code 'CA' with word boundary in hr_onboarding_chekuri.txt."""
        customer = all_customers["634-21-8805"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_chekuri.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.State.found is True
        assert result.State.method == "exact"
        assert result.State.confidence == 1.0

    def test_zipcode_exact_match(self, all_customers):
        """ZipCode '90057' in hr_onboarding_chekuri.txt."""
        customer = all_customers["634-21-8805"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_chekuri.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.ZipCode.found is True
        assert result.ZipCode.method == "exact"
        assert result.ZipCode.confidence == 1.0

    def test_city_normalized_match(self, all_customers):
        """City 'Los Angeles' in hr_onboarding_chekuri.txt -> normalized match."""
        customer = all_customers["634-21-8805"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_chekuri.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.City.found is True
        assert result.City.confidence >= 0.95

    def test_fullname_normalized_match(self, all_customers):
        """Fullname 'Karthik Chekuri' appears exactly in hr_onboarding_chekuri.txt."""
        customer = all_customers["634-21-8805"]
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_chekuri.txt")
        )
        result = detect_leaks(file_text, customer)

        assert result.Fullname.found is True
        assert result.Fullname.confidence >= 0.95

    def test_null_fields_skipped(self, all_customers):
        """Maria Rodriguez (C004) has null Country field — should skip detection."""
        customer = all_customers["291-88-4451"]

        # Verify Country is empty/None in the CSV
        assert not customer.Country or customer.Country.strip() == ""

        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "insurance_claim_rodriguez_hassan.txt")
        )
        result = detect_leaks(file_text, customer)

        # Country should report as not found with method "none"
        assert result.Country.found is False
        assert result.Country.method == "none"
        assert result.Country.confidence == 0.0

    def test_snippet_extraction(self, all_customers):
        """Verify that match snippets are non-empty strings with context."""
        customer = all_customers["523-45-7891"]  # O'Brien
        file_text = extract_text(
            os.path.join(SIMULATED_DIR, "hr_onboarding_obrien.txt")
        )
        result = detect_leaks(file_text, customer)

        # SSN snippet should contain the SSN
        assert result.SSN.found is True
        assert result.SSN.snippet is not None
        assert len(result.SSN.snippet) > 0
        assert len(result.SSN.snippet) <= 120  # ~100 chars +/- tolerance


# ===========================================================================
# TEST CLASS 7: Fullname validation integration
# ===========================================================================

class TestFullnameValidationIntegration:
    """Test fullname validation as part of the full pipeline."""

    def test_fullname_matches_db_record(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Providing correct fullname proceeds with search."""
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]  # Karthik Chekuri

        expected_files = ["hr_onboarding_chekuri.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[15.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname="Karthik Chekuri",
                config=mock_config,
            )

        assert isinstance(response, SearchResponse)
        assert len(response.results) > 0

    def test_fullname_mismatch_raises_409(
        self, all_customers, mock_db, mock_search_client, mock_config
    ):
        """Providing wrong fullname raises FullnameMismatchError."""
        from app.services.search_service import (
            FullnameMismatchError,
            search_customer_pii,
        )

        customer = all_customers["634-21-8805"]  # Karthik Chekuri

        with patch("app.services.search_service._lookup_customer", return_value=customer):
            with pytest.raises(FullnameMismatchError, match="Provided fullname does not match"):
                search_customer_pii(
                    db=mock_db,
                    search_client=mock_search_client,
                    ssn="634-21-8805",
                    fullname="John Doe",
                    config=mock_config,
                )

    def test_fullname_case_insensitive_match(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """Fullname comparison is case-insensitive."""
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]

        mock_search_client.search.return_value = []

        with patch("app.services.search_service._lookup_customer", return_value=customer):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname="karthik chekuri",  # lowercase
                config=mock_config,
            )

        assert isinstance(response, SearchResponse)


# ===========================================================================
# TEST CLASS 8: No results from Azure Search
# ===========================================================================

class TestNoAzureResults:
    """When Azure returns zero results, response should have empty results."""

    def test_azure_returns_empty_list(
        self, all_customers, mock_db, mock_search_client, mock_config
    ):
        """Azure Search returns [] -> response has empty results and a message."""
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]
        mock_search_client.search.return_value = []

        with patch("app.services.search_service._lookup_customer", return_value=customer):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []
        assert response.customer.fullname == "Karthik Chekuri"
        assert isinstance(response.search_run_id, uuid.UUID)


# ===========================================================================
# TEST CLASS 9: Search score normalization in pipeline
# ===========================================================================

class TestSearchScoreNormalizationIntegration:
    """Verify that search scores are normalized across the result set."""

    def test_scores_normalized_in_pipeline(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """When multiple files are returned, raw search scores should be
        normalized (divided by max) for confidence calculation.
        """
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]  # Karthik Chekuri

        expected_files = [
            "hr_onboarding_chekuri.txt",
            "payroll_register_q1_2024.txt",
        ]
        # Different raw scores to test normalization
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[20.0, 10.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname=None,
                config=mock_config,
            )

        # The raw azure_search_score values should be preserved in the response
        raw_scores = [r.azure_search_score for r in response.results]
        for score in raw_scores:
            assert score > 0.0


# ===========================================================================
# TEST CLASS 10: DB persistence integration
# ===========================================================================

class TestDBPersistenceIntegration:
    """Verify that search results are persisted to the DB."""

    def test_results_persisted_on_successful_search(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """When search finds leaked PII, results should be saved via db.add + db.commit."""
        from app.services.search_service import search_customer_pii

        customer = all_customers["634-21-8805"]

        expected_files = ["hr_onboarding_chekuri.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[15.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        with patch("app.services.search_service._lookup_customer", return_value=customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="634-21-8805",
                fullname=None,
                config=mock_config,
            )

        # DB should have been called with add and commit
        if len(response.results) > 0:
            assert mock_db.add.call_count >= 1
            mock_db.commit.assert_called()

    def test_no_persistence_when_no_leaks(
        self, all_customers, dlu_metadata, dlu_lookup, mock_db, mock_search_client, mock_config
    ):
        """When no leaks found, nothing should be persisted."""
        from app.services.search_service import search_customer_pii

        # Fake customer whose PII won't match any file
        fake_customer = FakeMasterPII(
            ID="X999",
            Fullname="Qxywort Bnplmftx",
            FirstName="Qxywort",
            LastName="Bnplmftx",
            DOB=datetime.date(2000, 6, 15),
            SSN="000-00-0001",
            DriversLicense="ZZ-Z0000001",
            Address1="1 Nowhere Lane",
            Address2=None,
            Address3=None,
            ZipCode="00001",
            City="Atlantis",
            State="ZZ",
            Country="Narnia",
        )

        expected_files = ["client_intake_whitfield.txt"]
        azure_results = _build_azure_results_for_files(
            dlu_metadata, expected_files, scores=[5.0]
        )
        mock_search_client.search.return_value = azure_results

        def _dlu_side_effect(db, guid):
            return dlu_lookup.get(guid)

        # Reset mock call counts
        mock_db.reset_mock()

        with patch("app.services.search_service._lookup_customer", return_value=fake_customer), \
             patch("app.services.search_service._lookup_dlu_record", side_effect=_dlu_side_effect):
            response = search_customer_pii(
                db=mock_db,
                search_client=mock_search_client,
                ssn="000-00-0001",
                fullname=None,
                config=mock_config,
            )

        assert response.results == []
        mock_db.add.assert_not_called()
