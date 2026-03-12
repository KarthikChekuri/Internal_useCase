"""V2 Integration Tests — replaced from V1.

V1 POST /search integration tests have been removed. The V1 endpoint
(POST /search -> search_customer_pii) was deleted in Phase V2-2.2.

V2 integration tests live in tests/test_v2_integration.py and exercise
the full batch pipeline: start_batch -> strategy search -> leak detection
-> confidence scoring -> result persistence -> status tracking.

This file retains only integration-level helpers/checks that apply to
V2 infrastructure: file extraction from disk and simulated data loading
(avoids sqlalchemy).
"""

import csv
import datetime
import os
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

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

def _load_customers_from_csv(csv_path: str) -> list:
    """Load customer rows from a seed CSV file into FakeMasterPII objects."""
    customers = []
    if not os.path.exists(csv_path):
        return customers

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dob = None
            if row.get("DOB"):
                try:
                    dob = datetime.date.fromisoformat(row["DOB"])
                except ValueError:
                    dob = None

            customers.append(
                FakeMasterPII(
                    ID=int(row.get("ID", 1)),
                    Fullname=row.get("Fullname") or None,
                    FirstName=row.get("FirstName") or None,
                    LastName=row.get("LastName") or None,
                    DOB=dob,
                    SSN=row.get("SSN") or None,
                    DriversLicense=row.get("DriversLicense") or None,
                    Address1=row.get("Address1") or None,
                    Address2=row.get("Address2") or None,
                    Address3=row.get("Address3") or None,
                    ZipCode=row.get("ZipCode") or None,
                    City=row.get("City") or None,
                    State=row.get("State") or None,
                    Country=row.get("Country") or None,
                )
            )

    return customers


# ---------------------------------------------------------------------------
# Tests: Text extraction from simulated files on disk
# ---------------------------------------------------------------------------

class TestTextExtractionIntegration:
    """Integration tests for text extraction from real files on disk."""

    def test_extract_text_from_simulated_txt_file(self):
        """WHEN a .txt file exists in simulated_files THEN extract_text returns non-empty string."""
        txt_files = []
        if os.path.exists(SIMULATED_DIR):
            for fname in os.listdir(SIMULATED_DIR):
                if fname.endswith(".txt"):
                    txt_files.append(os.path.join(SIMULATED_DIR, fname))

        if not txt_files:
            pytest.skip("No simulated .txt files found — run data generation first.")

        path = txt_files[0]
        text = extract_text(path)

        assert text is not None
        assert isinstance(text, str)
        assert len(text) > 0

    def test_extract_text_returns_none_for_missing_file(self):
        """WHEN file does not exist THEN extract_text returns None."""
        text = extract_text("/nonexistent/path/to/file.txt")
        assert text is None


# ---------------------------------------------------------------------------
# Tests: Leak detection on simulated file content
# ---------------------------------------------------------------------------

class TestLeakDetectionOnSimulatedData:
    """Integration tests using simulated PII-containing file text."""

    def test_detect_leaks_on_pii_embedded_text(self):
        """WHEN file contains known PII THEN detect_leaks finds the correct fields."""
        customer = FakeMasterPII(
            Fullname="Alice Smith",
            FirstName="Alice",
            LastName="Smith",
            SSN="555-66-7777",
            DOB=datetime.date(1985, 3, 20),
            ZipCode="12345",
            City="Springfield",
            State="IL",
            Country="United States",
            DriversLicense="IL1234567",
            Address1="456 Oak Ave",
            Address2=None,
            Address3=None,
        )

        file_text = (
            "Employee: Alice Smith\n"
            "SSN: 555-66-7777\n"
            "DOB: 1985-03-20\n"
            "Address: 456 Oak Ave, Springfield, IL 12345\n"
            "Country: United States\n"
            "License: IL1234567\n"
        )

        result = detect_leaks(file_text, customer)

        assert result.SSN.found is True
        assert result.SSN.confidence == 1.0
        assert result.Fullname.found is True
        assert result.DOB.found is True

    def test_detect_leaks_returns_no_match_for_blank_file(self):
        """WHEN file is empty THEN all fields not found."""
        customer = FakeMasterPII()
        result = detect_leaks("", customer)

        assert result.SSN.found is False
        assert result.Fullname.found is False

    def test_detect_leaks_handles_customer_with_null_fields(self):
        """WHEN customer has null DriversLicense THEN DriversLicense.found is False without error."""
        customer = FakeMasterPII(DriversLicense=None)
        file_text = "Name: Karthik Chekuri\nSSN: 343-43-4343\n"

        result = detect_leaks(file_text, customer)

        assert result.DriversLicense.found is False
        assert result.DriversLicense.method == "none"


# ---------------------------------------------------------------------------
# Tests: Confidence scoring with realistic values
# ---------------------------------------------------------------------------

class TestConfidenceScoringOnRealData:
    """Integration tests for confidence formulas on realistic input values."""

    def test_ssn_and_name_scenario_from_detect_leaks(self):
        """WHEN detect_leaks finds SSN and Fullname THEN SSN+Name confidence formula applies."""
        customer = FakeMasterPII()
        file_text = "Name: Karthik Chekuri\nSSN: 343-43-4343\n"
        leak_result = detect_leaks(file_text, customer)

        ssn_conf = leak_result.SSN.confidence
        name_conf = max(
            leak_result.Fullname.confidence,
            leak_result.FirstName.confidence,
            leak_result.LastName.confidence,
        )

        result = compute_overall_confidence(
            ssn_conf=ssn_conf,
            name_conf=name_conf,
            other_field_confs=[],
            search_score_norm=0.8,
        )

        assert result["scenario"] == "ssn_and_name"
        assert 0.0 < result["score"] <= 1.0

    def test_normalize_search_scores_integration(self):
        """WHEN normalizing realistic Azure search scores THEN max normalized to 1.0."""
        raw_scores = [15.3, 11.2, 7.8, 3.1]
        normalized = normalize_search_scores(raw_scores)

        assert abs(normalized[0] - 1.0) < 0.001
        assert all(0.0 <= s <= 1.0 for s in normalized)
        assert normalized[0] >= normalized[1] >= normalized[2] >= normalized[3]


# ---------------------------------------------------------------------------
# Tests: Seed CSV loading
# ---------------------------------------------------------------------------

class TestSeedDataLoading:
    """Integration tests for loading simulated customer data from CSV."""

    def test_seed_csv_loads_customers_if_exists(self):
        """WHEN seed CSV exists THEN customers can be loaded without sqlalchemy."""
        csv_candidates = [
            os.path.join(SEED_DIR, "master_pii.csv"),
            os.path.join(SEED_DIR, "customers.csv"),
        ]

        found_csv = None
        for path in csv_candidates:
            if os.path.exists(path):
                found_csv = path
                break

        if found_csv is None:
            pytest.skip("No seed CSV found in data/seed/. Run data generation first.")

        customers = _load_customers_from_csv(found_csv)
        assert len(customers) > 0

        # Verify the first customer has required PII attributes
        first = customers[0]
        assert hasattr(first, "SSN")
        assert hasattr(first, "Fullname")
        assert hasattr(first, "DOB")
