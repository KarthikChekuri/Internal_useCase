"""Shared pytest fixtures for breach-search tests (Phase 6.1).

Provides reusable fixtures across all test modules:
- sample_customer_pii: A standard fake customer with all 13 PII fields
- sample_customer_all_fields: A customer with ALL fields populated (no nulls)
- sample_customer_minimal: A customer with many null fields
- sample_file_text_with_pii: File text containing embedded PII for the sample customer
- sample_file_text_no_pii: File text with no PII matches
- mock_db_session: A MagicMock for SQLAlchemy session
- mock_search_client: A MagicMock for Azure AI Search client
- mock_settings: A MagicMock for app Settings
"""

import datetime
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fake MasterPII (avoids importing sqlalchemy which can hang)
# ---------------------------------------------------------------------------

class FakeMasterPII:
    """Lightweight stand-in for the MasterPII ORM model.

    Avoids importing sqlalchemy which can hang in this environment.
    All 13 PII fields are represented as simple attributes.
    """

    def __init__(self, **kwargs):
        defaults = {
            "ID": 1,
            "Fullname": "Karthik Chekuri",
            "FirstName": "Karthik",
            "LastName": "Chekuri",
            "DOB": datetime.date(1990, 5, 15),
            "SSN": "343-43-4343",
            "DriversLicense": "D1234567",
            "Address1": "123 Main St",
            "Address2": None,
            "Address3": None,
            "ZipCode": "90210",
            "City": "New York",
            "State": "CA",
            "Country": "United States",
        }
        defaults.update(kwargs)
        for k, v in defaults.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# Sample customer fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_customer_pii():
    """Standard test customer with common PII values.

    Address2, Address3 are None (common in real data).
    """
    return FakeMasterPII()


@pytest.fixture
def sample_customer_all_fields():
    """Customer with all 13 fields populated (no nulls)."""
    return FakeMasterPII(
        Address2="Apt 4B",
        Address3="Building C",
        Country="United States",
    )


@pytest.fixture
def sample_customer_minimal():
    """Customer with many null fields -- tests null-field handling."""
    return FakeMasterPII(
        Fullname="John Doe",
        FirstName="John",
        LastName="Doe",
        DOB=None,
        SSN="123-45-6789",
        DriversLicense=None,
        Address1=None,
        Address2=None,
        Address3=None,
        ZipCode=None,
        City=None,
        State=None,
        Country=None,
    )


# ---------------------------------------------------------------------------
# Sample file text fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_file_text_with_pii():
    """File text containing PII matching the sample_customer_pii fixture.

    Contains: Karthik Chekuri, SSN 343-43-4343, DOB 1990-05-15,
    D1234567, 123 Main St, 90210, New York, CA, United States.
    """
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
def sample_file_text_no_pii():
    """File text with absolutely no PII matches for any customer."""
    return (
        "Quarterly Financial Report Q3 2024\n"
        "Revenue increased by 15% compared to previous quarter.\n"
        "Operating expenses remained stable at projected levels.\n"
        "No material changes to report in this period.\n"
    )


# ---------------------------------------------------------------------------
# Mock infrastructure fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db_session():
    """A MagicMock for SQLAlchemy Session.

    Use this for unit tests that need a DB session without connecting.
    """
    session = MagicMock()
    return session


@pytest.fixture
def mock_search_client():
    """A MagicMock for Azure AI SearchClient.

    Use this for unit tests that need a search client without connecting.
    """
    client = MagicMock()
    return client


@pytest.fixture
def mock_settings():
    """A MagicMock for app Settings.

    Provides all required config fields with test values.
    """
    settings = MagicMock()
    settings.DATABASE_URL = "mssql+pyodbc://fake:fake@localhost/TestDB"
    settings.AZURE_SEARCH_ENDPOINT = "https://test.search.windows.net"
    settings.AZURE_SEARCH_KEY = "test-key-abc123"
    settings.AZURE_SEARCH_INDEX = "breach-file-index"
    settings.FILE_BASE_PATH = "C:/test/data"
    settings.CASE_NAME = "TestCase"
    return settings
