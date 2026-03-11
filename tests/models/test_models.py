"""
Tests for SQLAlchemy ORM model definitions (Phase 1.2).

These tests verify class-level metadata only — no database connection is made.
"""

import pytest
from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Float, Integer, String, Unicode,
    inspect as sa_inspect,
)
from sqlalchemy.orm import DeclarativeBase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(model_cls, col_name):
    """Return the SQLAlchemy Column object for the given attribute name."""
    mapper = sa_inspect(model_cls)
    return mapper.columns[col_name]


def _table_args(model_cls):
    """Return the __table_args__ dict/tuple from the model class."""
    return getattr(model_cls, "__table_args__", None)


def _schema(model_cls):
    """Extract the schema from __table_args__."""
    ta = _table_args(model_cls)
    if isinstance(ta, dict):
        return ta.get("schema")
    if isinstance(ta, tuple):
        for item in ta:
            if isinstance(item, dict):
                return item.get("schema")
    return None


# ---------------------------------------------------------------------------
# DLU model tests
# ---------------------------------------------------------------------------

class TestDLUModel:
    """[DLU].[datalakeuniverse] — read-only existing table."""

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.dlu import DLU
        self.DLU = DLU

    def test_table_name(self):
        assert self.DLU.__tablename__ == "datalakeuniverse"

    def test_schema(self):
        assert _schema(self.DLU) == "DLU"

    def test_column_guid_exists(self):
        col = _col(self.DLU, "GUID")
        assert col is not None

    def test_column_guid_is_primary_key(self):
        col = _col(self.DLU, "GUID")
        assert col.primary_key is True

    def test_column_textpath_exists(self):
        col = _col(self.DLU, "TEXTPATH")
        assert col is not None

    def test_column_filename_exists(self):
        col = _col(self.DLU, "fileName")
        assert col is not None

    def test_column_fileextension_exists(self):
        col = _col(self.DLU, "fileExtension")
        assert col is not None

    def test_column_casename_exists(self):
        col = _col(self.DLU, "caseName")
        assert col is not None

    def test_column_isexclusion_exists(self):
        col = _col(self.DLU, "isExclusion")
        assert col is not None

    def test_column_md5_exists(self):
        col = _col(self.DLU, "MD5")
        assert col is not None

    def test_all_expected_columns_present(self):
        mapper = sa_inspect(self.DLU)
        col_names = set(mapper.columns.keys())
        expected = {"GUID", "TEXTPATH", "fileName", "fileExtension", "caseName", "isExclusion", "MD5"}
        assert expected.issubset(col_names)

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.DLU, Base)


# ---------------------------------------------------------------------------
# MasterPII model tests
# ---------------------------------------------------------------------------

class TestMasterPIIModel:
    """[PII].[master_pii] — new table, 14 columns (ID + 13 PII fields)."""

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.master_pii import MasterPII
        self.MasterPII = MasterPII

    def test_table_name(self):
        assert self.MasterPII.__tablename__ == "master_pii"

    def test_schema(self):
        assert _schema(self.MasterPII) == "PII"

    def test_column_id_exists(self):
        col = _col(self.MasterPII, "ID")
        assert col is not None

    def test_column_id_is_primary_key(self):
        col = _col(self.MasterPII, "ID")
        assert col.primary_key is True

    def test_column_id_is_integer(self):
        col = _col(self.MasterPII, "ID")
        assert isinstance(col.type, Integer)

    def test_column_fullname_exists(self):
        col = _col(self.MasterPII, "Fullname")
        assert col is not None

    def test_column_fullname_is_unicode(self):
        col = _col(self.MasterPII, "Fullname")
        assert isinstance(col.type, (String, Unicode))

    def test_column_fullname_length(self):
        col = _col(self.MasterPII, "Fullname")
        assert col.type.length == 250

    def test_column_firstname_exists(self):
        col = _col(self.MasterPII, "FirstName")
        assert col is not None

    def test_column_firstname_length(self):
        col = _col(self.MasterPII, "FirstName")
        assert col.type.length == 100

    def test_column_lastname_exists(self):
        col = _col(self.MasterPII, "LastName")
        assert col is not None

    def test_column_lastname_length(self):
        col = _col(self.MasterPII, "LastName")
        assert col.type.length == 100

    def test_column_dob_exists(self):
        col = _col(self.MasterPII, "DOB")
        assert col is not None

    def test_column_dob_is_date(self):
        col = _col(self.MasterPII, "DOB")
        assert isinstance(col.type, Date)

    def test_column_ssn_exists(self):
        col = _col(self.MasterPII, "SSN")
        assert col is not None

    def test_column_ssn_length(self):
        col = _col(self.MasterPII, "SSN")
        assert col.type.length == 11

    def test_column_driverslicense_exists(self):
        col = _col(self.MasterPII, "DriversLicense")
        assert col is not None

    def test_column_driverslicense_length(self):
        col = _col(self.MasterPII, "DriversLicense")
        assert col.type.length == 50

    def test_column_address1_exists(self):
        col = _col(self.MasterPII, "Address1")
        assert col is not None

    def test_column_address1_length(self):
        col = _col(self.MasterPII, "Address1")
        assert col.type.length == 250

    def test_column_address2_exists(self):
        col = _col(self.MasterPII, "Address2")
        assert col is not None

    def test_column_address3_exists(self):
        col = _col(self.MasterPII, "Address3")
        assert col is not None

    def test_column_zipcode_exists(self):
        col = _col(self.MasterPII, "ZipCode")
        assert col is not None

    def test_column_zipcode_length(self):
        col = _col(self.MasterPII, "ZipCode")
        assert col.type.length == 10

    def test_column_city_exists(self):
        col = _col(self.MasterPII, "City")
        assert col is not None

    def test_column_city_length(self):
        col = _col(self.MasterPII, "City")
        assert col.type.length == 100

    def test_column_state_exists(self):
        col = _col(self.MasterPII, "State")
        assert col is not None

    def test_column_state_length(self):
        col = _col(self.MasterPII, "State")
        assert col.type.length == 2

    def test_column_country_exists(self):
        col = _col(self.MasterPII, "Country")
        assert col is not None

    def test_column_country_length(self):
        col = _col(self.MasterPII, "Country")
        assert col.type.length == 50

    def test_all_14_columns_present(self):
        mapper = sa_inspect(self.MasterPII)
        col_names = set(mapper.columns.keys())
        expected = {
            "ID", "Fullname", "FirstName", "LastName", "DOB", "SSN",
            "DriversLicense", "Address1", "Address2", "Address3",
            "ZipCode", "City", "State", "Country",
        }
        assert expected == col_names

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.MasterPII, Base)


# ---------------------------------------------------------------------------
# SearchResult model tests
# ---------------------------------------------------------------------------

class TestSearchResultModel:
    """[Search].[search_results] — new table with 13 LeakedX BIT columns."""

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.search_result import SearchResult
        self.SearchResult = SearchResult

    def test_table_name(self):
        assert self.SearchResult.__tablename__ == "search_results"

    def test_schema(self):
        assert _schema(self.SearchResult) == "Search"

    def test_column_id_is_primary_key(self):
        col = _col(self.SearchResult, "ID")
        assert col.primary_key is True

    def test_column_search_run_id_exists(self):
        col = _col(self.SearchResult, "SearchRunID")
        assert col is not None

    def test_column_customer_id_exists(self):
        col = _col(self.SearchResult, "CustomerID")
        assert col is not None

    def test_column_customer_id_is_string(self):
        col = _col(self.SearchResult, "CustomerID")
        assert isinstance(col.type, (String, Unicode))

    def test_column_file_guid_exists(self):
        col = _col(self.SearchResult, "FileGUID")
        assert col is not None

    def test_column_file_guid_has_fk(self):
        col = _col(self.SearchResult, "FileGUID")
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert any("datalakeuniverse" in t for t in fk_targets)

    # --- 13 LeakedX BIT columns ---

    @pytest.mark.parametrize("col_name", [
        "LeakedFullname",
        "LeakedFirstName",
        "LeakedLastName",
        "LeakedDOB",
        "LeakedSSN",
        "LeakedDriversLicense",
        "LeakedAddress1",
        "LeakedAddress2",
        "LeakedAddress3",
        "LeakedZipCode",
        "LeakedCity",
        "LeakedState",
        "LeakedCountry",
    ])
    def test_leaked_bit_column_exists(self, col_name):
        col = _col(self.SearchResult, col_name)
        assert col is not None

    @pytest.mark.parametrize("col_name", [
        "LeakedFullname",
        "LeakedFirstName",
        "LeakedLastName",
        "LeakedDOB",
        "LeakedSSN",
        "LeakedDriversLicense",
        "LeakedAddress1",
        "LeakedAddress2",
        "LeakedAddress3",
        "LeakedZipCode",
        "LeakedCity",
        "LeakedState",
        "LeakedCountry",
    ])
    def test_leaked_bit_column_is_boolean(self, col_name):
        col = _col(self.SearchResult, col_name)
        assert isinstance(col.type, Boolean)

    def test_column_leaked_fields_list_exists(self):
        col = _col(self.SearchResult, "LeakedFieldsList")
        assert col is not None

    def test_column_match_details_exists(self):
        col = _col(self.SearchResult, "MatchDetails")
        assert col is not None

    def test_column_overall_confidence_exists(self):
        col = _col(self.SearchResult, "OverallConfidence")
        assert col is not None

    def test_column_overall_confidence_is_float(self):
        col = _col(self.SearchResult, "OverallConfidence")
        assert isinstance(col.type, Float)

    def test_column_azure_search_score_exists(self):
        col = _col(self.SearchResult, "AzureSearchScore")
        assert col is not None

    def test_column_azure_search_score_is_float(self):
        col = _col(self.SearchResult, "AzureSearchScore")
        assert isinstance(col.type, Float)

    def test_column_needs_review_exists(self):
        col = _col(self.SearchResult, "NeedsReview")
        assert col is not None

    def test_column_needs_review_is_boolean(self):
        col = _col(self.SearchResult, "NeedsReview")
        assert isinstance(col.type, Boolean)

    def test_column_needs_review_default_false(self):
        col = _col(self.SearchResult, "NeedsReview")
        # SQLAlchemy stores the default as a ColumnDefault with arg=0 or False
        assert col.default is not None
        assert col.default.arg in (0, False)

    def test_column_searched_at_exists(self):
        col = _col(self.SearchResult, "SearchedAt")
        assert col is not None

    def test_column_searched_at_is_datetime(self):
        col = _col(self.SearchResult, "SearchedAt")
        assert isinstance(col.type, DateTime)

    def test_column_searched_at_has_server_default(self):
        col = _col(self.SearchResult, "SearchedAt")
        assert col.server_default is not None

    def test_all_core_columns_present(self):
        mapper = sa_inspect(self.SearchResult)
        col_names = set(mapper.columns.keys())
        expected = {
            "ID", "SearchRunID", "CustomerID", "FileGUID",
            "LeakedFullname", "LeakedFirstName", "LeakedLastName",
            "LeakedDOB", "LeakedSSN", "LeakedDriversLicense",
            "LeakedAddress1", "LeakedAddress2", "LeakedAddress3",
            "LeakedZipCode", "LeakedCity", "LeakedState", "LeakedCountry",
            "LeakedFieldsList", "MatchDetails",
            "OverallConfidence", "AzureSearchScore",
            "NeedsReview", "SearchedAt",
        }
        assert expected.issubset(col_names)

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.SearchResult, Base)


# ---------------------------------------------------------------------------
# Base / database module tests
# ---------------------------------------------------------------------------

class TestDatabaseModule:
    """Verify the Base class and factory functions exist in database.py."""

    def test_base_is_declarative(self):
        from app.models.database import Base
        assert issubclass(Base, DeclarativeBase)

    def test_get_engine_callable(self):
        from app.models.database import get_engine
        assert callable(get_engine)

    def test_get_session_factory_callable(self):
        from app.models.database import get_session_factory
        assert callable(get_session_factory)

    def test_get_db_callable(self):
        from app.models.database import get_db
        assert callable(get_db)
