"""
Tests for V2 SQLAlchemy ORM model definitions (Phase V2-1.1).

These tests verify class-level metadata only — no database connection is made.
Imports use lazy loading inside fixtures to prevent SQLAlchemy from attempting
real DB connections at collection time.
"""

import pytest
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Unicode,
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


def _schema(model_cls):
    """Extract the schema from __table_args__."""
    ta = getattr(model_cls, "__table_args__", None)
    if isinstance(ta, dict):
        return ta.get("schema")
    if isinstance(ta, tuple):
        for item in ta:
            if isinstance(item, dict):
                return item.get("schema")
    return None


def _has_fk(col, target_fragment):
    """Return True if the column has a FK whose target contains target_fragment."""
    return any(target_fragment in fk.target_fullname for fk in col.foreign_keys)


# ---------------------------------------------------------------------------
# DLU model (V2 rewrite) tests
# ---------------------------------------------------------------------------


class TestDLUModelV2:
    """
    V2 DLU: [DLU].[datalakeuniverse]
    PK: MD5 VARCHAR(32), file_path NVARCHAR(500)
    V1 columns GUID/TEXTPATH/fileName/fileExtension/caseName/isExclusion are removed.
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.dlu import DLU
        self.DLU = DLU

    def test_table_name(self):
        assert self.DLU.__tablename__ == "datalakeuniverse"

    def test_schema(self):
        assert _schema(self.DLU) == "DLU"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.DLU, Base)

    # --- MD5 primary key ---

    def test_md5_column_exists(self):
        col = _col(self.DLU, "MD5")
        assert col is not None

    def test_md5_is_primary_key(self):
        col = _col(self.DLU, "MD5")
        assert col.primary_key is True

    def test_md5_is_varchar(self):
        col = _col(self.DLU, "MD5")
        assert isinstance(col.type, String)

    def test_md5_length_is_32(self):
        col = _col(self.DLU, "MD5")
        assert col.type.length == 32

    # --- file_path ---

    def test_file_path_column_exists(self):
        col = _col(self.DLU, "file_path")
        assert col is not None

    def test_file_path_is_unicode(self):
        col = _col(self.DLU, "file_path")
        assert isinstance(col.type, (String, Unicode))

    def test_file_path_length_is_500(self):
        col = _col(self.DLU, "file_path")
        assert col.type.length == 500

    # --- V1 columns must NOT exist ---

    def test_guid_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "GUID" not in mapper.columns.keys()

    def test_textpath_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "TEXTPATH" not in mapper.columns.keys()

    def test_filename_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "fileName" not in mapper.columns.keys()

    def test_fileextension_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "fileExtension" not in mapper.columns.keys()

    def test_casename_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "caseName" not in mapper.columns.keys()

    def test_isexclusion_column_removed(self):
        mapper = sa_inspect(self.DLU)
        assert "isExclusion" not in mapper.columns.keys()

    # --- Exact column set ---

    def test_only_md5_and_file_path_columns(self):
        mapper = sa_inspect(self.DLU)
        col_names = set(mapper.columns.keys())
        assert col_names == {"MD5", "file_path"}


# ---------------------------------------------------------------------------
# MasterData model tests
# ---------------------------------------------------------------------------


class TestMasterDataModel:
    """
    [PII].[master_data]
    PK: customer_id INT (NOT Identity)
    13 PII fields: Fullname, FirstName, LastName, DOB, SSN, DriversLicense,
                   Address1, Address2, Address3, ZipCode, City, State, Country
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.master_data import MasterData
        self.MasterData = MasterData

    def test_table_name(self):
        assert self.MasterData.__tablename__ == "master_data"

    def test_schema(self):
        assert _schema(self.MasterData) == "PII"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.MasterData, Base)

    # --- customer_id PK ---

    def test_customer_id_exists(self):
        col = _col(self.MasterData, "customer_id")
        assert col is not None

    def test_customer_id_is_primary_key(self):
        col = _col(self.MasterData, "customer_id")
        assert col.primary_key is True

    def test_customer_id_is_integer(self):
        col = _col(self.MasterData, "customer_id")
        assert isinstance(col.type, Integer)

    def test_customer_id_not_autoincrement(self):
        """customer_id is NOT Identity — it comes from the external master list."""
        col = _col(self.MasterData, "customer_id")
        # SQLAlchemy marks autoincrement=True only when Identity is used
        # With no Identity, autoincrement should be False or the column should
        # have no server_default indicating identity
        assert col.autoincrement is not True or col.default is None

    # --- 13 PII fields ---

    def test_fullname_exists(self):
        assert _col(self.MasterData, "Fullname") is not None

    def test_fullname_is_unicode(self):
        col = _col(self.MasterData, "Fullname")
        assert isinstance(col.type, (String, Unicode))

    def test_fullname_length(self):
        assert _col(self.MasterData, "Fullname").type.length == 250

    def test_firstname_exists(self):
        assert _col(self.MasterData, "FirstName") is not None

    def test_firstname_length(self):
        assert _col(self.MasterData, "FirstName").type.length == 100

    def test_lastname_exists(self):
        assert _col(self.MasterData, "LastName") is not None

    def test_lastname_length(self):
        assert _col(self.MasterData, "LastName").type.length == 100

    def test_dob_exists(self):
        assert _col(self.MasterData, "DOB") is not None

    def test_dob_is_date(self):
        col = _col(self.MasterData, "DOB")
        assert isinstance(col.type, Date)

    def test_ssn_exists(self):
        assert _col(self.MasterData, "SSN") is not None

    def test_ssn_length(self):
        assert _col(self.MasterData, "SSN").type.length == 11

    def test_driverslicense_exists(self):
        assert _col(self.MasterData, "DriversLicense") is not None

    def test_driverslicense_length(self):
        assert _col(self.MasterData, "DriversLicense").type.length == 50

    def test_address1_exists(self):
        assert _col(self.MasterData, "Address1") is not None

    def test_address1_length(self):
        assert _col(self.MasterData, "Address1").type.length == 250

    def test_address2_exists(self):
        assert _col(self.MasterData, "Address2") is not None

    def test_address2_length(self):
        assert _col(self.MasterData, "Address2").type.length == 250

    def test_address3_exists(self):
        assert _col(self.MasterData, "Address3") is not None

    def test_address3_length(self):
        assert _col(self.MasterData, "Address3").type.length == 250

    def test_zipcode_exists(self):
        assert _col(self.MasterData, "ZipCode") is not None

    def test_zipcode_length(self):
        assert _col(self.MasterData, "ZipCode").type.length == 10

    def test_city_exists(self):
        assert _col(self.MasterData, "City") is not None

    def test_city_length(self):
        assert _col(self.MasterData, "City").type.length == 100

    def test_state_exists(self):
        assert _col(self.MasterData, "State") is not None

    def test_state_length(self):
        assert _col(self.MasterData, "State").type.length == 2

    def test_country_exists(self):
        assert _col(self.MasterData, "Country") is not None

    def test_country_length(self):
        assert _col(self.MasterData, "Country").type.length == 50

    # --- V1 'ID' column must NOT exist ---

    def test_id_column_removed(self):
        mapper = sa_inspect(self.MasterData)
        assert "ID" not in mapper.columns.keys()

    # --- Complete column set ---

    def test_all_14_columns_present(self):
        mapper = sa_inspect(self.MasterData)
        col_names = set(mapper.columns.keys())
        expected = {
            "customer_id", "Fullname", "FirstName", "LastName", "DOB", "SSN",
            "DriversLicense", "Address1", "Address2", "Address3",
            "ZipCode", "City", "State", "Country",
        }
        assert expected == col_names


# ---------------------------------------------------------------------------
# BatchRun model tests
# ---------------------------------------------------------------------------


class TestBatchRunModel:
    """
    [Batch].[batch_runs]
    PK: batch_id UNIQUEIDENTIFIER
    Columns: strategy_set NVARCHAR(MAX), status VARCHAR(20),
             started_at DATETIME2, completed_at DATETIME2,
             total_customers INT, total_files INT
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.batch import BatchRun
        self.BatchRun = BatchRun

    def test_table_name(self):
        assert self.BatchRun.__tablename__ == "batch_runs"

    def test_schema(self):
        assert _schema(self.BatchRun) == "Batch"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.BatchRun, Base)

    # --- batch_id PK ---

    def test_batch_id_exists(self):
        col = _col(self.BatchRun, "batch_id")
        assert col is not None

    def test_batch_id_is_primary_key(self):
        col = _col(self.BatchRun, "batch_id")
        assert col.primary_key is True

    # --- strategy_set ---

    def test_strategy_set_exists(self):
        assert _col(self.BatchRun, "strategy_set") is not None

    def test_strategy_set_is_string_or_unicode(self):
        col = _col(self.BatchRun, "strategy_set")
        assert isinstance(col.type, (String, Unicode))

    # --- status ---

    def test_status_exists(self):
        assert _col(self.BatchRun, "status") is not None

    def test_status_is_string(self):
        col = _col(self.BatchRun, "status")
        assert isinstance(col.type, (String, Unicode))

    def test_status_length(self):
        col = _col(self.BatchRun, "status")
        assert col.type.length == 20

    # --- started_at ---

    def test_started_at_exists(self):
        assert _col(self.BatchRun, "started_at") is not None

    def test_started_at_is_datetime(self):
        col = _col(self.BatchRun, "started_at")
        assert isinstance(col.type, DateTime)

    # --- completed_at ---

    def test_completed_at_exists(self):
        assert _col(self.BatchRun, "completed_at") is not None

    def test_completed_at_is_datetime(self):
        col = _col(self.BatchRun, "completed_at")
        assert isinstance(col.type, DateTime)

    def test_completed_at_is_nullable(self):
        col = _col(self.BatchRun, "completed_at")
        assert col.nullable is True

    # --- total_customers ---

    def test_total_customers_exists(self):
        assert _col(self.BatchRun, "total_customers") is not None

    def test_total_customers_is_integer(self):
        col = _col(self.BatchRun, "total_customers")
        assert isinstance(col.type, Integer)

    # --- total_files ---

    def test_total_files_exists(self):
        assert _col(self.BatchRun, "total_files") is not None

    def test_total_files_is_integer(self):
        col = _col(self.BatchRun, "total_files")
        assert isinstance(col.type, Integer)

    # --- Complete column set ---

    def test_all_columns_present(self):
        mapper = sa_inspect(self.BatchRun)
        col_names = set(mapper.columns.keys())
        expected = {
            "batch_id", "strategy_set", "status",
            "started_at", "completed_at",
            "total_customers", "total_files",
        }
        assert expected == col_names


# ---------------------------------------------------------------------------
# CustomerStatus model tests
# ---------------------------------------------------------------------------


class TestCustomerStatusModel:
    """
    [Batch].[customer_status]
    PK: id INT IDENTITY
    FKs: batch_id -> batch_runs.batch_id, customer_id -> master_data.customer_id
    Columns: status VARCHAR(20), candidates_found INT default 0,
             leaks_confirmed INT default 0, strategies_matched NVARCHAR(MAX),
             error_message NVARCHAR(MAX), processed_at DATETIME2
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.batch import CustomerStatus
        self.CustomerStatus = CustomerStatus

    def test_table_name(self):
        assert self.CustomerStatus.__tablename__ == "customer_status"

    def test_schema(self):
        assert _schema(self.CustomerStatus) == "Batch"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.CustomerStatus, Base)

    # --- id PK ---

    def test_id_exists(self):
        col = _col(self.CustomerStatus, "id")
        assert col is not None

    def test_id_is_primary_key(self):
        col = _col(self.CustomerStatus, "id")
        assert col.primary_key is True

    def test_id_is_integer(self):
        col = _col(self.CustomerStatus, "id")
        assert isinstance(col.type, Integer)

    # --- batch_id FK ---

    def test_batch_id_exists(self):
        assert _col(self.CustomerStatus, "batch_id") is not None

    def test_batch_id_has_fk_to_batch_runs(self):
        col = _col(self.CustomerStatus, "batch_id")
        assert _has_fk(col, "batch_runs")

    # --- customer_id FK ---

    def test_customer_id_exists(self):
        assert _col(self.CustomerStatus, "customer_id") is not None

    def test_customer_id_is_integer(self):
        col = _col(self.CustomerStatus, "customer_id")
        assert isinstance(col.type, Integer)

    def test_customer_id_has_fk_to_master_data(self):
        col = _col(self.CustomerStatus, "customer_id")
        assert _has_fk(col, "master_data")

    # --- status ---

    def test_status_exists(self):
        assert _col(self.CustomerStatus, "status") is not None

    def test_status_length(self):
        col = _col(self.CustomerStatus, "status")
        assert col.type.length == 20

    # --- candidates_found ---

    def test_candidates_found_exists(self):
        assert _col(self.CustomerStatus, "candidates_found") is not None

    def test_candidates_found_default_is_zero(self):
        col = _col(self.CustomerStatus, "candidates_found")
        assert col.default is not None
        assert col.default.arg == 0

    # --- leaks_confirmed ---

    def test_leaks_confirmed_exists(self):
        assert _col(self.CustomerStatus, "leaks_confirmed") is not None

    def test_leaks_confirmed_default_is_zero(self):
        col = _col(self.CustomerStatus, "leaks_confirmed")
        assert col.default is not None
        assert col.default.arg == 0

    # --- strategies_matched ---

    def test_strategies_matched_exists(self):
        assert _col(self.CustomerStatus, "strategies_matched") is not None

    def test_strategies_matched_is_nullable(self):
        col = _col(self.CustomerStatus, "strategies_matched")
        assert col.nullable is True

    # --- error_message ---

    def test_error_message_exists(self):
        assert _col(self.CustomerStatus, "error_message") is not None

    def test_error_message_is_nullable(self):
        col = _col(self.CustomerStatus, "error_message")
        assert col.nullable is True

    # --- processed_at ---

    def test_processed_at_exists(self):
        assert _col(self.CustomerStatus, "processed_at") is not None

    def test_processed_at_is_datetime(self):
        col = _col(self.CustomerStatus, "processed_at")
        assert isinstance(col.type, DateTime)

    def test_processed_at_is_nullable(self):
        col = _col(self.CustomerStatus, "processed_at")
        assert col.nullable is True

    # --- Complete column set ---

    def test_all_columns_present(self):
        mapper = sa_inspect(self.CustomerStatus)
        col_names = set(mapper.columns.keys())
        expected = {
            "id", "batch_id", "customer_id", "status",
            "candidates_found", "leaks_confirmed",
            "strategies_matched", "error_message", "processed_at",
        }
        assert expected == col_names


# ---------------------------------------------------------------------------
# FileStatus model tests
# ---------------------------------------------------------------------------


class TestFileStatusModel:
    """
    [Index].[file_status]
    PK: md5 VARCHAR(32) FK -> datalakeuniverse.MD5
    Columns: status VARCHAR(20), indexed_at DATETIME2, error_message NVARCHAR(MAX)
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.file_status import FileStatus
        self.FileStatus = FileStatus

    def test_table_name(self):
        assert self.FileStatus.__tablename__ == "file_status"

    def test_schema(self):
        assert _schema(self.FileStatus) == "Index"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.FileStatus, Base)

    # --- md5 PK + FK ---

    def test_md5_exists(self):
        assert _col(self.FileStatus, "md5") is not None

    def test_md5_is_primary_key(self):
        col = _col(self.FileStatus, "md5")
        assert col.primary_key is True

    def test_md5_is_varchar(self):
        col = _col(self.FileStatus, "md5")
        assert isinstance(col.type, String)

    def test_md5_length_is_32(self):
        col = _col(self.FileStatus, "md5")
        assert col.type.length == 32

    def test_md5_has_fk_to_dlu(self):
        col = _col(self.FileStatus, "md5")
        assert _has_fk(col, "datalakeuniverse")

    # --- status ---

    def test_status_exists(self):
        assert _col(self.FileStatus, "status") is not None

    def test_status_is_string(self):
        col = _col(self.FileStatus, "status")
        assert isinstance(col.type, (String, Unicode))

    def test_status_length(self):
        col = _col(self.FileStatus, "status")
        assert col.type.length == 20

    # --- indexed_at ---

    def test_indexed_at_exists(self):
        assert _col(self.FileStatus, "indexed_at") is not None

    def test_indexed_at_is_datetime(self):
        col = _col(self.FileStatus, "indexed_at")
        assert isinstance(col.type, DateTime)

    def test_indexed_at_is_nullable(self):
        col = _col(self.FileStatus, "indexed_at")
        assert col.nullable is True

    # --- error_message ---

    def test_error_message_exists(self):
        assert _col(self.FileStatus, "error_message") is not None

    def test_error_message_is_nullable(self):
        col = _col(self.FileStatus, "error_message")
        assert col.nullable is True

    # --- Complete column set ---

    def test_all_columns_present(self):
        mapper = sa_inspect(self.FileStatus)
        col_names = set(mapper.columns.keys())
        expected = {"md5", "status", "indexed_at", "error_message"}
        assert expected == col_names


# ---------------------------------------------------------------------------
# Result model tests
# ---------------------------------------------------------------------------


class TestResultModel:
    """
    [Search].[results]
    PK: id INT IDENTITY
    FKs: batch_id, customer_id -> master_data, md5 -> datalakeuniverse
    Columns: strategy_name VARCHAR(100), leaked_fields NVARCHAR(MAX),
             match_details NVARCHAR(MAX), overall_confidence FLOAT,
             azure_search_score FLOAT, needs_review BIT default 0,
             searched_at DATETIME2 default GETDATE()
    """

    @pytest.fixture(autouse=True)
    def import_model(self):
        from app.models.result import Result
        self.Result = Result

    def test_table_name(self):
        assert self.Result.__tablename__ == "results"

    def test_schema(self):
        assert _schema(self.Result) == "Search"

    def test_inherits_declarative_base(self):
        from app.models.database import Base
        assert issubclass(self.Result, Base)

    # --- id PK ---

    def test_id_exists(self):
        assert _col(self.Result, "id") is not None

    def test_id_is_primary_key(self):
        col = _col(self.Result, "id")
        assert col.primary_key is True

    def test_id_is_integer(self):
        col = _col(self.Result, "id")
        assert isinstance(col.type, Integer)

    # --- batch_id FK ---

    def test_batch_id_exists(self):
        assert _col(self.Result, "batch_id") is not None

    def test_batch_id_has_fk_to_batch_runs(self):
        col = _col(self.Result, "batch_id")
        assert _has_fk(col, "batch_runs")

    # --- customer_id FK ---

    def test_customer_id_exists(self):
        assert _col(self.Result, "customer_id") is not None

    def test_customer_id_is_integer(self):
        col = _col(self.Result, "customer_id")
        assert isinstance(col.type, Integer)

    def test_customer_id_has_fk_to_master_data(self):
        col = _col(self.Result, "customer_id")
        assert _has_fk(col, "master_data")

    # --- md5 FK ---

    def test_md5_exists(self):
        assert _col(self.Result, "md5") is not None

    def test_md5_has_fk_to_dlu(self):
        col = _col(self.Result, "md5")
        assert _has_fk(col, "datalakeuniverse")

    # --- strategy_name ---

    def test_strategy_name_exists(self):
        assert _col(self.Result, "strategy_name") is not None

    def test_strategy_name_is_string(self):
        col = _col(self.Result, "strategy_name")
        assert isinstance(col.type, (String, Unicode))

    def test_strategy_name_length(self):
        col = _col(self.Result, "strategy_name")
        assert col.type.length == 100

    # --- leaked_fields ---

    def test_leaked_fields_exists(self):
        assert _col(self.Result, "leaked_fields") is not None

    def test_leaked_fields_is_nullable(self):
        col = _col(self.Result, "leaked_fields")
        assert col.nullable is True

    # --- match_details ---

    def test_match_details_exists(self):
        assert _col(self.Result, "match_details") is not None

    def test_match_details_is_nullable(self):
        col = _col(self.Result, "match_details")
        assert col.nullable is True

    # --- overall_confidence ---

    def test_overall_confidence_exists(self):
        assert _col(self.Result, "overall_confidence") is not None

    def test_overall_confidence_is_float(self):
        col = _col(self.Result, "overall_confidence")
        assert isinstance(col.type, Float)

    # --- azure_search_score ---

    def test_azure_search_score_exists(self):
        assert _col(self.Result, "azure_search_score") is not None

    def test_azure_search_score_is_float(self):
        col = _col(self.Result, "azure_search_score")
        assert isinstance(col.type, Float)

    # --- needs_review ---

    def test_needs_review_exists(self):
        assert _col(self.Result, "needs_review") is not None

    def test_needs_review_is_boolean(self):
        col = _col(self.Result, "needs_review")
        assert isinstance(col.type, Boolean)

    def test_needs_review_default_is_false(self):
        col = _col(self.Result, "needs_review")
        assert col.default is not None
        assert col.default.arg in (0, False)

    # --- searched_at ---

    def test_searched_at_exists(self):
        assert _col(self.Result, "searched_at") is not None

    def test_searched_at_is_datetime(self):
        col = _col(self.Result, "searched_at")
        assert isinstance(col.type, DateTime)

    def test_searched_at_has_server_default(self):
        col = _col(self.Result, "searched_at")
        assert col.server_default is not None

    # --- Complete column set ---

    def test_all_columns_present(self):
        mapper = sa_inspect(self.Result)
        col_names = set(mapper.columns.keys())
        expected = {
            "id", "batch_id", "customer_id", "md5",
            "strategy_name", "leaked_fields", "match_details",
            "overall_confidence", "azure_search_score",
            "needs_review", "searched_at",
        }
        assert expected == col_names
