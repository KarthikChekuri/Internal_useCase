"""
Tests for SQLAlchemy ORM model definitions.

NOTE (V2-1.1): DLU has been rewritten (V2 schema), master_pii.py and
search_result.py have been removed. Tests for those V1 models have been
removed here; see tests/models/test_v2_models.py for the V2 equivalents.

These tests verify class-level metadata only — no database connection is made.
"""

import pytest
from sqlalchemy import (
    Boolean, Date, DateTime, Float, Integer, String, Unicode,
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


# ---------------------------------------------------------------------------
# NOTE: V1 DLU tests removed (V2-1.1 rewrote DLU to MD5 PK / file_path only)
# NOTE: V1 MasterPII tests removed (replaced by MasterData in master_data.py)
# NOTE: V1 SearchResult tests removed (replaced by Result in result.py)
# See tests/models/test_v2_models.py for all V2 model tests.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Smoke tests: V2 modules importable + V1 modules gone
# ---------------------------------------------------------------------------

class TestLegacyModelsRemoved:
    """Confirm V1 model files are gone and V2 replacements are importable."""

    def test_master_data_importable(self):
        from app.models.master_data import MasterData
        assert MasterData is not None

    def test_batch_importable(self):
        from app.models.batch import BatchRun, CustomerStatus
        assert BatchRun is not None
        assert CustomerStatus is not None

    def test_file_status_importable(self):
        from app.models.file_status import FileStatus
        assert FileStatus is not None

    def test_result_importable(self):
        from app.models.result import Result
        assert Result is not None

    def test_dlu_v2_importable(self):
        from app.models.dlu import DLU
        assert DLU is not None

    def test_master_pii_module_removed(self):
        import sys
        sys.modules.pop("app.models.master_pii", None)
        try:
            import app.models.master_pii  # noqa: F401
            assert False, "master_pii should not be importable after V2 removal"
        except (ImportError, ModuleNotFoundError):
            pass  # expected

    def test_search_result_module_removed(self):
        import sys
        sys.modules.pop("app.models.search_result", None)
        try:
            import app.models.search_result  # noqa: F401
            assert False, "search_result should not be importable after V2 removal"
        except (ImportError, ModuleNotFoundError):
            pass  # expected


# ---------------------------------------------------------------------------
# Preserved: Base / database module tests (still valid in V2)
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
