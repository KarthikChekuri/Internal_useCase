"""
SQLAlchemy 2.0 database configuration.

Engine and session factories use mssql+pyodbc dialect.
Connection string is read from the DATABASE_URL environment variable /
.env file via pydantic-settings (loaded in app/config.py).

Do NOT import pyodbc directly here — the import is lazy through SQLAlchemy.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import Any

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


def get_engine(database_url: str | None = None) -> Engine:
    """
    Create and return a SQLAlchemy Engine for SQL Server via pyodbc.

    Parameters
    ----------
    database_url:
        Full connection string, e.g.
        ``mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server``
        Falls back to the ``DATABASE_URL`` environment variable when not provided.
    """
    url = database_url or os.environ.get("DATABASE_URL", "")
    # use_setinputsizes=False works around the legacy SQL Server ODBC driver's
    # inability to handle NVARCHAR(MAX) parameters in batch inserts.
    return create_engine(url, echo=False, use_setinputsizes=False)


def get_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    """
    Return a :class:`~sqlalchemy.orm.sessionmaker` bound to *engine*.

    If *engine* is ``None`` the engine is built from the current environment.
    """
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db(engine: Engine | None = None) -> Generator[Session, Any, None]:
    """
    FastAPI dependency that yields a database session and closes it afterwards.

    Usage::

        @router.get("/")
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    factory = get_session_factory(engine)
    db = factory()
    try:
        yield db
    finally:
        db.close()
