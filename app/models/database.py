"""
SQLAlchemy 2.0 database configuration.

Engine and session factories use postgresql+psycopg2 dialect.
Connection string is read from the DATABASE_URL environment variable /
.env file via pydantic-settings (loaded in app/config.py).
"""

from __future__ import annotations

import os

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


def get_engine(database_url: str | None = None) -> Engine:
    """
    Create and return a SQLAlchemy Engine.

    Parameters
    ----------
    database_url:
        Full connection string, e.g.
        ``postgresql+psycopg2://user:pass@host:5432/dbname``
        Falls back to the ``DATABASE_URL`` environment variable when not provided.
    """
    url = database_url or os.environ.get("DATABASE_URL", "")
    return create_engine(url, echo=False)


def get_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    """
    Return a :class:`~sqlalchemy.orm.sessionmaker` bound to *engine*.

    If *engine* is ``None`` the engine is built from the current environment.
    """
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)
