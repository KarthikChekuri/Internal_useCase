"""Dependency injection functions for FastAPI.

Provides:
- get_db(): yields a SQLAlchemy session, closes on completion
- get_search_client(): returns an Azure SearchClient instance
- get_search_client_v3(): returns an Azure SearchClient instance for the V3 index
- get_settings(): returns a cached Settings instance
"""

import logging
from collections.abc import Generator
from functools import lru_cache
from typing import Any

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from sqlalchemy.orm import Session

from app.config import Settings
from app.models.database import get_engine, get_session_factory

logger = logging.getLogger(__name__)


@lru_cache
def get_settings() -> Any:
    """Return a cached Settings instance loaded from environment variables.

    Uses lru_cache so the Settings object is created only once and reused
    across all requests. Return type is Any to prevent FastAPI from treating
    Settings (a Pydantic BaseSettings subclass) as a body parameter.
    """
    return Settings()


def get_db(engine: Any = None) -> Generator[Session, Any, None]:
    """FastAPI dependency that yields a SQLAlchemy session.

    Yields a session from the session factory and ensures it is closed
    when the request completes, even if an exception is raised.

    Args:
        engine: Optional SQLAlchemy Engine. If None, uses DATABASE_URL from settings.
    """
    if engine is None:
        settings = get_settings()
        engine = get_engine(settings.DATABASE_URL)
    factory = get_session_factory(engine)
    db = factory()
    try:
        yield db
    finally:
        db.close()


def get_search_client() -> Any:
    """Return an Azure AI Search client configured from settings.

    Internally calls get_settings() to load configuration.
    Return type is Any to prevent FastAPI from treating SearchClient
    as a body parameter.

    Returns:
        Configured SearchClient instance.
    """
    settings = get_settings()
    credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    client = SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=settings.AZURE_SEARCH_INDEX,
        credential=credential,
    )
    return client


def get_search_client_v3() -> Any:
    """Return an Azure AI Search client configured for the V3 index.

    Internally calls get_settings() to load configuration.
    Uses AZURE_SEARCH_INDEX_V3 instead of AZURE_SEARCH_INDEX so that V2
    and V3 pipeline stages point at separate indexes.
    Return type is Any to prevent FastAPI from treating SearchClient
    as a body parameter.

    Returns:
        Configured SearchClient instance pointing at the V3 index.
    """
    settings = get_settings()
    credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    client = SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=settings.AZURE_SEARCH_INDEX_V3,
        credential=credential,
    )
    return client
