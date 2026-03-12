"""Standalone script to run the full indexing pipeline.

Usage:
    python scripts/run_indexing.py

Reads configuration from environment variables (or .env file via
pydantic-settings) and indexes all eligible DLU files into Azure AI Search.
"""

import logging
import sys

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

from app.config import get_settings
from app.models.database import get_session_factory
from app.services.indexing_service import index_all_files_v2 as index_all_files

logger = logging.getLogger(__name__)


def main():
    """Wire up dependencies and run the full indexing pipeline.

    Returns:
        IndexResponse from index_all_files.
    """
    settings = get_settings()

    # Create DB session using the settings DATABASE_URL
    engine = __import__('sqlalchemy').create_engine(
        settings.DATABASE_URL, echo=False, use_setinputsizes=False
    )
    session_factory = get_session_factory(engine)
    db = session_factory()

    # Create Azure AI Search client
    credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    search_client = SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=settings.AZURE_SEARCH_INDEX,
        credential=credential,
    )

    try:
        result = index_all_files(db, search_client, settings)
        logger.info(
            "Indexing complete: %d processed, %d succeeded, %d failed.",
            result.files_processed,
            result.files_succeeded,
            result.files_failed,
        )
        if result.errors:
            for err in result.errors:
                logger.error("  - %s", err)
        return result
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = main()
    if result and result.files_failed > 0:
        sys.exit(1)
