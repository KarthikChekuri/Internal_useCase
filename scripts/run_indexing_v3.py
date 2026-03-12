"""Standalone CLI script to run the V3 indexing pipeline with PII Detection.

Usage:
    python scripts/run_indexing_v3.py

Reads configuration from environment variables (or .env file via
pydantic-settings) and indexes all eligible DLU files into the Azure AI
Search V3 index with PII metadata fields (has_ssn, has_name, has_dob, etc.).

Requires environment variables (or .env file):
    DATABASE_URL
    AZURE_SEARCH_ENDPOINT
    AZURE_SEARCH_KEY
    AZURE_SEARCH_INDEX_V3        (default: breach-file-index-v3)
    AZURE_LANGUAGE_ENDPOINT      (Azure AI Language endpoint)
    AZURE_LANGUAGE_KEY           (Azure AI Language API key)
"""

import logging
import sys

from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

from app.config import get_settings
from app.models.database import get_session_factory
from app.services.indexing_service_v3 import index_all_files_v3

logger = logging.getLogger(__name__)


def main():
    """Wire up dependencies and run the V3 indexing pipeline.

    Returns:
        IndexResponse from index_all_files_v3.
    """
    settings = get_settings()

    # Create DB session
    engine = __import__('sqlalchemy').create_engine(
        settings.DATABASE_URL, echo=False, use_setinputsizes=False
    )
    session_factory = get_session_factory(engine)
    db = session_factory()

    # Create Azure AI Search client pointing at V3 index
    search_credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    search_client = SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=settings.AZURE_SEARCH_INDEX_V3,
        credential=search_credential,
    )

    # Create Azure AI Language (TextAnalytics) client for PII detection
    language_endpoint = getattr(settings, "AZURE_LANGUAGE_ENDPOINT", None)
    language_key = getattr(settings, "AZURE_LANGUAGE_KEY", None)

    pii_client = None
    if language_endpoint and language_key:
        pii_client = TextAnalyticsClient(
            endpoint=language_endpoint,
            credential=AzureKeyCredential(language_key),
        )
        logger.info("PII Detection client initialised at %s", language_endpoint)
    else:
        logger.warning(
            "AZURE_LANGUAGE_ENDPOINT or AZURE_LANGUAGE_KEY not set — "
            "PII Detection will be skipped and all documents indexed with default metadata."
        )

    try:
        result = index_all_files_v3(db, search_client, settings, pii_client=pii_client)
        logger.info(
            "V3 indexing complete: %d processed, %d succeeded, %d failed, %d skipped.",
            result.files_processed,
            result.files_succeeded,
            result.files_failed,
            result.files_skipped,
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
