"""CLI script to run a breach PII batch processing run.

Usage:
    python run_batch.py
    python run_batch.py --strategies custom_strategies.yaml

The script uses the same service layer as the API endpoint (batch_service).
Progress is logged to the console. The script exits when processing is complete.
"""

import argparse
import logging
import os
import sys

# Configure console logging before any imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Run breach PII batch processing from the command line."
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default=None,
        help=(
            "Path to a strategies YAML file. "
            "Defaults to 'strategies.yaml' in the project root."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Entry point: load config, connect to DB and Azure Search, run batch."""
    args = parse_args()

    # Resolve strategies file path
    if args.strategies:
        strategies_file = args.strategies
    else:
        # Default: strategies.yaml next to this script
        strategies_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "strategies.yaml")

    logger.info("Starting batch run using strategies file: %s", strategies_file)

    # Load strategies (pure Python, no DB needed)
    from app.utils.strategy_loader import load_strategies
    strategies = load_strategies(strategies_file)
    logger.info("Loaded %d strategies: %s", len(strategies), [s.name for s in strategies])

    # Load settings
    from app.config import Settings
    settings = Settings()

    # Build DB session (deferred import to avoid sqlalchemy hang in tests)
    from app.models.database import get_engine, get_session_factory
    engine = get_engine(settings.DATABASE_URL)
    session_factory = get_session_factory(engine)
    db = session_factory()

    # Build Azure Search client
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents import SearchClient
    credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    search_client = SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=settings.AZURE_SEARCH_INDEX,
        credential=credential,
    )

    try:
        from app.services import batch_service

        logger.info("Starting batch...")
        batch_id = batch_service.start_batch(
            db=db,
            search_client=search_client,
            strategies=strategies,
        )
        logger.info("Batch complete. batch_id: %s", batch_id)

    except ValueError as exc:
        logger.error("Batch start failed: %s", str(exc))
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected error during batch: %s", str(exc), exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
