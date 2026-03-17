"""app/cli.py — Click-based CLI entry point for breach-search.

Phase V4-2.1: CLI Entry Point.

All commands share a --verbose flag for DEBUG-level logging.
Error handling: catches service exceptions, prints user-friendly messages to
stderr (via click.echo), exits with code 1. Tracebacks only in --verbose mode.

Invokable as:
    breach-search <command>     (via Poetry scripts)
    python -m app <command>     (via app/__main__.py)
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from typing import Any

import click

# ---------------------------------------------------------------------------
# Lazy imports at module level (aliased so tests can patch them easily)
# ---------------------------------------------------------------------------

# Config / DB
from app.config import Settings
from app.models.database import get_engine, get_session_factory

# Azure Search
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

# Indexing services
from app.services.indexing_service import index_all_files_v2
from app.services.indexing_service_v3 import index_all_files_v3

# Index creation scripts
from scripts.create_search_index import create_index
from scripts.create_search_index_v3 import create_v3_index

# Batch services
from app.services.batch_service import start_batch
from app.services.batch_service_v3 import start_batch_v3

# Batch query service
from app.services.batch_query_service import (
    get_batch_status,
    get_customer_statuses,
    list_all_batches,
)

# Strategy loader
from app.utils.strategy_loader import load_strategies

# Scripts (data generation / seed / compare)
from scripts.generate_simulated_data import generate_all as generate_simulated_data_main
from scripts.seed_database import main as seed_database_main
from scripts.compare_v2_v3 import (
    get_batch_results as compare_get_batch_results,
    compare_results,
    format_comparison,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

DEFAULT_STRATEGIES_FILE = "strategies.yaml"


def _build_db_session() -> Any:
    """Load Settings, create engine from DATABASE_URL, return a Session.

    Raises:
        Exception: Any exception from engine creation or session factory.
    """
    settings = Settings()
    engine = get_engine(settings.DATABASE_URL)
    session_factory = get_session_factory(engine)
    return session_factory()


def _build_search_client(settings: Any, v3: bool = False) -> Any:
    """Create and return an Azure SearchClient.

    Args:
        settings: Settings instance with AZURE_SEARCH_* fields.
        v3: If True, use AZURE_SEARCH_INDEX_V3; otherwise use AZURE_SEARCH_INDEX.

    Returns:
        SearchClient configured for the appropriate index.
    """
    credential = AzureKeyCredential(settings.AZURE_SEARCH_KEY)
    index_name = settings.AZURE_SEARCH_INDEX_V3 if v3 else settings.AZURE_SEARCH_INDEX
    return SearchClient(
        endpoint=settings.AZURE_SEARCH_ENDPOINT,
        index_name=index_name,
        credential=credential,
    )


def _handle_error(
    message: str,
    exc: Exception,
    verbose: bool,
    exit_code: int = 1,
) -> None:
    """Print user-friendly error message. In verbose mode, also print traceback.

    Args:
        message: User-friendly error message to display.
        exc: The exception that was caught.
        verbose: Whether --verbose was passed.
        exit_code: Exit code to use (default 1).
    """
    click.echo(f"Error: {message}")
    if verbose:
        click.echo("")
        click.echo(traceback.format_exc())
    sys.exit(exit_code)


# ---------------------------------------------------------------------------
# Main CLI group
# ---------------------------------------------------------------------------

@click.group()
@click.option("--verbose", is_flag=True, default=False, help="Enable DEBUG-level logging.")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """Breach PII Search CLI — search breach files for customer PII.

    Available commands: generate, seed, index, run, status, compare
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s: %(message)s",
        )


# ---------------------------------------------------------------------------
# generate subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.pass_context
def generate(ctx: click.Context) -> None:
    """Generate simulated breach files for testing."""
    verbose = ctx.obj.get("verbose", False)
    try:
        generate_simulated_data_main()
        click.echo("Simulated data generated successfully.")
    except Exception as exc:
        _handle_error(
            "Failed to generate simulated data.",
            exc,
            verbose=verbose,
        )


# ---------------------------------------------------------------------------
# seed subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.pass_context
def seed(ctx: click.Context) -> None:
    """Seed the database with master customer data and DLU metadata."""
    verbose = ctx.obj.get("verbose", False)
    try:
        seed_database_main()
        click.echo("Database seeded successfully.")
    except Exception as exc:
        _handle_error(
            "Failed to seed database.",
            exc,
            verbose=verbose,
        )


# ---------------------------------------------------------------------------
# index subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.option("--v3", is_flag=True, default=False, help="Use V3 indexing pipeline.")
@click.pass_context
def index(ctx: click.Context, v3: bool) -> None:
    """Create Azure AI Search index and index all eligible files."""
    verbose = ctx.obj.get("verbose", False)
    try:
        settings = Settings()
        db = _build_db_session()
    except Exception as exc:
        _handle_error(
            "Could not connect to database. Check DB_SERVER and DATABASE_URL in your .env file.",
            exc,
            verbose=verbose,
        )
        return  # unreachable, sys.exit called above

    try:
        search_client = _build_search_client(settings, v3=v3)
        if v3:
            create_v3_index()
            response = index_all_files_v3(db, search_client, config=settings)
        else:
            create_index()
            response = index_all_files_v2(db, search_client, config=settings)
    except Exception as exc:
        err_str = str(exc).lower()
        if "auth" in err_str or "403" in err_str or "credential" in err_str or "key" in err_str:
            _handle_error(
                "Azure Search authentication failed. Check AZURE_SEARCH_KEY and "
                "AZURE_SEARCH_ENDPOINT in your .env file.",
                exc,
                verbose=verbose,
            )
        else:
            _handle_error(
                f"Indexing failed: {exc}",
                exc,
                verbose=verbose,
            )
        return

    finally:
        try:
            db.close()
        except Exception:
            pass

    click.echo(
        f"Indexing complete: {response.files_succeeded} succeeded, "
        f"{response.files_failed} failed, {response.files_skipped} skipped."
    )


# ---------------------------------------------------------------------------
# run subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.option("--v3", is_flag=True, default=False, help="Use V3 batch pipeline.")
@click.option(
    "--strategies",
    "strategies_file",
    default=None,
    help="Path to custom strategies YAML file.",
    metavar="FILE",
)
@click.pass_context
def run(ctx: click.Context, v3: bool, strategies_file: str | None) -> None:
    """Run a full batch processing run against all customers."""
    verbose = ctx.obj.get("verbose", False)

    # Build DB session
    try:
        db = _build_db_session()
    except Exception as exc:
        _handle_error(
            "Could not connect to database. Check DB_SERVER and DATABASE_URL in your .env file.",
            exc,
            verbose=verbose,
        )
        return

    try:
        settings = Settings()

        # Check for a running batch before starting a new one
        all_batches = list_all_batches(db)
        running = [b for b in all_batches if b.get("status") == "running"]
        if running:
            batch_id = running[0]["batch_id"]
            click.echo(f"Error: A batch is already running (batch_id: {batch_id})")
            sys.exit(1)

        search_client = _build_search_client(settings, v3=v3)

        if v3:
            batch_id = start_batch_v3(db, search_client)
        else:
            # Load strategies
            strategies_path = strategies_file if strategies_file else DEFAULT_STRATEGIES_FILE
            strategies = load_strategies(strategies_path)
            batch_id = start_batch(db, search_client, strategies)

        click.echo(f"Batch complete. batch_id: {batch_id}")

    except SystemExit:
        raise
    except Exception as exc:
        err_str = str(exc).lower()
        if "database" in err_str or "odbc" in err_str or "connection" in err_str or "sql" in err_str:
            _handle_error(
                "Could not connect to database. Check DB_SERVER and DATABASE_URL in your .env file.",
                exc,
                verbose=verbose,
            )
        else:
            _handle_error(
                f"Batch run failed: {exc}",
                exc,
                verbose=verbose,
            )
        return

    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# status subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.argument("batch_id")
@click.option(
    "--customers",
    is_flag=True,
    default=False,
    help="Include per-customer status entries in output.",
)
@click.pass_context
def status(ctx: click.Context, batch_id: str, customers: bool) -> None:
    """Query status of a batch run (prints JSON)."""
    verbose = ctx.obj.get("verbose", False)

    try:
        db = _build_db_session()
    except Exception as exc:
        _handle_error(
            "Could not connect to database. Check DB_SERVER and DATABASE_URL in your .env file.",
            exc,
            verbose=verbose,
        )
        return

    try:
        result = get_batch_status(db, batch_id)

        if result is None:
            click.echo(f"Batch not found: {batch_id}")
            sys.exit(1)

        if customers:
            customer_statuses = get_customer_statuses(db, batch_id)
            result["customer_statuses"] = customer_statuses or []

        click.echo(json.dumps(result, indent=2, default=str))

    except SystemExit:
        raise
    except Exception as exc:
        _handle_error(
            f"Failed to query batch status: {exc}",
            exc,
            verbose=verbose,
        )

    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# compare subcommand
# ---------------------------------------------------------------------------

@main.command()
@click.argument("v2_batch_id")
@click.argument("v3_batch_id")
@click.pass_context
def compare(ctx: click.Context, v2_batch_id: str, v3_batch_id: str) -> None:
    """Compare V2 and V3 batch results side-by-side."""
    verbose = ctx.obj.get("verbose", False)

    try:
        db = _build_db_session()
    except Exception as exc:
        _handle_error(
            "Could not connect to database. Check DB_SERVER and DATABASE_URL in your .env file.",
            exc,
            verbose=verbose,
        )
        return

    try:
        v2_results = compare_get_batch_results(db, v2_batch_id)
        v3_results = compare_get_batch_results(db, v3_batch_id)

        comparison = compare_results(v2_results, v3_results)
        output = format_comparison(comparison)
        click.echo(output)

    except SystemExit:
        raise
    except Exception as exc:
        _handle_error(
            f"Comparison failed: {exc}",
            exc,
            verbose=verbose,
        )

    finally:
        try:
            db.close()
        except Exception:
            pass
