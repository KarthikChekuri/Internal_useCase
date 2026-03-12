"""Batch router — Phase V2-3.3.

V2 batch endpoints:
- POST /batch/run: Start a new batch run (async via BackgroundTasks)
- POST /batch/{batch_id}/resume: Resume an interrupted batch
- GET  /batch/{batch_id}/status: Get batch progress summary
- GET  /batch/{batch_id}/customers: Get per-customer status list
- GET  /batch/{batch_id}/results: Get detection result rows
- GET  /batches: List all batch runs

Error handling:
- 404: Batch not found
- 409: Batch already running (on POST /batch/run)
- 400: Batch already completed (on POST /batch/{batch_id}/resume)
"""

import json
import logging
import os
from typing import Any, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from app.dependencies import get_db, get_search_client, get_settings
from app.schemas.batch import (
    BatchResultItem,
    BatchRunResponse,
    BatchStatusResponse,
    BatchSummaryItem,
    CustomerStatusItem,
    DetectionStatus,
    IndexingStatus,
    ResumeResponse,
    SearchingStatus,
)
from app.services import batch_service
from app.utils.strategy_loader import load_strategies

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal query helpers (DB queries, all deferred imports to avoid hang)
# ---------------------------------------------------------------------------

def _get_total_customers(db: Any) -> int:
    """Return total count of customers in master_data. Deferred import for safety."""
    from app.models.master_data import MasterData
    return db.query(MasterData).count()


def get_batch_status(db: Any, batch_id: str) -> Optional[dict]:
    """Build a BatchStatusResponse dict from DB tables.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string to look up.

    Returns:
        Dict matching BatchStatusResponse schema, or None if not found.
    """
    from app.models.batch import BatchRun, CustomerStatus
    from app.models.file_status import FileStatus

    # Deferred imports to avoid sqlalchemy hang at module level
    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    # Parse strategy_set JSON
    strategy_set: list[str] = []
    if batch_run.strategy_set:
        try:
            strategy_set = json.loads(batch_run.strategy_set)
        except (json.JSONDecodeError, TypeError):
            strategy_set = []

    # Indexing counts from file_status
    try:
        total_files = db.query(FileStatus).count()
        indexed = db.query(FileStatus).filter_by(status="indexed").count()
        failed_files = db.query(FileStatus).filter_by(status="failed").count()
        skipped = db.query(FileStatus).filter_by(status="skipped").count()
    except Exception:
        total_files = indexed = failed_files = skipped = 0

    # Customer status counts
    total_customers = batch_run.total_customers or 0
    completed_customers = db.query(CustomerStatus).filter_by(
        batch_id=batch_id, status="complete"
    ).count()
    failed_customers = db.query(CustomerStatus).filter_by(
        batch_id=batch_id, status="failed"
    ).count()
    pending_customers = total_customers - completed_customers - failed_customers

    # Detection counts from results table
    try:
        from app.models.result import Result
        total_pairs = db.query(Result).filter_by(batch_id=batch_id).count()
        # "leaks found" = rows where at least one field detected (all rows in results qualify)
        leaks_found = total_pairs
    except Exception:
        total_pairs = leaks_found = 0

    return {
        "batch_id": batch_run.batch_id,
        "status": batch_run.status,
        "started_at": batch_run.started_at,
        "completed_at": batch_run.completed_at,
        "strategy_set": strategy_set,
        "indexing": {
            "total": total_files,
            "indexed": indexed,
            "failed": failed_files,
            "skipped": skipped,
        },
        "searching": {
            "total_customers": total_customers,
            "completed": completed_customers,
            "failed": failed_customers,
            "pending": max(pending_customers, 0),
        },
        "detection": {
            "total_pairs_processed": total_pairs,
            "leaks_found": leaks_found,
        },
    }


def get_customer_statuses(
    db: Any,
    batch_id: str,
    status_filter: Optional[str] = None,
) -> Optional[List[dict]]:
    """Fetch per-customer status rows for a batch.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string.
        status_filter: Optional status to filter by (e.g. "failed").

    Returns:
        List of customer status dicts, or None if batch not found.
    """
    from app.models.batch import BatchRun, CustomerStatus

    # Verify batch exists
    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    query = db.query(CustomerStatus).filter_by(batch_id=batch_id)
    if status_filter:
        query = query.filter_by(status=status_filter)

    rows = query.all()

    results = []
    for row in rows:
        strategies_matched: list[str] = []
        if row.strategies_matched:
            try:
                strategies_matched = json.loads(row.strategies_matched)
            except (json.JSONDecodeError, TypeError):
                strategies_matched = []

        results.append({
            "customer_id": row.customer_id,
            "status": row.status,
            "candidates_found": row.candidates_found,
            "leaks_confirmed": row.leaks_confirmed,
            "strategies_matched": strategies_matched,
            "error_message": row.error_message,
            "processed_at": row.processed_at,
        })

    return results


def get_batch_results(
    db: Any,
    batch_id: str,
    customer_id: Optional[int] = None,
) -> Optional[List[dict]]:
    """Fetch result rows for a batch, ordered by customer_id and overall_confidence desc.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string.
        customer_id: Optional filter to a single customer.

    Returns:
        List of result dicts, or None if batch not found.
    """
    from app.models.batch import BatchRun
    from app.models.result import Result

    # Verify batch exists
    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    query = db.query(Result).filter_by(batch_id=batch_id)
    if customer_id is not None:
        query = query.filter_by(customer_id=customer_id)

    # Order: customer_id asc, overall_confidence desc
    query = query.order_by(Result.customer_id, Result.overall_confidence.desc())

    rows = query.all()

    results = []
    for row in rows:
        leaked_fields: list[str] = []
        if row.leaked_fields:
            try:
                leaked_fields = json.loads(row.leaked_fields)
            except (json.JSONDecodeError, TypeError):
                leaked_fields = []

        match_details: dict = {}
        if row.match_details:
            try:
                match_details = json.loads(row.match_details)
            except (json.JSONDecodeError, TypeError):
                match_details = {}

        results.append({
            "batch_id": row.batch_id,
            "customer_id": row.customer_id,
            "md5": row.md5,
            "strategy_name": row.strategy_name or "",
            "leaked_fields": leaked_fields,
            "match_details": match_details,
            "overall_confidence": row.overall_confidence or 0.0,
            "azure_search_score": row.azure_search_score or 0.0,
            "needs_review": bool(row.needs_review),
            "searched_at": row.searched_at,
        })

    return results


def list_all_batches(db: Any) -> List[dict]:
    """Fetch all batch runs ordered by started_at descending.

    Args:
        db: SQLAlchemy Session.

    Returns:
        List of batch summary dicts.
    """
    from app.models.batch import BatchRun

    rows = db.query(BatchRun).order_by(BatchRun.started_at.desc()).all()

    results = []
    for row in rows:
        strategy_count = 0
        if row.strategy_set:
            try:
                strategies = json.loads(row.strategy_set)
                strategy_count = len(strategies) if isinstance(strategies, list) else 0
            except (json.JSONDecodeError, TypeError):
                strategy_count = 0

        results.append({
            "batch_id": row.batch_id,
            "status": row.status,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
            "total_customers": row.total_customers or 0,
            "strategy_count": strategy_count,
        })

    return results


def _resolve_strategies_file(settings: Any) -> str:
    """Resolve the strategies YAML file path from settings or default.

    Args:
        settings: Application settings object.

    Returns:
        Absolute path to the strategies file.
    """
    # Use STRATEGIES_FILE setting if present, otherwise default to strategies.yaml next to run_batch.py
    strategies_file = getattr(settings, "STRATEGIES_FILE", None)
    if not strategies_file:
        strategies_file = "strategies.yaml"
    # Make absolute if relative (relative to the project root)
    if not os.path.isabs(strategies_file):
        # Project root is two levels up from this file (app/routers/ -> app/ -> project root)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        strategies_file = os.path.join(project_root, strategies_file)
    return strategies_file


# ---------------------------------------------------------------------------
# POST /batch/run — Start a new batch run
# ---------------------------------------------------------------------------

@router.post("/batch/run", response_model=BatchRunResponse)
def run_batch(
    background_tasks: BackgroundTasks,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> BatchRunResponse:
    """Start a new batch run asynchronously via BackgroundTasks.

    Loads strategies from the configured YAML file, checks for conflicts,
    returns immediately with the new batch_id and starts processing in the
    background.

    Returns:
        BatchRunResponse with batch_id, status='running', and total_customers.

    Raises:
        HTTPException 409: If a batch run is already in progress.
    """
    # Load strategies
    strategies_file = _resolve_strategies_file(settings)
    strategies = load_strategies(strategies_file)

    # Count customers for response (before starting batch)
    total = _get_total_customers(db)

    # Check for running batch conflict BEFORE starting background task
    running = batch_service._check_running_batch(db)
    if running is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A batch is already running (batch_id: {running.batch_id})",
        )

    # Create the batch_run row synchronously so we get a batch_id to return
    strategy_names = [s.name for s in strategies]
    batch_id = batch_service._create_batch_run(db, strategy_names=strategy_names, total_customers=total)

    # Initialize customer status rows
    customers = batch_service._get_all_customers(db)
    batch_service._init_customer_statuses(db, batch_id=batch_id, customers=customers)

    # Schedule the heavy processing as a background task
    # (background task creates its own session since this one closes after response)
    background_tasks.add_task(
        _run_batch_background,
        search_client=search_client,
        strategies=strategies,
        batch_id=batch_id,
    )

    logger.info("POST /batch/run: started batch %s with %d customers.", batch_id, total)

    return BatchRunResponse(
        batch_id=batch_id,
        status="running",
        total_customers=total,
    )


def _run_batch_background(
    search_client: Any,
    strategies: list,
    batch_id: str,
) -> None:
    """Background task: process all customers and complete the batch.

    This runs after the HTTP response is returned so POST /batch/run
    is non-blocking. Creates its own DB session since the request session
    is closed after the response is sent.

    Args:
        search_client: Azure SearchClient.
        strategies: List of Strategy objects.
        batch_id: UUID string for this batch run.
    """
    from app.models.database import get_engine, get_session_factory

    settings = get_settings()
    engine = get_engine(str(settings.DATABASE_URL))
    factory = get_session_factory(engine)
    db = factory()
    try:
        customers = batch_service._get_all_customers(db)
        batch_service._process_all_customers(
            db=db,
            search_client=search_client,
            customers=customers,
            strategies=strategies,
            batch_id=batch_id,
        )
        batch_service._complete_batch_run(db, batch_id=batch_id)
        logger.info("Background batch %s completed successfully.", batch_id)
    except Exception as exc:
        logger.error("Background batch %s failed: %s", batch_id, str(exc), exc_info=True)
        try:
            from app.models.batch import BatchRun
            import datetime
            row = db.query(BatchRun).filter_by(batch_id=batch_id).first()
            if row:
                row.status = "failed"
                row.completed_at = datetime.datetime.now(datetime.UTC)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# ---------------------------------------------------------------------------
# POST /batch/{batch_id}/resume — Resume an interrupted batch
# ---------------------------------------------------------------------------

@router.post("/batch/{batch_id}/resume", response_model=ResumeResponse)
def resume_batch(
    batch_id: str,
    background_tasks: BackgroundTasks,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> ResumeResponse:
    """Resume an interrupted batch run asynchronously via BackgroundTasks.

    Validates the batch exists and is resumable, then delegates processing
    to a background task so the HTTP response returns immediately.

    Returns:
        ResumeResponse with batch_id and status.

    Raises:
        HTTPException 400: Batch already completed.
        HTTPException 404: Batch not found.
    """
    strategies_file = _resolve_strategies_file(settings)
    strategies = load_strategies(strategies_file)

    # Validate batch is resumable before scheduling background work
    batch_run = batch_service._get_batch_run(db, batch_id)
    if batch_run is None:
        raise HTTPException(status_code=404, detail=f"Batch not found: {batch_id}")
    if batch_run.status == "completed":
        raise HTTPException(status_code=400, detail="Batch already completed")

    # Schedule resume as a background task (non-blocking)
    background_tasks.add_task(
        _resume_batch_background,
        search_client=search_client,
        strategies=strategies,
        batch_id=batch_id,
    )

    logger.info("POST /batch/%s/resume: batch resume scheduled.", batch_id)
    return ResumeResponse(
        batch_id=batch_id,
        status="running",
        message=f"Batch {batch_id} resume scheduled.",
    )


def _resume_batch_background(
    search_client: Any,
    strategies: list,
    batch_id: str,
) -> None:
    """Background task: resume batch processing.

    Runs after the HTTP response is returned so POST /batch/{id}/resume
    is non-blocking. Creates its own DB session.
    """
    from app.models.database import get_engine, get_session_factory

    settings = get_settings()
    engine = get_engine(str(settings.DATABASE_URL))
    factory = get_session_factory(engine)
    db = factory()
    try:
        batch_service.resume_batch(
            db=db,
            search_client=search_client,
            strategies=strategies,
            batch_id=batch_id,
        )
        logger.info("Background resume batch %s completed.", batch_id)
    except Exception as exc:
        logger.error("Background resume batch %s failed: %s", batch_id, str(exc), exc_info=True)
        try:
            from app.models.batch import BatchRun
            import datetime
            row = db.query(BatchRun).filter_by(batch_id=batch_id).first()
            if row:
                row.status = "failed"
                row.completed_at = datetime.datetime.now(datetime.UTC)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# ---------------------------------------------------------------------------
# GET /batch/{batch_id}/status — Batch progress summary
# ---------------------------------------------------------------------------

@router.get("/batch/{batch_id}/status", response_model=BatchStatusResponse)
def batch_status(
    batch_id: str,
    db: Any = Depends(get_db),
) -> BatchStatusResponse:
    """Return a summary of a batch run's progress.

    Raises:
        HTTPException 404: Batch not found.
    """
    status_dict = get_batch_status(db, batch_id)
    if status_dict is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    return BatchStatusResponse(
        batch_id=status_dict["batch_id"],
        status=status_dict["status"],
        started_at=status_dict["started_at"],
        completed_at=status_dict.get("completed_at"),
        strategy_set=status_dict["strategy_set"],
        indexing=IndexingStatus(**status_dict["indexing"]),
        searching=SearchingStatus(**status_dict["searching"]),
        detection=DetectionStatus(**status_dict["detection"]),
    )


# ---------------------------------------------------------------------------
# GET /batch/{batch_id}/customers — Per-customer status list
# ---------------------------------------------------------------------------

@router.get("/batch/{batch_id}/customers", response_model=List[CustomerStatusItem])
def batch_customers(
    batch_id: str,
    status: Optional[str] = None,
    db: Any = Depends(get_db),
) -> List[CustomerStatusItem]:
    """Return per-customer status within a batch.

    Args:
        batch_id: UUID string of the batch run.
        status: Optional filter (e.g. "failed", "complete", "pending").

    Raises:
        HTTPException 404: Batch not found.
    """
    rows = get_customer_statuses(db, batch_id, status_filter=status)
    if rows is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    return [CustomerStatusItem(**row) for row in rows]


# ---------------------------------------------------------------------------
# GET /batch/{batch_id}/results — Detection result rows
# ---------------------------------------------------------------------------

@router.get("/batch/{batch_id}/results", response_model=List[BatchResultItem])
def batch_results(
    batch_id: str,
    customer_id: Optional[int] = None,
    db: Any = Depends(get_db),
) -> List[BatchResultItem]:
    """Return detection result rows for a batch.

    Args:
        batch_id: UUID string of the batch run.
        customer_id: Optional filter to a single customer.

    Raises:
        HTTPException 404: Batch not found.
    """
    rows = get_batch_results(db, batch_id, customer_id=customer_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    return [BatchResultItem(**row) for row in rows]


# ---------------------------------------------------------------------------
# GET /batches — List all batch runs
# ---------------------------------------------------------------------------

@router.get("/batches", response_model=List[BatchSummaryItem])
def batches_list(
    db: Any = Depends(get_db),
) -> List[BatchSummaryItem]:
    """Return all batch runs ordered by started_at descending."""
    rows = list_all_batches(db)
    return [BatchSummaryItem(**row) for row in rows]
