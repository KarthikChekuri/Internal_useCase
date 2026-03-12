"""V3 Batch Router — Phase V3-4.1.

V3 batch endpoints (Azure-only pipeline):
- POST /v3/index/all:               Index all files via V3 indexing service
- POST /v3/batch/run:               Start a new V3 batch run (async via BackgroundTasks)
- GET  /v3/batch/{batch_id}/status: Get V3 batch progress summary
- GET  /v3/batch/{batch_id}/results: Get V3 detection result rows

Error handling:
- 404: Batch not found
- 409: Batch already running (on POST /v3/batch/run)
"""

import json
import logging
import uuid
from typing import Any, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from app.dependencies import get_db, get_search_client_v3, get_settings
from app.schemas.search_v3 import (
    V3BatchResultResponse,
    V3BatchRunResponse,
    V3BatchStatusResponse,
)
from app.services.indexing_service_v3 import index_all_files_v3

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal query helpers (deferred imports to avoid sqlalchemy hang)
# ---------------------------------------------------------------------------


def _get_total_customers_v3(db: Any) -> int:
    """Return total count of customers in master_data."""
    from app.models.master_data import MasterData
    return db.query(MasterData).count()


def _get_running_batch_v3(db: Any) -> Any:
    """Return a running BatchRun row, or None if none exist."""
    from app.models.batch import BatchRun
    return db.query(BatchRun).filter_by(status="running").first()


def _get_v3_batch_status(db: Any, batch_id: str) -> dict | None:
    """Build a V3BatchStatusResponse dict from DB tables.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string to look up.

    Returns:
        Dict matching V3BatchStatusResponse schema, or None if not found.
    """
    from app.models.batch import BatchRun, CustomerStatus

    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    total_customers = batch_run.total_customers or 0
    customers_completed = db.query(CustomerStatus).filter_by(
        batch_id=batch_id, status="complete"
    ).count()
    customers_failed = db.query(CustomerStatus).filter_by(
        batch_id=batch_id, status="failed"
    ).count()

    # Build per-customer detail list
    cs_rows = db.query(CustomerStatus).filter_by(batch_id=batch_id).all()
    customer_details = []
    for row in cs_rows:
        customer_details.append({
            "customer_id": row.customer_id,
            "status": row.status,
            "candidates_found": row.candidates_found,
            "leaks_confirmed": row.leaks_confirmed,
            "error_message": row.error_message,
        })

    return {
        "batch_id": batch_run.batch_id,
        "status": batch_run.status,
        "total_customers": total_customers,
        "customers_completed": customers_completed,
        "customers_failed": customers_failed,
        "customer_details": customer_details,
        "method": "v3_azure_only",
    }


def _get_v3_batch_results(db: Any, batch_id: str) -> list[dict] | None:
    """Fetch result rows for a V3 batch.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string.

    Returns:
        List of result dicts, or None if batch not found.
    """
    from app.models.batch import BatchRun
    from app.models.result import Result

    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    rows = db.query(Result).filter_by(batch_id=batch_id).all()

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
            "strategy_name": row.strategy_name or "v3_azure_only",
            "leaked_fields": leaked_fields,
            "match_details": match_details,
            "overall_confidence": row.overall_confidence or 0.0,
            "azure_search_score": row.azure_search_score or 0.0,
            "needs_review": bool(row.needs_review),
            "searched_at": row.searched_at,
        })

    return results


# ---------------------------------------------------------------------------
# Background task helper
# ---------------------------------------------------------------------------


def _run_v3_batch_background(search_client: Any, batch_id: str) -> None:
    """Background task: run the V3 batch pipeline.

    Creates its own DB session since the request session closes after the
    HTTP response is returned.

    Args:
        search_client: Azure SearchClient instance for the V3 index.
        batch_id: UUID string for this batch run (passed to start_batch_v3
                  so the DB row matches the ID returned to the user).
    """
    from app.models.database import get_engine, get_session_factory
    from app.services.batch_service_v3 import start_batch_v3

    settings = get_settings()
    engine = get_engine(str(settings.DATABASE_URL))
    factory = get_session_factory(engine)
    db = factory()
    try:
        start_batch_v3(db, search_client, batch_id=batch_id)
        logger.info("[V3] Background batch %s completed successfully.", batch_id)
    except Exception as exc:
        logger.error(
            "[V3] Background batch (pre-id=%s) failed: %s",
            batch_id,
            str(exc),
            exc_info=True,
        )
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
# POST /index/all — index all files via V3 indexing service
# ---------------------------------------------------------------------------


@router.post("/index/all")
def index_all_v3(
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client_v3),
) -> Any:
    """Index all eligible files from DLU into the V3 Azure AI Search index.

    Calls the V3 indexing service which adds PII metadata fields to each
    document before upload.

    Returns:
        IndexResponse with counts (files_processed, files_succeeded, etc.)
        and error messages.
    """
    config = get_settings()
    result = index_all_files_v3(db, search_client, config)
    return result


# ---------------------------------------------------------------------------
# POST /batch/run — Start a new V3 batch run
# ---------------------------------------------------------------------------


@router.post("/batch/run", response_model=V3BatchRunResponse, status_code=202)
def run_v3_batch(
    background_tasks: BackgroundTasks,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client_v3),
) -> V3BatchRunResponse:
    """Start a new V3 batch run asynchronously via BackgroundTasks.

    Checks for a concurrently running batch, then returns immediately with
    HTTP 202. The actual processing runs in a background task.

    Returns:
        V3BatchRunResponse with batch_id, status='running', total_customers,
        and method='v3_azure_only'.

    Raises:
        HTTPException 409: If a V3 batch run is already in progress.
    """
    # Check for concurrent running batch
    running = _get_running_batch_v3(db)
    if running is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A V3 batch is already running (batch_id: {running.batch_id})",
        )

    # Count customers synchronously for the immediate response
    total = _get_total_customers_v3(db)

    # Generate a batch_id to return in the response
    batch_id = str(uuid.uuid4())

    # Schedule heavy processing as a background task
    # (background task creates its own session since this one closes after response)
    background_tasks.add_task(
        _run_v3_batch_background,
        search_client=search_client,
        batch_id=batch_id,
    )

    logger.info(
        "POST /v3/batch/run: scheduled V3 batch (pre-id=%s) for %d customers.",
        batch_id,
        total,
    )

    return V3BatchRunResponse(
        batch_id=batch_id,
        status="running",
        total_customers=total,
        method="v3_azure_only",
    )


# ---------------------------------------------------------------------------
# GET /batch/{batch_id}/status — V3 batch progress summary
# ---------------------------------------------------------------------------


@router.get("/batch/{batch_id}/status", response_model=V3BatchStatusResponse)
def v3_batch_status(
    batch_id: str,
    db: Any = Depends(get_db),
) -> V3BatchStatusResponse:
    """Return a summary of a V3 batch run's progress.

    Raises:
        HTTPException 404: Batch not found.
    """
    status_dict = _get_v3_batch_status(db, batch_id)
    if status_dict is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    return V3BatchStatusResponse(
        batch_id=status_dict["batch_id"],
        status=status_dict["status"],
        total_customers=status_dict["total_customers"],
        customers_completed=status_dict["customers_completed"],
        customers_failed=status_dict["customers_failed"],
        customer_details=status_dict["customer_details"],
        method=status_dict["method"],
    )


# ---------------------------------------------------------------------------
# GET /batch/{batch_id}/results — V3 detection result rows
# ---------------------------------------------------------------------------


@router.get("/batch/{batch_id}/results", response_model=List[V3BatchResultResponse])
def v3_batch_results(
    batch_id: str,
    db: Any = Depends(get_db),
) -> List[V3BatchResultResponse]:
    """Return V3 result rows for a batch.

    Args:
        batch_id: UUID string of the V3 batch run.

    Raises:
        HTTPException 404: Batch not found.
    """
    rows = _get_v3_batch_results(db, batch_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    return [V3BatchResultResponse(**row) for row in rows]
