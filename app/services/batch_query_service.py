"""Batch Query Service — Phase V4-1.3.

Pure DB query functions extracted from app/routers/batch.py.

Zero FastAPI dependency: no Depends(), no Request, no Response.
All functions accept a SQLAlchemy Session and return plain Python dicts.

Functions:
- get_batch_status(db, batch_id) -> dict | None
- get_customer_statuses(db, batch_id, status_filter=None) -> list[dict] | None
- get_batch_results(db, batch_id, customer_id=None) -> list[dict] | None
- list_all_batches(db) -> list[dict]
"""

import json
from typing import Any, List, Optional


def get_batch_status(db: Any, batch_id: str) -> Optional[dict]:
    """Build a batch status dict from DB tables.

    Returns None if the batch does not exist.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string to look up.

    Returns:
        Dict with keys: batch_id, status, started_at, completed_at,
        strategy_set, total_customers, completed_customers, failed_customers,
        plus indexing and detection sub-dicts (carried from existing logic).
        Returns None if batch not found.
    """
    from app.models.batch import BatchRun, CustomerStatus
    from app.models.file_status import FileStatus

    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    # Parse strategy_set JSON
    strategy_set: list = []
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
        leaks_found = total_pairs
    except Exception:
        total_pairs = leaks_found = 0

    return {
        "batch_id": batch_run.batch_id,
        "status": batch_run.status,
        "started_at": batch_run.started_at,
        "completed_at": batch_run.completed_at,
        "strategy_set": strategy_set,
        "total_customers": total_customers,
        "completed_customers": completed_customers,
        "failed_customers": failed_customers,
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

    batch_run = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if batch_run is None:
        return None

    query = db.query(CustomerStatus).filter_by(batch_id=batch_id)
    if status_filter:
        query = query.filter_by(status=status_filter)

    rows = query.all()

    results = []
    for row in rows:
        strategies_matched: list = []
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
        Empty list if batch exists but has no results.
    """
    from app.models.batch import BatchRun
    from app.models.result import Result

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
        leaked_fields: list = []
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
        List of batch summary dicts (most recent first).
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
