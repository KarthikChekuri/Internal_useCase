"""Batch Orchestration Service — Phase V2-3.1.

Provides the batch processing pipeline that:
1. Creates a batch run (batch_runs row) with a unique UUID
2. Initializes per-customer status rows (customer_status) as 'pending'
3. Processes each customer in customer_id order:
   a. Check resumability: skip 'complete', retry 'failed', process 'pending'
   b. Mark status 'searching' -> run all strategies -> get candidate files
   c. Mark status 'detecting' -> run leak detection on each candidate
   d. Persist result rows into [Search].[results] for (customer, file) pairs with leaks
   e. Mark status 'complete' with candidate/leak counts
4. On error: mark customer 'failed', continue to next customer
5. On completion: update batch_runs to 'completed'

Public API:
    start_batch(db, search_client, strategies) -> batch_id (str)
    resume_batch(db, search_client, strategies, batch_id) -> batch_id (str)

Internal helpers (also unit-testable):
    _create_batch_run(db, strategy_names, total_customers) -> batch_id (str)
    _init_customer_statuses(db, batch_id, customers)
    _get_all_customers(db) -> list
    _get_batch_run(db, batch_id) -> BatchRun | None
    _check_running_batch(db) -> BatchRun | None
    _get_customer_status(db, batch_id, customer_id) -> CustomerStatus | None
    _update_customer_status(db, batch_id, customer_id, status, **kwargs)
    _complete_batch_run(db, batch_id)
    _process_customer(db, search_client, customer, strategies, batch_id)
    _process_all_customers(db, search_client, customers, strategies, batch_id)
    _persist_result(db, batch_id, customer_id, candidate, leak_result, overall_confidence)
    _compute_overall_confidence(leak_result) -> float
"""

import datetime
import json
import logging
import uuid
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Service-layer imports — these modules do NOT import sqlalchemy at module level,
# so they are safe to import here and can be patched in unit tests via
# patch("app.services.batch_service.<name>").
from app.services.search_service import search_customer
from app.services.text_extraction import extract_text
from app.services.leak_detection_service import detect_leaks
from app.utils.confidence import (
    compute_overall_confidence as _weighted_confidence,
    normalize_search_scores,
)

# SQLAlchemy ORM model imports are deferred to inside each function body
# to avoid the known hang issue in this environment.


# ---------------------------------------------------------------------------
# PII field names (all 13 fields evaluated by leak_detection_service)
# ---------------------------------------------------------------------------

ALL_PII_FIELDS = [
    "SSN", "DOB", "DriversLicense", "ZipCode", "State",
    "Fullname", "FirstName", "LastName",
    "Address1", "Address2", "Address3",
    "City", "Country",
]


# ---------------------------------------------------------------------------
# Structured progress logging helpers (spec-mandated formats)
# ---------------------------------------------------------------------------

def _log_customer_progress(
    customer_idx: int,
    total_customers: int,
    candidates_found: int,
    leaks_confirmed: int,
    strategies_matched: list,
) -> None:
    """Log per-customer completion progress in the spec-required format.

    Emits: "Customer {idx}/{total}: {candidates} candidates, {leaks} leaks confirmed ({strategies})"

    Args:
        customer_idx: 1-based index of the customer being processed.
        total_customers: Total number of customers in the batch.
        candidates_found: Number of candidate files found for this customer.
        leaks_confirmed: Number of files with at least one PII field detected.
        strategies_matched: List of strategy names that returned results.
    """
    strategy_str = ", ".join(strategies_matched) if strategies_matched else "none"
    logger.info(
        "Customer %d/%d: %d candidates, %d leaks confirmed (%s)",
        customer_idx,
        total_customers,
        candidates_found,
        leaks_confirmed,
        strategy_str,
    )


def _log_batch_complete(
    total_customers: int,
    total_leaks: int,
    files_with_leaks: int,
    customers_failed: int,
) -> None:
    """Log batch completion summary in the spec-required format.

    Emits: "Batch complete: {customers} customers, {leaks} total leaks across {files} files, {failed} customers failed"

    Args:
        total_customers: Total number of customers processed.
        total_leaks: Total number of confirmed leak results across all customers.
        files_with_leaks: Total number of unique files containing leaks.
        customers_failed: Number of customers that ended in 'failed' status.
    """
    logger.info(
        "Batch complete: %d customers, %d total leaks across %d files, %d customers failed",
        total_customers,
        total_leaks,
        files_with_leaks,
        customers_failed,
    )


def _collect_batch_summary(db: Any, batch_id: str) -> dict:
    """Collect summary statistics for a completed batch run.

    Queries customer_status rows to compute:
    - total_leaks: sum of leaks_confirmed across all customers
    - files_with_leaks: sum of candidates_found for customers with leaks > 0 (approximation)
    - customers_failed: count of customers with status 'failed'

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string for this batch run.

    Returns:
        Dict with keys: total_leaks, files_with_leaks, customers_failed.
    """
    from app.models.batch import CustomerStatus
    from app.models.result import Result

    rows = db.query(CustomerStatus).filter_by(batch_id=batch_id).all()

    total_leaks = sum(r.leaks_confirmed or 0 for r in rows)
    # Count distinct files with leaks (unique MD5s in results table)
    files_with_leaks = db.query(Result.md5).filter_by(batch_id=batch_id).distinct().count()
    customers_failed = sum(1 for r in rows if r.status == "failed")

    return {
        "total_leaks": total_leaks,
        "files_with_leaks": files_with_leaks,
        "customers_failed": customers_failed,
    }


# ---------------------------------------------------------------------------
# Public API: start_batch
# ---------------------------------------------------------------------------

def start_batch(
    db: Any,
    search_client: Any,
    strategies: list,
) -> str:
    """Start a new batch run, processing all customers sequentially.

    Raises:
        ValueError: If a batch is already running.

    Args:
        db: SQLAlchemy Session (mocked in unit tests).
        search_client: Azure SearchClient instance (mocked in unit tests).
        strategies: List of Strategy objects to use for searching.

    Returns:
        batch_id (str): UUID of the newly created batch run.
    """
    # Check for conflicts
    running = _check_running_batch(db)
    if running is not None:
        raise ValueError(
            f"A batch is already running (batch_id: {running.batch_id})"
        )

    # Load all customers
    customers = _get_all_customers(db)
    total_customers = len(customers)
    strategy_names = [s.name for s in strategies]

    logger.info(
        "Starting new batch run: %d customers, %d strategies: %s",
        total_customers,
        len(strategy_names),
        strategy_names,
    )

    # Count total files in DLU for status tracking
    from app.models.dlu import DLU
    total_files = db.query(DLU).count()

    # Create batch_runs row
    batch_id = _create_batch_run(
        db, strategy_names=strategy_names, total_customers=total_customers,
        total_files=total_files,
    )

    # Initialize all customer_status rows as 'pending'
    _init_customer_statuses(db, batch_id=batch_id, customers=customers)

    # Process all customers
    _process_all_customers(
        db=db,
        search_client=search_client,
        customers=customers,
        strategies=strategies,
        batch_id=batch_id,
    )

    # Mark batch as completed
    _complete_batch_run(db, batch_id=batch_id)

    # Emit spec-format completion summary log
    summary = _collect_batch_summary(db, batch_id=batch_id)
    _log_batch_complete(
        total_customers=total_customers,
        total_leaks=summary["total_leaks"],
        files_with_leaks=summary["files_with_leaks"],
        customers_failed=summary["customers_failed"],
    )

    logger.info("Batch %s complete.", batch_id)
    return batch_id


# ---------------------------------------------------------------------------
# Public API: resume_batch
# ---------------------------------------------------------------------------

def resume_batch(
    db: Any,
    search_client: Any,
    strategies: list,
    batch_id: str,
) -> str:
    """Resume an interrupted batch run.

    Skips 'complete' customers, retries 'failed', processes 'pending'.

    Raises:
        ValueError: If batch is already completed or not found.

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        strategies: List of Strategy objects.
        batch_id: UUID string of the batch to resume.

    Returns:
        batch_id (str): The batch_id that was resumed.
    """
    batch_run = _get_batch_run(db, batch_id)

    if batch_run is None:
        raise ValueError(f"Batch not found: {batch_id}")

    if batch_run.status == "completed":
        raise ValueError("Batch already completed")

    customers = _get_all_customers(db)

    logger.info(
        "Resuming batch %s: %d total customers.",
        batch_id,
        len(customers),
    )

    _process_all_customers(
        db=db,
        search_client=search_client,
        customers=customers,
        strategies=strategies,
        batch_id=batch_id,
    )

    _complete_batch_run(db, batch_id=batch_id)
    logger.info("Batch %s resumed and completed.", batch_id)
    return batch_id


# ---------------------------------------------------------------------------
# Internal: batch_runs CRUD
# ---------------------------------------------------------------------------

def _create_batch_run(
    db: Any,
    strategy_names: list[str],
    total_customers: int,
    total_files: int = 0,
) -> str:
    """Insert a new row into [Batch].[batch_runs] with status='running'.

    Args:
        db: SQLAlchemy Session.
        strategy_names: List of strategy names for audit trail.
        total_customers: Count of customers to process.
        total_files: Count of files in DLU (for status tracking).

    Returns:
        batch_id (str): The UUID string for the new batch run.
    """
    # Import here to avoid sqlalchemy hanging at module level
    from app.models.batch import BatchRun

    batch_id = str(uuid.uuid4())
    now = datetime.datetime.now(datetime.UTC)

    row = BatchRun(
        batch_id=batch_id,
        status="running",
        strategy_set=json.dumps(strategy_names),
        started_at=now,
        completed_at=None,
        total_customers=total_customers,
        total_files=total_files,
    )
    db.add(row)
    db.commit()

    logger.info(
        "Created batch run %s with %d customers, strategies: %s",
        batch_id,
        total_customers,
        strategy_names,
    )
    return batch_id


def _complete_batch_run(db: Any, batch_id: str) -> None:
    """Update batch_runs row to status='completed' with completed_at timestamp.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string of the batch run to complete.
    """
    from app.models.batch import BatchRun

    row = db.query(BatchRun).filter_by(batch_id=batch_id).first()
    if row is None:
        logger.warning("_complete_batch_run: batch_id %s not found.", batch_id)
        return

    row.status = "completed"
    row.completed_at = datetime.datetime.now(datetime.UTC)
    db.commit()

    logger.info("Batch %s marked as completed at %s", batch_id, row.completed_at)


def _get_batch_run(db: Any, batch_id: str) -> Optional[Any]:
    """Fetch a batch_runs row by batch_id.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string.

    Returns:
        BatchRun ORM row, or None if not found.
    """
    from app.models.batch import BatchRun

    return db.query(BatchRun).filter_by(batch_id=batch_id).first()


def _check_running_batch(db: Any) -> Optional[Any]:
    """Return the first running BatchRun if one exists, else None.

    Used for conflict detection: prevents starting a second batch while one is running.

    Args:
        db: SQLAlchemy Session.

    Returns:
        BatchRun row with status='running', or None.
    """
    from app.models.batch import BatchRun

    return db.query(BatchRun).filter_by(status="running").first()


# ---------------------------------------------------------------------------
# Internal: customer_status CRUD
# ---------------------------------------------------------------------------

def _init_customer_statuses(db: Any, batch_id: str, customers: list) -> None:
    """Insert one customer_status row per customer with status='pending'.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string for this batch run.
        customers: List of MasterData rows.
    """
    from app.models.batch import CustomerStatus

    for customer in customers:
        row = CustomerStatus(
            batch_id=batch_id,
            customer_id=customer.customer_id,
            status="pending",
            candidates_found=0,
            leaks_confirmed=0,
            strategies_matched=json.dumps([]),
            error_message=None,
            processed_at=None,
        )
        db.add(row)

    db.commit()
    logger.info(
        "Initialized %d customer_status rows as 'pending' for batch %s.",
        len(customers),
        batch_id,
    )


def _get_customer_status(db: Any, batch_id: str, customer_id: int) -> Optional[Any]:
    """Fetch the customer_status row for a specific (batch_id, customer_id) pair.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string.
        customer_id: Integer customer_id.

    Returns:
        CustomerStatus row, or None if not found.
    """
    from app.models.batch import CustomerStatus

    return db.query(CustomerStatus).filter_by(
        batch_id=batch_id,
        customer_id=customer_id,
    ).first()


def _update_customer_status(
    db: Any,
    batch_id: str,
    customer_id: int,
    status: str,
    candidates_found: Optional[int] = None,
    leaks_confirmed: Optional[int] = None,
    strategies_matched: Optional[list] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update a customer_status row with new status and optional counts.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string for the batch.
        customer_id: Integer customer_id.
        status: New status value (searching/detecting/complete/failed).
        candidates_found: Number of unique candidate files (set on complete).
        leaks_confirmed: Number of files with PII detected (set on complete).
        strategies_matched: List of strategy names that returned results.
        error_message: Error description (set on failed).
    """
    from app.models.batch import CustomerStatus

    row = db.query(CustomerStatus).filter_by(
        batch_id=batch_id,
        customer_id=customer_id,
    ).first()

    if row is None:
        logger.warning(
            "_update_customer_status: no row for batch=%s, customer=%d",
            batch_id,
            customer_id,
        )
        return

    row.status = status

    if candidates_found is not None:
        row.candidates_found = candidates_found
    if leaks_confirmed is not None:
        row.leaks_confirmed = leaks_confirmed
    if strategies_matched is not None:
        row.strategies_matched = json.dumps(strategies_matched)
    if error_message is not None:
        row.error_message = error_message

    # Set processed_at on terminal states
    if status in ("complete", "failed"):
        row.processed_at = datetime.datetime.now(datetime.UTC)

    db.commit()


# ---------------------------------------------------------------------------
# Internal: customer queries
# ---------------------------------------------------------------------------

def _get_all_customers(db: Any) -> list:
    """Fetch all customers from [PII].[master_data] ordered by customer_id.

    Args:
        db: SQLAlchemy Session.

    Returns:
        List of MasterData rows sorted by customer_id ascending.
    """
    from app.models.master_data import MasterData

    return db.query(MasterData).order_by(MasterData.customer_id).all()


# ---------------------------------------------------------------------------
# Internal: per-customer processing
# ---------------------------------------------------------------------------

def _process_all_customers(
    db: Any,
    search_client: Any,
    customers: list,
    strategies: list,
    batch_id: str,
) -> None:
    """Process all customers sequentially in customer_id order.

    Skips customers already 'complete'. Retries customers that are 'failed'.
    On per-customer error, marks the customer failed and continues to the next.

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        customers: List of MasterData rows (should be pre-sorted by customer_id).
        strategies: List of Strategy objects.
        batch_id: UUID string for this batch run.
    """
    total = len(customers)
    for idx, customer in enumerate(customers, start=1):
        cid = customer.customer_id

        # Check resumability
        cs = _get_customer_status(db, batch_id=batch_id, customer_id=cid)
        if cs is not None and cs.status == "complete":
            logger.info(
                "Customer %d/%d (id=%d): already complete — skipping.",
                idx, total, cid,
            )
            continue

        logger.info("Processing customer %d/%d (id=%d).", idx, total, cid)

        try:
            _process_customer(
                db=db,
                search_client=search_client,
                customer=customer,
                strategies=strategies,
                batch_id=batch_id,
            )
        except Exception as exc:
            # Catch unexpected errors from _process_customer that weren't handled inside
            logger.error(
                "Unexpected error for customer %d: %s",
                cid,
                str(exc),
                exc_info=True,
            )
            _update_customer_status(
                db, batch_id=batch_id, customer_id=cid,
                status="failed",
                error_message=str(exc),
            )

        # Emit spec-format customer progress log after each customer completes
        cs_after = _get_customer_status(db, batch_id=batch_id, customer_id=cid)
        if cs_after is not None:
            import json as _json
            raw_strategies = getattr(cs_after, "strategies_matched", None)
            try:
                strategies_matched = _json.loads(raw_strategies or "[]")
            except (ValueError, TypeError):
                strategies_matched = raw_strategies if isinstance(raw_strategies, list) else []
            _log_customer_progress(
                customer_idx=idx,
                total_customers=total,
                candidates_found=getattr(cs_after, "candidates_found", None) or 0,
                leaks_confirmed=getattr(cs_after, "leaks_confirmed", None) or 0,
                strategies_matched=strategies_matched,
            )


def _process_customer(
    db: Any,
    search_client: Any,
    customer: Any,
    strategies: list,
    batch_id: str,
) -> None:
    """Process a single customer: search -> detect -> persist -> update status.

    Status transitions:
        pending -> searching -> detecting -> complete
        (on error: -> failed)

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        customer: MasterData row with 13 PII fields.
        strategies: List of Strategy objects.
        batch_id: UUID string for this batch run.
    """
    cid = customer.customer_id
    _current_phase = "searching"

    # Step 1: Mark as 'searching'
    _update_customer_status(db, batch_id=batch_id, customer_id=cid, status="searching")

    try:
        # Step 2: Run all strategies, union candidate files
        candidates = search_customer(search_client, customer, strategies)

        logger.info(
            "Customer %d: %d candidate files found via search.",
            cid,
            len(candidates),
        )

        if not candidates:
            # No candidates — mark complete immediately
            _update_customer_status(
                db, batch_id=batch_id, customer_id=cid,
                status="complete",
                candidates_found=0,
                leaks_confirmed=0,
                strategies_matched=[],
            )
            logger.info("Customer %d: no candidates, marked complete.", cid)
            return

        # Step 3: Mark as 'detecting'
        _current_phase = "detecting"
        _update_customer_status(db, batch_id=batch_id, customer_id=cid, status="detecting")

        # Step 4: Normalize search scores across all candidates for this customer
        raw_scores = [c["azure_search_score"] for c in candidates]
        norm_scores = normalize_search_scores(raw_scores)

        # Step 5: Run leak detection on each candidate file
        leaks_confirmed = 0
        strategies_that_matched: set[str] = set()

        for i, candidate in enumerate(candidates):
            md5 = candidate["md5"]
            file_path = candidate["file_path"]
            strategy_name = candidate["strategy_that_found_it"]

            # Read file text from disk (authoritative source)
            file_text = extract_text(file_path)
            if file_text is None:
                logger.warning(
                    "Customer %d: could not read file %s (md5=%s) — skipping detection.",
                    cid, file_path, md5,
                )
                continue

            # Run three-tier leak detection on all 13 PII fields
            leak_result = detect_leaks(file_text, customer)

            # Compute overall confidence using weighted scenario formula
            confidence_result = _compute_overall_confidence(
                leak_result, customer, norm_scores[i],
            )
            overall_confidence = confidence_result["score"]
            # Combine needs_review: True if confidence scenario flags it OR leak detection does
            needs_review = confidence_result["needs_review"] or leak_result.needs_review

            # Check if any PII field was found
            any_found = any(
                getattr(leak_result, field).found
                for field in ALL_PII_FIELDS
            )

            if any_found:
                leaks_confirmed += 1
                strategies_that_matched.add(strategy_name)

                # Persist result row
                _persist_result(
                    db=db,
                    batch_id=batch_id,
                    customer_id=cid,
                    candidate=candidate,
                    leak_result=leak_result,
                    overall_confidence=overall_confidence,
                    needs_review=needs_review,
                    customer=customer,
                )

        # Step 5: Mark as 'complete'
        _update_customer_status(
            db, batch_id=batch_id, customer_id=cid,
            status="complete",
            candidates_found=len(candidates),
            leaks_confirmed=leaks_confirmed,
            strategies_matched=sorted(strategies_that_matched),
        )

        logger.info(
            "Customer %d: %d candidates, %d leaks confirmed (%s)",
            cid,
            len(candidates),
            leaks_confirmed,
            ", ".join(sorted(strategies_that_matched)) if strategies_that_matched else "none",
        )

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            "Customer %d: failed during %s — %s",
            cid,
            _current_phase,
            error_msg,
            exc_info=True,
        )
        _update_customer_status(
            db, batch_id=batch_id, customer_id=cid,
            status="failed",
            error_message=error_msg,
        )


# ---------------------------------------------------------------------------
# Internal: result persistence
# ---------------------------------------------------------------------------

def _persist_result(
    db: Any,
    batch_id: str,
    customer_id: int,
    candidate: dict,
    leak_result: Any,
    overall_confidence: float,
    needs_review: bool = False,
    customer: Any = None,
) -> None:
    """Insert a row into [Search].[results] for a (customer, file) pair with detected PII.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string for this batch run.
        customer_id: Integer customer_id.
        candidate: Dict with md5, file_path, azure_search_score, strategy_that_found_it.
        leak_result: LeakDetectionResult with per-field FieldMatchResult instances.
        overall_confidence: Float confidence score for this pair.
        needs_review: Whether this result needs human review (no-anchor or disambiguation).
        customer: MasterData row — used to populate leaked_* columns with
            the customer's actual PII value when found.
    """
    from app.models.result import Result

    # Build leaked_fields list (fields where found=True)
    leaked_fields = [
        field for field in ALL_PII_FIELDS
        if getattr(leak_result, field).found
    ]

    # Build match_details dict (all fields, for audit trail)
    match_details = {}
    for field in ALL_PII_FIELDS:
        field_result = getattr(leak_result, field)
        match_details[field] = {
            "found": field_result.found,
            "method": field_result.method,
            "confidence": field_result.confidence,
            "snippet": field_result.snippet,
        }

    # Build per-field leaked_* columns: use customer's master_data PII value when found
    from app.models.result import PII_FIELD_TO_LEAKED_COLUMN
    leaked_columns = {}
    for field in ALL_PII_FIELDS:
        col_name = PII_FIELD_TO_LEAKED_COLUMN.get(field)
        if col_name:
            field_result = getattr(leak_result, field)
            if field_result.found and customer is not None:
                val = getattr(customer, field, None)
                leaked_columns[col_name] = str(val) if val is not None else None
            else:
                leaked_columns[col_name] = None

    row = Result(
        batch_id=batch_id,
        customer_id=customer_id,
        md5=candidate["md5"],
        strategy_name=candidate["strategy_that_found_it"],
        leaked_fields=json.dumps(leaked_fields),
        match_details=json.dumps(match_details),
        overall_confidence=overall_confidence,
        azure_search_score=candidate["azure_search_score"],
        needs_review=needs_review,
        searched_at=datetime.datetime.now(datetime.UTC),
        **leaked_columns,
    )
    db.add(row)
    db.commit()


# ---------------------------------------------------------------------------
# Internal: confidence computation
# ---------------------------------------------------------------------------

def _compute_overall_confidence(
    leak_result: Any,
    customer: Any,
    search_score_norm: float,
) -> dict:
    """Compute overall confidence using the spec's weighted scenario formulas.

    Delegates to app.utils.confidence.compute_overall_confidence which selects
    the formula based on which anchors (SSN, Name) matched:
    - SSN+Name: 0.40*SSN + 0.30*Name + 0.15*OtherAvg + 0.15*SearchScore
    - SSN only: 0.60*SSN + 0.15*OtherAvg + 0.25*SearchScore
    - Name only: 0.50*Name + 0.20*OtherAvg + 0.30*SearchScore
    - No anchor: 0.50*OtherAvg + 0.50*SearchScore + needs_review=True

    Args:
        leak_result: LeakDetectionResult with per-field FieldMatchResult.
        customer: MasterData row (to determine which fields are non-null).
        search_score_norm: Normalized Azure AI Search score (0.0-1.0).

    Returns:
        Dict with keys: score, scenario, needs_review, other_fields_avg.
    """
    # SSN confidence
    ssn_conf = leak_result.SSN.confidence if leak_result.SSN.found else 0.0

    # Name confidence: max across Fullname, FirstName, LastName
    name_confs = []
    for field in ("Fullname", "FirstName", "LastName"):
        fr = getattr(leak_result, field)
        if fr.found:
            name_confs.append(fr.confidence)
    name_conf = max(name_confs) if name_confs else 0.0

    # Other field confidences: non-anchor fields that are non-null in customer
    # Per spec: only include fields that are evaluable (non-null in master_data)
    # Unmatched but evaluable fields contribute 0.0; null fields are excluded
    anchor_fields = {"SSN", "Fullname", "FirstName", "LastName"}
    other_field_confs = []
    for field in ALL_PII_FIELDS:
        if field in anchor_fields:
            continue
        customer_value = getattr(customer, field, None)
        if customer_value is not None and (
            not isinstance(customer_value, str) or customer_value.strip()
        ):
            fr = getattr(leak_result, field)
            other_field_confs.append(fr.confidence if fr.found else 0.0)

    return _weighted_confidence(
        ssn_conf=ssn_conf,
        name_conf=name_conf,
        other_field_confs=other_field_confs,
        search_score_norm=search_score_norm,
    )
