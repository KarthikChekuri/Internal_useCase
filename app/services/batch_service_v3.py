"""V3 Batch Service — Phase V3-3.2 (strategy-driven).

Provides the V3 batch processing pipeline that:
1. Loads search strategies from strategies.yaml (same as V2)
2. Creates a batch run (batch_runs row) with strategy_set listing strategy names
3. Initializes per-customer status rows (customer_status) as 'pending'
4. Processes each customer in order:
   a. Mark status 'searching'
   b. For each strategy: call search_customer_strategy_v3 with the strategy's
      fields (per-field Lucene queries + AND filter — Azure-only, no Python)
   c. Persist result rows into [Search].[results] for each matched document,
      with strategy_name = the strategy that found it
   d. Mark status 'complete' with candidates_found / leaks_confirmed counts
5. On per-customer error: mark customer 'failed', continue to next customer
6. On completion: update batch_runs to 'completed' with completed_at timestamp

Public API:
    start_batch_v3(db, search_client) -> batch_id (str)

Internal helpers:
    _process_customer_v3(db, search_client, customer, batch_id, strategies, customer_idx, total_customers)
    _persist_v3_result(db, batch_id, customer_id, result_dict, strategy_name)
"""

import json
import logging
import os
import uuid
from datetime import datetime, UTC
from typing import Any

logger = logging.getLogger(__name__)

# Deferred imports: placed inside function bodies to avoid sqlalchemy hang at import time.
# Tests patch these names at the module level via patch("app.services.batch_service_v3.<Name>").

# The actual ORM class references that tests can patch:
from app.services.search_service_v3 import search_customer_strategy_v3, enrich_matched_documents, compute_confidence_v3  # noqa: E402
from app.services.search_service import load_strategies  # noqa: E402


def _get_batch_run_class():
    from app.models.batch import BatchRun
    return BatchRun


def _get_customer_status_class():
    from app.models.batch import CustomerStatus
    return CustomerStatus


def _get_search_result_class():
    from app.models.result import Result
    return Result


def _get_master_data_class():
    from app.models.master_data import MasterData
    return MasterData


# Module-level aliases that tests can patch:
# These let `patch("app.services.batch_service_v3.BatchRun", ...)` work correctly
# without importing sqlalchemy at module load time.

try:
    from app.models.batch import BatchRun, CustomerStatus
    from app.models.result import Result as SearchResult
    from app.models.master_data import MasterData
except Exception:
    # In test environments, imports may be patched before this runs.
    # We define stubs so the names exist at module level.
    BatchRun = None  # type: ignore[assignment]
    CustomerStatus = None  # type: ignore[assignment]
    SearchResult = None  # type: ignore[assignment]
    MasterData = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Public API: start_batch_v3
# ---------------------------------------------------------------------------


def start_batch_v3(db: Any, search_client: Any, batch_id: str | None = None) -> str:
    """Start a new V3 batch run, processing all customers sequentially.

    V3 uses Azure AI Search exclusively — no Python regex or fuzzy detection.
    Strategies are loaded from strategies.yaml (same as V2). For each customer
    and each strategy, per-field Lucene queries are sent with AND filtering.

    Raises:
        ValueError: If a batch is already running.

    Args:
        db: SQLAlchemy Session (mocked in unit tests).
        search_client: Azure SearchClient instance (mocked in unit tests).
        batch_id: Optional pre-generated UUID. If None, a new one is created.

    Returns:
        batch_id (str): UUID of the newly created batch run.
    """
    # Check for concurrent batch conflict
    running = db.query(BatchRun).filter_by(status="running").first()
    if running is not None:
        raise ValueError(
            f"A batch is already running (batch_id: {running.batch_id})"
        )

    # Load strategies from YAML (same file V2 uses)
    strategies_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "strategies.yaml",
    )
    strategies = load_strategies(strategies_path)
    strategy_names = [s.name for s in strategies]

    # Load all customers ordered by customer_id
    customers = db.query(MasterData).order_by(MasterData.customer_id).all()
    total_customers = len(customers)

    # Create the batch run row
    if batch_id is None:
        batch_id = str(uuid.uuid4())
    batch_run = BatchRun(
        batch_id=batch_id,
        status="running",
        strategy_set=json.dumps(strategy_names),
        started_at=datetime.now(UTC),
        total_customers=total_customers,
        total_files=0,
    )
    db.add(batch_run)
    db.commit()

    logger.info(
        "[V3] Batch %s started: %d customers, strategies=%s",
        batch_id, total_customers, strategy_names,
    )

    # Initialize all customer_status rows as 'pending'
    for customer in customers:
        cs = CustomerStatus(
            batch_id=batch_id,
            customer_id=customer.customer_id,
            status="pending",
        )
        db.add(cs)
    db.commit()

    # Process all customers sequentially
    for idx, customer in enumerate(customers, start=1):
        try:
            _process_customer_v3(
                db=db,
                search_client=search_client,
                customer=customer,
                batch_id=batch_id,
                strategies=strategies,
                customer_idx=idx,
                total_customers=total_customers,
            )
        except Exception as exc:
            # Outer catch — _process_customer_v3 handles its own errors,
            # but this ensures any unexpected propagation is contained.
            logger.error(
                "[V3] Unexpected error for customer id=%d: %s",
                customer.customer_id,
                str(exc),
                exc_info=True,
            )

    # Mark batch completed
    batch_run.status = "completed"
    batch_run.completed_at = datetime.now(UTC)
    db.commit()

    logger.info(
        "[V3] Batch %s complete: %d customers processed",
        batch_id,
        total_customers,
    )

    return batch_id


# ---------------------------------------------------------------------------
# Internal: per-customer processing
# ---------------------------------------------------------------------------


def _process_customer_v3(
    db: Any,
    search_client: Any,
    customer: Any,
    batch_id: str,
    strategies: list,
    customer_idx: int,
    total_customers: int,
) -> None:
    """Process a single customer in the V3 pipeline.

    For each strategy, sends per-field Lucene queries for the strategy's
    fields and applies AND filtering.  Results are persisted with the
    strategy name (e.g. "fullname_ssn"), not a constant.

    Status transitions:
        pending -> searching -> complete
        (on error: -> failed)

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        customer: MasterData row (or SimpleNamespace in tests).
        batch_id: UUID string for this batch run.
        strategies: List of Strategy objects from strategies.yaml.
        customer_idx: 1-based index of this customer in the batch.
        total_customers: Total number of customers in the batch.
    """
    cid = customer.customer_id

    # Fetch the customer_status row to update in place
    cs_row = db.query(CustomerStatus).filter_by(
        batch_id=batch_id,
        customer_id=cid,
    ).first()

    try:
        # Mark as 'searching'
        if cs_row is not None:
            cs_row.status = "searching"
        db.commit()

        # Run each strategy and collect results
        all_results: list[tuple[str, dict]] = []  # (strategy_name, result_dict)
        seen_md5s: dict[str, str] = {}  # md5 -> first strategy that found it

        for strategy in strategies:
            results = search_customer_strategy_v3(
                search_client, customer, strategy.fields,
            )
            for result_dict in results:
                md5 = result_dict["md5"]
                if md5 not in seen_md5s:
                    seen_md5s[md5] = strategy.name
                all_results.append((strategy.name, result_dict))

        n_unique_docs = len(seen_md5s)
        n_total_rows = len(all_results)

        # Post-search enrichment: query ALL 13 PII fields for matched docs
        enrichment = enrich_matched_documents(
            search_client, customer, set(seen_md5s.keys()),
        )

        # Compute a global max_score across all enrichment results
        all_enrich_scores = [
            entry["score"]
            for doc_fields in enrichment.values()
            for entry in doc_fields.values()
            if entry.get("found") and entry.get("score") is not None
        ]
        global_max_score = max(all_enrich_scores) if all_enrich_scores else 1.0

        # Merge enrichment into each result; recompute confidence only if new fields added
        for _strategy_name, result_dict in all_results:
            md5 = result_dict["md5"]
            added_new = False
            if md5 in enrichment:
                for field_name, field_data in enrichment[md5].items():
                    if field_name not in result_dict["fields"]:
                        result_dict["fields"][field_name] = field_data
                        added_new = True
            if added_new:
                conf, review = compute_confidence_v3(
                    result_dict["fields"], global_max_score,
                )
                result_dict["confidence"] = conf
                result_dict["needs_review"] = review

        logger.info(
            "[V3] Customer %d/%d (id=%d): %d strategies, %d unique docs, %d strategy hits → %d deduped rows",
            customer_idx,
            total_customers,
            cid,
            len(strategies),
            n_unique_docs,
            n_total_rows,
            n_unique_docs,
        )

        # Deduplicate: one row per MD5, merge fields & collect strategies
        deduped: dict[str, dict] = {}  # md5 -> merged result_dict
        md5_strategies: dict[str, list[str]] = {}  # md5 -> list of strategy names
        for strategy_name, result_dict in all_results:
            md5 = result_dict["md5"]
            if md5 not in deduped:
                deduped[md5] = result_dict
                md5_strategies[md5] = [strategy_name]
            else:
                # Merge fields from this strategy into existing entry
                for fname, fdata in result_dict["fields"].items():
                    if fname not in deduped[md5]["fields"]:
                        deduped[md5]["fields"][fname] = fdata
                if strategy_name not in md5_strategies[md5]:
                    md5_strategies[md5].append(strategy_name)
                # Keep higher confidence
                if result_dict["confidence"] > deduped[md5]["confidence"]:
                    deduped[md5]["confidence"] = result_dict["confidence"]
                    deduped[md5]["needs_review"] = result_dict["needs_review"]

        # Persist one row per unique MD5
        for md5, result_dict in deduped.items():
            _persist_v3_result(
                db, batch_id=batch_id, customer_id=cid,
                result_dict=result_dict,
                strategy_name=json.dumps(md5_strategies[md5]),
                customer=customer,
            )

        # Update customer status to complete
        if cs_row is not None:
            cs_row.status = "complete"
            cs_row.candidates_found = n_unique_docs
            cs_row.leaks_confirmed = n_unique_docs  # all passed AND filter
        db.commit()

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            "[V3] Customer id=%d failed: %s",
            cid,
            error_msg,
            exc_info=True,
        )
        if cs_row is not None:
            cs_row.status = "failed"
            cs_row.error_message = error_msg
        db.commit()


# ---------------------------------------------------------------------------
# Internal: result persistence
# ---------------------------------------------------------------------------


def _persist_v3_result(
    db: Any,
    batch_id: str,
    customer_id: int,
    result_dict: dict,
    strategy_name: str,
    customer: Any = None,
) -> None:
    """Insert a row into [Search].[results] for a single V3 matched document.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string for this batch run.
        customer_id: Integer customer_id.
        result_dict: Dict as returned by search_customer_strategy_v3:
            {
                "md5": str,
                "fields": dict[str, {"found": bool, "score": float, "snippet": str|None}],
                "confidence": float,
                "needs_review": bool,
            }
        strategy_name: JSON list of strategy names that found this document.
        customer: MasterData row — used to populate leaked_* columns with
            the customer's actual PII value (from master_data) when found.
    """
    fields = result_dict["fields"]

    # leaked_fields: only field names where found=True
    leaked = [fname for fname, fdata in fields.items() if fdata.get("found")]

    # match_details: all fields as-is (for audit trail)
    match_details = fields

    # azure_search_score: max score among found fields
    max_score = max(
        (f["score"] for f in fields.values() if f.get("found") and f.get("score") is not None),
        default=0.0,
    )

    # Build per-field leaked_* columns: use customer's master_data PII value when found
    from app.models.result import PII_FIELD_TO_LEAKED_COLUMN
    leaked_columns = {}
    for field_name in _PII_FIELDS:
        col_name = PII_FIELD_TO_LEAKED_COLUMN.get(field_name)
        if col_name:
            fdata = fields.get(field_name, {})
            if fdata.get("found") and customer is not None:
                val = getattr(customer, field_name, None)
                leaked_columns[col_name] = str(val) if val is not None else None
            else:
                leaked_columns[col_name] = None

    row = SearchResult(
        batch_id=batch_id,
        customer_id=customer_id,
        md5=result_dict["md5"],
        strategy_name=strategy_name,
        leaked_fields=json.dumps(leaked),
        match_details=json.dumps(match_details),
        overall_confidence=result_dict["confidence"],
        azure_search_score=max_score,
        needs_review=result_dict["needs_review"],
        searched_at=datetime.now(UTC),
        **leaked_columns,
    )
    db.add(row)
    db.commit()


# ---------------------------------------------------------------------------
# Internal: helper to count non-null PII fields for logging
# ---------------------------------------------------------------------------

_PII_FIELDS = [
    "Fullname", "FirstName", "LastName",
    "DOB", "SSN", "DriversLicense",
    "Address1", "Address2", "Address3",
    "ZipCode", "City", "State", "Country",
]


import re as _re

def _extract_matched_value(snippet: str | None) -> str | None:
    """Extract clean matched text from an Azure AI Search highlight snippet.

    Azure returns full document text with [[MATCH]]...[[/MATCH]] tags around
    matched tokens. This extracts just the line containing the match and
    strips the tags, returning a clean value like '523-45-7891' or 'Robert O\\'Brien'.
    """
    if not snippet:
        return None
    # Find lines containing match tags
    for line in snippet.split("\n"):
        if "[[MATCH]]" in line:
            clean = line.replace("[[MATCH]]", "").replace("[[/MATCH]]", "").strip()
            # Remove leading label (e.g., "Full Name: " -> value)
            if ": " in clean:
                clean = clean.split(": ", 1)[1].strip()
            return clean
    return snippet[:200] if snippet else None


def _count_non_null_fields(customer: Any) -> int:
    """Count how many PII fields are non-null/non-empty on this customer."""
    count = 0
    for field in _PII_FIELDS:
        val = getattr(customer, field, None)
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        count += 1
    return count
