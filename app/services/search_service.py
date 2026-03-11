"""Search Orchestration Service — Phase 4.1.

Provides `search_customer_pii(db, search_client, ssn, fullname, config) -> SearchResponse`
that orchestrates the full PII search flow:

  1. Customer lookup by SSN in master_pii
  2. Optional fullname validation
  3. Lucene query construction (fuzzy name tokens + SSN variants)
  4. Azure AI Search execution
  5. Per-file: DLU lookup -> text extraction -> leak detection -> confidence scoring
  6. Search score normalization
  7. DB persistence of results
  8. Return structured, ordered response
"""

import json
import logging
import os
import re
import uuid
from typing import Any, Optional

from app.models.dlu import DLU
from app.models.master_pii import MasterPII
from app.models.search_result import SearchResult
from app.schemas.pii import CustomerSummary, FieldMatchResult
from app.schemas.search import FileResult, SearchResponse
from app.services.leak_detection_service import (
    ALL_FIELDS,
    LeakDetectionResult,
    detect_leaks,
)
from app.services.text_extraction import extract_text
from app.utils.confidence import (
    compute_overall_confidence,
    compute_per_field_confidence,
    normalize_search_scores,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class CustomerNotFoundError(Exception):
    """Raised when the SSN does not match any customer in master_pii."""
    pass


class DataIntegrityError(Exception):
    """Raised when multiple customers share the same SSN."""
    pass


class FullnameMismatchError(Exception):
    """Raised when provided fullname doesn't match the DB record."""
    pass


# ---------------------------------------------------------------------------
# Step 1: Customer lookup
# ---------------------------------------------------------------------------

def _normalize_ssn(ssn: str) -> tuple[str, str]:
    """Normalize SSN to both dashed and undashed formats.

    Args:
        ssn: SSN in XXX-XX-XXXX or XXXXXXXXX format.

    Returns:
        Tuple of (dashed, undashed) SSN strings.
    """
    digits = re.sub(r"[^0-9]", "", ssn)
    dashed = f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}" if len(digits) == 9 else ssn
    return dashed, digits


def _lookup_customer(db: Any, ssn: str) -> Any:
    """Look up a customer in master_pii by SSN.

    Normalizes SSN format before lookup so both dashed (XXX-XX-XXXX)
    and undashed (XXXXXXXXX) formats match regardless of DB storage format.

    Args:
        db: SQLAlchemy session.
        ssn: SSN in XXX-XX-XXXX or XXXXXXXXX format.

    Returns:
        The MasterPII record for the customer.

    Raises:
        CustomerNotFoundError: No customer found with the given SSN.
        DataIntegrityError: Multiple customers found with the same SSN.
    """
    dashed, undashed = _normalize_ssn(ssn)
    results = db.query(MasterPII).filter(
        MasterPII.SSN.in_([dashed, undashed])
    ).all()

    if len(results) == 0:
        raise CustomerNotFoundError("Customer not found")

    if len(results) > 1:
        raise DataIntegrityError(
            "Multiple customers found with this SSN -- data integrity error"
        )

    return results[0]


# ---------------------------------------------------------------------------
# Step 2: Fullname validation
# ---------------------------------------------------------------------------

def _validate_fullname(customer: Any, fullname: Optional[str]) -> None:
    """Validate the provided fullname against the DB record.

    If fullname is None or empty, validation is skipped.
    Comparison is case-insensitive.

    Args:
        customer: MasterPII record with Fullname attribute.
        fullname: The fullname from the search request, or None.

    Raises:
        FullnameMismatchError: Provided fullname doesn't match customer record.
    """
    if not fullname:
        return

    if customer.Fullname and fullname.strip().lower() != customer.Fullname.strip().lower():
        raise FullnameMismatchError(
            "Provided fullname does not match customer record"
        )


# ---------------------------------------------------------------------------
# Step 3: Lucene query construction
# ---------------------------------------------------------------------------

# Lucene special characters that need escaping
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}[\]^"~*?:\\/])')


def _escape_lucene(token: str) -> str:
    """Escape Lucene special characters in a token.

    Removes apostrophes entirely (they cause issues in Lucene) and
    escapes other special characters with a backslash.
    """
    # Remove apostrophes
    token = token.replace("'", "")
    # Escape remaining special characters
    token = _LUCENE_SPECIAL.sub(r"\\\1", token)
    return token


def _tokenize_for_lucene(fullname: str) -> list[str]:
    """Tokenize a name for Lucene query construction.

    Handles apostrophes, hyphens, and periods by:
    - Replacing hyphens with spaces (Anne-Marie -> Anne Marie)
    - Removing apostrophes (O'Brien -> OBrien)
    - Removing periods (J.R. -> JR)
    - Splitting on whitespace

    Args:
        fullname: The customer's full name from the database.

    Returns:
        List of cleaned name tokens.
    """
    if not fullname:
        return []

    # Replace hyphens with spaces
    name = fullname.replace("-", " ")
    # Remove apostrophes
    name = name.replace("'", "")
    # Remove periods
    name = name.replace(".", "")
    # Split on whitespace and filter empty tokens
    tokens = [t.strip() for t in name.split() if t.strip()]
    return tokens


def _build_lucene_query(fullname: str, ssn: str) -> str:
    """Build a full Lucene query from customer PII.

    Combines fuzzy name tokens and SSN variants:
    - Each name token gets ~1 fuzzy operator: (Karthik~1 Chekuri~1)
    - SSN in both dashed and undashed format: ("343-43-4343" | "343434343")

    Args:
        fullname: Customer's full name from DB.
        ssn: Customer's SSN.

    Returns:
        Full Lucene query string.
    """
    parts = []

    # Name tokens with fuzzy operators
    tokens = _tokenize_for_lucene(fullname)
    if tokens:
        fuzzy_tokens = " ".join(f"{_escape_lucene(t)}~1" for t in tokens)
        parts.append(f"({fuzzy_tokens})")

    # SSN variants
    digits = re.sub(r"[^0-9]", "", ssn)
    if len(digits) == 9:
        dashed = f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
        parts.append(f'("{dashed}" | "{digits}")')

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Step 4: Azure AI Search execution
# ---------------------------------------------------------------------------

def _execute_search(search_client: Any, query: str) -> list[dict]:
    """Execute a Lucene query against Azure AI Search.

    Uses queryType="full", searchMode="any", searches across content,
    content_phonetic, and content_lowercase fields with pii_boost
    scoring profile.

    Args:
        search_client: Azure SearchClient instance.
        query: Full Lucene query string.

    Returns:
        List of result dicts with file_guid, file_name, and search score.
    """
    results = search_client.search(
        search_text=query,
        query_type="full",
        search_mode="any",
        search_fields=["content", "content_phonetic", "content_lowercase"],
        scoring_profile="pii_boost",
        top=100,
    )

    parsed = []
    for result in results:
        parsed.append({
            "file_guid": result["file_guid"],
            "file_name": result["file_name"],
            "search_score": result["@search.score"],
        })

    return parsed


# ---------------------------------------------------------------------------
# Step 5: DLU record lookup + file path resolution
# ---------------------------------------------------------------------------

def _lookup_dlu_record(db: Any, file_guid: str) -> Any:
    """Look up a DLU record by file GUID.

    Args:
        db: SQLAlchemy session.
        file_guid: The file GUID from the search result.

    Returns:
        DLU record or None if not found.
    """
    return db.query(DLU).filter(DLU.GUID == file_guid).first()


def _resolve_file_path(base_path: str, textpath: Optional[str]) -> Optional[str]:
    """Combine the configured base path with the DLU TEXTPATH column.

    Args:
        base_path: Value of FILE_BASE_PATH from config.
        textpath: TEXTPATH column value from DLU record, or None.

    Returns:
        Full file system path as a string, or None if textpath is None/empty.
    """
    if not textpath:
        return None
    return os.path.join(base_path, textpath.lstrip(os.sep).lstrip("/").lstrip("\\"))


# ---------------------------------------------------------------------------
# Step 6 & 7: Process a single file (leak detection + confidence scoring)
# ---------------------------------------------------------------------------

def _get_leaked_fields(leak_result: LeakDetectionResult) -> list[str]:
    """Extract list of field names where leaks were detected.

    Args:
        leak_result: LeakDetectionResult from detect_leaks.

    Returns:
        List of PII field names that have found=True.
    """
    leaked = []
    for field_name in ALL_FIELDS:
        field_match: FieldMatchResult = getattr(leak_result, field_name)
        if field_match.found:
            leaked.append(field_name)
    return leaked


def _build_match_details(leak_result: LeakDetectionResult) -> dict[str, FieldMatchResult]:
    """Build match_details dict from leak detection result.

    Args:
        leak_result: LeakDetectionResult with per-field results.

    Returns:
        Dict mapping field name to FieldMatchResult for all found fields.
    """
    details = {}
    for field_name in ALL_FIELDS:
        field_match: FieldMatchResult = getattr(leak_result, field_name)
        if field_match.found:
            details[field_name] = field_match
    return details


def _compute_file_confidence(
    leak_result: LeakDetectionResult,
    customer: Any,
    search_score_norm: float,
) -> dict:
    """Compute overall confidence for a single file.

    Extracts SSN confidence, name confidence (max of Fullname, FirstName,
    LastName), and other field confidences, then delegates to
    compute_overall_confidence.

    Args:
        leak_result: LeakDetectionResult for this file.
        customer: MasterPII record with the customer's PII.
        search_score_norm: Normalized search score for this file.

    Returns:
        Dict with score, scenario, needs_review, other_fields_avg.
    """
    ssn_conf = leak_result.SSN.confidence if leak_result.SSN.found else 0.0

    # Name confidence = max of Fullname, FirstName, LastName
    name_confs = []
    for name_field in ("Fullname", "FirstName", "LastName"):
        field_result: FieldMatchResult = getattr(leak_result, name_field)
        if field_result.found:
            name_confs.append(field_result.confidence)
    name_conf = max(name_confs) if name_confs else 0.0

    # Other fields: non-anchor, non-null fields in master_pii
    anchor_fields = {"SSN", "Fullname", "FirstName", "LastName"}
    other_confs = []
    for field_name in ALL_FIELDS:
        if field_name in anchor_fields:
            continue
        pii_value = getattr(customer, field_name, None)
        if pii_value is None or (isinstance(pii_value, str) and pii_value.strip() == ""):
            continue  # Skip null PII fields
        field_result: FieldMatchResult = getattr(leak_result, field_name)
        other_confs.append(field_result.confidence)

    return compute_overall_confidence(ssn_conf, name_conf, other_confs, search_score_norm)


def _process_file(
    db: Any,
    customer: Any,
    search_result: dict,
    search_score_norm: float,
    config: Any,
) -> Optional[dict]:
    """Process a single search result file through leak detection and scoring.

    Args:
        db: SQLAlchemy session.
        customer: MasterPII record.
        search_result: Dict with file_guid, file_name, search_score.
        search_score_norm: Normalized search score for this file.
        config: Settings object with FILE_BASE_PATH.

    Returns:
        Dict with file result data, or None if the file should be skipped.
    """
    file_guid = search_result["file_guid"]
    file_name = search_result["file_name"]
    raw_score = search_result["search_score"]

    # Look up the DLU record to resolve file path
    dlu_record = _lookup_dlu_record(db, file_guid)
    if dlu_record is None:
        logger.warning("DLU record not found for GUID '%s'. Skipping.", file_guid)
        return None

    # Resolve file path and extract text
    file_path = _resolve_file_path(config.FILE_BASE_PATH, dlu_record.TEXTPATH)
    if file_path is None:
        logger.warning("TEXTPATH is null for GUID '%s'. Skipping.", file_guid)
        return None
    file_text = extract_text(file_path)

    if file_text is None:
        logger.warning("Text extraction failed for '%s'. Skipping.", file_path)
        return None

    # Run leak detection
    leak_result = detect_leaks(file_text, customer)

    # Get leaked fields
    leaked_fields = _get_leaked_fields(leak_result)

    # If no fields leaked, skip this file
    if not leaked_fields:
        return None

    # Build match details
    match_details = _build_match_details(leak_result)

    # Compute confidence
    confidence_result = _compute_file_confidence(leak_result, customer, search_score_norm)

    return {
        "file_guid": file_guid,
        "file_name": file_name,
        "leaked_fields": leaked_fields,
        "overall_confidence": confidence_result["score"],
        "azure_search_score": raw_score,
        "needs_review": confidence_result["needs_review"] or leak_result.needs_review,
        "match_details": match_details,
        "leak_detection": leak_result,
    }


# ---------------------------------------------------------------------------
# Step 8: Persistence
# ---------------------------------------------------------------------------

def _persist_results(
    db: Any,
    search_run_id: uuid.UUID,
    customer_id: int,
    file_results: list[dict],
) -> None:
    """Persist search results to [Search].[search_results].

    Each file result becomes one row with leaked field BIT columns,
    LeakedFieldsList JSON, MatchDetails JSON, and OverallConfidence.

    Args:
        db: SQLAlchemy session.
        search_run_id: UUID for this search run.
        customer_id: Foreign key to master_pii.
        file_results: List of file result dicts from _process_file.
    """
    if not file_results:
        return

    for fr in file_results:
        leak = fr["leak_detection"]

        # Build leaked fields list as JSON array
        leaked_fields_json = json.dumps(fr["leaked_fields"])

        # Build match details as JSON
        match_details_dict = {}
        for field_name, field_result in fr["match_details"].items():
            match_details_dict[field_name] = {
                "found": field_result.found,
                "method": field_result.method,
                "confidence": field_result.confidence,
                "snippet": field_result.snippet,
            }
        match_details_json = json.dumps(match_details_dict)

        row = SearchResult(
            SearchRunID=str(search_run_id),
            CustomerID=customer_id,
            FileGUID=fr["file_guid"],
            LeakedFullname=leak.Fullname.found,
            LeakedFirstName=leak.FirstName.found,
            LeakedLastName=leak.LastName.found,
            LeakedDOB=leak.DOB.found,
            LeakedSSN=leak.SSN.found,
            LeakedDriversLicense=leak.DriversLicense.found,
            LeakedAddress1=leak.Address1.found,
            LeakedAddress2=leak.Address2.found,
            LeakedAddress3=leak.Address3.found,
            LeakedZipCode=leak.ZipCode.found,
            LeakedCity=leak.City.found,
            LeakedState=leak.State.found,
            LeakedCountry=leak.Country.found,
            LeakedFieldsList=leaked_fields_json,
            MatchDetails=match_details_json,
            OverallConfidence=fr["overall_confidence"],
            AzureSearchScore=fr["azure_search_score"],
            NeedsReview=fr["needs_review"],
        )
        db.add(row)

    db.commit()
    logger.info(
        "Persisted %d search results for run %s.", len(file_results), search_run_id
    )


# ---------------------------------------------------------------------------
# Step 9: Main entry point
# ---------------------------------------------------------------------------

def search_customer_pii(
    db: Any,
    search_client: Any,
    ssn: str,
    fullname: Optional[str],
    config: Any,
) -> SearchResponse:
    """Execute a full PII search for a customer.

    Orchestrates the complete search flow:
      1. Look up customer by SSN
      2. Validate fullname (if provided)
      3. Build Lucene query
      4. Execute Azure AI Search
      5. For each result: resolve path, extract text, detect leaks, score
      6. Normalize search scores
      7. Persist results to DB
      8. Return structured response ordered by confidence descending

    Args:
        db: SQLAlchemy session.
        search_client: Azure SearchClient instance.
        ssn: Customer SSN in XXX-XX-XXXX or XXXXXXXXX format.
        fullname: Optional fullname for pre-search validation.
        config: Settings object with FILE_BASE_PATH, etc.

    Returns:
        SearchResponse with search_run_id, customer summary, and results.

    Raises:
        CustomerNotFoundError: SSN not found in master_pii (404).
        DataIntegrityError: Multiple SSN matches (409).
        FullnameMismatchError: Provided fullname doesn't match DB (409).
    """
    search_run_id = uuid.uuid4()

    # Step 1: Customer lookup
    customer = _lookup_customer(db, ssn)

    # Step 2: Fullname validation
    _validate_fullname(customer, fullname)

    # Step 3: Build Lucene query (always uses DB fullname)
    lucene_query = _build_lucene_query(customer.Fullname or "", customer.SSN or ssn)

    # Step 4: Execute Azure AI Search
    search_results = _execute_search(search_client, lucene_query)

    if not search_results:
        logger.info("No search results for SSN ending in ...%s", ssn[-4:])
        return SearchResponse(
            search_run_id=search_run_id,
            customer=CustomerSummary(fullname=customer.Fullname or "", ssn=customer.SSN or ssn),
            results=[],
        )

    # Step 6: Normalize search scores across the result set
    raw_scores = [r["search_score"] for r in search_results]
    norm_scores = normalize_search_scores(raw_scores)

    # Step 5 & 7: Process each file
    file_results = []
    for search_result, norm_score in zip(search_results, norm_scores):
        processed = _process_file(db, customer, search_result, norm_score, config)
        if processed is not None:
            file_results.append(processed)

    # Sort by overall_confidence descending
    file_results.sort(key=lambda x: x["overall_confidence"], reverse=True)

    # Step 8: Persist results
    _persist_results(db, search_run_id, customer.ID, file_results)

    # Step 9: Build response
    result_models = []
    for fr in file_results:
        result_models.append(
            FileResult(
                file_name=fr["file_name"],
                file_guid=fr["file_guid"],
                leaked_fields=fr["leaked_fields"],
                overall_confidence=fr["overall_confidence"],
                azure_search_score=fr["azure_search_score"],
                needs_review=fr["needs_review"],
                match_details=fr["match_details"],
            )
        )

    return SearchResponse(
        search_run_id=search_run_id,
        customer=CustomerSummary(fullname=customer.Fullname or "", ssn=customer.SSN or ssn),
        results=result_models,
    )
