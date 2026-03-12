"""Indexing pipeline service — Phase V3-2.1.

V3 extends V2 with Azure AI Language PII Detection:
- Calls PII Detection API per document to identify SSN, names, DOBs, addresses, phones
- Builds V3 documents with PII metadata fields (has_ssn, has_name, has_dob, etc.)
- Falls back gracefully (all has_*=False) when PII API is unavailable

Public API (V3):
    index_all_files_v3(db, search_client, config) -> IndexResponse

Internal helpers:
    _call_pii_detection(text, client=None, endpoint=None, key=None) -> list[dict]
    _map_pii_entities(entities) -> dict
    _build_v3_document(md5, file_path, content, pii_metadata) -> dict
    _query_all_dlu_records_v3(db) -> list
    _upload_documents_v3(search_client, documents) -> list[str]
"""

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from app.models.dlu import DLU
from app.services.text_extraction import extract_text

logger = logging.getLogger(__name__)

# Supported file extensions for indexing (same as V2)
SUPPORTED_EXTENSIONS = {".txt", ".xls", ".xlsx", ".csv"}

# Azure AI Search upload batch size limit
BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Response schema (reuse from V2)
# ---------------------------------------------------------------------------

class IndexResponse(BaseModel):
    """JSON-serializable response from indexing operations."""

    files_processed: int
    files_succeeded: int
    files_failed: int
    files_skipped: int = 0
    errors: list[str]


# ---------------------------------------------------------------------------
# PII entity type mapping rules
# ---------------------------------------------------------------------------
# Each rule: (substring_in_category, field_to_set)
_PII_FIELD_RULES = [
    ("SocialSecurity", "has_ssn"),
    ("Person", "has_name"),
    ("DateTime", "has_dob"),
    ("Date", "has_dob"),
    ("Address", "has_address"),
    ("PhoneNumber", "has_phone"),
]

_DEFAULT_PII_METADATA: dict[str, Any] = {
    "has_ssn": False,
    "has_name": False,
    "has_dob": False,
    "has_address": False,
    "has_phone": False,
    "pii_types": [],
    "pii_entity_count": 0,
}


# ---------------------------------------------------------------------------
# _call_pii_detection
# ---------------------------------------------------------------------------

def _call_pii_detection(
    text: str,
    client: Any = None,
    endpoint: str = None,
    key: str = None,
) -> list[dict]:
    """Call Azure AI Language PII Detection API and return a list of entity dicts.

    Each dict has at minimum: {"category": str, "text": str}.

    If the API call fails for any reason, returns an empty list and logs a
    warning so that indexing can continue with default (all-false) metadata.

    Args:
        text: The document text to analyse.
        client: An injected TextAnalyticsClient (for testing / DI). If None,
                a real client is constructed from endpoint + key.
        endpoint: Azure AI Language endpoint URL (unused when client is injected).
        key: Azure AI Language API key (unused when client is injected).

    Returns:
        List of entity dicts, or [] on error.
    """
    try:
        results = client.recognize_pii_entities([text], language="en")
        entities: list[dict] = []
        for result in results:
            if result.is_error:
                logger.warning(
                    "PII Detection returned an error result for document; skipping entities."
                )
                continue
            for entity in result.entities:
                entities.append({"category": entity.category, "text": entity.text})
        return entities
    except Exception as exc:
        logger.warning("PII Detection API call failed: %s — using default metadata.", exc)
        return []


# ---------------------------------------------------------------------------
# _map_pii_entities
# ---------------------------------------------------------------------------

def _map_pii_entities(entities: list[dict]) -> dict:
    """Map entity type strings to has_* boolean flags and aggregate counts.

    Args:
        entities: List of entity dicts, each with at least a "category" key.

    Returns:
        dict with keys:
            has_ssn, has_name, has_dob, has_address, has_phone  (bool)
            pii_types  (list[str] — distinct categories, preserving insertion order)
            pii_entity_count  (int — total entity count, including duplicates)
    """
    has_ssn = False
    has_name = False
    has_dob = False
    has_address = False
    has_phone = False
    seen_types: list[str] = []
    seen_set: set[str] = set()

    for entity in entities:
        category: str = entity.get("category", "")

        # Accumulate distinct pii_types (preserve insertion order)
        if category not in seen_set:
            seen_types.append(category)
            seen_set.add(category)

        # Apply mapping rules (substring match)
        if "SocialSecurity" in category:
            has_ssn = True
        if "Person" in category:
            has_name = True
        if "DateTime" in category or ("Date" in category and "DateTime" not in category):
            has_dob = True
        if "Address" in category:
            has_address = True
        if "PhoneNumber" in category:
            has_phone = True

    return {
        "has_ssn": has_ssn,
        "has_name": has_name,
        "has_dob": has_dob,
        "has_address": has_address,
        "has_phone": has_phone,
        "pii_types": seen_types,
        "pii_entity_count": len(entities),
    }


# ---------------------------------------------------------------------------
# _build_v3_document
# ---------------------------------------------------------------------------

def _build_v3_document(
    md5: str,
    file_path: str,
    content: str,
    pii_metadata: dict,
) -> dict:
    """Build a V3 search document dict for Azure AI Search.

    Includes all V2 fields plus the V3 PII metadata fields.

    Args:
        md5: MD5 hash of the file (used as document id).
        file_path: Path to the file (from DLU record).
        content: Extracted text content.
        pii_metadata: Dict returned by _map_pii_entities.

    Returns:
        Dictionary with all required V3 index fields.
    """
    return {
        # V2 base fields
        "id": md5,
        "md5": md5,
        "file_path": file_path,
        "content": content,
        "content_phonetic": content,
        "content_lowercase": content,
        # V3 PII metadata fields
        "has_ssn": pii_metadata["has_ssn"],
        "has_name": pii_metadata["has_name"],
        "has_dob": pii_metadata["has_dob"],
        "has_address": pii_metadata["has_address"],
        "has_phone": pii_metadata["has_phone"],
        "pii_types": pii_metadata["pii_types"],
        "pii_entity_count": pii_metadata["pii_entity_count"],
    }


# ---------------------------------------------------------------------------
# Internal DB / upload helpers
# ---------------------------------------------------------------------------

def _query_all_dlu_records_v3(db: Any) -> list[Any]:
    """Query all records from [DLU].[datalakeuniverse].

    Args:
        db: SQLAlchemy Session.

    Returns:
        List of all DLU records (MD5 + file_path).
    """
    records = db.query(DLU).all()
    logger.info("V3 indexer: found %d total DLU records.", len(records))
    return records


def _is_supported_extension_v3(file_path: str) -> bool:
    """Check if file_path has a supported extension (.txt, .xlsx, .xls, .csv)."""
    ext = Path(file_path).suffix.lower()
    return ext in SUPPORTED_EXTENSIONS


def _upload_documents_v3(search_client: Any, documents: list[dict]) -> list[str]:
    """Upload documents to Azure AI Search in batches.

    Args:
        search_client: Azure SearchClient instance.
        documents: List of document dicts to upload.

    Returns:
        List of error strings for any documents that failed to upload.
    """
    errors: list[str] = []
    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i : i + BATCH_SIZE]
        logger.info(
            "V3 upload: batch %d/%d (%d documents).",
            (i // BATCH_SIZE) + 1,
            (len(documents) + BATCH_SIZE - 1) // BATCH_SIZE,
            len(batch),
        )
        results = search_client.upload_documents(documents=batch)
        for result in results:
            if not result.succeeded:
                error_msg = (
                    f"{result.key}: {getattr(result, 'error_message', 'upload failed')}"
                )
                errors.append(error_msg)
                logger.error("V3 upload failed for %s: %s", result.key, error_msg)
    return errors


# ---------------------------------------------------------------------------
# index_all_files_v3 — main public entry point
# ---------------------------------------------------------------------------

def index_all_files_v3(
    db: Any,
    search_client: Any,
    config: Any = None,
    pii_client: Any = None,
) -> IndexResponse:
    """Index all eligible files from DLU into Azure AI Search V3 index.

    V3 behavior (extends V2):
    - Queries all DLU records (MD5 + file_path only)
    - Filters by extension from file_path at runtime (.txt/.csv/.xls/.xlsx)
    - Extracts text via extract_text() (reuses V2 service)
    - Calls PII Detection API per document
    - Falls back to default PII metadata if API fails
    - Uploads documents with full PII metadata to V3 index

    Args:
        db: SQLAlchemy Session (mocked in tests).
        search_client: Azure SearchClient instance pointing at V3 index.
        config: Settings object (provides AZURE_SEARCH_INDEX_V3, language keys).
        pii_client: Optional injected TextAnalyticsClient (for testing / DI).

    Returns:
        IndexResponse with counts and error messages.
    """
    all_records = _query_all_dlu_records_v3(db)

    if not all_records:
        logger.info("V3 indexer: No DLU records found. Nothing to index.")
        return IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )

    files_processed = 0
    files_succeeded = 0
    files_failed = 0
    files_skipped = 0
    extraction_errors: list[str] = []
    documents: list[dict] = []

    for record in all_records:
        md5 = record.MD5
        file_path = record.file_path

        # Skip unsupported extensions
        if not _is_supported_extension_v3(file_path or ""):
            ext = Path(file_path or "").suffix.lower() if file_path else "(none)"
            logger.warning(
                "V3: skipping unsupported extension '%s' for MD5 %s", ext, md5
            )
            files_skipped += 1
            continue

        files_processed += 1
        logger.info("V3: processing MD5=%s: %s", md5, file_path)

        # Text extraction
        text = extract_text(file_path)
        if text is None:
            files_failed += 1
            error_msg = f"{md5}: extraction failed for {file_path}"
            extraction_errors.append(error_msg)
            logger.warning(error_msg)
            continue

        # PII Detection (with fallback)
        entities = _call_pii_detection(text, client=pii_client)
        pii_metadata = _map_pii_entities(entities)

        # Build and queue document
        doc = _build_v3_document(md5, file_path, text, pii_metadata)
        documents.append(doc)

    logger.info(
        "V3 indexer: %d/%d files extracted (%d failed, %d skipped).",
        files_processed,
        files_processed + files_skipped,
        files_failed,
        files_skipped,
    )

    # Upload all successfully extracted documents
    upload_errors: list[str] = []
    if documents:
        upload_errors = _upload_documents_v3(search_client, documents)

    # Count upload successes and failures
    upload_failed_keys = set()
    for err in upload_errors:
        key = err.split(":")[0].strip()
        upload_failed_keys.add(key)

    for doc in documents:
        doc_md5 = doc["md5"]
        if doc_md5 in upload_failed_keys:
            files_failed += 1
        else:
            files_succeeded += 1

    all_errors = extraction_errors + upload_errors
    total_failed = files_failed  # already accumulated above

    return IndexResponse(
        files_processed=files_processed,
        files_succeeded=files_succeeded,
        files_failed=total_failed,
        files_skipped=files_skipped,
        errors=all_errors,
    )
