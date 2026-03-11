"""Indexing pipeline service — Phase 3.1.

Queries DLU for eligible breach files, extracts text via the text_extraction
service, builds Azure AI Search documents, and uploads them in batches.

Public API:
    index_all_files(db, search_client, config)  -> IndexResponse
    index_single_file(db, search_client, config, guid) -> IndexResponse | None
"""

import logging
import os
from typing import Any

from pydantic import BaseModel

from app.models.dlu import DLU
from app.services.text_extraction import extract_text

logger = logging.getLogger(__name__)

# Supported file extensions for indexing
# Match both with and without leading dot (DB may store either format)
SUPPORTED_EXTENSIONS = {".txt", ".xls", ".xlsx", ".csv", "txt", "xls", "xlsx", "csv"}

# Azure AI Search upload batch size limit
BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------

class IndexResponse(BaseModel):
    """JSON-serializable response from indexing operations."""

    files_processed: int
    files_succeeded: int
    files_failed: int
    errors: list[str]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _query_eligible_files(db: Any, case_name: str) -> list[Any]:
    """Query DLU for files eligible for indexing.

    Filters:
        - fileExtension in SUPPORTED_EXTENSIONS
        - isExclusion == False (or 0)
        - caseName == case_name
    """
    records = (
        db.query(DLU)
        .filter(
            DLU.fileExtension.in_(SUPPORTED_EXTENSIONS),
            DLU.isExclusion == False,  # noqa: E712
            DLU.caseName == case_name,
        )
        .all()
    )
    logger.info("Found %d eligible files for case '%s'.", len(records), case_name)
    return records


def _resolve_file_path(base_path: str, textpath: str | None) -> str | None:
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


def _build_document(record: Any, text: str, full_path: str) -> dict[str, str]:
    """Build a search document dict for Azure AI Search.

    Args:
        record: DLU record with GUID, fileName, fileExtension, caseName.
        text: Extracted text content.
        full_path: Resolved file path on disk.

    Returns:
        Dictionary with all required index fields.
    """
    return {
        "id": record.GUID,
        "file_guid": record.GUID,
        "content": text,
        "content_phonetic": text,
        "content_lowercase": text,
        "file_name": record.fileName,
        "file_path": full_path,
        "file_extension": record.fileExtension,
        "case_name": record.caseName,
    }


def _upload_documents(search_client: Any, documents: list[dict]) -> list[str]:
    """Upload documents to Azure AI Search in batches of up to BATCH_SIZE.

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
            "Uploading batch %d/%d (%d documents).",
            (i // BATCH_SIZE) + 1,
            (len(documents) + BATCH_SIZE - 1) // BATCH_SIZE,
            len(batch),
        )
        results = search_client.upload_documents(documents=batch)
        for result in results:
            if not result.succeeded:
                error_msg = f"{result.key}: {getattr(result, 'error_message', 'upload failed')}"
                errors.append(error_msg)
                logger.error("Upload failed for document %s: %s", result.key, error_msg)

    return errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def index_all_files(db: Any, search_client: Any, config: Any) -> IndexResponse:
    """Index all eligible files from DLU into Azure AI Search.

    Queries DLU for eligible files (supported extension, not excluded, matching
    case name), extracts text, builds search documents, and uploads in batches.

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        config: Settings object with FILE_BASE_PATH, CASE_NAME, etc.

    Returns:
        IndexResponse with counts and any error messages.
    """
    records = _query_eligible_files(db, config.CASE_NAME)

    if not records:
        logger.info("No eligible files found. Nothing to index.")
        return IndexResponse(
            files_processed=0, files_succeeded=0, files_failed=0, errors=[]
        )

    documents: list[dict] = []
    extraction_errors: list[str] = []
    extraction_failed_count = 0

    for record in records:
        full_path = _resolve_file_path(config.FILE_BASE_PATH, record.TEXTPATH)
        if full_path is None:
            extraction_failed_count += 1
            error_msg = f"{record.GUID}: TEXTPATH is null"
            extraction_errors.append(error_msg)
            logger.warning(error_msg)
            continue
        logger.info("Processing file %s: %s", record.GUID, full_path)

        text = extract_text(full_path)

        if text is None:
            extraction_failed_count += 1
            error_msg = f"{record.GUID}: extraction failed for {full_path}"
            extraction_errors.append(error_msg)
            logger.warning(error_msg)
            continue

        doc = _build_document(record, text, full_path)
        documents.append(doc)

    # Upload all successfully extracted documents
    upload_errors: list[str] = []
    if documents:
        upload_errors = _upload_documents(search_client, documents)

    all_errors = extraction_errors + upload_errors
    total_failed = extraction_failed_count + len(upload_errors)
    total_succeeded = len(records) - total_failed

    return IndexResponse(
        files_processed=len(records),
        files_succeeded=total_succeeded,
        files_failed=total_failed,
        errors=all_errors,
    )


def index_single_file(
    db: Any, search_client: Any, config: Any, guid: str
) -> IndexResponse | None:
    """Index a single file by its GUID.

    Looks up the GUID in DLU, extracts text, builds a document, and uploads it.

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        config: Settings object with FILE_BASE_PATH, CASE_NAME, etc.
        guid: The file GUID to index.

    Returns:
        IndexResponse on success/failure, or None if the GUID is not found
        in DLU (caller should raise 404).
    """
    record = db.query(DLU).filter(DLU.GUID == guid).first()

    if record is None:
        logger.warning("GUID '%s' not found in DLU.", guid)
        return None

    full_path = _resolve_file_path(config.FILE_BASE_PATH, record.TEXTPATH)
    if full_path is None:
        error_msg = f"{guid}: TEXTPATH is null"
        logger.warning(error_msg)
        return IndexResponse(
            files_processed=1, files_succeeded=0, files_failed=1, errors=[error_msg]
        )
    logger.info("Processing single file %s: %s", guid, full_path)

    text = extract_text(full_path)

    if text is None:
        error_msg = f"{guid}: extraction failed for {full_path}"
        logger.warning(error_msg)
        return IndexResponse(
            files_processed=1, files_succeeded=0, files_failed=1, errors=[error_msg]
        )

    doc = _build_document(record, text, full_path)
    upload_errors = _upload_documents(search_client, [doc])

    if upload_errors:
        return IndexResponse(
            files_processed=1, files_succeeded=0, files_failed=1, errors=upload_errors
        )

    return IndexResponse(
        files_processed=1, files_succeeded=1, files_failed=0, errors=[]
    )
