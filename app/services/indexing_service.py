"""Indexing pipeline service — Phase V2-2.1.

V2 rewrite: Queries DLU with MD5 PK, filters by extension from file_path,
uses file_path directly (no base path), builds docs with id=MD5, supports
resumability via file_status table, supports force re-index.

Public API (V2):
    index_all_files_v2(db, search_client, config, force=False)  -> IndexResponse
    index_single_file_v2(db, search_client, config, md5) -> IndexResponse | None

V1 compatibility (kept for existing tests):
    index_all_files(db, search_client, config) -> IndexResponse
    index_single_file(db, search_client, config, guid) -> IndexResponse | None
"""

import datetime
import logging
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from app.models.dlu import DLU
from app.models.file_status import FileStatus
from app.services.text_extraction import extract_text

logger = logging.getLogger(__name__)

# Supported file extensions for indexing (checked from file_path at runtime)
SUPPORTED_EXTENSIONS = {".txt", ".xls", ".xlsx", ".csv"}

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
    files_skipped: int = 0
    errors: list[str]


# ---------------------------------------------------------------------------
# V2 Internal helpers
# ---------------------------------------------------------------------------

def _is_supported_extension(file_path: str) -> bool:
    """Check if file_path has a supported extension (.txt, .xlsx, .xls, .csv).

    Args:
        file_path: The file path string from the DLU table.

    Returns:
        True if extension is supported, False otherwise.
    """
    ext = Path(file_path).suffix.lower()
    return ext in SUPPORTED_EXTENSIONS


def _query_all_dlu_records(db: Any) -> list[Any]:
    """Query all records from [DLU].[datalakeuniverse].

    V2: no filtering in DB query — extension filtering is done at runtime
    from file_path.

    Args:
        db: SQLAlchemy Session.

    Returns:
        List of all DLU records (MD5 + file_path).
    """
    records = db.query(DLU).all()
    logger.info("Found %d total DLU records.", len(records))
    return records


def _query_dlu_by_md5(db: Any, md5: str) -> Any:
    """Query a single DLU record by its MD5 primary key.

    Args:
        db: SQLAlchemy Session.
        md5: The MD5 hash to look up.

    Returns:
        DLU record, or None if not found.
    """
    return db.query(DLU).filter(DLU.MD5 == md5).first()


def _get_indexed_md5s(db: Any) -> set[str]:
    """Return the set of MD5 hashes that are already indexed (status='indexed').

    Args:
        db: SQLAlchemy Session.

    Returns:
        Set of MD5 strings with status='indexed' in [Index].[file_status].
    """
    rows = db.query(FileStatus).filter(FileStatus.status == "indexed").all()
    return {row.md5 for row in rows}


def _upsert_file_status(
    db: Any, md5: str, status: str, error_message: str | None = None
) -> None:
    """Insert or update a row in [Index].[file_status].

    Args:
        db: SQLAlchemy Session.
        md5: The MD5 hash (PK).
        status: 'indexed', 'failed', or 'skipped'.
        error_message: Error description for failed files.
    """
    existing = db.query(FileStatus).filter(FileStatus.md5 == md5).first()
    if existing is not None:
        existing.status = status
        existing.indexed_at = datetime.datetime.utcnow()
        existing.error_message = error_message
    else:
        row = FileStatus(
            md5=md5,
            status=status,
            indexed_at=datetime.datetime.utcnow(),
            error_message=error_message,
        )
        db.add(row)
    db.commit()


def _build_document_v2(record: Any, text: str) -> dict[str, str]:
    """Build a V2 search document dict for Azure AI Search.

    V2: id=MD5, md5, content/content_phonetic/content_lowercase all same text,
    file_path from DLU record directly.

    Args:
        record: DLU V2 record with MD5 and file_path.
        text: Extracted text content.

    Returns:
        Dictionary with all required V2 index fields.
    """
    return {
        "id": record.MD5,
        "md5": record.MD5,
        "content": text,
        "content_phonetic": text,
        "content_lowercase": text,
        "file_path": record.file_path,
    }


def _log_indexing_progress(processed: int, total: int, failed: int) -> None:
    """Log indexing progress in the spec-required format.

    Emits: "Indexing: {processed}/{total} files processed ({failed} failed)"

    Args:
        processed: Number of files processed so far (attempted).
        total: Total files to process.
        failed: Number of files that failed extraction or upload.
    """
    logger.info("Indexing: %d/%d files processed (%d failed)", processed, total, failed)


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
# V2 Public API
# ---------------------------------------------------------------------------

def index_all_files_v2(
    db: Any, search_client: Any, config: Any, force: bool = False
) -> IndexResponse:
    """Index all eligible files from DLU into Azure AI Search (V2).

    V2 behavior:
    - Queries all DLU records (MD5 + file_path only)
    - Filters by extension from file_path at runtime
    - Skips already-indexed files (unless force=True)
    - Uses file_path directly (no base path join)
    - Document id = MD5 hash
    - Updates [Index].[file_status] for each file

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        config: Settings object.
        force: If True, re-index all files regardless of previous status.

    Returns:
        IndexResponse with counts (including files_skipped) and error messages.
    """
    all_records = _query_all_dlu_records(db)

    if not all_records:
        logger.info("No DLU records found. Nothing to index.")
        return IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=0,
            errors=[],
        )

    # Get already-indexed MD5s for resumability (empty set when force=True)
    indexed_md5s: set[str] = set() if force else _get_indexed_md5s(db)

    files_processed = 0
    files_succeeded = 0
    files_failed = 0
    files_skipped = 0
    extraction_errors: list[str] = []
    documents: list[dict] = []

    for record in all_records:
        md5 = record.MD5
        file_path = record.file_path

        # Skip unsupported extensions (counted as skipped, not failed)
        if not _is_supported_extension(file_path or ""):
            ext = Path(file_path or "").suffix.lower() if file_path else "(none)"
            logger.warning("Skipping unsupported extension '%s' for MD5 %s", ext, md5)
            files_skipped += 1
            continue

        # Skip already-indexed files (resumability)
        if md5 in indexed_md5s:
            logger.debug("Skipping already-indexed MD5 %s", md5)
            files_skipped += 1
            continue

        # Process file
        files_processed += 1
        logger.info("Processing file MD5=%s: %s", md5, file_path)

        text = extract_text(file_path)

        if text is None:
            files_failed += 1
            error_msg = f"{md5}: extraction failed for {file_path}"
            extraction_errors.append(error_msg)
            logger.warning(error_msg)
            _upsert_file_status(db, md5, status="failed", error_message=error_msg)
            continue

        doc = _build_document_v2(record, text)
        documents.append(doc)

    # Log indexing progress summary after extraction loop
    _log_indexing_progress(processed=files_processed, total=files_processed + files_skipped, failed=files_failed)

    # Upload all successfully extracted documents
    upload_errors: list[str] = []
    if documents:
        upload_errors = _upload_documents(search_client, documents)

    # Track upload results in file_status and count successes/failures
    upload_failed_keys = set()
    for err in upload_errors:
        # Error format is "md5_key: error message"
        key = err.split(":")[0].strip()
        upload_failed_keys.add(key)

    for doc in documents:
        doc_md5 = doc["md5"]
        if doc_md5 in upload_failed_keys:
            _upsert_file_status(
                db, doc_md5, status="failed",
                error_message=next((e for e in upload_errors if e.startswith(doc_md5)), None)
            )
        else:
            files_succeeded += 1
            _upsert_file_status(db, doc_md5, status="indexed")

    all_errors = extraction_errors + upload_errors
    total_failed = files_failed + len(upload_errors)

    return IndexResponse(
        files_processed=files_processed,
        files_succeeded=files_succeeded,
        files_failed=total_failed,
        files_skipped=files_skipped,
        errors=all_errors,
    )


def index_single_file_v2(
    db: Any, search_client: Any, config: Any, md5: str
) -> "IndexResponse | None":
    """Index a single file by its MD5 hash (V2).

    Args:
        db: SQLAlchemy Session.
        search_client: Azure SearchClient instance.
        config: Settings object.
        md5: The MD5 hash to index.

    Returns:
        IndexResponse on success/failure, or None if MD5 not found in DLU
        (caller should raise 404).
    """
    record = _query_dlu_by_md5(db, md5)

    if record is None:
        logger.warning("MD5 '%s' not found in DLU.", md5)
        return None

    file_path = record.file_path

    # Check extension
    if not _is_supported_extension(file_path or ""):
        ext = Path(file_path or "").suffix.lower() if file_path else "(none)"
        logger.warning("Unsupported extension '%s' for MD5 %s", ext, md5)
        return IndexResponse(
            files_processed=0,
            files_succeeded=0,
            files_failed=0,
            files_skipped=1,
            errors=[],
        )

    logger.info("Processing single file MD5=%s: %s", md5, file_path)

    text = extract_text(file_path)

    if text is None:
        error_msg = f"{md5}: extraction failed for {file_path}"
        logger.warning(error_msg)
        _upsert_file_status(db, md5, status="failed", error_message=error_msg)
        return IndexResponse(
            files_processed=1,
            files_succeeded=0,
            files_failed=1,
            files_skipped=0,
            errors=[error_msg],
        )

    doc = _build_document_v2(record, text)
    upload_errors = _upload_documents(search_client, [doc])

    if upload_errors:
        _upsert_file_status(
            db, md5, status="failed",
            error_message="; ".join(upload_errors)
        )
        return IndexResponse(
            files_processed=1,
            files_succeeded=0,
            files_failed=1,
            files_skipped=0,
            errors=upload_errors,
        )

    _upsert_file_status(db, md5, status="indexed")
    return IndexResponse(
        files_processed=1,
        files_succeeded=1,
        files_failed=0,
        files_skipped=0,
        errors=[],
    )
