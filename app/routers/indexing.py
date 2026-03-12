"""Indexing router — Phase V2-2.1.

V2 endpoints:
- POST /index/all: indexes all eligible DLU files (with optional force=True)
- POST /index/{md5}: indexes a single file by its MD5 hash

Error handling:
- 404: MD5 not found in DLU table
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_db, get_search_client, get_settings
from app.services.indexing_service import (
    IndexResponse,
    index_all_files_v2,
    index_single_file_v2,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/index/all", response_model=IndexResponse)
def index_all(
    force: bool = False,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> IndexResponse:
    """Index all eligible breach files from DLU into Azure AI Search (V2).

    V2: Queries DLU by MD5 PK, filters by extension from file_path,
    uses file_path directly, supports resumability via file_status table.

    Args:
        force: If True, re-index all files regardless of previous status.
        db: SQLAlchemy session (injected).
        search_client: Azure SearchClient (injected).
        settings: Application settings (injected).

    Returns:
        IndexResponse with processing counts (including files_skipped) and errors.
    """
    result = index_all_files_v2(
        db=db, search_client=search_client, config=settings, force=force
    )
    return result


@router.post("/index/{md5}", response_model=IndexResponse)
def index_single(
    md5: str,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> IndexResponse:
    """Index a single breach file by its MD5 hash (V2).

    V2: Looks up MD5 in DLU, extracts text from file_path,
    uploads document to Azure AI Search with id=MD5.

    Args:
        md5: The file MD5 hash to index.
        db: SQLAlchemy session (injected).
        search_client: Azure SearchClient (injected).
        settings: Application settings (injected).

    Returns:
        IndexResponse with processing counts.

    Raises:
        HTTPException 404: MD5 not found in DLU.
    """
    result = index_single_file_v2(
        db=db, search_client=search_client, config=settings, md5=md5
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"MD5 '{md5}' not found in DLU",
        )

    return result
