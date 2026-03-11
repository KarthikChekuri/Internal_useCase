"""Indexing router — POST /index/all and POST /index/{guid} endpoints.

Triggers file indexing into Azure AI Search:
- POST /index/all: indexes all eligible DLU files
- POST /index/{guid}: indexes a single file by its GUID

Error handling:
- 404: GUID not found in DLU table
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_db, get_search_client, get_settings
from app.services.indexing_service import (
    IndexResponse,
    index_all_files,
    index_single_file,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/index/all", response_model=IndexResponse)
def index_all(
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> IndexResponse:
    """Index all eligible breach files from DLU into Azure AI Search.

    Queries DLU for files matching the configured case name with supported
    extensions and not excluded, extracts text, and uploads to the search index.

    Args:
        db: SQLAlchemy session (injected).
        search_client: Azure SearchClient (injected).
        settings: Application settings (injected).

    Returns:
        IndexResponse with processing counts and any errors.
    """
    result = index_all_files(db=db, search_client=search_client, config=settings)
    return result


@router.post("/index/{guid}", response_model=IndexResponse)
def index_single(
    guid: str,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> IndexResponse:
    """Index a single breach file by its GUID.

    Looks up the GUID in DLU, extracts text from the file, and uploads
    the document to Azure AI Search.

    Args:
        guid: The file GUID to index.
        db: SQLAlchemy session (injected).
        search_client: Azure SearchClient (injected).
        settings: Application settings (injected).

    Returns:
        IndexResponse with processing counts.

    Raises:
        HTTPException 404: GUID not found in DLU.
    """
    result = index_single_file(
        db=db, search_client=search_client, config=settings, guid=guid
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"GUID '{guid}' not found in DLU",
        )

    return result
