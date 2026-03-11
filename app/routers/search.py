"""Search router — POST /search endpoint.

Accepts a SearchRequest body with SSN (and optional fullname), calls
the search_service to orchestrate customer lookup, Azure AI Search,
leak detection, and returns a SearchResponse.

Error handling:
- 404: Customer not found by SSN
- 409: Fullname mismatch or duplicate SSN (data integrity)
- 422: SSN format validation (handled by Pydantic)
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_db, get_search_client, get_settings
from app.schemas.search import SearchRequest, SearchResponse
from app.services.search_service import (
    CustomerNotFoundError,
    DataIntegrityError,
    FullnameMismatchError,
    search_customer_pii,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/search", response_model=SearchResponse)
def search_pii(
    request: SearchRequest,
    db: Any = Depends(get_db),
    search_client: Any = Depends(get_search_client),
    settings: Any = Depends(get_settings),
) -> SearchResponse:
    """Search breach files for a customer's PII.

    Looks up the customer by SSN, optionally validates the fullname,
    searches Azure AI Search for candidate files, runs leak detection
    on each file, and returns results ordered by confidence.

    Args:
        request: SearchRequest with ssn and optional fullname.
        db: SQLAlchemy session (injected).
        search_client: Azure SearchClient (injected).
        settings: Application settings (injected).

    Returns:
        SearchResponse with search results.

    Raises:
        HTTPException 404: Customer not found.
        HTTPException 409: Fullname mismatch or duplicate SSN.
    """
    try:
        result = search_customer_pii(
            db=db,
            search_client=search_client,
            ssn=request.ssn,
            fullname=request.fullname,
            config=settings,
        )
        return result
    except CustomerNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except DataIntegrityError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except FullnameMismatchError as e:
        raise HTTPException(status_code=409, detail=str(e))
