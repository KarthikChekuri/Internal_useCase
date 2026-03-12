"""FastAPI application entry point — V2.

Creates the FastAPI app instance with:
- Router registration (batch, indexing) — V1 search router removed
- CORS middleware (allow all origins for dev)
- Lifespan handler for startup/shutdown logging
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.batch import router as batch_router
from app.routers.batch_v3 import router as batch_v3_router
from app.routers.indexing import router as indexing_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for startup and shutdown events.

    Logs startup and shutdown messages. Can be extended later
    to initialize database connections, warm caches, etc.
    """
    logger.info("Breach PII Search API starting up (V2).")
    yield
    logger.info("Breach PII Search API shutting down.")


app = FastAPI(
    title="Breach PII Search API",
    description="Search breach files for customer PII using Azure AI Search and fuzzy matching.",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS middleware — allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers — V2: batch replaces V1 search endpoint
app.include_router(batch_router)
app.include_router(indexing_router)

# Register V3 router — Azure-only pipeline
app.include_router(batch_v3_router, prefix="/v3", tags=["V3"])
