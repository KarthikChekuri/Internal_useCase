"""FastAPI application entry point.

Creates the FastAPI app instance with:
- Router registration (search, indexing)
- CORS middleware (allow all origins for dev)
- Lifespan handler for startup/shutdown logging
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.indexing import router as indexing_router
from app.routers.search import router as search_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for startup and shutdown events.

    Logs startup and shutdown messages. Can be extended later
    to initialize database connections, warm caches, etc.
    """
    logger.info("Breach PII Search API starting up.")
    yield
    logger.info("Breach PII Search API shutting down.")


app = FastAPI(
    title="Breach PII Search API",
    description="Search breach files for customer PII using Azure AI Search and fuzzy matching.",
    version="0.1.0",
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

# Register routers
app.include_router(search_router)
app.include_router(indexing_router)
