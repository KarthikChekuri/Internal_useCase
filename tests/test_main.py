"""Tests for app/main.py — FastAPI app instance, router registration, CORS, lifespan.

Updated for V2: batch router replaces V1 search router.

Tests cover:
- FastAPI app creates successfully
- Batch router is registered (POST /batch/run, GET /batch/{batch_id}/status, etc.)
- Indexing router is registered (POST /index/all and POST /index/{md5} are accessible)
- V1 POST /search is NOT registered (removed in V2)
- CORS middleware is configured (allow all origins for dev)
- Routes are visible in /docs
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app():
    """Return the FastAPI app instance."""
    from app.main import app
    return app


@pytest.fixture
def test_client(app):
    """Create a TestClient with mocked dependencies."""
    from app.dependencies import get_db, get_search_client, get_settings

    mock_db = MagicMock()
    mock_search_client = MagicMock()
    mock_settings = MagicMock()
    mock_settings.STRATEGIES_FILE = "strategies.yaml"

    def override_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_search_client] = lambda: mock_search_client
    app.dependency_overrides[get_settings] = lambda: mock_settings

    yield TestClient(app)

    app.dependency_overrides.clear()


class TestAppCreation:
    """Tests for the FastAPI app instance."""

    def test_app_is_fastapi_instance(self, app):
        """The app should be a FastAPI instance."""
        from fastapi import FastAPI
        assert isinstance(app, FastAPI)

    def test_app_has_title(self, app):
        """The app should have a descriptive title."""
        assert app.title is not None
        assert len(app.title) > 0


class TestRouterRegistration:
    """Tests that all routers are properly registered (V2)."""

    def test_batch_run_route_registered(self, app):
        """POST /batch/run should be a registered route (V2)."""
        routes = [route.path for route in app.routes]
        assert "/batch/run" in routes

    def test_batch_status_route_registered(self, app):
        """GET /batch/{batch_id}/status should be registered (V2)."""
        routes = [route.path for route in app.routes]
        assert "/batch/{batch_id}/status" in routes

    def test_index_all_route_registered(self, app):
        """POST /index/all should be a registered route."""
        routes = [route.path for route in app.routes]
        assert "/index/all" in routes

    def test_index_single_route_registered(self, app):
        """POST /index/{md5} should be a registered route (V2 uses md5 not guid)."""
        routes = [route.path for route in app.routes]
        assert "/index/{md5}" in routes

    def test_v1_search_route_removed(self, app):
        """POST /search should NOT be registered — V1 endpoint removed."""
        routes = [route.path for route in app.routes]
        assert "/search" not in routes

    def test_index_all_accepts_post(self, test_client):
        """POST /index/all should accept POST method."""
        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=0, files_succeeded=0, files_failed=0, errors=[]
        )
        with patch("app.routers.indexing.index_all_files_v2", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code != 405


class TestCORSMiddleware:
    """Tests for CORS middleware configuration."""

    def test_cors_allows_any_origin(self, test_client):
        """CORS should allow requests from any origin (dev mode)."""
        response = test_client.options(
            "/batches",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            },
        )
        # CORS preflight should be allowed
        assert response.headers.get("access-control-allow-origin") in ("*", "http://localhost:3000")

    def test_cors_allows_post_method(self, test_client):
        """CORS should allow POST method."""
        response = test_client.options(
            "/batch/run",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        allowed_methods = response.headers.get("access-control-allow-methods", "")
        assert "POST" in allowed_methods or "*" in allowed_methods


class TestOpenAPISchema:
    """Tests that routes are visible in OpenAPI schema (would appear in /docs)."""

    def test_openapi_schema_includes_batch_run(self, app):
        """The OpenAPI schema should include POST /batch/run."""
        schema = app.openapi()
        assert "/batch/run" in schema["paths"]
        assert "post" in schema["paths"]["/batch/run"]

    def test_openapi_schema_includes_index_all(self, app):
        """The OpenAPI schema should include POST /index/all."""
        schema = app.openapi()
        assert "/index/all" in schema["paths"]
        assert "post" in schema["paths"]["/index/all"]

    def test_openapi_schema_does_not_include_v1_search(self, app):
        """The OpenAPI schema should NOT include POST /search (V1 removed)."""
        schema = app.openapi()
        assert "/search" not in schema["paths"]
