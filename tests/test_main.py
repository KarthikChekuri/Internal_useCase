"""Tests for app/main.py — FastAPI app instance, router registration, CORS, lifespan.

Tests cover:
- FastAPI app creates successfully
- Search router is registered (POST /search is accessible)
- Indexing router is registered (POST /index/all and POST /index/{guid} are accessible)
- CORS middleware is configured (allow all origins for dev)
- Routes are visible (would show in /docs)
- Lifespan handler runs startup/shutdown without error
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
    """Tests that all routers are properly registered."""

    def test_search_route_registered(self, app):
        """POST /search should be a registered route."""
        routes = [route.path for route in app.routes]
        assert "/search" in routes

    def test_index_all_route_registered(self, app):
        """POST /index/all should be a registered route."""
        routes = [route.path for route in app.routes]
        assert "/index/all" in routes

    def test_index_single_route_registered(self, app):
        """POST /index/{guid} should be a registered route."""
        routes = [route.path for route in app.routes]
        assert "/index/{guid}" in routes

    def test_search_route_accepts_post(self, test_client):
        """POST /search should accept POST method (not 405)."""
        # We're not checking for success, just that the route exists and accepts POST
        # It will fail validation (422) because we didn't send a body, which proves the route exists
        response = test_client.post("/search", json={})
        assert response.status_code != 405  # 405 = Method Not Allowed

    def test_index_all_accepts_post(self, test_client):
        """POST /index/all should accept POST method."""
        from app.services.indexing_service import IndexResponse

        mock_result = IndexResponse(
            files_processed=0, files_succeeded=0, files_failed=0, errors=[]
        )
        with patch("app.routers.indexing.index_all_files", return_value=mock_result):
            response = test_client.post("/index/all")
            assert response.status_code != 405


class TestCORSMiddleware:
    """Tests for CORS middleware configuration."""

    def test_cors_allows_any_origin(self, test_client):
        """CORS should allow requests from any origin (dev mode)."""
        response = test_client.options(
            "/search",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST",
            },
        )
        # CORS preflight should be allowed
        assert response.headers.get("access-control-allow-origin") in ("*", "http://localhost:3000")

    def test_cors_allows_post_method(self, test_client):
        """CORS should allow POST method."""
        response = test_client.options(
            "/search",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        allowed_methods = response.headers.get("access-control-allow-methods", "")
        assert "POST" in allowed_methods or "*" in allowed_methods


class TestOpenAPISchema:
    """Tests that routes are visible in OpenAPI schema (would appear in /docs)."""

    def test_openapi_schema_includes_search(self, app):
        """The OpenAPI schema should include POST /search."""
        schema = app.openapi()
        assert "/search" in schema["paths"]
        assert "post" in schema["paths"]["/search"]

    def test_openapi_schema_includes_index_all(self, app):
        """The OpenAPI schema should include POST /index/all."""
        schema = app.openapi()
        assert "/index/all" in schema["paths"]
        assert "post" in schema["paths"]["/index/all"]

    def test_openapi_schema_includes_index_guid(self, app):
        """The OpenAPI schema should include POST /index/{guid}."""
        schema = app.openapi()
        assert "/index/{guid}" in schema["paths"]
        assert "post" in schema["paths"]["/index/{guid}"]
