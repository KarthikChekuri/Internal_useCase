"""Tests for app/main.py V2 — FastAPI app with batch router (Phase V2-3.3).

Covers:
- Batch router is registered (not the old search router)
- Indexing router is still registered
- POST /search is REMOVED (V1 endpoint gone)
- All batch routes are visible in the app
- CORS still configured
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app():
    """Return the FastAPI app instance (V2)."""
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


class TestV2AppCreation:
    """Tests for the V2 FastAPI app instance."""

    def test_app_is_fastapi_instance(self, app):
        """The app should be a FastAPI instance."""
        from fastapi import FastAPI
        assert isinstance(app, FastAPI)

    def test_app_has_title(self, app):
        """The app should have a descriptive title."""
        assert app.title is not None
        assert len(app.title) > 0


class TestV2RouterRegistration:
    """Tests that V2 routers are properly registered and V1 search is removed."""

    def test_batch_run_route_registered(self, app):
        """POST /batch/run should be a registered route."""
        routes = [route.path for route in app.routes]
        assert "/batch/run" in routes

    def test_batch_status_route_registered(self, app):
        """GET /batch/{batch_id}/status should be registered."""
        routes = [route.path for route in app.routes]
        assert "/batch/{batch_id}/status" in routes

    def test_batch_customers_route_registered(self, app):
        """GET /batch/{batch_id}/customers should be registered."""
        routes = [route.path for route in app.routes]
        assert "/batch/{batch_id}/customers" in routes

    def test_batch_results_route_registered(self, app):
        """GET /batch/{batch_id}/results should be registered."""
        routes = [route.path for route in app.routes]
        assert "/batch/{batch_id}/results" in routes

    def test_batch_resume_route_registered(self, app):
        """POST /batch/{batch_id}/resume should be registered."""
        routes = [route.path for route in app.routes]
        assert "/batch/{batch_id}/resume" in routes

    def test_batches_list_route_registered(self, app):
        """GET /batches should be registered."""
        routes = [route.path for route in app.routes]
        assert "/batches" in routes

    def test_index_all_route_still_registered(self, app):
        """POST /index/all should still be registered (indexing router retained)."""
        routes = [route.path for route in app.routes]
        assert "/index/all" in routes

    def test_index_single_route_still_registered(self, app):
        """POST /index/{md5} should still be registered."""
        routes = [route.path for route in app.routes]
        assert "/index/{md5}" in routes

    def test_v1_search_route_removed(self, app):
        """POST /search should NOT be registered (V1 removed)."""
        routes = [route.path for route in app.routes]
        assert "/search" not in routes


class TestV2OpenAPISchema:
    """Tests that batch routes appear in OpenAPI schema (visible in /docs)."""

    def test_openapi_includes_batch_run(self, app):
        """The OpenAPI schema should include POST /batch/run."""
        schema = app.openapi()
        assert "/batch/run" in schema["paths"]
        assert "post" in schema["paths"]["/batch/run"]

    def test_openapi_includes_batch_status(self, app):
        """The OpenAPI schema should include GET /batch/{batch_id}/status."""
        schema = app.openapi()
        assert "/batch/{batch_id}/status" in schema["paths"]
        assert "get" in schema["paths"]["/batch/{batch_id}/status"]

    def test_openapi_includes_batches_list(self, app):
        """The OpenAPI schema should include GET /batches."""
        schema = app.openapi()
        assert "/batches" in schema["paths"]
        assert "get" in schema["paths"]["/batches"]

    def test_openapi_does_not_include_v1_search(self, app):
        """POST /search should NOT be in the OpenAPI schema (V1 removed)."""
        schema = app.openapi()
        assert "/search" not in schema["paths"]


class TestV2CORSMiddleware:
    """Tests for CORS middleware in V2 app."""

    def test_cors_still_configured(self, test_client):
        """CORS should still be configured for development."""
        response = test_client.options(
            "/batches",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert response.headers.get("access-control-allow-origin") in ("*", "http://localhost:3000")
