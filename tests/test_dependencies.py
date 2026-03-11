"""Tests for app/dependencies.py — Dependency injection functions.

Tests cover:
- get_db() yields a SQLAlchemy session and closes it
- get_search_client() returns an Azure SearchClient
- get_settings() returns a cached Settings instance
"""

from unittest.mock import MagicMock, patch


class TestGetDb:
    """Tests for the get_db() dependency."""

    def test_get_db_yields_session(self):
        """get_db() should yield a SQLAlchemy Session."""
        from app.dependencies import get_db

        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.dependencies.get_session_factory", return_value=mock_session_factory):
            gen = get_db(engine=mock_engine)
            session = next(gen)
            assert session is mock_session

    def test_get_db_closes_session_after_use(self):
        """get_db() should close the session when the generator finishes."""
        from app.dependencies import get_db

        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.dependencies.get_session_factory", return_value=mock_session_factory):
            gen = get_db(engine=mock_engine)
            next(gen)
            try:
                next(gen)
            except StopIteration:
                pass
            mock_session.close.assert_called_once()

    def test_get_db_closes_session_on_exception(self):
        """get_db() should close the session even if the caller raises."""
        from app.dependencies import get_db

        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_session_factory = MagicMock(return_value=mock_session)

        with patch("app.dependencies.get_session_factory", return_value=mock_session_factory):
            gen = get_db(engine=mock_engine)
            next(gen)
            try:
                gen.throw(RuntimeError("test error"))
            except RuntimeError:
                pass
            mock_session.close.assert_called_once()


class TestGetSearchClient:
    """Tests for the get_search_client() dependency."""

    def test_get_search_client_returns_search_client(self):
        """get_search_client() should return an Azure SearchClient instance."""
        from app.dependencies import get_search_client

        mock_settings = MagicMock()
        mock_settings.AZURE_SEARCH_ENDPOINT = "https://test.search.windows.net"
        mock_settings.AZURE_SEARCH_KEY = "test-key"
        mock_settings.AZURE_SEARCH_INDEX = "test-index"

        mock_client = MagicMock()

        with patch("app.dependencies.get_settings", return_value=mock_settings), \
             patch("app.dependencies.SearchClient", return_value=mock_client) as mock_cls, \
             patch("app.dependencies.AzureKeyCredential") as mock_cred_cls:
            mock_cred = MagicMock()
            mock_cred_cls.return_value = mock_cred

            client = get_search_client()

            assert client is mock_client
            mock_cred_cls.assert_called_once_with("test-key")
            mock_cls.assert_called_once_with(
                endpoint="https://test.search.windows.net",
                index_name="test-index",
                credential=mock_cred,
            )

    def test_get_search_client_calls_get_settings(self):
        """get_search_client() should call get_settings() to load config."""
        from app.dependencies import get_search_client

        mock_settings = MagicMock()
        mock_settings.AZURE_SEARCH_ENDPOINT = "https://test.search.windows.net"
        mock_settings.AZURE_SEARCH_KEY = "test-key"
        mock_settings.AZURE_SEARCH_INDEX = "test-index"

        with patch("app.dependencies.get_settings", return_value=mock_settings) as mock_get, \
             patch("app.dependencies.SearchClient") as mock_cls, \
             patch("app.dependencies.AzureKeyCredential"):
            get_search_client()
            mock_get.assert_called_once()


class TestGetSettings:
    """Tests for the get_settings() dependency."""

    def test_get_settings_returns_settings_instance(self):
        """get_settings() should return a Settings instance."""
        from app.dependencies import get_settings

        mock_settings = MagicMock()
        with patch("app.dependencies.Settings", return_value=mock_settings):
            result = get_settings()
            assert result is mock_settings

    def test_get_settings_caches_result(self):
        """get_settings() should return the same cached instance on repeated calls."""
        from app.dependencies import get_settings

        mock_settings = MagicMock()
        with patch("app.dependencies.Settings", return_value=mock_settings):
            # Clear any cached result first
            get_settings.cache_clear()
            result1 = get_settings()
            result2 = get_settings()
            assert result1 is result2
            get_settings.cache_clear()
