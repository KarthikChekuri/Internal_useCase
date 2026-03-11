"""
Tests for app/config.py — Settings loaded via pydantic-settings BaseSettings.

TDD: These tests are written BEFORE the production code. They define the
expected contract for the Settings class.
"""
import os
import pytest
from pydantic import ValidationError


class TestSettingsFields:
    """Settings class must expose all 6 required configuration fields."""

    def test_settings_has_database_url_field(self, monkeypatch):
        """Settings must have a DATABASE_URL field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.DATABASE_URL, str)
        assert settings.DATABASE_URL == "mssql+pyodbc://user:pass@localhost/db"

    def test_settings_has_azure_search_endpoint_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_ENDPOINT field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_ENDPOINT, str)
        assert settings.AZURE_SEARCH_ENDPOINT == "https://example.search.windows.net"

    def test_settings_has_azure_search_key_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_KEY field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_KEY, str)
        assert settings.AZURE_SEARCH_KEY == "test-key-abc123"

    def test_settings_has_azure_search_index_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_INDEX field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_INDEX, str)

    def test_settings_has_file_base_path_field(self, monkeypatch):
        """Settings must have a FILE_BASE_PATH field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.FILE_BASE_PATH, str)
        assert settings.FILE_BASE_PATH == "/data/breach"

    def test_settings_has_case_name_field(self, monkeypatch):
        """Settings must have a CASE_NAME field of type str."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.CASE_NAME, str)
        assert settings.CASE_NAME == "test-case-001"


class TestSettingsDefaults:
    """Settings class must provide sensible defaults where appropriate."""

    def test_azure_search_index_defaults_to_breach_file_index(self, monkeypatch):
        """AZURE_SEARCH_INDEX must default to 'breach-file-index' when not set."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")
        # Explicitly unset AZURE_SEARCH_INDEX so default kicks in
        monkeypatch.delenv("AZURE_SEARCH_INDEX", raising=False)

        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX == "breach-file-index"

    def test_azure_search_index_can_be_overridden(self, monkeypatch):
        """AZURE_SEARCH_INDEX default can be overridden by environment variable."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("AZURE_SEARCH_INDEX", "custom-index-name")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX == "custom-index-name"


class TestSettingsLoadsFromEnvironment:
    """Settings class must load all values from environment variables."""

    def test_settings_loads_all_six_fields_from_env(self, monkeypatch):
        """Given all 6 env vars are set, Settings must load all of them correctly."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://sa:secret@localhost/BreachDB")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://mysearch.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "abc-def-ghi-jkl")
        monkeypatch.setenv("AZURE_SEARCH_INDEX", "my-breach-index")
        monkeypatch.setenv("FILE_BASE_PATH", "C:/data/breach_files")
        monkeypatch.setenv("CASE_NAME", "Case-2024-001")

        from app.config import Settings
        settings = Settings()

        assert settings.DATABASE_URL == "mssql+pyodbc://sa:secret@localhost/BreachDB"
        assert settings.AZURE_SEARCH_ENDPOINT == "https://mysearch.search.windows.net"
        assert settings.AZURE_SEARCH_KEY == "abc-def-ghi-jkl"
        assert settings.AZURE_SEARCH_INDEX == "my-breach-index"
        assert settings.FILE_BASE_PATH == "C:/data/breach_files"
        assert settings.CASE_NAME == "Case-2024-001"

    def test_settings_raises_when_required_fields_missing(self, monkeypatch):
        """Settings must raise ValidationError when required fields are absent."""
        # Clear all relevant env vars
        for key in ["DATABASE_URL", "AZURE_SEARCH_ENDPOINT", "AZURE_SEARCH_KEY",
                    "AZURE_SEARCH_INDEX", "FILE_BASE_PATH", "CASE_NAME"]:
            monkeypatch.delenv(key, raising=False)

        from app.config import Settings
        with pytest.raises(ValidationError):
            # Override _env_file to prevent reading .env from disk
            Settings(_env_file=None)

    def test_settings_is_importable_from_app_config(self):
        """The Settings class must be importable from app.config."""
        from app.config import Settings
        assert Settings is not None

    def test_get_settings_function_exists(self, monkeypatch):
        """app.config must expose a get_settings() callable for dependency injection."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://user:pass@localhost/db")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://example.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-key-abc123")
        monkeypatch.setenv("FILE_BASE_PATH", "/data/breach")
        monkeypatch.setenv("CASE_NAME", "test-case-001")

        from app.config import get_settings, Settings
        assert callable(get_settings)
        settings = get_settings()
        assert isinstance(settings, Settings)
