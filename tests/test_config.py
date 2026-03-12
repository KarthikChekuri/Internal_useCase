"""
Tests for app/config.py — Settings loaded via pydantic-settings BaseSettings.

V2 update: FILE_BASE_PATH and CASE_NAME have been removed. STRATEGIES_FILE
has been added with a default of "strategies.yaml".
"""
import os
import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Helpers — minimal required env vars for V2 Settings
# ---------------------------------------------------------------------------

REQUIRED_ENV = {
    "DATABASE_URL": "mssql+pyodbc://user:pass@localhost/db",
    "AZURE_SEARCH_ENDPOINT": "https://example.search.windows.net",
    "AZURE_SEARCH_KEY": "test-key-abc123",
}


def _set_required(monkeypatch):
    """Set the minimum required env vars so Settings() can be instantiated."""
    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# Field presence tests
# ---------------------------------------------------------------------------

class TestSettingsFields:
    """Settings class must expose all required V2 configuration fields."""

    def test_settings_has_database_url_field(self, monkeypatch):
        """Settings must have a DATABASE_URL field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.DATABASE_URL, str)
        assert settings.DATABASE_URL == "mssql+pyodbc://user:pass@localhost/db"

    def test_settings_has_azure_search_endpoint_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_ENDPOINT field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_ENDPOINT, str)
        assert settings.AZURE_SEARCH_ENDPOINT == "https://example.search.windows.net"

    def test_settings_has_azure_search_key_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_KEY field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_KEY, str)
        assert settings.AZURE_SEARCH_KEY == "test-key-abc123"

    def test_settings_has_azure_search_index_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_INDEX field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.AZURE_SEARCH_INDEX, str)

    def test_settings_has_strategies_file_field(self, monkeypatch):
        """Settings must have a STRATEGIES_FILE field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert isinstance(settings.STRATEGIES_FILE, str)

    def test_settings_does_not_have_file_base_path(self, monkeypatch):
        """FILE_BASE_PATH has been removed from Settings in V2."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert not hasattr(settings, "FILE_BASE_PATH"), (
            "FILE_BASE_PATH should have been removed from Settings in V2"
        )

    def test_settings_does_not_have_case_name(self, monkeypatch):
        """CASE_NAME has been removed from Settings in V2."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert not hasattr(settings, "CASE_NAME"), (
            "CASE_NAME should have been removed from Settings in V2"
        )


# ---------------------------------------------------------------------------
# Default value tests
# ---------------------------------------------------------------------------

class TestSettingsDefaults:
    """Settings class must provide sensible defaults where appropriate."""

    def test_azure_search_index_defaults_to_breach_file_index(self, monkeypatch):
        """AZURE_SEARCH_INDEX must default to 'breach-file-index' when not set."""
        _set_required(monkeypatch)
        monkeypatch.delenv("AZURE_SEARCH_INDEX", raising=False)
        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX == "breach-file-index"

    def test_azure_search_index_can_be_overridden(self, monkeypatch):
        """AZURE_SEARCH_INDEX default can be overridden by environment variable."""
        _set_required(monkeypatch)
        monkeypatch.setenv("AZURE_SEARCH_INDEX", "custom-index-name")
        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX == "custom-index-name"

    def test_strategies_file_defaults_to_strategies_yaml(self, monkeypatch):
        """STRATEGIES_FILE must default to 'strategies.yaml' when not set."""
        _set_required(monkeypatch)
        monkeypatch.delenv("STRATEGIES_FILE", raising=False)
        from app.config import Settings
        settings = Settings()
        assert settings.STRATEGIES_FILE == "strategies.yaml"

    def test_strategies_file_can_be_overridden(self, monkeypatch):
        """STRATEGIES_FILE default can be overridden by environment variable."""
        _set_required(monkeypatch)
        monkeypatch.setenv("STRATEGIES_FILE", "custom_strategies.yaml")
        from app.config import Settings
        settings = Settings()
        assert settings.STRATEGIES_FILE == "custom_strategies.yaml"


# ---------------------------------------------------------------------------
# Environment loading tests
# ---------------------------------------------------------------------------

class TestSettingsLoadsFromEnvironment:
    """Settings class must load all values from environment variables."""

    def test_settings_loads_all_required_fields_from_env(self, monkeypatch):
        """Given all required env vars are set, Settings must load them correctly."""
        monkeypatch.setenv("DATABASE_URL", "mssql+pyodbc://sa:secret@localhost/BreachDB")
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://mysearch.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "abc-def-ghi-jkl")
        monkeypatch.setenv("AZURE_SEARCH_INDEX", "my-breach-index")
        monkeypatch.setenv("STRATEGIES_FILE", "my_strategies.yaml")

        from app.config import Settings
        settings = Settings()

        assert settings.DATABASE_URL == "mssql+pyodbc://sa:secret@localhost/BreachDB"
        assert settings.AZURE_SEARCH_ENDPOINT == "https://mysearch.search.windows.net"
        assert settings.AZURE_SEARCH_KEY == "abc-def-ghi-jkl"
        assert settings.AZURE_SEARCH_INDEX == "my-breach-index"
        assert settings.STRATEGIES_FILE == "my_strategies.yaml"

    def test_settings_raises_when_required_fields_missing(self, monkeypatch):
        """Settings must raise ValidationError when required fields are absent."""
        for key in ["DATABASE_URL", "AZURE_SEARCH_ENDPOINT", "AZURE_SEARCH_KEY",
                    "AZURE_SEARCH_INDEX", "STRATEGIES_FILE"]:
            monkeypatch.delenv(key, raising=False)

        from app.config import Settings
        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_settings_is_importable_from_app_config(self):
        """The Settings class must be importable from app.config."""
        from app.config import Settings
        assert Settings is not None

    def test_get_settings_function_exists(self, monkeypatch):
        """app.config must expose a get_settings() callable for dependency injection."""
        _set_required(monkeypatch)
        from app.config import get_settings, Settings
        assert callable(get_settings)
        settings = get_settings()
        assert isinstance(settings, Settings)


# ---------------------------------------------------------------------------
# V3 Config tests
# ---------------------------------------------------------------------------

class TestSettingsV3:
    """Settings class must expose V3 configuration fields."""

    def test_settings_has_azure_search_index_v3_field(self, monkeypatch):
        """Settings must have an AZURE_SEARCH_INDEX_V3 field of type str."""
        _set_required(monkeypatch)
        from app.config import Settings
        settings = Settings()
        assert hasattr(settings, "AZURE_SEARCH_INDEX_V3")
        assert isinstance(settings.AZURE_SEARCH_INDEX_V3, str)

    def test_azure_search_index_v3_defaults_to_breach_file_index_v3(self, monkeypatch):
        """AZURE_SEARCH_INDEX_V3 must default to 'breach-file-index-v3' when not set."""
        _set_required(monkeypatch)
        monkeypatch.delenv("AZURE_SEARCH_INDEX_V3", raising=False)
        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX_V3 == "breach-file-index-v3"

    def test_azure_search_index_v3_can_be_overridden(self, monkeypatch):
        """AZURE_SEARCH_INDEX_V3 default can be overridden by environment variable."""
        _set_required(monkeypatch)
        monkeypatch.setenv("AZURE_SEARCH_INDEX_V3", "custom-v3-index")
        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX_V3 == "custom-v3-index"

    def test_v2_and_v3_indexes_are_different_defaults(self, monkeypatch):
        """V2 and V3 index names must have different default values."""
        _set_required(monkeypatch)
        monkeypatch.delenv("AZURE_SEARCH_INDEX", raising=False)
        monkeypatch.delenv("AZURE_SEARCH_INDEX_V3", raising=False)
        from app.config import Settings
        settings = Settings()
        assert settings.AZURE_SEARCH_INDEX != settings.AZURE_SEARCH_INDEX_V3
