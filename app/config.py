"""
app/config.py — Application settings loaded from environment variables.

Uses pydantic-settings BaseSettings so all fields are read from env vars
automatically. Use get_settings() for FastAPI dependency injection.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    DATABASE_URL: str
    AZURE_SEARCH_ENDPOINT: str
    AZURE_SEARCH_KEY: str
    AZURE_SEARCH_INDEX: str = "breach-file-index"
    AZURE_SEARCH_INDEX_V3: str = "breach-file-index-v3"
    STRATEGIES_FILE: str = "strategies.yaml"


def get_settings() -> Settings:
    """Return a Settings instance for FastAPI dependency injection."""
    return Settings()
