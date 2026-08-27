"""Application settings loaded from environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralized application configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_port: int = 9200
    app_host: str = "0.0.0.0"  # noqa: S104 - intentional for Docker
    log_level: str = "info"

    allowed_origins: str = "*"

    prefect_api_url: str
    prefect_api_auth_string: str

    minio_host: str
    minio_port: str
    minio_access_key: str
    minio_secret: str

    workflow_settings_file: str | None = None  # can be added later by POST /workflow/

    request_timeout_seconds: int = 30

    @property
    def cors_origins(self) -> list[str]:
        """Return CORS origins from comma-separated env string."""
        origins = [origin.strip() for origin in self.allowed_origins.split(",") if origin.strip()]
        return origins or ["*"]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
