"""Application settings loaded from environment variables."""

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REQUIRED_SETTINGS_STATE: dict[str, bool] = {
    "prefect_api_url": True,
    "prefect_api_auth_string": True,
}


class Settings(BaseSettings):
    """Centralized application configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_port: int = 9200
    app_host: str = "0.0.0.0"
    log_level: str = "info"

    allowed_origins: str = "*"

    prefect_api_url: str
    prefect_api_auth_string: str

    workflow_settings_file: str | None = None  # can be added later by POST /workflow/

    request_timeout_seconds: int = 30

    @model_validator(mode="after")
    def validate_required_env_vars(self) -> "Settings":
        """Validate required settings configured per environment variable."""
        required_fields = [name for name, is_required in REQUIRED_SETTINGS_STATE.items() if is_required]
        missing = [name for name in required_fields if not getattr(self, name)]
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(f"Missing required settings: {missing_list}")
        return self

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
