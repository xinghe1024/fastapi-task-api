from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    external_service_url: str = "https://example.com"
    database_url: str = "sqlite:///./test.db"
    jwt_secret_key: SecretStr
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = Field(
        default=30,
        ge=1,
        le=1440,
    )
    cors_allowed_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:5173",
        ],
    )
    external_service_max_attempts: int = Field(
        default=3,
        ge=1,
        le=5,
    )
    external_service_base_delay: float = Field(
        default=0.5,
        ge=0.0,
        le=10.0,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()