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
    redis_url: str = "redis://127.0.0.1:6379/0"
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
    external_service_max_delay: float = Field(
        default=5.0,
        ge=0.0,
        le=30.0,
    )
    external_service_total_timeout: float = Field(
        default=15.0,
        gt=0.0,
        le=60.0,
    )
    external_service_failure_threshold: int = Field(
        default=3,
        ge=1,
        le=20,
    )
    external_service_recovery_timeout: float = Field(
        default=30.0,
        gt=0.0,
        le=300.0,
    )
    login_rate_limit: int = Field(
        default=5,
        ge=1,
        le=100,
    )
    login_rate_window_seconds: int = Field(
        default=60,
        ge=1,
        le=3600,
    )
    task_cache_ttl_seconds: int = Field(
        default=300,
        ge=1,
        le=86400,
    )
    task_negative_cache_ttl_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
    )
    task_cache_ttl_jitter_seconds: int = Field(
        default=60,
        ge=0,
        le=3600,
    )
    task_cache_lock_ttl_seconds: int = Field(
        default=5,
        ge=1,
        le=60,
    )
    task_cache_lock_wait_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
    )
    task_cache_lock_wait_seconds: float = Field(
        default=0.05,
        gt=0.0,
        le=1.0,
    )
    websocket_ticket_ttl_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()