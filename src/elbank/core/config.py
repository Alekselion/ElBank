"""Application settings loaded from environment variables and the ``.env`` file."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["local", "dev", "prod"]
type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Application ---
    app_name: str = "elbank"
    app_env: Environment = "local"
    app_host: str = "0.0.0.0"  # noqa: S104 - bind address inside a container
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_debug: bool = False
    log_level: LogLevel = "INFO"
    log_json: bool = False

    # --- PostgreSQL ---
    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_user: str
    postgres_password: SecretStr
    postgres_db: str
    postgres_pool_size: int = Field(default=10, ge=1)
    postgres_max_overflow: int = Field(default=20, ge=0)
    postgres_echo: bool = False

    # --- Redis ---
    redis_url: RedisDsn

    # --- Kafka ---
    kafka_bootstrap_servers: str
    kafka_client_id: str = "elbank"

    # --- JWT ---
    jwt_secret: SecretStr = Field(min_length=32)
    jwt_algorithm: Literal["HS256", "HS384", "HS512"] = "HS256"
    jwt_ttl_access_minutes: int = Field(default=15, ge=1)
    jwt_ttl_refresh_days: int = Field(default=7, ge=1)

    # --- Logstash ---
    logstash_enabled: bool = True
    logstash_host: str = "localhost"
    logstash_port: int = Field(default=5000, ge=1, le=65535)

    # --- External services ---
    wiremock_base_url: str = "http://localhost:8080"
    elastic_url: str = "http://localhost:9200"

    # --- Health checks ---
    readiness_timeout_seconds: float = Field(default=2.0, gt=0)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        """SQLAlchemy async DSN (``postgresql+asyncpg://``)."""
        dsn = PostgresDsn.build(
            scheme="postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            path=self.postgres_db,
        )
        return str(dsn)

    @property
    def kafka_servers(self) -> list[str]:
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",") if s.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
