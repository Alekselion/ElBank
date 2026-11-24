"""Application settings loaded from environment variables and the ``.env`` file.

Settings are grouped into nested sections (``settings.postgres.host``, ...).
Each section reads its own env prefix, so the variable names stay flat
(``POSTGRES_HOST``, ``JWT_SECRET``, ...) — see ``.env.example``.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["local", "dev", "prod"]
type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class _Section(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


class AppSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="APP_")

    name: str = "elbank"
    env: Environment = "local"
    host: str = "0.0.0.0"  # noqa: S104 - bind address inside a container
    port: int = Field(default=8000, ge=1, le=65535)
    debug: bool = False


class LogSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: LogLevel = "INFO"
    # true: one JSON object per line on stdout; false: human-readable console (local dev).
    # Logs shipped to Logstash are always JSON.
    json_format: bool = Field(default=True, validation_alias="LOG_JSON")


class PostgresSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")

    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    user: str
    password: SecretStr
    db: str
    pool_size: int = Field(default=10, ge=1)
    max_overflow: int = Field(default=20, ge=0)
    echo: bool = False

    @property
    def dsn(self) -> str:
        """SQLAlchemy async DSN (``postgresql+asyncpg://``)."""
        return str(
            PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.user,
                password=self.password.get_secret_value(),
                host=self.host,
                port=self.port,
                path=self.db,
            )
        )


class RedisSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: RedisDsn


class KafkaSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str
    client_id: str = "elbank"

    @property
    def servers(self) -> list[str]:
        return [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]


class JwtSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="JWT_")

    secret: SecretStr = Field(min_length=32)
    algorithm: Literal["HS256", "HS384", "HS512"] = "HS256"
    ttl_access_minutes: int = Field(default=15, ge=1)
    ttl_refresh_days: int = Field(default=7, ge=1)


class LogstashSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="LOGSTASH_")

    enabled: bool = True
    host: str = "localhost"
    port: int = Field(default=5000, ge=1, le=65535)


class WiremockSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="WIREMOCK_")

    base_url: str = "http://localhost:8081"


class ElasticSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="ELASTIC_")

    url: str = "http://localhost:9200"


class Settings(_Section):
    app: AppSettings = Field(default_factory=AppSettings)
    log: LogSettings = Field(default_factory=LogSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    jwt: JwtSettings = Field(default_factory=JwtSettings)
    logstash: LogstashSettings = Field(default_factory=LogstashSettings)
    wiremock: WiremockSettings = Field(default_factory=WiremockSettings)
    elastic: ElasticSettings = Field(default_factory=ElasticSettings)

    readiness_timeout_seconds: float = Field(default=2.0, gt=0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
