"""Application settings loaded from environment variables and the ``.env`` file.

Settings are grouped into nested sections (``settings.postgres.host``, ...).
Each section reads its own env prefix, so the variable names stay flat
(``POSTGRES_HOST``, ``JWT_SECRET``, ...) — see ``.env.example``.
"""

from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr, model_validator
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
    ttl_refresh_days: int = Field(default=30, ge=1)
    # Short-lived proof that an OTP was verified (reset password, 2FA, login with 2FA).
    ttl_otp_minutes: int = Field(default=5, ge=1)


class OtpSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="OTP_")

    ttl_seconds: int = Field(default=300, ge=30)
    # Wrong codes allowed per issued OTP; the code is burned after that.
    max_attempts: int = Field(default=5, ge=1)
    # Minimal interval between two OTPs for the same phone and purpose (anti SMS-bombing).
    resend_cooldown_seconds: int = Field(default=60, ge=0)


class LoginRateLimitSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="LOGIN_RATE_LIMIT_")

    max_attempts: int = Field(default=5, ge=1)
    window_seconds: int = Field(default=900, ge=1)


class LogstashSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="LOGSTASH_")

    enabled: bool = True
    host: str = "localhost"
    port: int = Field(default=5000, ge=1, le=65535)


class WiremockSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="WIREMOCK_")

    base_url: str = "http://localhost:8081"
    timeout_seconds: float = Field(default=5.0, gt=0)


class GosuslugiSettings(_Section):
    """Passport check at Gosuslugi (served by WireMock under ``WIREMOCK_BASE_URL``)."""

    model_config = SettingsConfigDict(env_prefix="GOSUSLUGI_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    retries: int = Field(default=2, ge=0, le=5)  # extra attempts after the first one


class KycSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="KYC_")

    # Passport scans; /app/uploads (a named volume) in docker-compose.
    uploads_dir: Path = Path("uploads")
    max_upload_bytes: int = Field(default=5 * 1024 * 1024, ge=1024)


class BankSettings(_Section):
    """Requisites of the emulated bank itself (pseudo values, not a real bank)."""

    model_config = SettingsConfigDict(env_prefix="BANK_")

    name: str = "АО «ЭлБанк»"  # noqa: RUF001 - Cyrillic on purpose
    bic: str = Field(default="044525999", pattern=r"^04\d{7}$")
    inn: str = Field(default="7700000000", pattern=r"^\d{10}$")
    kpp: str = Field(default="770001001", pattern=r"^\d{9}$")
    swift: str = Field(default="ELBKRUMM", pattern=r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$")
    # Positions 10-13 of every account number (bank branch code).
    branch_code: str = Field(default="0001", pattern=r"^\d{4}$")


class AccountSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="ACCOUNTS_")

    list_cache_ttl_seconds: int = Field(default=30, ge=1)
    # Limits assigned to a newly opened account (in the account currency).
    default_daily_limit: Decimal = Field(default=Decimal("100000.00"), gt=0, decimal_places=2)
    default_monthly_limit: Decimal = Field(default=Decimal("1000000.00"), gt=0, decimal_places=2)

    @model_validator(mode="after")
    def _check_limits(self) -> Self:
        if self.default_monthly_limit < self.default_daily_limit:
            raise ValueError(
                "ACCOUNTS_DEFAULT_MONTHLY_LIMIT must be >= ACCOUNTS_DEFAULT_DAILY_LIMIT"
            )
        return self


class StatementSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="STATEMENTS_")

    # Generated CSV files; /app/uploads/statements in docker-compose (shared by app and worker).
    dir: Path = Path("uploads/statements")
    # How long a job (and its file) is available after the request.
    job_ttl_seconds: int = Field(default=24 * 3600, ge=60)
    max_period_days: int = Field(default=366, ge=1)
    consumer_group: str = "elbank-statements"


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
    otp: OtpSettings = Field(default_factory=OtpSettings)
    login_rate_limit: LoginRateLimitSettings = Field(default_factory=LoginRateLimitSettings)
    logstash: LogstashSettings = Field(default_factory=LogstashSettings)
    wiremock: WiremockSettings = Field(default_factory=WiremockSettings)
    gosuslugi: GosuslugiSettings = Field(default_factory=GosuslugiSettings)
    kyc: KycSettings = Field(default_factory=KycSettings)
    bank: BankSettings = Field(default_factory=BankSettings)
    accounts: AccountSettings = Field(default_factory=AccountSettings)
    statements: StatementSettings = Field(default_factory=StatementSettings)
    elastic: ElasticSettings = Field(default_factory=ElasticSettings)

    readiness_timeout_seconds: float = Field(default=2.0, gt=0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
