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


class InterbankSettings(_Section):
    """Interbank transfer gateway (``POST /interbank/transfer`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="INTERBANK_")

    timeout_seconds: float = Field(default=10.0, gt=0)
    # Extra attempts on connection errors and 502/503/504 (never after a read timeout).
    retries: int = Field(default=2, ge=0, le=5)


class SbpSettings(_Section):
    """Faster Payments System gateway (``POST /sbp/send`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="SBP_")

    timeout_seconds: float = Field(default=10.0, gt=0)
    retries: int = Field(default=2, ge=0, le=5)


class ProviderSettings(_Section):
    """Service providers (``POST /providers/{name}/pay`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="PROVIDERS_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    # Extra attempts on 5xx, timeouts and connection errors (never on 4xx).
    retries: int = Field(default=2, ge=0, le=5)


class CardProcessorSettings(_Section):
    """Card processor (``/card-processor/*`` on WireMock): issue, block, unblock."""

    model_config = SettingsConfigDict(env_prefix="CARD_PROCESSOR_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    # Extra attempts on 5xx, timeouts and connection errors (never on 4xx).
    retries: int = Field(default=2, ge=0, le=5)


class CardSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CARDS_")

    # HMAC key for cards.pan_hash. Changing it makes every stored hash unusable.
    pan_hash_key: SecretStr = Field(min_length=32)
    # Cards per account that are not reissued/closed/expired.
    max_per_account: int = Field(default=5, ge=1)
    # Limits assigned to a newly issued card (in the account currency).
    default_daily_limit: Decimal = Field(default=Decimal("100000.00"), gt=0, decimal_places=2)
    default_monthly_limit: Decimal = Field(default=Decimal("500000.00"), gt=0, decimal_places=2)
    default_per_transaction_limit: Decimal = Field(
        default=Decimal("50000.00"), gt=0, decimal_places=2
    )

    @model_validator(mode="after")
    def _check_limits(self) -> Self:
        if not (
            self.default_per_transaction_limit
            <= self.default_daily_limit
            <= self.default_monthly_limit
        ):
            raise ValueError(
                "CARDS_DEFAULT_*_LIMIT must satisfy per_transaction <= daily <= monthly"
            )
        return self


class BkiSettings(_Section):
    """Credit bureau scoring (``POST /bki/score`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="BKI_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    # Extra attempts on 5xx, timeouts and connection errors (never on 4xx).
    retries: int = Field(default=2, ge=0, le=5)


class BrokerSettings(_Section):
    """Broker (``GET /broker/quotes``, ``POST /broker/orders`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="BROKER_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    retries: int = Field(default=2, ge=0, le=5)


class CbrSettings(_Section):
    """Bank of Russia exchange rates (``GET /cbr/rates`` on WireMock)."""

    model_config = SettingsConfigDict(env_prefix="CBR_")

    timeout_seconds: float = Field(default=5.0, gt=0)
    retries: int = Field(default=2, ge=0, le=5)
    rates_cache_ttl_seconds: int = Field(default=60, ge=1)


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


class CreditSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CREDITS_")

    min_amount: Decimal = Field(default=Decimal("10000.00"), gt=0, decimal_places=2)
    max_amount: Decimal = Field(default=Decimal("5000000.00"), gt=0, decimal_places=2)
    min_term_months: int = Field(default=3, ge=1)
    max_term_months: int = Field(default=84, ge=1)
    # Approved only if the bureau says "approve" and the score is at least this.
    min_score: int = Field(default=600, ge=0)
    # Annual rate, percent; the lower one for scores >= good_score.
    base_rate: Decimal = Field(default=Decimal("21.9"), ge=0, le=100)
    good_score: int = Field(default=750, ge=0)
    good_score_rate: Decimal = Field(default=Decimal("17.9"), ge=0, le=100)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.max_amount < self.min_amount:
            raise ValueError("CREDITS_MAX_AMOUNT must be >= CREDITS_MIN_AMOUNT")
        if self.max_term_months < self.min_term_months:
            raise ValueError("CREDITS_MAX_TERM_MONTHS must be >= CREDITS_MIN_TERM_MONTHS")
        return self


class DepositSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="DEPOSITS_")

    min_amount: Decimal = Field(default=Decimal("1000.00"), gt=0, decimal_places=2)
    # The client chooses the rate within (0, max_rate].
    max_rate: Decimal = Field(default=Decimal("18"), gt=0, le=100)
    min_term_months: int = Field(default=1, ge=1)
    max_term_months: int = Field(default=60, ge=1)
    # Interest is recalculated at this rate when a deposit is closed before its end date.
    early_close_rate: Decimal = Field(default=Decimal("0.01"), ge=0, le=100)
    consumer_group: str = "elbank-deposits"
    accrual_batch_size: int = Field(default=100, ge=1, le=1000)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.max_term_months < self.min_term_months:
            raise ValueError("DEPOSITS_MAX_TERM_MONTHS must be >= DEPOSITS_MIN_TERM_MONTHS")
        return self


class StatementSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="STATEMENTS_")

    # Generated CSV files; /app/uploads/statements in docker-compose (shared by app and worker).
    dir: Path = Path("uploads/statements")
    # How long a job (and its file) is available after the request.
    job_ttl_seconds: int = Field(default=24 * 3600, ge=60)
    max_period_days: int = Field(default=366, ge=1)
    consumer_group: str = "elbank-statements"


class TransferSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="TRANSFERS_")

    # Fee of an internal transfer: percent of the amount, clamped to [min, max].
    fee_percent: Decimal = Field(default=Decimal("0.5"), ge=0, le=100)
    fee_min: Decimal = Field(default=Decimal("10.00"), ge=0, decimal_places=2)
    fee_max: Decimal = Field(default=Decimal("5000.00"), ge=0, decimal_places=2)
    # A transfer stays "pending" (and can be cancelled) this long, then it is completed.
    cancel_window_seconds: int = Field(default=60, ge=0)
    # Redis lock op_lock:* held while a request with an idempotency key is processed.
    lock_ttl_seconds: int = Field(default=30, ge=1)
    # How long a parallel request with the same key waits for the lock before 409.
    lock_wait_seconds: float = Field(default=5.0, ge=0)
    # Currency conversion between own accounts: percent of the debited amount.
    conversion_fee_percent: Decimal = Field(default=Decimal("1"), ge=0, le=100)
    # External transfer timed out: it stays "pending" (EXTERNAL_TIMEOUT) this long, then fails.
    external_timeout_fail_after_seconds: int = Field(default=5, ge=0)
    # External transfer stuck in "processing" (process died mid-request) is failed after this.
    external_stale_after_seconds: int = Field(default=120, ge=30)
    finalizer_interval_seconds: float = Field(default=5.0, gt=0)
    finalizer_batch_size: int = Field(default=100, ge=1, le=1000)

    @model_validator(mode="after")
    def _check_fee(self) -> Self:
        if self.fee_max < self.fee_min:
            raise ValueError("TRANSFERS_FEE_MAX must be >= TRANSFERS_FEE_MIN")
        return self


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
    interbank: InterbankSettings = Field(default_factory=InterbankSettings)
    sbp: SbpSettings = Field(default_factory=SbpSettings)
    cbr: CbrSettings = Field(default_factory=CbrSettings)
    providers: ProviderSettings = Field(default_factory=ProviderSettings)
    bki: BkiSettings = Field(default_factory=BkiSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    card_processor: CardProcessorSettings = Field(default_factory=CardProcessorSettings)
    cards: CardSettings = Field(default_factory=CardSettings)
    kyc: KycSettings = Field(default_factory=KycSettings)
    bank: BankSettings = Field(default_factory=BankSettings)
    accounts: AccountSettings = Field(default_factory=AccountSettings)
    credits: CreditSettings = Field(default_factory=CreditSettings)
    deposits: DepositSettings = Field(default_factory=DepositSettings)
    statements: StatementSettings = Field(default_factory=StatementSettings)
    transfers: TransferSettings = Field(default_factory=TransferSettings)
    elastic: ElasticSettings = Field(default_factory=ElasticSettings)

    readiness_timeout_seconds: float = Field(default=2.0, gt=0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
