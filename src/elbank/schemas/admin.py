"""Back-office (``/api/v1/admin``) request/response schemas."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from elbank.db.models import (
    Currency,
    OperationStatus,
    OperationType,
    TariffCode,
    UserRole,
    UserStatus,
)
from elbank.services.ledger import percent_of
from elbank.services.rates import RateSource


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Read(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PageRead[T](BaseModel):
    items: list[T]
    page: int
    size: int
    total: int = Field(description="Total number of matching items")


# ---------- users ----------


class UserAdminRead(_Read):
    id: uuid.UUID
    phone: str
    email: str | None
    full_name: str | None
    status: UserStatus
    role: UserRole
    is_2fa_enabled: bool
    created_at: datetime
    updated_at: datetime


class BlockUserRequest(_Request):
    reason: (
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
        | None
    ) = None


class ChangeRoleRequest(_Request):
    role: UserRole


# ---------- tariffs ----------

TariffAmount = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=2)]


class FeeRule(_Request):
    """``percent`` of the amount rounded half-up to kopecks, clamped to [min, max]."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    percent: Annotated[Decimal, Field(ge=0, le=100, decimal_places=4, examples=["0.5"])]
    min: TariffAmount = Decimal(0)
    max: TariffAmount | None = Field(default=None, description="null: no upper bound")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.max is not None and self.max < self.min:
            raise ValueError("max must be >= min")
        return self

    def fee(self, amount: Decimal) -> Decimal:
        fee = max(percent_of(amount, self.percent), self.min)
        return min(fee, self.max) if self.max is not None else fee


class TariffRead(BaseModel):
    code: TariffCode
    rule: FeeRule
    is_default: bool = Field(description="true: not set in the back office, env default applies")
    updated_at: datetime | None
    updated_by_id: uuid.UUID | None


class TariffsUpdateRequest(_Request):
    """Only the listed tariffs change; ``null`` resets a tariff to the env default."""

    tariffs: Annotated[dict[TariffCode, FeeRule | None], Field(min_length=1)] = Field(
        examples=[
            {
                "internal_transfer": {"percent": "0.3", "min": "5.00", "max": "3000.00"},
                "conversion": None,
            }
        ]
    )


# ---------- rates ----------

Rate = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=8)]


class EffectiveRateRead(BaseModel):
    base: Currency
    quote: Currency
    rate: Decimal = Field(description="Units of quote currency for one unit of base currency")
    date: datetime
    source: RateSource


class RateOverrideRead(_Read):
    base: Currency
    quote: Currency
    rate: Decimal
    updated_at: datetime
    updated_by_id: uuid.UUID | None


class RatesRead(BaseModel):
    rates: list[EffectiveRateRead] = Field(description="Every currency pair, as conversions see it")
    overrides: list[RateOverrideRead]


class RateOverrideRequest(_Request):
    rate: Rate = Field(examples=["92.50000000"])


# ---------- audit ----------


class AuditRecordRead(BaseModel):
    id: str
    timestamp: datetime
    action: str
    actor_id: uuid.UUID | None = Field(description="Who did it; null: the system")
    user_id: uuid.UUID | None = Field(description="Whose account/data it concerns")
    entity: str | None
    entity_id: str | None
    details: dict[str, Any]
    correlation_id: str | None
    service: str | None


# ---------- reconciliation ----------


class TotalsRow(BaseModel):
    type: OperationType
    status: OperationStatus
    currency: str
    count: int
    amount: Decimal


class CheckResult(BaseModel):
    description: str
    count: int
    items: list[dict[str, Any]] = Field(description="First items (up to the configured limit)")


class ReconciliationStatus(StrEnum):
    OK = "ok"
    MISMATCHES = "mismatches"


class ReconciliationReport(BaseModel):
    run_id: uuid.UUID
    status: ReconciliationStatus
    date_from: datetime
    date_to: datetime
    requested_by: uuid.UUID
    requested_at: datetime
    generated_at: datetime
    totals: list[TotalsRow]
    checks: dict[str, CheckResult]


class ReconciliationRunRequest(_Request):
    date_from: AwareDatetime = Field(description="Inclusive", examples=["2026-11-01T00:00:00Z"])
    date_to: AwareDatetime = Field(description="Exclusive", examples=["2026-12-01T00:00:00Z"])


class ReconciliationRunRead(BaseModel):
    run_id: uuid.UUID
    date_from: datetime
    date_to: datetime
    requested_at: datetime
    totals: list[TotalsRow] = Field(description="Snapshot at request time")
    report_url: str = Field(description="Available once the worker has written the report")


# ---------- fraud rules ----------

RuleName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
RuleExpression = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
    Field(examples=["amount > 300000 and hour < 6"]),
]


class FraudRuleRead(_Read):
    id: uuid.UUID
    name: str
    expression: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


class FraudRuleCreate(_Request):
    name: RuleName
    expression: RuleExpression
    enabled: bool = True


class FraudRuleUpdate(_Request):
    """Partial update: only sent fields change."""

    name: RuleName | None = None
    expression: RuleExpression | None = None
    enabled: bool | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("at least one field must be provided")
        if any(getattr(self, f) is None for f in self.model_fields_set):
            raise ValueError("fields cannot be null")
        return self
