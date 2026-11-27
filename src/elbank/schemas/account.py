"""Account, limits and statement request/response schemas.

Money (``Numeric(18, 2)`` in the DB) is a ``Decimal`` in Python and always a
*string* in JSON (``"1500.00"``) so clients never lose precision to floats.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    WithJsonSchema,
    model_validator,
)

from elbank.db.models import AccountStatus, AccountType, Currency


def _format_money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


_MONEY_JSON_SCHEMA = WithJsonSchema(
    {"type": "string", "pattern": r"^\d{1,16}(\.\d{1,2})?$", "examples": ["1500.00"]}
)

# Output money: always serialized as a string with 2 decimals.
Money = Annotated[
    Decimal,
    PlainSerializer(_format_money, return_type=str, when_used="json"),
    _MONEY_JSON_SCHEMA,
]
# Input money: a positive amount, accepted as a JSON string (preferred) or number.
PositiveMoney = Annotated[
    Decimal,
    Field(gt=0, max_digits=18, decimal_places=2),
    PlainSerializer(_format_money, return_type=str, when_used="json"),
    _MONEY_JSON_SCHEMA,
]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------- accounts ----------


class AccountOpenRequest(_Request):
    type: AccountType
    currency: Currency

    @model_validator(mode="after")
    def _check_currency(self) -> Self:
        if self.type is AccountType.FOREIGN and self.currency is Currency.RUB:
            raise ValueError("a foreign currency account cannot be opened in RUB")
        if self.type is not AccountType.FOREIGN and self.currency is not Currency.RUB:
            raise ValueError(f"a {self.type.value} account is opened in RUB only")
        return self


class AccountRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    number: str = Field(description="20-digit account number", examples=["40817810500010123456"])
    type: AccountType
    currency: Currency
    balance: Money
    status: AccountStatus
    created_at: datetime
    updated_at: datetime


class AccountRequisites(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    recipient: str | None = Field(description="Account holder's full name")
    account_number: str
    currency: Currency
    bank_name: str
    bic: str
    correspondent_account: str
    bank_inn: str
    bank_kpp: str
    swift: str
    iban: str = Field(description="Pseudo IBAN, for display only")


# ---------- limits ----------


class AccountLimitsRead(BaseModel):
    account_id: uuid.UUID
    currency: Currency
    daily_limit: Money
    monthly_limit: Money


class AccountLimitsUpdate(_Request):
    daily_limit: PositiveMoney
    monthly_limit: PositiveMoney

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.monthly_limit < self.daily_limit:
            raise ValueError("monthly_limit must be greater than or equal to daily_limit")
        return self


# ---------- statements ----------


class StatementJobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"


class StatementJobAccepted(BaseModel):
    job_id: uuid.UUID
    status: StatementJobStatus
    status_url: str = Field(description="Poll this URL for the job status")


class StatementJobRead(BaseModel):
    job_id: uuid.UUID
    account_id: uuid.UUID
    date_from: date
    date_to: date
    status: StatementJobStatus
    created_at: datetime
    completed_at: datetime | None
    error: str | None
    download_url: str | None = Field(description="CSV file URL; set when status is ready")
