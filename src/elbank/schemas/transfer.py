"""Transfer, conversion and exchange rate schemas (money as strings, see ``schemas.account``)."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
    WithJsonSchema,
    model_validator,
)

from elbank.db.models import Currency, OperationStatus
from elbank.schemas.account import Money, PositiveMoney

IdempotencyKey = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9._:-]{8,64}$"),
    Field(
        description="Client-generated unique key (e.g. a UUID); a retry with the same key "
        "returns the operation created by the first request",
        examples=["5f0c7a1e-3c39-4a4e-9d43-2a0f1c1a9b11"],
    ),
]
Description = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
# Exchange rate: string in JSON, 8 decimals.
Rate = Annotated[
    Decimal,
    PlainSerializer(lambda v: format(v, "f"), return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "examples": ["0.01243781"]}),
]


class TransferChannel(StrEnum):
    INTERNAL = "internal"
    EXTERNAL = "external"
    SBP = "sbp"
    CONVERSION = "conversion"


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class InternalTransferRequest(_Request):
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: PositiveMoney
    currency: Currency
    description: Description | None = None
    idempotency_key: IdempotencyKey

    @model_validator(mode="after")
    def _check_accounts(self) -> Self:
        if self.from_account_id == self.to_account_id:
            raise ValueError("from_account_id and to_account_id must differ")
        return self


class ExternalTransferRequest(_Request):
    """Transfer in roubles to an account in another bank."""

    from_account_id: uuid.UUID
    bic: Annotated[str, StringConstraints(pattern=r"^04\d{7}$"), Field(examples=["044525225"])]
    account_number: Annotated[
        str, StringConstraints(pattern=r"^\d{20}$"), Field(examples=["40817810238000000001"])
    ]
    recipient_name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
        Field(examples=["Петров Пётр Петрович"]),
    ]
    amount: PositiveMoney
    description: Description | None = None
    idempotency_key: IdempotencyKey


class SbpTransferRequest(_Request):
    """Transfer in roubles by the recipient's phone (key: ``Idempotency-Key`` header)."""

    from_account_id: uuid.UUID
    phone: Annotated[
        str,
        StringConstraints(strip_whitespace=True, pattern=r"^\+7\d{10}$"),
        Field(description="Russian phone, E.164", examples=["+79991234567"]),
    ]
    amount: PositiveMoney
    description: Description | None = None


class ConversionRequest(_Request):
    """Exchange between own accounts in different currencies; amount in the source currency."""

    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: PositiveMoney
    idempotency_key: IdempotencyKey

    @model_validator(mode="after")
    def _check_accounts(self) -> Self:
        if self.from_account_id == self.to_account_id:
            raise ValueError("from_account_id and to_account_id must differ")
        return self


class TransferRead(BaseModel):
    operation_id: uuid.UUID
    type: TransferChannel
    status: OperationStatus
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID | None = Field(description="Recipient account in this bank")
    counterparty: str | None = Field(
        description="Recipient account number (internal, external) or phone (SBP)"
    )
    amount: Money
    fee: Money = Field(description="Fee charged on top of the amount (0 if not charged)")
    currency: Currency
    converted_amount: Money | None = Field(description="Conversion: credited amount")
    exchange_rate: Rate | None = Field(description="Conversion: units of target per source unit")
    description: str | None
    error_code: str | None = Field(description="Reason of a failed / unconfirmed transfer")
    external_id: str | None = Field(description="Transaction id at the interbank / SBP gateway")
    cancellable_until: datetime | None = Field(
        description="An internal transfer can be cancelled until this moment while pending"
    )
    created_at: datetime
    updated_at: datetime


class ExchangeRateRead(BaseModel):
    base: Currency
    quote: Currency
    rate: Rate = Field(description="Units of quote currency for one unit of base currency")
    date: datetime = Field(description="Date of the Bank of Russia rates")
