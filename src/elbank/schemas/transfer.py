"""Transfer request/response schemas (money as strings, see ``schemas.account``)."""

import uuid
from datetime import datetime
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

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


class InternalTransferRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: PositiveMoney
    currency: Currency
    description: (
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
        | None
    ) = None
    idempotency_key: IdempotencyKey

    @model_validator(mode="after")
    def _check_accounts(self) -> Self:
        if self.from_account_id == self.to_account_id:
            raise ValueError("from_account_id and to_account_id must differ")
        return self


class TransferRead(BaseModel):
    operation_id: uuid.UUID
    status: OperationStatus
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: Money
    fee: Money = Field(description="Fee charged on top of the amount (0 if not charged)")
    currency: Currency
    description: str | None
    error_code: str | None = Field(description="Reason of a failed transfer")
    cancellable_until: datetime | None = Field(
        description="The transfer can be cancelled until this moment while it is pending"
    )
    created_at: datetime
    updated_at: datetime
