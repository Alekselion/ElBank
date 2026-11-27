"""Card request/response schemas. The full PAN never appears here: only its mask."""

import uuid
from datetime import date, datetime
from enum import StrEnum
from itertools import pairwise
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from elbank.db.models import Card, CardStatus, CardType, Currency
from elbank.schemas.account import Money, PositiveMoney

MAX_MCC_CODES = 500

MccCode = Annotated[
    str,
    Field(pattern=r"^\d{4}$", description="Merchant Category Code", examples=["5411"]),
]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------- issue ----------


class DeliveryAddress(_Request):
    postal_code: str = Field(pattern=r"^\d{6}$", examples=["101000"])
    city: str = Field(min_length=1, max_length=100, examples=["Москва"])
    street: str = Field(min_length=1, max_length=200, examples=["ул. Мясницкая"])
    house: str = Field(min_length=1, max_length=20, examples=["10к2"])
    apartment: str | None = Field(default=None, min_length=1, max_length=20, examples=["15"])


class VirtualCardIssueRequest(_Request):
    account_id: uuid.UUID
    type: Literal[CardType.VIRTUAL] = CardType.VIRTUAL


class PlasticCardOrderRequest(_Request):
    account_id: uuid.UUID
    address: DeliveryAddress


# ---------- block ----------


class CardBlockReason(StrEnum):
    LOST = "lost"
    STOLEN = "stolen"
    COMPROMISED = "compromised"
    OTHER = "other"


class CardBlockRequest(_Request):
    reason: CardBlockReason = CardBlockReason.OTHER


# ---------- limits ----------


class CardLimits(_Request):
    """Spending limits in the account currency; ``null`` means no limit."""

    daily: PositiveMoney | None
    monthly: PositiveMoney | None
    per_transaction: PositiveMoney | None

    @model_validator(mode="after")
    def _check_order(self) -> Self:
        ordered = [
            (name, value)
            for name, value in (
                ("per_transaction", self.per_transaction),
                ("daily", self.daily),
                ("monthly", self.monthly),
            )
            if value is not None
        ]
        for (smaller, low), (bigger, high) in pairwise(ordered):
            if low > high:
                raise ValueError(f"{smaller} limit must not exceed the {bigger} limit")
        return self


class CardLimitsRead(BaseModel):
    card_id: uuid.UUID
    currency: Currency
    daily: Money | None
    monthly: Money | None
    per_transaction: Money | None


# ---------- MCC restrictions ----------


class MccRestrictions(_Request):
    """``allow`` empty: every MCC except ``deny`` is allowed; otherwise only ``allow``."""

    allow: list[MccCode] = Field(default_factory=list, max_length=MAX_MCC_CODES)
    deny: list[MccCode] = Field(default_factory=list, max_length=MAX_MCC_CODES)

    @field_validator("allow", "deny")
    @classmethod
    def _normalize(cls, codes: list[str]) -> list[str]:
        return sorted(set(codes))

    @model_validator(mode="after")
    def _check_overlap(self) -> Self:
        if both := sorted(set(self.allow) & set(self.deny)):
            raise ValueError(f"MCC codes are both allowed and denied: {', '.join(both)}")
        return self


class CardMccRestrictionsRead(BaseModel):
    card_id: uuid.UUID
    allow: list[str]
    deny: list[str]


# ---------- read ----------


def _format_expiry(expiry: date) -> str:
    return expiry.strftime("%m/%y")


class CardRead(BaseModel):
    id: uuid.UUID
    account_id: uuid.UUID
    pan_masked: str = Field(examples=["220070******1234"])
    expiry: str = Field(description="MM/YY", examples=["11/30"])
    type: CardType
    status: CardStatus
    reissued_from_id: uuid.UUID | None = Field(description="The card this one replaced")
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_card(cls, card: Card) -> Self:
        return cls(
            id=card.id,
            account_id=card.account_id,
            pan_masked=card.pan_masked,
            expiry=_format_expiry(card.expiry),
            type=card.type,
            status=card.status,
            reissued_from_id=card.reissued_from_id,
            created_at=card.created_at,
            updated_at=card.updated_at,
        )


class CardDetails(CardRead):
    currency: Currency = Field(description="Account currency: limits are in it")
    limits: CardLimits
    mcc_restrictions: MccRestrictions
    delivery_address: DeliveryAddress | None = Field(description="Plastic cards only")

    @classmethod
    def from_card_and_currency(cls, card: Card, currency: Currency) -> Self:
        base = CardRead.from_card(card).model_dump()
        return cls(
            **base,
            currency=currency,
            limits=CardLimits.model_validate(card.limits),
            mcc_restrictions=MccRestrictions.model_validate(card.mcc_restrictions),
            delivery_address=(
                DeliveryAddress.model_validate(card.delivery_address)
                if card.delivery_address is not None
                else None
            ),
        )
