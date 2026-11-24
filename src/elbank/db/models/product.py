"""Banking products: credits and deposits."""

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, text
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import RATE, Base, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import CreditStatus, DepositStatus

_PRODUCT_CHECKS = (
    CheckConstraint("amount > 0", name="amount_positive"),
    CheckConstraint("rate >= 0", name="rate_non_negative"),
    CheckConstraint("term_months > 0", name="term_positive"),
)


class Credit(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "credits"
    __table_args__ = _PRODUCT_CHECKS

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    amount: Mapped[Decimal]
    rate: Mapped[Decimal] = mapped_column(RATE)
    term_months: Mapped[int]
    status: Mapped[CreditStatus] = mapped_column(
        str_enum(CreditStatus, "credit_status"),
        default=CreditStatus.REQUESTED,
        server_default="requested",
    )
    # [{"n": 1, "due_date": "...", "principal": "...", "interest": "...", "paid_at": null}, ...]
    schedule: Mapped[list[dict[str, Any]]] = mapped_column(
        default=list, server_default=text("'[]'::jsonb")
    )


class Deposit(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "deposits"
    __table_args__ = _PRODUCT_CHECKS

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), index=True
    )
    amount: Mapped[Decimal]
    rate: Mapped[Decimal] = mapped_column(RATE)
    term_months: Mapped[int]
    status: Mapped[DepositStatus] = mapped_column(
        str_enum(DepositStatus, "deposit_status"),
        default=DepositStatus.ACTIVE,
        server_default="active",
    )
    interest_schedule: Mapped[list[dict[str, Any]]] = mapped_column(
        default=list, server_default=text("'[]'::jsonb")
    )
