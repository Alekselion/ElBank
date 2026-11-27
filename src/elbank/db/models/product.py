"""Banking products: credits, deposits and brokerage (orders, positions)."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import PRICE, RATE, Base, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.account import CURRENCY_CHECK
from elbank.db.models.enums import (
    CreditStatus,
    DepositStatus,
    InvestmentOrderStatus,
    InvestmentSide,
)


def _product_checks() -> tuple[CheckConstraint, ...]:
    # A function, not a shared tuple: a constraint object belongs to exactly one table.
    return (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("rate >= 0", name="rate_non_negative"),
        CheckConstraint("term_months > 0", name="term_positive"),
    )


class Credit(UUIDMixin, TimestampMixin, Base):
    """Consumer credit.

    ``requested → approved | rejected`` (scoring), ``approved → active`` (signed with
    an OTP: the amount is credited to ``account_id``), ``active → closed`` (repaid).

    ``schedule`` holds the annuity installments::

        [{"n": 1, "due_date": "2027-01-27", "payment": "8884.88", "principal": "7051.55",
          "interest": "1833.33", "paid_principal": "0.00", "paid_interest": "0.00",
          "paid_at": null}, ...]
    """

    __tablename__ = "credits"
    __table_args__ = (
        *_product_checks(),
        CheckConstraint("principal_outstanding >= 0", name="outstanding_non_negative"),
        CheckConstraint("principal_outstanding <= amount", name="outstanding_lte_amount"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    # Disbursement and repayment account (RUB).
    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), index=True
    )
    amount: Mapped[Decimal]
    rate: Mapped[Decimal] = mapped_column(RATE)
    term_months: Mapped[int]
    purpose: Mapped[str] = mapped_column(String(255))
    status: Mapped[CreditStatus] = mapped_column(
        str_enum(CreditStatus, "credit_status"),
        default=CreditStatus.REQUESTED,
        server_default="requested",
    )
    score: Mapped[int | None]  # credit bureau score
    scoring_comment: Mapped[str | None] = mapped_column(String(500))
    monthly_payment: Mapped[Decimal | None]
    principal_outstanding: Mapped[Decimal] = mapped_column(
        default=Decimal("0.00"), server_default="0"
    )
    signed_at: Mapped[datetime | None]
    closed_at: Mapped[datetime | None]
    schedule: Mapped[list[dict[str, Any]]] = mapped_column(
        default=list, server_default=text("'[]'::jsonb")
    )


class Deposit(UUIDMixin, TimestampMixin, Base):
    """Term deposit with simple daily interest, paid out on closing.

    Interest is accrued for every day in ``[start_date, end_date)`` at
    ``amount * rate / 100 / days_in_year``. ``accrued_until`` is exclusive: interest
    for all days before it is in ``accrued_interest``; ``interest_schedule`` holds one
    entry per accrual (``{"date_from", "date_to", "days", "principal", "interest"}``),
    so the interest can be recalculated at another rate on early closing.
    """

    __tablename__ = "deposits"
    __table_args__ = (
        *_product_checks(),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
        CheckConstraint("accrued_interest >= 0", name="interest_non_negative"),
        CheckConstraint("end_date > start_date", name="end_after_start"),
        CheckConstraint(
            "accrued_until BETWEEN start_date AND end_date", name="accrued_within_term"
        ),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), index=True
    )
    amount: Mapped[Decimal]  # current principal (grows with top-ups)
    currency: Mapped[str] = mapped_column(String(3))
    rate: Mapped[Decimal] = mapped_column(RATE)
    term_months: Mapped[int]
    status: Mapped[DepositStatus] = mapped_column(
        str_enum(DepositStatus, "deposit_status"),
        default=DepositStatus.ACTIVE,
        server_default="active",
    )
    start_date: Mapped[date]
    end_date: Mapped[date]
    accrued_until: Mapped[date]
    accrued_interest: Mapped[Decimal] = mapped_column(default=Decimal("0.00"), server_default="0")
    paid_interest: Mapped[Decimal | None]  # set on closing (lower than accrued if early)
    closed_at: Mapped[datetime | None]
    interest_schedule: Mapped[list[dict[str, Any]]] = mapped_column(
        default=list, server_default=text("'[]'::jsonb")
    )


class InvestmentOrder(UUIDMixin, TimestampMixin, Base):
    """Limit order sent to the broker; filled at ``price`` or rejected (no money moved)."""

    __tablename__ = "investment_orders"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("limit_price > 0", name="limit_price_positive"),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
        CheckConstraint(
            "status <> 'filled' OR (price IS NOT NULL AND amount IS NOT NULL"
            " AND operation_id IS NOT NULL)",
            name="filled_has_execution",
        ),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"))
    ticker: Mapped[str] = mapped_column(String(12))
    side: Mapped[InvestmentSide] = mapped_column(str_enum(InvestmentSide, "investment_side"))
    quantity: Mapped[int]
    limit_price: Mapped[Decimal] = mapped_column(PRICE)
    currency: Mapped[str] = mapped_column(String(3))
    status: Mapped[InvestmentOrderStatus] = mapped_column(
        str_enum(InvestmentOrderStatus, "investment_order_status")
    )
    price: Mapped[Decimal | None] = mapped_column(PRICE)  # execution price
    amount: Mapped[Decimal | None]  # price * quantity, rounded to kopecks
    broker_order_id: Mapped[str | None] = mapped_column(String(64))
    reject_code: Mapped[str | None] = mapped_column(String(64))
    reject_reason: Mapped[str | None] = mapped_column(String(255))
    operation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("operations.id", ondelete="RESTRICT"), unique=True
    )


class InvestmentPosition(UUIDMixin, TimestampMixin, Base):
    """Securities a user holds; removed when the quantity drops to zero."""

    __tablename__ = "investment_positions"
    __table_args__ = (
        UniqueConstraint("user_id", "ticker"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    ticker: Mapped[str] = mapped_column(String(12))
    quantity: Mapped[int]
    avg_price: Mapped[Decimal] = mapped_column(PRICE)  # weighted average purchase price
    currency: Mapped[str] = mapped_column(String(3))
