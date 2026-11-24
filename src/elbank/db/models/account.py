"""Accounts, their limits and cards."""

import uuid
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, ForeignKey, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from elbank.db.base import Base, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import AccountStatus, AccountType, CardStatus, CardType

if TYPE_CHECKING:
    from elbank.db.models.user import User

CURRENCY_CHECK = "currency ~ '^[A-Z]{3}$'"  # ISO 4217 alphabetic code


class Account(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint("balance >= 0", name="balance_non_negative"),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    number: Mapped[str] = mapped_column(String(20), unique=True)  # 20-digit account number
    type: Mapped[AccountType] = mapped_column(str_enum(AccountType, "account_type"))
    currency: Mapped[str] = mapped_column(String(3))
    balance: Mapped[Decimal] = mapped_column(default=Decimal("0.00"), server_default="0")
    status: Mapped[AccountStatus] = mapped_column(
        str_enum(AccountStatus, "account_status"),
        default=AccountStatus.ACTIVE,
        server_default="active",
    )

    user: Mapped[User] = relationship(back_populates="accounts", lazy="raise")
    limit: Mapped[AccountLimit | None] = relationship(back_populates="account", lazy="raise")
    cards: Mapped[list[Card]] = relationship(back_populates="account", lazy="raise")


class AccountLimit(UUIDMixin, TimestampMixin, Base):
    """Outgoing limits of an account (one row per account)."""

    __tablename__ = "account_limits"
    __table_args__ = (
        CheckConstraint("daily_limit > 0", name="daily_limit_positive"),
        CheckConstraint("monthly_limit >= daily_limit", name="monthly_gte_daily"),
    )

    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"), unique=True
    )
    daily_limit: Mapped[Decimal]
    monthly_limit: Mapped[Decimal]

    account: Mapped[Account] = relationship(back_populates="limit", lazy="raise")


class Card(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "cards"

    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), index=True
    )
    pan_masked: Mapped[str] = mapped_column(String(19))  # e.g. 220070******1234; never the full PAN
    expiry: Mapped[date]  # last day of the expiry month
    status: Mapped[CardStatus] = mapped_column(
        str_enum(CardStatus, "card_status"), default=CardStatus.ACTIVE, server_default="active"
    )
    type: Mapped[CardType] = mapped_column(str_enum(CardType, "card_type"))
    limits: Mapped[dict[str, Any]] = mapped_column(default=dict, server_default=text("'{}'::jsonb"))

    account: Mapped[Account] = relationship(back_populates="cards", lazy="raise")
