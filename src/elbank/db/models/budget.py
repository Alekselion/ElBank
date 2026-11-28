"""Monthly spending budgets of a user, per expense category and currency."""

import uuid
from decimal import Decimal

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import Base, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.account import CURRENCY_CHECK
from elbank.db.models.enums import SpendingCategory


class Budget(UUIDMixin, TimestampMixin, Base):
    """A limit on settled expenses of one category in one currency per calendar month (UTC).

    Accounts in different currencies are never mixed: a budget counts only operations
    in its own currency.
    """

    __tablename__ = "budgets"
    __table_args__ = (
        UniqueConstraint("user_id", "category", "currency"),
        CheckConstraint("monthly_limit > 0", name="monthly_limit_positive"),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    category: Mapped[SpendingCategory] = mapped_column(
        str_enum(SpendingCategory, "spending_category")
    )
    currency: Mapped[str] = mapped_column(String(3))
    monthly_limit: Mapped[Decimal]
