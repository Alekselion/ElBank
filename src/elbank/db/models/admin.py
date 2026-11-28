"""Back-office configuration: tariffs (fees) and manual exchange rate overrides."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import EXCHANGE_RATE, Base, str_enum
from elbank.db.models.enums import TariffCode


class Tariff(Base):
    """Fee of an operation type. No row: the defaults from the environment apply.

    ``config`` is validated by :class:`elbank.services.tariffs.FeeRule`
    (``{"percent": "0.5", "min": "10.00", "max": "5000.00"}``) and read at the
    moment an operation is created, so a change applies to new operations only.
    """

    __tablename__ = "tariffs"

    code: Mapped[TariffCode] = mapped_column(str_enum(TariffCode, "tariff_code"), primary_key=True)
    config: Mapped[dict[str, Any]] = mapped_column(server_default=text("'{}'::jsonb"))
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    updated_by_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )


class RateOverride(Base):
    """Exchange rate set by an admin instead of the Bank of Russia rate.

    ``rate`` is units of ``quote`` for one unit of ``base``; the inverse pair uses ``1 / rate``
    unless it has its own override.
    """

    __tablename__ = "rate_overrides"
    __table_args__ = (
        CheckConstraint("rate > 0", name="rate_positive"),
        CheckConstraint("base <> quote", name="distinct_currencies"),
        CheckConstraint("base ~ '^[A-Z]{3}$'", name="base_iso4217"),
        CheckConstraint("quote ~ '^[A-Z]{3}$'", name="quote_iso4217"),
    )

    base: Mapped[str] = mapped_column(String(3), primary_key=True)
    quote: Mapped[str] = mapped_column(String(3), primary_key=True)
    rate: Mapped[Decimal] = mapped_column(EXCHANGE_RATE)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    updated_by_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
