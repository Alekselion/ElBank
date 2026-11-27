"""Declarative base, shared column types and mixins for all ORM models."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from sqlalchemy import Date, DateTime, Enum, MetaData, Numeric, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Deterministic constraint names so Alembic autogenerate produces stable migrations.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

MONEY = Numeric(18, 2)
RATE = Numeric(7, 4)  # annual interest rate in percent, e.g. 12.5000
EXCHANGE_RATE = Numeric(18, 8)  # currency exchange rate, e.g. 0.01081234
PRICE = Numeric(18, 4)  # security price, e.g. 0.0245 (amounts stay MONEY)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    # Fetch server-generated values (created_at, updated_at onupdate, ...) via RETURNING
    # on INSERT *and* UPDATE, so they never need a lazy (sync) refresh in async code.
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012 - SQLAlchemy reads a plain dict

    type_annotation_map: ClassVar[dict[Any, Any]] = {
        uuid.UUID: Uuid(as_uuid=True),
        datetime: DateTime(timezone=True),
        date: Date(),
        Decimal: MONEY,
        dict[str, Any]: JSONB,
        list[dict[str, Any]]: JSONB,
    }


def str_enum[E: StrEnum](enum_cls: type[E], name: str) -> Enum:
    """Enum stored as VARCHAR + CHECK constraint (not a native PG type).

    Adding/removing values then only needs a CHECK change in a migration instead
    of ``ALTER TYPE``. The DB stores the enum *values* (``"active"``), not names.
    """
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=32,
        values_callable=lambda members: [m.value for m in members],
        validate_strings=True,
    )


class UUIDMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
        sort_order=-100,
    )


class CreatedAtMixin:
    """For append-only records (audit, messages, tokens)."""

    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), sort_order=100)


class TimestampMixin(CreatedAtMixin):
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now(), sort_order=101
    )
