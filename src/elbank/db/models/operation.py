"""Money movements: operations, transfer templates, payment orders."""

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from elbank.db.base import Base, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.account import CURRENCY_CHECK
from elbank.db.models.enums import (
    OperationStatus,
    OperationType,
    PaymentOrderStatus,
    PaymentProvider,
)


class Operation(UUIDMixin, TimestampMixin, Base):
    """A single posting on an account.

    A transfer is a ``transfer_out`` (the main operation: it carries the initiator and
    the idempotency key) plus a ``transfer_in`` and a ``fee`` referencing it via
    ``parent_operation_id``. Compensating postings of a cancelled transfer reference
    the transfer too, and the posting they compensate via ``reversed_operation_id``.
    """

    __tablename__ = "operations"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(CURRENCY_CHECK, name="currency_iso4217"),
        # Idempotent retries: one operation per (initiator, client-supplied key).
        UniqueConstraint("user_id", "idempotency_key"),
        Index(None, "account_id", "created_at"),  # account statement
        Index(
            None, "status", postgresql_where=text("status IN ('created', 'pending', 'processing')")
        ),
    )

    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"))
    # Who initiated the operation; NULL for postings made by the bank (interest, reversals).
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    type: Mapped[OperationType] = mapped_column(str_enum(OperationType, "operation_type"))
    amount: Mapped[Decimal]
    currency: Mapped[str] = mapped_column(String(3))
    status: Mapped[OperationStatus] = mapped_column(
        str_enum(OperationStatus, "operation_status"),
        default=OperationStatus.CREATED,
        server_default="created",
    )
    counterparty_account: Mapped[str | None] = mapped_column(String(34))  # up to IBAN length
    description: Mapped[str | None] = mapped_column(String(255))
    idempotency_key: Mapped[str | None] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(64))
    parent_operation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("operations.id", ondelete="RESTRICT"), index=True
    )
    reversed_operation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("operations.id", ondelete="RESTRICT")
    )

    parent: Mapped[Operation | None] = relationship(
        remote_side="Operation.id", foreign_keys=[parent_operation_id], lazy="raise"
    )


class TransferTemplate(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "transfer_templates"
    __table_args__ = (
        UniqueConstraint("user_id", "name"),
        CheckConstraint("amount IS NULL OR amount > 0", name="amount_positive"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(100))
    recipient_account: Mapped[str] = mapped_column(String(34))
    amount: Mapped[Decimal | None]  # NULL: amount is entered on each use
    description: Mapped[str | None] = mapped_column(String(255))


class PaymentOrder(UUIDMixin, TimestampMixin, Base):
    """Payment to an external provider (mocked by WireMock)."""

    __tablename__ = "payment_orders"
    __table_args__ = (UniqueConstraint("provider", "external_id"),)

    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("accounts.id", ondelete="RESTRICT"), index=True
    )
    provider: Mapped[PaymentProvider] = mapped_column(str_enum(PaymentProvider, "payment_provider"))
    payload: Mapped[dict[str, Any]] = mapped_column(
        default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentOrderStatus] = mapped_column(
        str_enum(PaymentOrderStatus, "payment_order_status"),
        default=PaymentOrderStatus.CREATED,
        server_default="created",
    )
    external_id: Mapped[str | None] = mapped_column(String(64))  # provider's transaction id
