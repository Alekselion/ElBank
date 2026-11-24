"""Audit trail and anti-fraud rules."""

import uuid
from typing import Any

from sqlalchemy import Boolean, ForeignKey, Index, String, Text, text, true
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDMixin


class AuditEvent(UUIDMixin, CreatedAtMixin, Base):
    """Append-only record of a security- or money-relevant action."""

    __tablename__ = "audit_events"
    __table_args__ = (
        Index(None, "entity", "entity_id"),
        Index(None, "user_id", "created_at"),
        Index(None, "correlation_id"),
    )

    user_id: Mapped[uuid.UUID | None] = mapped_column(  # NULL: system or anonymous actor
        ForeignKey("users.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(64))  # e.g. "user.login", "operation.create"
    entity: Mapped[str] = mapped_column(String(64))  # e.g. "account"
    entity_id: Mapped[str | None] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(
        default=dict, server_default=text("'{}'::jsonb")
    )
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    ip: Mapped[str | None] = mapped_column(INET)


class FraudRule(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "fraud_rules"

    name: Mapped[str] = mapped_column(String(100), unique=True)
    expression: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true())
