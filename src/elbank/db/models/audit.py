"""Audit trail, anti-fraud rules and the cases they open."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, ForeignKey, Index, String, Text, text, true
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import FraudCaseStatus


class AuditEvent(UUIDMixin, CreatedAtMixin, Base):
    """Legacy table, unused: the audit trail lives in Elasticsearch (``audit.event``)."""

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
    """``condition``: JSON DSL, see :mod:`elbank.services.fraud_service`."""

    __tablename__ = "fraud_rules"

    name: Mapped[str] = mapped_column(String(100), unique=True)
    condition: Mapped[dict[str, Any]]
    description: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true())


class FraudCase(UUIDMixin, TimestampMixin, Base):
    """An operation held by anti-fraud rules, waiting for (or after) a manual decision."""

    __tablename__ = "fraud_cases"
    __table_args__ = (Index(None, "status", "created_at"),)  # review queue

    operation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("operations.id", ondelete="RESTRICT"), unique=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    status: Mapped[FraudCaseStatus] = mapped_column(
        str_enum(FraudCaseStatus, "fraud_case_status"),
        default=FraudCaseStatus.OPEN,
        server_default="open",
    )
    # Rules that fired: [{"id", "name"}] (snapshot: a rule may be edited or deleted later).
    rules: Mapped[list[dict[str, Any]]]
    # Channel data needed to continue the operation after confirmation (gateway request).
    resume: Mapped[dict[str, Any]] = mapped_column(server_default=text("'{}'::jsonb"))
    ticket_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("support_tickets.id", ondelete="SET NULL")
    )
    decided_by_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    decided_at: Mapped[datetime | None]
    decision_reason: Mapped[str | None] = mapped_column(String(500))
