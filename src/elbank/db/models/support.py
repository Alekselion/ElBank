"""Support tickets and their message threads."""

import uuid

from sqlalchemy import ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from elbank.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import TicketPriority, TicketStatus


class SupportTicket(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "support_tickets"
    __table_args__ = (Index(None, "status", "priority"),)  # manager queue

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    subject: Mapped[str] = mapped_column(String(200))
    category: Mapped[str] = mapped_column(String(50))
    status: Mapped[TicketStatus] = mapped_column(
        str_enum(TicketStatus, "ticket_status"), default=TicketStatus.OPEN, server_default="open"
    )
    priority: Mapped[TicketPriority] = mapped_column(
        str_enum(TicketPriority, "ticket_priority"),
        default=TicketPriority.NORMAL,
        server_default="normal",
    )

    messages: Mapped[list[SupportMessage]] = relationship(
        back_populates="ticket", lazy="raise", order_by="SupportMessage.created_at"
    )


class SupportMessage(UUIDMixin, CreatedAtMixin, Base):
    __tablename__ = "support_messages"
    __table_args__ = (Index(None, "ticket_id", "created_at"),)

    ticket_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("support_tickets.id", ondelete="CASCADE")
    )
    author_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    body: Mapped[str] = mapped_column(Text)

    ticket: Mapped[SupportTicket] = relationship(back_populates="messages", lazy="raise")
