"""Support: tickets, their message threads and attachments; FAQ articles."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    false,
    true,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from elbank.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import TicketCategory, TicketPriority, TicketStatus


class SupportTicket(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "support_tickets"
    __table_args__ = (
        Index(None, "status", "priority"),  # manager queue
        CheckConstraint("rating IS NULL OR rating BETWEEN 1 AND 5", name="rating_range"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    subject: Mapped[str] = mapped_column(String(200))
    category: Mapped[TicketCategory] = mapped_column(str_enum(TicketCategory, "ticket_category"))
    status: Mapped[TicketStatus] = mapped_column(
        str_enum(TicketStatus, "ticket_status"), default=TicketStatus.OPEN, server_default="open"
    )
    priority: Mapped[TicketPriority] = mapped_column(
        str_enum(TicketPriority, "ticket_priority"),
        default=TicketPriority.NORMAL,
        server_default="normal",
    )
    # Manager who took the ticket into work (open → in_progress).
    assignee_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    closed_at: Mapped[datetime | None]
    # CSAT: the author's score 1..5 after the ticket is resolved/closed; set once.
    rating: Mapped[int | None] = mapped_column(SmallInteger)
    rating_comment: Mapped[str | None] = mapped_column(String(1000))
    rated_at: Mapped[datetime | None]

    messages: Mapped[list[SupportMessage]] = relationship(
        back_populates="ticket", lazy="raise", order_by="SupportMessage.created_at"
    )


class SupportMessage(UUIDMixin, CreatedAtMixin, Base):
    __tablename__ = "support_messages"
    __table_args__ = (Index(None, "ticket_id", "created_at"),)

    ticket_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("support_tickets.id", ondelete="CASCADE")
    )
    # NULL: an automatic message of the bank (e.g. the anti-fraud notice of a fraud ticket).
    author_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    # Written by bank staff (fixed at write time: the author's role may change later).
    from_staff: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())
    body: Mapped[str] = mapped_column(Text)

    ticket: Mapped[SupportTicket] = relationship(back_populates="messages", lazy="raise")
    attachments: Mapped[list[SupportAttachment]] = relationship(
        back_populates="message", lazy="raise", order_by="SupportAttachment.created_at"
    )


class SupportAttachment(UUIDMixin, CreatedAtMixin, Base):
    """A file attached to a message; stored as ``{SUPPORT_UPLOADS_DIR}/{id}``."""

    __tablename__ = "support_attachments"
    __table_args__ = (CheckConstraint("size > 0", name="size_positive"),)

    message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("support_messages.id", ondelete="CASCADE"), index=True
    )
    file_name: Mapped[str] = mapped_column(String(255))  # sanitized client file name
    content_type: Mapped[str] = mapped_column(String(100))  # detected from content
    size: Mapped[int] = mapped_column(Integer)

    message: Mapped[SupportMessage] = relationship(back_populates="attachments", lazy="raise")


class FaqArticle(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "faq_articles"
    __table_args__ = (Index(None, "category", "position"),)

    slug: Mapped[str] = mapped_column(String(100), unique=True)
    category: Mapped[TicketCategory] = mapped_column(str_enum(TicketCategory, "ticket_category"))
    question: Mapped[str] = mapped_column(String(300))
    answer: Mapped[str] = mapped_column(Text)
    position: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    is_published: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true())
