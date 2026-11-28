"""User notifications."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, SmallInteger, String, text
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import Base, CreatedAtMixin, UUIDMixin, str_enum
from elbank.db.models.enums import NotificationChannel, NotificationDeliveryStatus


class Notification(UUIDMixin, CreatedAtMixin, Base):
    """A notification as stored by the notifications worker (``notification.send``).

    ``dedup_key`` = ``{event_id}:{user_id}``: a replayed event never creates a second
    notification (nor a second delivery).
    """

    __tablename__ = "notifications"
    __table_args__ = (
        Index(None, "user_id", "created_at"),
        # Fast "unread" badge/listing.
        Index(
            "ix_notifications_user_id_unread", "user_id", postgresql_where=text("read_at IS NULL")
        ),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    channel: Mapped[NotificationChannel] = mapped_column(
        str_enum(NotificationChannel, "notification_channel")
    )
    type: Mapped[str] = mapped_column(String(64))  # template, e.g. "budget.warn"
    payload: Mapped[dict[str, Any]] = mapped_column(
        default=dict, server_default=text("'{}'::jsonb")
    )
    read_at: Mapped[datetime | None]
    dedup_key: Mapped[str] = mapped_column(String(100), unique=True)
    delivery_status: Mapped[NotificationDeliveryStatus] = mapped_column(
        str_enum(NotificationDeliveryStatus, "notification_delivery_status"),
        default=NotificationDeliveryStatus.PENDING,
        server_default="pending",
    )
    # Why it was skipped / failed: "channel_disabled", "no_recipient", "in_app", gateway error.
    delivery_note: Mapped[str | None] = mapped_column(String(255))
    delivery_attempts: Mapped[int] = mapped_column(SmallInteger, default=0, server_default="0")
    provider_message_id: Mapped[str | None] = mapped_column(String(64))
    sent_at: Mapped[datetime | None]
