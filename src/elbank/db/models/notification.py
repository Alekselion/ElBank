"""User notifications."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, String, text
from sqlalchemy.orm import Mapped, mapped_column

from elbank.db.base import Base, CreatedAtMixin, UUIDMixin, str_enum
from elbank.db.models.enums import NotificationChannel


class Notification(UUIDMixin, CreatedAtMixin, Base):
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
    type: Mapped[str] = mapped_column(String(64))  # e.g. "operation.completed"
    payload: Mapped[dict[str, Any]] = mapped_column(
        default=dict, server_default=text("'{}'::jsonb")
    )
    read_at: Mapped[datetime | None]
