"""Notification schemas."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from elbank.db.models import NotificationChannel, NotificationDeliveryStatus


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    channel: NotificationChannel
    type: str = Field(description="Template, e.g. budget.warn, support.reply")
    payload: dict[str, Any]
    delivery_status: NotificationDeliveryStatus
    created_at: datetime
    read_at: datetime | None


class NotificationPage(BaseModel):
    items: list[NotificationRead]
    limit: int
    offset: int
    total: int = Field(description="Total number of matching notifications")
    unread: int = Field(description="Unread notifications of the user (ignores filters)")


class ReadAllResult(BaseModel):
    updated: int = Field(description="Notifications marked as read")
