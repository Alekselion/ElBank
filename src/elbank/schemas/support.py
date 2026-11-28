"""Support ticket and FAQ schemas."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from elbank.db.models import TicketCategory, TicketPriority, TicketStatus

TicketSubject = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)
]
MessageBody = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=4000)
]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MessageCreate(_Request):
    body: MessageBody


class TicketRate(_Request):
    score: int = Field(ge=1, le=5, description="CSAT: 1 (bad) .. 5 (excellent)")
    comment: Annotated[
        str | None, StringConstraints(strip_whitespace=True, min_length=1, max_length=1000)
    ] = None


class TicketStatusChange(_Request):
    status: TicketStatus


class AttachmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    file_name: str
    content_type: str
    size: int


class MessageRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    author_id: uuid.UUID | None = Field(description="null: automatic message of the bank")
    from_staff: bool
    body: str
    created_at: datetime
    attachments: list[AttachmentRead] = Field(
        description="Download: GET /api/v1/support/tickets/{ticket_id}/attachments/{id}"
    )


class TicketRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    subject: str
    category: TicketCategory
    priority: TicketPriority
    status: TicketStatus
    assignee_id: uuid.UUID | None
    rating: int | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None


class TicketDetail(TicketRead):
    rating_comment: str | None
    rated_at: datetime | None
    messages: list[MessageRead]


class TicketEventKind(StrEnum):
    """``kind`` of a ``ticket.updated`` event (also what WebSocket clients receive)."""

    MESSAGE = "message"
    STATUS = "status"
    RATED = "rated"


class FaqArticleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    slug: str
    category: TicketCategory
    question: str
    answer: str
