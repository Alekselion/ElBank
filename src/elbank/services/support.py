"""Support tickets: creation with attachments, message thread, statuses, CSAT.

Access: a client sees only own tickets (others' are reported as not found), staff
(manager, admin) sees all. Statuses::

    open ──▶ in_progress ──▶ resolved ──▶ closed
               ▲   │  ▲          │
               │   ▼  └──────────┘ (reopen)
          waiting_client

- staff changes the status along the arrows (``POST /{id}/status``); taking an
  ``open`` ticket into work assigns it to that manager;
- the first staff reply to an ``open`` ticket moves it to ``in_progress`` as well;
- a client reply to a ``waiting_client`` or ``resolved`` ticket moves it back to
  ``in_progress``;
- the author or staff can close a ticket from any status; ``closed`` is final;
- the author rates (CSAT 1..5) a ``resolved``/``closed`` ticket once.

Events (after commit, best effort): ``ticket.created``; ``ticket.updated`` with
``kind`` = ``message`` / ``status`` / ``rated`` — the WebSocket fan-out
(:mod:`elbank.services.ticket_hub`) forwards them to subscribed clients. A staff
reply also sends ``notification.send`` to the author.

Attachments: up to ``SUPPORT_MAX_ATTACHMENTS`` files of ``SUPPORT_MAX_ATTACHMENT_BYTES``
each, JPEG/PNG/PDF detected by content (the client's content type is ignored),
stored as ``{SUPPORT_UPLOADS_DIR}/{attachment_id}``.
"""

import asyncio
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import set_committed_value

from elbank.core.config import SupportSettings
from elbank.core.exceptions import (
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PayloadTooLargeError,
    PermissionDeniedError,
    UnprocessableError,
    UnsupportedMediaTypeError,
)
from elbank.core.logging import get_logger
from elbank.db.models import (
    SupportAttachment,
    SupportMessage,
    SupportTicket,
    TicketCategory,
    TicketPriority,
    TicketStatus,
    User,
    UserRole,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.support import TicketEventKind

logger = get_logger(__name__)

STAFF_ROLES = frozenset({UserRole.MANAGER, UserRole.ADMIN})

STATUS_TRANSITIONS: dict[TicketStatus, frozenset[TicketStatus]] = {
    TicketStatus.OPEN: frozenset({TicketStatus.IN_PROGRESS, TicketStatus.CLOSED}),
    TicketStatus.IN_PROGRESS: frozenset(
        {TicketStatus.WAITING_CLIENT, TicketStatus.RESOLVED, TicketStatus.CLOSED}
    ),
    TicketStatus.WAITING_CLIENT: frozenset(
        {TicketStatus.IN_PROGRESS, TicketStatus.RESOLVED, TicketStatus.CLOSED}
    ),
    TicketStatus.RESOLVED: frozenset({TicketStatus.IN_PROGRESS, TicketStatus.CLOSED}),
    TicketStatus.CLOSED: frozenset(),
}
_CLIENT_REOPENS = frozenset({TicketStatus.WAITING_CLIENT, TicketStatus.RESOLVED})
_RATEABLE = frozenset({TicketStatus.RESOLVED, TicketStatus.CLOSED})

# Content sniffing: (magic prefix, content type, extension).
_SIGNATURES: tuple[tuple[bytes, str, str], ...] = (
    (b"\xff\xd8\xff", "image/jpeg", ".jpg"),
    (b"\x89PNG\r\n\x1a\n", "image/png", ".png"),
    (b"%PDF-", "application/pdf", ".pdf"),
)
_UNSAFE_NAME_CHARS = re.compile(r'[\x00-\x1f\x7f"\\/:*?<>|]')
_MAX_NAME = 255


def is_staff(user: User) -> bool:
    return user.role in STAFF_ROLES


@dataclass(slots=True, frozen=True)
class UploadedFile:
    """An uploaded file read by the route (at most ``max_attachment_bytes + 1`` bytes)."""

    file_name: str | None
    content: bytes


@dataclass(slots=True, frozen=True)
class _CheckedFile:
    file_name: str
    content_type: str
    content: bytes


def _sniff(content: bytes) -> tuple[str, str] | None:
    for magic, content_type, extension in _SIGNATURES:
        if content.startswith(magic):
            return content_type, extension
    return None


def _safe_file_name(raw: str | None, extension: str) -> str:
    """Base name only, no control/reserved characters; the detected extension is enforced."""
    name = PureWindowsPath(PurePosixPath(raw or "").name).name
    name = _UNSAFE_NAME_CHARS.sub("_", name).strip(" .")
    stem = Path(name).stem if name else ""
    return (stem or "attachment")[: _MAX_NAME - len(extension)] + extension


class TicketService:
    def __init__(
        self, session: AsyncSession, publisher: EventPublisher, settings: SupportSettings
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._settings = settings

    def attachment_path(self, attachment_id: uuid.UUID) -> Path:
        return self._settings.uploads_dir / str(attachment_id)

    # ---------- create ----------

    async def create(
        self,
        user: User,
        *,
        subject: str,
        category: TicketCategory,
        priority: TicketPriority,
        body: str,
        files: Sequence[UploadedFile],
    ) -> SupportTicket:
        checked = self._check_files(files)
        ticket = SupportTicket(
            id=uuid.uuid4(),
            user_id=user.id,
            subject=subject,
            category=category,
            priority=priority,
            status=TicketStatus.OPEN,
        )
        message = SupportMessage(
            id=uuid.uuid4(),
            ticket_id=ticket.id,
            author_id=user.id,
            from_staff=is_staff(user),
            body=body,
        )
        attachments = [
            SupportAttachment(
                id=uuid.uuid4(),
                message_id=message.id,
                file_name=f.file_name,
                content_type=f.content_type,
                size=len(f.content),
            )
            for f in checked
        ]
        self._session.add_all([ticket, message, *attachments])

        paths = [self.attachment_path(a.id) for a in attachments]
        try:
            for path, f in zip(paths, checked, strict=True):
                await asyncio.to_thread(self._write_file, path, f.content)
            await self._session.commit()
        except BaseException:
            for path in paths:
                await asyncio.to_thread(path.unlink, missing_ok=True)
            raise

        logger.info(
            "ticket created",
            ticket_id=str(ticket.id),
            category=category.value,
            priority=priority.value,
            attachments=len(attachments),
        )
        await self._publisher.publish_safely(
            EventType.TICKET_CREATED,
            {
                **self._ticket_payload(ticket),
                "subject": ticket.subject,
                "category": ticket.category.value,
                "priority": ticket.priority.value,
                "message_id": str(message.id),
                "attachments": len(attachments),
            },
            key=str(ticket.id),
        )
        return await self.get_detail(user, ticket.id)

    def _check_files(self, files: Sequence[UploadedFile]) -> list[_CheckedFile]:
        if len(files) > self._settings.max_attachments:
            raise UnprocessableError(
                f"No more than {self._settings.max_attachments} attachments per ticket"
            )
        checked: list[_CheckedFile] = []
        for f in files:
            if not f.content:
                raise UnprocessableError("Attachment must not be empty")
            if len(f.content) > self._settings.max_attachment_bytes:
                raise PayloadTooLargeError(
                    f"Attachment must not exceed {self._settings.max_attachment_bytes} bytes"
                )
            sniffed = _sniff(f.content)
            if sniffed is None:
                raise UnsupportedMediaTypeError("Attachments must be JPEG, PNG or PDF")
            content_type, extension = sniffed
            checked.append(
                _CheckedFile(_safe_file_name(f.file_name, extension), content_type, f.content)
            )
        return checked

    @staticmethod
    def _write_file(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    # ---------- read ----------

    async def list_for_user(
        self,
        user: User,
        *,
        status: TicketStatus | None,
        limit: int,
        offset: int,
    ) -> tuple[list[SupportTicket], int]:
        where = [] if is_staff(user) else [SupportTicket.user_id == user.id]
        if status is not None:
            where.append(SupportTicket.status == status)
        total = await self._session.scalar(
            select(func.count()).select_from(SupportTicket).where(*where)
        )
        rows = await self._session.scalars(
            select(SupportTicket)
            .where(*where)
            .order_by(SupportTicket.updated_at.desc(), SupportTicket.id)
            .limit(limit)
            .offset(offset)
        )
        return list(rows), total or 0

    async def get_accessible(
        self, user: User, ticket_id: uuid.UUID, *, for_update: bool = False
    ) -> SupportTicket:
        """The ticket if ``user`` may see it; another client's ticket is "not found"."""
        query = select(SupportTicket).where(SupportTicket.id == ticket_id)
        if not is_staff(user):
            query = query.where(SupportTicket.user_id == user.id)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        ticket = await self._session.scalar(query)
        if ticket is None:
            raise NotFoundError("Ticket not found")
        return ticket

    async def get_detail(self, user: User, ticket_id: uuid.UUID) -> SupportTicket:
        await self.get_accessible(user, ticket_id)
        ticket = await self._session.scalar(
            select(SupportTicket)
            .where(SupportTicket.id == ticket_id)
            .options(selectinload(SupportTicket.messages).selectinload(SupportMessage.attachments))
            .execution_options(populate_existing=True)
        )
        assert ticket is not None
        return ticket

    async def attachment(
        self, user: User, ticket_id: uuid.UUID, attachment_id: uuid.UUID
    ) -> tuple[SupportAttachment, Path]:
        await self.get_accessible(user, ticket_id)
        attachment = await self._session.scalar(
            select(SupportAttachment)
            .join(SupportMessage, SupportMessage.id == SupportAttachment.message_id)
            .where(SupportAttachment.id == attachment_id, SupportMessage.ticket_id == ticket_id)
        )
        if attachment is None:
            raise NotFoundError("Attachment not found")
        path = self.attachment_path(attachment.id)
        if not await asyncio.to_thread(path.is_file):
            raise NotFoundError("Attachment file is missing")
        return attachment, path

    # ---------- changes ----------

    async def add_message(self, user: User, ticket_id: uuid.UUID, body: str) -> SupportMessage:
        ticket = await self.get_accessible(user, ticket_id, for_update=True)
        if ticket.status is TicketStatus.CLOSED:
            raise InvalidStateError("Ticket is closed")
        staff = is_staff(user)
        old_status = ticket.status
        if staff and ticket.status is TicketStatus.OPEN:
            self._take(ticket, user)
        elif not staff and ticket.status in _CLIENT_REOPENS:
            ticket.status = TicketStatus.IN_PROGRESS

        message = SupportMessage(
            id=uuid.uuid4(), ticket_id=ticket.id, author_id=user.id, from_staff=staff, body=body
        )
        self._session.add(message)
        ticket.updated_at = func.now()  # list order follows the latest activity
        await self._session.commit()
        await self._session.refresh(ticket, ["updated_at"])

        logger.info(
            "ticket message added",
            ticket_id=str(ticket.id),
            message_id=str(message.id),
            from_staff=staff,
        )
        await self._publish_updated(
            ticket,
            TicketEventKind.MESSAGE,
            old_status=old_status,
            message={
                "id": str(message.id),
                "author_id": str(message.author_id),
                "from_staff": staff,
                "body": message.body,
                "created_at": message.created_at.isoformat(),
            },
        )
        if staff and ticket.user_id != user.id:
            await self._publisher.publish_safely(
                EventType.NOTIFICATION_SEND,
                {
                    "user_id": str(ticket.user_id),
                    "channel": "push",
                    "template": "support.reply",
                    "ticket_id": str(ticket.id),
                    "subject": ticket.subject,
                },
                key=str(ticket.user_id),
            )
        # A new message has no attachments: mark the collection loaded (it is lazy="raise").
        set_committed_value(message, "attachments", [])
        return message

    async def close(self, user: User, ticket_id: uuid.UUID) -> SupportTicket:
        ticket = await self.get_accessible(user, ticket_id, for_update=True)
        if ticket.user_id != user.id and not is_staff(user):
            raise PermissionDeniedError("Only the author or a manager can close the ticket")
        if ticket.status is TicketStatus.CLOSED:
            raise InvalidStateError("Ticket is already closed")
        return await self._set_status(ticket, TicketStatus.CLOSED, user)

    async def change_status(
        self, manager: User, ticket_id: uuid.UUID, status: TicketStatus
    ) -> SupportTicket:
        """Staff only (checked by the route)."""
        ticket = await self.get_accessible(manager, ticket_id, for_update=True)
        if status not in STATUS_TRANSITIONS[ticket.status]:
            raise InvalidStateError(
                f"Ticket status cannot change from {ticket.status.value} to {status.value}"
            )
        return await self._set_status(ticket, status, manager)

    async def rate(
        self, user: User, ticket_id: uuid.UUID, score: int, comment: str | None
    ) -> SupportTicket:
        ticket = await self.get_accessible(user, ticket_id, for_update=True)
        if ticket.user_id != user.id:
            raise PermissionDeniedError("Only the author can rate the ticket")
        if ticket.status not in _RATEABLE:
            raise InvalidStateError("Only a resolved or closed ticket can be rated")
        if ticket.rating is not None:
            raise ConflictError("Ticket is already rated")
        ticket.rating = score
        ticket.rating_comment = comment
        ticket.rated_at = datetime.now(UTC)
        await self._session.commit()
        logger.info("ticket rated", ticket_id=str(ticket.id), score=score)
        await self._publish_updated(
            ticket, TicketEventKind.RATED, old_status=ticket.status, rating=score
        )
        return ticket

    async def _set_status(
        self, ticket: SupportTicket, status: TicketStatus, actor: User
    ) -> SupportTicket:
        old_status = ticket.status
        if old_status is TicketStatus.OPEN and status is TicketStatus.IN_PROGRESS:
            self._take(ticket, actor)
        else:
            ticket.status = status
        if status is TicketStatus.CLOSED:
            ticket.closed_at = datetime.now(UTC)
        await self._session.commit()
        logger.info(
            "ticket status changed",
            ticket_id=str(ticket.id),
            old_status=old_status.value,
            new_status=status.value,
        )
        await self._publish_updated(ticket, TicketEventKind.STATUS, old_status=old_status)
        return ticket

    @staticmethod
    def _take(ticket: SupportTicket, manager: User) -> None:
        ticket.status = TicketStatus.IN_PROGRESS
        if ticket.assignee_id is None:
            ticket.assignee_id = manager.id

    # ---------- events ----------

    @staticmethod
    def _ticket_payload(ticket: SupportTicket) -> dict[str, Any]:
        return {
            "ticket_id": str(ticket.id),
            "user_id": str(ticket.user_id),
            "status": ticket.status.value,
        }

    async def _publish_updated(
        self,
        ticket: SupportTicket,
        kind: TicketEventKind,
        *,
        old_status: TicketStatus,
        **extra: Any,
    ) -> None:
        payload = {
            **self._ticket_payload(ticket),
            "kind": kind.value,
            "old_status": old_status.value,
            "assignee_id": str(ticket.assignee_id) if ticket.assignee_id else None,
            **extra,
        }
        await self._publisher.publish_safely(EventType.TICKET_UPDATED, payload, key=str(ticket.id))
