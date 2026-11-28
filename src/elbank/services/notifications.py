"""Notifications: delivery of ``notification.send`` and the user's notification list.

Event payload (envelope ``payload``)::

    {"user_id": "...", "channel": "sms" | "email" | "push" | "in_app",
     "template": "budget.warn", ...template fields...}

Worker flow (:class:`NotificationDispatcher`, at-least-once):

1. ``INSERT ... ON CONFLICT (dedup_key) DO NOTHING`` with ``dedup_key =
   {event_id}:{user_id}`` and status ``pending``, committed. A replayed event finds
   the row: a finished one (sent/failed/skipped) is skipped, a ``pending`` one (the
   worker died before finishing) is delivered again — with the same
   ``Idempotency-Key``, so the gateway does not deliver twice.
2. Decide: the producer already delivered it (``provider_message_id`` in the payload,
   e.g. OTP SMS) → ``sent``; ``in_app`` channel → ``skipped`` (``in_app``); channel
   switched off in ``users.notification_settings`` → ``skipped``
   (``channel_disabled``); no e-mail address → ``skipped`` (``no_recipient``).
3. Otherwise send through the gateway (no DB transaction is held meanwhile):
   ``sent`` + provider message id, or after the retries ``failed`` + error (logged as
   ``error``).

Events without ``user_id`` (OTP during registration) are not stored.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.clients.notifications import (
    EmailMessage,
    NotificationDeliveryError,
    NotificationGatewayClient,
    OutgoingMessage,
    PushMessage,
    SmsMessage,
)
from elbank.core.exceptions import NotFoundError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Notification,
    NotificationChannel,
    NotificationDeliveryStatus,
    User,
)
from elbank.kafka import EventEnvelope
from elbank.schemas.users import NotificationSettings

logger = get_logger(__name__)

_ROUTING_KEYS = frozenset({"user_id", "channel", "template"})
_MAX_TYPE = 64

# template -> (title, text); fields come from the event payload, missing ones are blank.
_TEMPLATES: dict[str, tuple[str, str]] = {
    "budget.warn": (
        "Бюджет почти исчерпан",
        "Расходы «{category}» за {period}: {spent} из {monthly_limit} {currency} ({percent} %).",
    ),
    "budget.danger": (
        "Бюджет превышен",
        "Расходы «{category}» за {period}: {spent} при лимите {monthly_limit} {currency} "
        "({percent} %).",
    ),
    "support.reply": ("Ответ поддержки", "По обращению «{subject}» пришёл ответ."),
}
_DEFAULT_TEMPLATE = ("Уведомление ЭлБанка", "У вас новое уведомление.")  # noqa: RUF001


class _Blank(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return ""


def render(template: str, data: dict[str, Any]) -> tuple[str, str]:
    title, text = _TEMPLATES.get(template, _DEFAULT_TEMPLATE)
    return title, text.format_map(_Blank(data))


class MalformedNotificationError(ValueError):
    """The event cannot be turned into a notification."""


@dataclass(slots=True, frozen=True)
class _Request:
    user_id: uuid.UUID
    channel: NotificationChannel
    template: str
    data: dict[str, Any]
    dedup_key: str


def _parse(envelope: EventEnvelope) -> _Request | None:
    payload = envelope.payload
    if payload.get("user_id") is None:
        return None
    try:
        user_id = uuid.UUID(str(payload["user_id"]))
        channel = NotificationChannel(str(payload["channel"]))
    except (KeyError, ValueError) as exc:
        raise MalformedNotificationError(f"bad user_id/channel: {exc}") from exc
    template = str(payload.get("template") or envelope.event_type.value)[:_MAX_TYPE]
    data = {k: v for k, v in payload.items() if k not in _ROUTING_KEYS}
    return _Request(user_id, channel, template, data, f"{envelope.event_id}:{user_id}")


class NotificationDispatcher:
    """Worker side: store and deliver one ``notification.send`` event."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: NotificationGatewayClient,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway

    async def handle(self, envelope: EventEnvelope) -> None:
        """Raises :class:`MalformedNotificationError` for an event that can never be stored."""
        request = _parse(envelope)
        if request is None:
            logger.info("notification without user skipped", event_id=str(envelope.event_id))
            return

        async with self._session_factory() as session:
            user = await session.get(User, request.user_id)
            if user is None:
                logger.warning(
                    "notification for unknown user skipped", user_id=str(request.user_id)
                )
                return
            notification = await self._store(session, request)
            if notification is None:
                logger.info("duplicate notification skipped", dedup_key=request.dedup_key)
                return

            message = self._prepare(notification, user, request)
            await session.commit()  # no transaction is held while the gateway is called
            if message is None:
                return
            await self._deliver(session, notification, message)

    @staticmethod
    async def _store(session: AsyncSession, request: _Request) -> Notification | None:
        """The notification to deliver; ``None`` if this event was already handled."""
        inserted = await session.scalar(
            insert(Notification)
            .values(
                id=uuid.uuid4(),
                user_id=request.user_id,
                channel=request.channel,
                type=request.template,
                payload=request.data,
                dedup_key=request.dedup_key,
                delivery_status=NotificationDeliveryStatus.PENDING,
            )
            .on_conflict_do_nothing(index_elements=[Notification.dedup_key])
            .returning(Notification.id)
        )
        await session.commit()
        notification = await session.scalar(
            select(Notification).where(Notification.dedup_key == request.dedup_key)
        )
        assert notification is not None
        if (
            inserted is None
            and notification.delivery_status is not NotificationDeliveryStatus.PENDING
        ):
            return None
        return notification

    @staticmethod
    def _prepare(
        notification: Notification, user: User, request: _Request
    ) -> OutgoingMessage | None:
        """Settle notifications that need no delivery; otherwise the message to send."""
        data = request.data
        if data.get("provider_message_id"):
            # Delivered by the producer itself (OTP SMS): only recorded here.
            notification.delivery_status = NotificationDeliveryStatus.SENT
            notification.provider_message_id = str(data["provider_message_id"])[:64]
            notification.sent_at = datetime.now(UTC)
            return None

        settings = NotificationSettings.model_validate(user.notification_settings)
        if request.channel is NotificationChannel.IN_APP:
            reason: str | None = "in_app"
        elif not getattr(settings, request.channel.value):
            reason = "channel_disabled"
        elif request.channel is NotificationChannel.EMAIL and not user.email:
            reason = "no_recipient"
        else:
            reason = None
        if reason is not None:
            _skip(notification, reason)
            return None
        return _build_message(notification, user, request)

    async def _deliver(
        self, session: AsyncSession, notification: Notification, message: OutgoingMessage
    ) -> None:
        try:
            result = await self._gateway.send(message, notification.dedup_key)
        except NotificationDeliveryError as exc:
            notification.delivery_status = NotificationDeliveryStatus.FAILED
            notification.delivery_attempts += exc.attempts
            notification.delivery_note = str(exc)[:255]
            await session.commit()
            logger.error(  # noqa: TRY400 - the reason is in the message
                "notification delivery failed",
                notification_id=str(notification.id),
                channel=notification.channel.value,
                attempts=exc.attempts,
                error=str(exc),
            )
            return
        notification.delivery_status = NotificationDeliveryStatus.SENT
        notification.delivery_attempts += result.attempts
        notification.provider_message_id = result.message_id[:64]
        notification.sent_at = datetime.now(UTC)
        await session.commit()
        logger.info(
            "notification sent",
            notification_id=str(notification.id),
            channel=notification.channel.value,
            attempts=result.attempts,
        )


def _build_message(notification: Notification, user: User, request: _Request) -> OutgoingMessage:
    title, text = render(request.template, request.data)
    match request.channel:
        case NotificationChannel.SMS:
            return SmsMessage(phone=user.phone, text=text)
        case NotificationChannel.EMAIL:
            assert user.email is not None  # checked by the caller
            return EmailMessage(to=user.email, subject=title, text=text)
        case NotificationChannel.PUSH:
            return PushMessage(
                user_id=user.id,
                phone=user.phone,
                title=title,
                body=text,
                data={"notification_id": str(notification.id), "type": notification.type},
            )
        case NotificationChannel.IN_APP:
            raise ValueError("in-app notifications are not sent through a gateway")


def _skip(notification: Notification, reason: str) -> None:
    notification.delivery_status = NotificationDeliveryStatus.SKIPPED
    notification.delivery_note = reason
    logger.info(
        "notification stored without delivery",
        notification_id=str(notification.id),
        reason=reason,
    )


class NotificationService:
    """API side: the user's notifications."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_for_user(
        self, user: User, *, unread: bool, limit: int, offset: int
    ) -> tuple[list[Notification], int, int]:
        """(page, total matching, unread count)."""
        where = [Notification.user_id == user.id]
        if unread:
            where.append(Notification.read_at.is_(None))
        total = await self._session.scalar(
            select(func.count()).select_from(Notification).where(*where)
        )
        unread_count = await self._session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.user_id == user.id, Notification.read_at.is_(None))
        )
        rows = await self._session.scalars(
            select(Notification)
            .where(*where)
            .order_by(Notification.created_at.desc(), Notification.id.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(rows), total or 0, unread_count or 0

    async def mark_read(self, user: User, notification_id: uuid.UUID) -> Notification:
        """Idempotent: an already read notification keeps its ``read_at``."""
        notification = await self._session.scalar(
            select(Notification).where(
                Notification.id == notification_id, Notification.user_id == user.id
            )
        )
        if notification is None:
            raise NotFoundError("Notification not found")
        if notification.read_at is None:
            notification.read_at = datetime.now(UTC)
            await self._session.commit()
        return notification

    async def mark_all_read(self, user: User) -> int:
        result = await self._session.execute(
            update(Notification)
            .where(Notification.user_id == user.id, Notification.read_at.is_(None))
            .values(read_at=func.now())
        )
        await self._session.commit()
        return int(result.rowcount)  # type: ignore[attr-defined]
