"""Notification gateways (WireMock): ``POST /sms/send``, ``/email/send``, ``/push/send``.

Requests::

    /sms/send    {"phone": "+7...", "text": "..."}
    /email/send  {"to": "a@b.c", "subject": "...", "text": "..."}
    /push/send   {"user_id": "...", "phone": "+7...", "title": "...", "body": "...", "data": {...}}

Response 2xx: ``{"message_id": "...", "status": "..."}``.

Retry policy: 5xx, timeouts and connection errors are retried
(``NOTIFICATIONS_RETRIES`` extra attempts, exponential backoff), 4xx never. Every
request carries ``Idempotency-Key: <notification dedup key>``, so a retry after a
timeout does not deliver twice. Final failure → :class:`NotificationDeliveryError`
(``attempts`` = how many requests were made).
"""

import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from elbank.clients.external_transfer import IDEMPOTENCY_HEADER
from elbank.clients.http import is_server_or_transport_error, retrying
from elbank.core.config import NotificationDeliverySettings
from elbank.core.logging import get_logger

logger = get_logger(__name__)


class GatewayAnswer(BaseModel):
    model_config = ConfigDict(extra="ignore")

    message_id: str
    status: str


@dataclass(slots=True, frozen=True)
class DeliveryResult:
    message_id: str
    attempts: int


class NotificationDeliveryError(Exception):
    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


@dataclass(slots=True, frozen=True)
class SmsMessage:
    phone: str
    text: str


@dataclass(slots=True, frozen=True)
class EmailMessage:
    to: str
    subject: str
    text: str


@dataclass(slots=True, frozen=True)
class PushMessage:
    user_id: uuid.UUID
    phone: str  # device binding in this emulator: the gateway resolves devices by phone
    title: str
    body: str
    data: dict[str, Any]


type OutgoingMessage = SmsMessage | EmailMessage | PushMessage


class NotificationGatewayClient:
    def __init__(self, http: httpx.AsyncClient, settings: NotificationDeliverySettings) -> None:
        self._http = http
        self._settings = settings

    async def send(self, message: OutgoingMessage, idempotency_key: str) -> DeliveryResult:
        path, body = self._request(message)
        service = f"notifications:{path}"
        attempts = 0
        try:
            async for attempt in retrying(
                self._settings.retries, service, retry_if=is_server_or_transport_error
            ):
                with attempt:
                    attempts += 1
                    response = await self._http.post(
                        path,
                        json=body,
                        headers={IDEMPOTENCY_HEADER: idempotency_key},
                        timeout=self._settings.timeout_seconds,
                    )
                    response.raise_for_status()
            answer = GatewayAnswer.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError) as exc:
            raise NotificationDeliveryError(
                f"{service}: {type(exc).__name__}: {exc}"[:255], attempts
            ) from exc
        return DeliveryResult(answer.message_id, attempts)

    @staticmethod
    def _request(message: OutgoingMessage) -> tuple[str, dict[str, Any]]:
        match message:
            case SmsMessage():
                return "/sms/send", {"phone": message.phone, "text": message.text}
            case EmailMessage():
                return "/email/send", {
                    "to": message.to,
                    "subject": message.subject,
                    "text": message.text,
                }
            case PushMessage():
                return "/push/send", {
                    "user_id": str(message.user_id),
                    "phone": message.phone,
                    "title": message.title,
                    "body": message.body,
                    "data": message.data,
                }
