"""Domain events published to Kafka in a common envelope.

Envelope::

    {"event_id": "...", "event_type": "user.registered", "occurred_at": "...",
     "correlation_id": "...", "payload": {...}}

The topic name equals ``event_type``. The correlation id and the event metadata
are also sent as Kafka record headers so consumers can route/trace without
parsing the value.
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from elbank.core.context import get_correlation_id
from elbank.core.logging import get_logger
from elbank.kafka.producer import KafkaProducer

logger = get_logger(__name__)


class EventType(StrEnum):
    USER_REGISTERED = "user.registered"
    USER_DELETED = "user.deleted"
    PROFILE_UPDATED = "profile.updated"
    NOTIFICATION_SEND = "notification.send"
    KYC_REQUESTED = "kyc.requested"
    KYC_DECIDED = "kyc.decided"
    ACCOUNT_OPENED = "account.opened"
    ACCOUNT_CLOSED = "account.closed"
    ACCOUNT_BLOCKED = "account.blocked"
    ACCOUNT_UNBLOCKED = "account.unblocked"
    STATEMENT_REQUESTED = "statement.requested"
    TRANSFER_COMPLETED = "transfer.completed"
    TRANSFER_FAILED = "transfer.failed"
    TRANSFER_REVERSED = "transfer.reversed"
    PAYMENT_PROCESSED = "payment.processed"
    PAYMENT_FAILED = "payment.failed"


class EventEnvelope(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str | None = Field(default_factory=get_correlation_id)
    payload: dict[str, Any]

    def kafka_headers(self) -> list[tuple[str, bytes]]:
        headers = [
            ("event_id", str(self.event_id).encode()),
            ("event_type", self.event_type.value.encode()),
        ]
        if self.correlation_id:
            headers.append(("correlation_id", self.correlation_id.encode()))
        return headers


class EventPublisher:
    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer

    async def publish(
        self, event_type: EventType, payload: dict[str, Any], key: str | None = None
    ) -> EventEnvelope:
        """Publish an event; raises if Kafka is unavailable."""
        envelope = EventEnvelope(event_type=event_type, payload=payload)
        await self._producer.send(
            envelope.event_type.value,
            value=envelope.model_dump(mode="json"),
            key=key,
            headers=envelope.kafka_headers(),
        )
        logger.info(
            "event published",
            event_type=envelope.event_type.value,
            event_id=str(envelope.event_id),
        )
        return envelope

    async def publish_safely(
        self, event_type: EventType, payload: dict[str, Any], key: str | None = None
    ) -> bool:
        """Publish an event after the business transaction is already committed.

        A Kafka outage must not turn a completed operation into an HTTP error,
        so failures are logged (and visible in Kibana) instead of raised.
        """
        try:
            await self.publish(event_type, payload, key)
        except Exception as exc:  # noqa: BLE001 - delivery failure is reported, not propagated
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "event publish failed",
                event_type=event_type.value,
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        return True
