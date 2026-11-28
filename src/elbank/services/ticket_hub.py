"""Fan-out of ``ticket.updated`` to WebSocket subscribers of this API process.

Every API process runs one Kafka consumer of ``ticket.updated`` *without* a consumer
group (each process must see every event; nothing is committed) reading from the
latest offset: a WebSocket delivers live updates only, history comes from
``GET /support/tickets/{id}``. Each WebSocket owns a bounded ``asyncio.Queue``;
an event is put to the queues of its ticket's subscribers. A subscriber whose queue
is full has fallen behind: its queue is replaced by an overflow marker and the
socket is closed (the client reconnects and reloads the ticket).

The consumer starts in the background and retries while Kafka is unavailable, so
the app boots without Kafka; a consumer failure restarts it after a pause.
"""

import asyncio
import contextlib
import uuid
from collections import defaultdict
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import ValidationError

from elbank.core.config import KafkaSettings
from elbank.core.logging import get_logger
from elbank.kafka import EventEnvelope, EventType

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 1000
_RETRY_SECONDS = 5.0


class TicketSubscription:
    """One WebSocket's view of one ticket. ``None`` in the queue means "overflowed"."""

    def __init__(self, ticket_id: uuid.UUID, queue_size: int) -> None:
        self.ticket_id = ticket_id
        # One slot more than the limit, reserved for the overflow marker.
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(queue_size + 1)
        self._limit = queue_size
        self.overflowed = False

    def deliver(self, event: dict[str, Any]) -> None:
        if self.overflowed:
            return
        if self._queue.qsize() >= self._limit:
            self.overflowed = True
            self._queue.put_nowait(None)
            return
        self._queue.put_nowait(event)

    async def get(self) -> dict[str, Any] | None:
        return await self._queue.get()


class TicketEventHub:
    def __init__(self, settings: KafkaSettings, queue_size: int) -> None:
        self._settings = settings
        self._queue_size = queue_size
        self._subscribers: dict[uuid.UUID, set[TicketSubscription]] = defaultdict(set)
        self._task: asyncio.Task[None] | None = None

    # ---------- subscribers ----------

    def subscribe(self, ticket_id: uuid.UUID) -> TicketSubscription:
        subscription = TicketSubscription(ticket_id, self._queue_size)
        self._subscribers[ticket_id].add(subscription)
        return subscription

    def unsubscribe(self, subscription: TicketSubscription) -> None:
        subscribers = self._subscribers.get(subscription.ticket_id)
        if subscribers is None:
            return
        subscribers.discard(subscription)
        if not subscribers:
            del self._subscribers[subscription.ticket_id]

    @property
    def subscriber_count(self) -> int:
        return sum(len(s) for s in self._subscribers.values())

    def dispatch(self, envelope: EventEnvelope) -> None:
        """Hand an event to the subscribers of its ticket (no-op without subscribers)."""
        try:
            ticket_id = uuid.UUID(str(envelope.payload["ticket_id"]))
        except KeyError, ValueError:
            logger.warning(
                "ticket event without ticket_id skipped", event_id=str(envelope.event_id)
            )
            return
        subscribers = self._subscribers.get(ticket_id)
        if not subscribers:
            return
        event = {
            "event_id": str(envelope.event_id),
            "occurred_at": envelope.occurred_at.isoformat(),
            **envelope.payload,
        }
        for subscription in list(subscribers):
            subscription.deliver(event)

    # ---------- consumer ----------

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="ticket-event-hub")

    async def stop(self) -> None:
        if self._task is None:
            return
        task, self._task = self._task, None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                EventType.TICKET_UPDATED.value,
                bootstrap_servers=self._settings.servers,
                client_id=f"{self._settings.client_id}-ticket-hub",
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="latest",
            )
            try:
                await consumer.start()
                logger.info("ticket event hub started")
                while True:
                    batches = await consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                    for records in batches.values():
                        for record in records:
                            self._handle(record)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - Kafka outage: retry, the app keeps serving
                logger.warning("ticket event hub consumer failed, retrying", error=str(exc))
            finally:
                with contextlib.suppress(Exception):
                    await consumer.stop()
            await asyncio.sleep(_RETRY_SECONDS)

    def _handle(self, record: ConsumerRecord) -> None:
        try:
            envelope = EventEnvelope.model_validate_json(record.value)
        except ValidationError as exc:
            logger.error(  # noqa: TRY400 - the reason is in the message
                "malformed ticket event skipped",
                partition=record.partition,
                offset=record.offset,
                error=str(exc),
            )
            return
        self.dispatch(envelope)
