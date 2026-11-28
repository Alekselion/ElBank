"""Shared loop of the Kafka-consumer workers.

- The consumer is started with retries while Kafka is unavailable.
- Offsets are committed manually after a message is handled: a crash replays it
  (at-least-once), so handlers must be idempotent.
- A message that can never be handled (bad JSON, missing fields) raises
  :class:`MalformedEventError` and is skipped instead of blocking the partition.
- Any other error stops the worker without committing: the container restarts
  and handles the message again.
- The correlation id of the event is bound to the logs of its handling.
"""

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable

import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import ValidationError

from elbank.core.config import Settings
from elbank.core.context import correlation_id_var
from elbank.core.logging import get_logger
from elbank.kafka import EventEnvelope, EventType

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 1000
_START_RETRY_SECONDS = 5.0

type EventHandler = Callable[[EventEnvelope], Awaitable[None]]


class MalformedEventError(ValueError):
    """The event payload is invalid; retrying cannot help."""


class EventConsumerWorker:
    def __init__(self, name: str, consumer: AIOKafkaConsumer, handler: EventHandler) -> None:
        self._name = name
        self._consumer = consumer
        self._handler = handler
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop)

    async def run(self) -> None:
        if not await self._start_consumer():
            return
        logger.info(
            "worker started", worker=self._name, topics=sorted(self._consumer.subscription())
        )
        try:
            while not self._stopping.is_set():
                batches = await self._consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                for partition, records in batches.items():
                    for record in records:
                        await self._handle(record)
                        await self._consumer.commit({partition: record.offset + 1})
        finally:
            await self._consumer.stop()
            logger.info("worker stopped", worker=self._name)

    async def _start_consumer(self) -> bool:
        """Start the consumer, retrying while Kafka is unavailable; False if stopped meanwhile."""
        while not self._stopping.is_set():
            try:
                await self._consumer.start()
            except Exception as exc:  # noqa: BLE001 - broker not reachable yet, retry
                logger.warning("kafka consumer start failed, retrying", error=str(exc))
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), _START_RETRY_SECONDS)
            else:
                return True
        return False

    async def _handle(self, record: ConsumerRecord) -> None:
        try:
            envelope = EventEnvelope.model_validate_json(record.value)
        except ValidationError as exc:
            self._skip(record, str(exc))
            return

        token = correlation_id_var.set(envelope.correlation_id)
        try:
            with structlog.contextvars.bound_contextvars(
                correlation_id=envelope.correlation_id, event_id=str(envelope.event_id)
            ):
                await self._handler(envelope)
        except MalformedEventError as exc:
            self._skip(record, str(exc))
        finally:
            correlation_id_var.reset(token)

    def _skip(self, record: ConsumerRecord, reason: str) -> None:
        logger.error(
            "malformed event skipped",
            worker=self._name,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            error=reason,
        )


def create_consumer(
    settings: Settings, topic: EventType, group_id: str, suffix: str
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic.value,
        bootstrap_servers=settings.kafka.servers,
        client_id=f"{settings.kafka.client_id}-{suffix}",
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
