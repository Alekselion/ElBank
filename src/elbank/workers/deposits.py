"""Deposits worker: consumes ``deposit.interest.tick`` and accrues deposit interest.

Run: ``elbank-deposits-worker`` (or ``python -m elbank.workers.deposits``).

A tick ``{"as_of": "YYYY-MM-DD"}`` posts interest of every active deposit for the
days before ``as_of``. In production a cron job publishes it daily; here it is
``POST /api/v1/admin/deposits/accrue``. Accrual is idempotent (it starts from each
deposit's ``accrued_until``), so offsets are committed after a tick is handled and a
replayed tick changes nothing. A failed tick (DB down) stops the worker without
committing: the container restarts and handles it again.
"""

import asyncio
import contextlib
import signal
from datetime import date

import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import ValidationError

from elbank.core.config import Settings, get_settings
from elbank.core.context import correlation_id_var
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventType
from elbank.services.deposits import DepositAccrualJob

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 1000
_START_RETRY_SECONDS = 5.0


class DepositWorker:
    def __init__(self, consumer: AIOKafkaConsumer, job: DepositAccrualJob) -> None:
        self._consumer = consumer
        self._job = job
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        if not await self._start_consumer():
            return
        logger.info("deposit worker started", topic=EventType.DEPOSIT_INTEREST_TICK.value)
        try:
            while not self._stopping.is_set():
                batches = await self._consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                for partition, records in batches.items():
                    for record in records:
                        await self._handle(record)
                        await self._consumer.commit({partition: record.offset + 1})
        finally:
            await self._consumer.stop()
            logger.info("deposit worker stopped")

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
            as_of = date.fromisoformat(str(envelope.payload["as_of"]))
        except (ValidationError, KeyError, ValueError) as exc:
            # A malformed message can never succeed: skip it instead of blocking the partition.
            logger.error(  # noqa: TRY400 - the reason is in the message
                "malformed accrual tick skipped",
                partition=record.partition,
                offset=record.offset,
                error=str(exc),
            )
            return

        token = correlation_id_var.set(envelope.correlation_id)
        try:
            with structlog.contextvars.bound_contextvars(
                correlation_id=envelope.correlation_id, as_of=as_of.isoformat()
            ):
                await self._job.run(as_of)
        finally:
            correlation_id_var.reset(token)


def _create_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        EventType.DEPOSIT_INTEREST_TICK.value,
        bootstrap_servers=settings.kafka.servers,
        client_id=f"{settings.kafka.client_id}-deposits",
        group_id=settings.deposits.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    worker = DepositWorker(
        consumer=_create_consumer(settings),
        job=DepositAccrualJob(create_session_factory(engine), settings.deposits),
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
