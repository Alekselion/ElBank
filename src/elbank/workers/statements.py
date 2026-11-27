"""Statement worker: consumes ``statement.requested`` and renders CSV statements.

Run: ``elbank-statements-worker`` (or ``python -m elbank.workers.statements``).

Offsets are committed manually after a message is handled, so a crash replays
unfinished jobs (at-least-once); handling is idempotent — a job that is already
``ready`` or has expired is skipped. A failed generation marks the job
``failed``: the client sees it and requests a new statement. Infrastructure
errors (Redis down, Kafka commit failed) stop the worker; the container restarts
and continues from the last committed offset.
"""

import asyncio
import contextlib
import signal
import uuid
from datetime import UTC, datetime

import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import Settings, get_settings
from elbank.core.context import correlation_id_var
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventType
from elbank.schemas.account import StatementJobStatus
from elbank.services.statements import StatementBuilder, StatementJob, StatementJobStore

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 1000
_START_RETRY_SECONDS = 5.0
_CLEANUP_INTERVAL_SECONDS = 3600.0


class StatementWorker:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        session_factory: async_sessionmaker[AsyncSession],
        store: StatementJobStore,
        builder: StatementBuilder,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._store = store
        self._builder = builder
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        if not await self._start_consumer():
            return
        cleanup = asyncio.create_task(self._cleanup_loop(), name="statements-cleanup")
        logger.info("statement worker started", topic=EventType.STATEMENT_REQUESTED.value)
        try:
            while not self._stopping.is_set():
                batches = await self._consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                for partition, records in batches.items():
                    for record in records:
                        await self._handle(record)
                        await self._consumer.commit({partition: record.offset + 1})
        finally:
            cleanup.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cleanup
            await self._consumer.stop()
            logger.info("statement worker stopped")

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
            job_id = uuid.UUID(str(envelope.payload["job_id"]))
        except (ValidationError, KeyError, ValueError) as exc:
            # A malformed message can never succeed: skip it instead of blocking the partition.
            logger.error(  # noqa: TRY400 - the reason is in the message
                "malformed statement event skipped",
                partition=record.partition,
                offset=record.offset,
                error=str(exc),
            )
            return

        token = correlation_id_var.set(envelope.correlation_id)
        try:
            with structlog.contextvars.bound_contextvars(
                correlation_id=envelope.correlation_id, job_id=str(job_id)
            ):
                await self._process(job_id)
        finally:
            correlation_id_var.reset(token)

    async def _process(self, job_id: uuid.UUID) -> None:
        job = await self._store.get(job_id)
        if job is None:
            logger.warning("statement job expired before processing")
            return
        if job.status is StatementJobStatus.READY:
            logger.info("statement job already done, skipped")
            return

        job.status = StatementJobStatus.PROCESSING
        await self._store.update(job)
        try:
            async with self._session_factory() as session:
                rows = await self._builder.build(session, job)
        except Exception:
            logger.exception("statement generation failed")
            await self._finish(job, StatementJobStatus.FAILED, error="Statement generation failed")
            return
        await self._finish(job, StatementJobStatus.READY, rows=rows)
        logger.info("statement ready", account_id=str(job.account_id), rows=rows)

    async def _finish(
        self,
        job: StatementJob,
        status: StatementJobStatus,
        *,
        rows: int | None = None,
        error: str | None = None,
    ) -> None:
        job.status = status
        job.rows = rows
        job.error = error
        job.completed_at = datetime.now(UTC)
        if not await self._store.update(job):
            logger.warning("statement job expired while processing")

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                removed = await self._builder.remove_expired_files()
            except OSError as exc:
                logger.warning("statement files cleanup failed", error=str(exc))
            else:
                if removed:
                    logger.info("expired statement files removed", count=removed)
            await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)


def _create_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        EventType.STATEMENT_REQUESTED.value,
        bootstrap_servers=settings.kafka.servers,
        client_id=f"{settings.kafka.client_id}-statements",
        group_id=settings.statements.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    worker = StatementWorker(
        consumer=_create_consumer(settings),
        session_factory=create_session_factory(engine),
        store=StatementJobStore(redis, settings.statements.job_ttl_seconds),
        builder=StatementBuilder(settings.statements),
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
