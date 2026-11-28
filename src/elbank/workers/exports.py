"""Exports worker: consumes ``export.requested`` and renders CSV/PDF operation history.

Run: ``elbank-exports-worker`` (or ``python -m elbank.workers.exports``).

Handling is idempotent: a job that is already ``ready`` or has expired is skipped.
A failed generation marks the job ``failed`` (the client sees the reason and
requests a new export). Infrastructure errors (Redis down, Kafka commit failed)
stop the worker; the container restarts and continues from the last committed
offset. Files older than the job TTL are removed hourly.
"""

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime

import structlog
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventType
from elbank.schemas.operation import ExportJobStatus
from elbank.services.export_builder import ExportBuilder, ExportTooLargeError
from elbank.services.exports import ExportJob, ExportJobStore
from elbank.workers.consumer import EventConsumerWorker, MalformedEventError, create_consumer

logger = get_logger(__name__)

_CLEANUP_INTERVAL_SECONDS = 3600.0


class ExportHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        store: ExportJobStore,
        builder: ExportBuilder,
    ) -> None:
        self._session_factory = session_factory
        self._store = store
        self._builder = builder

    async def __call__(self, envelope: EventEnvelope) -> None:
        try:
            job_id = uuid.UUID(str(envelope.payload["job_id"]))
        except (KeyError, ValueError) as exc:
            raise MalformedEventError(f"bad job_id: {exc}") from exc
        with structlog.contextvars.bound_contextvars(job_id=str(job_id)):
            await self._process(job_id)

    async def _process(self, job_id: uuid.UUID) -> None:
        job = await self._store.get(job_id)
        if job is None:
            logger.warning("export job expired before processing")
            return
        if job.status is ExportJobStatus.READY:
            logger.info("export job already done, skipped")
            return

        job.status = ExportJobStatus.PROCESSING
        await self._store.update(job)
        try:
            async with self._session_factory() as session:
                rows = await self._builder.build(session, job)
        except ExportTooLargeError as exc:
            logger.warning("export too large", error=str(exc))
            await self._finish(job, ExportJobStatus.FAILED, error=str(exc))
            return
        except Exception:
            logger.exception("export generation failed")
            await self._finish(job, ExportJobStatus.FAILED, error="Export generation failed")
            return
        await self._finish(job, ExportJobStatus.READY, rows=rows)
        logger.info("export ready", format=job.format.value, rows=rows)

    async def _finish(
        self,
        job: ExportJob,
        status: ExportJobStatus,
        *,
        rows: int | None = None,
        error: str | None = None,
    ) -> None:
        job.status = status
        job.rows = rows
        job.error = error
        job.completed_at = datetime.now(UTC)
        if not await self._store.update(job):
            logger.warning("export job expired while processing")


async def _cleanup_loop(builder: ExportBuilder) -> None:
    while True:
        try:
            removed = await builder.remove_expired_files()
        except OSError as exc:
            logger.warning("export files cleanup failed", error=str(exc))
        else:
            if removed:
                logger.info("expired export files removed", count=removed)
        await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    builder = ExportBuilder(settings.exports)
    handler = ExportHandler(
        create_session_factory(engine),
        ExportJobStore(redis, settings.exports.job_ttl_seconds),
        builder,
    )
    worker = EventConsumerWorker(
        "exports",
        create_consumer(
            settings, EventType.EXPORT_REQUESTED, settings.exports.consumer_group, "exports"
        ),
        handler,
    )
    worker.install_signal_handlers()
    cleanup = asyncio.create_task(_cleanup_loop(builder), name="exports-cleanup")
    try:
        await worker.run()
    finally:
        cleanup.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
