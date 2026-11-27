"""Transfers worker: background state changes of transfers.

Run: ``elbank-transfers-worker`` (or ``python -m elbank.workers.transfers``).

Every ``TRANSFERS_FINALIZER_INTERVAL_SECONDS``:

- completes internal transfers whose cancel window has passed
  (``transfer.completed``);
- fails external/SBP transfers left unconfirmed after a gateway timeout or stuck
  in ``processing``, returning the money (``transfer.failed``).

Rows are taken with ``SELECT ... FOR UPDATE SKIP LOCKED``, so several replicas are
safe. A full batch is followed by the next one immediately. Errors are logged and
retried on the next tick.
"""

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventPublisher, KafkaProducer
from elbank.services.account_cache import AccountListCache
from elbank.services.external_transfers import ExternalTimeoutResolver
from elbank.services.transfers import TransferFinalizer

logger = get_logger(__name__)


class TransferWorker:
    def __init__(self, jobs: dict[str, Callable[[], Awaitable[int]]], settings: Settings) -> None:
        self._jobs = jobs
        self._interval = settings.transfers.finalizer_interval_seconds
        self._batch_size = settings.transfers.finalizer_batch_size
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        logger.info("transfers worker started")
        while not self._stopping.is_set():
            busy = False
            for name, job in self._jobs.items():
                try:
                    handled = await job()
                except Exception:
                    logger.exception("transfers job failed", job=name)
                    handled = 0
                busy = busy or handled >= self._batch_size
            if busy:
                continue  # more due transfers are likely waiting
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), self._interval)
        logger.info("transfers worker stopped")


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    kafka = KafkaProducer(settings.kafka)
    session_factory = create_session_factory(engine)
    publisher = EventPublisher(kafka)
    cache = AccountListCache(redis, settings.accounts.list_cache_ttl_seconds)
    finalizer = TransferFinalizer(session_factory, publisher, settings.transfers)
    resolver = ExternalTimeoutResolver(session_factory, cache, publisher, settings.transfers)
    worker = TransferWorker(
        {"finalize_internal": finalizer.run_once, "resolve_external": resolver.run_once},
        settings,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await kafka.stop()
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
