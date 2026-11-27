"""Transfers worker: completes pending transfers once their cancel window has passed.

Run: ``elbank-transfers-worker`` (or ``python -m elbank.workers.transfers``).

Every ``TRANSFERS_FINALIZER_INTERVAL_SECONDS`` it takes due transfers with
``SELECT ... FOR UPDATE SKIP LOCKED`` (several replicas are safe), moves their
operations to ``completed`` and publishes ``transfer.completed``. A full batch
is followed by the next one immediately. Errors are logged and retried on the
next tick.
"""

import asyncio
import contextlib
import signal

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventPublisher, KafkaProducer
from elbank.services.transfers import TransferFinalizer

logger = get_logger(__name__)


class TransferWorker:
    def __init__(self, finalizer: TransferFinalizer, settings: Settings) -> None:
        self._finalizer = finalizer
        self._interval = settings.transfers.finalizer_interval_seconds
        self._batch_size = settings.transfers.finalizer_batch_size
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        logger.info("transfers worker started")
        while not self._stopping.is_set():
            try:
                completed = await self._finalizer.run_once()
            except Exception:
                logger.exception("transfer finalization failed")
                completed = 0
            if completed >= self._batch_size:
                continue  # more due transfers are likely waiting
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), self._interval)
        logger.info("transfers worker stopped")


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    kafka = KafkaProducer(settings.kafka)
    worker = TransferWorker(
        TransferFinalizer(
            create_session_factory(engine), EventPublisher(kafka), settings.transfers
        ),
        settings,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await kafka.stop()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
