"""Operations worker: reacts to ``operation.completed``.

Run: ``elbank-operations-worker`` (or ``python -m elbank.workers.operations``).

For every completed operation:

1. drops the analytics cache of the account owner (``analytics:{user_id}``);
2. checks the owner's budget for the operation's category and currency and sends
   ``notification.send`` (``warn`` from 80 %, ``danger`` from 100 %), once per level
   and month — see :mod:`elbank.services.budgets`.

Replays are harmless: invalidation is idempotent and alerts are deduplicated in
Redis. DB/Redis errors stop the worker without committing the offset (restart and
retry). Safe to scale up to the number of partitions of ``operation.completed``.
"""

import asyncio
import uuid

import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.models import Account, Operation, PaymentOrder
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventPublisher, EventType, KafkaProducer
from elbank.services.analytics import AnalyticsCache
from elbank.services.budgets import BudgetAlerts, BudgetChecker
from elbank.workers.consumer import EventConsumerWorker, MalformedEventError, create_consumer

logger = get_logger(__name__)


class OperationCompletedHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        cache: AnalyticsCache,
        checker: BudgetChecker,
    ) -> None:
        self._session_factory = session_factory
        self._cache = cache
        self._checker = checker

    async def __call__(self, envelope: EventEnvelope) -> None:
        try:
            operation_id = uuid.UUID(str(envelope.payload["operation_id"]))
        except (KeyError, ValueError) as exc:
            raise MalformedEventError(f"bad operation_id: {exc}") from exc

        with structlog.contextvars.bound_contextvars(operation_id=str(operation_id)):
            async with self._session_factory() as session:
                row = (
                    (
                        await session.execute(
                            select(Operation, Account.user_id, PaymentOrder.provider)
                            .join(Account, Account.id == Operation.account_id)
                            .outerjoin(PaymentOrder, PaymentOrder.operation_id == Operation.id)
                            .where(Operation.id == operation_id)
                        )
                    )
                    .tuples()
                    .one_or_none()
                )
                if row is None:
                    logger.warning("completed operation not found, skipped")
                    return
                operation, owner_id, provider = row
                await self._cache.invalidate(owner_id)
                await self._checker.check(session, operation, owner_id, provider)


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    kafka = KafkaProducer(settings.kafka)
    handler = OperationCompletedHandler(
        create_session_factory(engine),
        AnalyticsCache(redis, settings.analytics.cache_ttl_seconds),
        BudgetChecker(BudgetAlerts(redis), EventPublisher(kafka), settings.budgets),
    )
    worker = EventConsumerWorker(
        "operations",
        create_consumer(
            settings, EventType.OPERATION_COMPLETED, settings.budgets.consumer_group, "operations"
        ),
        handler,
    )
    worker.install_signal_handlers()
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
