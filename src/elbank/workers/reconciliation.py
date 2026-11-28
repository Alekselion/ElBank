"""Reconciliation worker: consumes ``reconciliation.requested`` and writes a JSON report.

Run: ``elbank-reconciliation-worker`` (or ``python -m elbank.workers.reconciliation``).

All checks run in one read-only ``REPEATABLE READ`` transaction, so they see one
consistent state of the ledger. The report goes to ``{RECONCILIATION_DIR}/{run_id}.json``
(``/app/uploads/recon`` in docker-compose) and is served by
``GET /api/v1/admin/reconciliation/{run_id}``. Handling is idempotent: a run whose
report already exists is skipped. DB or file system errors stop the worker without
committing the offset; the container restarts and the run is retried.
"""

import asyncio

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventPublisher, EventType, KafkaProducer
from elbank.services.audit_service import AuditService
from elbank.services.reconciliation import ReconciliationBuilder, ReconciliationRequest
from elbank.workers.consumer import EventConsumerWorker, MalformedEventError, create_consumer

logger = get_logger(__name__)


class ReconciliationHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        builder: ReconciliationBuilder,
        audit: AuditService,
    ) -> None:
        self._session_factory = session_factory
        self._builder = builder
        self._audit = audit

    async def __call__(self, envelope: EventEnvelope) -> None:
        try:
            request = ReconciliationRequest.model_validate(envelope.payload)
        except ValidationError as exc:
            raise MalformedEventError(str(exc)) from exc

        with structlog.contextvars.bound_contextvars(run_id=str(request.run_id)):
            if self._builder.exists(request.run_id):
                logger.info("reconciliation report already exists, skipped")
                return
            async with self._session_factory() as session:
                # Must be the first statement of the transaction to set its isolation.
                await session.connection(
                    execution_options={
                        "isolation_level": "REPEATABLE READ",
                        "postgresql_readonly": True,
                    }
                )
                report = await self._builder.build(session, request)
                await session.rollback()  # read-only: nothing to commit
            path = await asyncio.to_thread(self._builder.write, report)

            issues = {name: check.count for name, check in report.checks.items() if check.count}
            logger.info(
                "reconciliation report written",
                path=str(path),
                status=report.status.value,
                issues=issues,
            )
            await self._audit.record(
                "reconciliation.completed",
                user_id=None,  # the system
                entity="reconciliation",
                entity_id=request.run_id,
                payload={
                    "status": report.status,
                    "issues": issues,
                    "requested_by": request.requested_by,
                },
            )


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    kafka = KafkaProducer(settings.kafka)
    handler = ReconciliationHandler(
        create_session_factory(engine),
        ReconciliationBuilder(settings.reconciliation),
        AuditService(EventPublisher(kafka)),
    )
    worker = EventConsumerWorker(
        "reconciliation",
        create_consumer(
            settings,
            EventType.RECONCILIATION_REQUESTED,
            settings.reconciliation.consumer_group,
            "reconciliation",
        ),
        handler,
    )
    worker.install_signal_handlers()
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
