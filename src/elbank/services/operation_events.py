"""``operation.completed``: published for every operation that reaches ``completed``.

Operations complete in many services (API and workers), always through
:meth:`OperationService.mark_completed` and inside a transaction the caller commits.
The event may only leave after that commit, so:

1. ``mark_completed`` calls :func:`record_completed`: a payload snapshot is stored in
   ``session.info``;
2. the ``after_commit`` hook of :class:`OperationEventSession` hands the snapshots to
   the :class:`OperationEventRelay` bound to the session factory, which publishes
   them in a background task;
3. ``after_rollback`` drops them: a rolled back completion never happened.

Delivery is best effort, like every post-commit event here (``publish_safely``):
consumers must tolerate a lost event — the analytics cache also expires by TTL.
:meth:`OperationEventRelay.drain` waits for in-flight publications on shutdown.

Payload::

    {"operation_id", "account_id", "user_id" (initiator or null), "type", "amount",
     "currency", "parent_operation_id", "reversed_operation_id", "completed_at"}
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session

from elbank.core.logging import get_logger
from elbank.db.models import Operation
from elbank.kafka import EventPublisher, EventType

logger = get_logger(__name__)

_PENDING_KEY = "elbank.operations.completed"
_RELAY_KEY = "elbank.operations.relay"


def _payload(operation: Operation) -> dict[str, Any]:
    def opt(value: object | None) -> str | None:
        return None if value is None else str(value)

    return {
        "operation_id": str(operation.id),
        "account_id": str(operation.account_id),
        "user_id": opt(operation.user_id),
        "type": operation.type.value,
        "amount": format(operation.amount, "f"),
        "currency": operation.currency,
        "parent_operation_id": opt(operation.parent_operation_id),
        "reversed_operation_id": opt(operation.reversed_operation_id),
        "completed_at": datetime.now(UTC).isoformat(),
    }


def record_completed(session: AsyncSession, operation: Operation) -> None:
    """Remember that ``operation`` completes in the current transaction of ``session``."""
    pending: list[dict[str, Any]] = session.info.setdefault(_PENDING_KEY, [])
    pending.append(_payload(operation))


class OperationEventRelay:
    """Publishes recorded completions after commit; owns the background tasks."""

    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher
        self._tasks: set[asyncio.Task[None]] = set()

    def submit(self, payloads: list[dict[str, Any]]) -> None:
        # Called from a sync SQLAlchemy hook; with AsyncSession it runs in the event
        # loop thread (greenlet), and the task inherits the correlation id context.
        task = asyncio.get_running_loop().create_task(self._publish(payloads))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _publish(self, payloads: list[dict[str, Any]]) -> None:
        for payload in payloads:
            await self._publisher.publish_safely(
                EventType.OPERATION_COMPLETED, payload, key=payload["account_id"]
            )

    async def drain(self, grace_seconds: float) -> None:
        """Wait (bounded) for publications still in flight, e.g. before stopping Kafka."""
        if self._tasks:
            _, pending = await asyncio.wait(set(self._tasks), timeout=grace_seconds)
            if pending:
                logger.warning("operation events not published before shutdown", count=len(pending))


class OperationEventSession(Session):
    """Sync session class whose commit/rollback hooks drive :class:`OperationEventRelay`."""


@event.listens_for(OperationEventSession, "after_commit")
def _after_commit(session: Session) -> None:
    payloads: list[dict[str, Any]] | None = session.info.pop(_PENDING_KEY, None)
    if not payloads:
        return
    relay: OperationEventRelay | None = session.info.get(_RELAY_KEY)
    if relay is None:
        logger.error("operation.completed not published: no relay bound", count=len(payloads))
        return
    relay.submit(payloads)


@event.listens_for(OperationEventSession, "after_rollback")
def _after_rollback(session: Session) -> None:
    session.info.pop(_PENDING_KEY, None)


def bind_operation_events(
    factory: async_sessionmaker[AsyncSession], relay: OperationEventRelay
) -> None:
    """Make sessions of ``factory`` publish ``operation.completed`` through ``relay``."""
    factory.configure(sync_session_class=OperationEventSession, info={_RELAY_KEY: relay})
