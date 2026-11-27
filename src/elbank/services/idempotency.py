"""Idempotent execution of money operations, shared by all transfer channels.

The idempotency key is unique per initiator in the DB
(``uq_operations_user_id_idempotency_key``), the main operation stores it.
:meth:`IdempotencyGuard.run`:

1. an operation with this key exists → ``replay(operation)`` (the API answers 200);
2. otherwise takes the Redis lock ``op_lock:{user_id}:{key}`` (TTL
   ``TRANSFERS_LOCK_TTL_SECONDS``); a parallel request with the same key waits up to
   ``TRANSFERS_LOCK_WAIT_SECONDS`` and then gets the stored result or ``409``;
3. runs ``execute()``; if two requests still race (lock expired, Redis down), the
   unique key rejects the second commit and it gets the replay instead.
"""

import uuid
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import TransferSettings
from elbank.core.exceptions import OperationInProgressError
from elbank.core.logging import get_logger
from elbank.db.models import Operation

logger = get_logger(__name__)

_CONSTRAINT = "uq_operations_user_id_idempotency_key"
_LOCK_POLL_SECONDS = 0.05


class IdempotencyGuard:
    def __init__(self, session: AsyncSession, redis: Redis, settings: TransferSettings) -> None:
        self._session = session
        self._redis = redis
        self._settings = settings

    async def run[T](
        self,
        user_id: uuid.UUID,
        key: str,
        *,
        execute: Callable[[], Awaitable[T]],
        replay: Callable[[Operation], Awaitable[T]],
    ) -> T:
        if (existing := await self._find(user_id, key)) is not None:
            return await replay(existing)

        lock = self._redis.lock(
            f"op_lock:{user_id}:{key}",
            timeout=self._settings.lock_ttl_seconds,
            blocking_timeout=self._settings.lock_wait_seconds,
            sleep=_LOCK_POLL_SECONDS,
        )
        if not await self._acquire(lock):
            # The parallel request may have finished while we were waiting.
            if (existing := await self._find(user_id, key)) is not None:
                return await replay(existing)
            raise OperationInProgressError
        try:
            if (existing := await self._find(user_id, key)) is not None:
                return await replay(existing)
            try:
                return await execute()
            except IntegrityError as exc:
                if _CONSTRAINT not in str(exc.orig):
                    raise
                await self._session.rollback()
                if (existing := await self._find(user_id, key)) is None:
                    raise
                return await replay(existing)
        finally:
            await self._release(lock)

    async def _find(self, user_id: uuid.UUID, key: str) -> Operation | None:
        result: Operation | None = await self._session.scalar(
            select(Operation).where(Operation.user_id == user_id, Operation.idempotency_key == key)
        )
        return result

    @staticmethod
    async def _acquire(lock: Lock) -> bool:
        try:
            return bool(await lock.acquire())
        except RedisError as exc:
            # The DB unique key still guarantees idempotency; only the waiting is lost.
            logger.warning("op_lock unavailable, continuing without it", error=str(exc))
            return True

    @staticmethod
    async def _release(lock: Lock) -> None:
        try:
            await lock.release()
        except LockError:
            # Expired (processing took longer than the TTL) or never taken (Redis down).
            logger.warning("op_lock was not held on release", lock=str(lock.name))
        except RedisError as exc:
            logger.warning("op_lock release failed", error=str(exc))
