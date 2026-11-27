"""Redis cache of a user's account list (``accounts:{user_id}``).

The cache is an optimization only: Redis errors are logged and the caller falls
back to the database. Every change of an account visible in the list (open,
close, block, balance change, ...) must call :meth:`AccountListCache.invalidate`
*after* the DB commit.
"""

import uuid

from pydantic import TypeAdapter, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from elbank.core.logging import get_logger
from elbank.schemas.account import AccountRead

logger = get_logger(__name__)

_adapter = TypeAdapter(list[AccountRead])


def _key(user_id: uuid.UUID) -> str:
    return f"accounts:{user_id}"


class AccountListCache:
    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    async def get(self, user_id: uuid.UUID) -> list[AccountRead] | None:
        try:
            raw = await self._redis.get(_key(user_id))
        except RedisError as exc:
            logger.warning("accounts cache read failed", user_id=str(user_id), error=str(exc))
            return None
        if raw is None:
            return None
        try:
            return _adapter.validate_json(raw)
        except ValidationError:
            # Schema changed since the value was written: treat as a miss.
            logger.warning("accounts cache entry is stale, ignored", user_id=str(user_id))
            return None

    async def set(self, user_id: uuid.UUID, accounts: list[AccountRead]) -> None:
        try:
            await self._redis.set(_key(user_id), _adapter.dump_json(accounts), ex=self._ttl)
        except RedisError as exc:
            logger.warning("accounts cache write failed", user_id=str(user_id), error=str(exc))

    async def invalidate(self, user_id: uuid.UUID) -> None:
        try:
            await self._redis.delete(_key(user_id))
        except RedisError as exc:
            # The entry expires by TTL anyway; the list may be stale until then.
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "accounts cache invalidation failed", user_id=str(user_id), error=str(exc)
            )
