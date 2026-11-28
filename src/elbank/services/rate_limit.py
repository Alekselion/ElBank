"""Redis fixed-window rate limiting."""

from redis.asyncio import Redis

from elbank.core.config import LoginRateLimitSettings
from elbank.core.exceptions import TooManyRequestsError
from elbank.core.logging import get_logger
from elbank.core.pii import mask_phone

logger = get_logger(__name__)

LOGIN_BLOCKED_METRIC_KEY = "metrics:user.login_blocked"


def _login_attempts_key(phone: str) -> str:
    return f"ratelimit:login:{phone}"


class LoginRateLimiter:
    """At most ``max_attempts`` login attempts per phone within ``window_seconds``.

    Every attempt counts (successful or not). The window starts with the first
    attempt; once it is exceeded the caller gets 429 with ``Retry-After`` until
    the window expires.
    """

    def __init__(self, redis: Redis, settings: LoginRateLimitSettings) -> None:
        self._redis = redis
        self._settings = settings

    async def hit(self, phone: str) -> None:
        key = _login_attempts_key(phone)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, self._settings.window_seconds, nx=True)
            pipe.ttl(key)
            attempts, _, ttl = await pipe.execute()

        if int(attempts) <= self._settings.max_attempts:
            return

        blocked_total = await self._redis.incr(LOGIN_BLOCKED_METRIC_KEY)
        retry_after = int(ttl) if int(ttl) > 0 else self._settings.window_seconds
        logger.warning(
            "user.login_blocked",
            phone=mask_phone(phone),
            attempts=int(attempts),
            retry_after=retry_after,
            login_blocked_total=blocked_total,
        )
        raise TooManyRequestsError(
            "Too many login attempts, try again later", retry_after=retry_after
        )
