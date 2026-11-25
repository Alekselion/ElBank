"""User sessions: issuing token pairs and revoking them.

Refresh tokens live in the DB (sha256 only) and are revoked inside the caller's
transaction. Access tokens are stateless JWTs, so revocation is a Redis marker
"reject tokens issued before T" that only has to outlive the access TTL; set it
after the transaction is committed.
"""

import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import JwtSettings
from elbank.core.security import (
    IssuedToken,
    create_access_token,
    create_refresh_token,
    hash_token,
)
from elbank.db.models import RefreshToken, User


@dataclass(frozen=True, slots=True)
class TokenPair:
    access: IssuedToken
    refresh: IssuedToken


def _access_revoked_key(user_id: uuid.UUID) -> str:
    return f"auth:access_revoked_before:{user_id}"


async def access_revoked_before(redis: Redis, user_id: uuid.UUID) -> int | None:
    """Unix time before which the user's access tokens are no longer accepted."""
    value = await redis.get(_access_revoked_key(user_id))
    return int(value) if value is not None else None


class SessionService:
    def __init__(self, session: AsyncSession, redis: Redis, settings: JwtSettings) -> None:
        self._session = session
        self._redis = redis
        self._settings = settings

    def issue_pair(self, user: User) -> TokenPair:
        """Create a token pair and stage its refresh token; the caller commits."""
        access = create_access_token(user.id, self._settings)
        refresh = create_refresh_token(user.id, self._settings)
        self._session.add(
            RefreshToken(
                user_id=user.id,
                token_hash=hash_token(refresh.token),
                expires_at=refresh.expires_at,
            )
        )
        return TokenPair(access=access, refresh=refresh)

    async def revoke_refresh_tokens(self, user_id: uuid.UUID) -> None:
        """Revoke all active refresh tokens of the user; the caller commits."""
        await self._session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC))
        )

    async def revoke_access_tokens(
        self, user_id: uuid.UUID, issued_before: datetime | None = None
    ) -> None:
        """Reject access tokens issued before ``issued_before`` (default: now).

        Call after the commit. When a fresh pair was issued in the same operation,
        pass its ``issued_at`` so the new access token stays valid.
        """
        cutoff = int(issued_before.timestamp()) if issued_before else int(time.time())
        await self._redis.set(
            _access_revoked_key(user_id), cutoff, ex=self._settings.ttl_access_minutes * 60
        )
