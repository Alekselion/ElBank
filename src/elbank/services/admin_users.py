"""Back office: user search, blocking and roles.

- Blocking sets ``status=blocked``, revokes every refresh token and every access
  token issued so far (the user is logged out everywhere) and publishes
  ``user.blocked``; unblocking publishes ``user.unblocked``.
- A manager may block/unblock clients only; an admin anyone but themselves.
- Only an admin changes roles, and not their own (no accidental lock-out).
- Deleted users can be neither blocked, unblocked nor given a role.
"""

import uuid
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import JwtSettings
from elbank.core.exceptions import InvalidStateError, NotFoundError, PermissionDeniedError
from elbank.core.logging import get_logger
from elbank.db.models import User, UserRole, UserStatus
from elbank.kafka import EventPublisher, EventType
from elbank.services.audit_service import AuditService
from elbank.services.sessions import SessionService

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class UserFilter:
    search: str | None = None  # substring of phone, email or full name (case-insensitive)
    status: UserStatus | None = None
    role: UserRole | None = None


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class AdminUserService:
    def __init__(
        self,
        session: AsyncSession,
        redis: Redis,
        publisher: EventPublisher,
        jwt: JwtSettings,
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._audit_service = AuditService(publisher)
        self._sessions = SessionService(session, redis, jwt)

    async def search(self, filters: UserFilter, page: int, size: int) -> tuple[list[User], int]:
        conditions = []
        if filters.search:
            pattern = f"%{_escape_like(filters.search.strip())}%"
            conditions.append(
                or_(
                    User.phone.ilike(pattern, escape="\\"),
                    User.email.ilike(pattern, escape="\\"),
                    User.full_name.ilike(pattern, escape="\\"),
                )
            )
        if filters.status is not None:
            conditions.append(User.status == filters.status)
        if filters.role is not None:
            conditions.append(User.role == filters.role)

        total = await self._session.scalar(select(func.count(User.id)).where(*conditions)) or 0
        rows = await self._session.scalars(
            select(User)
            .where(*conditions)
            .order_by(User.created_at.desc(), User.id)
            .limit(size)
            .offset((page - 1) * size)
        )
        return list(rows), total

    async def block(self, staff: User, user_id: uuid.UUID, reason: str | None) -> User:
        user = await self._target(staff, user_id)
        if user.status is UserStatus.BLOCKED:
            raise InvalidStateError("User is already blocked")
        user.status = UserStatus.BLOCKED
        await self._sessions.revoke_refresh_tokens(user.id)
        await self._session.commit()
        try:
            await self._sessions.revoke_access_tokens(user.id)
        except RedisError as exc:
            # Access tokens are still rejected: every request re-reads the user status.
            logger.warning("access token revocation marker not set", error=str(exc))

        await self._audit("admin.user.block", staff, user, {"reason": reason})
        await self._publisher.publish_safely(
            EventType.USER_BLOCKED,
            {"user_id": str(user.id), "blocked_by": str(staff.id), "reason": reason},
            key=str(user.id),
        )
        return user

    async def unblock(self, staff: User, user_id: uuid.UUID) -> User:
        user = await self._target(staff, user_id)
        if user.status is not UserStatus.BLOCKED:
            raise InvalidStateError("User is not blocked")
        user.status = UserStatus.ACTIVE
        await self._session.commit()

        await self._audit("admin.user.unblock", staff, user, {})
        await self._publisher.publish_safely(
            EventType.USER_UNBLOCKED,
            {"user_id": str(user.id), "unblocked_by": str(staff.id)},
            key=str(user.id),
        )
        return user

    async def change_role(self, admin: User, user_id: uuid.UUID, role: UserRole) -> User:
        if admin.id == user_id:
            raise PermissionDeniedError("You cannot change your own role")
        user = await self._load(user_id)
        if user.status is UserStatus.DELETED:
            raise InvalidStateError("User is deleted")
        previous = user.role
        if previous is role:
            return user
        user.role = role
        await self._session.commit()
        await self._audit("admin.user.role_change", admin, user, {"from": previous, "to": role})
        return user

    async def _target(self, staff: User, user_id: uuid.UUID) -> User:
        """User to block/unblock, with the permission rules applied."""
        if staff.id == user_id:
            raise PermissionDeniedError("You cannot block or unblock yourself")
        user = await self._load(user_id)
        if staff.role is not UserRole.ADMIN and user.role is not UserRole.CLIENT:
            raise PermissionDeniedError("Managers can block and unblock clients only")
        if user.status is UserStatus.DELETED:
            raise InvalidStateError("User is deleted")
        return user

    async def _load(self, user_id: uuid.UUID) -> User:
        user = await self._session.get(User, user_id, with_for_update=True, populate_existing=True)
        if user is None:
            raise NotFoundError("User not found")
        return user

    async def _audit(
        self, action: str, staff: User, user: User, details: dict[str, object]
    ) -> None:
        await self._audit_service.record(
            action,
            user_id=staff.id,
            entity="user",
            entity_id=user.id,
            payload={**details, "actor_role": staff.role},
        )
        logger.info(action, user_id=str(user.id), actor_id=str(staff.id))
