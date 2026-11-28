"""Profile use cases: personal data, password, phone change, deletion, notification settings."""

from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import Settings
from elbank.core.exceptions import (
    ConflictError,
    ExternalServiceError,
    InvalidOtpCodeError,
    InvalidPasswordError,
    InvalidStateError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.core.pii import mask_phone
from elbank.core.security import OtpPurpose, hash_password, verify_password
from elbank.db.models import User, UserStatus
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.users import NotificationSettings
from elbank.services.otp import OtpService
from elbank.services.sessions import SessionService, TokenPair

logger = get_logger(__name__)

_PROFILE_FIELDS = frozenset({"full_name", "email", "address"})


def _pending_phone_key(user: User) -> str:
    return f"phone_change:{user.id}"


class ProfileService:
    def __init__(
        self,
        session: AsyncSession,
        redis: Redis,
        otp: OtpService,
        publisher: EventPublisher,
        settings: Settings,
    ) -> None:
        self._session = session
        self._redis = redis
        self._otp = otp
        self._publisher = publisher
        self._settings = settings
        self._sessions = SessionService(session, redis, settings.jwt)

    # ---------- personal data ----------

    async def update_profile(self, user: User, changes: dict[str, Any]) -> User:
        unknown = changes.keys() - _PROFILE_FIELDS
        if unknown:
            raise ValueError(f"not updatable: {sorted(unknown)}")
        changed = {k: v for k, v in changes.items() if getattr(user, k) != v}
        if not changed:
            return user

        if "email" in changed:
            await self._ensure_email_free(user, changed["email"])
        for field, value in changed.items():
            setattr(user, field, value)
        await self._commit_unique("User with this email already exists")

        logger.info("profile updated", user_id=str(user.id), fields=sorted(changed))
        await self._publish_profile_updated(user, changed)
        return user

    # ---------- password ----------

    async def change_password(self, user: User, old_password: str, new_password: str) -> TokenPair:
        """Change the password and end all other sessions; returns a fresh token pair."""
        if not verify_password(old_password, user.password_hash):
            raise InvalidPasswordError
        user.password_hash = hash_password(new_password)
        await self._sessions.revoke_refresh_tokens(user.id)
        pair = self._sessions.issue_pair(user)
        await self._session.commit()
        await self._sessions.revoke_access_tokens(user.id, issued_before=pair.access.issued_at)
        logger.info("password changed", user_id=str(user.id))
        return pair

    # ---------- phone change (OTP to both the current and the new phone) ----------

    async def request_phone_change(self, user: User, new_phone: str) -> None:
        if new_phone == user.phone:
            raise UnprocessableError("New phone is the same as the current one")
        await self._ensure_phone_free(new_phone)

        await self._otp.ensure_can_send(user.phone, OtpPurpose.PHONE_CHANGE)
        await self._otp.ensure_can_send(new_phone, OtpPurpose.PHONE_CHANGE)
        await self._redis.set(
            _pending_phone_key(user), new_phone, ex=self._settings.otp.ttl_seconds
        )
        try:
            await self._otp.send(user.phone, OtpPurpose.PHONE_CHANGE, user.id)
            await self._otp.send(new_phone, OtpPurpose.PHONE_CHANGE, user.id)
        except ExternalServiceError:
            await self._otp.discard(user.phone, OtpPurpose.PHONE_CHANGE)
            await self._otp.discard(new_phone, OtpPurpose.PHONE_CHANGE)
            await self._redis.delete(_pending_phone_key(user))
            raise
        logger.info("phone change requested", user_id=str(user.id), new_phone=mask_phone(new_phone))

    async def confirm_phone_change(self, user: User, old_code: str, new_code: str) -> User:
        pending = await self._redis.get(_pending_phone_key(user))
        new_phone = pending.decode() if isinstance(pending, bytes) else pending
        if new_phone is None:
            raise InvalidStateError("No pending phone change: request it first")

        result = await self._otp.check_codes(
            [(user.phone, old_code), (new_phone, new_code)], OtpPurpose.PHONE_CHANGE
        )
        if not result.verified:
            if result.attempts_left == 0:
                await self._redis.delete(_pending_phone_key(user))
                raise InvalidOtpCodeError(
                    "Codes are invalid or expired: request a new phone change"
                )
            raise InvalidOtpCodeError(f"Invalid code(s), attempts left: {result.attempts_left}")

        await self._ensure_phone_free(new_phone)
        old_phone, user.phone = user.phone, new_phone
        await self._commit_unique("User with this phone already exists")
        await self._redis.delete(_pending_phone_key(user))

        logger.info(
            "phone changed",
            user_id=str(user.id),
            old_phone=mask_phone(old_phone),
            new_phone=mask_phone(new_phone),
        )
        await self._publish_profile_updated(user, {"phone": new_phone})
        return user

    # ---------- deletion ----------

    async def delete_account(self, user: User) -> None:
        """Soft delete: the row stays (audit, money history), the user can no longer log in."""
        user.status = UserStatus.DELETED
        await self._sessions.revoke_refresh_tokens(user.id)
        await self._session.commit()
        await self._sessions.revoke_access_tokens(user.id)

        logger.info("user deleted", user_id=str(user.id))
        await self._publisher.publish_safely(
            EventType.USER_DELETED,
            {"user_id": str(user.id), "deleted_at": datetime.now(UTC).isoformat()},
            key=str(user.id),
        )

    # ---------- notification settings ----------

    @staticmethod
    def get_notification_settings(user: User) -> NotificationSettings:
        # Stored JSON may lack channels added later: model defaults fill them in.
        return NotificationSettings.model_validate(user.notification_settings)

    async def update_notification_settings(
        self, user: User, changes: dict[str, bool]
    ) -> NotificationSettings:
        current = self.get_notification_settings(user)
        updated = current.model_copy(update=changes)
        if updated == current:
            return current

        # Assign a new dict: in-place JSONB mutations are not tracked by SQLAlchemy.
        user.notification_settings = updated.model_dump()
        await self._session.commit()
        logger.info("notification settings updated", user_id=str(user.id), changes=changes)
        await self._publish_profile_updated(user, {"notification_settings": updated.model_dump()})
        return updated

    # ---------- helpers ----------

    async def _ensure_email_free(self, user: User, email: str) -> None:
        taken = await self._session.scalar(
            select(User.id).where(User.email == email, User.id != user.id).limit(1)
        )
        if taken is not None:
            raise ConflictError("User with this email already exists")

    async def _ensure_phone_free(self, phone: str) -> None:
        taken = await self._session.scalar(select(User.id).where(User.phone == phone).limit(1))
        if taken is not None:
            raise ConflictError("This phone is already registered")

    async def _commit_unique(self, conflict_message: str) -> None:
        try:
            await self._session.commit()
        except IntegrityError as exc:  # lost a race on a unique column
            await self._session.rollback()
            raise ConflictError(conflict_message) from exc

    async def _publish_profile_updated(self, user: User, changes: dict[str, Any]) -> None:
        await self._publisher.publish_safely(
            EventType.PROFILE_UPDATED,
            {"user_id": str(user.id), "changes": changes},
            key=str(user.id),
        )
