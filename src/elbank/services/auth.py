"""Authentication use cases: registration, login, token rotation, password reset, 2FA."""

from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients.sms import mask_phone
from elbank.core.audit import audit
from elbank.core.config import Settings
from elbank.core.exceptions import (
    AuthenticationError,
    ConflictError,
    InvalidCredentialsError,
    InvalidOtpTokenError,
    OtpRequiredError,
    UserInactiveError,
)
from elbank.core.logging import get_logger
from elbank.core.security import (
    InvalidTokenError,
    OtpPurpose,
    TokenType,
    decode_token,
    dummy_verify_password,
    hash_password,
    hash_token,
    verify_password,
)
from elbank.db.models import RefreshToken, User, UserStatus
from elbank.kafka import EventPublisher, EventType
from elbank.services.otp import OtpService
from elbank.services.rate_limit import LoginRateLimiter
from elbank.services.sessions import SessionService, TokenPair

logger = get_logger(__name__)

# Public /send-otp only serves registered phones; phone_change OTPs are sent by
# the profile flow (/users/me/change-phone/request).
_PUBLIC_OTP_PURPOSES = frozenset({OtpPurpose.LOGIN, OtpPurpose.RESET, OtpPurpose.TWO_FACTOR})


class AuthService:
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
        self._login_limiter = LoginRateLimiter(redis, settings.login_rate_limit)
        self._sessions = SessionService(session, redis, settings.jwt)

    # ---------- registration ----------

    async def register(self, phone: str, password: str, email: str) -> User:
        existing = await self._session.scalar(
            select(User.phone).where(or_(User.phone == phone, User.email == email)).limit(1)
        )
        if existing is not None:
            raise ConflictError("User with this phone or email already exists")

        user = User(
            phone=phone,
            email=email,
            password_hash=hash_password(password),
            status=UserStatus.ACTIVE,
        )
        self._session.add(user)
        try:
            await self._session.commit()
        except IntegrityError as exc:  # concurrent registration with the same phone/email
            await self._session.rollback()
            raise ConflictError("User with this phone or email already exists") from exc

        logger.info("user registered", user_id=str(user.id))
        await self._publisher.publish_safely(
            EventType.USER_REGISTERED,
            {
                "user_id": str(user.id),
                "phone": user.phone,
                "email": user.email,
                "status": user.status.value,
                "role": user.role.value,
            },
            key=str(user.id),
        )
        return user

    # ---------- login / tokens ----------

    async def login(self, phone: str, password: str, otp_token: str | None) -> TokenPair:
        await self._login_limiter.hit(phone)

        user = await self._session.scalar(select(User).where(User.phone == phone))
        if user is None:
            dummy_verify_password()
            raise InvalidCredentialsError
        if not verify_password(password, user.password_hash):
            logger.info("login failed: wrong password", user_id=str(user.id))
            audit(
                "auth.login_failed",
                actor_id=user.id,
                user_id=user.id,
                entity="user",
                entity_id=user.id,
                details={"reason": "wrong_password"},
            )
            raise InvalidCredentialsError
        self._ensure_active(user)

        if user.is_2fa_enabled:
            if otp_token is None:
                raise OtpRequiredError
            await self._otp.consume_token(otp_token, user.phone, OtpPurpose.LOGIN)

        pair = self._sessions.issue_pair(user)
        await self._session.commit()
        logger.info("user logged in", user_id=str(user.id))
        audit("auth.login", actor_id=user.id, user_id=user.id, entity="user", entity_id=user.id)
        return pair

    async def refresh(self, refresh_token: str) -> TokenPair:
        try:
            payload = decode_token(refresh_token, TokenType.REFRESH, self._settings.jwt)
        except InvalidTokenError as exc:
            raise AuthenticationError("Invalid refresh token") from exc

        stored = await self._session.scalar(
            select(RefreshToken)
            .where(RefreshToken.token_hash == hash_token(refresh_token))
            .with_for_update()
        )
        if stored is None or str(stored.user_id) != payload.sub:
            raise AuthenticationError("Invalid refresh token")

        if stored.revoked_at is not None:
            # A rotated token is being replayed: assume it was stolen and
            # terminate every session of the user.
            await self._sessions.revoke_refresh_tokens(stored.user_id)
            await self._session.commit()
            logger.warning("refresh token reuse detected", user_id=str(stored.user_id))
            raise AuthenticationError("Refresh token has been revoked")

        now = datetime.now(UTC)
        if stored.expires_at <= now:
            raise AuthenticationError("Refresh token has expired")

        user = await self._session.get(User, stored.user_id)
        if user is None:
            raise AuthenticationError("Invalid refresh token")
        self._ensure_active(user)

        stored.revoked_at = now
        pair = self._sessions.issue_pair(user)
        await self._session.commit()
        logger.info("tokens refreshed", user_id=str(user.id))
        return pair

    async def logout(self, user: User) -> None:
        await self._sessions.revoke_refresh_tokens(user.id)
        await self._session.commit()
        await self._sessions.revoke_access_tokens(user.id)
        logger.info("user logged out", user_id=str(user.id))

    # ---------- OTP ----------

    async def send_otp(self, phone: str, purpose: OtpPurpose) -> None:
        if purpose not in _PUBLIC_OTP_PURPOSES:
            raise ValueError(f"OTP purpose {purpose.value} is not available via /send-otp")
        await self._otp.ensure_can_send(phone, purpose)
        user_id = await self._session.scalar(
            select(User.id).where(User.phone == phone, User.status == UserStatus.ACTIVE)
        )
        if user_id is None:
            # Same response as for a registered phone: do not reveal who is a client.
            logger.info("otp skipped: phone not registered", phone=mask_phone(phone))
            return

        await self._otp.send(phone, purpose, user_id)

    # ---------- password reset ----------

    async def reset_password(self, phone: str, otp_token: str, new_password: str) -> None:
        await self._otp.consume_token(otp_token, phone, OtpPurpose.RESET)
        user = await self._session.scalar(select(User).where(User.phone == phone))
        if user is None:
            raise InvalidOtpTokenError
        self._ensure_active(user)

        user.password_hash = hash_password(new_password)
        await self._sessions.revoke_refresh_tokens(user.id)
        await self._session.commit()
        await self._sessions.revoke_access_tokens(user.id)
        logger.info("password reset", user_id=str(user.id))

    # ---------- two-factor authentication ----------

    async def set_two_factor(self, user: User, otp_token: str, *, enabled: bool) -> User:
        await self._otp.consume_token(otp_token, user.phone, OtpPurpose.TWO_FACTOR)
        if user.is_2fa_enabled != enabled:
            user.is_2fa_enabled = enabled
            await self._session.commit()
            logger.info("2fa settings changed", user_id=str(user.id), enabled=enabled)
        return user

    # ---------- helpers ----------

    @staticmethod
    def _ensure_active(user: User) -> None:
        if user.status is not UserStatus.ACTIVE:
            raise UserInactiveError
