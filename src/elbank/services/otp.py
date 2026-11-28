"""One-time passwords: generation, SMS delivery, verification and OTP tokens.

Redis layout:

* ``otp:{phone}:{purpose}``          hash {code_hash, attempts}, TTL = OTP_TTL_SECONDS
* ``otp:cooldown:{phone}:{purpose}`` resend throttle, TTL = OTP_RESEND_COOLDOWN_SECONDS
* ``otp:token_used:{jti}``           marks an OTP token as consumed until it expires

Only a hash of the code is stored, and a code is burned after
``OTP_MAX_ATTEMPTS`` wrong guesses or after its first successful verification.
"""

import hashlib
import hmac
import secrets
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from redis.asyncio import Redis

from elbank.clients.sms import SmsClient, SmsGatewayError
from elbank.core.config import Settings
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidOtpTokenError,
    TooManyRequestsError,
)
from elbank.core.logging import get_logger
from elbank.core.pii import mask_phone
from elbank.core.security import (
    InvalidTokenError,
    IssuedToken,
    OtpPurpose,
    OtpTokenPayload,
    create_otp_token,
    decode_otp_token,
)
from elbank.kafka import EventPublisher, EventType

logger = get_logger(__name__)

OTP_LENGTH = 6

_SMS_TEXT = {
    OtpPurpose.LOGIN: "ElBank: login code {code}. Do not share it with anyone.",
    OtpPurpose.RESET: "ElBank: password reset code {code}. Do not share it with anyone.",
    OtpPurpose.PHONE_CHANGE: "ElBank: phone change confirmation code {code}.",
    OtpPurpose.TWO_FACTOR: "ElBank: two-factor settings confirmation code {code}.",
    OtpPurpose.CREDIT_SIGN: "ElBank: credit agreement signing code {code}. Do not share it.",
}


def _otp_key(phone: str, purpose: OtpPurpose) -> str:
    return f"otp:{phone}:{purpose.value}"


def _cooldown_key(phone: str, purpose: OtpPurpose) -> str:
    return f"otp:cooldown:{phone}:{purpose.value}"


def _used_token_key(jti: str) -> str:
    return f"otp:token_used:{jti}"


# Atomic check-and-burn of one or more codes that must all be correct together
# (e.g. old and new phone on phone change).
# KEYS: otp keys; ARGV[1]: max attempts; ARGV[1 + i]: code hash for KEYS[i].
# Returns {status, attempts_left}: 1 = all matched (codes burned), 0 = wrong code(s),
# -1 = some code is missing/expired. A wrong guess counts against every code of
# the set; when attempts run out, all of them are burned.
_VERIFY_LUA = """
local all_match = true
for i, key in ipairs(KEYS) do
    local expected = redis.call('HGET', key, 'code_hash')
    if not expected then
        return {-1, 0}
    end
    if expected ~= ARGV[i + 1] then
        all_match = false
    end
end
if all_match then
    redis.call('DEL', unpack(KEYS))
    return {1, 0}
end
local max_used = 0
for _, key in ipairs(KEYS) do
    local attempts = redis.call('HINCRBY', key, 'attempts', 1)
    if attempts > max_used then
        max_used = attempts
    end
end
local left = tonumber(ARGV[1]) - max_used
if left <= 0 then
    redis.call('DEL', unpack(KEYS))
    left = 0
end
return {0, left}
"""


def _hash_code(phone: str, purpose: OtpPurpose, code: str) -> str:
    return hashlib.sha256(f"{phone}:{purpose.value}:{code}".encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class OtpVerification:
    verified: bool
    attempts_left: int
    token: IssuedToken | None = None


class OtpService:
    def __init__(
        self,
        redis: Redis,
        sms: SmsClient,
        publisher: EventPublisher,
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._sms = sms
        self._publisher = publisher
        self._settings = settings
        self._verify_script = redis.register_script(_VERIFY_LUA)

    async def ensure_can_send(self, phone: str, purpose: OtpPurpose) -> None:
        """Acquire the resend cooldown or raise 429.

        Called for every request, including ones for unknown phones, so the
        response does not reveal whether the phone is registered.
        """
        cooldown = self._settings.otp.resend_cooldown_seconds
        if cooldown == 0:
            return
        acquired = await self._redis.set(_cooldown_key(phone, purpose), "1", ex=cooldown, nx=True)
        if not acquired:
            ttl = await self._redis.ttl(_cooldown_key(phone, purpose))
            raise TooManyRequestsError(
                "OTP was sent recently, try again later", retry_after=int(ttl) if ttl > 0 else 1
            )

    async def send(self, phone: str, purpose: OtpPurpose, user_id: uuid.UUID | None) -> None:
        code = f"{secrets.randbelow(10**OTP_LENGTH):0{OTP_LENGTH}d}"
        key = _otp_key(phone, purpose)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            pipe.hset(key, mapping={"code_hash": _hash_code(phone, purpose, code), "attempts": 0})
            pipe.expire(key, self._settings.otp.ttl_seconds)
            await pipe.execute()

        try:
            sms = await self._sms.send(phone, _SMS_TEXT[purpose].format(code=code))
        except SmsGatewayError as exc:
            # The user never got the code: drop it and let them retry immediately.
            await self.discard(phone, purpose)
            raise ExternalServiceError("Could not send SMS, try again later") from exc

        logger.info("otp sent", phone=mask_phone(phone), purpose=purpose.value)
        await self._publisher.publish_safely(
            EventType.NOTIFICATION_SEND,
            {
                "user_id": str(user_id) if user_id else None,
                "channel": "sms",
                "template": f"otp.{purpose.value}",
                "recipient": mask_phone(phone),
                "provider_message_id": sms.message_id,
                "provider_status": sms.status,
            },
            key=str(user_id) if user_id else phone,
        )

    async def verify(self, phone: str, code: str, purpose: OtpPurpose) -> OtpVerification:
        """Check a code; on success burn it and issue an OTP token."""
        result = await self.check_codes([(phone, code)], purpose)
        if not result.verified:
            return result
        token = create_otp_token(phone, purpose, self._settings.jwt)
        return OtpVerification(verified=True, attempts_left=0, token=token)

    async def check_codes(
        self, codes: Sequence[tuple[str, str]], purpose: OtpPurpose
    ) -> OtpVerification:
        """Check ``(phone, code)`` pairs atomically: all are burned only if all match."""
        status, attempts_left = await self._verify_script(
            keys=[_otp_key(phone, purpose) for phone, _ in codes],
            args=[
                self._settings.otp.max_attempts,
                *(_hash_code(phone, purpose, code) for phone, code in codes),
            ],
        )
        phones = [mask_phone(phone) for phone, _ in codes]
        if int(status) == 1:
            logger.info("otp verified", phones=phones, purpose=purpose.value)
            return OtpVerification(verified=True, attempts_left=0)

        logger.warning(
            "otp verification failed",
            phones=phones,
            purpose=purpose.value,
            reason="no_active_code" if int(status) == -1 else "wrong_code",
            attempts_left=int(attempts_left),
        )
        return OtpVerification(verified=False, attempts_left=int(attempts_left))

    async def discard(self, phone: str, purpose: OtpPurpose) -> None:
        """Drop an issued code and its resend cooldown (e.g. the flow was aborted)."""
        await self._redis.delete(_otp_key(phone, purpose), _cooldown_key(phone, purpose))

    async def consume_token(self, token: str, phone: str, purpose: OtpPurpose) -> OtpTokenPayload:
        """Validate an OTP token for ``phone``/``purpose`` and mark it used (single use)."""
        try:
            payload = decode_otp_token(token, purpose, self._settings.jwt)
        except InvalidTokenError as exc:
            raise InvalidOtpTokenError from exc
        if not hmac.compare_digest(payload.sub, phone):
            raise InvalidOtpTokenError

        ttl = max(payload.exp - int(time.time()), 1)
        if not await self._redis.set(_used_token_key(payload.jti), "1", ex=ttl, nx=True):
            raise InvalidOtpTokenError
        return payload
