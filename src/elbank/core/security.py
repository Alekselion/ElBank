"""Password hashing (bcrypt) and JWT issuing/verification.

Three token types share one signing key and are told apart by the ``type`` claim:

* ``access``  — authorizes API calls, short-lived;
* ``refresh`` — exchanged for a new token pair; its sha256 is stored in the DB;
* ``otp``     — proof that the holder verified an OTP sent to ``sub`` (a phone)
  for a given ``purpose``; carries ``otp_verified: true``.
"""

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, ValidationError

from elbank.core.config import JwtSettings

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class TokenType(StrEnum):
    ACCESS = "access"
    REFRESH = "refresh"
    OTP = "otp"


class OtpPurpose(StrEnum):
    LOGIN = "login"
    RESET = "reset"
    PHONE_CHANGE = "phone_change"
    TWO_FACTOR = "two_factor"  # enabling/disabling 2FA


class TokenPayload(BaseModel):
    sub: str
    type: TokenType
    jti: str
    iat: int
    exp: int


class OtpTokenPayload(TokenPayload):
    purpose: OtpPurpose
    otp_verified: Literal[True]


class InvalidTokenError(Exception):
    """Raised when a JWT is malformed, expired, badly signed or of the wrong type."""


@dataclass(frozen=True, slots=True)
class IssuedToken:
    token: str
    jti: str
    issued_at: datetime
    expires_at: datetime


# ---------- passwords ----------


def hash_password(password: str) -> str:
    hashed: str = _pwd_context.hash(password)
    return hashed


def verify_password(password: str, password_hash: str) -> bool:
    return bool(_pwd_context.verify(password, password_hash))


def dummy_verify_password() -> None:
    """Spend the same time as ``verify_password`` when the user does not exist.

    Prevents user enumeration through login response timing.
    """
    _pwd_context.dummy_verify()


def hash_token(token: str) -> str:
    """sha256 hex digest; used to store refresh tokens without keeping them in clear."""
    return hashlib.sha256(token.encode()).hexdigest()


# ---------- JWT ----------


def _encode(
    subject: str,
    token_type: TokenType,
    ttl: timedelta,
    settings: JwtSettings,
    extra_claims: dict[str, Any] | None = None,
) -> IssuedToken:
    now = datetime.now(UTC).replace(microsecond=0)  # JWT times have second precision
    expires_at = now + ttl
    jti = uuid.uuid4().hex
    claims: dict[str, Any] = {
        **(extra_claims or {}),
        "sub": subject,
        "type": token_type.value,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    token: str = jwt.encode(
        claims, settings.secret.get_secret_value(), algorithm=settings.algorithm
    )
    return IssuedToken(token=token, jti=jti, issued_at=now, expires_at=expires_at)


def create_access_token(user_id: uuid.UUID, settings: JwtSettings) -> IssuedToken:
    ttl = timedelta(minutes=settings.ttl_access_minutes)
    return _encode(str(user_id), TokenType.ACCESS, ttl, settings)


def create_refresh_token(user_id: uuid.UUID, settings: JwtSettings) -> IssuedToken:
    ttl = timedelta(days=settings.ttl_refresh_days)
    return _encode(str(user_id), TokenType.REFRESH, ttl, settings)


def create_otp_token(phone: str, purpose: OtpPurpose, settings: JwtSettings) -> IssuedToken:
    ttl = timedelta(minutes=settings.ttl_otp_minutes)
    claims = {"purpose": purpose.value, "otp_verified": True}
    return _encode(phone, TokenType.OTP, ttl, settings, claims)


def _decode_raw(token: str, settings: JwtSettings) -> dict[str, Any]:
    try:
        raw: dict[str, Any] = jwt.decode(
            token, settings.secret.get_secret_value(), algorithms=[settings.algorithm]
        )
    except JWTError as exc:
        raise InvalidTokenError(str(exc)) from exc
    return raw


def decode_token(token: str, expected_type: TokenType, settings: JwtSettings) -> TokenPayload:
    try:
        payload = TokenPayload.model_validate(_decode_raw(token, settings))
    except ValidationError as exc:
        raise InvalidTokenError(str(exc)) from exc
    if payload.type is not expected_type:
        raise InvalidTokenError(f"expected {expected_type.value} token, got {payload.type.value}")
    return payload


def decode_otp_token(
    token: str, expected_purpose: OtpPurpose, settings: JwtSettings
) -> OtpTokenPayload:
    raw = _decode_raw(token, settings)
    if raw.get("type") != TokenType.OTP:
        raise InvalidTokenError(f"expected {TokenType.OTP.value} token")
    try:
        payload = OtpTokenPayload.model_validate(raw)
    except ValidationError as exc:
        raise InvalidTokenError(str(exc)) from exc
    if payload.purpose is not expected_purpose:
        raise InvalidTokenError(
            f"otp token issued for {payload.purpose.value}, expected {expected_purpose.value}"
        )
    return payload
