"""Password hashing and JWT issuing/verification."""

import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, ValidationError

from elbank.core.config import JwtSettings

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class TokenType(StrEnum):
    ACCESS = "access"
    REFRESH = "refresh"


class TokenPayload(BaseModel):
    sub: str
    type: TokenType
    jti: str
    iat: int
    exp: int


class InvalidTokenError(Exception):
    """Raised when a JWT is malformed, expired, badly signed or of the wrong type."""


def hash_password(password: str) -> str:
    hashed: str = _pwd_context.hash(password)
    return hashed


def verify_password(password: str, password_hash: str) -> bool:
    return bool(_pwd_context.verify(password, password_hash))


def _ttl_for(token_type: TokenType, settings: JwtSettings) -> timedelta:
    match token_type:
        case TokenType.ACCESS:
            return timedelta(minutes=settings.ttl_access_minutes)
        case TokenType.REFRESH:
            return timedelta(days=settings.ttl_refresh_days)


def create_token(
    subject: str,
    token_type: TokenType,
    settings: JwtSettings,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    now = datetime.now(UTC)
    claims: dict[str, Any] = {
        **(extra_claims or {}),
        "sub": subject,
        "type": token_type.value,
        "jti": uuid.uuid4().hex,
        "iat": int(now.timestamp()),
        "exp": int((now + _ttl_for(token_type, settings)).timestamp()),
    }
    token: str = jwt.encode(
        claims,
        settings.secret.get_secret_value(),
        algorithm=settings.algorithm,
    )
    return token


def decode_token(token: str, expected_type: TokenType, settings: JwtSettings) -> TokenPayload:
    try:
        raw = jwt.decode(
            token,
            settings.secret.get_secret_value(),
            algorithms=[settings.algorithm],
        )
        payload = TokenPayload.model_validate(raw)
    except (JWTError, ValidationError) as exc:
        raise InvalidTokenError(str(exc)) from exc
    if payload.type is not expected_type:
        raise InvalidTokenError(f"expected {expected_type.value} token, got {payload.type.value}")
    return payload
