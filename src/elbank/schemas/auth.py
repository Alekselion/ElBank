"""Auth request/response schemas."""

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, EmailStr, Field, StringConstraints

from elbank.core.security import OtpPurpose
from elbank.db.models import UserRole, UserStatus

BCRYPT_MAX_BYTES = 72


def _check_password(value: str) -> str:
    if len(value.encode()) > BCRYPT_MAX_BYTES:
        raise ValueError(f"password must not exceed {BCRYPT_MAX_BYTES} bytes in UTF-8")
    if value.isdigit() or value.isalpha():
        raise ValueError("password must contain both letters and digits/symbols")
    return value


Phone = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^\+[1-9]\d{7,14}$"),
    Field(description="Phone in E.164 format", examples=["+79991234567"]),
]
Password = Annotated[
    str,
    Field(min_length=8, max_length=BCRYPT_MAX_BYTES, examples=["S3cure-pass"]),
    AfterValidator(_check_password),
]


def _public_otp_purpose(value: OtpPurpose) -> OtpPurpose:
    if value is OtpPurpose.PHONE_CHANGE:
        raise ValueError("phone_change codes are requested via /api/v1/users/me/change-phone")
    return value


PublicOtpPurpose = Annotated[OtpPurpose, AfterValidator(_public_otp_purpose)]
OtpCode = Annotated[str, StringConstraints(pattern=r"^\d{6}$"), Field(examples=["123456"])]
Email = Annotated[EmailStr, AfterValidator(str.lower)]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RegisterRequest(_Request):
    phone: Phone
    password: Password
    email: Email


class LoginRequest(_Request):
    phone: Phone
    password: Annotated[str, Field(min_length=1, max_length=BCRYPT_MAX_BYTES)]
    otp_token: str | None = Field(
        default=None,
        description="Required when 2FA is enabled: token from /verify-otp with purpose=login",
    )


class RefreshRequest(_Request):
    refresh_token: str


class SendOtpRequest(_Request):
    phone: Phone
    purpose: PublicOtpPurpose


class VerifyOtpRequest(_Request):
    phone: Phone
    code: OtpCode
    purpose: PublicOtpPurpose


class ResetPasswordRequest(_Request):
    phone: Phone
    otp_token: str
    new_password: Password


class TwoFactorRequest(_Request):
    otp_token: str = Field(description="Token from /verify-otp with purpose=two_factor")


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    phone: str
    email: str | None
    full_name: str | None
    address: str | None
    status: UserStatus
    role: UserRole
    is_2fa_enabled: bool
    created_at: datetime


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: Literal["bearer"] = "bearer"  # noqa: S105 - OAuth2 token type, not a secret
    expires_in: int = Field(description="Access token lifetime, seconds")
    refresh_expires_in: int = Field(description="Refresh token lifetime, seconds")


class SendOtpResponse(BaseModel):
    expires_in: int = Field(description="Code lifetime, seconds")
    resend_after: int = Field(description="Seconds until another code may be requested")


class VerifyOtpResponse(BaseModel):
    verified: bool
    otp_token: str | None = None
    otp_token_expires_in: int | None = None
    attempts_left: int


class TwoFactorResponse(BaseModel):
    is_2fa_enabled: bool
