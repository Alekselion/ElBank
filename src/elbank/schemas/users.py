"""Profile request/response schemas."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from elbank.schemas.auth import BCRYPT_MAX_BYTES, Email, OtpCode, Password, Phone

FullName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    Field(examples=["Иванов Иван Иванович"]),
]
Address = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=500),
    Field(examples=["г. Москва, ул. Тверская, д. 1, кв. 1"]),  # noqa: RUF001
]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProfileUpdateRequest(_Request):
    """Partial update: only sent fields change; ``null`` clears full_name/address."""

    full_name: FullName | None = None
    email: Email | None = Field(default=None, description="Cannot be cleared, only changed")
    address: Address | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("at least one field must be provided")
        if "email" in self.model_fields_set and self.email is None:
            raise ValueError("email cannot be removed")
        return self


class ChangePasswordRequest(_Request):
    old_password: Annotated[str, Field(min_length=1, max_length=BCRYPT_MAX_BYTES)]
    new_password: Password

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.old_password == self.new_password:
            raise ValueError("new password must differ from the current one")
        return self


class ChangePhoneRequest(_Request):
    new_phone: Phone


class ChangePhoneRequestResponse(BaseModel):
    expires_in: int = Field(description="Codes lifetime, seconds")
    resend_after: int = Field(description="Seconds until codes may be requested again")


class ChangePhoneConfirmRequest(_Request):
    old_code: OtpCode = Field(description="Code sent to the current phone")
    new_code: OtpCode = Field(description="Code sent to the new phone")


class NotificationSettings(BaseModel):
    """Which channels the user receives notifications through."""

    email: bool = True
    sms: bool = True
    push: bool = True
    in_app: bool = True


class NotificationSettingsUpdate(_Request):
    email: bool | None = None
    sms: bool | None = None
    push: bool | None = None
    in_app: bool | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.model_dump(exclude_none=True):
            raise ValueError("at least one channel must be provided")
        return self
