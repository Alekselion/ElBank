"""Users, their refresh tokens and KYC requests."""

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Index, String, Text, false
from sqlalchemy.orm import Mapped, mapped_column, relationship

from elbank.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDMixin, str_enum
from elbank.db.models.enums import KycStatus, UserRole, UserStatus

if TYPE_CHECKING:
    from elbank.db.models.account import Account


class User(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "users"

    phone: Mapped[str] = mapped_column(String(20), unique=True)  # E.164, e.g. +79991234567
    email: Mapped[str | None] = mapped_column(String(255), unique=True)  # stored lower-cased
    password_hash: Mapped[str] = mapped_column(String(255))
    status: Mapped[UserStatus] = mapped_column(
        str_enum(UserStatus, "user_status"), default=UserStatus.ACTIVE, server_default="active"
    )
    role: Mapped[UserRole] = mapped_column(
        str_enum(UserRole, "user_role"), default=UserRole.CLIENT, server_default="client"
    )
    is_2fa_enabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())

    accounts: Mapped[list[Account]] = relationship(back_populates="user", lazy="raise")


class RefreshToken(UUIDMixin, CreatedAtMixin, Base):
    """Only a hash of the token is stored; the raw token is given to the client once."""

    __tablename__ = "refresh_tokens"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)  # sha256 hex
    expires_at: Mapped[datetime]
    revoked_at: Mapped[datetime | None]


class KycRequest(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "kyc_requests"
    __table_args__ = (Index(None, "user_id", "status"),)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    status: Mapped[KycStatus] = mapped_column(
        str_enum(KycStatus, "kyc_status"), default=KycStatus.PENDING, server_default="pending"
    )
    passport_series: Mapped[str] = mapped_column(String(4))
    passport_number: Mapped[str] = mapped_column(String(6))
    birth_date: Mapped[date]
    external_ref: Mapped[str | None] = mapped_column(String(64), unique=True)  # KYC provider id
    reject_reason: Mapped[str | None] = mapped_column(Text)
