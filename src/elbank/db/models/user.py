"""Users, their refresh tokens and KYC requests."""

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, ForeignKey, Index, String, Text, false, text
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
    full_name: Mapped[str | None] = mapped_column(String(255))
    address: Mapped[str | None] = mapped_column(String(500))
    # Per-channel switches, e.g. {"email": true, "sms": false}; missing keys mean defaults.
    notification_settings: Mapped[dict[str, Any]] = mapped_column(
        default=dict, server_default=text("'{}'::jsonb")
    )
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
    __table_args__ = (
        Index(None, "user_id", "status"),
        Index(None, "status", "created_at"),  # manager queue
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    status: Mapped[KycStatus] = mapped_column(
        str_enum(KycStatus, "kyc_status"), default=KycStatus.PENDING, server_default="pending"
    )
    passport_series: Mapped[str] = mapped_column(String(4))
    passport_number: Mapped[str] = mapped_column(String(6))
    birth_date: Mapped[date]
    external_ref: Mapped[str | None] = mapped_column(String(64), unique=True)  # KYC provider id
    reject_reason: Mapped[str | None] = mapped_column(Text)
    # Set when a decision (approved/rejected) is made; NULL decided_by_id: automatic decision.
    decided_at: Mapped[datetime | None]
    decided_by_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
