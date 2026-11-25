"""KYC request/response schemas."""

import uuid
from datetime import date, datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from elbank.db.models import KycStatus

PassportSeries = Annotated[str, StringConstraints(pattern=r"^\d{4}$")]
PassportNumber = Annotated[str, StringConstraints(pattern=r"^\d{6}$")]


class KycRequestRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: KycStatus
    passport_series: str
    passport_number: str
    birth_date: date
    reject_reason: str | None
    decided_at: datetime | None
    created_at: datetime
    updated_at: datetime


class KycRequestAdminRead(KycRequestRead):
    user_id: uuid.UUID
    decided_by_id: uuid.UUID | None


class KycRejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1000)]


class Page[T](BaseModel):
    items: list[T]
    limit: int
    offset: int
    total: int = Field(description="Total number of matching items")
