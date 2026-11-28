"""Operation history and export schemas."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field

from elbank.db.models import OperationStatus, OperationType, PaymentProvider, SpendingCategory
from elbank.schemas.account import Money


class Direction(StrEnum):
    CREDIT = "credit"
    DEBIT = "debit"


class OperationRead(BaseModel):
    id: uuid.UUID
    account_id: uuid.UUID
    account_number: str
    type: OperationType
    direction: Direction
    category: SpendingCategory | None = Field(
        description="Expense category of a debit; null for credits, conversions, compensations"
    )
    amount: Money
    currency: str
    status: OperationStatus
    counterparty_account: str | None
    description: str | None
    created_at: datetime


class OperationDetail(OperationRead):
    provider: PaymentProvider | None = Field(description="Set for payments")
    error_code: str | None
    external_id: str | None
    exchange_rate: Decimal | None
    parent_operation_id: uuid.UUID | None = Field(
        description="Main operation of a fee / credit leg / compensation"
    )
    reversed_operation_id: uuid.UUID | None = Field(
        description="Set for a compensation: the posting it cancels"
    )
    initiator_id: uuid.UUID | None = Field(
        description="User who initiated the operation; null for postings made by the bank"
    )
    updated_at: datetime


class OperationPage(BaseModel):
    items: list[OperationRead]
    total: int = Field(description="Total number of matching operations")
    page: int
    size: int


# ---------- export ----------


class ExportFormat(StrEnum):
    CSV = "csv"
    PDF = "pdf"


class ExportJobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"


class ExportJobAccepted(BaseModel):
    job_id: uuid.UUID
    status: ExportJobStatus
    status_url: str = Field(description="Poll this URL for the job status")


class ExportJobRead(BaseModel):
    job_id: uuid.UUID
    format: ExportFormat
    date_from: date
    date_to: date
    status: ExportJobStatus
    created_at: datetime
    completed_at: datetime | None
    rows: int | None
    error: str | None
    download_url: str | None = Field(description="File URL; set when status is ready")
