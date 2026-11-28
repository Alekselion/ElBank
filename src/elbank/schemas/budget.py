"""Budget schemas."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from elbank.db.models import Currency, SpendingCategory
from elbank.schemas.account import Money, PositiveMoney


class BudgetCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: SpendingCategory
    monthly_limit: PositiveMoney
    currency: Currency = Currency.RUB


class BudgetUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    monthly_limit: PositiveMoney


class BudgetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    category: SpendingCategory
    currency: str
    monthly_limit: Money
    created_at: datetime
    updated_at: datetime


class BudgetLevel(StrEnum):
    OK = "ok"
    WARN = "warn"
    DANGER = "danger"


class BudgetStatus(BaseModel):
    budget_id: uuid.UUID
    category: SpendingCategory
    currency: str
    period: str = Field(description="Calendar month (UTC), YYYY-MM", examples=["2026-11"])
    spent: Money
    monthly_limit: Money
    remaining: Money = Field(description="Zero when the limit is exceeded")
    percent: Decimal = Field(description="spent / limit * 100, two decimals")
    level: BudgetLevel


class BudgetStatusReport(BaseModel):
    items: list[BudgetStatus]
