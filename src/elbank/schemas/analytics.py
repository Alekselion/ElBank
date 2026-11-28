"""Analytics responses.

Amounts are never summed across currencies: every aggregate carries its currency.
Only settled money counts (see :mod:`elbank.services.categories`).
"""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field

from elbank.db.models import SpendingCategory
from elbank.schemas.account import Money


class CategorySpending(BaseModel):
    category: SpendingCategory
    currency: str
    amount: Money
    operations: int
    share_percent: Decimal = Field(description="Share of all expenses in this currency")


class CategoryReport(BaseModel):
    date_from: date
    date_to: date
    items: list[CategorySpending]


class MonthTotals(BaseModel):
    month: int = Field(ge=1, le=12)
    currency: str
    income: Money
    expense: Money
    net: Money


class MonthlyReport(BaseModel):
    year: int
    items: list[MonthTotals] = Field(description="All 12 months for every currency with data")


class BalanceDay(BaseModel):
    date: date
    credit: Money
    debit: Money
    net: Money
    cumulative: Money = Field(description="Net change since the start of the period")


class CurrencyBalanceDynamics(BaseModel):
    currency: str
    days: list[BalanceDay]


class BalanceDynamicsReport(BaseModel):
    date_from: date
    date_to: date
    items: list[CurrencyBalanceDynamics]
