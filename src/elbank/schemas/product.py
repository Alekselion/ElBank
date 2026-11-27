"""Credits, deposits and investments: request/response schemas.

Money and prices are strings in JSON (``"1500.00"``, ``"285.4000"``), rates are
percent per year as strings (``"17.9000"``).
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, WithJsonSchema

from elbank.db.models import (
    Credit,
    CreditStatus,
    Currency,
    Deposit,
    DepositStatus,
    InvestmentOrderStatus,
    InvestmentSide,
)
from elbank.schemas.account import Money, PositiveMoney
from elbank.schemas.auth import OtpCode

Price = Annotated[
    Decimal,
    PlainSerializer(lambda v: format(v, "f"), return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "examples": ["285.4000"]}),
]
Rate = Annotated[
    Decimal,
    PlainSerializer(lambda v: format(v, "f"), return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "description": "Percent per year", "examples": ["17.9"]}),
]
TermMonths = Annotated[int, Field(ge=1, le=600, examples=[24])]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ======================================================================= credits


class CreditApplicationRequest(_Request):
    account_id: uuid.UUID = Field(description="RUB account: disbursement and repayments")
    amount: PositiveMoney
    term_months: TermMonths
    purpose: str = Field(min_length=3, max_length=255, examples=["Ремонт квартиры"])


class CreditSignRequest(_Request):
    otp_code: OtpCode


class CreditRepayRequest(_Request):
    amount: PositiveMoney
    early: bool = Field(
        default=False,
        description=(
            "false: pays the installments in due order (interest first); "
            "true: reduces the principal and recalculates the remaining installments"
        ),
    )


class ScheduleEntry(BaseModel):
    n: int
    due_date: date
    payment: Money
    principal: Money
    interest: Money
    paid_principal: Money
    paid_interest: Money
    paid_at: datetime | None


class CreditRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    account_id: uuid.UUID
    amount: Money
    currency: Currency = Currency.RUB
    rate: Rate
    term_months: int
    purpose: str
    status: CreditStatus
    score: int | None
    scoring_comment: str | None
    monthly_payment: Money | None
    principal_outstanding: Money
    signed_at: datetime | None
    closed_at: datetime | None
    created_at: datetime
    updated_at: datetime


class CreditSchedule(BaseModel):
    credit_id: uuid.UUID
    status: CreditStatus
    principal_outstanding: Money
    left_to_pay: Money = Field(description="Unpaid interest and principal of all installments")
    installments: list[ScheduleEntry]

    @classmethod
    def from_credit(cls, credit: Credit, left_to_pay: Decimal) -> Self:
        return cls(
            credit_id=credit.id,
            status=credit.status,
            principal_outstanding=credit.principal_outstanding,
            left_to_pay=left_to_pay,
            installments=[ScheduleEntry.model_validate(e) for e in credit.schedule],
        )


class CreditRepayResult(BaseModel):
    credit: CreditRead
    operation_id: uuid.UUID
    paid_interest: Money
    paid_principal: Money


class SignOtpSent(BaseModel):
    expires_in: int = Field(description="Seconds the code is valid")
    resend_after: int = Field(description="Seconds before another code can be requested")


# ====================================================================== deposits


class DepositOpenRequest(_Request):
    account_id: uuid.UUID
    amount: PositiveMoney
    term_months: TermMonths
    rate: Annotated[Decimal, Field(gt=0, le=100, max_digits=7, decimal_places=4)]


class DepositTopUpRequest(_Request):
    amount: PositiveMoney


class AccrualEntry(BaseModel):
    date_from: date
    date_to: date = Field(description="Exclusive")
    days: int
    principal: Money
    interest: Money


class DepositRead(BaseModel):
    id: uuid.UUID
    account_id: uuid.UUID
    amount: Money = Field(description="Current principal")
    currency: Currency
    rate: Rate
    term_months: int
    status: DepositStatus
    start_date: date
    end_date: date
    accrued_interest: Money = Field(description="Interest posted up to accrued_until")
    accrued_until: date = Field(description="Exclusive")
    interest_to_date: Money = Field(
        description="Interest calculated for every day up to today (or the end date)"
    )
    paid_interest: Money | None = Field(description="Paid out on closing")
    closed_at: datetime | None
    created_at: datetime

    @classmethod
    def from_deposit(cls, deposit: Deposit, interest_to_date: Decimal) -> Self:
        return cls(
            id=deposit.id,
            account_id=deposit.account_id,
            amount=deposit.amount,
            currency=Currency(deposit.currency),
            rate=deposit.rate,
            term_months=deposit.term_months,
            status=deposit.status,
            start_date=deposit.start_date,
            end_date=deposit.end_date,
            accrued_interest=deposit.accrued_interest,
            accrued_until=deposit.accrued_until,
            interest_to_date=interest_to_date,
            paid_interest=deposit.paid_interest,
            closed_at=deposit.closed_at,
            created_at=deposit.created_at,
        )


class DepositDetails(DepositRead):
    accruals: list[AccrualEntry]

    @classmethod
    def from_deposit_details(cls, deposit: Deposit, interest_to_date: Decimal) -> Self:
        base = DepositRead.from_deposit(deposit, interest_to_date)
        entries: list[dict[str, Any]] = deposit.interest_schedule
        return cls(
            **dict(base),
            accruals=[AccrualEntry.model_validate(e) for e in entries],
        )


class DepositCloseResult(BaseModel):
    deposit: DepositRead
    early: bool = Field(description="Closed before the end date: interest at the early rate")
    principal: Money
    interest: Money
    operation_id: uuid.UUID = Field(description="deposit_close posting on the account")


class AccrualRequest(_Request):
    as_of: date | None = Field(
        default=None, description="Accrue interest for the days before this date; default today"
    )


class AccrualAccepted(BaseModel):
    as_of: date
    event_id: uuid.UUID


# =================================================================== investments


class StockQuote(BaseModel):
    ticker: str
    name: str
    price: Price
    currency: Currency
    change_percent: Rate


class StockQuotes(BaseModel):
    as_of: datetime
    quotes: list[StockQuote]


class InvestmentOrderRequest(_Request):
    account_id: uuid.UUID = Field(description="Account paid from (buy) or credited (sell)")
    ticker: str = Field(pattern=r"^[A-Z0-9]{1,12}$", examples=["SBER"])
    quantity: int = Field(gt=0, le=1_000_000)
    side: InvestmentSide


class InvestmentOrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    account_id: uuid.UUID
    ticker: str
    side: InvestmentSide
    quantity: int
    limit_price: Price = Field(description="Quote at the moment of the order")
    currency: Currency
    status: InvestmentOrderStatus
    price: Price | None = Field(description="Execution price")
    amount: Money | None
    broker_order_id: str | None
    reject_code: str | None
    reject_reason: str | None
    operation_id: uuid.UUID | None
    created_at: datetime


class PortfolioPosition(BaseModel):
    ticker: str
    name: str | None
    quantity: int
    currency: Currency
    avg_price: Price
    cost: Money = Field(description="avg_price * quantity")
    market_price: Price | None = Field(description="null when quotes are unavailable")
    market_value: Money | None
    profit: Money | None


class Portfolio(BaseModel):
    positions: list[PortfolioPosition]
    total_cost: Money
    total_market_value: Money | None
    quotes_as_of: datetime | None
