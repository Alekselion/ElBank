"""Spending analytics (``/api/v1/analytics``), cached for ANALYTICS_CACHE_TTL_SECONDS."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query, status

from elbank.api.deps import AnalyticsServiceDep, CurrentUserDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.analytics import BalanceDynamicsReport, CategoryReport, MonthlyReport

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_INVALID: Responses = {status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Invalid period")}

router = APIRouter(tags=["analytics"], responses={**_AUTH_ERRORS, **_INVALID})

FromQuery = Annotated[date, Query(alias="from", description="First day (UTC), inclusive")]
ToQuery = Annotated[date, Query(alias="to", description="Last day (UTC), inclusive")]


@router.get(
    "/categories",
    summary="Expenses by category",
    description=(
        "Settled expenses per category and currency. Conversions and transfers between "
        "own accounts are not expenses."
    ),
)
async def categories(
    user: CurrentUserDep, analytics: AnalyticsServiceDep, date_from: FromQuery, date_to: ToQuery
) -> CategoryReport:
    return await analytics.categories(user, date_from, date_to)


@router.get("/monthly", summary="Income and expenses by month of a year (UTC)")
async def monthly(
    user: CurrentUserDep, analytics: AnalyticsServiceDep, year: Annotated[int, Query()]
) -> MonthlyReport:
    return await analytics.monthly(user, year)


@router.get(
    "/balance-dynamics",
    summary="Daily balance change per currency",
    description="Credits, debits, net and cumulative change per day; all own accounts or one.",
    responses={status.HTTP_404_NOT_FOUND: error_response("Account not found")},
)
async def balance_dynamics(
    user: CurrentUserDep,
    analytics: AnalyticsServiceDep,
    date_from: FromQuery,
    date_to: ToQuery,
    account_id: uuid.UUID | None = None,
) -> BalanceDynamicsReport:
    return await analytics.balance_dynamics(user, date_from, date_to, account_id)
