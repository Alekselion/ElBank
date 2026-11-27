"""Investments (``/api/v1/investments``, simplified): quotes, orders, portfolio."""

from fastapi import APIRouter, status

from elbank.api.deps import CurrentUserDep, InvestmentServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.product import (
    InvestmentOrderRead,
    InvestmentOrderRequest,
    Portfolio,
    StockQuotes,
)

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_BROKER_DOWN: Responses = {
    status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Broker is unavailable")
}

router = APIRouter(tags=["investments"], responses=_AUTH_ERRORS)


@router.get("/stocks", summary="Stock quotes from the broker", responses=_BROKER_DOWN)
async def list_stocks(_: CurrentUserDep, investments: InvestmentServiceDep) -> StockQuotes:
    return await investments.stocks()


@router.post(
    "/orders",
    status_code=status.HTTP_201_CREATED,
    summary="Buy/sell at the current quote → investment.order.created (filled or rejected)",
    responses={
        status.HTTP_404_NOT_FOUND: error_response("Account not found"),
        status.HTTP_409_CONFLICT: error_response("Account is not active"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
            "Unknown ticker, insufficient funds or securities"
        ),
        **_BROKER_DOWN,
    },
)
async def place_order(
    body: InvestmentOrderRequest, user: CurrentUserDep, investments: InvestmentServiceDep
) -> InvestmentOrderRead:
    order = await investments.place_order(
        user,
        account_id=body.account_id,
        ticker=body.ticker,
        quantity=body.quantity,
        side=body.side,
    )
    return InvestmentOrderRead.model_validate(order)


@router.get("/portfolio", summary="Current user's positions valued at market prices")
async def get_portfolio(user: CurrentUserDep, investments: InvestmentServiceDep) -> Portfolio:
    return await investments.portfolio(user)
