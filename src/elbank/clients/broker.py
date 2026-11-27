"""Broker client (WireMock): quotes and limit orders.

``GET /broker/quotes``::

    {"as_of": "...", "quotes": [{"ticker": "SBER", "name": "...", "price": "285.40",
      "currency": "RUB", "change_percent": "-0.35"}, ...]}

``POST /broker/orders`` (``Idempotency-Key: <order id>``)::

    {"order_id": "...", "account_number": "...", "ticker": "SBER", "side": "buy",
     "quantity": 10, "limit_price": "285.4000"}
    -> 200 {"status": "filled", "broker_order_id": "...", "price": "285.4000",
            "quantity": 10, "executed_at": "..."}
    -> 4xx {"status": "rejected", "code": "...", "message": "..."}

A buy is never filled above the limit price, a sell never below it. 5xx, timeouts
and connection errors are retried (``BROKER_RETRIES``): the order is idempotent by
its key. A 4xx → :class:`BrokerRejectedError`, other final failures →
:class:`BrokerUnavailableError`.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from elbank.clients.external_transfer import IDEMPOTENCY_HEADER
from elbank.clients.http import is_server_or_transport_error, retrying
from elbank.core.config import BrokerSettings
from elbank.core.logging import get_logger
from elbank.db.models import InvestmentSide

logger = get_logger(__name__)

_SERVICE = "broker"
_QUOTES_PATH = "/broker/quotes"
_ORDERS_PATH = "/broker/orders"


class Quote(BaseModel):
    ticker: str = Field(pattern=r"^[A-Z0-9]{1,12}$")
    name: str
    price: Decimal = Field(gt=0, decimal_places=4)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    change_percent: Decimal


class Quotes(BaseModel):
    as_of: datetime
    quotes: list[Quote]

    def find(self, ticker: str) -> Quote | None:
        return next((q for q in self.quotes if q.ticker == ticker), None)


class OrderFill(BaseModel):
    status: Literal["filled"]
    broker_order_id: str = Field(max_length=64)
    price: Decimal = Field(gt=0, decimal_places=4)
    quantity: int = Field(gt=0)
    executed_at: datetime


class _Rejection(BaseModel):
    status: Literal["rejected"]
    code: str
    message: str = ""


class BrokerRejectedError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class BrokerUnavailableError(Exception):
    """The broker is unavailable or answered with something we cannot interpret."""


class BrokerClient:
    def __init__(self, http: httpx.AsyncClient, settings: BrokerSettings) -> None:
        self._http = http
        self._settings = settings

    async def quotes(self) -> Quotes:
        response = await self._request("GET", _QUOTES_PATH, action="quotes")
        if response.is_client_error:
            raise BrokerUnavailableError(f"quotes answered {response.status_code}")
        return self._parse(response, Quotes, action="quotes")

    async def place_order(
        self,
        *,
        order_id: uuid.UUID,
        account_number: str,
        ticker: str,
        side: InvestmentSide,
        quantity: int,
        limit_price: Decimal,
    ) -> OrderFill:
        body = {
            "order_id": str(order_id),
            "account_number": account_number,
            "ticker": ticker,
            "side": side.value,
            "quantity": quantity,
            "limit_price": format(limit_price, "f"),
        }
        response = await self._request(
            "POST",
            _ORDERS_PATH,
            action="order",
            json=body,
            headers={IDEMPOTENCY_HEADER: str(order_id)},
        )
        if response.is_client_error:
            rejection = self._parse(response, _Rejection, action="order")
            logger.warning("broker rejected the order", order_id=str(order_id), code=rejection.code)
            raise BrokerRejectedError(rejection.code, rejection.message)

        fill = self._parse(response, OrderFill, action="order")
        worse_than_limit = (
            fill.price > limit_price if side is InvestmentSide.BUY else fill.price < limit_price
        )
        if fill.quantity != quantity or worse_than_limit:
            logger.error(
                "broker fill does not match the order",
                order_id=str(order_id),
                quantity=fill.quantity,
                price=str(fill.price),
                limit_price=str(limit_price),
            )
            raise BrokerUnavailableError("fill does not match the order")
        logger.info("broker filled the order", order_id=str(order_id), price=str(fill.price))
        return fill

    # ---------- internals ----------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        action: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            async for attempt in retrying(
                self._settings.retries, _SERVICE, retry_if=is_server_or_transport_error
            ):
                with attempt:
                    response = await self._http.request(
                        method,
                        path,
                        json=json,
                        headers=headers,
                        timeout=self._settings.timeout_seconds,
                    )
                    if response.is_server_error:
                        response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "broker request failed", action=action, error=f"{type(exc).__name__}: {exc}"
            )
            raise BrokerUnavailableError(str(exc)) from exc
        return response

    @staticmethod
    def _parse[M: BaseModel](response: httpx.Response, model: type[M], *, action: str) -> M:
        try:
            return model.model_validate_json(response.content)
        except ValidationError as exc:
            logger.error(  # noqa: TRY400 - not our bug, traceback is noise
                "broker answered with an unexpected body",
                action=action,
                http_status=response.status_code,
                error=str(exc),
            )
            raise BrokerUnavailableError(f"unexpected {action} response") from exc
