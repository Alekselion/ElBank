"""Brokerage (simplified): quotes, market orders sent as limit orders, positions.

An order is sent with the current quote as its limit price, so the broker cannot
fill a buy above the money that was checked. The account (and, for a sale, the
position) stays locked while the broker answers; the order, the posting and the
position change are committed together. A broker refusal is stored as a
``rejected`` order without money movement; an unavailable broker → 503 and nothing
is stored.
"""

import uuid
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients import (
    BrokerClient,
    BrokerRejectedError,
    BrokerUnavailableError,
    OrderFill,
    Quotes,
)
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.base import PRICE
from elbank.db.models import (
    Account,
    AccountStatus,
    Currency,
    InvestmentOrder,
    InvestmentOrderStatus,
    InvestmentPosition,
    InvestmentSide,
    OperationType,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.product import (
    Portfolio,
    PortfolioPosition,
    StockQuote,
    StockQuotes,
)
from elbank.services.account_cache import AccountListCache
from elbank.services.finance import ZERO, fmt, money
from elbank.services.ledger import lock_accounts
from elbank.services.operation_service import OperationService

logger = get_logger(__name__)

_OPERATION_TYPES = {
    InvestmentSide.BUY: OperationType.INVESTMENT_BUY,
    InvestmentSide.SELL: OperationType.INVESTMENT_SELL,
}


def _order_payload(order: InvestmentOrder) -> dict[str, Any]:
    return {
        "order_id": str(order.id),
        "user_id": str(order.user_id),
        "account_id": str(order.account_id),
        "ticker": order.ticker,
        "side": order.side.value,
        "quantity": order.quantity,
        "status": order.status.value,
        "price": format(order.price, "f") if order.price is not None else None,
        "amount": fmt(order.amount) if order.amount is not None else None,
        "reject_code": order.reject_code,
    }


class InvestmentService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        broker: BrokerClient,
        cache: AccountListCache,
        publisher: EventPublisher,
    ) -> None:
        self._session = session
        self._broker = broker
        self._cache = cache
        self._publisher = publisher
        self._ops = OperationService(session)

    async def stocks(self) -> StockQuotes:
        quotes = await self._quotes()
        return StockQuotes(
            as_of=quotes.as_of,
            quotes=[StockQuote.model_validate(q.model_dump()) for q in quotes.quotes],
        )

    async def place_order(
        self,
        user: User,
        *,
        account_id: uuid.UUID,
        ticker: str,
        quantity: int,
        side: InvestmentSide,
    ) -> InvestmentOrder:
        quote = (await self._quotes()).find(ticker)
        if quote is None:
            raise UnprocessableError(f"Unknown ticker {ticker}")
        estimate = money(quote.price * quantity)
        if estimate <= 0:
            raise UnprocessableError("Order amount is less than one kopeck")

        account = await self._lock_account(user, account_id, quote.currency, ticker)
        position: InvestmentPosition | None = None
        if side is InvestmentSide.BUY:
            if account.balance < estimate:
                raise UnprocessableError("Insufficient funds on the account")
        else:
            position = await self._lock_position(user, ticker)
            if position is None or position.quantity < quantity:
                held = position.quantity if position else 0
                raise UnprocessableError(f"Not enough {ticker} to sell: {held} held")

        order = InvestmentOrder(
            id=uuid.uuid4(),
            user_id=user.id,
            account_id=account.id,
            ticker=ticker,
            side=side,
            quantity=quantity,
            limit_price=quote.price,
            currency=quote.currency,
        )
        try:
            fill = await self._broker.place_order(
                order_id=order.id,
                account_number=account.number,
                ticker=ticker,
                side=side,
                quantity=quantity,
                limit_price=quote.price,
            )
        except BrokerRejectedError as exc:
            order.status = InvestmentOrderStatus.REJECTED
            order.reject_code = exc.code[:64]
            order.reject_reason = exc.message[:255] or None
        except BrokerUnavailableError as exc:
            raise ExternalServiceError("Broker is unavailable, try again later") from exc
        else:
            await self._settle(order, fill, account, position)

        self._session.add(order)
        await self._session.commit()
        if order.status is InvestmentOrderStatus.FILLED:
            await self._cache.invalidate(user.id)

        logger.info(
            "investment order created",
            order_id=str(order.id),
            ticker=ticker,
            side=side.value,
            quantity=quantity,
            status=order.status.value,
        )
        await self._publisher.publish_safely(
            EventType.INVESTMENT_ORDER_CREATED, _order_payload(order), key=str(user.id)
        )
        return order

    async def portfolio(self, user: User) -> Portfolio:
        rows = await self._session.scalars(
            select(InvestmentPosition)
            .where(InvestmentPosition.user_id == user.id)
            .order_by(InvestmentPosition.ticker)
        )
        positions: Sequence[InvestmentPosition] = rows.all()
        quotes: Quotes | None = None
        if positions:
            try:
                quotes = await self._broker.quotes()
            except BrokerUnavailableError:
                logger.warning("portfolio shown without market prices: broker unavailable")

        items: list[PortfolioPosition] = []
        for p in positions:
            quote = quotes.find(p.ticker) if quotes else None
            cost = money(p.avg_price * p.quantity)
            value = money(quote.price * p.quantity) if quote else None
            items.append(
                PortfolioPosition(
                    ticker=p.ticker,
                    name=quote.name if quote else None,
                    quantity=p.quantity,
                    currency=Currency(p.currency),
                    avg_price=p.avg_price,
                    cost=cost,
                    market_price=quote.price if quote else None,
                    market_value=value,
                    profit=value - cost if value is not None else None,
                )
            )
        values = [i.market_value for i in items]
        return Portfolio(
            positions=items,
            total_cost=sum((i.cost for i in items), ZERO),
            total_market_value=(
                sum((v for v in values if v is not None), ZERO) if None not in values else None
            ),
            quotes_as_of=quotes.as_of if quotes else None,
        )

    # ---------- internals ----------

    async def _quotes(self) -> Quotes:
        try:
            return await self._broker.quotes()
        except BrokerUnavailableError as exc:
            raise ExternalServiceError("Broker is unavailable, try again later") from exc

    async def _lock_account(
        self, user: User, account_id: uuid.UUID, currency: str, ticker: str
    ) -> Account:
        account = (await lock_accounts(self._session, {account_id})).get(account_id)
        if account is None or account.user_id != user.id:
            raise NotFoundError("Account not found")
        if account.status is not AccountStatus.ACTIVE:
            raise InvalidStateError(f"Account must be active, it is {account.status.value}")
        if account.currency != currency:
            raise UnprocessableError(f"{ticker} is traded in {currency}")
        return account

    async def _lock_position(self, user: User, ticker: str) -> InvestmentPosition | None:
        position: InvestmentPosition | None = await self._session.scalar(
            select(InvestmentPosition)
            .where(InvestmentPosition.user_id == user.id, InvestmentPosition.ticker == ticker)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return position

    async def _settle(
        self,
        order: InvestmentOrder,
        fill: OrderFill,
        account: Account,
        position: InvestmentPosition | None,
    ) -> None:
        amount = money(fill.price * fill.quantity)
        operation = self._ops.create_completed(
            account_id=account.id,
            user_id=order.user_id,
            type=_OPERATION_TYPES[order.side],
            amount=amount,
            currency=account.currency,
            description=f"{order.side.value} {order.quantity} {order.ticker}",
        )
        order.status = InvestmentOrderStatus.FILLED
        order.price = fill.price
        order.amount = amount
        order.broker_order_id = fill.broker_order_id
        order.operation_id = operation.id

        if order.side is InvestmentSide.BUY:
            account.balance -= amount
            await self._add_to_position(order, fill.price)
            return
        assert position is not None
        account.balance += amount
        position.quantity -= order.quantity
        if position.quantity == 0:
            await self._session.delete(position)

    async def _add_to_position(self, order: InvestmentOrder, price: Decimal) -> None:
        """Upsert: a concurrent first purchase of the same ticker cannot duplicate it."""
        table = InvestmentPosition.__table__
        stmt = insert(InvestmentPosition).values(
            user_id=order.user_id,
            ticker=order.ticker,
            quantity=order.quantity,
            avg_price=price,
            currency=order.currency,
        )
        new_quantity = table.c.quantity + stmt.excluded.quantity
        weighted_avg = func.round(
            (
                table.c.quantity * table.c.avg_price
                + stmt.excluded.quantity * stmt.excluded.avg_price
            )
            / new_quantity,
            PRICE.scale,
        )
        await self._session.execute(
            stmt.on_conflict_do_update(
                index_elements=[InvestmentPosition.user_id, InvestmentPosition.ticker],
                set_={
                    "quantity": new_quantity,
                    "avg_price": weighted_avg,
                    "updated_at": func.now(),
                },
            )
        )
