"""Monthly spending budgets.

Spent = settled expenses of the budget's category and currency in the current
calendar month (UTC), counted like ``/analytics/categories``.

Alerts: the operations worker calls :meth:`BudgetChecker.check` for every
``operation.completed``. Crossing ``BUDGETS_WARN_PERCENT`` (80 %) sends
``notification.send`` with level ``warn``, reaching 100 % — level ``danger``. Each
level is sent once per budget and month: a Redis marker
``budget_alert:{budget_id}:{YYYY-MM}:{level}`` (``SET NX``) deduplicates replays and
later operations. Changing the limit clears the markers of the current month.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import BudgetSettings
from elbank.core.exceptions import ConflictError, NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    Budget,
    Operation,
    OperationStatus,
    OperationType,
    PaymentProvider,
    SpendingCategory,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.budget import (
    BudgetCreate,
    BudgetLevel,
    BudgetStatus,
    BudgetStatusReport,
    BudgetUpdate,
)
from elbank.services.analytics import share_percent, spending_by_category
from elbank.services.categories import CONVERSION_TYPES, category_of, is_credit

logger = get_logger(__name__)

_ZERO = Decimal(0)
_HUNDRED = Decimal(100)
# A marker must outlive its month; it is useless afterwards.
_ALERT_TTL_SECONDS = 40 * 24 * 3600
_ALERT_LEVELS = (BudgetLevel.WARN, BudgetLevel.DANGER)


def month_bounds(moment: datetime) -> tuple[datetime, datetime]:
    """Start of the UTC month containing ``moment`` and of the next one."""
    utc = moment.astimezone(UTC)
    start = datetime(utc.year, utc.month, 1, tzinfo=UTC)
    end = datetime(utc.year + utc.month // 12, utc.month % 12 + 1, 1, tzinfo=UTC)
    return start, end


def period_of(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m")


def level_for(percent: Decimal, warn_percent: Decimal) -> BudgetLevel:
    if percent >= _HUNDRED:
        return BudgetLevel.DANGER
    if percent >= warn_percent:
        return BudgetLevel.WARN
    return BudgetLevel.OK


class BudgetAlerts:
    """Redis markers of alerts already sent."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @staticmethod
    def _key(budget_id: uuid.UUID, period: str, level: BudgetLevel) -> str:
        return f"budget_alert:{budget_id}:{period}:{level.value}"

    async def claim(self, budget_id: uuid.UUID, period: str, level: BudgetLevel) -> bool:
        """``True`` if this alert has not been sent yet (and is now reserved)."""
        key = self._key(budget_id, period, level)
        return bool(await self._redis.set(key, "1", nx=True, ex=_ALERT_TTL_SECONDS))

    async def release(self, budget_id: uuid.UUID, period: str, level: BudgetLevel) -> None:
        await self._redis.delete(self._key(budget_id, period, level))

    async def reset(self, budget_id: uuid.UUID, period: str) -> None:
        """Best effort (called after a committed change): a failure only skips alerts."""
        try:
            await self._redis.delete(*(self._key(budget_id, period, lvl) for lvl in _ALERT_LEVELS))
        except RedisError as exc:
            logger.warning("budget alert reset failed", budget_id=str(budget_id), error=str(exc))


class BudgetService:
    """API side: CRUD and the current month status."""

    def __init__(
        self, session: AsyncSession, alerts: BudgetAlerts, settings: BudgetSettings
    ) -> None:
        self._session = session
        self._alerts = alerts
        self._settings = settings

    async def list_for_user(self, user: User) -> list[Budget]:
        rows = await self._session.scalars(
            select(Budget)
            .where(Budget.user_id == user.id)
            .order_by(Budget.currency, Budget.category)
        )
        return list(rows)

    async def get_for_user(self, user: User, budget_id: uuid.UUID) -> Budget:
        budget = await self._session.scalar(
            select(Budget).where(Budget.id == budget_id, Budget.user_id == user.id)
        )
        if budget is None:
            raise NotFoundError("Budget not found")
        return budget

    async def create(self, user: User, data: BudgetCreate) -> Budget:
        count = await self._session.scalar(
            select(func.count()).select_from(Budget).where(Budget.user_id == user.id)
        )
        if (count or 0) >= self._settings.max_per_user:
            raise UnprocessableError(
                f"No more than {self._settings.max_per_user} budgets per client"
            )
        budget = Budget(
            id=uuid.uuid4(),
            user_id=user.id,
            category=data.category,
            currency=data.currency.value,
            monthly_limit=data.monthly_limit,
        )
        self._session.add(budget)
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            raise ConflictError(
                f"Budget for {data.category.value} in {data.currency.value} already exists"
            ) from exc
        logger.info(
            "budget created",
            budget_id=str(budget.id),
            category=budget.category.value,
            currency=budget.currency,
        )
        return budget

    async def update(self, user: User, budget_id: uuid.UUID, data: BudgetUpdate) -> Budget:
        budget = await self.get_for_user(user, budget_id)
        budget.monthly_limit = data.monthly_limit
        await self._session.commit()
        # The new limit gets its own alerts this month.
        await self._alerts.reset(budget.id, period_of(datetime.now(UTC)))
        logger.info("budget updated", budget_id=str(budget.id))
        return budget

    async def delete(self, user: User, budget_id: uuid.UUID) -> None:
        budget = await self.get_for_user(user, budget_id)
        await self._session.delete(budget)
        await self._session.commit()
        await self._alerts.reset(budget.id, period_of(datetime.now(UTC)))
        logger.info("budget deleted", budget_id=str(budget.id))

    async def status(self, user: User) -> BudgetStatusReport:
        budgets = await self.list_for_user(user)
        if not budgets:
            return BudgetStatusReport(items=[])
        now = datetime.now(UTC)
        start, end = month_bounds(now)
        spent = {
            (t.category, t.currency): t.amount
            for t in await spending_by_category(self._session, user.id, start, end)
        }
        period = period_of(now)
        items = []
        for budget in budgets:
            amount = spent.get((budget.category, budget.currency), _ZERO)
            percent = share_percent(amount, budget.monthly_limit)
            items.append(
                BudgetStatus(
                    budget_id=budget.id,
                    category=budget.category,
                    currency=budget.currency,
                    period=period,
                    spent=amount,
                    monthly_limit=budget.monthly_limit,
                    remaining=max(budget.monthly_limit - amount, _ZERO),
                    percent=percent,
                    level=level_for(percent, self._settings.warn_percent),
                )
            )
        return BudgetStatusReport(items=items)


class BudgetChecker:
    """Worker side: alerts for the budget an operation counts towards."""

    def __init__(
        self, alerts: BudgetAlerts, publisher: EventPublisher, settings: BudgetSettings
    ) -> None:
        self._alerts = alerts
        self._publisher = publisher
        self._settings = settings

    async def check(
        self,
        session: AsyncSession,
        operation: Operation,
        owner_id: uuid.UUID,
        provider: PaymentProvider | None,
    ) -> None:
        category = self._expense_category(operation, provider)
        if category is None or await self._is_own_transfer(session, operation, owner_id):
            return
        budget = await session.scalar(
            select(Budget).where(
                Budget.user_id == owner_id,
                Budget.category == category,
                Budget.currency == operation.currency,
            )
        )
        if budget is None:
            return

        start, end = month_bounds(operation.created_at)
        spent = next(
            (
                t.amount
                for t in await spending_by_category(session, owner_id, start, end)
                if t.category is category and t.currency == budget.currency
            ),
            _ZERO,
        )
        percent = share_percent(spent, budget.monthly_limit)
        level = level_for(percent, self._settings.warn_percent)
        if level is BudgetLevel.OK:
            return

        period = period_of(operation.created_at)
        if not await self._alerts.claim(budget.id, period, level):
            return  # already notified this month
        sent = await self._publisher.publish_safely(
            EventType.NOTIFICATION_SEND,
            {
                "user_id": str(owner_id),
                "channel": "push",
                "template": f"budget.{level.value}",
                "level": level.value,
                "budget_id": str(budget.id),
                "category": category.value,
                "currency": budget.currency,
                "period": period,
                "spent": format(spent, "f"),
                "monthly_limit": format(budget.monthly_limit, "f"),
                "percent": format(percent, "f"),
                "operation_id": str(operation.id),
            },
            key=str(owner_id),
        )
        if not sent:
            # Let the next operation of the month try again.
            await self._alerts.release(budget.id, period, level)
            return
        logger.info(
            "budget alert sent",
            budget_id=str(budget.id),
            level=level.value,
            percent=str(percent),
        )

    @staticmethod
    def _expense_category(
        operation: Operation, provider: PaymentProvider | None
    ) -> SpendingCategory | None:
        if (
            operation.status is not OperationStatus.COMPLETED
            or operation.reversed_operation_id is not None
            or is_credit(operation.type)
            or operation.type in CONVERSION_TYPES
        ):
            return None
        return category_of(operation.type, provider)

    @staticmethod
    async def _is_own_transfer(
        session: AsyncSession, operation: Operation, owner_id: uuid.UUID
    ) -> bool:
        if operation.type is not OperationType.TRANSFER_OUT:
            return False
        own = await session.scalar(
            select(
                exists().where(
                    Account.number == operation.counterparty_account, Account.user_id == owner_id
                )
            )
        )
        return bool(own)
