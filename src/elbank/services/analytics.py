"""Spending analytics of a client, cached in Redis.

What counts is defined in :mod:`elbank.services.categories`: settled operations
only; categories and monthly income/expense leave internal moves (conversions,
transfers between own accounts) out, the balance dynamics keeps them (they do
change the balance of an account / currency). Amounts are grouped per currency.

Cache: one Redis hash ``analytics:{user_id}`` per client, one field per report and
parameters, each field with its own TTL (``HEXPIRE``, ``ANALYTICS_CACHE_TTL_SECONDS``).
The operations worker drops the whole hash on ``operation.completed`` of any of the
client's accounts. The cache is an optimization: Redis errors fall back to the DB.
"""

import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import ColumnElement, Date, cast, extract, func, not_, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import AnalyticsSettings
from elbank.core.exceptions import NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Account, Operation, PaymentOrder, SpendingCategory, User
from elbank.schemas.analytics import (
    BalanceDay,
    BalanceDynamicsReport,
    CategoryReport,
    CategorySpending,
    CurrencyBalanceDynamics,
    MonthlyReport,
    MonthTotals,
)
from elbank.services.categories import (
    category_column,
    credit_column,
    internal_move_column,
    settled_filter,
)
from elbank.services.operations import utc_bounds, validate_period

logger = get_logger(__name__)

_ZERO = Decimal(0)
_PERCENT = Decimal("0.01")
_MIN_YEAR = 2000


def _utc_created_at() -> ColumnElement[datetime]:
    """``created_at`` as a UTC wall-clock ``timestamp`` (independent of the DB session zone)."""
    return func.timezone("UTC", Operation.created_at)


@dataclass(slots=True, frozen=True)
class CategoryTotal:
    category: SpendingCategory
    currency: str
    amount: Decimal
    operations: int


async def spending_by_category(
    session: AsyncSession, user_id: uuid.UUID, start: datetime, end: datetime
) -> list[CategoryTotal]:
    """Settled expenses of ``user_id`` in ``[start, end)`` per (category, currency)."""
    category = category_column().label("category")
    rows = await session.execute(
        select(category, Operation.currency, func.sum(Operation.amount), func.count())
        .select_from(Operation)
        .join(Account, Account.id == Operation.account_id)
        .outerjoin(PaymentOrder, PaymentOrder.operation_id == Operation.id)
        .where(
            Account.user_id == user_id,
            settled_filter(),
            not_(credit_column()),
            not_(internal_move_column(user_id)),
            Operation.created_at >= start,
            Operation.created_at < end,
        )
        .group_by(category, Operation.currency)
    )
    totals: dict[tuple[SpendingCategory, str], CategoryTotal] = {}
    for raw_category, currency, amount, count in rows.tuples():
        # Every debit type is mapped; OTHER guards against a type added later.
        cat = SpendingCategory(raw_category) if raw_category else SpendingCategory.OTHER
        prev = totals.get((cat, currency))
        totals[(cat, currency)] = CategoryTotal(
            cat,
            currency,
            Decimal(amount) + (prev.amount if prev else _ZERO),
            int(count) + (prev.operations if prev else 0),
        )
    return list(totals.values())


class AnalyticsCache:
    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    @staticmethod
    def _key(user_id: uuid.UUID) -> str:
        return f"analytics:{user_id}"

    async def get[M: BaseModel](self, user_id: uuid.UUID, field: str, model: type[M]) -> M | None:
        try:
            raw = await self._redis.hget(self._key(user_id), field)
        except RedisError as exc:
            logger.warning("analytics cache read failed", user_id=str(user_id), error=str(exc))
            return None
        if raw is None:
            return None
        try:
            return model.model_validate_json(raw)
        except ValidationError:
            logger.warning("analytics cache entry is stale, ignored", field=field)
            return None

    async def set(self, user_id: uuid.UUID, field: str, value: BaseModel) -> None:
        key = self._key(user_id)
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.hset(key, field, value.model_dump_json())
                pipe.hexpire(key, self._ttl, field)
                await pipe.execute()
        except RedisError as exc:
            logger.warning("analytics cache write failed", user_id=str(user_id), error=str(exc))

    async def invalidate(self, user_id: uuid.UUID) -> None:
        """Raises :class:`RedisError`: the caller decides whether a stale cache is fatal."""
        await self._redis.delete(self._key(user_id))


class AnalyticsService:
    def __init__(
        self, session: AsyncSession, cache: AnalyticsCache, settings: AnalyticsSettings
    ) -> None:
        self._session = session
        self._cache = cache
        self._settings = settings

    # ---------- categories ----------

    async def categories(self, user: User, date_from: date, date_to: date) -> CategoryReport:
        validate_period(date_from, date_to, self._settings.max_period_days)
        field = f"categories:{date_from}:{date_to}"
        if (cached := await self._cache.get(user.id, field, CategoryReport)) is not None:
            return cached

        start, end = utc_bounds(date_from, date_to)
        assert start is not None and end is not None
        totals = await spending_by_category(self._session, user.id, start, end)
        per_currency: dict[str, Decimal] = defaultdict(Decimal)
        for t in totals:
            per_currency[t.currency] += t.amount
        items = [
            CategorySpending(
                category=t.category,
                currency=t.currency,
                amount=t.amount,
                operations=t.operations,
                share_percent=share_percent(t.amount, per_currency[t.currency]),
            )
            for t in sorted(totals, key=lambda t: (t.currency, -t.amount, t.category))
        ]
        report = CategoryReport(date_from=date_from, date_to=date_to, items=items)
        await self._cache.set(user.id, field, report)
        return report

    # ---------- monthly ----------

    async def monthly(self, user: User, year: int) -> MonthlyReport:
        if not _MIN_YEAR <= year <= datetime.now(UTC).year:
            raise UnprocessableError(f"'year' must be between {_MIN_YEAR} and the current year")
        field = f"monthly:{year}"
        if (cached := await self._cache.get(user.id, field, MonthlyReport)) is not None:
            return cached

        month = extract("month", _utc_created_at()).label("month")
        credit = credit_column().label("credit")
        rows = await self._session.execute(
            select(month, Operation.currency, credit, func.sum(Operation.amount))
            .select_from(Operation)
            .join(Account, Account.id == Operation.account_id)
            .where(
                Account.user_id == user.id,
                settled_filter(),
                not_(internal_move_column(user.id)),
                Operation.created_at >= datetime(year, 1, 1, tzinfo=UTC),
                Operation.created_at < datetime(year + 1, 1, 1, tzinfo=UTC),
            )
            .group_by(month, Operation.currency, credit)
        )
        # currency -> month -> [income, expense]
        sums: dict[str, dict[int, list[Decimal]]] = defaultdict(
            lambda: {m: [_ZERO, _ZERO] for m in range(1, 13)}
        )
        for raw_month, currency, is_credit, amount in rows.tuples():
            sums[currency][int(raw_month)][0 if is_credit else 1] += Decimal(amount)

        items = [
            MonthTotals(month=m, currency=currency, income=inc, expense=exp, net=inc - exp)
            for currency in sorted(sums)
            for m, (inc, exp) in sums[currency].items()
        ]
        report = MonthlyReport(year=year, items=items)
        await self._cache.set(user.id, field, report)
        return report

    # ---------- balance dynamics ----------

    async def balance_dynamics(
        self, user: User, date_from: date, date_to: date, account_id: uuid.UUID | None
    ) -> BalanceDynamicsReport:
        validate_period(date_from, date_to, self._settings.max_period_days)
        field = f"balance:{date_from}:{date_to}:{account_id or 'all'}"
        if (cached := await self._cache.get(user.id, field, BalanceDynamicsReport)) is not None:
            return cached

        where = [Account.user_id == user.id]
        if account_id is not None:
            owned = await self._session.scalar(
                select(Account.id).where(Account.id == account_id, Account.user_id == user.id)
            )
            if owned is None:
                raise NotFoundError("Account not found")
            where.append(Operation.account_id == account_id)

        start, end = utc_bounds(date_from, date_to)
        assert start is not None and end is not None
        day = cast(_utc_created_at(), Date).label("day")
        credit = credit_column().label("credit")
        rows = await self._session.execute(
            select(day, Operation.currency, credit, func.sum(Operation.amount))
            .select_from(Operation)
            .join(Account, Account.id == Operation.account_id)
            .where(
                *where, settled_filter(), Operation.created_at >= start, Operation.created_at < end
            )
            .group_by(day, Operation.currency, credit)
        )
        # currency -> day -> [credit, debit]
        sums: dict[str, dict[date, list[Decimal]]] = defaultdict(dict)
        for raw_day, currency, is_credit, amount in rows.tuples():
            sums[currency].setdefault(raw_day, [_ZERO, _ZERO])[0 if is_credit else 1] += Decimal(
                amount
            )

        days = [date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)]
        items: list[CurrencyBalanceDynamics] = []
        for currency in sorted(sums):
            cumulative = _ZERO
            series: list[BalanceDay] = []
            for d in days:
                cr, db = sums[currency].get(d, (_ZERO, _ZERO))
                cumulative += cr - db
                series.append(
                    BalanceDay(date=d, credit=cr, debit=db, net=cr - db, cumulative=cumulative)
                )
            items.append(CurrencyBalanceDynamics(currency=currency, days=series))

        report = BalanceDynamicsReport(date_from=date_from, date_to=date_to, items=items)
        await self._cache.set(user.id, field, report)
        return report


def share_percent(part: Decimal, whole: Decimal) -> Decimal:
    """``part / whole * 100`` rounded to two decimals; zero for an empty whole."""
    if whole == 0:
        return _ZERO
    return (part * 100 / whole).quantize(_PERCENT, rounding=ROUND_HALF_UP)
