"""Exchange rates: admin overrides, else the Bank of Russia; cached in Redis.

Cache keys: ``rates:{base}:{quote}``.

Resolution of ``base/quote``:

1. an override of ``base/quote`` (table ``rate_overrides``, set in the back office);
2. an override of ``quote/base`` inverted (``1 / rate``);
3. the Bank of Russia cross rate ``rub_per_unit(base) / rub_per_unit(quote)``.

Rates are rounded to 8 decimals. Setting or removing an override deletes the cached
``base/quote`` and ``quote/base`` keys, so the next request (a conversion included)
uses the new rate. Redis is an optimization: its errors are logged and the rate is
resolved from the source; a failed invalidation is bounded by ``CBR_RATES_CACHE_TTL_SECONDS``.
"""

import itertools
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum

from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients.cbr import CbrClient, CbrDailyRates, CbrError
from elbank.core.config import CbrSettings
from elbank.core.exceptions import ExternalServiceError, NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Currency, RateOverride, User
from elbank.services.audit_service import AuditService

logger = get_logger(__name__)

_RATE_QUANTUM = Decimal("0.00000001")  # matches operations.exchange_rate Numeric(18, 8)


class RateSource(StrEnum):
    CBR = "cbr"
    OVERRIDE = "override"


@dataclass(frozen=True, slots=True)
class ExchangeRate:
    base: Currency
    quote: Currency
    rate: Decimal  # units of quote for one unit of base
    date: datetime  # date of the Bank of Russia rates, or when the override was set
    source: RateSource


class _CachedRate(BaseModel):
    rate: Decimal
    date: datetime
    source: RateSource = RateSource.CBR


def _key(base: Currency, quote: Currency) -> str:
    return f"rates:{base.value}:{quote.value}"


def _quantize(rate: Decimal) -> Decimal:
    return rate.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)


class RatesService:
    def __init__(
        self,
        session: AsyncSession,
        redis: Redis,
        cbr: CbrClient,
        settings: CbrSettings,
        audit: AuditService,
    ) -> None:
        self._session = session
        self._audit = audit
        self._redis = redis
        self._cbr = cbr
        self._settings = settings
        self._daily: CbrDailyRates | None = None  # fetched at most once per service instance

    # ---------- read ----------

    async def get_rate(self, base: Currency, quote: Currency) -> ExchangeRate:
        if base is quote:
            raise UnprocessableError("base and quote currencies must differ")
        if (cached := await self._cached(base, quote)) is not None:
            return cached
        rate = await self._override(base, quote) or await self._from_cbr(base, quote)
        await self._store(rate)
        return rate

    async def all_rates(self) -> list[ExchangeRate]:
        """Effective rates of every ordered pair of supported currencies."""
        return [
            await self.get_rate(base, quote) for base, quote in itertools.permutations(Currency, 2)
        ]

    async def list_overrides(self) -> list[RateOverride]:
        return list(
            await self._session.scalars(
                select(RateOverride).order_by(RateOverride.base, RateOverride.quote)
            )
        )

    # ---------- overrides (admin) ----------

    async def set_override(
        self, admin: User, base: Currency, quote: Currency, rate: Decimal
    ) -> RateOverride:
        if base is quote:
            raise UnprocessableError("base and quote currencies must differ")
        rate = _quantize(rate)
        if rate <= 0:
            raise UnprocessableError("rate is too small")
        override = await self._session.get(
            RateOverride, (base.value, quote.value), with_for_update=True
        )
        previous = override.rate if override is not None else None
        if override is None:
            override = RateOverride(
                base=base.value, quote=quote.value, rate=rate, updated_by_id=admin.id
            )
            self._session.add(override)
        else:
            override.rate = rate
            override.updated_by_id = admin.id
        await self._session.commit()
        await self._invalidate(base, quote)
        await self._audit.record(
            "admin.rate.override",
            user_id=admin.id,
            entity="rate",
            entity_id=f"{base.value}-{quote.value}",
            payload={"previous": previous, "rate": rate},
        )
        logger.info("rate override set", base=base.value, quote=quote.value, rate=str(rate))
        return override

    async def delete_override(self, admin: User, base: Currency, quote: Currency) -> None:
        override = await self._session.get(RateOverride, (base.value, quote.value))
        if override is None:
            raise NotFoundError("Rate override not found")
        previous = override.rate
        await self._session.delete(override)
        await self._session.commit()
        await self._invalidate(base, quote)
        await self._audit.record(
            "admin.rate.override_removed",
            user_id=admin.id,
            entity="rate",
            entity_id=f"{base.value}-{quote.value}",
            payload={"previous": previous},
        )
        logger.info("rate override removed", base=base.value, quote=quote.value)

    # ---------- internals ----------

    async def _override(self, base: Currency, quote: Currency) -> ExchangeRate | None:
        rows = {
            (o.base, o.quote): o
            for o in await self._session.scalars(
                select(RateOverride).where(
                    or_(
                        and_(RateOverride.base == base.value, RateOverride.quote == quote.value),
                        and_(RateOverride.base == quote.value, RateOverride.quote == base.value),
                    )
                )
            )
        }
        if (direct := rows.get((base.value, quote.value))) is not None:
            return ExchangeRate(base, quote, direct.rate, direct.updated_at, RateSource.OVERRIDE)
        if (inverse := rows.get((quote.value, base.value))) is not None:
            return ExchangeRate(
                base, quote, _quantize(1 / inverse.rate), inverse.updated_at, RateSource.OVERRIDE
            )
        return None

    async def _from_cbr(self, base: Currency, quote: Currency) -> ExchangeRate:
        if self._daily is None:
            try:
                self._daily = await self._cbr.daily_rates()
            except CbrError as exc:
                raise ExternalServiceError(
                    "Exchange rates are unavailable, try again later"
                ) from exc
        base_rub = self._daily.rub_per_unit(base.value)
        quote_rub = self._daily.rub_per_unit(quote.value)
        if base_rub is None or quote_rub is None:
            raise ExternalServiceError(f"No Bank of Russia rate for {base.value}/{quote.value}")
        return ExchangeRate(
            base, quote, _quantize(base_rub / quote_rub), self._daily.date, RateSource.CBR
        )

    async def _cached(self, base: Currency, quote: Currency) -> ExchangeRate | None:
        try:
            raw = await self._redis.get(_key(base, quote))
        except RedisError as exc:
            logger.warning("rates cache read failed", error=str(exc))
            return None
        if raw is None:
            return None
        try:
            cached = _CachedRate.model_validate_json(raw)
        except ValidationError:
            return None
        return ExchangeRate(base, quote, cached.rate, cached.date, cached.source)

    async def _store(self, rate: ExchangeRate) -> None:
        value = _CachedRate(rate=rate.rate, date=rate.date, source=rate.source).model_dump_json()
        try:
            await self._redis.set(
                _key(rate.base, rate.quote), value, ex=self._settings.rates_cache_ttl_seconds
            )
        except RedisError as exc:
            logger.warning("rates cache write failed", error=str(exc))

    async def _invalidate(self, base: Currency, quote: Currency) -> None:
        try:
            await self._redis.delete(_key(base, quote), _key(quote, base))
        except RedisError as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "rates cache invalidation failed: old rate is served until the TTL expires",
                base=base.value,
                quote=quote.value,
                ttl_seconds=self._settings.rates_cache_ttl_seconds,
                error=str(exc),
            )


def parse_pair(pair: str) -> tuple[Currency, Currency]:
    """``USD-RUB`` → (USD, RUB)."""
    base, sep, quote = pair.upper().partition("-")
    supported = {c.value: c for c in Currency}
    if not sep or base not in supported or quote not in supported:
        raise UnprocessableError(
            f"Bad currency pair {pair!r}: expected BASE-QUOTE of {', '.join(supported)}"
        )
    return supported[base], supported[quote]
