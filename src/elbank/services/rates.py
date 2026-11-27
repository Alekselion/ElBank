"""Exchange rates from the Bank of Russia, cached in Redis (``rates:{base}:{quote}``).

The Bank of Russia quotes every currency in roubles, so a cross rate is
``rub_per_unit(base) / rub_per_unit(quote)``, rounded to 8 decimals. Redis is an
optimization: its errors are logged and the rate is fetched from the source.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from elbank.clients.cbr import CbrClient, CbrError
from elbank.core.config import CbrSettings
from elbank.core.exceptions import ExternalServiceError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Currency

logger = get_logger(__name__)

_RATE_QUANTUM = Decimal("0.00000001")  # matches operations.exchange_rate Numeric(18, 8)


@dataclass(frozen=True, slots=True)
class ExchangeRate:
    base: Currency
    quote: Currency
    rate: Decimal  # units of quote for one unit of base
    date: datetime  # date of the Bank of Russia rates


class _CachedRate(BaseModel):
    rate: Decimal
    date: datetime


def _key(base: Currency, quote: Currency) -> str:
    return f"rates:{base.value}:{quote.value}"


class RatesService:
    def __init__(self, redis: Redis, cbr: CbrClient, settings: CbrSettings) -> None:
        self._redis = redis
        self._cbr = cbr
        self._settings = settings

    async def get_rate(self, base: Currency, quote: Currency) -> ExchangeRate:
        if base is quote:
            raise UnprocessableError("base and quote currencies must differ")
        if (cached := await self._cached(base, quote)) is not None:
            return cached

        try:
            daily = await self._cbr.daily_rates()
        except CbrError as exc:
            raise ExternalServiceError("Exchange rates are unavailable, try again later") from exc
        base_rub, quote_rub = daily.rub_per_unit(base.value), daily.rub_per_unit(quote.value)
        if base_rub is None or quote_rub is None:
            raise ExternalServiceError(f"No Bank of Russia rate for {base.value}/{quote.value}")

        rate = ExchangeRate(
            base=base,
            quote=quote,
            rate=(base_rub / quote_rub).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP),
            date=daily.date,
        )
        await self._store(rate)
        return rate

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
        return ExchangeRate(base=base, quote=quote, rate=cached.rate, date=cached.date)

    async def _store(self, rate: ExchangeRate) -> None:
        value = _CachedRate(rate=rate.rate, date=rate.date).model_dump_json()
        try:
            await self._redis.set(
                _key(rate.base, rate.quote), value, ex=self._settings.rates_cache_ttl_seconds
            )
        except RedisError as exc:
            logger.warning("rates cache write failed", error=str(exc))
