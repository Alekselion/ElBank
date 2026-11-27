"""Bank of Russia daily exchange rates (``GET /cbr/rates``, WireMock).

The payload follows the Bank of Russia daily rates (``XML_daily``) in its JSON
form: for every currency, ``Value`` roubles are paid for ``Nominal`` units::

    {"Date": "2026-11-27T11:30:00+03:00", "PreviousDate": "...",
     "Valute": {"USD": {"ID": "R01235", "NumCode": "840", "CharCode": "USD",
                        "Nominal": 1, "Name": "Доллар США",
                        "Value": 80.1234, "Previous": 79.9876}, ...}}
"""

from datetime import datetime
from decimal import Decimal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from elbank.clients.http import retrying
from elbank.core.config import CbrSettings
from elbank.core.logging import get_logger

logger = get_logger(__name__)

_PATH = "/cbr/rates"


class CbrValute(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    char_code: str = Field(alias="CharCode")
    nominal: int = Field(alias="Nominal", gt=0)
    value: Decimal = Field(alias="Value", gt=0)
    name: str = Field(alias="Name")


class CbrDailyRates(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: datetime = Field(alias="Date")
    valute: dict[str, CbrValute] = Field(alias="Valute")

    def rub_per_unit(self, code: str) -> Decimal | None:
        """Roubles for one unit of ``code`` (1 for RUB), ``None`` if not quoted."""
        if code == "RUB":
            return Decimal(1)
        valute = self.valute.get(code)
        return None if valute is None else valute.value / valute.nominal


class CbrError(Exception):
    """Rates are unavailable or the response cannot be interpreted."""


class CbrClient:
    def __init__(self, http: httpx.AsyncClient, settings: CbrSettings) -> None:
        self._http = http
        self._settings = settings

    async def daily_rates(self) -> CbrDailyRates:
        try:
            async for attempt in retrying(self._settings.retries, "cbr"):
                with attempt:
                    response = await self._http.get(_PATH, timeout=self._settings.timeout_seconds)
                    response.raise_for_status()
            # Parsed from the raw JSON text, so numbers become exact Decimals (no float).
            return CbrDailyRates.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError) as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "cbr rates request failed", error=f"{type(exc).__name__}: {exc}"
            )
            raise CbrError(str(exc)) from exc
