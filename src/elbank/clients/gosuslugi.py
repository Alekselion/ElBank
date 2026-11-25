"""Gosuslugi passport verification client (``POST /gosuslugi/verify-passport``, WireMock).

Request: ``{series, number, birth_date}``; response: ``{valid, full_name, message}``.
Timeouts, connection errors and 5xx are retried (``GOSUSLUGI_RETRIES`` extra
attempts with a short exponential backoff); 4xx and malformed responses are not.
"""

from datetime import date

import httpx
from pydantic import BaseModel, ValidationError
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from elbank.core.config import GosuslugiSettings
from elbank.core.logging import get_logger

logger = get_logger(__name__)

_PATH = "/gosuslugi/verify-passport"


class PassportVerification(BaseModel):
    valid: bool
    full_name: str | None = None
    message: str | None = None


class GosuslugiError(Exception):
    """Gosuslugi is unavailable or answered with something we cannot interpret."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):  # timeouts, connection errors
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.is_server_error


def _log_retry(state: RetryCallState) -> None:
    exc = state.outcome.exception() if state.outcome else None
    logger.warning(
        "gosuslugi request failed, retrying",
        attempt=state.attempt_number,
        error=f"{type(exc).__name__}: {exc}" if exc else None,
    )


class GosuslugiClient:
    def __init__(self, http: httpx.AsyncClient, settings: GosuslugiSettings) -> None:
        self._http = http
        self._settings = settings

    async def verify_passport(
        self, series: str, number: str, birth_date: date
    ) -> PassportVerification:
        body = {"series": series, "number": number, "birth_date": birth_date.isoformat()}
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._settings.retries + 1),
            wait=wait_exponential(multiplier=0.2, max=2),
            retry=retry_if_exception(_is_retryable),
            before_sleep=_log_retry,
            reraise=True,
        )
        try:
            async for attempt in retrying:
                with attempt:
                    response = await self._http.post(
                        _PATH, json=body, timeout=self._settings.timeout_seconds
                    )
                    response.raise_for_status()
            result = PassportVerification.model_validate(response.json())
        except (httpx.HTTPError, ValueError, ValidationError) as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "gosuslugi passport check failed", error=f"{type(exc).__name__}: {exc}"
            )
            raise GosuslugiError(str(exc)) from exc

        logger.info("gosuslugi passport checked", valid=result.valid)
        return result
