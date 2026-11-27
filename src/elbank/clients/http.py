"""Shared httpx client factory and retry policy for external services."""

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from elbank.core.context import CORRELATION_ID_HEADER, get_correlation_id
from elbank.core.logging import get_logger

logger = get_logger(__name__)


async def _propagate_correlation_id(request: httpx.Request) -> None:
    if (correlation_id := get_correlation_id()) is not None:
        request.headers.setdefault(CORRELATION_ID_HEADER, correlation_id)


def create_http_client(base_url: str, timeout_seconds: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(timeout_seconds),
        event_hooks={"request": [_propagate_correlation_id]},
    )


_RETRYABLE_STATUSES = frozenset({502, 503, 504})


def is_retryable(exc: BaseException) -> bool:
    """Connection failures and gateway errors: the request was not processed, retry it.

    A read timeout is *not* retried here: the server may have processed the request.
    """
    if isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout | httpx.PoolTimeout):
        return True
    return (
        isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in _RETRYABLE_STATUSES
    )


def retrying(retries: int, service: str) -> AsyncRetrying:
    """Tenacity policy shared by external clients: ``retries`` extra attempts, short backoff."""

    def log_retry(state: RetryCallState) -> None:
        exc = state.outcome.exception() if state.outcome else None
        logger.warning(
            "external request failed, retrying",
            service=service,
            attempt=state.attempt_number,
            error=f"{type(exc).__name__}: {exc}" if exc else None,
        )

    return AsyncRetrying(
        stop=stop_after_attempt(retries + 1),
        wait=wait_exponential(multiplier=0.2, max=2),
        retry=retry_if_exception(is_retryable),
        before_sleep=log_retry,
        reraise=True,
    )
