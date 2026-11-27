"""Shared httpx client factory and retry policy for external services."""

from collections.abc import Callable

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


def is_server_or_transport_error(exc: BaseException) -> bool:
    """Any 5xx, timeout or connection error; never a 4xx.

    Only for endpoints that deduplicate by ``Idempotency-Key``: after a timeout the
    first attempt may have been processed.
    """
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.is_server_error


def retrying(
    retries: int,
    service: str,
    *,
    retry_if: Callable[[BaseException], bool] = is_retryable,
) -> AsyncRetrying:
    """Tenacity policy shared by external clients: ``retries`` extra attempts with an
    exponential backoff (0.2 s, 0.4 s, ... up to 2 s) for errors matching ``retry_if``."""

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
        retry=retry_if_exception(retry_if),
        before_sleep=log_retry,
        reraise=True,
    )
