"""Shared httpx client factory for external services."""

import httpx

from elbank.core.context import CORRELATION_ID_HEADER, get_correlation_id


async def _propagate_correlation_id(request: httpx.Request) -> None:
    if (correlation_id := get_correlation_id()) is not None:
        request.headers.setdefault(CORRELATION_ID_HEADER, correlation_id)


def create_http_client(base_url: str, timeout_seconds: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(timeout_seconds),
        event_hooks={"request": [_propagate_correlation_id]},
    )
