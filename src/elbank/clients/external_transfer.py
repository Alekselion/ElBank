"""Common protocol of outgoing money transfer gateways (interbank, SBP).

Every request carries ``Idempotency-Key: <our operation id>``, so a retry after a
connection failure or a 502/503/504 cannot send the money twice. Response::

    {"status": "accepted" | "rejected", "external_id": "...", "reason": "..."}

A timeout means the outcome is unknown (:class:`ExternalTimeoutError`); any other
failure means the transfer was not made (:class:`ExternalGatewayError`).
"""

from decimal import Decimal
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError

from elbank.clients.http import retrying
from elbank.core.logging import get_logger

logger = get_logger(__name__)

IDEMPOTENCY_HEADER = "Idempotency-Key"


class GatewayStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class GatewayResult(BaseModel):
    status: GatewayStatus
    external_id: str | None = None
    reason: str | None = None


class ExternalTimeoutError(Exception):
    """The gateway did not answer in time: the transfer may or may not have been made."""


class ExternalGatewayError(Exception):
    """The gateway is unavailable or answered with something we cannot interpret."""


def money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


async def post_transfer(
    http: httpx.AsyncClient,
    *,
    service: str,
    path: str,
    body: dict[str, Any],
    idempotency_key: str,
    timeout_seconds: float,
    retries: int,
) -> GatewayResult:
    try:
        async for attempt in retrying(retries, service):
            with attempt:
                response = await http.post(
                    path,
                    json=body,
                    headers={IDEMPOTENCY_HEADER: idempotency_key},
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
        result = GatewayResult.model_validate_json(response.content)
    except httpx.TimeoutException as exc:
        logger.warning("transfer gateway timeout", service=service, error=type(exc).__name__)
        raise ExternalTimeoutError(f"{service} did not answer in {timeout_seconds}s") from exc
    except (httpx.HTTPError, ValidationError) as exc:
        logger.error(  # noqa: TRY400 - expected outage, traceback is noise
            "transfer gateway failed", service=service, error=f"{type(exc).__name__}: {exc}"
        )
        raise ExternalGatewayError(str(exc)) from exc

    logger.info(
        "transfer gateway answered",
        service=service,
        status=result.status.value,
        external_id=result.external_id,
    )
    return result
