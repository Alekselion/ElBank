"""Service providers client: ``POST /providers/{name}/pay`` (WireMock).

Request::

    {"payment_id": "...", "payer_account": "...", "amount": "100.00",
     "currency": "RUB", "payload": {...provider-specific fields...}}

Response (2xx — accepted, 4xx — refused by the provider)::

    {"status": "accepted", "transaction_id": "...", "receipt": {...}}
    {"status": "error", "code": "ACCOUNT_NOT_FOUND", "message": "..."}

Retry policy: 5xx, timeouts and connection errors are retried
(``PROVIDERS_RETRIES`` extra attempts, exponential backoff), 4xx never. Every
request carries ``Idempotency-Key: <payment id>``, so a retry after a timeout
cannot pay twice. Final timeout → :class:`ExternalTimeoutError`; other
failures → :class:`ExternalGatewayError`.
"""

import uuid
from decimal import Decimal
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from elbank.clients.external_transfer import (
    IDEMPOTENCY_HEADER,
    ExternalGatewayError,
    ExternalTimeoutError,
    money,
)
from elbank.clients.http import is_server_or_transport_error, retrying
from elbank.core.config import ProviderSettings
from elbank.core.logging import get_logger
from elbank.db.models import PaymentProvider

logger = get_logger(__name__)

PROVIDER_PATHS: dict[PaymentProvider, str] = {
    PaymentProvider.UTILITY: "/providers/utility/pay",
    PaymentProvider.MOBILE: "/providers/mobile/pay",
    PaymentProvider.INTERNET: "/providers/internet/pay",
    PaymentProvider.TAX: "/providers/fns/pay",
    PaymentProvider.TRAFFIC_FINE: "/providers/gibdd/pay",
    PaymentProvider.QR: "/providers/qr/pay",
    PaymentProvider.RECEIPT: "/providers/receipt/pay",
}


class ProviderStatus(StrEnum):
    ACCEPTED = "accepted"
    ERROR = "error"


class ProviderResponse(BaseModel):
    model_config = ConfigDict(extra="allow")  # receipts differ per provider; kept as is

    status: ProviderStatus
    transaction_id: str | None = None
    code: str | None = None
    message: str | None = None

    def raw(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ProviderClient:
    def __init__(self, http: httpx.AsyncClient, settings: ProviderSettings) -> None:
        self._http = http
        self._settings = settings

    async def pay(
        self,
        provider: PaymentProvider,
        *,
        payment_id: uuid.UUID,
        payer_account: str,
        amount: Decimal,
        payload: dict[str, Any],
    ) -> ProviderResponse:
        service = f"provider:{provider.value}"
        body = {
            "payment_id": str(payment_id),
            "payer_account": payer_account,
            "amount": money(amount),
            "currency": "RUB",
            "payload": payload,
        }
        try:
            async for attempt in retrying(
                self._settings.retries, service, retry_if=is_server_or_transport_error
            ):
                with attempt:
                    response = await self._http.post(
                        PROVIDER_PATHS[provider],
                        json=body,
                        headers={IDEMPOTENCY_HEADER: str(payment_id)},
                        timeout=self._settings.timeout_seconds,
                    )
                    if response.is_server_error:
                        response.raise_for_status()
            result = ProviderResponse.model_validate_json(response.content)
        except httpx.TimeoutException as exc:
            logger.warning("provider timeout", service=service, error=type(exc).__name__)
            raise ExternalTimeoutError(f"{service} did not answer in time") from exc
        except (httpx.HTTPError, ValidationError) as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "provider request failed", service=service, error=f"{type(exc).__name__}: {exc}"
            )
            raise ExternalGatewayError(str(exc)) from exc

        if response.is_client_error and result.status is not ProviderStatus.ERROR:
            raise ExternalGatewayError(f"{service} answered {response.status_code} without error")
        logger.info(
            "provider answered",
            service=service,
            http_status=response.status_code,
            status=result.status.value,
            code=result.code,
        )
        return result
