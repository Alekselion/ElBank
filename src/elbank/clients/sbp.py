"""Faster Payments System (SBP) client (``POST /sbp/send``, WireMock).

Request::

    {"transfer_id": "...", "payer_account": "...", "phone": "+79991234567",
     "amount": "100.00", "currency": "RUB", "purpose": "..."}

Response and error semantics: see :mod:`elbank.clients.external_transfer`.
"""

import uuid
from decimal import Decimal

import httpx

from elbank.clients.external_transfer import GatewayResult, money, post_transfer
from elbank.core.config import SbpSettings

_PATH = "/sbp/send"


class SbpClient:
    def __init__(self, http: httpx.AsyncClient, settings: SbpSettings) -> None:
        self._http = http
        self._settings = settings

    async def send(
        self,
        *,
        transfer_id: uuid.UUID,
        payer_account: str,
        phone: str,
        amount: Decimal,
        purpose: str | None,
    ) -> GatewayResult:
        body = {
            "transfer_id": str(transfer_id),
            "payer_account": payer_account,
            "phone": phone,
            "amount": money(amount),
            "currency": "RUB",
            "purpose": purpose,
        }
        return await post_transfer(
            self._http,
            service="sbp",
            path=_PATH,
            body=body,
            idempotency_key=str(transfer_id),
            timeout_seconds=self._settings.timeout_seconds,
            retries=self._settings.retries,
        )
