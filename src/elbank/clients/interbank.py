"""Interbank transfer gateway client (``POST /interbank/transfer``, WireMock).

Request::

    {"transfer_id": "...", "amount": "100.00", "currency": "RUB", "purpose": "...",
     "payer": {"bic": "...", "account": "...", "name": "..."},
     "payee": {"bic": "...", "account": "...", "name": "..."}}

Response and error semantics: see :mod:`elbank.clients.external_transfer`.
"""

import uuid
from dataclasses import dataclass
from decimal import Decimal

import httpx

from elbank.clients.external_transfer import GatewayResult, money, post_transfer
from elbank.core.config import InterbankSettings

_PATH = "/interbank/transfer"


@dataclass(frozen=True, slots=True)
class BankParty:
    bic: str
    account: str
    name: str | None


class InterbankClient:
    def __init__(self, http: httpx.AsyncClient, settings: InterbankSettings) -> None:
        self._http = http
        self._settings = settings

    async def send(
        self,
        *,
        transfer_id: uuid.UUID,
        payer: BankParty,
        payee: BankParty,
        amount: Decimal,
        currency: str,
        purpose: str | None,
    ) -> GatewayResult:
        body = {
            "transfer_id": str(transfer_id),
            "amount": money(amount),
            "currency": currency,
            "purpose": purpose,
            "payer": {"bic": payer.bic, "account": payer.account, "name": payer.name},
            "payee": {"bic": payee.bic, "account": payee.account, "name": payee.name},
        }
        return await post_transfer(
            self._http,
            service="interbank",
            path=_PATH,
            body=body,
            idempotency_key=str(transfer_id),
            timeout_seconds=self._settings.timeout_seconds,
            retries=self._settings.retries,
        )
