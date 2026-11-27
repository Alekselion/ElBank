"""Card processor client (WireMock).

``POST /card-processor/issue`` (``Idempotency-Key: <card id>``)::

    {"card_id": "...", "card_type": "virtual", "account_number": "...", "currency": "RUB",
     "holder_name": "...", "holder_phone": "+7...", "delivery_address": {...} | null,
     "replaces_pan_hash": "..." | null}
    -> 200 {"status": "issued", "pan": "2200701234567890", "expiry_month": 11,
            "expiry_year": 2030}

``POST /card-processor/{pan_hash}/block`` and ``/unblock``::

    {"card_id": "...", "reason": "lost"}  /  {"card_id": "..."}
    -> 200 {"status": "blocked"}  /  {"status": "active"}

Refusal (4xx): ``{"status": "error", "code": "...", "message": "..."}`` →
:class:`CardProcessorRefusedError`. 5xx, timeouts and connection errors are retried
(``CARD_PROCESSOR_RETRIES`` extra attempts): every call is idempotent (issue is
deduplicated by ``Idempotency-Key``, block/unblock set a state). The final failure →
:class:`CardProcessorUnavailableError`.

The response carries the full PAN: the client returns it to the caller only, it is
never logged.
"""

import uuid
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from elbank.clients.external_transfer import IDEMPOTENCY_HEADER
from elbank.clients.http import is_server_or_transport_error, retrying
from elbank.core.config import CardProcessorSettings
from elbank.core.logging import get_logger
from elbank.db.models import CardType

logger = get_logger(__name__)

_SERVICE = "card-processor"
_ISSUE_PATH = "/card-processor/issue"


class CardProcessorRefusedError(Exception):
    """The processor refused the request (4xx); ``code`` is its machine-readable reason."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class CardProcessorUnavailableError(Exception):
    """The processor is unavailable or answered with something we cannot interpret."""


class IssuedCard(BaseModel):
    status: Literal["issued"]
    pan: str = Field(pattern=r"^\d{16}$", repr=False)
    expiry_month: int = Field(ge=1, le=12)
    expiry_year: int = Field(ge=2000, le=2100)


class _StatusResponse(BaseModel):
    status: Literal["blocked", "active"]


class _ErrorResponse(BaseModel):
    status: Literal["error"]
    code: str
    message: str = ""


class CardProcessorClient:
    def __init__(self, http: httpx.AsyncClient, settings: CardProcessorSettings) -> None:
        self._http = http
        self._settings = settings

    async def issue(
        self,
        *,
        card_id: uuid.UUID,
        card_type: CardType,
        account_number: str,
        currency: str,
        holder_name: str | None,
        holder_phone: str,
        delivery_address: dict[str, Any] | None = None,
        replaces_pan_hash: str | None = None,
    ) -> IssuedCard:
        body = {
            "card_id": str(card_id),
            "card_type": card_type.value,
            "account_number": account_number,
            "currency": currency,
            "holder_name": holder_name,
            "holder_phone": holder_phone,
            "delivery_address": delivery_address,
            "replaces_pan_hash": replaces_pan_hash,
        }
        response = await self._post(
            _ISSUE_PATH, body, action="issue", headers={IDEMPOTENCY_HEADER: str(card_id)}
        )
        issued = self._parse(response, IssuedCard, action="issue")
        logger.info("card processor issued a card", card_id=str(card_id), type=card_type.value)
        return issued

    async def block(self, pan_hash: str, *, card_id: uuid.UUID, reason: str) -> None:
        response = await self._post(
            f"/card-processor/{pan_hash}/block",
            {"card_id": str(card_id), "reason": reason},
            action="block",
        )
        self._expect_status(response, "blocked", action="block")

    async def unblock(self, pan_hash: str, *, card_id: uuid.UUID) -> None:
        response = await self._post(
            f"/card-processor/{pan_hash}/unblock", {"card_id": str(card_id)}, action="unblock"
        )
        self._expect_status(response, "active", action="unblock")

    # ---------- internals ----------

    async def _post(
        self,
        path: str,
        body: dict[str, Any],
        *,
        action: str,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            async for attempt in retrying(
                self._settings.retries, _SERVICE, retry_if=is_server_or_transport_error
            ):
                with attempt:
                    response = await self._http.post(
                        path, json=body, headers=headers, timeout=self._settings.timeout_seconds
                    )
                    if response.is_server_error:
                        response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "card processor request failed",
                action=action,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise CardProcessorUnavailableError(str(exc)) from exc

        if response.is_client_error:
            raise self._rejection(response, action)
        return response

    @staticmethod
    def _rejection(response: httpx.Response, action: str) -> Exception:
        try:
            error = _ErrorResponse.model_validate_json(response.content)
        except ValidationError as exc:
            logger.error(  # noqa: TRY400 - not our bug, traceback is noise
                "card processor answered 4xx without an error body",
                action=action,
                http_status=response.status_code,
            )
            return CardProcessorUnavailableError(
                f"{_SERVICE} answered {response.status_code}: {exc}"
            )
        logger.warning(
            "card processor refused",
            action=action,
            http_status=response.status_code,
            code=error.code,
        )
        return CardProcessorRefusedError(error.code, error.message)

    @staticmethod
    def _parse[M: BaseModel](response: httpx.Response, model: type[M], *, action: str) -> M:
        try:
            return model.model_validate_json(response.content)
        except ValidationError as exc:
            # The error text may contain the PAN: log only where validation failed.
            logger.error(  # noqa: TRY400 - not our bug, traceback is noise
                "card processor answered with an unexpected body",
                action=action,
                fields=[".".join(map(str, e["loc"])) for e in exc.errors()],
            )
            raise CardProcessorUnavailableError(f"unexpected {action} response") from None

    def _expect_status(self, response: httpx.Response, expected: str, *, action: str) -> None:
        result = self._parse(response, _StatusResponse, action=action)
        if result.status != expected:
            raise CardProcessorUnavailableError(
                f"{action}: processor reports status {result.status}"
            )
        logger.info("card processor changed card status", action=action, status=result.status)
