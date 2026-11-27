"""Credit bureau (BKI) scoring client: ``POST /bki/score`` (WireMock).

Request (``Idempotency-Key: <application id>``)::

    {"application_id": "...", "full_name": "...", "phone": "+7...",
     "amount": "300000.00", "term_months": 24, "purpose": "..."}

Response: ``{"score": 742, "decision": "approve" | "reject", "comment": "..."}``.

5xx, timeouts and connection errors are retried (``BKI_RETRIES`` extra attempts);
the request is idempotent by its key. Any final failure, a 4xx included, raises
:class:`BkiUnavailableError`: an application cannot be decided without a score.
"""

import uuid
from decimal import Decimal
from enum import StrEnum

import httpx
from pydantic import BaseModel, Field, ValidationError

from elbank.clients.external_transfer import IDEMPOTENCY_HEADER, money
from elbank.clients.http import is_server_or_transport_error, retrying
from elbank.core.config import BkiSettings
from elbank.core.logging import get_logger

logger = get_logger(__name__)

_SERVICE = "bki"
_PATH = "/bki/score"


class BkiDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class ScoringResult(BaseModel):
    score: int = Field(ge=0, le=1000)
    decision: BkiDecision
    comment: str | None = Field(default=None, max_length=500)


class BkiUnavailableError(Exception):
    """The bureau is unavailable or answered with something we cannot interpret."""


class BkiClient:
    def __init__(self, http: httpx.AsyncClient, settings: BkiSettings) -> None:
        self._http = http
        self._settings = settings

    async def score(
        self,
        *,
        application_id: uuid.UUID,
        full_name: str | None,
        phone: str,
        amount: Decimal,
        term_months: int,
        purpose: str,
    ) -> ScoringResult:
        body = {
            "application_id": str(application_id),
            "full_name": full_name,
            "phone": phone,
            "amount": money(amount),
            "term_months": term_months,
            "purpose": purpose,
        }
        try:
            async for attempt in retrying(
                self._settings.retries, _SERVICE, retry_if=is_server_or_transport_error
            ):
                with attempt:
                    response = await self._http.post(
                        _PATH,
                        json=body,
                        headers={IDEMPOTENCY_HEADER: str(application_id)},
                        timeout=self._settings.timeout_seconds,
                    )
                    response.raise_for_status()
            result = ScoringResult.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError) as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "bki scoring failed", error=f"{type(exc).__name__}: {exc}"
            )
            raise BkiUnavailableError(str(exc)) from exc

        logger.info(
            "bki scoring done",
            application_id=str(application_id),
            score=result.score,
            decision=result.decision.value,
        )
        return result
