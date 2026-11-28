"""SMS gateway client (``POST /sms/send``, mocked by WireMock)."""

import httpx
from pydantic import BaseModel, ValidationError

from elbank.core.logging import get_logger
from elbank.core.pii import mask_phone

logger = get_logger(__name__)


class SmsSendResult(BaseModel):
    message_id: str
    status: str


class SmsGatewayError(Exception):
    """The SMS gateway is unreachable or rejected the message."""


class SmsClient:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def send(self, phone: str, text: str) -> SmsSendResult:
        try:
            response = await self._http.post("/sms/send", json={"phone": phone, "text": text})
            response.raise_for_status()
            result = SmsSendResult.model_validate(response.json())
        except (httpx.HTTPError, ValueError, ValidationError) as exc:
            logger.error(  # noqa: TRY400 - expected outage, traceback is noise
                "sms send failed", phone=mask_phone(phone), error=f"{type(exc).__name__}: {exc}"
            )
            raise SmsGatewayError(str(exc)) from exc
        logger.info("sms sent", phone=mask_phone(phone), message_id=result.message_id)
        return result
