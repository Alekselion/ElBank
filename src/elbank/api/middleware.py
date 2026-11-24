"""ASGI middleware."""

import re
import uuid

import structlog
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from elbank.core.context import CORRELATION_ID_HEADER, correlation_id_var

_FALLBACK_HEADERS = ("X-Request-ID",)
# Accept only safe, bounded client-supplied ids (no log injection, no huge values).
_VALID_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


class CorrelationIdMiddleware:
    """Propagate a correlation id through the whole request.

    The id is taken from ``X-Correlation-Id`` (or ``X-Request-ID``) when the client
    sends a valid one, otherwise generated. It is stored in ``correlation_id_var``
    (read by the Kafka producer and outgoing HTTP clients), bound to every log
    record and echoed in the response header.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        correlation_id = self._extract(Headers(scope=scope)) or uuid.uuid4().hex

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[CORRELATION_ID_HEADER] = correlation_id
            await send(message)

        token = correlation_id_var.set(correlation_id)
        try:
            with structlog.contextvars.bound_contextvars(correlation_id=correlation_id):
                await self.app(scope, receive, send_with_header)
        finally:
            correlation_id_var.reset(token)

    @staticmethod
    def _extract(headers: Headers) -> str | None:
        for name in (CORRELATION_ID_HEADER, *_FALLBACK_HEADERS):
            value = headers.get(name)
            if value and _VALID_ID.fullmatch(value):
                return value
        return None
