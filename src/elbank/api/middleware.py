"""ASGI middleware."""

import re
import uuid

import structlog
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

CORRELATION_ID_HEADER = "X-Correlation-ID"
_FALLBACK_HEADERS = ("X-Request-ID",)
# Accept only safe, bounded client-supplied ids (no log injection, no huge values).
_VALID_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


class CorrelationIdMiddleware:
    """Bind a correlation id to every log record emitted while handling a request.

    The id is taken from ``X-Correlation-ID`` (or ``X-Request-ID``) when the client
    sends a valid one, otherwise generated. It is echoed in the response header.
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

        with structlog.contextvars.bound_contextvars(correlation_id=correlation_id):
            await self.app(scope, receive, send_with_header)

    @staticmethod
    def _extract(headers: Headers) -> str | None:
        for name in (CORRELATION_ID_HEADER, *_FALLBACK_HEADERS):
            value = headers.get(name)
            if value and _VALID_ID.fullmatch(value):
                return value
        return None
