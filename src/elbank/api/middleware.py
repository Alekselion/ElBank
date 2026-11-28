"""ASGI middleware."""

import ipaddress
import re
import time
import uuid
from typing import Any

import structlog
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from elbank.core.context import (
    CORRELATION_ID_HEADER,
    client_ip_var,
    correlation_id_var,
    user_agent_var,
)
from elbank.core.logging import get_logger
from elbank.core.pii import redact
from elbank.kafka import EventPublisher
from elbank.services.audit_service import MAX_USER_AGENT_LENGTH, AuditEvent, AuditService

logger = get_logger(__name__)

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


_AUDITED_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_AUDITED_PREFIX = "/api/"


def _client_ip(scope: Scope) -> str | None:
    """Peer address (uvicorn --proxy-headers already applied X-Forwarded-For)."""
    client = scope.get("client")
    if not client:
        return None
    try:
        return str(ipaddress.ip_address(client[0]))
    except ValueError:
        return None


def _route_template(path: str, path_params: dict[str, Any]) -> str:
    """``/api/v1/admin/operations/<uuid>/confirm`` → ``.../operations/{operation_id}/confirm``.

    Built from the request path: the matched route object holds the path without the
    prefixes of the routers it was included through.
    """
    by_value = {str(value): name for name, value in path_params.items()}
    return "/".join(
        f"{{{by_value[segment]}}}" if segment in by_value else segment
        for segment in path.split("/")
    )


class AuditMiddleware:
    """Audit every state-changing API request and expose ip/user agent to audit events.

    For the whole request ``client_ip_var`` / ``user_agent_var`` are set, so domain
    audit events (:class:`AuditService`) carry them too. After a ``POST/PUT/PATCH/DELETE``
    under ``/api/`` that matched a route, ``audit.event`` is published with
    ``action="http.<endpoint function>"`` and the payload ``method, route, path_params,
    status_code, duration_ms``. Request bodies and query strings are never recorded
    (they may hold PII); ``user_id`` is the authenticated user, if any.
    Must be installed inside :class:`CorrelationIdMiddleware`.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        user_agent = Headers(scope=scope).get("user-agent")
        ip_token = client_ip_var.set(_client_ip(scope))
        ua_token = user_agent_var.set(user_agent[:MAX_USER_AGENT_LENGTH] if user_agent else None)
        status_code = 500  # an exception that escapes the app becomes a 500
        started = time.perf_counter()

        async def send_with_status(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            try:
                if scope["method"] in _AUDITED_METHODS and scope["path"].startswith(
                    _AUDITED_PREFIX
                ):
                    await self._record(scope, status_code, time.perf_counter() - started)
            finally:
                user_agent_var.reset(ua_token)
                client_ip_var.reset(ip_token)

    @staticmethod
    async def _record(scope: Scope, status_code: int, elapsed: float) -> None:
        endpoint = scope.get("endpoint")
        if endpoint is None:
            return  # no route matched (404/405): nothing was done
        resources = getattr(scope["app"].state, "resources", None)
        if resources is None:
            return
        state: dict[str, Any] = scope.get("state") or {}
        user_id = state.get("audit_user_id")
        path_params: dict[str, Any] = scope.get("path_params") or {}
        route = _route_template(scope["path"], path_params)
        event = AuditEvent(
            user_id=user_id if isinstance(user_id, uuid.UUID) else None,
            action=f"http.{getattr(endpoint, '__name__', 'unknown')}",
            entity="http_request",
            entity_id=f"{scope['method']} {route}",
            payload=redact(
                {
                    "method": scope["method"],
                    "route": route,
                    "path_params": {k: str(v) for k, v in path_params.items()},
                    "status_code": status_code,
                    "duration_ms": round(elapsed * 1000, 1),
                }
            ),
        )
        await AuditService(EventPublisher(resources.kafka)).publish(event)
