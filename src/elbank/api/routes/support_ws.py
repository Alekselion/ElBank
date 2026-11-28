"""WebSocket ``/ws/tickets/{ticket_id}?token=<access token>``: live ticket updates.

The server pushes every ``ticket.updated`` event of the ticket as a JSON object::

    {"event_id", "occurred_at", "ticket_id", "user_id", "status", "old_status",
     "assignee_id", "kind": "message" | "status" | "rated",
     "message": {...} (kind=message), "rating": 5 (kind=rated)}

Only live events are sent; load the history with ``GET /api/v1/support/tickets/{id}``.
Messages from the client are ignored (they may be used as keep-alive pings).

The connection is accepted first and then closed with an application code, so the
client can tell why: 4401 — missing/invalid/revoked/expired token (also when the
token expires during the session), 4403 — user blocked, 4404 — no such ticket or
not the user's, 1013 — the client fell behind (reconnect and reload the ticket).
No DB connection is held while the socket is open.
"""

import asyncio
import contextlib
import time
import uuid
from typing import cast

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from elbank.api.deps import authenticate_access_token
from elbank.core.config import Settings
from elbank.core.exceptions import AuthenticationError, NotFoundError, UserInactiveError
from elbank.core.logging import get_logger
from elbank.deps import Resources
from elbank.kafka import EventPublisher
from elbank.services.support import TicketService
from elbank.services.ticket_hub import TicketSubscription

logger = get_logger(__name__)

CLOSE_UNAUTHORIZED = 4401
CLOSE_FORBIDDEN = 4403
CLOSE_NOT_FOUND = 4404

router = APIRouter()


@router.websocket("/ws/tickets/{ticket_id}")
async def ticket_updates(
    websocket: WebSocket, ticket_id: uuid.UUID, token: str | None = None
) -> None:
    resources = cast(Resources, websocket.app.state.resources)
    settings = cast(Settings, websocket.app.state.settings)
    await websocket.accept()

    if not token:
        await websocket.close(CLOSE_UNAUTHORIZED, "Missing token")
        return
    try:
        # A short-lived session: only for the checks, not for the socket's lifetime.
        async with resources.session_factory() as session:
            user, payload = await authenticate_access_token(
                token, session, resources.redis, settings
            )
            tickets = TicketService(session, EventPublisher(resources.kafka), settings.support)
            await tickets.get_accessible(user, ticket_id)
    except AuthenticationError:
        await websocket.close(CLOSE_UNAUTHORIZED, "Invalid or revoked token")
        return
    except UserInactiveError:
        await websocket.close(CLOSE_FORBIDDEN, "User is blocked")
        return
    except NotFoundError:
        await websocket.close(CLOSE_NOT_FOUND, "Ticket not found")
        return

    subscription = resources.ticket_hub.subscribe(ticket_id)
    logger.info("ticket websocket opened", ticket_id=str(ticket_id), user_id=str(user.id))
    pump = asyncio.create_task(_pump(websocket, subscription))
    listen = asyncio.create_task(_listen(websocket))
    try:
        done, _ = await asyncio.wait(
            {pump, listen},
            timeout=max(payload.exp - time.time(), 0),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            await _close(websocket, CLOSE_UNAUTHORIZED, "Token expired")
        elif pump in done and subscription.overflowed:
            await _close(websocket, status.WS_1013_TRY_AGAIN_LATER, "Too many pending events")
    finally:
        for task in (pump, listen):
            task.cancel()
            # The socket may already be gone: send/receive errors only end the session.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        resources.ticket_hub.unsubscribe(subscription)
        logger.info("ticket websocket closed", ticket_id=str(ticket_id), user_id=str(user.id))


async def _pump(websocket: WebSocket, subscription: TicketSubscription) -> None:
    """Send queued events until the subscription overflows (or the socket fails)."""
    while (event := await subscription.get()) is not None:
        await websocket.send_json(event)


async def _listen(websocket: WebSocket) -> None:
    """Drain client frames; returns when the client disconnects."""
    with contextlib.suppress(WebSocketDisconnect):
        while True:
            await websocket.receive_text()


async def _close(websocket: WebSocket, code: int, reason: str) -> None:
    with contextlib.suppress(RuntimeError, WebSocketDisconnect):
        await websocket.close(code, reason)
