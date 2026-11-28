"""Notifications worker: consumes ``notification.send``, stores and delivers notifications.

Run: ``elbank-notifications-worker`` (or ``python -m elbank.workers.notifications``).

See :mod:`elbank.services.notifications` for the flow. Handling is idempotent
(``notifications.dedup_key``), so offsets are committed after an event is handled
and a replay after a crash neither duplicates a notification nor (thanks to the
``Idempotency-Key`` header) its delivery. Gateway failures are retried
(``NOTIFICATIONS_RETRIES``) and then stored as ``failed``; DB errors stop the
worker (restart and retry). Messages of one user are ordered (key = user id).
"""

import asyncio

from elbank.clients.http import create_http_client
from elbank.clients.notifications import NotificationGatewayClient
from elbank.core.config import Settings, get_settings
from elbank.core.logging import setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventEnvelope, EventType
from elbank.services.notifications import MalformedNotificationError, NotificationDispatcher
from elbank.workers.consumer import EventConsumerWorker, MalformedEventError, create_consumer


class NotificationHandler:
    def __init__(self, dispatcher: NotificationDispatcher) -> None:
        self._dispatcher = dispatcher

    async def __call__(self, envelope: EventEnvelope) -> None:
        try:
            await self._dispatcher.handle(envelope)
        except MalformedNotificationError as exc:
            raise MalformedEventError(str(exc)) from exc


async def run(settings: Settings) -> None:
    engine = create_engine(settings.postgres)
    http = create_http_client(settings.wiremock.base_url, settings.notifications.timeout_seconds)
    dispatcher = NotificationDispatcher(
        create_session_factory(engine), NotificationGatewayClient(http, settings.notifications)
    )
    worker = EventConsumerWorker(
        "notifications",
        create_consumer(
            settings,
            EventType.NOTIFICATION_SEND,
            settings.notifications.consumer_group,
            "notifications",
        ),
        NotificationHandler(dispatcher),
    )
    worker.install_signal_handlers()
    try:
        await worker.run()
    finally:
        await http.aclose()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
