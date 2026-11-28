"""Audit worker: consumes ``audit.event`` and stores every event in Elasticsearch.

Run: ``elbank-audit-worker`` (or ``python -m elbank.workers.audit``).

- On start the composable index template ``elbank-audit`` is installed (retried while
  Elasticsearch is unavailable): strict mapping, ``payload`` as ``flattened``.
- Each event becomes a document in ``elbank-audit-YYYY.MM.DD`` (UTC date of the event)
  with ``_id = event_id``: a replayed message overwrites the same document, so
  at-least-once delivery creates no duplicates.
- The payload is redacted once more (:func:`elbank.core.pii.redact`).
- Elasticsearch unavailable: the worker stops without committing the offset; the
  container restarts and the events are written later (nothing is lost while Kafka
  keeps them). A document Elasticsearch rejects (mapping error, 400) is skipped and logged.
"""

import asyncio
import contextlib
import signal

import structlog
from elastic_transport import ConnectionError as EsConnectionError
from elastic_transport import ConnectionTimeout
from elasticsearch import ApiError, AsyncElasticsearch
from pydantic import ValidationError

from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.kafka import EventEnvelope, EventType
from elbank.services.audit_search import create_elastic_client
from elbank.services.audit_service import AUDIT_INDEX_TEMPLATE, AUDIT_TEMPLATE_NAME, AuditEvent
from elbank.workers.consumer import EventConsumerWorker, MalformedEventError, create_consumer

logger = get_logger(__name__)

_TEMPLATE_RETRY_SECONDS = 5.0
_HTTP_BAD_REQUEST = 400


class AuditEventHandler:
    def __init__(self, client: AsyncElasticsearch) -> None:
        self._client = client

    async def __call__(self, envelope: EventEnvelope) -> None:
        try:
            event = AuditEvent.model_validate(envelope.payload)
        except ValidationError as exc:
            raise MalformedEventError(str(exc)) from exc

        index = event.index_name()
        with structlog.contextvars.bound_contextvars(audit_event_id=str(event.event_id)):
            try:
                await self._client.index(
                    index=index, id=str(event.event_id), document=event.document()
                )
            except ApiError as exc:
                if exc.meta.status == _HTTP_BAD_REQUEST:
                    raise MalformedEventError(f"rejected by Elasticsearch: {exc}") from exc
                raise
            logger.debug("audit event stored", index=index, action=event.action)


async def install_template(client: AsyncElasticsearch, stopping: asyncio.Event) -> bool:
    """Install the index template, retrying while Elasticsearch is down; False if stopped."""
    while not stopping.is_set():
        try:
            await client.indices.put_index_template(
                name=AUDIT_TEMPLATE_NAME, **AUDIT_INDEX_TEMPLATE
            )
        except (EsConnectionError, ConnectionTimeout) as exc:
            logger.warning("elasticsearch unavailable, retrying", error=str(exc))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stopping.wait(), _TEMPLATE_RETRY_SECONDS)
        else:
            logger.info("audit index template installed", template=AUDIT_TEMPLATE_NAME)
            return True
    return False


async def run(settings: Settings) -> None:
    client = create_elastic_client(settings.elastic)
    stopping = asyncio.Event()
    worker = EventConsumerWorker(
        "audit",
        create_consumer(
            settings, EventType.AUDIT_EVENT, settings.elastic.audit_consumer_group, "audit"
        ),
        AuditEventHandler(client),
    )

    def stop() -> None:  # also interrupts the template installation retries
        stopping.set()
        worker.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)
    try:
        if await install_template(client, stopping):
            await worker.run()
    finally:
        await client.close()


def main() -> None:
    settings = get_settings()
    setup_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
