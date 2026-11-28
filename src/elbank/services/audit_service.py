"""End-to-end audit trail.

Producers (API process, workers, CLI) publish ``audit.event`` to Kafka:

- :class:`~elbank.api.middleware.AuditMiddleware` — every state-changing HTTP request
  (``POST/PUT/PATCH/DELETE`` under ``/api``): ``action="http.<endpoint name>"``;
- :class:`AuditService` — domain actions with their details (``auth.login``,
  ``admin.user.block``, ``fraud.operation_held``, ...).

The audit worker (:mod:`elbank.workers.audit`) writes every event as a document to
Elasticsearch, index ``elbank-audit-YYYY.MM.DD`` (UTC date of the event), document id
= ``event_id`` (a replayed Kafka message overwrites the same document).
``GET /api/v1/admin/audit`` reads them back.

Event fields: ``event_id, user_id`` (who acted; ``null`` for the system),
``action, entity, entity_id, payload, correlation_id, ip, user_agent, timestamp``.
The payload never carries PII: it goes through :func:`elbank.core.pii.redact`
(here and again in the worker). Delivery is best effort like any post-commit event:
a Kafka outage is logged and must not fail the business action.
"""

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from elbank.core.context import client_ip_var, get_correlation_id, user_agent_var
from elbank.core.logging import get_logger
from elbank.core.pii import redact
from elbank.kafka import EventPublisher, EventType

logger = get_logger(__name__)

AUDIT_INDEX_PREFIX = "elbank-audit-"
AUDIT_INDEX_PATTERN = f"{AUDIT_INDEX_PREFIX}*"
AUDIT_TEMPLATE_NAME = "elbank-audit"
MAX_USER_AGENT_LENGTH = 256

# Composable index template, installed by the audit worker on start.
AUDIT_INDEX_TEMPLATE: dict[str, Any] = {
    "index_patterns": [AUDIT_INDEX_PATTERN],
    "priority": 200,
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "event_id": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "user_id": {"type": "keyword"},
                "action": {"type": "keyword"},
                "entity": {"type": "keyword"},
                "entity_id": {"type": "keyword"},
                # Free-form, differently typed per action: values are indexed as keywords.
                "payload": {"type": "flattened"},
                "correlation_id": {"type": "keyword"},
                "ip": {"type": "ip"},
                "user_agent": {"type": "keyword", "ignore_above": MAX_USER_AGENT_LENGTH},
            },
        },
    },
}


def _request_ip() -> str | None:
    return client_ip_var.get()


def _request_user_agent() -> str | None:
    return user_agent_var.get()


class AuditEvent(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    user_id: uuid.UUID | None
    action: str
    entity: str
    entity_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = Field(default_factory=get_correlation_id)
    ip: str | None = Field(default_factory=_request_ip)
    user_agent: str | None = Field(default_factory=_request_user_agent)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def index_name(self) -> str:
        return f"{AUDIT_INDEX_PREFIX}{self.timestamp.astimezone(UTC):%Y.%m.%d}"

    def document(self) -> dict[str, Any]:
        """The Elasticsearch document (payload redacted once more)."""
        doc = self.model_dump(mode="json")
        doc["payload"] = redact(doc["payload"])
        return doc


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_jsonable(v) for v in value]
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)  # UUID, Decimal, datetime, enums


class AuditService:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    async def record(
        self,
        action: str,
        *,
        user_id: uuid.UUID | None,
        entity: str,
        entity_id: str | uuid.UUID | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> AuditEvent:
        """Publish an audit event; never raises (a lost event is logged)."""
        event = AuditEvent(
            user_id=user_id,
            action=action,
            entity=entity,
            entity_id=str(entity_id) if entity_id is not None else None,
            payload=redact(_jsonable(payload or {})),
        )
        await self.publish(event)
        return event

    async def publish(self, event: AuditEvent) -> None:
        delivered = await self._publisher.publish_safely(
            EventType.AUDIT_EVENT, event.model_dump(mode="json"), key=str(event.event_id)
        )
        if not delivered:
            logger.error(
                "audit event lost", action=event.action, audit_event_id=str(event.event_id)
            )
