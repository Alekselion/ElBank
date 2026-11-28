"""Audit trail: security- and money-relevant actions as structured log records.

An audit record is an ordinary JSON log line with ``log_type="audit"``; Logstash
routes such lines to the ``elbank-audit-YYYY.MM.dd`` index (mapping:
``logstash/templates/elbank-audit.json``), and ``GET /api/v1/admin/audit`` reads
them back from Elasticsearch.

Fields: ``action`` (``admin.user.block``), ``actor_id`` (who did it; ``null`` for
the system), ``user_id`` (whose data/account it concerns), ``entity``/``entity_id``
and free-form ``details`` (mapped as ``flattened``: values are searchable as strings).
"""

import uuid
from collections.abc import Mapping
from typing import Any

from elbank.core.logging import get_logger

AUDIT_LOG_TYPE = "audit"

_logger = get_logger("elbank.audit")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_jsonable(v) for v in value]
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)  # UUID, Decimal, datetime, enums


def audit(
    action: str,
    *,
    actor_id: uuid.UUID | None,
    user_id: uuid.UUID | None,
    entity: str,
    entity_id: str | uuid.UUID | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    """Record an audit event (never raises: shipping is asynchronous, see core.logging)."""
    _logger.info(
        f"audit: {action}",
        log_type=AUDIT_LOG_TYPE,
        action=action,
        actor_id=str(actor_id) if actor_id else None,
        user_id=str(user_id) if user_id else None,
        entity=entity,
        entity_id=str(entity_id) if entity_id is not None else None,
        details=_jsonable(details or {}),
    )
