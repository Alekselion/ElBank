"""Reading the audit trail from Elasticsearch (``elbank-audit-*``, written by Logstash).

The database is not involved: audit records exist only as log events, see
:mod:`elbank.core.audit`.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from elastic_transport import ConnectionError as EsConnectionError
from elastic_transport import ConnectionTimeout, HttpxAsyncHttpNode
from elasticsearch import ApiError, AsyncElasticsearch

from elbank.core.config import ElasticSettings
from elbank.core.exceptions import ExternalServiceError
from elbank.core.logging import get_logger

logger = get_logger(__name__)

# from + size of a plain search; deeper pages need a narrower period.
MAX_RESULT_WINDOW = 10_000

_SOURCE_FIELDS = [
    "@timestamp",
    "action",
    "actor_id",
    "user_id",
    "entity",
    "entity_id",
    "details",
    "correlation_id",
    "service",
]


@dataclass(frozen=True, slots=True)
class AuditQuery:
    user_id: uuid.UUID | None = None  # actor or subject of the event
    action: str | None = None  # exact, or a prefix ending with "*": "admin.user.*"
    date_from: datetime | None = None
    date_to: datetime | None = None


@dataclass(frozen=True, slots=True)
class AuditRecord:
    id: str
    timestamp: datetime
    action: str
    actor_id: uuid.UUID | None
    user_id: uuid.UUID | None
    entity: str | None
    entity_id: str | None
    details: dict[str, Any]
    correlation_id: str | None
    service: str | None


def create_elastic_client(settings: ElasticSettings) -> AsyncElasticsearch:
    return AsyncElasticsearch(
        settings.url,
        node_class=HttpxAsyncHttpNode,  # httpx is already a dependency; no aiohttp needed
        request_timeout=settings.timeout_seconds,
        retry_on_timeout=False,
        max_retries=1,
    )


def _build_query(query: AuditQuery) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []
    if query.user_id is not None:
        value = str(query.user_id)
        filters.append(
            {
                "bool": {
                    "should": [{"term": {"user_id": value}}, {"term": {"actor_id": value}}],
                    "minimum_should_match": 1,
                }
            }
        )
    if query.action:
        if query.action.endswith("*"):
            filters.append({"prefix": {"action": query.action[:-1]}})
        else:
            filters.append({"term": {"action": query.action}})
    if query.date_from is not None or query.date_to is not None:
        bounds: dict[str, str] = {}
        if query.date_from is not None:
            bounds["gte"] = query.date_from.isoformat()
        if query.date_to is not None:
            bounds["lt"] = query.date_to.isoformat()
        filters.append({"range": {"@timestamp": bounds}})
    return {"bool": {"filter": filters}} if filters else {"match_all": {}}


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value)) if value else None
    except ValueError:
        return None


def _record(hit: dict[str, Any]) -> AuditRecord:
    source = hit["_source"]
    details = source.get("details")
    return AuditRecord(
        id=hit["_id"],
        timestamp=datetime.fromisoformat(source["@timestamp"]),
        action=source.get("action", ""),
        actor_id=_uuid(source.get("actor_id")),
        user_id=_uuid(source.get("user_id")),
        entity=source.get("entity"),
        entity_id=source.get("entity_id"),
        details=details if isinstance(details, dict) else {},
        correlation_id=source.get("correlation_id"),
        service=source.get("service"),
    )


class AuditSearchService:
    def __init__(self, client: AsyncElasticsearch, settings: ElasticSettings) -> None:
        self._client = client
        self._settings = settings

    async def search(
        self, query: AuditQuery, page: int, size: int
    ) -> tuple[list[AuditRecord], int]:
        try:
            response = await self._client.search(
                index=self._settings.audit_index_pattern,
                query=_build_query(query),
                sort=[{"@timestamp": {"order": "desc"}}],
                from_=(page - 1) * size,
                size=size,
                track_total_hits=True,
                source_includes=_SOURCE_FIELDS,
                # No audit index yet (nothing audited so far): an empty result, not an error.
                ignore_unavailable=True,
                allow_no_indices=True,
            )
        except (EsConnectionError, ConnectionTimeout) as exc:
            logger.warning("elasticsearch unavailable", error=str(exc))
            raise ExternalServiceError("Audit storage is unavailable, try again later") from exc
        except ApiError as exc:
            logger.error(  # noqa: TRY400 - the status and message are enough
                "audit search failed", status=exc.meta.status, error=str(exc)
            )
            raise ExternalServiceError("Audit search failed") from exc

        hits = response["hits"]
        return [_record(h) for h in hits["hits"]], int(hits["total"]["value"])
