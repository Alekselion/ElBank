"""Reading the audit trail from Elasticsearch (``elbank-audit-*``, see :mod:`audit_service`).

The database is not involved: audit events exist only in Elasticsearch, written
by the audit worker from ``audit.event``.
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
from elbank.services.audit_service import AUDIT_INDEX_PATTERN, AuditEvent

logger = get_logger(__name__)

# from + size of a plain search; deeper pages need a narrower period.
MAX_RESULT_WINDOW = 10_000


@dataclass(frozen=True, slots=True)
class AuditQuery:
    # Who acted (user_id), or the user the action was about (entity=user, entity_id).
    user_id: uuid.UUID | None = None
    action: str | None = None  # exact, or a prefix ending with "*": "admin.user.*"
    date_from: datetime | None = None
    date_to: datetime | None = None


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
                    "should": [
                        {"term": {"user_id": value}},
                        {
                            "bool": {
                                "filter": [
                                    {"term": {"entity": "user"}},
                                    {"term": {"entity_id": value}},
                                ]
                            }
                        },
                    ],
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
        filters.append({"range": {"timestamp": bounds}})
    return {"bool": {"filter": filters}} if filters else {"match_all": {}}


class AuditSearchService:
    def __init__(self, client: AsyncElasticsearch) -> None:
        self._client = client

    async def search(self, query: AuditQuery, page: int, size: int) -> tuple[list[AuditEvent], int]:
        try:
            response = await self._client.search(
                index=AUDIT_INDEX_PATTERN,
                query=_build_query(query),
                sort=[{"timestamp": {"order": "desc"}}],
                from_=(page - 1) * size,
                size=size,
                track_total_hits=True,
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
        events = [AuditEvent.model_validate(h["_source"]) for h in hits["hits"]]
        return events, int(hits["total"]["value"])
