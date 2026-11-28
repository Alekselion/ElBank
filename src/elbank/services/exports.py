"""Asynchronous export of the operation history (CSV or PDF), all accounts of a client.

1. API (``GET /operations/export``): a job is stored in Redis
   (``export_job:{job_id}``, TTL ``EXPORTS_JOB_TTL_SECONDS``) and
   ``export.requested`` is published; the client gets ``202`` + ``job_id``.
2. Worker (``elbank-exports-worker``): writes ``{EXPORTS_DIR}/{job_id}.{csv|pdf}``
   (:mod:`elbank.services.export_builder`) and marks the job ``ready``/``failed``.
3. API (``GET /operations/exports/{job_id}``): status; ``.../file`` downloads it.
"""

import asyncio
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import BaseModel
from redis.asyncio import Redis

from elbank.core.config import ExportSettings
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import User
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.operation import ExportFormat, ExportJobStatus
from elbank.services.operations import validate_period

logger = get_logger(__name__)

_MEDIA_TYPES = {ExportFormat.CSV: "text/csv; charset=utf-8", ExportFormat.PDF: "application/pdf"}


class ExportJob(BaseModel):
    """An export job as stored in Redis."""

    job_id: uuid.UUID
    user_id: uuid.UUID
    format: ExportFormat
    date_from: date
    date_to: date
    status: ExportJobStatus
    created_at: datetime
    completed_at: datetime | None = None
    rows: int | None = None
    error: str | None = None

    @property
    def download_name(self) -> str:
        return f"operations_{self.date_from}_{self.date_to}.{self.format.value}"

    @property
    def media_type(self) -> str:
        return _MEDIA_TYPES[self.format]


def _job_key(job_id: uuid.UUID) -> str:
    return f"export_job:{job_id}"


class ExportJobStore:
    """Jobs live in Redis as JSON strings and expire with their TTL."""

    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    async def create(self, job: ExportJob) -> None:
        await self._redis.set(_job_key(job.job_id), job.model_dump_json(), ex=self._ttl)

    async def get(self, job_id: uuid.UUID) -> ExportJob | None:
        raw = await self._redis.get(_job_key(job_id))
        return None if raw is None else ExportJob.model_validate_json(raw)

    async def update(self, job: ExportJob) -> bool:
        """Save a changed job keeping its expiry; ``False`` if it has already expired."""
        saved = await self._redis.set(
            _job_key(job.job_id), job.model_dump_json(), keepttl=True, xx=True
        )
        return bool(saved)

    async def delete(self, job_id: uuid.UUID) -> None:
        await self._redis.delete(_job_key(job_id))


def export_path(settings: ExportSettings, job: ExportJob) -> Path:
    return settings.dir / f"{job.job_id}.{job.format.value}"


class ExportService:
    """API side: create jobs, report their status, hand out ready files."""

    def __init__(
        self, store: ExportJobStore, publisher: EventPublisher, settings: ExportSettings
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._settings = settings

    async def request(
        self, user: User, fmt: ExportFormat, date_from: date, date_to: date
    ) -> ExportJob:
        validate_period(date_from, date_to, self._settings.max_period_days)
        if date_to > datetime.now(UTC).date():
            raise UnprocessableError("'to' must not be in the future")
        job = ExportJob(
            job_id=uuid.uuid4(),
            user_id=user.id,
            format=fmt,
            date_from=date_from,
            date_to=date_to,
            status=ExportJobStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        await self._store.create(job)
        try:
            await self._publisher.publish(
                EventType.EXPORT_REQUESTED,
                {
                    "job_id": str(job.job_id),
                    "user_id": str(job.user_id),
                    "format": fmt.value,
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                },
                key=str(user.id),
            )
        except Exception as exc:
            # Without the event no worker will ever pick the job up.
            await self._store.delete(job.job_id)
            raise ExternalServiceError("Export service is unavailable, try again later") from exc

        logger.info(
            "export requested",
            job_id=str(job.job_id),
            format=fmt.value,
            date_from=str(date_from),
            date_to=str(date_to),
        )
        return job

    async def get_for_user(self, user: User, job_id: uuid.UUID) -> ExportJob:
        job = await self._store.get(job_id)
        if job is None or job.user_id != user.id:
            raise NotFoundError("Export job not found or expired")
        return job

    async def file_for_user(self, user: User, job_id: uuid.UUID) -> tuple[ExportJob, Path]:
        job = await self.get_for_user(user, job_id)
        if job.status is not ExportJobStatus.READY:
            raise InvalidStateError(f"Export is not ready, job is {job.status.value}")
        path = export_path(self._settings, job)
        if not await asyncio.to_thread(path.is_file):
            raise NotFoundError("Export file is missing")
        return job, path
