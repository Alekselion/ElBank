"""Asynchronous account statements (CSV).

1. API: the client asks for a statement of an account for a period; a job is
   stored in Redis (``statement_job:{job_id}``, TTL ``STATEMENTS_JOB_TTL_SECONDS``)
   and ``statement.requested`` is published; the client gets ``202`` + ``job_id``.
2. Worker (``elbank-statements-worker``): consumes ``statement.requested``,
   writes ``{STATEMENTS_DIR}/{job_id}.csv`` and marks the job ``ready``/``failed``.
3. API: the client polls the job and downloads the file once it is ready.
"""

import asyncio
import csv
import io
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import TextIO

from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import StatementSettings
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import Account, Operation, OperationType, User
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.account import StatementJobStatus

logger = get_logger(__name__)

_CREDIT_TYPES = frozenset(
    {
        OperationType.TRANSFER_IN,
        OperationType.CONVERSION_IN,
        OperationType.INTEREST,
        OperationType.CREDIT_DISBURSEMENT,
        OperationType.DEPOSIT_CLOSE,
        OperationType.INVESTMENT_SELL,
    }
)
_CSV_HEADER = (
    "operation_id",
    "created_at",
    "type",
    "direction",
    "amount",
    "currency",
    "status",
    "counterparty_account",
    "description",
)
# Spreadsheets execute cells starting with these as formulas (CSV injection).
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
_STREAM_BATCH = 500


class StatementJob(BaseModel):
    """A statement job as stored in Redis."""

    job_id: uuid.UUID
    user_id: uuid.UUID
    account_id: uuid.UUID
    account_number: str
    date_from: date
    date_to: date
    status: StatementJobStatus
    created_at: datetime
    completed_at: datetime | None = None
    rows: int | None = None
    error: str | None = None

    @property
    def download_name(self) -> str:
        return f"statement_{self.account_number}_{self.date_from}_{self.date_to}.csv"


def _job_key(job_id: uuid.UUID) -> str:
    return f"statement_job:{job_id}"


class StatementJobStore:
    """Jobs live in Redis as JSON strings and expire with their TTL."""

    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    async def create(self, job: StatementJob) -> None:
        await self._redis.set(_job_key(job.job_id), job.model_dump_json(), ex=self._ttl)

    async def get(self, job_id: uuid.UUID) -> StatementJob | None:
        raw = await self._redis.get(_job_key(job_id))
        return None if raw is None else StatementJob.model_validate_json(raw)

    async def update(self, job: StatementJob) -> bool:
        """Save a changed job keeping its expiry; ``False`` if it has already expired."""
        saved = await self._redis.set(
            _job_key(job.job_id), job.model_dump_json(), keepttl=True, xx=True
        )
        return bool(saved)

    async def delete(self, job_id: uuid.UUID) -> None:
        await self._redis.delete(_job_key(job_id))


def statement_path(settings: StatementSettings, job_id: uuid.UUID) -> Path:
    return settings.dir / f"{job_id}.csv"


class StatementService:
    """API side: create jobs, report their status, hand out ready files."""

    def __init__(
        self, store: StatementJobStore, publisher: EventPublisher, settings: StatementSettings
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._settings = settings

    async def request(self, account: Account, date_from: date, date_to: date) -> StatementJob:
        self._validate_period(date_from, date_to)
        job = StatementJob(
            job_id=uuid.uuid4(),
            user_id=account.user_id,
            account_id=account.id,
            account_number=account.number,
            date_from=date_from,
            date_to=date_to,
            status=StatementJobStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        await self._store.create(job)
        try:
            await self._publisher.publish(
                EventType.STATEMENT_REQUESTED,
                {
                    "job_id": str(job.job_id),
                    "user_id": str(job.user_id),
                    "account_id": str(job.account_id),
                    "date_from": job.date_from.isoformat(),
                    "date_to": job.date_to.isoformat(),
                },
                key=str(job.account_id),
            )
        except Exception as exc:
            # Without the event no worker will ever pick the job up.
            await self._store.delete(job.job_id)
            raise ExternalServiceError("Statement service is unavailable, try again later") from exc

        logger.info(
            "statement requested",
            job_id=str(job.job_id),
            account_id=str(job.account_id),
            date_from=str(date_from),
            date_to=str(date_to),
        )
        return job

    async def get_for_user(self, user: User, job_id: uuid.UUID) -> StatementJob:
        job = await self._store.get(job_id)
        if job is None or job.user_id != user.id:
            raise NotFoundError("Statement job not found or expired")
        return job

    async def file_for_user(self, user: User, job_id: uuid.UUID) -> tuple[StatementJob, Path]:
        job = await self.get_for_user(user, job_id)
        if job.status is not StatementJobStatus.READY:
            raise InvalidStateError(f"Statement is not ready, job is {job.status.value}")
        path = statement_path(self._settings, job.job_id)
        if not await asyncio.to_thread(path.is_file):
            raise NotFoundError("Statement file is missing")
        return job, path

    def _validate_period(self, date_from: date, date_to: date) -> None:
        if date_from > date_to:
            raise UnprocessableError("'from' must not be later than 'to'")
        if date_to > datetime.now(UTC).date():
            raise UnprocessableError("'to' must not be in the future")
        if (date_to - date_from).days + 1 > self._settings.max_period_days:
            raise UnprocessableError(
                f"Statement period must not exceed {self._settings.max_period_days} days"
            )


# ---------- worker side ----------


def _safe_cell(value: str | None) -> str:
    if not value:
        return ""
    return "'" + value if value.startswith(_FORMULA_PREFIXES) else value


def _csv_row(op: Operation) -> tuple[str, ...]:
    return (
        str(op.id),
        op.created_at.isoformat(),
        op.type.value,
        "credit" if op.type in _CREDIT_TYPES else "debit",
        format(op.amount, "f"),
        op.currency,
        op.status.value,
        _safe_cell(op.counterparty_account),
        _safe_cell(op.description),
    )


class StatementBuilder:
    """Worker side: render a job's operations into a CSV file."""

    def __init__(self, settings: StatementSettings) -> None:
        self._settings = settings

    async def build(self, session: AsyncSession, job: StatementJob) -> int:
        """Write the CSV atomically (temp file + rename); returns the number of operations."""
        path = statement_path(self._settings, job.job_id)
        tmp = path.with_name(path.name + ".part")
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)

        file = await asyncio.to_thread(tmp.open, "w", encoding="utf-8", newline="")
        try:
            rows = await self._write(session, job, file)
        except BaseException:
            await asyncio.to_thread(file.close)
            await asyncio.to_thread(tmp.unlink, missing_ok=True)
            raise
        await asyncio.to_thread(file.close)
        await asyncio.to_thread(tmp.replace, path)
        return rows

    @staticmethod
    async def _write(session: AsyncSession, job: StatementJob, file: TextIO) -> int:
        # Operation timestamps are UTC; the period covers whole UTC days.
        start = datetime.combine(job.date_from, dt_time.min, tzinfo=UTC)
        end = datetime.combine(job.date_to + timedelta(days=1), dt_time.min, tzinfo=UTC)
        query = (
            select(Operation)
            .where(
                Operation.account_id == job.account_id,
                Operation.created_at >= start,
                Operation.created_at < end,
            )
            .order_by(Operation.created_at, Operation.id)
            .execution_options(yield_per=_STREAM_BATCH)
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(_CSV_HEADER)
        rows = 0
        result = await session.stream_scalars(query)
        async for batch in result.partitions(_STREAM_BATCH):
            for op in batch:
                writer.writerow(_csv_row(op))
            rows += len(batch)
            await asyncio.to_thread(file.write, buffer.getvalue())
            buffer.seek(0)
            buffer.truncate()
        await asyncio.to_thread(file.write, buffer.getvalue())
        return rows

    async def remove_expired_files(self) -> int:
        """Delete CSV files older than the job TTL (their jobs are gone from Redis)."""
        return await asyncio.to_thread(self._remove_expired_files_sync)

    def _remove_expired_files_sync(self) -> int:
        directory = self._settings.dir
        if not directory.is_dir():
            return 0
        deadline = time.time() - self._settings.job_ttl_seconds
        removed = 0
        for path in directory.iterdir():
            if path.suffix in {".csv", ".part"} and path.stat().st_mtime < deadline:
                path.unlink(missing_ok=True)
                removed += 1
        return removed
