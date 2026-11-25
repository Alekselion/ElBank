"""KYC: passport submission, automatic check at Gosuslugi, manual decisions.

Flow of a submission:

1. the request is stored as ``pending`` and the scan saved to
   ``{KYC_UPLOADS_DIR}/{request_id}.jpg``; ``kyc.requested`` is published;
2. the passport is checked at Gosuslugi (no DB transaction is held meanwhile):
   valid → ``approved``, invalid → ``rejected`` with the reason, Gosuslugi
   unavailable or unintelligible → ``manual_review`` (a manager decides);
3. a final decision publishes ``kyc.decided``.
"""

import asyncio
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients.gosuslugi import GosuslugiClient, GosuslugiError
from elbank.core.config import KycSettings
from elbank.core.exceptions import (
    InvalidStateError,
    NotFoundError,
    PayloadTooLargeError,
    UnprocessableError,
    UnsupportedMediaTypeError,
)
from elbank.core.logging import get_logger
from elbank.db.models import KycRequest, KycStatus, User
from elbank.kafka import EventPublisher, EventType

logger = get_logger(__name__)

_JPEG_MAGIC = b"\xff\xd8\xff"
_MIN_AGE_YEARS = 14  # a Russian passport is issued from 14
_MAX_AGE_YEARS = 120
_OPEN_STATUSES = (KycStatus.PENDING, KycStatus.MANUAL_REVIEW)
_DEFAULT_REJECT_REASON = "Passport data was not confirmed by Gosuslugi"


def _age_on(birth_date: date, today: date) -> int:
    return (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )


def _validate_birth_date(birth_date: date) -> None:
    age = _age_on(birth_date, datetime.now(UTC).date())
    if not _MIN_AGE_YEARS <= age <= _MAX_AGE_YEARS:
        raise UnprocessableError(
            f"birth_date must correspond to an age of {_MIN_AGE_YEARS}..{_MAX_AGE_YEARS} years"
        )


def _validate_scan(content: bytes, max_bytes: int) -> None:
    if len(content) > max_bytes:
        raise PayloadTooLargeError(f"Scan must not exceed {max_bytes} bytes")
    if not content.startswith(_JPEG_MAGIC):
        raise UnsupportedMediaTypeError("Scan must be a JPEG image")


class KycService:
    def __init__(
        self,
        session: AsyncSession,
        gosuslugi: GosuslugiClient,
        publisher: EventPublisher,
        settings: KycSettings,
    ) -> None:
        self._session = session
        self._gosuslugi = gosuslugi
        self._publisher = publisher
        self._settings = settings

    def scan_path(self, request_id: uuid.UUID) -> Path:
        return self._settings.uploads_dir / f"{request_id}.jpg"

    # ---------- client side ----------

    async def submit(
        self,
        user: User,
        passport_series: str,
        passport_number: str,
        birth_date: date,
        scan: bytes,
    ) -> KycRequest:
        _validate_birth_date(birth_date)
        _validate_scan(scan, self._settings.max_upload_bytes)

        # Serialize submissions of one user so two parallel requests cannot both pass
        # the "no open request" check; the lock is released by the commit below.
        await self._session.execute(select(User.id).where(User.id == user.id).with_for_update())
        await self._ensure_can_submit(user)

        request = KycRequest(
            id=uuid.uuid4(),
            user_id=user.id,
            status=KycStatus.PENDING,
            passport_series=passport_series,
            passport_number=passport_number,
            birth_date=birth_date,
        )
        self._session.add(request)
        path = self.scan_path(request.id)
        await asyncio.to_thread(self._write_scan, path, scan)
        try:
            await self._session.commit()
        except BaseException:
            await asyncio.to_thread(path.unlink, missing_ok=True)
            raise

        logger.info("kyc requested", kyc_request_id=str(request.id), user_id=str(user.id))
        await self._publisher.publish_safely(
            EventType.KYC_REQUESTED,
            {
                "request_id": str(request.id),
                "user_id": str(user.id),
                "status": request.status.value,
            },
            key=str(user.id),
        )

        await self._auto_decide(user, request)
        return request

    async def list_for_user(
        self, user: User, limit: int, offset: int
    ) -> tuple[list[KycRequest], int]:
        return await self._page(KycRequest.user_id == user.id, limit=limit, offset=offset)

    async def get_for_user(self, user: User, request_id: uuid.UUID) -> KycRequest:
        request = await self._session.get(KycRequest, request_id)
        if request is None or request.user_id != user.id:
            raise NotFoundError("KYC request not found")
        return request

    # ---------- manager side ----------

    async def list_all(
        self, status: KycStatus | None, limit: int, offset: int
    ) -> tuple[list[KycRequest], int]:
        condition = (KycRequest.status == status) if status is not None else None
        return await self._page(condition, limit=limit, offset=offset)

    async def get(self, request_id: uuid.UUID) -> KycRequest:
        request = await self._session.get(KycRequest, request_id)
        if request is None:
            raise NotFoundError("KYC request not found")
        return request

    async def get_scan(self, request_id: uuid.UUID) -> Path:
        request = await self.get(request_id)
        path = self.scan_path(request.id)
        if not await asyncio.to_thread(path.is_file):
            raise NotFoundError("Scan file is missing")
        return path

    async def decide(
        self,
        manager: User,
        request_id: uuid.UUID,
        *,
        approve: bool,
        reason: str | None = None,
    ) -> KycRequest:
        request = await self._session.scalar(
            select(KycRequest).where(KycRequest.id == request_id).with_for_update()
        )
        if request is None:
            raise NotFoundError("KYC request not found")
        if request.status not in _OPEN_STATUSES:
            raise InvalidStateError(f"KYC request is already {request.status.value}")

        self._apply_decision(request, approve=approve, reason=reason, decided_by=manager)
        await self._session.commit()
        logger.info(
            "kyc decided manually",
            kyc_request_id=str(request.id),
            status=request.status.value,
            manager_id=str(manager.id),
        )
        await self._publish_decided(request, source="manager")
        return request

    # ---------- internals ----------

    async def _ensure_can_submit(self, user: User) -> None:
        statuses = (*_OPEN_STATUSES, KycStatus.APPROVED)
        existing = await self._session.scalar(
            select(KycRequest.status)
            .where(KycRequest.user_id == user.id, KycRequest.status.in_(statuses))
            .limit(1)
        )
        if existing is KycStatus.APPROVED:
            raise InvalidStateError("Identity is already verified")
        if existing is not None:
            raise InvalidStateError("There is already a KYC request in progress")

    @staticmethod
    def _write_scan(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def _auto_decide(self, user: User, request: KycRequest) -> None:
        try:
            result = await self._gosuslugi.verify_passport(
                request.passport_series, request.passport_number, request.birth_date
            )
        except GosuslugiError:
            request.status = KycStatus.MANUAL_REVIEW
            await self._session.commit()
            logger.warning("kyc sent to manual review", kyc_request_id=str(request.id))
            return

        if result.valid:
            self._apply_decision(request, approve=True)
            if result.full_name:
                user.full_name = result.full_name  # legal name confirmed by the state registry
        else:
            self._apply_decision(
                request, approve=False, reason=result.message or _DEFAULT_REJECT_REASON
            )
        await self._session.commit()
        logger.info(
            "kyc decided automatically",
            kyc_request_id=str(request.id),
            status=request.status.value,
        )
        await self._publish_decided(request, source="gosuslugi")

    @staticmethod
    def _apply_decision(
        request: KycRequest,
        *,
        approve: bool,
        reason: str | None = None,
        decided_by: User | None = None,
    ) -> None:
        request.status = KycStatus.APPROVED if approve else KycStatus.REJECTED
        request.reject_reason = None if approve else reason
        request.decided_at = datetime.now(UTC)
        request.decided_by_id = decided_by.id if decided_by else None

    async def _publish_decided(self, request: KycRequest, *, source: str) -> None:
        payload: dict[str, Any] = {
            "request_id": str(request.id),
            "user_id": str(request.user_id),
            "status": request.status.value,
            "reject_reason": request.reject_reason,
            "decided_at": request.decided_at.isoformat() if request.decided_at else None,
            "decided_by": str(request.decided_by_id) if request.decided_by_id else None,
            "source": source,
        }
        await self._publisher.publish_safely(
            EventType.KYC_DECIDED, payload, key=str(request.user_id)
        )

    async def _page(
        self, condition: ColumnElement[bool] | None, *, limit: int, offset: int
    ) -> tuple[list[KycRequest], int]:
        items_query = select(KycRequest).order_by(KycRequest.created_at.desc(), KycRequest.id)
        total_query = select(func.count()).select_from(KycRequest)
        if condition is not None:
            items_query = items_query.where(condition)
            total_query = total_query.where(condition)
        items = (await self._session.scalars(items_query.limit(limit).offset(offset))).all()
        total = await self._session.scalar(total_query) or 0
        return list(items), total
