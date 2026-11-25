"""KYC: client endpoints (``/api/v1/kyc``) and back-office endpoints (``/api/v1/admin/kyc``)."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, File, Form, Query, UploadFile, status
from fastapi.responses import FileResponse

from elbank.api.deps import CurrentUserDep, KycServiceDep, StaffUserDep
from elbank.api.openapi import Responses, error_response
from elbank.db.models import KycStatus
from elbank.deps import SettingsDep
from elbank.schemas.kyc import (
    KycRejectRequest,
    KycRequestAdminRead,
    KycRequestRead,
    Page,
    PassportNumber,
    PassportSeries,
)

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("KYC request not found")}

Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0)]

router = APIRouter(prefix="/requests", tags=["kyc"], responses=_AUTH_ERRORS)
admin_router = APIRouter(prefix="/requests", tags=["kyc-admin"], responses=_AUTH_ERRORS)


# ---------- client ----------


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Submit passport data and scan; checked at Gosuslugi right away",
    responses={
        status.HTTP_409_CONFLICT: error_response("A request is in progress or KYC is passed"),
        status.HTTP_413_CONTENT_TOO_LARGE: error_response("Scan is too large"),
        status.HTTP_415_UNSUPPORTED_MEDIA_TYPE: error_response("Scan is not a JPEG"),
    },
)
async def submit_kyc_request(
    *,
    passport_series: Annotated[PassportSeries, Form(examples=["4510"])],
    passport_number: Annotated[PassportNumber, Form(examples=["123456"])],
    birth_date: Annotated[date, Form(examples=["1990-05-17"])],
    file: Annotated[UploadFile, File(description="Passport scan, JPEG")],
    user: CurrentUserDep,
    kyc: KycServiceDep,
    settings: SettingsDep,
) -> KycRequestRead:
    # Read one byte past the limit to detect oversized uploads without loading more.
    scan = await file.read(settings.kyc.max_upload_bytes + 1)
    request = await kyc.submit(user, passport_series, passport_number, birth_date, scan)
    return KycRequestRead.model_validate(request)


@router.get("", summary="Current user's KYC requests, newest first")
async def list_my_kyc_requests(
    user: CurrentUserDep, kyc: KycServiceDep, limit: Limit = 20, offset: Offset = 0
) -> Page[KycRequestRead]:
    items, total = await kyc.list_for_user(user, limit, offset)
    return Page(
        items=[KycRequestRead.model_validate(i) for i in items],
        limit=limit,
        offset=offset,
        total=total,
    )


@router.get("/{request_id}", summary="KYC request status", responses=_NOT_FOUND)
async def get_my_kyc_request(
    request_id: uuid.UUID, user: CurrentUserDep, kyc: KycServiceDep
) -> KycRequestRead:
    return KycRequestRead.model_validate(await kyc.get_for_user(user, request_id))


# ---------- back office (manager, admin) ----------


@admin_router.get("", summary="KYC requests queue (manager/admin)")
async def list_kyc_requests(
    _: StaffUserDep,
    kyc: KycServiceDep,
    status_filter: Annotated[KycStatus | None, Query(alias="status")] = None,
    limit: Limit = 50,
    offset: Offset = 0,
) -> Page[KycRequestAdminRead]:
    items, total = await kyc.list_all(status_filter, limit, offset)
    return Page(
        items=[KycRequestAdminRead.model_validate(i) for i in items],
        limit=limit,
        offset=offset,
        total=total,
    )


@admin_router.get(
    "/{request_id}/file",
    summary="Passport scan of a KYC request (manager/admin)",
    response_class=FileResponse,
    responses={
        **_NOT_FOUND,
        status.HTTP_200_OK: {"content": {"image/jpeg": {}}, "description": "JPEG scan"},
    },
)
async def get_kyc_scan(request_id: uuid.UUID, _: StaffUserDep, kyc: KycServiceDep) -> FileResponse:
    path = await kyc.get_scan(request_id)
    return FileResponse(path, media_type="image/jpeg", filename=path.name)


@admin_router.post(
    "/{request_id}/approve",
    summary="Approve a pending / manual-review KYC request (manager/admin)",
    responses={**_NOT_FOUND, status.HTTP_409_CONFLICT: error_response("Already decided")},
)
async def approve_kyc_request(
    request_id: uuid.UUID, manager: StaffUserDep, kyc: KycServiceDep
) -> KycRequestAdminRead:
    request = await kyc.decide(manager, request_id, approve=True)
    return KycRequestAdminRead.model_validate(request)


@admin_router.post(
    "/{request_id}/reject",
    summary="Reject a pending / manual-review KYC request (manager/admin)",
    responses={**_NOT_FOUND, status.HTTP_409_CONFLICT: error_response("Already decided")},
)
async def reject_kyc_request(
    request_id: uuid.UUID, body: KycRejectRequest, manager: StaffUserDep, kyc: KycServiceDep
) -> KycRequestAdminRead:
    request = await kyc.decide(manager, request_id, approve=False, reason=body.reason)
    return KycRequestAdminRead.model_validate(request)
