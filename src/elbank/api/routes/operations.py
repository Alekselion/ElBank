"""Operation history (``/api/v1/operations``): list, details, CSV/PDF export."""

import uuid
from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import FileResponse

from elbank.api.deps import CurrentUserDep, ExportServiceDep, OperationHistoryServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.db.models import OperationStatus, OperationType
from elbank.schemas.operation import (
    ExportFormat,
    ExportJobAccepted,
    ExportJobRead,
    ExportJobStatus,
    OperationDetail,
    OperationPage,
)
from elbank.services.exports import ExportJob
from elbank.services.operations import OperationFilters

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_JOB_NOT_FOUND: Responses = {
    status.HTTP_404_NOT_FOUND: error_response("Export job not found or expired")
}
_INVALID: Responses = {
    status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Invalid filter or period")
}
MAX_PAGE_SIZE = 100

router = APIRouter(tags=["operations"], responses=_AUTH_ERRORS)


def _job_read(request: Request, job: ExportJob) -> ExportJobRead:
    download_url = None
    if job.status is ExportJobStatus.READY:
        download_url = str(request.app.url_path_for("download_export", job_id=str(job.job_id)))
    return ExportJobRead(
        job_id=job.job_id,
        format=job.format,
        date_from=job.date_from,
        date_to=job.date_to,
        status=job.status,
        created_at=job.created_at,
        completed_at=job.completed_at,
        rows=job.rows,
        error=job.error,
        download_url=download_url,
    )


@router.get(
    "",
    summary="Operations of all own accounts, newest first",
    description=(
        "Periods are whole UTC days. Several types: `?type=payment&type=fee` "
        "or `?type[]=payment&type[]=fee`."
    ),
    responses=_INVALID,
)
async def list_operations(
    *,
    user: CurrentUserDep,
    history: OperationHistoryServiceDep,
    date_from: Annotated[date | None, Query(alias="from")] = None,
    date_to: Annotated[date | None, Query(alias="to")] = None,
    account_id: uuid.UUID | None = None,
    types: Annotated[list[OperationType] | None, Query(alias="type")] = None,
    types_brackets: Annotated[
        list[OperationType] | None,
        Query(alias="type[]", description="Same as `type` (PHP/axios array style)"),
    ] = None,
    status_: Annotated[OperationStatus | None, Query(alias="status")] = None,
    amount_min: Annotated[Decimal | None, Query(ge=0, max_digits=18, decimal_places=2)] = None,
    amount_max: Annotated[Decimal | None, Query(ge=0, max_digits=18, decimal_places=2)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 20,
) -> OperationPage:
    filters = OperationFilters(
        date_from=date_from,
        date_to=date_to,
        account_id=account_id,
        types=tuple(dict.fromkeys([*(types or ()), *(types_brackets or ())])),
        status=status_,
        amount_min=amount_min,
        amount_max=amount_max,
    )
    items, total = await history.list_for_user(user, filters, page=page, size=size)
    return OperationPage(items=items, total=total, page=page, size=size)


# Declared before "/{operation_id}" so "export(s)" is not parsed as an operation id.
@router.get(
    "/export",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Request a CSV/PDF export of the history for a period (asynchronous)",
    responses={
        **_INVALID,
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Export service is unavailable"),
    },
)
async def request_export(
    *,
    request: Request,
    user: CurrentUserDep,
    exports: ExportServiceDep,
    date_from: Annotated[date, Query(alias="from")],
    date_to: Annotated[date, Query(alias="to")],
    fmt: Annotated[ExportFormat, Query(alias="format")] = ExportFormat.CSV,
) -> ExportJobAccepted:
    job = await exports.request(user, fmt, date_from, date_to)
    return ExportJobAccepted(
        job_id=job.job_id,
        status=job.status,
        status_url=str(request.app.url_path_for("get_export_job", job_id=str(job.job_id))),
    )


@router.get(
    "/exports/{job_id}", summary="Export job status and download link", responses=_JOB_NOT_FOUND
)
async def get_export_job(
    job_id: uuid.UUID, request: Request, user: CurrentUserDep, exports: ExportServiceDep
) -> ExportJobRead:
    return _job_read(request, await exports.get_for_user(user, job_id))


@router.get(
    "/exports/{job_id}/file",
    summary="Download a ready export (CSV or PDF)",
    response_class=FileResponse,
    responses={
        **_JOB_NOT_FOUND,
        status.HTTP_200_OK: {
            "content": {"text/csv": {}, "application/pdf": {}},
            "description": "Export file",
        },
        status.HTTP_409_CONFLICT: error_response("Export is not ready yet"),
    },
)
async def download_export(
    job_id: uuid.UUID, user: CurrentUserDep, exports: ExportServiceDep
) -> FileResponse:
    job, path = await exports.file_for_user(user, job_id)
    return FileResponse(path, media_type=job.media_type, filename=job.download_name)


@router.get(
    "/{operation_id}",
    summary="Operation details",
    responses={status.HTTP_404_NOT_FOUND: error_response("Operation not found")},
)
async def get_operation(
    operation_id: uuid.UUID, user: CurrentUserDep, history: OperationHistoryServiceDep
) -> OperationDetail:
    return await history.get_for_user(user, operation_id)
