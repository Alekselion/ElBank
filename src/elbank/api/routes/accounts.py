"""Accounts (``/api/v1/accounts``): opening, requisites, blocking, closing, limits, statements."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import FileResponse

from elbank.api.deps import (
    AccountServiceDep,
    CurrentUserDep,
    StaffUserDep,
    StatementServiceDep,
)
from elbank.api.openapi import Responses, error_response
from elbank.schemas.account import (
    AccountLimitsRead,
    AccountLimitsUpdate,
    AccountOpenRequest,
    AccountRead,
    AccountRequisites,
    StatementJobAccepted,
    StatementJobRead,
    StatementJobStatus,
)
from elbank.services.statements import StatementJob

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Account not found")}
_JOB_NOT_FOUND: Responses = {
    status.HTTP_404_NOT_FOUND: error_response("Statement job not found or expired")
}
_WRONG_STATE: Responses = {
    status.HTTP_409_CONFLICT: error_response("Not allowed in the current account status")
}

router = APIRouter(tags=["accounts"], responses=_AUTH_ERRORS)


def _job_read(request: Request, job: StatementJob) -> StatementJobRead:
    download_url = None
    if job.status is StatementJobStatus.READY:
        download_url = str(request.app.url_path_for("download_statement", job_id=str(job.job_id)))
    return StatementJobRead(
        job_id=job.job_id,
        account_id=job.account_id,
        date_from=job.date_from,
        date_to=job.date_to,
        status=job.status,
        created_at=job.created_at,
        completed_at=job.completed_at,
        error=job.error,
        download_url=download_url,
    )


# ---------- accounts ----------


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Open an account (KYC must be approved)",
    responses={
        status.HTTP_403_FORBIDDEN: error_response("KYC is not approved (code KYC_REQUIRED)"),
    },
)
async def open_account(
    body: AccountOpenRequest, user: CurrentUserDep, accounts: AccountServiceDep
) -> AccountRead:
    account = await accounts.open(user, body.type, body.currency)
    return AccountRead.model_validate(account)


@router.get("", summary="Current user's accounts (cached for a short time)")
async def list_accounts(user: CurrentUserDep, accounts: AccountServiceDep) -> list[AccountRead]:
    return await accounts.list_for_user(user)


# Declared before "/{account_id}..." so "statements" is not parsed as an account id.
@router.get(
    "/statements/{job_id}",
    summary="Statement job status and download link",
    responses=_JOB_NOT_FOUND,
)
async def get_statement_job(
    job_id: uuid.UUID, request: Request, user: CurrentUserDep, statements: StatementServiceDep
) -> StatementJobRead:
    return _job_read(request, await statements.get_for_user(user, job_id))


@router.get(
    "/statements/{job_id}/file",
    summary="Download a ready statement (CSV)",
    response_class=FileResponse,
    responses={
        **_JOB_NOT_FOUND,
        status.HTTP_200_OK: {"content": {"text/csv": {}}, "description": "Statement CSV"},
        status.HTTP_409_CONFLICT: error_response("Statement is not ready yet"),
    },
)
async def download_statement(
    job_id: uuid.UUID, user: CurrentUserDep, statements: StatementServiceDep
) -> FileResponse:
    job, path = await statements.file_for_user(user, job_id)
    return FileResponse(path, media_type="text/csv; charset=utf-8", filename=job.download_name)


@router.get("/{account_id}", summary="Account details", responses=_NOT_FOUND)
async def get_account(
    account_id: uuid.UUID, user: CurrentUserDep, accounts: AccountServiceDep
) -> AccountRead:
    return AccountRead.model_validate(await accounts.get_for_user(user, account_id))


@router.get(
    "/{account_id}/requisites",
    summary="Payment requisites: account number, BIC, correspondent account, IBAN",
    responses=_NOT_FOUND,
)
async def get_requisites(
    account_id: uuid.UUID, user: CurrentUserDep, accounts: AccountServiceDep
) -> AccountRequisites:
    return AccountRequisites.model_validate(await accounts.requisites(user, account_id))


@router.delete(
    "/{account_id}",
    summary="Close an account (active, zero balance, no operations in progress)",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def close_account(
    account_id: uuid.UUID, user: CurrentUserDep, accounts: AccountServiceDep
) -> AccountRead:
    return AccountRead.model_validate(await accounts.close(user, account_id))


# ---------- back office (manager, admin) ----------


@router.post(
    "/{account_id}/block",
    summary="Block (freeze) an account (manager/admin)",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def block_account(
    account_id: uuid.UUID, staff: StaffUserDep, accounts: AccountServiceDep
) -> AccountRead:
    return AccountRead.model_validate(await accounts.block(staff, account_id))


@router.post(
    "/{account_id}/unblock",
    summary="Unblock a frozen account (manager/admin)",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def unblock_account(
    account_id: uuid.UUID, staff: StaffUserDep, accounts: AccountServiceDep
) -> AccountRead:
    return AccountRead.model_validate(await accounts.unblock(staff, account_id))


# ---------- limits ----------


@router.get("/{account_id}/limits", summary="Outgoing limits", responses=_NOT_FOUND)
async def get_limits(
    account_id: uuid.UUID, user: CurrentUserDep, accounts: AccountServiceDep
) -> AccountLimitsRead:
    return await accounts.get_limits(user, account_id)


@router.put(
    "/{account_id}/limits",
    summary="Set daily/monthly outgoing limits",
    responses={**_NOT_FOUND, status.HTTP_409_CONFLICT: error_response("Account is closed")},
)
async def update_limits(
    account_id: uuid.UUID,
    body: AccountLimitsUpdate,
    user: CurrentUserDep,
    accounts: AccountServiceDep,
) -> AccountLimitsRead:
    return await accounts.update_limits(user, account_id, body.daily_limit, body.monthly_limit)


# ---------- statements ----------


@router.get(
    "/{account_id}/statement",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Request a CSV statement for a period; generated asynchronously",
    responses={
        **_NOT_FOUND,
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Invalid period"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Statement queue is unavailable"),
    },
)
async def request_statement(
    *,
    account_id: uuid.UUID,
    request: Request,
    user: CurrentUserDep,
    accounts: AccountServiceDep,
    statements: StatementServiceDep,
    date_from: Annotated[date, Query(alias="from", description="First day, inclusive (UTC)")],
    date_to: Annotated[date, Query(alias="to", description="Last day, inclusive (UTC)")],
) -> StatementJobAccepted:
    account = await accounts.get_for_user(user, account_id)
    job = await statements.request(account, date_from, date_to)
    return StatementJobAccepted(
        job_id=job.job_id,
        status=job.status,
        status_url=str(request.app.url_path_for("get_statement_job", job_id=str(job.job_id))),
    )
