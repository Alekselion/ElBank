"""Deposits (``/api/v1/deposits``) and the manual accrual trigger (``/api/v1/admin/deposits``)."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Body, status

from elbank.api.deps import CurrentUserDep, DepositServiceDep, StaffUserDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.product import (
    AccrualAccepted,
    AccrualRequest,
    DepositCloseResult,
    DepositDetails,
    DepositOpenRequest,
    DepositRead,
    DepositTopUpRequest,
)
from elbank.services.deposits import deposit_read

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Deposit not found")}
_WRONG_STATE: Responses = {
    status.HTTP_409_CONFLICT: error_response("Not allowed in the current deposit/account status")
}
_NO_FUNDS: Responses = {
    status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Insufficient funds or invalid terms")
}

router = APIRouter(tags=["deposits"], responses=_AUTH_ERRORS)
admin_router = APIRouter(tags=["deposits-admin"], responses=_AUTH_ERRORS)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Open a deposit: the amount is debited from the account → deposit.opened",
    responses={
        status.HTTP_404_NOT_FOUND: error_response("Account not found"),
        **_WRONG_STATE,
        **_NO_FUNDS,
    },
)
async def open_deposit(
    body: DepositOpenRequest, user: CurrentUserDep, deposits: DepositServiceDep
) -> DepositRead:
    deposit = await deposits.open(user, body.account_id, body.amount, body.term_months, body.rate)
    return deposit_read(deposit)


@router.get("", summary="Current user's deposits")
async def list_deposits(user: CurrentUserDep, deposits: DepositServiceDep) -> list[DepositRead]:
    return [deposit_read(d) for d in await deposits.list_for_user(user)]


@router.get(
    "/{deposit_id}",
    summary="Deposit details: interest accrued by dates and to date",
    responses=_NOT_FOUND,
)
async def get_deposit(
    deposit_id: uuid.UUID, user: CurrentUserDep, deposits: DepositServiceDep
) -> DepositDetails:
    deposit = await deposits.get_for_user(user, deposit_id)
    return DepositDetails.from_deposit_details(deposit, deposit_read(deposit).interest_to_date)


@router.post(
    "/{deposit_id}/top-up",
    summary="Top up from the deposit's account",
    responses={**_NOT_FOUND, **_WRONG_STATE, **_NO_FUNDS},
)
async def top_up_deposit(
    deposit_id: uuid.UUID,
    body: DepositTopUpRequest,
    user: CurrentUserDep,
    deposits: DepositServiceDep,
) -> DepositRead:
    return deposit_read(await deposits.top_up(user, deposit_id, body.amount))


@router.post(
    "/{deposit_id}/close",
    summary="Close: principal and interest go to the account → deposit.closed",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def close_deposit(
    deposit_id: uuid.UUID, user: CurrentUserDep, deposits: DepositServiceDep
) -> DepositCloseResult:
    return await deposits.close(user, deposit_id)


@admin_router.post(
    "/accrue",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger interest accrual (deposit.interest.tick) — manager/admin",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("as_of is in the future"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Kafka is unavailable"),
    },
)
async def trigger_accrual(
    _: StaffUserDep,
    deposits: DepositServiceDep,
    body: Annotated[AccrualRequest | None, Body()] = None,
) -> AccrualAccepted:
    return await deposits.request_accrual(body.as_of if body else None)
