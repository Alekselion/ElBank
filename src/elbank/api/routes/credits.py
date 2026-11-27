"""Credits (``/api/v1/credits``): application, signing with OTP, schedule, repayment."""

import uuid

from fastapi import APIRouter, status

from elbank.api.deps import CreditServiceDep, CurrentUserDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.product import (
    CreditApplicationRequest,
    CreditRead,
    CreditRepayRequest,
    CreditRepayResult,
    CreditSchedule,
    CreditSignRequest,
    SignOtpSent,
)
from elbank.services.credits import left_to_pay

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Credit not found")}
_WRONG_STATE: Responses = {
    status.HTTP_409_CONFLICT: error_response("Not allowed in the current credit/account status")
}

router = APIRouter(tags=["credits"], responses=_AUTH_ERRORS)


@router.post(
    "/applications",
    status_code=status.HTTP_201_CREATED,
    summary="Apply for a credit: scored at once → approved (credit.approved) or rejected",
    responses={
        status.HTTP_404_NOT_FOUND: error_response("Account not found"),
        status.HTTP_409_CONFLICT: error_response("Account is not active"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Amount/term out of range"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Credit bureau is unavailable"),
    },
)
async def apply_for_credit(
    body: CreditApplicationRequest, user: CurrentUserDep, credits: CreditServiceDep
) -> CreditRead:
    credit = await credits.apply(user, body.account_id, body.amount, body.term_months, body.purpose)
    return CreditRead.model_validate(credit)


@router.get("", summary="Current user's credits and applications")
async def list_credits(user: CurrentUserDep, credits: CreditServiceDep) -> list[CreditRead]:
    return [CreditRead.model_validate(c) for c in await credits.list_for_user(user)]


@router.get("/{credit_id}", summary="Credit details", responses=_NOT_FOUND)
async def get_credit(
    credit_id: uuid.UUID, user: CurrentUserDep, credits: CreditServiceDep
) -> CreditRead:
    return CreditRead.model_validate(await credits.get_for_user(user, credit_id))


@router.get("/{credit_id}/schedule", summary="Annuity payment schedule", responses=_NOT_FOUND)
async def get_schedule(
    credit_id: uuid.UUID, user: CurrentUserDep, credits: CreditServiceDep
) -> CreditSchedule:
    credit = await credits.get_for_user(user, credit_id)
    return CreditSchedule.from_credit(credit, left_to_pay(credit))


@router.post(
    "/{credit_id}/sign/otp",
    summary="Send the signing code again (it is sent automatically on approval)",
    responses={
        **_NOT_FOUND,
        **_WRONG_STATE,
        status.HTTP_429_TOO_MANY_REQUESTS: error_response("A code was sent recently"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("SMS gateway is unavailable"),
    },
)
async def send_sign_otp(
    credit_id: uuid.UUID, user: CurrentUserDep, credits: CreditServiceDep
) -> SignOtpSent:
    return await credits.send_sign_otp(user, credit_id)


@router.post(
    "/{credit_id}/sign",
    summary="Sign the agreement with the SMS code: the amount is credited → credit.signed",
    responses={
        **_NOT_FOUND,
        **_WRONG_STATE,
        status.HTTP_400_BAD_REQUEST: error_response("Wrong or expired code"),
    },
)
async def sign_credit(
    credit_id: uuid.UUID, body: CreditSignRequest, user: CurrentUserDep, credits: CreditServiceDep
) -> CreditRead:
    return CreditRead.model_validate(await credits.sign(user, credit_id, body.otp_code))


@router.post(
    "/{credit_id}/repay",
    summary="Repay from the credit account; closed when fully repaid → credit.closed",
    responses={
        **_NOT_FOUND,
        **_WRONG_STATE,
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
            "Insufficient funds or amount above what is owed"
        ),
    },
)
async def repay_credit(
    credit_id: uuid.UUID, body: CreditRepayRequest, user: CurrentUserDep, credits: CreditServiceDep
) -> CreditRepayResult:
    return await credits.repay(user, credit_id, body.amount, early=body.early)
