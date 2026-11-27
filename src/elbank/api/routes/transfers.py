"""Transfers (``/api/v1/transfers``): internal transfers, status, cancellation."""

import uuid

from fastapi import APIRouter, Response, status

from elbank.api.deps import CurrentUserDep, TransferServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.transfer import InternalTransferRequest, TransferRead
from elbank.services.transfers import InternalTransfer, TransferView

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Transfer not found")}

router = APIRouter(tags=["transfers"], responses=_AUTH_ERRORS)


def _read(view: TransferView) -> TransferRead:
    op = view.operation
    return TransferRead.model_validate(
        {
            "operation_id": op.id,
            "status": op.status,
            "from_account_id": op.account_id,
            "to_account_id": view.to_account_id,
            "amount": op.amount,
            "fee": view.fee,
            "currency": op.currency,
            "description": op.description,
            "error_code": op.error_code,
            "cancellable_until": view.cancellable_until,
            "created_at": op.created_at,
            "updated_at": op.updated_at,
        }
    )


@router.post(
    "/internal",
    status_code=status.HTTP_201_CREATED,
    summary="Transfer between accounts of the bank (own or another client's)",
    description=(
        "Returns **201** for a new transfer and **200** for a retry with an already used "
        "`idempotency_key`. A transfer that fails a business check (`INSUFFICIENT_FUNDS`, "
        "`DAILY_LIMIT_EXCEEDED`, ...) is stored with `status=failed` and `error_code`; "
        "retry it with a new key."
    ),
    responses={
        status.HTTP_200_OK: {"model": TransferRead, "description": "Idempotent replay"},
        status.HTTP_404_NOT_FOUND: error_response("Source or recipient account not found"),
        status.HTTP_409_CONFLICT: error_response("Same key is being processed right now"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
            "Invalid request, currency mismatch or idempotency key reused"
        ),
    },
)
async def create_internal_transfer(
    body: InternalTransferRequest,
    response: Response,
    user: CurrentUserDep,
    transfers: TransferServiceDep,
) -> TransferRead:
    result = await transfers.create_internal(
        InternalTransfer(
            user=user,
            from_account_id=body.from_account_id,
            to_account_id=body.to_account_id,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            idempotency_key=body.idempotency_key,
        )
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return _read(result.transfer)


@router.get("/{operation_id}", summary="Transfer status", responses=_NOT_FOUND)
async def get_transfer(
    operation_id: uuid.UUID, user: CurrentUserDep, transfers: TransferServiceDep
) -> TransferRead:
    return _read(await transfers.get_for_user(user, operation_id))


@router.post(
    "/{operation_id}/cancel",
    summary="Cancel a pending transfer within the cancel window (money is returned)",
    responses={
        **_NOT_FOUND,
        status.HTTP_409_CONFLICT: error_response(
            "Already completed/failed/cancelled, window expired or funds already spent"
        ),
    },
)
async def cancel_transfer(
    operation_id: uuid.UUID, user: CurrentUserDep, transfers: TransferServiceDep
) -> TransferRead:
    return _read(await transfers.cancel(user, operation_id))
