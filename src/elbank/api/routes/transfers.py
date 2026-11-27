"""Transfers (``/api/v1/transfers``): internal, external (interbank), SBP, conversion, rates."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, Query, Response, status

from elbank.api.deps import (
    ConversionServiceDep,
    CurrentUserDep,
    ExternalTransferServiceDep,
    RatesServiceDep,
    TransferServiceDep,
)
from elbank.api.openapi import Responses, error_response
from elbank.db.models import Currency
from elbank.schemas.transfer import (
    ConversionRequest,
    ExchangeRateRead,
    ExternalTransferRequest,
    IdempotencyKey,
    InternalTransferRequest,
    SbpTransferRequest,
    TransferRead,
)
from elbank.services.conversion import Conversion
from elbank.services.external_transfers import ExternalTransfer, SbpTransfer
from elbank.services.transfer_views import TransferResult, TransferView
from elbank.services.transfers import InternalTransfer

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Transfer not found")}
_CREATE_RESPONSES: Responses = {
    status.HTTP_200_OK: {"model": TransferRead, "description": "Idempotent replay"},
    status.HTTP_404_NOT_FOUND: error_response("Source or recipient account not found"),
    status.HTTP_409_CONFLICT: error_response("Same idempotency key is being processed now"),
    status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
        "Invalid request or idempotency key reused with other parameters"
    ),
}
_CREATE_DESCRIPTION = (
    "Returns **201** for a new transfer and **200** for a retry with an already used "
    "idempotency key. A transfer that fails a business check (`INSUFFICIENT_FUNDS`, "
    "`DAILY_LIMIT_EXCEEDED`, ...) or is rejected downstream is stored with `status=failed` "
    "and `error_code`; retry it with a new key."
)

router = APIRouter(tags=["transfers"], responses=_AUTH_ERRORS)


def _read(view: TransferView) -> TransferRead:
    op = view.operation
    return TransferRead.model_validate(
        {
            "operation_id": op.id,
            "type": view.channel,
            "status": op.status,
            "from_account_id": op.account_id,
            "to_account_id": view.to_account_id,
            "counterparty": op.counterparty_account,
            "amount": op.amount,
            "fee": view.fee,
            "currency": op.currency,
            "converted_amount": view.converted_amount,
            "exchange_rate": op.exchange_rate,
            "description": op.description,
            "error_code": op.error_code,
            "external_id": op.external_id,
            "cancellable_until": view.cancellable_until,
            "created_at": op.created_at,
            "updated_at": op.updated_at,
        }
    )


def _respond(result: TransferResult, response: Response) -> TransferRead:
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return _read(result.transfer)


@router.post(
    "/internal",
    status_code=status.HTTP_201_CREATED,
    summary="Transfer between accounts of the bank (own or another client's)",
    description=_CREATE_DESCRIPTION,
    responses=_CREATE_RESPONSES,
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
    return _respond(result, response)


@router.post(
    "/external",
    status_code=status.HTTP_201_CREATED,
    summary="Transfer in RUB to another bank by requisites",
    description=_CREATE_DESCRIPTION
    + " If the interbank gateway times out, the transfer stays `pending` with "
    "`error_code=EXTERNAL_TIMEOUT` and fails a few seconds later (money is returned).",
    responses=_CREATE_RESPONSES,
)
async def create_external_transfer(
    body: ExternalTransferRequest,
    response: Response,
    user: CurrentUserDep,
    transfers: ExternalTransferServiceDep,
) -> TransferRead:
    result = await transfers.send_external(
        ExternalTransfer(
            user=user,
            from_account_id=body.from_account_id,
            bic=body.bic,
            account_number=body.account_number,
            recipient_name=body.recipient_name,
            amount=body.amount,
            description=body.description,
            idempotency_key=body.idempotency_key,
        )
    )
    return _respond(result, response)


@router.post(
    "/sbp",
    status_code=status.HTTP_201_CREATED,
    summary="Transfer in RUB by phone number (Faster Payments System)",
    description=_CREATE_DESCRIPTION
    + " The idempotency key is passed in the `Idempotency-Key` header.",
    responses=_CREATE_RESPONSES,
)
async def create_sbp_transfer(
    body: SbpTransferRequest,
    response: Response,
    user: CurrentUserDep,
    transfers: ExternalTransferServiceDep,
    idempotency_key: Annotated[IdempotencyKey, Header(alias="Idempotency-Key")],
) -> TransferRead:
    result = await transfers.send_sbp(
        SbpTransfer(
            user=user,
            from_account_id=body.from_account_id,
            phone=body.phone,
            amount=body.amount,
            description=body.description,
            idempotency_key=idempotency_key,
        )
    )
    return _respond(result, response)


@router.post(
    "/convert",
    status_code=status.HTTP_201_CREATED,
    summary="Currency exchange between own accounts (Bank of Russia rate + 1% fee)",
    description=_CREATE_DESCRIPTION,
    responses={
        **_CREATE_RESPONSES,
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Exchange rates are unavailable"),
    },
)
async def convert(
    body: ConversionRequest,
    response: Response,
    user: CurrentUserDep,
    conversions: ConversionServiceDep,
) -> TransferRead:
    result = await conversions.convert(
        Conversion(
            user=user,
            from_account_id=body.from_account_id,
            to_account_id=body.to_account_id,
            amount=body.amount,
            idempotency_key=body.idempotency_key,
        )
    )
    return _respond(result, response)


# Declared before "/{operation_id}" so "rates" is not parsed as an operation id.
@router.get(
    "/rates",
    summary="Bank of Russia exchange rate (cached for 60 s)",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Same base and quote"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Exchange rates are unavailable"),
    },
)
async def get_rate(
    _: CurrentUserDep,
    rates: RatesServiceDep,
    base: Annotated[Currency, Query()] = Currency.RUB,
    quote: Annotated[Currency, Query()] = Currency.USD,
) -> ExchangeRateRead:
    rate = await rates.get_rate(base, quote)
    return ExchangeRateRead(base=rate.base, quote=rate.quote, rate=rate.rate, date=rate.date)


@router.get("/{operation_id}", summary="Transfer status (any type)", responses=_NOT_FOUND)
async def get_transfer(
    operation_id: uuid.UUID, user: CurrentUserDep, transfers: TransferServiceDep
) -> TransferRead:
    return _read(await transfers.get_for_user(user, operation_id))


@router.post(
    "/{operation_id}/cancel",
    summary="Cancel a pending internal transfer within the cancel window (money is returned)",
    responses={
        **_NOT_FOUND,
        status.HTTP_409_CONFLICT: error_response(
            "Not internal, already completed/failed/cancelled, window expired or funds spent"
        ),
    },
)
async def cancel_transfer(
    operation_id: uuid.UUID, user: CurrentUserDep, transfers: TransferServiceDep
) -> TransferRead:
    return _read(await transfers.cancel(user, operation_id))
