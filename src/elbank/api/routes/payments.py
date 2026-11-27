"""Payments to service providers (``/api/v1/payments``), provider catalog, templates."""

import uuid

from fastapi import APIRouter, Response, status

from elbank.api.deps import CurrentUserDep, PaymentServiceDep, TemplateServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.payment import (
    PAYLOAD_MODELS,
    PaymentCreateRequest,
    PaymentRead,
    ProviderRead,
    TemplateRead,
    TemplateWrite,
)
from elbank.services.payment_providers import PROVIDERS
from elbank.services.payments import PaymentRequest, PaymentView

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_TEMPLATE_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Template not found")}
_TEMPLATE_NAME_TAKEN: Responses = {
    status.HTTP_409_CONFLICT: error_response("A template with this name already exists")
}

router = APIRouter(tags=["payments"], responses=_AUTH_ERRORS)

# The catalog is static: build the response once.
_PROVIDERS = [
    ProviderRead(
        code=info.code,
        name=info.name,
        description=info.description,
        min_amount=info.min_amount,
        max_amount=info.max_amount,
        payload_schema=PAYLOAD_MODELS[info.code].model_json_schema(),
    )
    for info in PROVIDERS.values()
]


def _read(view: PaymentView) -> PaymentRead:
    order, op = view.order, view.operation
    return PaymentRead(
        id=order.id,
        operation_id=op.id,
        account_id=order.account_id,
        provider=order.provider,
        amount=op.amount,
        currency=op.currency,
        status=order.status,
        operation_status=op.status,
        error_code=op.error_code,
        payload=order.payload,
        external_id=order.external_id,
        provider_response=order.provider_response,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Pay a service provider",
    description=(
        "Returns **201** for a new payment and **200** for a retry with an already used "
        "`idempotency_key`. A payment refused by a check or by the provider is stored with "
        "`status=failed` and `error_code`. If the provider does not answer, the payment stays "
        "`sent` (`error_code=EXTERNAL_TIMEOUT`) and fails a few seconds later with a refund."
    ),
    responses={
        status.HTTP_200_OK: {"model": PaymentRead, "description": "Idempotent replay"},
        status.HTTP_404_NOT_FOUND: error_response("Account not found"),
        status.HTTP_409_CONFLICT: error_response("Same idempotency key is being processed now"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
            "Invalid payload or amount, idempotency key reused with other parameters"
        ),
    },
)
async def create_payment(
    body: PaymentCreateRequest,
    response: Response,
    user: CurrentUserDep,
    payments: PaymentServiceDep,
) -> PaymentRead:
    result = await payments.pay(
        PaymentRequest(
            user=user,
            account_id=body.account_id,
            provider=body.provider,
            amount=body.amount,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
        )
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return _read(result.payment)


# Static paths are declared before "/{payment_id}".
@router.get("/providers", summary="Service providers and their payload schemas")
async def list_providers(_: CurrentUserDep) -> list[ProviderRead]:
    return _PROVIDERS


# ---------- templates ----------


@router.get("/templates", summary="Saved templates of the current user")
async def list_templates(user: CurrentUserDep, templates: TemplateServiceDep) -> list[TemplateRead]:
    return [TemplateRead.model_validate(t) for t in await templates.list_for_user(user)]


@router.post(
    "/templates",
    status_code=status.HTTP_201_CREATED,
    summary="Save a template",
    responses=_TEMPLATE_NAME_TAKEN,
)
async def create_template(
    body: TemplateWrite, user: CurrentUserDep, templates: TemplateServiceDep
) -> TemplateRead:
    return TemplateRead.model_validate(await templates.create(user, body))


@router.get("/templates/{template_id}", summary="A template", responses=_TEMPLATE_NOT_FOUND)
async def get_template(
    template_id: uuid.UUID, user: CurrentUserDep, templates: TemplateServiceDep
) -> TemplateRead:
    return TemplateRead.model_validate(await templates.get_for_user(user, template_id))


@router.put(
    "/templates/{template_id}",
    summary="Replace a template",
    responses={**_TEMPLATE_NOT_FOUND, **_TEMPLATE_NAME_TAKEN},
)
async def update_template(
    template_id: uuid.UUID,
    body: TemplateWrite,
    user: CurrentUserDep,
    templates: TemplateServiceDep,
) -> TemplateRead:
    return TemplateRead.model_validate(await templates.update(user, template_id, body))


@router.delete(
    "/templates/{template_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a template",
    responses=_TEMPLATE_NOT_FOUND,
)
async def delete_template(
    template_id: uuid.UUID, user: CurrentUserDep, templates: TemplateServiceDep
) -> None:
    await templates.delete(user, template_id)


# ---------- payment status ----------


@router.get(
    "/{payment_id}",
    summary="Payment status",
    responses={status.HTTP_404_NOT_FOUND: error_response("Payment not found")},
)
async def get_payment(
    payment_id: uuid.UUID, user: CurrentUserDep, payments: PaymentServiceDep
) -> PaymentRead:
    return _read(await payments.get_for_user(user, payment_id))
