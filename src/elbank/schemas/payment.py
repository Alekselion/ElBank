# ruff: noqa: RUF001 - Russian descriptions and examples on purpose
"""Payment, provider catalog and template schemas.

``payload`` is provider-specific; it is validated against the provider's model
(:data:`PAYLOAD_MODELS`) and stored normalized. The models' JSON schemas are
published by ``GET /payments/providers``.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

from elbank.db.models import OperationStatus, PaymentOrderStatus, PaymentProvider
from elbank.schemas.account import Money, PositiveMoney
from elbank.schemas.transfer import Description, IdempotencyKey

# ---------- provider payloads ----------


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class UtilityPayload(_Payload):
    personal_account: Annotated[
        str, StringConstraints(pattern=r"^\d{6,20}$"), Field(description="Лицевой счёт")
    ]
    period: Annotated[
        str,
        StringConstraints(pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
        Field(description="Billing month, YYYY-MM", examples=["2026-10"]),
    ]
    service: Literal["housing", "electricity", "water", "gas", "heating"]


class MobilePayload(_Payload):
    phone: Annotated[
        str, StringConstraints(pattern=r"^\+7\d{10}$"), Field(examples=["+79991234567"])
    ]


class InternetPayload(_Payload):
    contract_number: Annotated[
        str, StringConstraints(pattern=r"^[A-Za-z0-9-]{4,32}$"), Field(examples=["INET-104233"])
    ]


class TaxPayload(_Payload):
    inn: Annotated[str, StringConstraints(pattern=r"^\d{12}$"), Field(description="ИНН физлица")]
    uin: Annotated[
        str,
        StringConstraints(pattern=r"^(\d{20}|\d{25})$"),
        Field(description="УИН (индекс документа) из налогового уведомления"),
    ]


class TrafficFinePayload(_Payload):
    uin: Annotated[
        str,
        StringConstraints(pattern=r"^188\d{17}(\d{5})?$"),
        Field(description="УИН постановления ГИБДД (начинается с 188)"),
    ]


class QrPayload(_Payload):
    qr: Annotated[
        str,
        StringConstraints(pattern=r"^ST0001[0-9]\|.+$", max_length=2000),
        Field(
            description="Payment QR code text, GOST R 56042-2014",
            examples=[
                "ST00012|Name=ООО Ромашка|PersonalAcc=40702810938000000001|"
                "BankName=ПАО Сбербанк|BIC=044525225|CorrespAcc=30101810400000000225"
            ],
        ),
    ]

    @model_validator(mode="after")
    def _check_required_fields(self) -> Self:
        fields = dict(part.split("=", 1) for part in self.qr.split("|")[1:] if "=" in part)
        missing = {"Name", "PersonalAcc", "BankName", "BIC", "CorrespAcc"} - fields.keys()
        if missing:
            raise ValueError(f"QR code lacks required fields: {', '.join(sorted(missing))}")
        return self


class ReceiptPayload(_Payload):
    receipt_number: Annotated[
        str,
        StringConstraints(pattern=r"^\d{10,25}$"),
        Field(description="Номер квитанции (штрихкод под QR)"),
    ]


PAYLOAD_MODELS: dict[PaymentProvider, type[_Payload]] = {
    PaymentProvider.UTILITY: UtilityPayload,
    PaymentProvider.MOBILE: MobilePayload,
    PaymentProvider.INTERNET: InternetPayload,
    PaymentProvider.TAX: TaxPayload,
    PaymentProvider.TRAFFIC_FINE: TrafficFinePayload,
    PaymentProvider.QR: QrPayload,
    PaymentProvider.RECEIPT: ReceiptPayload,
}


def normalize_payload(provider: PaymentProvider, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate ``payload`` for ``provider``; ``ValueError`` lists the problems."""
    try:
        return PAYLOAD_MODELS[provider].model_validate(payload).model_dump(mode="json")
    except ValidationError as exc:
        problems = "; ".join(
            f"payload.{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}" for e in exc.errors()
        )
        raise ValueError(f"invalid payload for provider {provider.value}: {problems}") from None


# ---------- payments ----------


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: uuid.UUID
    provider: PaymentProvider
    amount: PositiveMoney
    payload: dict[str, Any] = Field(description="Provider-specific details, see /providers")
    idempotency_key: IdempotencyKey

    @model_validator(mode="after")
    def _check_payload(self) -> Self:
        self.payload = normalize_payload(self.provider, self.payload)
        return self


class PaymentRead(BaseModel):
    id: uuid.UUID
    operation_id: uuid.UUID
    account_id: uuid.UUID
    provider: PaymentProvider
    amount: Money
    currency: str
    status: PaymentOrderStatus = Field(description="created → sent → completed / failed")
    operation_status: OperationStatus
    error_code: str | None = Field(
        description="EXTERNAL_REJECTED, EXTERNAL_TIMEOUT, INSUFFICIENT_FUNDS, ..."
    )
    payload: dict[str, Any]
    external_id: str | None = Field(description="Provider's transaction id")
    provider_response: dict[str, Any] | None = Field(description="Receipt or provider's error")
    created_at: datetime
    updated_at: datetime


class ProviderRead(BaseModel):
    code: PaymentProvider
    name: str
    description: str
    min_amount: Money
    max_amount: Money
    payload_schema: dict[str, Any] = Field(description="JSON schema of the payment payload")


# ---------- templates ----------


class TemplateWrite(BaseModel):
    """A payment template (``provider`` + ``payload``) or a transfer one (``recipient_account``)."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
    provider: PaymentProvider | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    recipient_account: Annotated[str, StringConstraints(pattern=r"^\d{20}$")] | None = None
    amount: PositiveMoney | None = Field(default=None, description="null: entered on each use")
    description: Description | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.provider is None and self.recipient_account is None:
            raise ValueError("either provider or recipient_account is required")
        if self.provider is not None:
            self.payload = normalize_payload(self.provider, self.payload)
        elif self.payload:
            raise ValueError("payload is only allowed together with provider")
        return self


class TemplateRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    provider: PaymentProvider | None
    payload: dict[str, Any]
    recipient_account: str | None
    amount: Money | None
    description: str | None
    created_at: datetime
    updated_at: datetime
