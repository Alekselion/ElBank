"""Payments to service providers and payment templates.

A payment is an ``Operation`` (type ``payment``, the debit; its status and
``error_code`` are authoritative) plus a ``PaymentOrder`` (provider, payload,
provider's answer). Flow — the same as for external transfers, no DB
transaction is held while the provider is called:

1. Tx 1: the account is locked, checked (status, balance, limits) and debited;
   operation ``pending → processing``, order ``created → sent``. A failed check
   stores both as failed and publishes ``payment.failed``.
2. ``POST /providers/{name}/pay`` (5xx/timeouts retried, 4xx not).
3. Tx 2, by the answer:

   ================  ==============================================  =====================
   answer            operation / order                               event
   ================  ==============================================  =====================
   accepted          ``completed`` / ``completed`` + transaction id  ``payment.processed``
   error (4xx)       ``failed`` ``EXTERNAL_REJECTED``, refund        ``payment.failed``
   5xx, garbage      ``failed`` ``EXTERNAL_UNAVAILABLE``, refund     ``payment.failed``
   timeout           ``pending`` ``EXTERNAL_TIMEOUT`` / ``sent``     later, see below
   ================  ==============================================  =====================

An unconfirmed payment is failed (with a refund and ``payment.failed``) by
:class:`PaymentTimeoutResolver` in the transfers worker, like external transfers.
"""

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.clients.external_transfer import ExternalGatewayError, ExternalTimeoutError
from elbank.clients.providers import ProviderClient, ProviderResponse, ProviderStatus
from elbank.core.config import TransferSettings
from elbank.core.exceptions import (
    ConflictError,
    IdempotencyKeyReusedError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import (
    Currency,
    Operation,
    OperationStatus,
    OperationType,
    PaymentOrder,
    PaymentOrderStatus,
    PaymentProvider,
    TransferTemplate,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.payment import TemplateWrite
from elbank.services.account_cache import AccountListCache
from elbank.services.external_transfers import lock_unconfirmed
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.ledger import OutgoingChecks, TransferErrorCode, lock_accounts
from elbank.services.operation_service import OperationService
from elbank.services.payment_providers import PROVIDERS

logger = get_logger(__name__)

_ORDER_TRANSITIONS: dict[PaymentOrderStatus, frozenset[PaymentOrderStatus]] = {
    PaymentOrderStatus.CREATED: frozenset({PaymentOrderStatus.SENT, PaymentOrderStatus.FAILED}),
    PaymentOrderStatus.SENT: frozenset({PaymentOrderStatus.COMPLETED, PaymentOrderStatus.FAILED}),
    PaymentOrderStatus.COMPLETED: frozenset(),
    PaymentOrderStatus.FAILED: frozenset(),
    PaymentOrderStatus.CANCELLED: frozenset(),
}
_TEMPLATE_NAME_CONSTRAINT = "uq_transfer_templates_user_id_name"


def _set_order_status(order: PaymentOrder, status: PaymentOrderStatus) -> None:
    if status not in _ORDER_TRANSITIONS[order.status]:
        raise RuntimeError(f"Payment order {order.id}: {order.status} -> {status} is not allowed")
    order.status = status


@dataclass(frozen=True, slots=True)
class PaymentRequest:
    user: User
    account_id: uuid.UUID
    provider: PaymentProvider
    amount: Decimal
    payload: dict[str, Any]
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class PaymentView:
    order: PaymentOrder
    operation: Operation


@dataclass(frozen=True, slots=True)
class PaymentResult:
    payment: PaymentView
    created: bool  # False: the idempotency key was seen before, stored result returned


def payment_payload(view: PaymentView, reason: str | None = None) -> dict[str, Any]:
    """Payload of ``payment.processed`` / ``payment.failed``."""
    order, op = view.order, view.operation
    return {
        "payment_id": str(order.id),
        "operation_id": str(op.id),
        "user_id": str(op.user_id),
        "account_id": str(order.account_id),
        "provider": order.provider.value,
        "amount": format(op.amount, "f"),
        "currency": op.currency,
        "status": order.status.value,
        "external_id": order.external_id,
        "error_code": op.error_code,
        "reason": reason,
    }


async def _lock_order(session: AsyncSession, operation_id: uuid.UUID) -> PaymentOrder:
    order = await session.scalar(
        select(PaymentOrder)
        .where(PaymentOrder.operation_id == operation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if order is None:
        raise RuntimeError(f"Payment order of operation {operation_id} is missing")
    return order


class PaymentService:
    def __init__(
        self,
        session: AsyncSession,
        guard: IdempotencyGuard,
        checks: OutgoingChecks,
        *,
        providers: ProviderClient,
        cache: AccountListCache,
        publisher: EventPublisher,
    ) -> None:
        self._session = session
        self._guard = guard
        self._checks = checks
        self._providers = providers
        self._cache = cache
        self._publisher = publisher
        self._ops = OperationService(session)

    async def pay(self, request: PaymentRequest) -> PaymentResult:
        info = PROVIDERS[request.provider]
        if not info.min_amount <= request.amount <= info.max_amount:
            raise UnprocessableError(
                f"Amount for {info.name} must be within {info.min_amount}..{info.max_amount}"
            )

        async def replay(existing: Operation) -> PaymentResult:
            if existing.type is not OperationType.PAYMENT:
                raise IdempotencyKeyReusedError
            view = await self._view_of(existing)
            same = (
                view.order.account_id == request.account_id
                and view.order.provider is request.provider
                and view.order.payload == request.payload
                and existing.amount == request.amount
            )
            if not same:
                raise IdempotencyKeyReusedError
            return PaymentResult(view, created=False)

        async def execute() -> PaymentResult:
            view, payer_account = await self._debit(request)
            if view.order.status is PaymentOrderStatus.FAILED:
                return PaymentResult(view, created=True)
            outcome = await self._call(view, request, payer_account)
            return PaymentResult(await self._apply(view.operation.id, outcome), created=True)

        return await self._guard.run(
            request.user.id, request.idempotency_key, execute=execute, replay=replay
        )

    async def get_for_user(self, user: User, payment_id: uuid.UUID) -> PaymentView:
        order = await self._session.get(PaymentOrder, payment_id)
        operation = await self._session.get(Operation, order.operation_id) if order else None
        if order is None or operation is None or operation.user_id != user.id:
            raise NotFoundError("Payment not found")
        return PaymentView(order, operation)

    # ---------- internals ----------

    async def _debit(self, request: PaymentRequest) -> tuple[PaymentView, str]:
        """Tx 1: checks and debit; the operation is ``processing`` (or ``failed``).

        Returns the view and the payer's account number.
        """
        account = (await lock_accounts(self._session, {request.account_id})).get(request.account_id)
        if account is None or account.user_id != request.user.id:
            raise NotFoundError("Account not found")
        if account.currency != Currency.RUB:
            raise UnprocessableError("Payments are made from RUB accounts only")

        error = await self._checks.check(account, request.amount, Decimal(0))
        operation = self._ops.create_pending(
            account_id=account.id,
            user_id=request.user.id,
            type=OperationType.PAYMENT,
            amount=request.amount,
            currency=account.currency,
            description=PROVIDERS[request.provider].name,
            idempotency_key=request.idempotency_key,
        )
        order = PaymentOrder(
            id=uuid.uuid4(),
            account_id=account.id,
            operation_id=operation.id,
            provider=request.provider,
            payload=request.payload,
            status=PaymentOrderStatus.CREATED,
        )
        self._session.add(order)
        view = PaymentView(order, operation)

        if error is not None:
            self._ops.mark_failed(operation, error)
            _set_order_status(order, PaymentOrderStatus.FAILED)
            await self._session.commit()
            logger.info("payment failed", payment_id=str(order.id), error_code=error.value)
            await self._publish(EventType.PAYMENT_FAILED, view)
            return view, account.number

        account.balance -= request.amount
        self._ops.mark_processing(operation)
        _set_order_status(order, PaymentOrderStatus.SENT)
        await self._session.commit()
        await self._cache.invalidate(request.user.id)
        logger.info("payment sent", payment_id=str(order.id), provider=request.provider.value)
        return view, account.number

    async def _call(
        self, view: PaymentView, request: PaymentRequest, payer_account: str
    ) -> ProviderResponse | TransferErrorCode:
        try:
            return await self._providers.pay(
                request.provider,
                payment_id=view.order.id,
                payer_account=payer_account,
                amount=request.amount,
                payload=request.payload,
            )
        except ExternalTimeoutError:
            return TransferErrorCode.EXTERNAL_TIMEOUT
        except ExternalGatewayError:
            return TransferErrorCode.EXTERNAL_UNAVAILABLE

    async def _apply(
        self, operation_id: uuid.UUID, outcome: ProviderResponse | TransferErrorCode
    ) -> PaymentView:
        """Tx 2: apply the provider's answer (operation first, then order and account)."""
        operation = await self._session.scalar(
            select(Operation)
            .where(Operation.id == operation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert operation is not None
        order = await _lock_order(self._session, operation_id)
        view = PaymentView(order, operation)
        if operation.status is not OperationStatus.PROCESSING:
            logger.error(
                "provider answer for an already resolved payment ignored",
                payment_id=str(order.id),
                status=operation.status.value,
                outcome=str(outcome),
            )
            await self._session.rollback()
            return view

        if outcome is TransferErrorCode.EXTERNAL_TIMEOUT:
            self._ops.mark_pending(operation, outcome)
            await self._session.commit()
            logger.warning("payment unconfirmed", payment_id=str(order.id))
            return view

        reason: str | None = None
        if isinstance(outcome, ProviderResponse):
            order.provider_response = outcome.raw()
            reason = outcome.message
            if outcome.status is ProviderStatus.ACCEPTED:
                order.external_id = operation.external_id = outcome.transaction_id
                self._ops.mark_completed(operation)
                _set_order_status(order, PaymentOrderStatus.COMPLETED)
                await self._session.commit()
                logger.info("payment processed", payment_id=str(order.id))
                await self._publish(EventType.PAYMENT_PROCESSED, view)
                return view
            outcome = TransferErrorCode.EXTERNAL_REJECTED

        self._ops.mark_failed(operation, outcome)
        _set_order_status(order, PaymentOrderStatus.FAILED)
        account = (await lock_accounts(self._session, {operation.account_id}))[operation.account_id]
        account.balance += operation.amount
        await self._session.commit()
        await self._cache.invalidate(account.user_id)
        logger.info(
            "payment failed", payment_id=str(order.id), error_code=outcome.value, reason=reason
        )
        await self._publish(EventType.PAYMENT_FAILED, view, reason)
        return view

    async def _view_of(self, operation: Operation) -> PaymentView:
        order = await self._session.scalar(
            select(PaymentOrder).where(PaymentOrder.operation_id == operation.id)
        )
        if order is None:
            raise RuntimeError(f"Payment order of operation {operation.id} is missing")
        return PaymentView(order, operation)

    async def _publish(
        self, event: EventType, view: PaymentView, reason: str | None = None
    ) -> None:
        await self._publisher.publish_safely(
            event, payment_payload(view, reason), key=str(view.order.account_id)
        )


class PaymentTimeoutResolver:
    """Fails unconfirmed payments (timed out / stuck) and refunds the money."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: TransferSettings,
    ) -> None:
        self._session_factory = session_factory
        self._cache = cache
        self._publisher = publisher
        self._settings = settings

    async def run_once(self) -> int:
        async with self._session_factory() as session:
            operations = await lock_unconfirmed(session, (OperationType.PAYMENT,), self._settings)
            if not operations:
                return 0
            ops = OperationService(session)
            views: list[PaymentView] = []
            for operation in operations:
                order = await _lock_order(session, operation.id)
                ops.mark_failed(operation, TransferErrorCode.EXTERNAL_TIMEOUT)
                _set_order_status(order, PaymentOrderStatus.FAILED)
                views.append(PaymentView(order, operation))
            accounts = await lock_accounts(session, {o.account_id for o in operations})
            for operation in operations:
                accounts[operation.account_id].balance += operation.amount
            await session.commit()

        for user_id in {a.user_id for a in accounts.values()}:
            await self._cache.invalidate(user_id)
        for view in views:
            logger.warning("payment failed by timeout", payment_id=str(view.order.id))
            await self._publisher.publish_safely(
                EventType.PAYMENT_FAILED,
                payment_payload(view, "Provider did not confirm the payment"),
                key=str(view.order.account_id),
            )
        return len(views)


class TemplateService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_for_user(self, user: User) -> list[TransferTemplate]:
        rows = await self._session.scalars(
            select(TransferTemplate)
            .where(TransferTemplate.user_id == user.id)
            .order_by(TransferTemplate.name)
        )
        return list(rows)

    async def get_for_user(self, user: User, template_id: uuid.UUID) -> TransferTemplate:
        template = await self._session.get(TransferTemplate, template_id)
        if template is None or template.user_id != user.id:
            raise NotFoundError("Template not found")
        return template

    async def create(self, user: User, data: TemplateWrite) -> TransferTemplate:
        template = TransferTemplate(id=uuid.uuid4(), user_id=user.id, **data.model_dump())
        self._session.add(template)
        await self._commit()
        logger.info("template created", template_id=str(template.id))
        return template

    async def update(
        self, user: User, template_id: uuid.UUID, data: TemplateWrite
    ) -> TransferTemplate:
        template = await self.get_for_user(user, template_id)
        for field, value in data.model_dump().items():
            setattr(template, field, value)
        await self._commit()
        return template

    async def delete(self, user: User, template_id: uuid.UUID) -> None:
        template = await self.get_for_user(user, template_id)
        await self._session.delete(template)
        await self._session.commit()
        logger.info("template deleted", template_id=str(template_id))

    async def _commit(self) -> None:
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if _TEMPLATE_NAME_CONSTRAINT in str(exc.orig):
                raise ConflictError("A template with this name already exists") from exc
            raise
