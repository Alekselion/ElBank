"""Outgoing transfers through external gateways: another bank by requisites, SBP by phone.

Flow (no DB transaction is held while the gateway is called):

1. Tx 1: the source account is locked, checked (status, balance, limits) and
   debited; the operation goes ``pending → processing``. A failed check stores
   the operation as ``failed`` and publishes ``transfer.failed``.
2. The gateway is called with ``Idempotency-Key: <operation id>``.
3. Tx 2, by the answer:

   ===================  ==========================================  ====================
   answer               operation                                   event
   ===================  ==========================================  ====================
   ``accepted``         ``completed`` + ``external_id``             ``transfer.completed``
   ``rejected``         ``failed`` / ``EXTERNAL_REJECTED``, refund  ``transfer.failed``
   error / 4xx / 5xx    ``failed`` / ``EXTERNAL_UNAVAILABLE``, refund ``transfer.failed``
   timeout              ``pending`` / ``EXTERNAL_TIMEOUT``          later, see below
   ===================  ==========================================  ====================

A timed-out transfer is resolved by :class:`ExternalTimeoutResolver` (transfers
worker) ``TRANSFERS_EXTERNAL_TIMEOUT_FAIL_AFTER_SECONDS`` later: ``failed``,
refund, ``transfer.failed``. It also fails transfers stuck in ``processing``
(the API process died during the call) after ``TRANSFERS_EXTERNAL_STALE_AFTER_SECONDS``.
A failed debit is refunded by restoring the balance: a failed operation does not
count in the ledger, so no compensating posting is needed.

Anti-fraud hold: after a successful check the money is debited, but the operation
stays ``pending`` (``FRAUD_REVIEW``) and the gateway is not called; the gateway request
is kept in the fraud case (``resume``). Confirmation moves it to ``processing`` and
continues from step 2 (:meth:`ExternalTransferService.continue_held`); rejection fails
it with ``FRAUD_REJECTED`` and refunds the money.
"""

import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.clients.external_transfer import (
    ExternalGatewayError,
    ExternalTimeoutError,
    GatewayResult,
    GatewayStatus,
)
from elbank.clients.interbank import BankParty, InterbankClient
from elbank.clients.sbp import SbpClient
from elbank.core.config import BankSettings, TransferSettings
from elbank.core.exceptions import IdempotencyKeyReusedError, NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Account, Currency, Operation, OperationStatus, OperationType, User
from elbank.kafka import EventPublisher, EventType
from elbank.services.account_cache import AccountListCache
from elbank.services.fraud_service import FraudService, held
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.ledger import OutgoingChecks, TransferErrorCode, lock_accounts
from elbank.services.operation_service import OperationService
from elbank.services.requisites import is_valid_account_number
from elbank.services.transfer_views import (
    CHANNEL_BY_TYPE,
    TransferResult,
    TransferView,
    load_view,
    transfer_payload,
)

logger = get_logger(__name__)

_EXTERNAL_TYPES = (OperationType.EXTERNAL_TRANSFER, OperationType.SBP_TRANSFER)

type GatewayCall = Callable[[Operation, Account], Awaitable[GatewayResult]]


@dataclass(frozen=True, slots=True)
class ExternalTransfer:
    user: User
    from_account_id: uuid.UUID
    bic: str
    account_number: str
    recipient_name: str
    amount: Decimal
    description: str | None
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class SbpTransfer:
    user: User
    from_account_id: uuid.UUID
    phone: str
    amount: Decimal
    description: str | None
    idempotency_key: str


def _view(operation: Operation) -> TransferView:
    return TransferView(operation, CHANNEL_BY_TYPE[operation.type], None, Decimal(0))


async def _refund(session: AsyncSession, operation: Operation) -> Account:
    account = (await lock_accounts(session, {operation.account_id}))[operation.account_id]
    account.balance += operation.amount
    return account


class ExternalTransferService:
    def __init__(
        self,
        session: AsyncSession,
        guard: IdempotencyGuard,
        checks: OutgoingChecks,
        *,
        interbank: InterbankClient,
        sbp: SbpClient,
        fraud: FraudService,
        cache: AccountListCache,
        publisher: EventPublisher,
        bank: BankSettings,
        settings: TransferSettings,
    ) -> None:
        self._session = session
        self._guard = guard
        self._checks = checks
        self._interbank = interbank
        self._sbp = sbp
        self._fraud = fraud
        self._cache = cache
        self._publisher = publisher
        self._bank = bank
        self._settings = settings
        self._ops = OperationService(session)

    async def send_external(self, request: ExternalTransfer) -> TransferResult:
        if request.bic == self._bank.bic:
            raise UnprocessableError("The recipient is a client of this bank: use /internal")
        if not is_valid_account_number(request.account_number, request.bic):
            raise UnprocessableError("account_number does not match the BIC (control key)")

        return await self._run(
            user=request.user,
            from_account_id=request.from_account_id,
            type_=OperationType.EXTERNAL_TRANSFER,
            amount=request.amount,
            counterparty=request.account_number,
            description=request.description,
            idempotency_key=request.idempotency_key,
            resume={
                "payer_name": request.user.full_name,
                "bic": request.bic,
                "account_number": request.account_number,
                "recipient_name": request.recipient_name,
                "purpose": request.description,
            },
        )

    async def send_sbp(self, request: SbpTransfer) -> TransferResult:
        return await self._run(
            user=request.user,
            from_account_id=request.from_account_id,
            type_=OperationType.SBP_TRANSFER,
            amount=request.amount,
            counterparty=request.phone,
            description=request.description,
            idempotency_key=request.idempotency_key,
            resume={"phone": request.phone, "purpose": request.description},
        )

    # ---------- anti-fraud decisions ----------

    def release_held(self, operation: Operation) -> None:
        """Confirmed (operation locked by the caller, who commits): ``processing``."""
        self._ops.mark_processing(operation)
        operation.error_code = None

    async def continue_held(self, operation: Operation, resume: dict[str, Any]) -> TransferView:
        """After the confirmation is committed: call the gateway and apply its answer."""
        source = await self._session.get(Account, operation.account_id)
        assert source is not None
        call = self._gateway_call(operation.type, resume)
        return await self._apply(operation.id, await self._call(call, operation, source))

    async def reject_held(self, operation: Operation) -> tuple[TransferView, list[Account]]:
        """Rejected (operation locked by the caller, who commits): failed, money returned."""
        self._ops.mark_failed(operation, TransferErrorCode.FRAUD_REJECTED)
        return _view(operation), [await _refund(self._session, operation)]

    async def publish_failed(self, view: TransferView) -> None:
        await self._publish_failed(view.operation)

    def _gateway_call(self, type_: OperationType, resume: dict[str, Any]) -> GatewayCall:
        """The gateway request of a transfer, built from what ``send_*`` received."""
        if type_ is OperationType.EXTERNAL_TRANSFER:

            async def interbank(operation: Operation, source: Account) -> GatewayResult:
                return await self._interbank.send(
                    transfer_id=operation.id,
                    payer=BankParty(self._bank.bic, source.number, resume["payer_name"]),
                    payee=BankParty(
                        resume["bic"], resume["account_number"], resume["recipient_name"]
                    ),
                    amount=operation.amount,
                    currency=operation.currency,
                    purpose=resume["purpose"],
                )

            return interbank

        async def sbp(operation: Operation, source: Account) -> GatewayResult:
            return await self._sbp.send(
                transfer_id=operation.id,
                payer_account=source.number,
                phone=resume["phone"],
                amount=operation.amount,
                purpose=resume["purpose"],
            )

        return sbp

    # ---------- internals ----------

    async def _run(
        self,
        *,
        user: User,
        from_account_id: uuid.UUID,
        type_: OperationType,
        amount: Decimal,
        counterparty: str,
        description: str | None,
        idempotency_key: str,
        resume: dict[str, Any],
    ) -> TransferResult:
        async def replay(existing: Operation) -> TransferResult:
            same = (
                existing.type is type_
                and existing.account_id == from_account_id
                and existing.counterparty_account == counterparty
                and existing.amount == amount
            )
            if not same:
                raise IdempotencyKeyReusedError
            return TransferResult(await self._load(existing), created=False)

        async def execute() -> TransferResult:
            operation, source = await self._debit(
                user,
                from_account_id=from_account_id,
                type_=type_,
                amount=amount,
                counterparty=counterparty,
                description=description,
                idempotency_key=idempotency_key,
                resume=resume,
            )
            if operation.status is OperationStatus.FAILED or held(operation):
                return TransferResult(_view(operation), created=True)
            outcome = await self._call(self._gateway_call(type_, resume), operation, source)
            return TransferResult(await self._apply(operation.id, outcome), created=True)

        return await self._guard.run(user.id, idempotency_key, execute=execute, replay=replay)

    async def _debit(
        self,
        user: User,
        *,
        from_account_id: uuid.UUID,
        type_: OperationType,
        amount: Decimal,
        counterparty: str,
        description: str | None,
        idempotency_key: str,
        resume: dict[str, Any],
    ) -> tuple[Operation, Account]:
        """Tx 1: checks and debit; the operation is ``processing`` (``failed``, or held)."""
        source = (await lock_accounts(self._session, {from_account_id})).get(from_account_id)
        if source is None or source.user_id != user.id:
            raise NotFoundError("Account not found")
        if source.currency != Currency.RUB:
            raise UnprocessableError("External transfers are made from RUB accounts only")

        error = await self._checks.check(source, amount, Decimal(0))
        operation = self._ops.create_pending(
            account_id=source.id,
            user_id=user.id,
            type=type_,
            amount=amount,
            currency=source.currency,
            counterparty_account=counterparty,
            description=description,
            idempotency_key=idempotency_key,
        )
        if error is not None:
            self._ops.mark_failed(operation, error)
            await self._session.commit()
            await self._publish_failed(operation)
            return operation, source

        hits = await self._fraud.evaluate(source, amount, Decimal(0), type_)
        source.balance -= amount
        if hits:
            hold = await self._fraud.hold(operation, user, hits, resume)
            await self._session.commit()
            await self._cache.invalidate(user.id)
            await self._fraud.announce(hold)
            return operation, source
        self._ops.mark_processing(operation)
        await self._session.commit()
        await self._cache.invalidate(user.id)
        logger.info("external transfer sent", operation_id=str(operation.id), type=type_.value)
        return operation, source

    @staticmethod
    async def _call(
        call: GatewayCall, operation: Operation, source: Account
    ) -> GatewayResult | TransferErrorCode:
        try:
            return await call(operation, source)
        except ExternalTimeoutError:
            return TransferErrorCode.EXTERNAL_TIMEOUT
        except ExternalGatewayError:
            return TransferErrorCode.EXTERNAL_UNAVAILABLE

    async def _apply(
        self, operation_id: uuid.UUID, outcome: GatewayResult | TransferErrorCode
    ) -> TransferView:
        """Tx 2: apply the gateway answer to the (re-locked) operation."""
        operation = await self._session.scalar(
            select(Operation)
            .where(Operation.id == operation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert operation is not None
        if operation.status is not OperationStatus.PROCESSING:
            # Resolved meanwhile as stale (the call took longer than the stale timeout).
            logger.error(
                "gateway answer for an already resolved transfer ignored",
                operation_id=str(operation.id),
                status=operation.status.value,
                outcome=str(outcome),
            )
            await self._session.rollback()
            return await self._load(operation)

        if outcome is TransferErrorCode.EXTERNAL_TIMEOUT:
            self._ops.mark_pending(operation, outcome)
            await self._session.commit()
            logger.warning("external transfer unconfirmed", operation_id=str(operation.id))
            return _view(operation)

        reason: str | None = None
        if isinstance(outcome, GatewayResult):
            operation.external_id = outcome.external_id
            reason = outcome.reason
            if outcome.status is GatewayStatus.ACCEPTED:
                self._ops.mark_completed(operation)
                await self._session.commit()
                logger.info("external transfer completed", operation_id=str(operation.id))
                await self._publisher.publish_safely(
                    EventType.TRANSFER_COMPLETED,
                    transfer_payload(_view(operation)),
                    key=str(operation.account_id),
                )
                return _view(operation)
            outcome = TransferErrorCode.EXTERNAL_REJECTED

        self._ops.mark_failed(operation, outcome)
        account = await _refund(self._session, operation)
        await self._session.commit()
        await self._cache.invalidate(account.user_id)
        logger.info(
            "external transfer failed",
            operation_id=str(operation.id),
            error_code=outcome.value,
            reason=reason,
        )
        await self._publish_failed(operation, reason)
        return _view(operation)

    async def _publish_failed(self, operation: Operation, reason: str | None = None) -> None:
        await self._publisher.publish_safely(
            EventType.TRANSFER_FAILED,
            transfer_payload(_view(operation)) | {"reason": reason},
            key=str(operation.account_id),
        )

    async def _load(self, operation: Operation) -> TransferView:
        return await load_view(
            self._session, operation, cancel_window_seconds=self._settings.cancel_window_seconds
        )


async def lock_unconfirmed(
    session: AsyncSession, types: Sequence[OperationType], settings: TransferSettings
) -> list[Operation]:
    """Lock (``SKIP LOCKED``) operations of ``types`` that must be failed now: unconfirmed
    after a timeout for ``external_timeout_fail_after_seconds``, or stuck in
    ``processing`` for ``external_stale_after_seconds``."""
    now = datetime.now(UTC)
    timed_out_before = now - timedelta(seconds=settings.external_timeout_fail_after_seconds)
    stale_before = now - timedelta(seconds=settings.external_stale_after_seconds)
    rows = await session.scalars(
        select(Operation)
        .where(
            Operation.type.in_(types),
            or_(
                and_(
                    Operation.status == OperationStatus.PENDING,
                    Operation.error_code == TransferErrorCode.EXTERNAL_TIMEOUT,
                    Operation.updated_at <= timed_out_before,
                ),
                and_(
                    Operation.status == OperationStatus.PROCESSING,
                    Operation.updated_at <= stale_before,
                ),
            ),
        )
        .order_by(Operation.updated_at)
        .limit(settings.finalizer_batch_size)
        .with_for_update(skip_locked=True)
    )
    return list(rows)


class ExternalTimeoutResolver:
    """Fails unconfirmed external transfers (timed out / stuck) and refunds the money."""

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
            operations = await lock_unconfirmed(session, _EXTERNAL_TYPES, self._settings)
            if not operations:
                return 0
            ops = OperationService(session)
            accounts = await lock_accounts(session, {o.account_id for o in operations})
            for operation in operations:
                ops.mark_failed(operation, TransferErrorCode.EXTERNAL_TIMEOUT)
                accounts[operation.account_id].balance += operation.amount
            await session.commit()

        for user_id in {a.user_id for a in accounts.values()}:
            await self._cache.invalidate(user_id)
        for operation in operations:
            logger.warning("external transfer failed by timeout", operation_id=str(operation.id))
            await self._publisher.publish_safely(
                EventType.TRANSFER_FAILED,
                transfer_payload(_view(operation)),
                key=str(operation.account_id),
            )
        return len(operations)
