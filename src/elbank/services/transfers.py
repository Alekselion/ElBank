"""Internal transfers between accounts of the bank; status of transfers of any channel.

Lifecycle of an internal transfer (all status changes go through :class:`OperationService`):

1. ``POST /transfers/internal`` — one DB transaction with both accounts locked
   (``SELECT ... FOR UPDATE``): checks, debit of ``amount + fee``, credit of
   ``amount`` and ``pending`` operations ``transfer_out`` (the transfer itself),
   ``transfer_in`` and ``fee``. A failed check stores the ``transfer_out`` as
   ``failed`` with an ``error_code`` and publishes ``transfer.failed``.
2. During ``TRANSFERS_CANCEL_WINDOW_SECONDS`` the transfer can be cancelled:
   compensating operations return the money, the original ones become
   ``cancelled``, ``transfer.reversed`` is published.
3. Then :class:`TransferFinalizer` (the transfers worker) moves the operations
   ``pending → processing → completed`` and publishes ``transfer.completed``.

Idempotency: see :mod:`elbank.services.idempotency`.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import TransferSettings
from elbank.core.exceptions import (
    IdempotencyKeyReusedError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    Currency,
    Operation,
    OperationStatus,
    OperationType,
    TariffCode,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.transfer import TransferChannel
from elbank.services.account_cache import AccountListCache
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.ledger import (
    OutgoingChecks,
    child_postings,
    lock_accounts,
)
from elbank.services.operation_service import CANCELLABLE_STATUSES, OperationService
from elbank.services.tariffs import TariffService
from elbank.services.transfer_views import (
    TransferResult,
    TransferView,
    get_user_transfer,
    load_view,
    transfer_payload,
)

logger = get_logger(__name__)

# Credit postings increase the balance; their reversal is a debit and vice versa.
_CREDIT_TYPES = frozenset({OperationType.TRANSFER_IN})
_REVERSAL_TYPE: dict[OperationType, OperationType] = {
    OperationType.TRANSFER_OUT: OperationType.TRANSFER_IN,
    OperationType.TRANSFER_IN: OperationType.TRANSFER_OUT,
    OperationType.FEE: OperationType.TRANSFER_IN,  # fee refund
}


@dataclass(frozen=True, slots=True)
class InternalTransfer:
    user: User
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    idempotency_key: str


class TransferService:
    def __init__(
        self,
        session: AsyncSession,
        guard: IdempotencyGuard,
        checks: OutgoingChecks,
        *,
        tariffs: TariffService,
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: TransferSettings,
    ) -> None:
        self._session = session
        self._guard = guard
        self._checks = checks
        self._tariffs = tariffs
        self._cache = cache
        self._publisher = publisher
        self._settings = settings
        self._ops = OperationService(session)

    # ---------- create ----------

    async def create_internal(self, request: InternalTransfer) -> TransferResult:
        async def replay(existing: Operation) -> TransferResult:
            view = await self._load(existing)
            same = (
                existing.type is OperationType.TRANSFER_OUT
                and existing.account_id == request.from_account_id
                and view.to_account_id == request.to_account_id
                and existing.amount == request.amount
                and existing.currency == request.currency
            )
            if not same:
                raise IdempotencyKeyReusedError
            return TransferResult(view, created=False)

        return await self._guard.run(
            request.user.id,
            request.idempotency_key,
            execute=lambda: self._execute(request),
            replay=replay,
        )

    async def _execute(self, request: InternalTransfer) -> TransferResult:
        accounts = await lock_accounts(
            self._session, {request.from_account_id, request.to_account_id}
        )
        source = accounts.get(request.from_account_id)
        target = accounts.get(request.to_account_id)
        if source is None or source.user_id != request.user.id:
            raise NotFoundError("Account not found")
        if target is None:
            raise NotFoundError("Recipient account not found")
        if source.currency != request.currency or target.currency != request.currency:
            raise UnprocessableError("Transfer currency must match both accounts' currency")

        fee = (await self._tariffs.fee_rule(TariffCode.INTERNAL_TRANSFER)).fee(request.amount)
        error = await self._checks.check(
            source, request.amount, fee, operation=OperationType.TRANSFER_OUT, target=target
        )
        transfer = self._ops.create_pending(
            account_id=source.id,
            user_id=request.user.id,
            type=OperationType.TRANSFER_OUT,
            amount=request.amount,
            currency=source.currency,
            counterparty_account=target.number,
            description=request.description,
            idempotency_key=request.idempotency_key,
        )

        if error is not None:
            self._ops.mark_failed(transfer, error)
            await self._session.commit()
            view = TransferView(transfer, TransferChannel.INTERNAL, target.id, Decimal(0))
            logger.info("transfer failed", operation_id=str(transfer.id), error_code=error.value)
            await self._publisher.publish_safely(
                EventType.TRANSFER_FAILED, transfer_payload(view), key=str(source.id)
            )
            return TransferResult(view, created=True)

        source.balance -= request.amount + fee
        target.balance += request.amount
        self._ops.create_pending(
            account_id=target.id,
            type=OperationType.TRANSFER_IN,
            amount=request.amount,
            currency=target.currency,
            parent_operation_id=transfer.id,
            counterparty_account=source.number,
            description=request.description,
        )
        if fee > 0:
            self._ops.create_pending(
                account_id=source.id,
                type=OperationType.FEE,
                amount=fee,
                currency=source.currency,
                parent_operation_id=transfer.id,
                description="Internal transfer fee",
            )
        await self._session.commit()
        await self._invalidate(source, target)

        logger.info(
            "transfer accepted",
            operation_id=str(transfer.id),
            from_account_id=str(source.id),
            to_account_id=str(target.id),
            amount=str(request.amount),
            fee=str(fee),
        )
        view = TransferView(
            transfer,
            TransferChannel.INTERNAL,
            target.id,
            fee,
            cancellable_until=transfer.created_at + self._window,
        )
        return TransferResult(view, created=True)

    # ---------- read (any channel) ----------

    async def get_for_user(self, user: User, operation_id: uuid.UUID) -> TransferView:
        return await self._load(await get_user_transfer(self._session, user, operation_id))

    # ---------- cancel (internal only) ----------

    async def cancel(self, user: User, operation_id: uuid.UUID) -> TransferView:
        transfer = await get_user_transfer(self._session, user, operation_id, for_update=True)
        if transfer.type is not OperationType.TRANSFER_OUT:
            raise InvalidStateError("Only internal transfers can be cancelled")
        if transfer.status not in CANCELLABLE_STATUSES:
            raise InvalidStateError(f"Transfer is already {transfer.status.value}")
        if datetime.now(UTC) > transfer.created_at + self._window:
            # Not finalized yet only because the worker lags behind: still too late.
            raise InvalidStateError("Cancellation window has expired")

        postings = [transfer, *await child_postings(self._session, [transfer.id])]
        accounts = await lock_accounts(self._session, {p.account_id for p in postings})
        target_id = transfer.account_id
        fee = Decimal(0)
        for posting in postings:
            if posting.type is OperationType.TRANSFER_IN:
                target_id = posting.account_id
                if accounts[posting.account_id].balance < posting.amount:
                    raise InvalidStateError(
                        "The recipient has already spent the funds: transfer cannot be cancelled"
                    )
            elif posting.type is OperationType.FEE:
                fee = posting.amount

        for posting in postings:
            self._reverse(posting, accounts[posting.account_id], transfer)
            self._ops.cancel(posting)
        await self._session.commit()
        await self._invalidate(*accounts.values())

        view = TransferView(transfer, TransferChannel.INTERNAL, target_id, fee)
        logger.info("transfer reversed", operation_id=str(transfer.id))
        await self._publisher.publish_safely(
            EventType.TRANSFER_REVERSED, transfer_payload(view), key=str(transfer.account_id)
        )
        return view

    def _reverse(self, posting: Operation, account: Account, transfer: Operation) -> None:
        """Book a completed compensating posting for ``posting`` and adjust the balance."""
        compensation = self._ops.create_pending(
            account_id=posting.account_id,
            type=_REVERSAL_TYPE[posting.type],
            amount=posting.amount,
            currency=posting.currency,
            parent_operation_id=transfer.id,
            reversed_operation_id=posting.id,
            counterparty_account=posting.counterparty_account,
            description="Transfer cancellation",
        )
        self._ops.mark_processing(compensation)
        self._ops.mark_completed(compensation)
        if posting.type in _CREDIT_TYPES:
            account.balance -= posting.amount
        else:
            account.balance += posting.amount

    # ---------- internals ----------

    @property
    def _window(self) -> timedelta:
        return timedelta(seconds=self._settings.cancel_window_seconds)

    async def _load(self, transfer: Operation) -> TransferView:
        return await load_view(
            self._session, transfer, cancel_window_seconds=self._settings.cancel_window_seconds
        )

    async def _invalidate(self, *accounts: Account) -> None:
        for user_id in {a.user_id for a in accounts}:
            await self._cache.invalidate(user_id)


class TransferFinalizer:
    """Completes pending internal transfers whose cancel window has passed."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: TransferSettings,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._settings = settings

    async def run_once(self) -> int:
        """Finalize one batch; returns the number of completed transfers."""
        deadline = datetime.now(UTC) - timedelta(seconds=self._settings.cancel_window_seconds)
        async with self._session_factory() as session:
            transfers = list(
                await session.scalars(
                    select(Operation)
                    .where(
                        Operation.type == OperationType.TRANSFER_OUT,
                        Operation.parent_operation_id.is_(None),
                        Operation.status == OperationStatus.PENDING,
                        Operation.created_at <= deadline,
                    )
                    .order_by(Operation.created_at)
                    .limit(self._settings.finalizer_batch_size)
                    # Several workers split the work; a transfer being cancelled is skipped.
                    .with_for_update(skip_locked=True)
                )
            )
            if not transfers:
                return 0
            children = await child_postings(session, [t.id for t in transfers])

            ops = OperationService(session)
            views: list[TransferView] = []
            for transfer in transfers:
                postings = [c for c in children if c.parent_operation_id == transfer.id]
                target_id = transfer.account_id
                fee = Decimal(0)
                for posting in (transfer, *postings):
                    ops.mark_processing(posting)
                    ops.mark_completed(posting)
                    if posting.type is OperationType.TRANSFER_IN:
                        target_id = posting.account_id
                    elif posting.type is OperationType.FEE:
                        fee = posting.amount
                views.append(TransferView(transfer, TransferChannel.INTERNAL, target_id, fee))
            await session.commit()

        for view in views:
            logger.info("transfer completed", operation_id=str(view.operation.id))
            await self._publisher.publish_safely(
                EventType.TRANSFER_COMPLETED,
                transfer_payload(view),
                key=str(view.operation.account_id),
            )
        return len(views)
