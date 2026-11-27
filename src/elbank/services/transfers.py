"""Internal transfers between accounts of the bank.

Lifecycle of a transfer (all status changes go through :class:`OperationService`):

1. ``POST /transfers/internal`` — one DB transaction with both accounts locked
   (``SELECT ... FOR UPDATE``, in id order so two opposite transfers cannot
   deadlock): checks, debit of ``amount + fee``, credit of ``amount`` and three
   ``pending`` operations — ``transfer_out`` (the transfer itself, carries the
   idempotency key), ``transfer_in`` and ``fee``. A failed check stores the
   ``transfer_out`` as ``failed`` with an ``error_code`` and publishes
   ``transfer.failed``.
2. During ``TRANSFERS_CANCEL_WINDOW_SECONDS`` the transfer can be cancelled:
   compensating operations return the money, the original ones become
   ``cancelled``, ``transfer.reversed`` is published.
3. Then :class:`TransferFinalizer` (the transfers worker) moves the operations
   ``pending → processing → completed`` and publishes ``transfer.completed``.

Idempotency: the key is unique per initiator in the DB; while a request is
processed a Redis lock ``op_lock:{user_id}:{key}`` makes a parallel request with
the same key wait (up to ``TRANSFERS_LOCK_WAIT_SECONDS``) and then either get the
stored result or ``409``.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, RedisError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import AccountSettings, TransferSettings
from elbank.core.exceptions import (
    IdempotencyKeyReusedError,
    InvalidStateError,
    NotFoundError,
    OperationInProgressError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    AccountLimit,
    AccountStatus,
    Currency,
    Operation,
    OperationStatus,
    OperationType,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.services.account_cache import AccountListCache
from elbank.services.operation_service import CANCELLABLE_STATUSES, OperationService

logger = get_logger(__name__)

_CENT = Decimal("0.01")
_IDEMPOTENCY_CONSTRAINT = "uq_operations_user_id_idempotency_key"
# Operations that count towards outgoing limits (fees and compensations do not).
_LIMITED_TYPES = (OperationType.TRANSFER_OUT, OperationType.PAYMENT)
_LIMITED_STATUSES = (
    OperationStatus.PENDING,
    OperationStatus.PROCESSING,
    OperationStatus.COMPLETED,
)
# Credit postings increase the balance; their reversal is a debit and vice versa.
_CREDIT_TYPES = frozenset({OperationType.TRANSFER_IN, OperationType.INTEREST})
_REVERSAL_TYPE: dict[OperationType, OperationType] = {
    OperationType.TRANSFER_OUT: OperationType.TRANSFER_IN,
    OperationType.TRANSFER_IN: OperationType.TRANSFER_OUT,
    OperationType.FEE: OperationType.TRANSFER_IN,  # fee refund
}


class TransferErrorCode(StrEnum):
    ACCOUNT_NOT_ACTIVE = "ACCOUNT_NOT_ACTIVE"
    RECIPIENT_ACCOUNT_NOT_ACTIVE = "RECIPIENT_ACCOUNT_NOT_ACTIVE"
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    DAILY_LIMIT_EXCEEDED = "DAILY_LIMIT_EXCEEDED"
    MONTHLY_LIMIT_EXCEEDED = "MONTHLY_LIMIT_EXCEEDED"


def calculate_fee(amount: Decimal, settings: TransferSettings) -> Decimal:
    """``fee_percent`` of the amount rounded half-up to kopecks, clamped to [min, max]."""
    fee = (amount * settings.fee_percent / 100).quantize(_CENT, rounding=ROUND_HALF_UP)
    return min(max(fee, settings.fee_min), settings.fee_max)


@dataclass(frozen=True, slots=True)
class InternalTransfer:
    user: User
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class TransferView:
    operation: Operation  # the transfer_out
    to_account_id: uuid.UUID
    fee: Decimal
    cancellable_until: datetime | None


@dataclass(frozen=True, slots=True)
class TransferResult:
    transfer: TransferView
    created: bool  # False: the idempotency key was seen before, stored result returned


def _transfer_payload(view: TransferView) -> dict[str, Any]:
    op = view.operation
    return {
        "operation_id": str(op.id),
        "user_id": str(op.user_id),
        "from_account_id": str(op.account_id),
        "to_account_id": str(view.to_account_id),
        "amount": format(op.amount, "f"),
        "fee": format(view.fee, "f"),
        "currency": op.currency,
        "status": op.status.value,
    }


async def _lock_accounts(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, Account]:
    """``SELECT ... FOR UPDATE`` in a stable (id) order to avoid deadlocks."""
    rows = await session.scalars(
        select(Account).where(Account.id.in_(ids)).order_by(Account.id).with_for_update()
    )
    return {a.id: a for a in rows}


async def _child_postings(
    session: AsyncSession, transfer_ids: Sequence[uuid.UUID]
) -> list[Operation]:
    """``transfer_in``/``fee`` postings of the transfers (not their compensations)."""
    rows = await session.scalars(
        select(Operation).where(
            Operation.parent_operation_id.in_(transfer_ids),
            Operation.reversed_operation_id.is_(None),
        )
    )
    return list(rows)


class TransferService:
    def __init__(
        self,
        session: AsyncSession,
        redis: Redis,
        *,
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: TransferSettings,
        account_settings: AccountSettings,
    ) -> None:
        self._session = session
        self._redis = redis
        self._cache = cache
        self._publisher = publisher
        self._settings = settings
        self._account_settings = account_settings
        self._ops = OperationService(session)

    # ---------- create ----------

    async def create_internal(self, request: InternalTransfer) -> TransferResult:
        user = request.user
        if (existing := await self._find_by_key(user.id, request.idempotency_key)) is not None:
            return await self._replay(existing, request)

        lock = self._redis.lock(
            f"op_lock:{user.id}:{request.idempotency_key}",
            timeout=self._settings.lock_ttl_seconds,
            blocking_timeout=self._settings.lock_wait_seconds,
            sleep=0.05,
        )
        acquired = await self._acquire(lock)
        if not acquired:
            # The parallel request may have finished while we were waiting.
            existing = await self._find_by_key(user.id, request.idempotency_key)
            if existing is not None:
                return await self._replay(existing, request)
            raise OperationInProgressError
        try:
            existing = await self._find_by_key(user.id, request.idempotency_key)
            if existing is not None:
                return await self._replay(existing, request)
            return await self._execute(request)
        finally:
            await self._release(lock)

    async def _execute(self, request: InternalTransfer) -> TransferResult:
        accounts = await _lock_accounts(
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

        fee = calculate_fee(request.amount, self._settings)
        error = await self._check(source, target, request.amount, fee)
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
            if (replayed := await self._commit(request)) is not None:
                return replayed
            view = TransferView(transfer, target.id, Decimal(0), None)
            logger.info("transfer failed", operation_id=str(transfer.id), error_code=error.value)
            await self._publisher.publish_safely(
                EventType.TRANSFER_FAILED,
                _transfer_payload(view) | {"error_code": error.value},
                key=str(source.id),
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
        self._ops.create_pending(
            account_id=source.id,
            type=OperationType.FEE,
            amount=fee,
            currency=source.currency,
            parent_operation_id=transfer.id,
            description="Internal transfer fee",
        )
        if (replayed := await self._commit(request)) is not None:
            return replayed
        await self._invalidate(source, target)

        logger.info(
            "transfer accepted",
            operation_id=str(transfer.id),
            from_account_id=str(source.id),
            to_account_id=str(target.id),
            amount=str(request.amount),
            fee=str(fee),
        )
        return TransferResult(self._view(transfer, target.id, fee), created=True)

    async def _check(
        self, source: Account, target: Account, amount: Decimal, fee: Decimal
    ) -> TransferErrorCode | None:
        if source.status is not AccountStatus.ACTIVE:
            return TransferErrorCode.ACCOUNT_NOT_ACTIVE
        if target.status is not AccountStatus.ACTIVE:
            return TransferErrorCode.RECIPIENT_ACCOUNT_NOT_ACTIVE
        if source.balance < amount + fee:
            return TransferErrorCode.INSUFFICIENT_FUNDS

        daily_limit, monthly_limit = await self._limits(source)
        spent_today, spent_this_month = await self._spent(source)
        if spent_today + amount > daily_limit:
            return TransferErrorCode.DAILY_LIMIT_EXCEEDED
        if spent_this_month + amount > monthly_limit:
            return TransferErrorCode.MONTHLY_LIMIT_EXCEEDED
        return None

    async def _limits(self, account: Account) -> tuple[Decimal, Decimal]:
        limit = await self._session.scalar(
            select(AccountLimit).where(AccountLimit.account_id == account.id)
        )
        if limit is None:
            return (
                self._account_settings.default_daily_limit,
                self._account_settings.default_monthly_limit,
            )
        return limit.daily_limit, limit.monthly_limit

    async def _spent(self, account: Account) -> tuple[Decimal, Decimal]:
        """Outgoing amounts since the start of the current UTC day and month."""
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = day_start.replace(day=1)
        row = (
            await self._session.execute(
                select(
                    func.coalesce(
                        func.sum(Operation.amount).filter(Operation.created_at >= day_start), 0
                    ),
                    func.coalesce(func.sum(Operation.amount), 0),
                ).where(
                    Operation.account_id == account.id,
                    Operation.type.in_(_LIMITED_TYPES),
                    Operation.status.in_(_LIMITED_STATUSES),
                    Operation.parent_operation_id.is_(None),
                    Operation.created_at >= month_start,
                )
            )
        ).one()
        return Decimal(row[0]), Decimal(row[1])

    async def _commit(self, request: InternalTransfer) -> TransferResult | None:
        """Commit; on an idempotency-key race return the stored result instead."""
        try:
            await self._session.commit()
        except IntegrityError as exc:
            if _IDEMPOTENCY_CONSTRAINT not in str(exc.orig):
                raise
            await self._session.rollback()
            existing = await self._find_by_key(request.user.id, request.idempotency_key)
            if existing is None:
                raise
            return await self._replay(existing, request)
        return None

    # ---------- read ----------

    async def get_for_user(self, user: User, operation_id: uuid.UUID) -> TransferView:
        transfer = await self._get_transfer(user, operation_id, for_update=False)
        return await self._load_view(transfer)

    # ---------- cancel ----------

    async def cancel(self, user: User, operation_id: uuid.UUID) -> TransferView:
        transfer = await self._get_transfer(user, operation_id, for_update=True)
        if transfer.status not in CANCELLABLE_STATUSES:
            raise InvalidStateError(f"Transfer is already {transfer.status.value}")
        window = timedelta(seconds=self._settings.cancel_window_seconds)
        if datetime.now(UTC) > transfer.created_at + window:
            # Not finalized yet only because the worker lags behind: still too late.
            raise InvalidStateError("Cancellation window has expired")

        postings = [transfer, *await _child_postings(self._session, [transfer.id])]
        accounts = await _lock_accounts(self._session, {p.account_id for p in postings})
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

        view = TransferView(transfer, target_id, fee, None)
        logger.info("transfer reversed", operation_id=str(transfer.id))
        await self._publisher.publish_safely(
            EventType.TRANSFER_REVERSED, _transfer_payload(view), key=str(transfer.account_id)
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

    async def _find_by_key(self, user_id: uuid.UUID, key: str) -> Operation | None:
        result: Operation | None = await self._session.scalar(
            select(Operation).where(Operation.user_id == user_id, Operation.idempotency_key == key)
        )
        return result

    async def _replay(self, existing: Operation, request: InternalTransfer) -> TransferResult:
        view = await self._load_view(existing)
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

    async def _get_transfer(
        self, user: User, operation_id: uuid.UUID, *, for_update: bool
    ) -> Operation:
        query = select(Operation).where(Operation.id == operation_id)
        if for_update:
            query = query.with_for_update()
        transfer: Operation | None = await self._session.scalar(query)
        if (
            transfer is None
            or transfer.user_id != user.id
            or transfer.type is not OperationType.TRANSFER_OUT
            or transfer.parent_operation_id is not None
        ):
            raise NotFoundError("Transfer not found")
        return transfer

    async def _load_view(self, transfer: Operation) -> TransferView:
        target_id = await self._session.scalar(
            select(Account.id).where(Account.number == transfer.counterparty_account)
        )
        if target_id is None:
            raise RuntimeError(f"Recipient account of transfer {transfer.id} is missing")
        fee = Decimal(0)
        for posting in await _child_postings(self._session, [transfer.id]):
            if posting.type is OperationType.FEE:
                fee = posting.amount
        return self._view(transfer, target_id, fee)

    def _view(self, transfer: Operation, target_id: uuid.UUID, fee: Decimal) -> TransferView:
        cancellable_until = None
        if transfer.status in CANCELLABLE_STATUSES:
            cancellable_until = transfer.created_at + timedelta(
                seconds=self._settings.cancel_window_seconds
            )
        return TransferView(transfer, target_id, fee, cancellable_until)

    async def _invalidate(self, *accounts: Account) -> None:
        for user_id in {a.user_id for a in accounts}:
            await self._cache.invalidate(user_id)

    async def _acquire(self, lock: Lock) -> bool:
        try:
            return bool(await lock.acquire())
        except RedisError as exc:
            # The DB unique key still guarantees idempotency; only the waiting is lost.
            logger.warning("op_lock unavailable, continuing without it", error=str(exc))
            return True

    async def _release(self, lock: Lock) -> None:
        try:
            await lock.release()
        except LockError:
            # Expired (processing took longer than the TTL) or never taken (Redis down).
            logger.warning("op_lock was not held on release", lock=str(lock.name))
        except RedisError as exc:
            logger.warning("op_lock release failed", error=str(exc))


class TransferFinalizer:
    """Completes pending transfers whose cancel window has passed."""

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
            children = await _child_postings(session, [t.id for t in transfers])

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
                views.append(TransferView(transfer, target_id, fee, None))
            await session.commit()

        for view in views:
            logger.info("transfer completed", operation_id=str(view.operation.id))
            await self._publisher.publish_safely(
                EventType.TRANSFER_COMPLETED,
                _transfer_payload(view),
                key=str(view.operation.account_id),
            )
        return len(views)
