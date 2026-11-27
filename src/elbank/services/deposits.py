"""Term deposits: opening, top-up, interest accrual and closing.

Interest is simple and daily (see :class:`elbank.db.models.Deposit`). It is posted
to the deposit by the accrual job (``deposit.interest.tick`` → deposits worker) and
before every principal change (top-up, closing), so each accrual entry has one
principal. On closing the principal and the interest go back to the account; before
the end date the interest is recalculated at ``DEPOSITS_EARLY_CLOSE_RATE``.

Lock order: the deposit row first, then the account.
"""

import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from elbank.core.config import DepositSettings
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    AccountStatus,
    Deposit,
    DepositStatus,
    OperationType,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.product import AccrualAccepted, DepositCloseResult, DepositRead
from elbank.services.account_cache import AccountListCache
from elbank.services.finance import (
    ZERO,
    accrual_entry,
    add_months,
    daily_interest,
    fmt,
    interest_at_rate,
)
from elbank.services.ledger import lock_accounts
from elbank.services.operation_service import OperationService

logger = get_logger(__name__)


def _today() -> date:
    return datetime.now(UTC).date()


def accrue(deposit: Deposit, as_of: date) -> bool:
    """Post interest for the days before ``as_of`` (capped by the end date).

    Extends the last accrual entry while the principal is unchanged, so entries only
    split on top-ups. Returns whether anything was posted.
    """
    until = min(as_of, deposit.end_date)
    if until <= deposit.accrued_until:
        return False
    entries: list[dict[str, Any]] = list(deposit.interest_schedule)
    last = entries[-1] if entries else None
    if (
        last is not None
        and Decimal(last["principal"]) == deposit.amount
        and date.fromisoformat(last["date_to"]) == deposit.accrued_until
    ):
        entries[-1] = accrual_entry(
            deposit.amount, deposit.rate, date.fromisoformat(last["date_from"]), until
        )
    else:
        entries.append(accrual_entry(deposit.amount, deposit.rate, deposit.accrued_until, until))
    deposit.interest_schedule = entries  # a new list: JSONB changes are detected by assignment
    deposit.accrued_interest = sum((Decimal(e["interest"]) for e in entries), ZERO)
    deposit.accrued_until = until
    return True


def interest_to_date(deposit: Deposit, today: date) -> Decimal:
    """Posted interest plus the not yet posted days up to today (or the end date)."""
    if deposit.status is not DepositStatus.ACTIVE:
        return deposit.accrued_interest
    pending = daily_interest(
        deposit.amount, deposit.rate, deposit.accrued_until, min(today, deposit.end_date)
    )
    return deposit.accrued_interest + pending


def deposit_read(deposit: Deposit) -> DepositRead:
    return DepositRead.from_deposit(deposit, interest_to_date(deposit, _today()))


def _deposit_payload(deposit: Deposit) -> dict[str, Any]:
    return {
        "deposit_id": str(deposit.id),
        "user_id": str(deposit.user_id),
        "account_id": str(deposit.account_id),
        "amount": fmt(deposit.amount),
        "currency": deposit.currency,
        "rate": format(deposit.rate, "f"),
        "term_months": deposit.term_months,
        "start_date": deposit.start_date.isoformat(),
        "end_date": deposit.end_date.isoformat(),
        "status": deposit.status.value,
    }


class DepositService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: DepositSettings,
    ) -> None:
        self._session = session
        self._cache = cache
        self._publisher = publisher
        self._settings = settings
        self._ops = OperationService(session)

    # ---------- client side ----------

    async def open(
        self, user: User, account_id: uuid.UUID, amount: Decimal, term_months: int, rate: Decimal
    ) -> Deposit:
        s = self._settings
        if amount < s.min_amount:
            raise UnprocessableError(f"Minimal deposit amount is {s.min_amount}")
        if not s.min_term_months <= term_months <= s.max_term_months:
            raise UnprocessableError(
                f"Term must be within {s.min_term_months}..{s.max_term_months} months"
            )
        if rate > s.max_rate:
            raise UnprocessableError(f"Rate must not exceed {s.max_rate}%")

        account = (await lock_accounts(self._session, {account_id})).get(account_id)
        if account is None or account.user_id != user.id:
            raise NotFoundError("Account not found")
        self._debit_checks(account, amount)

        today = _today()
        deposit = Deposit(
            id=uuid.uuid4(),
            user_id=user.id,
            account_id=account.id,
            amount=amount,
            currency=account.currency,
            rate=rate,
            term_months=term_months,
            status=DepositStatus.ACTIVE,
            start_date=today,
            end_date=add_months(today, term_months),
            accrued_until=today,
            accrued_interest=ZERO,
            interest_schedule=[],
        )
        self._session.add(deposit)
        account.balance -= amount
        self._ops.create_completed(
            account_id=account.id,
            user_id=user.id,
            type=OperationType.DEPOSIT_OPEN,
            amount=amount,
            currency=account.currency,
            description=f"Deposit {deposit.id}",
        )
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info(
            "deposit opened", deposit_id=str(deposit.id), amount=fmt(amount), rate=str(rate)
        )
        await self._publisher.publish_safely(
            EventType.DEPOSIT_OPENED, _deposit_payload(deposit), key=str(user.id)
        )
        return deposit

    async def list_for_user(self, user: User) -> Sequence[Deposit]:
        rows = await self._session.scalars(
            select(Deposit)
            .where(Deposit.user_id == user.id)
            .order_by(Deposit.created_at.desc(), Deposit.id)
        )
        return rows.all()

    async def get_for_user(
        self, user: User, deposit_id: uuid.UUID, *, for_update: bool = False
    ) -> Deposit:
        query = select(Deposit).where(Deposit.id == deposit_id, Deposit.user_id == user.id)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        deposit: Deposit | None = await self._session.scalar(query)
        if deposit is None:
            raise NotFoundError("Deposit not found")
        return deposit

    async def top_up(self, user: User, deposit_id: uuid.UUID, amount: Decimal) -> Deposit:
        deposit = await self.get_for_user(user, deposit_id, for_update=True)
        self._require_active(deposit)
        today = _today()
        if today >= deposit.end_date:
            raise InvalidStateError("The deposit term has ended: it can only be closed")
        account = await self._lock_account(deposit)
        self._debit_checks(account, amount)

        accrue(deposit, today)  # the old principal earns interest up to today
        deposit.amount += amount
        account.balance -= amount
        self._ops.create_completed(
            account_id=account.id,
            user_id=user.id,
            type=OperationType.DEPOSIT_TOP_UP,
            amount=amount,
            currency=account.currency,
            description=f"Deposit {deposit.id} top-up",
        )
        await self._session.commit()
        await self._cache.invalidate(user.id)
        logger.info(
            "deposit topped up",
            deposit_id=str(deposit.id),
            amount=fmt(amount),
            principal=fmt(deposit.amount),
        )
        return deposit

    async def close(self, user: User, deposit_id: uuid.UUID) -> DepositCloseResult:
        deposit = await self.get_for_user(user, deposit_id, for_update=True)
        self._require_active(deposit)
        account = await self._lock_account(deposit)
        if account.status is AccountStatus.CLOSED:
            raise InvalidStateError("The deposit account is closed")

        today = _today()
        accrue(deposit, today)
        early = today < deposit.end_date
        interest = (
            interest_at_rate(deposit.interest_schedule, self._settings.early_close_rate)
            if early
            else deposit.accrued_interest
        )
        principal = deposit.amount

        close_op = self._ops.create_completed(
            account_id=account.id,
            user_id=user.id,
            type=OperationType.DEPOSIT_CLOSE,
            amount=principal,
            currency=account.currency,
            description=f"Deposit {deposit.id} closing",
        )
        if interest > 0:
            self._ops.create_completed(
                account_id=account.id,
                type=OperationType.INTEREST,
                amount=interest,
                currency=account.currency,
                parent_operation_id=close_op.id,
                description=f"Deposit {deposit.id} interest",
            )
        account.balance += principal + interest
        deposit.status = DepositStatus.EARLY_CLOSED if early else DepositStatus.CLOSED
        deposit.paid_interest = interest
        deposit.closed_at = datetime.now(UTC)
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info(
            "deposit closed",
            deposit_id=str(deposit.id),
            early=early,
            principal=fmt(principal),
            interest=fmt(interest),
        )
        await self._publisher.publish_safely(
            EventType.DEPOSIT_CLOSED,
            _deposit_payload(deposit) | {"early": early, "paid_interest": fmt(interest)},
            key=str(user.id),
        )
        return DepositCloseResult(
            deposit=deposit_read(deposit),
            early=early,
            principal=principal,
            interest=interest,
            operation_id=close_op.id,
        )

    # ---------- back office ----------

    async def request_accrual(self, as_of: date | None) -> AccrualAccepted:
        """Publish ``deposit.interest.tick`` (in production a cron job does this)."""
        today = _today()
        as_of = as_of or today
        if as_of > today:
            raise UnprocessableError("Interest cannot be accrued for future days")
        try:
            envelope = await self._publisher.publish(
                EventType.DEPOSIT_INTEREST_TICK, {"as_of": as_of.isoformat()}
            )
        except Exception as exc:
            raise ExternalServiceError("Accrual queue is unavailable, try again later") from exc
        logger.info("deposit accrual requested", as_of=as_of.isoformat())
        return AccrualAccepted(as_of=as_of, event_id=envelope.event_id)

    # ---------- internals ----------

    async def _lock_account(self, deposit: Deposit) -> Account:
        account = (await lock_accounts(self._session, {deposit.account_id})).get(deposit.account_id)
        if account is None:
            raise RuntimeError(f"Account of deposit {deposit.id} is missing")
        return account

    @staticmethod
    def _debit_checks(account: Account, amount: Decimal) -> None:
        if account.status is not AccountStatus.ACTIVE:
            raise InvalidStateError(f"Account must be active, it is {account.status.value}")
        if account.balance < amount:
            raise UnprocessableError("Insufficient funds on the account")

    @staticmethod
    def _require_active(deposit: Deposit) -> None:
        if deposit.status is not DepositStatus.ACTIVE:
            raise InvalidStateError(f"The deposit is {deposit.status.value}")


class DepositAccrualJob:
    """Posts interest of all active deposits up to a date, in batches.

    Rows are taken with ``FOR UPDATE SKIP LOCKED``: a deposit being topped up or
    closed right now is skipped (that request accrues it itself), and several workers
    can run at once. Re-running for the same date changes nothing.
    """

    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], settings: DepositSettings
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = settings.accrual_batch_size

    async def run(self, as_of: date) -> int:
        total = 0
        while True:
            handled = await self._run_batch(as_of)
            total += handled
            if handled < self._batch_size:
                break
        logger.info("deposit interest accrued", as_of=as_of.isoformat(), deposits=total)
        return total

    async def _run_batch(self, as_of: date) -> int:
        async with self._session_factory() as session:
            rows = await session.scalars(
                select(Deposit)
                .where(
                    Deposit.status == DepositStatus.ACTIVE,
                    Deposit.accrued_until < func.least(Deposit.end_date, as_of),
                )
                .order_by(Deposit.id)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            deposits = rows.all()
            for deposit in deposits:
                accrue(deposit, as_of)
            await session.commit()
            return len(deposits)
