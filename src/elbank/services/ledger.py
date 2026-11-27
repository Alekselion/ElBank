"""Shared building blocks of money movements: account locking and outgoing checks."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import AccountSettings
from elbank.db.models import (
    Account,
    AccountLimit,
    AccountStatus,
    Operation,
    OperationStatus,
    OperationType,
)

CENT = Decimal("0.01")

# Operations that count towards outgoing limits (fees, conversions and compensations do not).
LIMITED_TYPES = (
    OperationType.TRANSFER_OUT,
    OperationType.EXTERNAL_TRANSFER,
    OperationType.SBP_TRANSFER,
    OperationType.PAYMENT,
)
_LIMITED_STATUSES = (
    OperationStatus.PENDING,
    OperationStatus.PROCESSING,
    OperationStatus.COMPLETED,
)


class TransferErrorCode(StrEnum):
    ACCOUNT_NOT_ACTIVE = "ACCOUNT_NOT_ACTIVE"
    RECIPIENT_ACCOUNT_NOT_ACTIVE = "RECIPIENT_ACCOUNT_NOT_ACTIVE"
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    DAILY_LIMIT_EXCEEDED = "DAILY_LIMIT_EXCEEDED"
    MONTHLY_LIMIT_EXCEEDED = "MONTHLY_LIMIT_EXCEEDED"
    EXTERNAL_REJECTED = "EXTERNAL_REJECTED"
    EXTERNAL_TIMEOUT = "EXTERNAL_TIMEOUT"
    EXTERNAL_UNAVAILABLE = "EXTERNAL_UNAVAILABLE"


def percent_of(amount: Decimal, percent: Decimal) -> Decimal:
    """``percent`` of ``amount`` rounded half-up to kopecks."""
    return (amount * percent / 100).quantize(CENT, rounding=ROUND_HALF_UP)


async def lock_accounts(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, Account]:
    """``SELECT ... FOR UPDATE`` in a stable (id) order, so two opposite movements cannot
    deadlock. Lock order across the code base: operation rows first, then accounts."""
    rows = await session.scalars(
        select(Account)
        .where(Account.id.in_(ids))
        .order_by(Account.id)
        .with_for_update()
        # Sessions keep objects across commits (expire_on_commit=False): overwrite them
        # with the locked row, otherwise a stale balance/status would be used.
        .execution_options(populate_existing=True)
    )
    return {a.id: a for a in rows}


async def child_postings(session: AsyncSession, parent_ids: Sequence[uuid.UUID]) -> list[Operation]:
    """Postings belonging to the given main operations (not their compensations)."""
    rows = await session.scalars(
        select(Operation).where(
            Operation.parent_operation_id.in_(parent_ids),
            Operation.reversed_operation_id.is_(None),
        )
    )
    return list(rows)


class OutgoingChecks:
    """Business checks of a debit; a failure is stored as a failed operation, not raised."""

    def __init__(self, session: AsyncSession, settings: AccountSettings) -> None:
        self._session = session
        self._settings = settings

    async def check(
        self,
        source: Account,
        amount: Decimal,
        fee: Decimal,
        *,
        target: Account | None = None,
        apply_limits: bool = True,
    ) -> TransferErrorCode | None:
        """``source`` (and ``target``) must be locked by the caller."""
        if source.status is not AccountStatus.ACTIVE:
            return TransferErrorCode.ACCOUNT_NOT_ACTIVE
        if target is not None and target.status is not AccountStatus.ACTIVE:
            return TransferErrorCode.RECIPIENT_ACCOUNT_NOT_ACTIVE
        if source.balance < amount + fee:
            return TransferErrorCode.INSUFFICIENT_FUNDS
        return await self._check_limits(source, amount) if apply_limits else None

    async def _check_limits(self, source: Account, amount: Decimal) -> TransferErrorCode | None:
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
            return self._settings.default_daily_limit, self._settings.default_monthly_limit
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
                    Operation.type.in_(LIMITED_TYPES),
                    Operation.status.in_(_LIMITED_STATUSES),
                    Operation.parent_operation_id.is_(None),
                    Operation.created_at >= month_start,
                )
            )
        ).one()
        return Decimal(row[0]), Decimal(row[1])
