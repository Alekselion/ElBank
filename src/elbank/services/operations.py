"""Operation history of a client: every posting on the client's accounts.

Unlike analytics, history shows operations in any status, compensations included.
Periods are whole UTC days (operation timestamps are UTC).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from decimal import Decimal

from sqlalchemy import ColumnElement, Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.exceptions import NotFoundError, UnprocessableError
from elbank.db.models import (
    Account,
    Operation,
    OperationStatus,
    OperationType,
    PaymentOrder,
    PaymentProvider,
    User,
)
from elbank.schemas.operation import Direction, OperationDetail, OperationRead
from elbank.services.categories import category_of, is_credit


def utc_bounds(
    date_from: date | None, date_to: date | None
) -> tuple[datetime | None, datetime | None]:
    """``[start of date_from, start of the day after date_to)`` in UTC."""
    start = datetime.combine(date_from, dt_time.min, tzinfo=UTC) if date_from else None
    end = (
        datetime.combine(date_to + timedelta(days=1), dt_time.min, tzinfo=UTC) if date_to else None
    )
    return start, end


def validate_period(date_from: date, date_to: date, max_days: int) -> None:
    if date_from > date_to:
        raise UnprocessableError("'from' must not be later than 'to'")
    if (date_to - date_from).days + 1 > max_days:
        raise UnprocessableError(f"Period must not exceed {max_days} days")


@dataclass(slots=True, frozen=True)
class OperationFilters:
    date_from: date | None = None
    date_to: date | None = None
    account_id: uuid.UUID | None = None
    types: tuple[OperationType, ...] = ()
    status: OperationStatus | None = None
    amount_min: Decimal | None = None
    amount_max: Decimal | None = None

    def validate(self) -> None:
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise UnprocessableError("'from' must not be later than 'to'")
        if (
            self.amount_min is not None
            and self.amount_max is not None
            and self.amount_min > self.amount_max
        ):
            raise UnprocessableError("'amount_min' must not exceed 'amount_max'")

    def conditions(self, user_id: uuid.UUID) -> list[ColumnElement[bool]]:
        """WHERE clauses; the query must join ``Account`` on the operation's account."""
        start, end = utc_bounds(self.date_from, self.date_to)
        where: list[ColumnElement[bool]] = [Account.user_id == user_id]
        if start is not None:
            where.append(Operation.created_at >= start)
        if end is not None:
            where.append(Operation.created_at < end)
        if self.account_id is not None:
            where.append(Operation.account_id == self.account_id)
        if self.types:
            where.append(Operation.type.in_(self.types))
        if self.status is not None:
            where.append(Operation.status == self.status)
        if self.amount_min is not None:
            where.append(Operation.amount >= self.amount_min)
        if self.amount_max is not None:
            where.append(Operation.amount <= self.amount_max)
        return where


def history_query() -> Select[Operation, str, PaymentProvider]:
    """Operations with their account number and payment provider (for the category).

    The provider comes from an outer join: it is ``None`` for anything but payments.
    """
    return (
        select(Operation, Account.number, PaymentOrder.provider)
        .join(Account, Account.id == Operation.account_id)
        .outerjoin(PaymentOrder, PaymentOrder.operation_id == Operation.id)
    )


def to_read(op: Operation, account_number: str, provider: PaymentProvider | None) -> OperationRead:
    return OperationRead(
        id=op.id,
        account_id=op.account_id,
        account_number=account_number,
        type=op.type,
        direction=Direction.CREDIT if is_credit(op.type) else Direction.DEBIT,
        # A compensation returns money: it is not an expense even when it is a debit.
        category=(
            None
            if is_credit(op.type) or op.reversed_operation_id is not None
            else category_of(op.type, provider)
        ),
        amount=op.amount,
        currency=op.currency,
        status=op.status,
        counterparty_account=op.counterparty_account,
        description=op.description,
        created_at=op.created_at,
    )


class OperationHistoryService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_for_user(
        self, user: User, filters: OperationFilters, *, page: int, size: int
    ) -> tuple[list[OperationRead], int]:
        filters.validate()
        where = filters.conditions(user.id)
        total = await self._session.scalar(
            select(func.count())
            .select_from(Operation)
            .join(Account, Account.id == Operation.account_id)
            .where(*where)
        )
        rows = await self._session.execute(
            history_query()
            .where(*where)
            .order_by(Operation.created_at.desc(), Operation.id.desc())
            .offset((page - 1) * size)
            .limit(size)
        )
        return [to_read(*row) for row in rows.tuples()], total or 0

    async def get_for_user(self, user: User, operation_id: uuid.UUID) -> OperationDetail:
        row = (
            (
                await self._session.execute(
                    history_query().where(Operation.id == operation_id, Account.user_id == user.id)
                )
            )
            .tuples()
            .one_or_none()
        )
        if row is None:
            raise NotFoundError("Operation not found")
        op, number, provider = row
        return OperationDetail(
            **to_read(op, number, provider).model_dump(),
            provider=provider,
            error_code=op.error_code,
            external_id=op.external_id,
            exchange_rate=op.exchange_rate,
            parent_operation_id=op.parent_operation_id,
            reversed_operation_id=op.reversed_operation_id,
            initiator_id=op.user_id,
            updated_at=op.updated_at,
        )
