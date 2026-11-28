"""Classification of operations: direction (credit/debit), expense category, internal moves.

Analytics and budgets count only *settled* money: completed operations that are not
compensations (a cancelled transfer and its compensating postings net to zero and
are both left out).

Expenses are debits except internal moves — currency conversions and transfers
between the client's own accounts (their fees are real expenses and stay). Income is
credits except internal moves. The category of a debit comes from its type and, for
payments, from the provider of the payment order.
"""

import uuid
from typing import Any

from sqlalchemy import ColumnElement, and_, case, exists, or_, select
from sqlalchemy.orm import aliased

from elbank.db.models import (
    Account,
    Operation,
    OperationStatus,
    OperationType,
    PaymentOrder,
    PaymentProvider,
    SpendingCategory,
)

# Credit postings increase the balance; every other type is a debit.
CREDIT_TYPES = frozenset(
    {
        OperationType.TRANSFER_IN,
        OperationType.CONVERSION_IN,
        OperationType.INTEREST,
        OperationType.CREDIT_DISBURSEMENT,
        OperationType.DEPOSIT_CLOSE,
        OperationType.INVESTMENT_SELL,
    }
)
CONVERSION_TYPES = frozenset({OperationType.CONVERSION_OUT, OperationType.CONVERSION_IN})
_INTERNAL_TRANSFER_TYPES = (OperationType.TRANSFER_OUT, OperationType.TRANSFER_IN)

PROVIDER_CATEGORIES: dict[PaymentProvider, SpendingCategory] = {
    PaymentProvider.UTILITY: SpendingCategory.UTILITIES,
    PaymentProvider.MOBILE: SpendingCategory.MOBILE,
    PaymentProvider.INTERNET: SpendingCategory.INTERNET,
    PaymentProvider.TAX: SpendingCategory.TAXES,
    PaymentProvider.TRAFFIC_FINE: SpendingCategory.FINES,
    PaymentProvider.QR: SpendingCategory.QR_PAYMENTS,
    PaymentProvider.RECEIPT: SpendingCategory.BILLS,
}
TYPE_CATEGORIES: dict[OperationType, SpendingCategory] = {
    OperationType.TRANSFER_OUT: SpendingCategory.TRANSFERS,
    OperationType.EXTERNAL_TRANSFER: SpendingCategory.TRANSFERS,
    OperationType.SBP_TRANSFER: SpendingCategory.TRANSFERS,
    OperationType.FEE: SpendingCategory.FEES,
    OperationType.CREDIT_REPAYMENT: SpendingCategory.LOANS,
    OperationType.DEPOSIT_OPEN: SpendingCategory.SAVINGS,
    OperationType.DEPOSIT_TOP_UP: SpendingCategory.SAVINGS,
    OperationType.INVESTMENT_BUY: SpendingCategory.INVESTMENTS,
}


def is_credit(op_type: OperationType) -> bool:
    return op_type in CREDIT_TYPES


def category_of(
    op_type: OperationType, provider: PaymentProvider | None
) -> SpendingCategory | None:
    """Expense category of a debit; ``None`` for credits and conversions."""
    if op_type is OperationType.PAYMENT:
        if provider is None:
            return SpendingCategory.OTHER
        return PROVIDER_CATEGORIES.get(provider, SpendingCategory.OTHER)
    return TYPE_CATEGORIES.get(op_type)


# ---------- SQL counterparts (the query must join PaymentOrder on operation_id) ----------


def category_column() -> ColumnElement[Any]:
    """``CASE`` computing :func:`category_of` in SQL; ``NULL`` for credits/conversions."""
    whens: list[tuple[ColumnElement[bool], str]] = [
        (
            and_(Operation.type == OperationType.PAYMENT, PaymentOrder.provider == provider),
            category.value,
        )
        for provider, category in PROVIDER_CATEGORIES.items()
    ]
    whens.append((Operation.type == OperationType.PAYMENT, SpendingCategory.OTHER.value))
    whens.extend((Operation.type == t, c.value) for t, c in TYPE_CATEGORIES.items())
    return case(*whens, else_=None)


def credit_column() -> ColumnElement[bool]:
    return Operation.type.in_(CREDIT_TYPES)


def internal_move_column(user_id: uuid.UUID) -> ColumnElement[bool]:
    """``TRUE`` for conversions and transfers to/from another account of ``user_id``."""
    own = aliased(Account)
    own_counterparty = exists(
        select(own.id).where(own.number == Operation.counterparty_account, own.user_id == user_id)
    )
    return or_(
        Operation.type.in_(CONVERSION_TYPES),
        and_(Operation.type.in_(_INTERNAL_TRANSFER_TYPES), own_counterparty),
    )


def settled_filter() -> ColumnElement[bool]:
    """Completed operations that are not compensations of cancelled ones."""
    return and_(
        Operation.status == OperationStatus.COMPLETED,
        Operation.reversed_operation_id.is_(None),
    )
