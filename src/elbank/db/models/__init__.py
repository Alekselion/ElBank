"""ORM models.

Every model module is imported here so ``Base.metadata`` is fully populated
for Alembic autogenerate (``alembic/env.py`` imports this package).
"""

from elbank.db.base import Base
from elbank.db.models.account import Account, AccountLimit, Card
from elbank.db.models.audit import AuditEvent, FraudRule
from elbank.db.models.enums import (
    AccountStatus,
    AccountType,
    CardStatus,
    CardType,
    CreditStatus,
    Currency,
    DepositStatus,
    KycStatus,
    NotificationChannel,
    OperationStatus,
    OperationType,
    PaymentOrderStatus,
    PaymentProvider,
    TicketPriority,
    TicketStatus,
    UserRole,
    UserStatus,
)
from elbank.db.models.notification import Notification
from elbank.db.models.operation import Operation, PaymentOrder, TransferTemplate
from elbank.db.models.product import Credit, Deposit
from elbank.db.models.support import SupportMessage, SupportTicket
from elbank.db.models.user import KycRequest, RefreshToken, User

__all__ = [
    "Account",
    "AccountLimit",
    "AccountStatus",
    "AccountType",
    "AuditEvent",
    "Base",
    "Card",
    "CardStatus",
    "CardType",
    "Credit",
    "CreditStatus",
    "Currency",
    "Deposit",
    "DepositStatus",
    "FraudRule",
    "KycRequest",
    "KycStatus",
    "Notification",
    "NotificationChannel",
    "Operation",
    "OperationStatus",
    "OperationType",
    "PaymentOrder",
    "PaymentOrderStatus",
    "PaymentProvider",
    "RefreshToken",
    "SupportMessage",
    "SupportTicket",
    "TicketPriority",
    "TicketStatus",
    "TransferTemplate",
    "User",
    "UserRole",
    "UserStatus",
]
