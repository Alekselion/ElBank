"""ORM models.

Every model module is imported here so ``Base.metadata`` is fully populated
for Alembic autogenerate (``alembic/env.py`` imports this package).
"""

from elbank.db.base import Base
from elbank.db.models.account import Account, AccountLimit, Card
from elbank.db.models.admin import RateOverride, Tariff
from elbank.db.models.audit import AuditEvent, FraudRule
from elbank.db.models.budget import Budget
from elbank.db.models.enums import (
    AccountStatus,
    AccountType,
    CardStatus,
    CardType,
    CreditStatus,
    Currency,
    DepositStatus,
    InvestmentOrderStatus,
    InvestmentSide,
    KycStatus,
    NotificationChannel,
    NotificationDeliveryStatus,
    OperationStatus,
    OperationType,
    PaymentOrderStatus,
    PaymentProvider,
    SpendingCategory,
    TariffCode,
    TicketCategory,
    TicketPriority,
    TicketStatus,
    UserRole,
    UserStatus,
)
from elbank.db.models.notification import Notification
from elbank.db.models.operation import Operation, PaymentOrder, TransferTemplate
from elbank.db.models.product import Credit, Deposit, InvestmentOrder, InvestmentPosition
from elbank.db.models.support import (
    FaqArticle,
    SupportAttachment,
    SupportMessage,
    SupportTicket,
)
from elbank.db.models.user import KycRequest, RefreshToken, User

__all__ = [
    "Account",
    "AccountLimit",
    "AccountStatus",
    "AccountType",
    "AuditEvent",
    "Base",
    "Budget",
    "Card",
    "CardStatus",
    "CardType",
    "Credit",
    "CreditStatus",
    "Currency",
    "Deposit",
    "DepositStatus",
    "FaqArticle",
    "FraudRule",
    "InvestmentOrder",
    "InvestmentOrderStatus",
    "InvestmentPosition",
    "InvestmentSide",
    "KycRequest",
    "KycStatus",
    "Notification",
    "NotificationChannel",
    "NotificationDeliveryStatus",
    "Operation",
    "OperationStatus",
    "OperationType",
    "PaymentOrder",
    "PaymentOrderStatus",
    "PaymentProvider",
    "RateOverride",
    "RefreshToken",
    "SpendingCategory",
    "SupportAttachment",
    "SupportMessage",
    "SupportTicket",
    "Tariff",
    "TariffCode",
    "TicketCategory",
    "TicketPriority",
    "TicketStatus",
    "TransferTemplate",
    "User",
    "UserRole",
    "UserStatus",
]
