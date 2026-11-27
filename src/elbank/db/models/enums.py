"""Domain enums used by ORM models (stored as their string values)."""

from enum import StrEnum


class UserStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    DELETED = "deleted"


class UserRole(StrEnum):
    CLIENT = "client"
    MANAGER = "manager"
    ADMIN = "admin"
    AUDITOR = "auditor"


class KycStatus(StrEnum):
    PENDING = "pending"
    MANUAL_REVIEW = "manual_review"  # external check failed/unavailable: a manager decides
    APPROVED = "approved"
    REJECTED = "rejected"


class AccountType(StrEnum):
    CHECKING = "checking"
    SAVINGS = "savings"
    FOREIGN = "foreign"


class Currency(StrEnum):
    """Currencies an account can be opened in (stored as ISO 4217 alpha code)."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"
    CNY = "CNY"


class AccountStatus(StrEnum):
    ACTIVE = "active"
    FROZEN = "frozen"
    CLOSED = "closed"


class OperationType(StrEnum):
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"  # internal transfer (to an account of this bank)
    EXTERNAL_TRANSFER = "external_transfer"  # to another bank by requisites
    SBP_TRANSFER = "sbp_transfer"  # Faster Payments System, by phone number
    CONVERSION_OUT = "conversion_out"  # currency exchange between own accounts: debit leg
    CONVERSION_IN = "conversion_in"  # ... and credit leg
    PAYMENT = "payment"
    FEE = "fee"
    INTEREST = "interest"


class OperationStatus(StrEnum):
    CREATED = "created"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CardStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    EXPIRED = "expired"
    CLOSED = "closed"


class CardType(StrEnum):
    VIRTUAL = "virtual"
    PLASTIC = "plastic"


class PaymentProvider(StrEnum):
    UTILITY = "utility"  # housing and utilities (ЖКУ)
    MOBILE = "mobile"
    INTERNET = "internet"
    TAX = "tax"  # Federal Tax Service (ФНС)
    TRAFFIC_FINE = "traffic_fine"  # traffic police fines (ГИБДД)
    QR = "qr"  # payment QR code, GOST R 56042-2014
    RECEIPT = "receipt"  # paper receipt by its number


class PaymentOrderStatus(StrEnum):
    CREATED = "created"
    SENT = "sent"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CreditStatus(StrEnum):
    REQUESTED = "requested"
    APPROVED = "approved"
    REJECTED = "rejected"
    ACTIVE = "active"
    OVERDUE = "overdue"
    CLOSED = "closed"


class DepositStatus(StrEnum):
    ACTIVE = "active"
    CLOSED = "closed"
    EARLY_CLOSED = "early_closed"


class TicketStatus(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    WAITING_CLIENT = "waiting_client"
    RESOLVED = "resolved"
    CLOSED = "closed"


class TicketPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class NotificationChannel(StrEnum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    IN_APP = "in_app"
