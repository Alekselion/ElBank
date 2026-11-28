"""Anti-fraud: rules (JSON DSL), evaluation before an operation, holding suspicious ones.

Rule condition (``fraud_rules.condition``, jsonb) — a comparison or a combination::

    {"field": "amount", "op": ">", "value": 100000}
    {"field": "hour", "op": "in", "value": [0, 1, 2, 3, 4]}
    {"all": [{"field": "operation", "op": "==", "value": "sbp_transfer"},
             {"field": "ops_last_hour", "op": ">=", "value": 5}]}
    {"any": [...]}      {"not": {...}}

Operators: ``== != > >= < <= in not_in`` (ordering only for numeric fields; ``in``
takes a list of up to 100 values). Fields (:data:`FIELDS`):

=================== ========= ================================================
amount              number    operation amount (account currency)
fee                 number    fee charged on top of the amount
balance             number    account balance before the operation
currency            text      account currency, e.g. ``RUB``
operation           text      transfer_out, external_transfer, sbp_transfer, payment
hour, weekday       number    UTC hour (0-23), weekday (0 = Monday)
account_age_days    number    whole days since the account was opened
ops_last_hour       number    outgoing operations of the account in the last hour
spent_today         number    outgoing amount of the account today (UTC)
=================== ========= ================================================

Checked: money leaving the client — internal transfers, interbank/SBP transfers,
provider payments (not conversions between own accounts). When at least one
enabled rule matches, the operation is **held**: the money is debited (reserved)
but not sent, the operation stays ``pending`` with ``error_code=FRAUD_REVIEW``, a
:class:`FraudCase` and a support ticket (category ``fraud``) are created in the same
transaction, and after the commit ``fraud.suspicious`` and an audit event are
published. A manager then confirms or rejects it (:mod:`elbank.services.fraud_review`).
"""

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.exceptions import ConflictError, NotFoundError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    FraudCase,
    FraudCaseStatus,
    FraudRule,
    Operation,
    OperationStatus,
    OperationType,
    SupportMessage,
    SupportTicket,
    TicketCategory,
    TicketPriority,
    TicketStatus,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.services.audit_service import AuditService
from elbank.services.ledger import TransferErrorCode

logger = get_logger(__name__)

# ---------- DSL ----------

NUMERIC_FIELDS = frozenset(
    {
        "amount",
        "fee",
        "balance",
        "hour",
        "weekday",
        "account_age_days",
        "ops_last_hour",
        "spent_today",
    }
)
TEXT_FIELDS = frozenset({"currency", "operation"})
FIELDS = NUMERIC_FIELDS | TEXT_FIELDS

type FieldName = Literal[
    "amount",
    "fee",
    "balance",
    "currency",
    "operation",
    "hour",
    "weekday",
    "account_age_days",
    "ops_last_hour",
    "spent_today",
]
type Operator = Literal["==", "!=", ">", ">=", "<", "<=", "in", "not_in"]
type Scalar = Decimal | str

_ORDERING = frozenset({">", ">=", "<", "<="})
_MEMBERSHIP = frozenset({"in", "not_in"})
MAX_LIST_VALUES = 100
MAX_DEPTH = 5


class Comparison(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    field: FieldName
    op: Operator
    value: Decimal | str | list[Decimal] | list[str]

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        """Numbers of numeric fields become exact Decimals; text fields need strings."""
        if not isinstance(data, Mapping) or "value" not in data:
            return data
        field, value = data.get("field"), data["value"]
        if field not in FIELDS:
            return data  # the Literal check reports the unknown field
        numeric = field in NUMERIC_FIELDS

        def norm(item: Any) -> Scalar:
            if isinstance(item, bool) or item is None:
                raise ValueError(f"unsupported value {item!r}")
            if not numeric:
                if not isinstance(item, str):
                    raise ValueError(f"{field} needs text values")
                return item
            try:
                number = Decimal(str(item))
            except InvalidOperation as exc:
                raise ValueError(f"{field} needs numbers, got {item!r}") from exc
            if not number.is_finite():
                raise ValueError(f"{field} needs finite numbers")
            return number

        normalized = [norm(v) for v in value] if isinstance(value, list) else norm(value)
        return {**data, "value": normalized}

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.op in _MEMBERSHIP:
            if not isinstance(self.value, list) or not 1 <= len(self.value) <= MAX_LIST_VALUES:
                raise ValueError(f"'{self.op}' needs a list of 1..{MAX_LIST_VALUES} values")
        elif isinstance(self.value, list):
            raise ValueError(f"'{self.op}' needs a single value, not a list")
        if self.op in _ORDERING and self.field not in NUMERIC_FIELDS:
            raise ValueError(f"'{self.op}' applies to numeric fields only")
        return self

    def matches(self, ctx: Mapping[str, Scalar]) -> bool:  # noqa: PLR0911 - one per operator
        actual = ctx[self.field]
        match self.op:
            case "==":
                return actual == self.value
            case "!=":
                return actual != self.value
            case "in":
                return actual in self.value  # type: ignore[operator]
            case "not_in":
                return actual not in self.value  # type: ignore[operator]
        assert isinstance(actual, Decimal) and isinstance(self.value, Decimal)
        match self.op:
            case ">":
                return actual > self.value
            case ">=":
                return actual >= self.value
            case "<":
                return actual < self.value
            case _:
                return actual <= self.value


class AllOf(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    all: Annotated[list[Condition], Field(min_length=1, max_length=20)]

    def matches(self, ctx: Mapping[str, Scalar]) -> bool:
        return all(c.matches(ctx) for c in self.all)


class AnyOf(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    any: Annotated[list[Condition], Field(min_length=1, max_length=20)]

    def matches(self, ctx: Mapping[str, Scalar]) -> bool:
        return any(c.matches(ctx) for c in self.any)


class NotOf(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    not_: Condition = Field(alias="not")

    def matches(self, ctx: Mapping[str, Scalar]) -> bool:
        return not self.not_.matches(ctx)


def _condition_kind(raw: Any) -> str | None:
    if isinstance(raw, BaseModel):
        return {Comparison: "cmp", AllOf: "all", AnyOf: "any", NotOf: "not"}.get(type(raw))
    if isinstance(raw, Mapping):
        for key in ("all", "any", "not"):
            if key in raw:
                return key
        return "cmp" if "field" in raw else None
    return None


type Condition = Annotated[
    Annotated[Comparison, Tag("cmp")]
    | Annotated[AllOf, Tag("all")]
    | Annotated[AnyOf, Tag("any")]
    | Annotated[NotOf, Tag("not")],
    Discriminator(
        _condition_kind,
        custom_error_type="invalid_condition",
        custom_error_message='expected {"field", "op", "value"}, {"all": [...]}, '
        '{"any": [...]} or {"not": {...}}',
    ),
]

AllOf.model_rebuild()
AnyOf.model_rebuild()
NotOf.model_rebuild()

_CONDITION = TypeAdapter[Condition](Condition)


def _depth(condition: Condition) -> int:
    match condition:
        case AllOf(all=items) | AnyOf(any=items):
            return 1 + max(_depth(c) for c in items)
        case NotOf(not_=inner):
            return 1 + _depth(inner)
        case _:
            return 1


def parse_condition(raw: Any) -> Condition:
    """Validate a condition; raises :class:`pydantic.ValidationError` or ``ValueError``."""
    condition = _CONDITION.validate_python(raw)
    if _depth(condition) > MAX_DEPTH:
        raise ValueError(f"condition is nested deeper than {MAX_DEPTH} levels")
    return condition


def dump_condition(condition: Condition) -> dict[str, Any]:
    """jsonb form (numbers as JSON strings keep their exact decimal value)."""
    return _CONDITION.dump_python(condition, mode="json", by_alias=True)  # type: ignore[no-any-return]


# ---------- evaluation and holds ----------

# Money leaving the client; conversions between own accounts are not checked.
FRAUD_CHECKED_TYPES = frozenset(
    {
        OperationType.TRANSFER_OUT,
        OperationType.EXTERNAL_TRANSFER,
        OperationType.SBP_TRANSFER,
        OperationType.PAYMENT,
    }
)
_COUNTED_STATUSES = (OperationStatus.PENDING, OperationStatus.PROCESSING, OperationStatus.COMPLETED)

_TICKET_SUBJECT = "Проверка операции службой безопасности"
_OPERATION_TITLES = {
    OperationType.TRANSFER_OUT: "перевод клиенту банка",
    OperationType.EXTERNAL_TRANSFER: "перевод в другой банк",
    OperationType.SBP_TRANSFER: "перевод по СБП",
    OperationType.PAYMENT: "оплата услуг",
}


@dataclass(frozen=True, slots=True)
class FraudHit:
    rule_id: uuid.UUID
    rule_name: str


@dataclass(frozen=True, slots=True)
class FraudHold:
    case: FraudCase
    operation: Operation
    ticket: SupportTicket
    message: SupportMessage


def held(operation: Operation) -> bool:
    return (
        operation.status is OperationStatus.PENDING
        and operation.error_code == TransferErrorCode.FRAUD_REVIEW
    )


class FraudService:
    def __init__(
        self, session: AsyncSession, publisher: EventPublisher, audit: AuditService
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._audit = audit

    async def evaluate(
        self, source: Account, amount: Decimal, fee: Decimal, operation: OperationType
    ) -> list[FraudHit]:
        """Enabled rules matching the operation (``source`` is locked by the caller)."""
        if operation not in FRAUD_CHECKED_TYPES:
            return []
        rules = list(
            await self._session.scalars(
                select(FraudRule).where(FraudRule.enabled.is_(True)).order_by(FraudRule.name)
            )
        )
        if not rules:
            return []
        context = await self._context(source, amount, fee, operation)
        hits = []
        for rule in rules:
            try:
                if parse_condition(rule.condition).matches(context):
                    hits.append(FraudHit(rule.id, rule.name))
            except (ValidationError, ValueError) as exc:
                # A broken rule must not block every payment: skip it and report.
                logger.error(  # noqa: TRY400 - expected, traceback is noise
                    "fraud rule skipped: invalid condition", rule=rule.name, error=str(exc)
                )
        return hits

    async def hold(
        self,
        operation: Operation,
        user: User,
        hits: list[FraudHit],
        resume: Mapping[str, Any] | None = None,
    ) -> FraudHold:
        """Stage the hold (pending operation, case, ticket); the caller commits.

        ``operation`` must be ``pending`` (just created); ``resume`` keeps what is needed
        to continue it after confirmation.
        """
        operation.error_code = TransferErrorCode.FRAUD_REVIEW
        # The case references the operation and the ticket by FK only (no relationship),
        # so the unit of work does not order the INSERTs: write them before the case.
        await self._session.flush()
        ticket = SupportTicket(
            id=uuid.uuid4(),
            user_id=user.id,
            subject=_TICKET_SUBJECT,
            category=TicketCategory.FRAUD,
            priority=TicketPriority.HIGH,
            status=TicketStatus.OPEN,
        )
        message = SupportMessage(
            id=uuid.uuid4(),
            ticket_id=ticket.id,
            author_id=None,  # automatic message of the bank
            from_staff=True,
            body=(
                f"Операция «{_OPERATION_TITLES.get(operation.type, operation.type.value)}» "
                f"на сумму {operation.amount} {operation.currency} приостановлена для проверки. "
                "Деньги зарезервированы на счёте. Мы сообщим о решении в этом обращении; "  # noqa: RUF001
                "если операцию совершали не вы, напишите нам."
            ),
        )
        self._session.add_all([ticket, message])
        await self._session.flush()  # FK-only references: tickets before the case
        case = FraudCase(
            id=uuid.uuid4(),
            operation_id=operation.id,
            user_id=user.id,
            status=FraudCaseStatus.OPEN,
            rules=[{"id": str(h.rule_id), "name": h.rule_name} for h in hits],
            resume=dict(resume or {}),
            ticket_id=ticket.id,
        )
        self._session.add(case)
        return FraudHold(case, operation, ticket, message)

    async def announce(self, hold: FraudHold) -> None:
        """After the commit: ``fraud.suspicious``, ``ticket.created``, audit."""
        operation, case = hold.operation, hold.case
        logger.warning(
            "operation held by anti-fraud rules",
            operation_id=str(operation.id),
            fraud_case_id=str(case.id),
            rules=[r["name"] for r in case.rules],
        )
        await self._publisher.publish_safely(
            EventType.FRAUD_SUSPICIOUS,
            {
                "fraud_case_id": str(case.id),
                "operation_id": str(operation.id),
                "account_id": str(operation.account_id),
                "user_id": str(case.user_id),
                "type": operation.type.value,
                "amount": format(operation.amount, "f"),
                "currency": operation.currency,
                "rules": case.rules,
                "ticket_id": str(hold.ticket.id),
            },
            key=str(operation.account_id),
        )
        await self._publisher.publish_safely(
            EventType.TICKET_CREATED,
            {
                "ticket_id": str(hold.ticket.id),
                "user_id": str(hold.ticket.user_id),
                "status": hold.ticket.status.value,
                "subject": hold.ticket.subject,
                "category": hold.ticket.category.value,
                "priority": hold.ticket.priority.value,
                "message_id": str(hold.message.id),
                "attachments": 0,
            },
            key=str(hold.ticket.id),
        )
        await self._audit.record(
            "fraud.operation_held",
            user_id=operation.user_id,
            entity="operation",
            entity_id=operation.id,
            payload={
                "fraud_case_id": case.id,
                "type": operation.type.value,
                "amount": operation.amount,
                "currency": operation.currency,
                "rules": case.rules,
            },
        )

    async def _context(
        self, source: Account, amount: Decimal, fee: Decimal, operation: OperationType
    ) -> dict[str, Scalar]:
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        hour_ago = now - timedelta(hours=1)
        row = (
            await self._session.execute(
                select(
                    func.count().filter(Operation.created_at >= hour_ago),
                    func.coalesce(
                        func.sum(Operation.amount).filter(
                            Operation.created_at >= day_start,
                            Operation.status.in_(_COUNTED_STATUSES),
                        ),
                        0,
                    ),
                ).where(
                    Operation.account_id == source.id,
                    Operation.type.in_(FRAUD_CHECKED_TYPES),
                    Operation.parent_operation_id.is_(None),
                    Operation.created_at >= min(day_start, hour_ago),
                )
            )
        ).one()
        return {
            "amount": amount,
            "fee": fee,
            "balance": source.balance,
            "currency": source.currency,
            "operation": operation.value,
            "hour": Decimal(now.hour),
            "weekday": Decimal(now.weekday()),
            "account_age_days": Decimal((now - source.created_at).days),
            "ops_last_hour": Decimal(row[0]),
            "spent_today": Decimal(row[1]),
        }


# ---------- back-office CRUD of rules ----------


class FraudRuleService:
    def __init__(self, session: AsyncSession, audit: AuditService) -> None:
        self._session = session
        self._audit = audit

    async def list_all(self) -> list[FraudRule]:
        return list(await self._session.scalars(select(FraudRule).order_by(FraudRule.name)))

    async def get(self, rule_id: uuid.UUID) -> FraudRule:
        rule = await self._session.get(FraudRule, rule_id)
        if rule is None:
            raise NotFoundError("Fraud rule not found")
        return rule

    async def create(
        self,
        admin: User,
        *,
        name: str,
        condition: Condition,
        description: str | None,
        enabled: bool,
    ) -> FraudRule:
        rule = FraudRule(
            name=name,
            condition=dump_condition(condition),
            description=description,
            enabled=enabled,
        )
        self._session.add(rule)
        await self._commit_unique(name)
        await self._record("admin.fraud_rule.create", admin, rule, {"after": _snapshot(rule)})
        return rule

    async def update(
        self, admin: User, rule_id: uuid.UUID, changes: Mapping[str, Any]
    ) -> FraudRule:
        """Partial update: ``changes`` holds only the sent fields (condition already parsed)."""
        rule = await self.get(rule_id)
        before = _snapshot(rule)
        for key, value in changes.items():
            setattr(rule, key, dump_condition(value) if key == "condition" else value)
        await self._commit_unique(rule.name)
        await self._record(
            "admin.fraud_rule.update", admin, rule, {"before": before, "after": _snapshot(rule)}
        )
        return rule

    async def delete(self, admin: User, rule_id: uuid.UUID) -> None:
        rule = await self.get(rule_id)
        before = _snapshot(rule)
        await self._session.delete(rule)
        await self._session.commit()
        await self._record("admin.fraud_rule.delete", admin, rule, {"before": before})

    async def _commit_unique(self, name: str) -> None:
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            raise ConflictError(f"Fraud rule {name!r} already exists") from exc

    async def _record(
        self, action: str, admin: User, rule: FraudRule, payload: dict[str, Any]
    ) -> None:
        logger.info(action, rule_id=str(rule.id), rule=rule.name)
        await self._audit.record(
            action, user_id=admin.id, entity="fraud_rule", entity_id=rule.id, payload=payload
        )


def _snapshot(rule: FraudRule) -> dict[str, Any]:
    return {
        "name": rule.name,
        "condition": rule.condition,
        "description": rule.description,
        "enabled": rule.enabled,
    }
