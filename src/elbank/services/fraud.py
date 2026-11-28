"""Anti-fraud rules: boolean expressions over an outgoing operation.

A rule is an expression such as::

    amount > 300000 and hour < 6
    operation in ("sbp_transfer", "external_transfer") and ops_last_hour >= 5
    account_age_days < 1 and amount > balance * 0.9

An enabled rule whose expression is true blocks the operation: it is stored as
``failed`` with ``error_code=FRAUD_SUSPECTED`` (see :class:`elbank.services.ledger.OutgoingChecks`).
Rules apply to operations that leave the client: internal transfers to any
account, interbank/SBP transfers and payments (not conversions between own accounts).

Expressions are parsed with :mod:`ast` and evaluated by a small interpreter: only
literals, the variables below, ``and/or/not``, comparisons (including ``in``) and
``+ - * /`` are allowed — no calls, attributes or subscripts, so a rule can never
execute code. Numbers are compared as :class:`~decimal.Decimal`.

Variables (:data:`VARIABLES`):

=================== ======================================================
amount              operation amount (account currency)
fee                 fee charged on top of the amount
balance             account balance before the operation
currency            account currency, e.g. ``"RUB"``
operation           operation type: transfer_out, external_transfer, sbp_transfer, payment
hour, weekday       UTC hour (0-23) and weekday (0 = Monday)
account_age_days    whole days since the account was opened
ops_last_hour       outgoing operations of the account during the last hour (any status)
spent_today         outgoing amount of the account today (UTC), pending and completed
=================== ======================================================
"""

import ast
import operator
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.audit import audit
from elbank.core.exceptions import ConflictError, NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Account, FraudRule, Operation, OperationStatus, OperationType, User

logger = get_logger(__name__)

type Value = Decimal | str | bool
type Context = Mapping[str, Value]

MAX_EXPRESSION_LENGTH = 1000
_MAX_NODES = 200

VARIABLES: dict[str, Value] = {
    # Sample values: a new rule is evaluated against them to catch type errors early.
    "amount": Decimal("1000.00"),
    "fee": Decimal("10.00"),
    "balance": Decimal("50000.00"),
    "currency": "RUB",
    "operation": OperationType.TRANSFER_OUT.value,
    "hour": Decimal(12),
    "weekday": Decimal(2),
    "account_age_days": Decimal(30),
    "ops_last_hour": Decimal(1),
    "spent_today": Decimal("5000.00"),
}

# Outgoing operations fraud rules apply to (same set as outgoing limits).
FRAUD_CHECKED_TYPES = frozenset(
    {
        OperationType.TRANSFER_OUT,
        OperationType.EXTERNAL_TRANSFER,
        OperationType.SBP_TRANSFER,
        OperationType.PAYMENT,
    }
)
_COUNTED_STATUSES = (
    OperationStatus.PENDING,
    OperationStatus.PROCESSING,
    OperationStatus.COMPLETED,
)

_COMPARE: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: operator.contains(b, a),
    ast.NotIn: lambda a, b: not operator.contains(b, a),
}
_ARITHMETIC: dict[type[ast.operator], Callable[[Decimal, Decimal], Decimal]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


class RuleSyntaxError(ValueError):
    """The expression is not a valid rule (unsupported syntax, unknown variable, wrong types)."""


class RuleEvaluationError(ValueError):
    """The expression failed on real data (type mismatch, division by zero)."""


@dataclass(frozen=True, slots=True)
class CompiledRule:
    expression: str
    tree: ast.expr

    def matches(self, context: Context) -> bool:
        result = _eval(self.tree, context)
        if not isinstance(result, bool):
            raise RuleEvaluationError("expression must evaluate to true/false")
        return result


def compile_rule(expression: str) -> CompiledRule:
    """Parse and validate a rule; raises :class:`RuleSyntaxError`."""
    if len(expression) > MAX_EXPRESSION_LENGTH:
        raise RuleSyntaxError(f"expression is longer than {MAX_EXPRESSION_LENGTH} characters")
    try:
        tree = ast.parse(expression.strip(), mode="eval").body
    except SyntaxError as exc:
        raise RuleSyntaxError(f"syntax error: {exc.msg}") from exc
    if sum(1 for _ in ast.walk(tree)) > _MAX_NODES:
        raise RuleSyntaxError("expression is too complex")
    _validate(tree)
    rule = CompiledRule(expression, tree)
    try:
        rule.matches(VARIABLES)
    except RuleEvaluationError as exc:
        raise RuleSyntaxError(str(exc)) from exc
    return rule


def _validate(node: ast.expr) -> None:  # noqa: PLR0912 - one branch per node type
    match node:
        case ast.BoolOp(values=values):
            for value in values:
                _validate(value)
        case ast.UnaryOp(op=ast.Not() | ast.USub(), operand=operand):
            _validate(operand)
        case ast.Compare(left=left, ops=ops, comparators=comparators):
            if any(type(op) not in _COMPARE for op in ops):
                raise RuleSyntaxError("unsupported comparison (use == != < <= > >= in, not in)")
            _validate(left)
            for comparator in comparators:
                _validate(comparator)
        case ast.BinOp(op=op, left=left, right=right):
            if type(op) not in _ARITHMETIC:
                raise RuleSyntaxError("only + - * / are allowed")
            _validate(left)
            _validate(right)
        case ast.Tuple(elts=elts) | ast.List(elts=elts) | ast.Set(elts=elts):
            for elt in elts:
                if not isinstance(elt, ast.Constant):
                    raise RuleSyntaxError("collections may contain literals only")
                _validate(elt)
        case ast.Constant(value=value):
            if value is None or not isinstance(value, bool | int | float | str):
                raise RuleSyntaxError(f"unsupported literal: {value!r}")
        case ast.Name(id=name):
            if name not in VARIABLES:
                allowed = ", ".join(sorted(VARIABLES))
                raise RuleSyntaxError(f"unknown variable {name!r} (allowed: {allowed})")
        case _:
            raise RuleSyntaxError(f"unsupported syntax: {type(node).__name__}")


def _literal(value: object) -> Value:
    if isinstance(value, bool | str):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise RuleEvaluationError(f"bad number {value!r}") from exc


def _eval(node: ast.expr, ctx: Context) -> Any:  # noqa: PLR0911, PLR0912 - one branch per node type
    match node:
        case ast.BoolOp(op=ast.And(), values=values):
            return all(_truth(_eval(v, ctx)) for v in values)
        case ast.BoolOp(op=ast.Or(), values=values):
            return any(_truth(_eval(v, ctx)) for v in values)
        case ast.UnaryOp(op=ast.Not(), operand=operand):
            return not _truth(_eval(operand, ctx))
        case ast.UnaryOp(op=ast.USub(), operand=operand):
            return -_number(_eval(operand, ctx))
        case ast.Compare(left=left, ops=ops, comparators=comparators):
            current = _eval(left, ctx)
            for op, comparator in zip(ops, comparators, strict=True):
                right = _eval(comparator, ctx)
                try:
                    ok = _COMPARE[type(op)](current, right)
                except TypeError as exc:
                    raise RuleEvaluationError(f"cannot compare {current!r} and {right!r}") from exc
                if not ok:
                    return False
                current = right
            return True
        case ast.BinOp(op=op, left=left, right=right):
            try:
                return _ARITHMETIC[type(op)](_number(_eval(left, ctx)), _number(_eval(right, ctx)))
            except (ArithmeticError, InvalidOperation) as exc:
                raise RuleEvaluationError(f"arithmetic error: {exc}") from exc
        case ast.Tuple(elts=elts) | ast.List(elts=elts) | ast.Set(elts=elts):
            return frozenset(_eval(e, ctx) for e in elts)
        case ast.Constant(value=value):
            return _literal(value)
        case ast.Name(id=name):
            return ctx[name]
        case _:
            raise RuleEvaluationError(f"unsupported syntax: {type(node).__name__}")


def _truth(value: Any) -> bool:
    if not isinstance(value, bool):
        raise RuleEvaluationError(f"{value!r} is not true/false: use a comparison")
    return value


def _number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, Decimal):
        raise RuleEvaluationError(f"{value!r} is not a number")
    return value


# ---------- evaluation against an operation ----------


@dataclass(frozen=True, slots=True)
class FraudHit:
    rule_id: uuid.UUID
    rule_name: str


class FraudChecker:
    """Evaluates the enabled rules against an outgoing operation (accounts locked by the caller)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def first_hit(
        self,
        source: Account,
        amount: Decimal,
        fee: Decimal,
        operation: OperationType,
        *,
        spent_today: Decimal | None = None,
    ) -> FraudHit | None:
        if operation not in FRAUD_CHECKED_TYPES:
            return None
        rules = list(
            await self._session.scalars(
                select(FraudRule).where(FraudRule.enabled.is_(True)).order_by(FraudRule.name)
            )
        )
        if not rules:
            return None

        context = await self._context(source, amount, fee, operation, spent_today)
        for rule in rules:
            try:
                if compile_rule(rule.expression).matches(context):
                    return FraudHit(rule.id, rule.name)
            except (RuleSyntaxError, RuleEvaluationError) as exc:
                # A rule that cannot be evaluated must not block every payment: skip, report.
                logger.error(  # noqa: TRY400 - expected, traceback is noise
                    "fraud rule failed", rule=rule.name, error=str(exc)
                )
        return None

    async def _context(
        self,
        source: Account,
        amount: Decimal,
        fee: Decimal,
        operation: OperationType,
        spent_today: Decimal | None,
    ) -> dict[str, Value]:
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        row = (
            await self._session.execute(
                select(
                    func.count().filter(Operation.created_at >= now - timedelta(hours=1)),
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
                    Operation.created_at >= min(day_start, now - timedelta(hours=1)),
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
            "spent_today": spent_today if spent_today is not None else Decimal(row[1]),
        }


# ---------- back-office CRUD ----------


class FraudRuleService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[FraudRule]:
        return list(await self._session.scalars(select(FraudRule).order_by(FraudRule.name)))

    async def get(self, rule_id: uuid.UUID) -> FraudRule:
        rule = await self._session.get(FraudRule, rule_id)
        if rule is None:
            raise NotFoundError("Fraud rule not found")
        return rule

    async def create(self, admin: User, name: str, expression: str, *, enabled: bool) -> FraudRule:
        rule = FraudRule(name=name, expression=_checked(expression), enabled=enabled)
        self._session.add(rule)
        await self._commit_unique(name)
        self._audit("admin.fraud_rule.create", admin, rule, {"after": _snapshot(rule)})
        return rule

    async def update(
        self, admin: User, rule_id: uuid.UUID, changes: Mapping[str, Any]
    ) -> FraudRule:
        """Partial update; ``changes`` holds only the sent fields (name, expression, enabled)."""
        rule = await self.get(rule_id)
        before = _snapshot(rule)
        if "expression" in changes:
            rule.expression = _checked(changes["expression"])
        if "name" in changes:
            rule.name = changes["name"]
        if "enabled" in changes:
            rule.enabled = changes["enabled"]
        await self._commit_unique(rule.name)
        self._audit(
            "admin.fraud_rule.update", admin, rule, {"before": before, "after": _snapshot(rule)}
        )
        return rule

    async def delete(self, admin: User, rule_id: uuid.UUID) -> None:
        rule = await self.get(rule_id)
        before = _snapshot(rule)
        await self._session.delete(rule)
        await self._session.commit()
        self._audit("admin.fraud_rule.delete", admin, rule, {"before": before})

    async def _commit_unique(self, name: str) -> None:
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            raise ConflictError(f"Fraud rule {name!r} already exists") from exc

    @staticmethod
    def _audit(action: str, admin: User, rule: FraudRule, details: dict[str, Any]) -> None:
        audit(
            action,
            actor_id=admin.id,
            user_id=admin.id,
            entity="fraud_rule",
            entity_id=rule.id,
            details=details,
        )
        logger.info(action, rule_id=str(rule.id), rule=rule.name)


def _checked(expression: str) -> str:
    try:
        return compile_rule(expression).expression.strip()
    except RuleSyntaxError as exc:
        raise UnprocessableError(f"Invalid rule expression: {exc}") from exc


def _snapshot(rule: FraudRule) -> dict[str, Any]:
    return {"name": rule.name, "expression": rule.expression, "enabled": rule.enabled}
