"""Reconciliation of operations over a period.

``POST /api/v1/admin/reconciliation/run`` aggregates operations created in
``[date_from, date_to)`` (count and sum per type/status/currency), publishes
``reconciliation.requested`` with that snapshot and returns the run id. The
reconciliation worker (:mod:`elbank.workers.reconciliation`) recomputes the totals
in one consistent (repeatable read) snapshot, runs the ledger checks below and
writes ``{RECONCILIATION_DIR}/{run_id}.json``.

Checks (each lists up to ``RECONCILIATION_MAX_ISSUES_PER_CHECK`` items, the count is exact):

- ``transfer_credit``: an internal ``transfer_out`` must have a ``transfer_in`` of the
  same amount in the same status;
- ``conversion_credit``: a ``conversion_out`` must have a ``conversion_in`` of
  ``amount * exchange_rate`` rounded down to kopecks, in the same status;
- ``fee_status``: a fee must be in the status of its operation;
- ``cancellation_compensated``: a cancelled posting must have a completed compensation;
- ``payment_order_status``: a payment order and its debit must agree on completed/failed;
- ``stuck_operations``: created/pending/processing longer than
  ``RECONCILIATION_STUCK_AFTER_SECONDS``;
- ``negative_balances``: accounts with a balance below zero (whole bank, not the period);
- ``snapshot_drift``: totals that changed between the request and the run (informational:
  pending operations complete in the meantime).
"""

import json
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from elbank.core.audit import audit
from elbank.core.config import ReconciliationSettings
from elbank.core.exceptions import NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    Operation,
    OperationStatus,
    OperationType,
    PaymentOrder,
    PaymentOrderStatus,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.admin import (
    CheckResult,
    ReconciliationReport,
    ReconciliationStatus,
    TotalsRow,
)

logger = get_logger(__name__)

_OPEN_STATUSES = (OperationStatus.CREATED, OperationStatus.PENDING, OperationStatus.PROCESSING)


class ReconciliationRequest(BaseModel):
    """Payload of ``reconciliation.requested``."""

    run_id: uuid.UUID
    date_from: datetime
    date_to: datetime
    requested_by: uuid.UUID
    requested_at: datetime
    snapshot: list[TotalsRow]


def report_path(settings: ReconciliationSettings, run_id: uuid.UUID) -> Path:
    return settings.dir / f"{run_id}.json"


async def operation_totals(
    session: AsyncSession, date_from: datetime, date_to: datetime
) -> list[TotalsRow]:
    rows = await session.execute(
        select(
            Operation.type,
            Operation.status,
            Operation.currency,
            func.count(),
            func.coalesce(func.sum(Operation.amount), 0),
        )
        .where(Operation.created_at >= date_from, Operation.created_at < date_to)
        .group_by(Operation.type, Operation.status, Operation.currency)
        .order_by(Operation.type, Operation.status, Operation.currency)
    )
    return [
        TotalsRow(type=t, status=s, currency=c, count=n, amount=Decimal(a))
        for t, s, c, n, a in rows.tuples()
    ]


# ---------- API side ----------


@dataclass(frozen=True, slots=True)
class ReconciliationRun:
    run_id: uuid.UUID
    date_from: datetime
    date_to: datetime
    requested_at: datetime
    totals: list[TotalsRow]


class ReconciliationService:
    def __init__(
        self,
        session: AsyncSession,
        publisher: EventPublisher,
        settings: ReconciliationSettings,
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._settings = settings

    async def request(
        self, staff: User, date_from: datetime, date_to: datetime
    ) -> ReconciliationRun:
        if date_from >= date_to:
            raise UnprocessableError("date_from must be before date_to")
        if date_to - date_from > timedelta(days=self._settings.max_period_days):
            raise UnprocessableError(
                f"Period must not exceed {self._settings.max_period_days} days"
            )
        totals = await operation_totals(self._session, date_from, date_to)
        request = ReconciliationRequest(
            run_id=uuid.uuid4(),
            date_from=date_from,
            date_to=date_to,
            requested_by=staff.id,
            requested_at=datetime.now(UTC),
            snapshot=totals,
        )
        # Raises when Kafka is down: the run would never happen, so the caller must know.
        await self._publisher.publish(
            EventType.RECONCILIATION_REQUESTED,
            request.model_dump(mode="json"),
            key=str(request.run_id),
        )
        audit(
            "admin.reconciliation.run",
            actor_id=staff.id,
            user_id=staff.id,
            entity="reconciliation",
            entity_id=request.run_id,
            details={"date_from": date_from, "date_to": date_to, "rows": len(totals)},
        )
        logger.info("reconciliation requested", run_id=str(request.run_id))
        return ReconciliationRun(request.run_id, date_from, date_to, request.requested_at, totals)

    def load_report(self, run_id: uuid.UUID) -> ReconciliationReport:
        path = report_path(self._settings, run_id)
        try:
            raw = path.read_bytes()
        except FileNotFoundError as exc:
            raise NotFoundError("Report not found or not ready yet") from exc
        return ReconciliationReport.model_validate_json(raw)


# ---------- worker side ----------


class ReconciliationBuilder:
    def __init__(self, settings: ReconciliationSettings) -> None:
        self._settings = settings

    def exists(self, run_id: uuid.UUID) -> bool:
        return report_path(self._settings, run_id).exists()

    async def build(
        self, session: AsyncSession, request: ReconciliationRequest
    ) -> ReconciliationReport:
        """Run every check in the caller's (repeatable read) transaction."""
        period = (Operation.created_at >= request.date_from, Operation.created_at < request.date_to)
        totals = await operation_totals(session, request.date_from, request.date_to)
        checks = {
            "transfer_credit": await self._transfer_credit(session, period),
            "conversion_credit": await self._conversion_credit(session, period),
            "fee_status": await self._fee_status(session, period),
            "cancellation_compensated": await self._cancellations(session, period),
            "payment_order_status": await self._payment_orders(session, period),
            "stuck_operations": await self._stuck(session, period),
            "negative_balances": await self._negative_balances(session),
            "snapshot_drift": _drift(request.snapshot, totals),
        }
        failed = any(c.count for name, c in checks.items() if name != "snapshot_drift")
        return ReconciliationReport(
            run_id=request.run_id,
            status=ReconciliationStatus.MISMATCHES if failed else ReconciliationStatus.OK,
            date_from=request.date_from,
            date_to=request.date_to,
            requested_by=request.requested_by,
            requested_at=request.requested_at,
            generated_at=datetime.now(UTC),
            totals=totals,
            checks=checks,
        )

    def write(self, report: ReconciliationReport) -> Path:
        """Atomic write: a reader never sees a half-written report."""
        path = report_path(self._settings, report.run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, path)  # noqa: PTH105 - atomic rename over an existing file
        return path

    # ---------- checks ----------

    async def _collect(
        self, session: AsyncSession, description: str, query: Select[*tuple[Any, ...]]
    ) -> CheckResult:
        count = await session.scalar(select(func.count()).select_from(query.subquery())) or 0
        rows = await session.execute(query.limit(self._settings.max_issues_per_check))
        return CheckResult(
            description=description,
            count=count,
            items=[_jsonable(dict(r)) for r in rows.mappings()],
        )

    async def _transfer_credit(self, session: AsyncSession, period: tuple[Any, ...]) -> CheckResult:
        credit = aliased(Operation)
        query = (
            select(
                Operation.id.label("operation_id"),
                Operation.status,
                Operation.amount,
                credit.id.label("credit_id"),
                credit.status.label("credit_status"),
                credit.amount.label("credit_amount"),
            )
            .outerjoin(
                credit,
                and_(
                    credit.parent_operation_id == Operation.id,
                    credit.type == OperationType.TRANSFER_IN,
                    credit.reversed_operation_id.is_(None),
                ),
            )
            .where(
                *period,
                Operation.type == OperationType.TRANSFER_OUT,
                Operation.parent_operation_id.is_(None),
                Operation.status != OperationStatus.FAILED,  # failed checks book no credit
                or_(
                    credit.id.is_(None),
                    credit.amount != Operation.amount,
                    credit.status != Operation.status,
                ),
            )
            .order_by(Operation.created_at)
        )
        return await self._collect(
            session, "transfer_out without a matching transfer_in (amount, status)", query
        )

    async def _conversion_credit(
        self, session: AsyncSession, period: tuple[Any, ...]
    ) -> CheckResult:
        credit = aliased(Operation)
        expected = func.trunc(Operation.amount * Operation.exchange_rate, 2)
        query = (
            select(
                Operation.id.label("operation_id"),
                Operation.status,
                Operation.amount,
                Operation.exchange_rate,
                expected.label("expected_credit"),
                credit.id.label("credit_id"),
                credit.status.label("credit_status"),
                credit.amount.label("credit_amount"),
            )
            .outerjoin(
                credit,
                and_(
                    credit.parent_operation_id == Operation.id,
                    credit.type == OperationType.CONVERSION_IN,
                ),
            )
            .where(
                *period,
                Operation.type == OperationType.CONVERSION_OUT,
                Operation.status != OperationStatus.FAILED,
                or_(
                    credit.id.is_(None),
                    credit.amount != expected,
                    credit.status != Operation.status,
                ),
            )
            .order_by(Operation.created_at)
        )
        return await self._collect(
            session, "conversion_out without a matching conversion_in (rate, status)", query
        )

    async def _fee_status(self, session: AsyncSession, period: tuple[Any, ...]) -> CheckResult:
        parent = aliased(Operation)
        query = (
            select(
                Operation.id.label("fee_id"),
                Operation.status.label("fee_status"),
                Operation.amount,
                parent.id.label("operation_id"),
                parent.type.label("operation_type"),
                parent.status.label("operation_status"),
            )
            .join(parent, parent.id == Operation.parent_operation_id)
            .where(
                *period,
                Operation.type == OperationType.FEE,
                Operation.reversed_operation_id.is_(None),
                Operation.status != parent.status,
            )
            .order_by(Operation.created_at)
        )
        return await self._collect(session, "fee in a status other than its operation", query)

    async def _cancellations(self, session: AsyncSession, period: tuple[Any, ...]) -> CheckResult:
        compensation = aliased(Operation)
        query = (
            select(
                Operation.id.label("operation_id"),
                Operation.type,
                Operation.amount,
                Operation.account_id,
            )
            .where(
                *period,
                Operation.status == OperationStatus.CANCELLED,
                ~select(compensation.id)
                .where(
                    compensation.reversed_operation_id == Operation.id,
                    compensation.status == OperationStatus.COMPLETED,
                    compensation.amount == Operation.amount,
                )
                .exists(),
            )
            .order_by(Operation.created_at)
        )
        return await self._collect(
            session, "cancelled posting without a completed compensation", query
        )

    async def _payment_orders(self, session: AsyncSession, period: tuple[Any, ...]) -> CheckResult:
        order_done = PaymentOrder.status == PaymentOrderStatus.COMPLETED
        op_done = Operation.status == OperationStatus.COMPLETED
        order_failed = PaymentOrder.status == PaymentOrderStatus.FAILED
        op_failed = Operation.status == OperationStatus.FAILED
        query = (
            select(
                PaymentOrder.id.label("payment_order_id"),
                PaymentOrder.provider,
                PaymentOrder.status.label("order_status"),
                Operation.id.label("operation_id"),
                Operation.status.label("operation_status"),
                Operation.amount,
            )
            .join(Operation, Operation.id == PaymentOrder.operation_id)
            .where(*period, or_(order_done != op_done, order_failed != op_failed))
            .order_by(Operation.created_at)
        )
        return await self._collect(
            session, "payment order and its debit disagree on completed/failed", query
        )

    async def _stuck(self, session: AsyncSession, period: tuple[Any, ...]) -> CheckResult:
        threshold = datetime.now(UTC) - timedelta(seconds=self._settings.stuck_after_seconds)
        query = (
            select(
                Operation.id.label("operation_id"),
                Operation.type,
                Operation.status,
                Operation.amount,
                Operation.currency,
                Operation.error_code,
                Operation.created_at,
            )
            .where(
                *period,
                Operation.status.in_(_OPEN_STATUSES),
                Operation.created_at < threshold,
            )
            .order_by(Operation.created_at)
        )
        return await self._collect(
            session,
            f"not finished {self._settings.stuck_after_seconds} s after creation",
            query,
        )

    async def _negative_balances(self, session: AsyncSession) -> CheckResult:
        query = (
            select(
                Account.id.label("account_id"),
                Account.number,
                Account.currency,
                Account.balance,
            )
            .where(Account.balance < 0)
            .order_by(Account.balance)
        )
        return await self._collect(session, "accounts with a negative balance", query)


def _drift(snapshot: list[TotalsRow], totals: list[TotalsRow]) -> CheckResult:
    def index(rows: list[TotalsRow]) -> dict[tuple[str, str, str], TotalsRow]:
        return {(r.type.value, r.status.value, r.currency): r for r in rows}

    before, after = index(snapshot), index(totals)
    items = []
    for key in sorted(before.keys() | after.keys()):
        old, new = before.get(key), after.get(key)
        old_count, new_count = (old.count if old else 0), (new.count if new else 0)
        old_amount = old.amount if old else Decimal(0)
        new_amount = new.amount if new else Decimal(0)
        if old_count != new_count or old_amount != new_amount:
            items.append(
                {
                    "type": key[0],
                    "status": key[1],
                    "currency": key[2],
                    "requested_count": old_count,
                    "actual_count": new_count,
                    "requested_amount": str(old_amount),
                    "actual_amount": str(new_amount),
                }
            )
    return CheckResult(
        description="totals changed between the request and the run (informational)",
        count=len(items),
        items=items,
    )


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or isinstance(value, bool | int | str):  # enums are str
            out[key] = value
        else:
            out[key] = str(value)  # UUID, Decimal, datetime
    return out
