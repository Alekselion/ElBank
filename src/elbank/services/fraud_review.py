"""Manual decision on operations held by anti-fraud rules.

``POST /api/v1/admin/operations/{id}/confirm`` and ``/reject`` (manager, admin).
One transaction locks the case and the operation (in that order), applies the
channel-specific change and closes the case; after the commit:

========================  =====================================================
confirm                   internal transfer: the recipient is credited, the transfers
                          worker completes it after the cancel window;
                          interbank/SBP transfer, payment: ``processing``, then the
                          gateway/provider is called as for a new operation (the
                          response reports the result of that call)
reject                    operation (and its postings) ``failed`` / ``FRAUD_REJECTED``,
                          money returned, ``transfer.failed`` / ``payment.failed``
========================  =====================================================

The fraud ticket of the case gets a staff message with the decision and is resolved;
the decision is audited (``fraud.operation_confirmed`` / ``fraud.operation_rejected``).
"""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.exceptions import InvalidStateError, NotFoundError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    FraudCase,
    FraudCaseStatus,
    Operation,
    OperationType,
    SupportMessage,
    SupportTicket,
    TicketStatus,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.support import TicketEventKind
from elbank.services.account_cache import AccountListCache
from elbank.services.audit_service import AuditService
from elbank.services.external_transfers import ExternalTransferService
from elbank.services.fraud_service import held
from elbank.services.payments import PaymentService
from elbank.services.transfers import TransferService

logger = get_logger(__name__)

_CONFIRMED_TEXT = "Проверка завершена: операция подтверждена и продолжена. Спасибо за ожидание."
_REJECTED_TEXT = (
    "Проверка завершена: операция отклонена службой безопасности, деньги возвращены на счёт."
)


@dataclass(frozen=True, slots=True)
class ReviewResult:
    case: FraudCase
    operation: Operation


class FraudReviewService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        transfers: TransferService,
        external: ExternalTransferService,
        payments: PaymentService,
        cache: AccountListCache,
        publisher: EventPublisher,
        audit: AuditService,
    ) -> None:
        self._session = session
        self._transfers = transfers
        self._external = external
        self._payments = payments
        self._cache = cache
        self._publisher = publisher
        self._audit = audit

    async def list_cases(
        self, status: FraudCaseStatus | None, page: int, size: int
    ) -> tuple[list[tuple[FraudCase, Operation]], int]:
        conditions = [FraudCase.status == status] if status is not None else []
        total = await self._session.scalar(select(func.count(FraudCase.id)).where(*conditions))
        rows = await self._session.execute(
            select(FraudCase, Operation)
            .join(Operation, Operation.id == FraudCase.operation_id)
            .where(*conditions)
            .order_by(FraudCase.created_at.desc())
            .limit(size)
            .offset((page - 1) * size)
        )
        return [(case, op) for case, op in rows.tuples()], total or 0

    async def confirm(
        self, manager: User, operation_id: uuid.UUID, reason: str | None
    ) -> ReviewResult:
        case, operation = await self._lock(operation_id)
        touched: list[Account] = []
        match operation.type:
            case OperationType.TRANSFER_OUT:
                touched = await self._transfers.release_held(operation)
            case OperationType.EXTERNAL_TRANSFER | OperationType.SBP_TRANSFER:
                self._external.release_held(operation)
            case OperationType.PAYMENT:
                await self._payments.release_held(operation)
            case _:
                raise InvalidStateError(f"Unsupported held operation type {operation.type.value}")
        ticket = await self._close_case(case, FraudCaseStatus.CONFIRMED, manager, reason)
        await self._session.commit()

        await self._invalidate(touched)
        await self._after_decision(case, operation, ticket, manager)
        # Continue the external part now that the decision is durable.
        match operation.type:
            case OperationType.EXTERNAL_TRANSFER | OperationType.SBP_TRANSFER:
                operation = (await self._external.continue_held(operation, case.resume)).operation
            case OperationType.PAYMENT:
                operation = (await self._payments.continue_held(operation)).operation
            case _:
                pass
        return ReviewResult(case, operation)

    async def reject(
        self, manager: User, operation_id: uuid.UUID, reason: str | None
    ) -> ReviewResult:
        case, operation = await self._lock(operation_id)
        publish: Callable[[], Awaitable[None]]
        match operation.type:
            case OperationType.TRANSFER_OUT:
                transfer_view, touched = await self._transfers.reject_held(operation)
                publish = lambda: self._transfers.publish_failed(transfer_view)  # noqa: E731
            case OperationType.EXTERNAL_TRANSFER | OperationType.SBP_TRANSFER:
                external_view, touched = await self._external.reject_held(operation)
                publish = lambda: self._external.publish_failed(external_view)  # noqa: E731
            case OperationType.PAYMENT:
                payment_view, touched = await self._payments.reject_held(operation)
                publish = lambda: self._payments.publish_failed(payment_view)  # noqa: E731
            case _:
                raise InvalidStateError(f"Unsupported held operation type {operation.type.value}")
        ticket = await self._close_case(case, FraudCaseStatus.REJECTED, manager, reason)
        await self._session.commit()

        await self._invalidate(touched)
        await publish()
        await self._after_decision(case, operation, ticket, manager)
        return ReviewResult(case, operation)

    # ---------- internals ----------

    async def _lock(self, operation_id: uuid.UUID) -> tuple[FraudCase, Operation]:
        case = await self._session.scalar(
            select(FraudCase)
            .where(FraudCase.operation_id == operation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if case is None:
            raise NotFoundError("No anti-fraud case for this operation")
        if case.status is not FraudCaseStatus.OPEN:
            raise InvalidStateError(f"The case is already {case.status.value}")
        operation = await self._session.scalar(
            select(Operation)
            .where(Operation.id == operation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert operation is not None
        if not held(operation):
            raise InvalidStateError(f"Operation is not held (status {operation.status.value})")
        return case, operation

    async def _close_case(
        self, case: FraudCase, status: FraudCaseStatus, manager: User, reason: str | None
    ) -> tuple[SupportTicket, TicketStatus] | None:
        """Close the case; resolve its ticket. Returns the ticket and its previous status."""
        case.status = status
        case.decided_by_id = manager.id
        case.decided_at = datetime.now(UTC)
        case.decision_reason = reason
        if case.ticket_id is None:
            return None
        ticket = await self._session.get(SupportTicket, case.ticket_id, with_for_update=True)
        if ticket is None or ticket.status is TicketStatus.CLOSED:
            return None
        text = _CONFIRMED_TEXT if status is FraudCaseStatus.CONFIRMED else _REJECTED_TEXT
        self._session.add(
            SupportMessage(
                id=uuid.uuid4(),
                ticket_id=ticket.id,
                author_id=manager.id,
                from_staff=True,
                body=text,
            )
        )
        old_status = ticket.status
        ticket.status = TicketStatus.RESOLVED
        if ticket.assignee_id is None:
            ticket.assignee_id = manager.id
        return ticket, old_status

    async def _invalidate(self, accounts: list[Account]) -> None:
        for user_id in {a.user_id for a in accounts}:
            await self._cache.invalidate(user_id)

    async def _after_decision(
        self,
        case: FraudCase,
        operation: Operation,
        ticket: tuple[SupportTicket, TicketStatus] | None,
        manager: User,
    ) -> None:
        confirmed = case.status is FraudCaseStatus.CONFIRMED
        logger.info(
            "fraud case decided",
            fraud_case_id=str(case.id),
            operation_id=str(operation.id),
            decision=case.status.value,
        )
        if ticket is not None:
            resolved, old_status = ticket
            await self._publisher.publish_safely(
                EventType.TICKET_UPDATED,
                {
                    "ticket_id": str(resolved.id),
                    "user_id": str(resolved.user_id),
                    "status": resolved.status.value,
                    "kind": TicketEventKind.STATUS.value,
                    "old_status": old_status.value,
                    "assignee_id": str(resolved.assignee_id) if resolved.assignee_id else None,
                },
                key=str(resolved.id),
            )
        await self._audit.record(
            "fraud.operation_confirmed" if confirmed else "fraud.operation_rejected",
            user_id=manager.id,
            entity="operation",
            entity_id=operation.id,
            payload={
                "fraud_case_id": case.id,
                "client_id": case.user_id,
                "rules": case.rules,
                "reason": case.decision_reason,
            },
        )
