"""Operation lifecycle: the only place where ``Operation.status`` changes.

State machine::

    created    → pending | failed | cancelled
    pending    → processing | failed | cancelled
    processing → completed | failed

``completed``, ``failed`` and ``cancelled`` are terminal. An invalid transition
raises :class:`InvalidStateError` (HTTP 409). The service does not commit: the
caller owns the transaction.
"""

import uuid
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.exceptions import InvalidStateError
from elbank.core.logging import get_logger
from elbank.db.models import Operation, OperationStatus, OperationType

logger = get_logger(__name__)

_TRANSITIONS: dict[OperationStatus, frozenset[OperationStatus]] = {
    OperationStatus.CREATED: frozenset(
        {OperationStatus.PENDING, OperationStatus.FAILED, OperationStatus.CANCELLED}
    ),
    OperationStatus.PENDING: frozenset(
        {OperationStatus.PROCESSING, OperationStatus.FAILED, OperationStatus.CANCELLED}
    ),
    OperationStatus.PROCESSING: frozenset({OperationStatus.COMPLETED, OperationStatus.FAILED}),
    OperationStatus.COMPLETED: frozenset(),
    OperationStatus.FAILED: frozenset(),
    OperationStatus.CANCELLED: frozenset(),
}

CANCELLABLE_STATUSES = frozenset(
    s for s, targets in _TRANSITIONS.items() if OperationStatus.CANCELLED in targets
)


def can_transition(current: OperationStatus, target: OperationStatus) -> bool:
    return target in _TRANSITIONS[current]


class OperationService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def create_pending(
        self,
        *,
        account_id: uuid.UUID,
        type: OperationType,
        amount: Decimal,
        currency: str,
        user_id: uuid.UUID | None = None,
        parent_operation_id: uuid.UUID | None = None,
        reversed_operation_id: uuid.UUID | None = None,
        counterparty_account: str | None = None,
        description: str | None = None,
        idempotency_key: str | None = None,
    ) -> Operation:
        """Add a new operation to the session and move it ``created`` → ``pending``."""
        operation = Operation(
            id=uuid.uuid4(),
            account_id=account_id,
            user_id=user_id,
            type=type,
            amount=amount,
            currency=currency,
            status=OperationStatus.CREATED,
            parent_operation_id=parent_operation_id,
            reversed_operation_id=reversed_operation_id,
            counterparty_account=counterparty_account,
            description=description,
            idempotency_key=idempotency_key,
        )
        self._transition(operation, OperationStatus.PENDING)
        self._session.add(operation)
        return operation

    def mark_processing(self, operation: Operation) -> None:
        self._transition(operation, OperationStatus.PROCESSING)

    def mark_completed(self, operation: Operation) -> None:
        self._transition(operation, OperationStatus.COMPLETED)

    def mark_failed(self, operation: Operation, error_code: str) -> None:
        self._transition(operation, OperationStatus.FAILED)
        operation.error_code = error_code

    def cancel(self, operation: Operation) -> None:
        self._transition(operation, OperationStatus.CANCELLED)

    @staticmethod
    def _transition(operation: Operation, target: OperationStatus) -> None:
        current = operation.status
        if not can_transition(current, target):
            raise InvalidStateError(
                f"Operation cannot change status from {current.value} to {target.value}"
            )
        operation.status = target
        logger.debug(
            "operation status changed",
            operation_id=str(operation.id),
            old_status=current.value,
            new_status=target.value,
        )
