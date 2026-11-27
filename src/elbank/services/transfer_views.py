"""Read side of transfers of every channel: views, lookups and Kafka payloads.

A transfer is represented by its *main* operation (the one with the initiator and
the idempotency key); its type tells the channel.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.exceptions import NotFoundError
from elbank.db.models import Account, Operation, OperationType, User
from elbank.schemas.transfer import TransferChannel
from elbank.services.ledger import child_postings
from elbank.services.operation_service import CANCELLABLE_STATUSES

CHANNEL_BY_TYPE: dict[OperationType, TransferChannel] = {
    OperationType.TRANSFER_OUT: TransferChannel.INTERNAL,
    OperationType.EXTERNAL_TRANSFER: TransferChannel.EXTERNAL,
    OperationType.SBP_TRANSFER: TransferChannel.SBP,
    OperationType.CONVERSION_OUT: TransferChannel.CONVERSION,
}
_OWN_BANK_CHANNELS = frozenset({TransferChannel.INTERNAL, TransferChannel.CONVERSION})


@dataclass(frozen=True, slots=True)
class TransferView:
    operation: Operation  # the main operation
    channel: TransferChannel
    to_account_id: uuid.UUID | None  # an account of this bank (internal, conversion)
    fee: Decimal
    converted_amount: Decimal | None = None
    cancellable_until: datetime | None = None


@dataclass(frozen=True, slots=True)
class TransferResult:
    transfer: TransferView
    created: bool  # False: the idempotency key was seen before, stored result returned


def transfer_payload(view: TransferView) -> dict[str, Any]:
    """Payload of ``transfer.completed`` / ``transfer.failed`` / ``transfer.reversed``."""
    op = view.operation
    return {
        "operation_id": str(op.id),
        "type": view.channel.value,
        "user_id": str(op.user_id),
        "from_account_id": str(op.account_id),
        "to_account_id": str(view.to_account_id) if view.to_account_id else None,
        "counterparty": op.counterparty_account,
        "amount": format(op.amount, "f"),
        "fee": format(view.fee, "f"),
        "currency": op.currency,
        "converted_amount": (
            format(view.converted_amount, "f") if view.converted_amount is not None else None
        ),
        "exchange_rate": format(op.exchange_rate, "f") if op.exchange_rate is not None else None,
        "external_id": op.external_id,
        "status": op.status.value,
        "error_code": op.error_code,
    }


async def get_user_transfer(
    session: AsyncSession, user: User, operation_id: uuid.UUID, *, for_update: bool = False
) -> Operation:
    """The main operation of a transfer initiated by ``user`` (else 404)."""
    query = select(Operation).where(Operation.id == operation_id)
    if for_update:
        query = query.with_for_update().execution_options(populate_existing=True)
    transfer: Operation | None = await session.scalar(query)
    if (
        transfer is None
        or transfer.user_id != user.id
        or transfer.type not in CHANNEL_BY_TYPE
        or transfer.parent_operation_id is not None
    ):
        raise NotFoundError("Transfer not found")
    return transfer


async def load_view(
    session: AsyncSession, transfer: Operation, *, cancel_window_seconds: int
) -> TransferView:
    channel = CHANNEL_BY_TYPE[transfer.type]
    fee = Decimal(0)
    converted: Decimal | None = None
    to_account_id: uuid.UUID | None = None
    for posting in await child_postings(session, [transfer.id]):
        if posting.type is OperationType.FEE:
            fee = posting.amount
        elif posting.type is OperationType.CONVERSION_IN:
            to_account_id, converted = posting.account_id, posting.amount

    if to_account_id is None and channel in _OWN_BANK_CHANNELS:
        # A failed transfer has no credit posting: resolve the recipient by account number.
        to_account_id = await session.scalar(
            select(Account.id).where(Account.number == transfer.counterparty_account)
        )
    cancellable_until = None
    if channel is TransferChannel.INTERNAL and transfer.status in CANCELLABLE_STATUSES:
        cancellable_until = transfer.created_at + timedelta(seconds=cancel_window_seconds)
    return TransferView(transfer, channel, to_account_id, fee, converted, cancellable_until)
