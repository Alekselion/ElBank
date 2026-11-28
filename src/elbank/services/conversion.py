"""Currency conversion between own accounts.

``amount`` is debited from the source account in its currency together with a fee
from the ``conversion`` tariff (default ``TRANSFERS_CONVERSION_FEE_PERCENT``);
``amount * rate`` (rounded down to kopecks/cents, in the bank's favour) is credited
to the target account. The rate comes from :class:`RatesService`: an admin override
or the Bank of Russia cross rate (same as ``GET /transfers/rates``). Everything
happens in one DB transaction with both accounts locked; the operations
``conversion_out`` (main), ``conversion_in`` and ``fee`` go
``pending → processing → completed`` and ``transfer.completed`` with
``type=conversion`` is published. A failed check (status, balance) stores a
``failed`` operation and publishes ``transfer.failed``. Outgoing limits do not
apply: the money stays with the client.
"""

import uuid
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import TransferSettings
from elbank.core.exceptions import IdempotencyKeyReusedError, NotFoundError, UnprocessableError
from elbank.core.logging import get_logger
from elbank.db.models import Account, Currency, Operation, OperationType, TariffCode, User
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.transfer import TransferChannel
from elbank.services.account_cache import AccountListCache
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.ledger import CENT, OutgoingChecks, lock_accounts
from elbank.services.operation_service import OperationService
from elbank.services.rates import RatesService
from elbank.services.tariffs import TariffService
from elbank.services.transfer_views import (
    TransferResult,
    TransferView,
    load_view,
    transfer_payload,
)

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Conversion:
    user: User
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: Decimal
    idempotency_key: str


class ConversionService:
    def __init__(
        self,
        session: AsyncSession,
        guard: IdempotencyGuard,
        checks: OutgoingChecks,
        *,
        rates: RatesService,
        tariffs: TariffService,
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: TransferSettings,
    ) -> None:
        self._session = session
        self._guard = guard
        self._checks = checks
        self._rates = rates
        self._tariffs = tariffs
        self._cache = cache
        self._publisher = publisher
        self._settings = settings
        self._ops = OperationService(session)

    async def convert(self, request: Conversion) -> TransferResult:
        async def replay(existing: Operation) -> TransferResult:
            view = await load_view(
                self._session,
                existing,
                cancel_window_seconds=self._settings.cancel_window_seconds,
            )
            same = (
                existing.type is OperationType.CONVERSION_OUT
                and existing.account_id == request.from_account_id
                and existing.amount == request.amount
                and view.to_account_id == request.to_account_id
            )
            if not same:
                raise IdempotencyKeyReusedError
            return TransferResult(view, created=False)

        return await self._guard.run(
            request.user.id,
            request.idempotency_key,
            execute=lambda: self._execute(request),
            replay=replay,
        )

    async def _execute(self, request: Conversion) -> TransferResult:
        # Resolve the rate before taking row locks: it may need an HTTP call.
        source, target = await self._own_accounts(request, lock=False)
        if source.currency == target.currency:
            raise UnprocessableError("Accounts have the same currency: use /internal")
        rate = await self._rates.get_rate(Currency(source.currency), Currency(target.currency))

        fee_rule = await self._tariffs.fee_rule(TariffCode.CONVERSION)
        source, target = await self._own_accounts(request, lock=True)
        fee = fee_rule.fee(request.amount)
        converted = (request.amount * rate.rate).quantize(CENT, rounding=ROUND_DOWN)
        if converted <= 0:
            raise UnprocessableError("Amount is too small to convert")

        error = await self._checks.check(
            source,
            request.amount,
            fee,
            operation=OperationType.CONVERSION_OUT,
            target=target,
            apply_limits=False,
        )
        main = self._ops.create_pending(
            account_id=source.id,
            user_id=request.user.id,
            type=OperationType.CONVERSION_OUT,
            amount=request.amount,
            currency=source.currency,
            counterparty_account=target.number,
            description=f"Conversion {source.currency} -> {target.currency}",
            idempotency_key=request.idempotency_key,
            exchange_rate=rate.rate,
        )
        if error is not None:
            self._ops.mark_failed(main, error)
            await self._session.commit()
            view = TransferView(main, TransferChannel.CONVERSION, target.id, Decimal(0))
            logger.info("conversion failed", operation_id=str(main.id), error_code=error.value)
            await self._publisher.publish_safely(
                EventType.TRANSFER_FAILED, transfer_payload(view), key=str(source.id)
            )
            return TransferResult(view, created=True)

        postings = [
            main,
            self._ops.create_pending(
                account_id=target.id,
                type=OperationType.CONVERSION_IN,
                amount=converted,
                currency=target.currency,
                parent_operation_id=main.id,
                counterparty_account=source.number,
                description=main.description,
                exchange_rate=rate.rate,
            ),
        ]
        if fee > 0:
            postings.append(
                self._ops.create_pending(
                    account_id=source.id,
                    type=OperationType.FEE,
                    amount=fee,
                    currency=source.currency,
                    parent_operation_id=main.id,
                    description="Currency conversion fee",
                )
            )
        source.balance -= request.amount + fee
        target.balance += converted
        for posting in postings:
            self._ops.mark_processing(posting)
            self._ops.mark_completed(posting)
        await self._session.commit()
        await self._cache.invalidate(request.user.id)

        view = TransferView(main, TransferChannel.CONVERSION, target.id, fee, converted)
        logger.info(
            "conversion completed",
            operation_id=str(main.id),
            amount=str(request.amount),
            converted=str(converted),
            rate=str(rate.rate),
            fee=str(fee),
        )
        await self._publisher.publish_safely(
            EventType.TRANSFER_COMPLETED, transfer_payload(view), key=str(source.id)
        )
        return TransferResult(view, created=True)

    async def _own_accounts(self, request: Conversion, *, lock: bool) -> tuple[Account, Account]:
        ids = {request.from_account_id, request.to_account_id}
        if lock:
            accounts = await lock_accounts(self._session, ids)
        else:
            accounts = {i: a for i in ids if (a := await self._session.get(Account, i))}
        source = accounts.get(request.from_account_id)
        target = accounts.get(request.to_account_id)
        if source is None or source.user_id != request.user.id:
            raise NotFoundError("Account not found")
        if target is None or target.user_id != request.user.id:
            raise NotFoundError("Recipient account not found")
        return source, target
