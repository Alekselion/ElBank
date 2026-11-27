"""Accounts: opening (KYC-approved users only), listing, blocking, closing, limits."""

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import AccountSettings, BankSettings
from elbank.core.exceptions import InvalidStateError, KycRequiredError, NotFoundError
from elbank.core.logging import get_logger
from elbank.db.models import (
    Account,
    AccountLimit,
    AccountStatus,
    AccountType,
    Currency,
    KycRequest,
    KycStatus,
    Operation,
    OperationStatus,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.account import AccountLimitsRead, AccountRead
from elbank.services.account_cache import AccountListCache
from elbank.services.requisites import Requisites, build_requisites, generate_account_number

logger = get_logger(__name__)

# A random 7-digit personal part rarely collides; retry a few times if it does.
_NUMBER_ATTEMPTS = 5
_NUMBER_CONSTRAINT = "uq_accounts_number"
_UNFINISHED_OPERATIONS = (
    OperationStatus.CREATED,
    OperationStatus.PENDING,
    OperationStatus.PROCESSING,
)


def _is_number_conflict(exc: IntegrityError) -> bool:
    return _NUMBER_CONSTRAINT in str(exc.orig)


def _account_payload(account: Account) -> dict[str, Any]:
    return {
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "number": account.number,
        "type": account.type.value,
        "currency": account.currency,
        "status": account.status.value,
    }


class AccountService:
    def __init__(
        self,
        session: AsyncSession,
        cache: AccountListCache,
        publisher: EventPublisher,
        bank: BankSettings,
        settings: AccountSettings,
    ) -> None:
        self._session = session
        self._cache = cache
        self._publisher = publisher
        self._bank = bank
        self._settings = settings

    # ---------- client side ----------

    async def open(self, user: User, account_type: AccountType, currency: Currency) -> Account:
        if not await self._is_kyc_approved(user):
            raise KycRequiredError

        account = await self._insert_account(user, account_type, currency)
        self._session.add(
            AccountLimit(
                account_id=account.id,
                daily_limit=self._settings.default_daily_limit,
                monthly_limit=self._settings.default_monthly_limit,
            )
        )
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info(
            "account opened",
            account_id=str(account.id),
            user_id=str(user.id),
            type=account.type.value,
            currency=account.currency,
        )
        await self._publisher.publish_safely(
            EventType.ACCOUNT_OPENED, _account_payload(account), key=str(user.id)
        )
        return account

    async def list_for_user(self, user: User) -> list[AccountRead]:
        cached = await self._cache.get(user.id)
        if cached is not None:
            return cached
        rows = await self._session.scalars(
            select(Account)
            .where(Account.user_id == user.id)
            .order_by(Account.created_at, Account.id)
        )
        accounts = [AccountRead.model_validate(a) for a in rows]
        await self._cache.set(user.id, accounts)
        return accounts

    async def get_for_user(
        self, user: User, account_id: uuid.UUID, *, for_update: bool = False
    ) -> Account:
        account = await self._get(account_id, for_update=for_update)
        if account is None or account.user_id != user.id:
            raise NotFoundError("Account not found")
        return account

    async def requisites(self, user: User, account_id: uuid.UUID) -> Requisites:
        account = await self.get_for_user(user, account_id)
        return build_requisites(
            user.full_name, account.number, Currency(account.currency), self._bank
        )

    async def close(self, user: User, account_id: uuid.UUID) -> Account:
        account = await self.get_for_user(user, account_id, for_update=True)
        if account.status is not AccountStatus.ACTIVE:
            raise InvalidStateError(
                f"Only an active account can be closed, it is {account.status.value}"
            )
        if account.balance != 0:
            raise InvalidStateError("Account balance must be zero to close it")
        if await self._has_unfinished_operations(account):
            raise InvalidStateError("Account has operations in progress")

        account.status = AccountStatus.CLOSED
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info("account closed", account_id=str(account.id), user_id=str(user.id))
        await self._publisher.publish_safely(
            EventType.ACCOUNT_CLOSED, _account_payload(account), key=str(user.id)
        )
        return account

    # ---------- limits ----------

    async def get_limits(self, user: User, account_id: uuid.UUID) -> AccountLimitsRead:
        account = await self.get_for_user(user, account_id)
        limit = await self._session.scalar(
            select(AccountLimit).where(AccountLimit.account_id == account.id)
        )
        if limit is None:  # accounts opened before limits were assigned on opening
            return self._limits_read(
                account, self._settings.default_daily_limit, self._settings.default_monthly_limit
            )
        return self._limits_read(account, limit.daily_limit, limit.monthly_limit)

    async def update_limits(
        self, user: User, account_id: uuid.UUID, daily_limit: Decimal, monthly_limit: Decimal
    ) -> AccountLimitsRead:
        account = await self.get_for_user(user, account_id, for_update=True)
        if account.status is AccountStatus.CLOSED:
            raise InvalidStateError("Limits of a closed account cannot be changed")

        await self._session.execute(
            insert(AccountLimit)
            .values(account_id=account.id, daily_limit=daily_limit, monthly_limit=monthly_limit)
            .on_conflict_do_update(
                index_elements=[AccountLimit.account_id],
                set_={"daily_limit": daily_limit, "monthly_limit": monthly_limit},
            )
        )
        await self._session.commit()
        logger.info(
            "account limits updated",
            account_id=str(account.id),
            daily_limit=str(daily_limit),
            monthly_limit=str(monthly_limit),
        )
        return self._limits_read(account, daily_limit, monthly_limit)

    # ---------- back office (manager, admin) ----------

    async def block(self, staff: User, account_id: uuid.UUID) -> Account:
        return await self._change_status(
            staff,
            account_id,
            expected=AccountStatus.ACTIVE,
            new=AccountStatus.FROZEN,
            event=EventType.ACCOUNT_BLOCKED,
        )

    async def unblock(self, staff: User, account_id: uuid.UUID) -> Account:
        return await self._change_status(
            staff,
            account_id,
            expected=AccountStatus.FROZEN,
            new=AccountStatus.ACTIVE,
            event=EventType.ACCOUNT_UNBLOCKED,
        )

    # ---------- internals ----------

    async def _get(self, account_id: uuid.UUID, *, for_update: bool) -> Account | None:
        query = select(Account).where(Account.id == account_id)
        if for_update:
            query = query.with_for_update()
        result: Account | None = await self._session.scalar(query)
        return result

    async def _is_kyc_approved(self, user: User) -> bool:
        approved = await self._session.scalar(
            select(
                exists().where(
                    KycRequest.user_id == user.id, KycRequest.status == KycStatus.APPROVED
                )
            )
        )
        return bool(approved)

    async def _insert_account(
        self, user: User, account_type: AccountType, currency: Currency
    ) -> Account:
        for _ in range(_NUMBER_ATTEMPTS):
            account = Account(
                id=uuid.uuid4(),
                user_id=user.id,
                number=generate_account_number(account_type, currency, self._bank),
                type=account_type,
                currency=currency.value,
                status=AccountStatus.ACTIVE,
            )
            try:
                async with self._session.begin_nested():
                    self._session.add(account)
                    await self._session.flush()
            except IntegrityError as exc:
                if not _is_number_conflict(exc):
                    raise
                logger.warning("account number collision, regenerating", number=account.number)
                continue
            return account
        raise RuntimeError(
            f"Could not generate a unique account number in {_NUMBER_ATTEMPTS} tries"
        )

    async def _has_unfinished_operations(self, account: Account) -> bool:
        found = await self._session.scalar(
            select(
                exists().where(
                    Operation.account_id == account.id,
                    Operation.status.in_(_UNFINISHED_OPERATIONS),
                )
            )
        )
        return bool(found)

    async def _change_status(
        self,
        staff: User,
        account_id: uuid.UUID,
        *,
        expected: AccountStatus,
        new: AccountStatus,
        event: EventType,
    ) -> Account:
        account = await self._get(account_id, for_update=True)
        if account is None:
            raise NotFoundError("Account not found")
        if account.status is not expected:
            raise InvalidStateError(
                f"Account must be {expected.value} for this action, it is {account.status.value}"
            )

        account.status = new
        await self._session.commit()
        await self._cache.invalidate(account.user_id)

        logger.info(
            "account status changed",
            account_id=str(account.id),
            status=new.value,
            staff_id=str(staff.id),
        )
        payload = _account_payload(account) | {"changed_by": str(staff.id)}
        await self._publisher.publish_safely(event, payload, key=str(account.user_id))
        return account

    @staticmethod
    def _limits_read(account: Account, daily: Decimal, monthly: Decimal) -> AccountLimitsRead:
        return AccountLimitsRead(
            account_id=account.id,
            currency=Currency(account.currency),
            daily_limit=daily,
            monthly_limit=monthly,
        )
