"""Cards: issue (virtual / plastic), block, unblock, reissue, limits, MCC restrictions.

Every state change goes to the card processor first and is committed only after it
succeeds, so the bank never shows a state the processor does not have. The card row
is locked (``SELECT ... FOR UPDATE``) for the duration of the call; issuing locks the
account row instead, which also serializes the per-account card count check.

The full PAN exists only in memory between the processor's answer and the insert:
the DB gets its mask and HMAC (``CARDS_PAN_HASH_KEY``), logs and events — the mask.
"""

import calendar
import hashlib
import hmac
import uuid
from collections.abc import Awaitable
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients import (
    CardProcessorClient,
    CardProcessorRefusedError,
    CardProcessorUnavailableError,
    IssuedCard,
)
from elbank.core.config import CardSettings
from elbank.core.exceptions import (
    CardProcessorRejectedError,
    ExternalServiceError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.db.models import Account, AccountStatus, Card, CardStatus, CardType, Currency, User
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.card import (
    CardBlockReason,
    CardLimits,
    CardLimitsRead,
    CardMccRestrictionsRead,
    DeliveryAddress,
    MccRestrictions,
)

logger = get_logger(__name__)

# Cards counted against CARDS_MAX_PER_ACCOUNT and whose settings can still be changed.
_LIVE_STATUSES = (CardStatus.ACTIVE, CardStatus.BLOCKED)
_REISSUABLE_STATUSES = (CardStatus.ACTIVE, CardStatus.BLOCKED, CardStatus.EXPIRED)
_PAN_HASH_CONSTRAINT = "uq_cards_pan_hash"


def mask_pan(pan: str) -> str:
    """First 6 (BIN) and last 4 digits, the rest replaced with ``*``."""
    return f"{pan[:6]}{'*' * (len(pan) - 10)}{pan[-4:]}"


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _limits_to_json(limits: CardLimits) -> dict[str, Any]:
    return limits.model_dump(mode="json")


def _card_payload(card: Card, account: Account) -> dict[str, Any]:
    return {
        "card_id": str(card.id),
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "pan_masked": card.pan_masked,
        "type": card.type.value,
        "status": card.status.value,
    }


class CardService:
    def __init__(
        self,
        session: AsyncSession,
        processor: CardProcessorClient,
        publisher: EventPublisher,
        settings: CardSettings,
    ) -> None:
        self._session = session
        self._processor = processor
        self._publisher = publisher
        self._settings = settings

    # ---------- issue ----------

    async def issue_virtual(self, user: User, account_id: uuid.UUID) -> tuple[Card, Currency]:
        account = await self._lock_issuing_account(user, account_id)
        card = await self._issue(user, account, CardType.VIRTUAL)
        return card, Currency(account.currency)

    async def order_plastic(
        self, user: User, account_id: uuid.UUID, address: DeliveryAddress
    ) -> tuple[Card, Currency]:
        if not user.full_name:
            raise UnprocessableError(
                "Fill in full_name in the profile first: it is embossed on a plastic card"
            )
        account = await self._lock_issuing_account(user, account_id)
        card = await self._issue(
            user, account, CardType.PLASTIC, delivery_address=address.model_dump(mode="json")
        )
        return card, Currency(account.currency)

    async def reissue(self, user: User, card_id: uuid.UUID) -> tuple[Card, Currency]:
        old, account = await self._get_owned(user, card_id, for_update=True)
        if old.status not in _REISSUABLE_STATUSES:
            raise InvalidStateError(f"A {old.status.value} card cannot be reissued")
        self._require_active_account(account)

        card = await self._issue(
            user,
            account,
            old.type,
            delivery_address=old.delivery_address,
            limits=old.limits,
            mcc_restrictions=old.mcc_restrictions,
            replaces=old,
        )
        return card, Currency(account.currency)

    # ---------- read ----------

    async def list_for_user(self, user: User) -> list[Card]:
        rows = await self._session.scalars(
            select(Card)
            .join(Account, Card.account_id == Account.id)
            .where(Account.user_id == user.id)
            .order_by(Card.created_at, Card.id)
        )
        return list(rows)

    async def get_for_user(self, user: User, card_id: uuid.UUID) -> tuple[Card, Currency]:
        card, account = await self._get_owned(user, card_id)
        return card, Currency(account.currency)

    # ---------- block / unblock ----------

    async def block(
        self, user: User, card_id: uuid.UUID, reason: CardBlockReason
    ) -> tuple[Card, Currency]:
        card, account = await self._get_owned(user, card_id, for_update=True)
        if card.status is not CardStatus.ACTIVE:
            raise InvalidStateError(
                f"Only an active card can be blocked, it is {card.status.value}"
            )

        await self._call_processor(
            self._processor.block(card.pan_hash, card_id=card.id, reason=reason.value)
        )
        card.status = CardStatus.BLOCKED
        await self._session.commit()

        logger.info("card blocked", card_id=str(card.id), reason=reason.value)
        await self._publisher.publish_safely(
            EventType.CARD_BLOCKED,
            _card_payload(card, account) | {"reason": reason.value},
            key=str(account.user_id),
        )
        return card, Currency(account.currency)

    async def unblock(self, user: User, card_id: uuid.UUID) -> tuple[Card, Currency]:
        card, account = await self._get_owned(user, card_id, for_update=True)
        if card.status is not CardStatus.BLOCKED:
            raise InvalidStateError(
                f"Only a blocked card can be unblocked, it is {card.status.value}"
            )
        if card.expiry < datetime.now(UTC).date():
            raise InvalidStateError("Card has expired: reissue it instead")
        self._require_active_account(account)

        await self._call_processor(self._processor.unblock(card.pan_hash, card_id=card.id))
        card.status = CardStatus.ACTIVE
        await self._session.commit()

        logger.info("card unblocked", card_id=str(card.id))
        await self._publisher.publish_safely(
            EventType.CARD_UNBLOCKED, _card_payload(card, account), key=str(account.user_id)
        )
        return card, Currency(account.currency)

    # ---------- limits and MCC restrictions ----------

    async def get_limits(self, user: User, card_id: uuid.UUID) -> CardLimitsRead:
        card, account = await self._get_owned(user, card_id)
        return self._limits_read(card, account)

    async def update_limits(
        self, user: User, card_id: uuid.UUID, limits: CardLimits
    ) -> CardLimitsRead:
        card, account = await self._get_owned(user, card_id, for_update=True)
        self._require_live(card)

        card.limits = _limits_to_json(limits)
        await self._session.commit()
        logger.info("card limits updated", card_id=str(card.id), **card.limits)
        return self._limits_read(card, account)

    async def update_mcc_restrictions(
        self, user: User, card_id: uuid.UUID, restrictions: MccRestrictions
    ) -> CardMccRestrictionsRead:
        card, _ = await self._get_owned(user, card_id, for_update=True)
        self._require_live(card)

        card.mcc_restrictions = restrictions.model_dump(mode="json")
        await self._session.commit()
        logger.info(
            "card mcc restrictions updated",
            card_id=str(card.id),
            allow=len(restrictions.allow),
            deny=len(restrictions.deny),
        )
        return CardMccRestrictionsRead(
            card_id=card.id, allow=restrictions.allow, deny=restrictions.deny
        )

    # ---------- internals ----------

    async def _get_owned(
        self, user: User, card_id: uuid.UUID, *, for_update: bool = False
    ) -> tuple[Card, Account]:
        query = (
            select(Card, Account)
            .join(Account, Card.account_id == Account.id)
            .where(Card.id == card_id, Account.user_id == user.id)
        )
        if for_update:
            query = query.with_for_update(of=Card)
        row = (await self._session.execute(query)).tuples().one_or_none()
        if row is None:
            raise NotFoundError("Card not found")
        return row

    async def _lock_issuing_account(self, user: User, account_id: uuid.UUID) -> Account:
        account: Account | None = await self._session.scalar(
            select(Account)
            .where(Account.id == account_id, Account.user_id == user.id)
            .with_for_update()
        )
        if account is None:
            raise NotFoundError("Account not found")
        self._require_active_account(account)

        live_cards = await self._session.scalar(
            select(func.count())
            .select_from(Card)
            .where(Card.account_id == account.id, Card.status.in_(_LIVE_STATUSES))
        )
        if (live_cards or 0) >= self._settings.max_per_account:
            raise InvalidStateError(
                f"An account can have at most {self._settings.max_per_account} cards"
            )
        return account

    async def _issue(
        self,
        user: User,
        account: Account,
        card_type: CardType,
        *,
        delivery_address: dict[str, Any] | None = None,
        limits: dict[str, Any] | None = None,
        mcc_restrictions: dict[str, Any] | None = None,
        replaces: Card | None = None,
    ) -> Card:
        card_id = uuid.uuid4()
        issued: IssuedCard = await self._call_processor(
            self._processor.issue(
                card_id=card_id,
                card_type=card_type,
                account_number=account.number,
                currency=account.currency,
                holder_name=user.full_name,
                holder_phone=user.phone,
                delivery_address=delivery_address,
                replaces_pan_hash=replaces.pan_hash if replaces is not None else None,
            )
        )
        card = Card(
            id=card_id,
            account_id=account.id,
            pan_masked=mask_pan(issued.pan),
            pan_hash=self._hash_pan(issued.pan),
            expiry=_last_day_of_month(issued.expiry_year, issued.expiry_month),
            status=CardStatus.ACTIVE,
            type=card_type,
            limits=limits if limits is not None else self._default_limits(),
            mcc_restrictions=mcc_restrictions or {"allow": [], "deny": []},
            delivery_address=delivery_address,
            reissued_from_id=replaces.id if replaces is not None else None,
        )
        if replaces is not None:
            replaces.status = CardStatus.REISSUED
        self._session.add(card)
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if _PAN_HASH_CONSTRAINT not in str(exc.orig):
                raise
            logger.error(  # noqa: TRY400 - the traceback would carry the PAN hash, not useful
                "card processor returned a PAN that is already in use"
            )
            raise ExternalServiceError("Card processor error, try again later") from None

        logger.info(
            "card issued",
            card_id=str(card.id),
            account_id=str(account.id),
            type=card_type.value,
            pan_masked=card.pan_masked,
            reissued_from_id=str(replaces.id) if replaces is not None else None,
        )
        payload = _card_payload(card, account) | {
            "expiry": card.expiry.strftime("%m/%y"),
            "reissued_from_id": str(replaces.id) if replaces is not None else None,
        }
        await self._publisher.publish_safely(
            EventType.CARD_ISSUED, payload, key=str(account.user_id)
        )
        return card

    @staticmethod
    async def _call_processor[T](call: Awaitable[T]) -> T:
        try:
            return await call
        except CardProcessorRefusedError as exc:
            raise CardProcessorRejectedError(f"Card processor refused: {exc.message}") from exc
        except CardProcessorUnavailableError as exc:
            raise ExternalServiceError("Card processor is unavailable, try again later") from exc

    def _hash_pan(self, pan: str) -> str:
        key = self._settings.pan_hash_key.get_secret_value().encode()
        return hmac.new(key, pan.encode(), hashlib.sha256).hexdigest()

    def _default_limits(self) -> dict[str, Any]:
        return _limits_to_json(
            CardLimits(
                daily=self._settings.default_daily_limit,
                monthly=self._settings.default_monthly_limit,
                per_transaction=self._settings.default_per_transaction_limit,
            )
        )

    @staticmethod
    def _require_active_account(account: Account) -> None:
        if account.status is not AccountStatus.ACTIVE:
            raise InvalidStateError(f"Account must be active, it is {account.status.value}")

    @staticmethod
    def _require_live(card: Card) -> None:
        if card.status not in _LIVE_STATUSES:
            raise InvalidStateError(f"Settings of a {card.status.value} card cannot be changed")

    @staticmethod
    def _limits_read(card: Card, account: Account) -> CardLimitsRead:
        limits = CardLimits.model_validate(card.limits)
        return CardLimitsRead(
            card_id=card.id,
            currency=Currency(account.currency),
            daily=limits.daily,
            monthly=limits.monthly,
            per_transaction=limits.per_transaction,
        )
