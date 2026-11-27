"""Credits: application with bureau scoring, OTP signing, annuity schedule, repayment.

Lifecycle::

    POST /applications → scoring → approved (schedule, OTP sent) | rejected
    POST /{id}/sign    → OTP checked → active: amount credited to the account
    POST /{id}/repay   → installments paid / principal reduced → closed when repaid

Lock order: the credit row first, then the account (as everywhere: operations /
products before accounts).
"""

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.clients import BkiClient, BkiDecision, BkiUnavailableError
from elbank.core.config import CreditSettings, OtpSettings
from elbank.core.exceptions import (
    ExternalServiceError,
    InvalidOtpCodeError,
    InvalidStateError,
    NotFoundError,
    UnprocessableError,
)
from elbank.core.logging import get_logger
from elbank.core.security import OtpPurpose
from elbank.db.models import (
    Account,
    AccountStatus,
    Credit,
    CreditStatus,
    Currency,
    OperationType,
    User,
)
from elbank.kafka import EventPublisher, EventType
from elbank.schemas.product import CreditRead, CreditRepayResult, SignOtpSent
from elbank.services.account_cache import AccountListCache
from elbank.services.finance import (
    ZERO,
    annuity_payment,
    annuity_schedule,
    due_dates_from,
    fmt,
    installment_left,
    is_paid,
    is_untouched,
)
from elbank.services.ledger import lock_accounts
from elbank.services.operation_service import OperationService
from elbank.services.otp import OtpService

logger = get_logger(__name__)

_REPAYABLE = (CreditStatus.ACTIVE, CreditStatus.OVERDUE)
_RATE_STEP = Decimal("0.0001")  # credits.rate is NUMERIC(7, 4)


def _now() -> datetime:
    return datetime.now(UTC)


def _credit_payload(credit: Credit) -> dict[str, Any]:
    return {
        "credit_id": str(credit.id),
        "user_id": str(credit.user_id),
        "account_id": str(credit.account_id),
        "amount": fmt(credit.amount),
        "rate": format(credit.rate, "f"),
        "term_months": credit.term_months,
        "status": credit.status.value,
        "score": credit.score,
    }


def left_to_pay(credit: Credit) -> Decimal:
    return sum((sum(installment_left(e), ZERO) for e in credit.schedule if not is_paid(e)), ZERO)


class CreditService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        bki: BkiClient,
        otp: OtpService,
        cache: AccountListCache,
        publisher: EventPublisher,
        settings: CreditSettings,
        otp_settings: OtpSettings,
    ) -> None:
        self._session = session
        self._bki = bki
        self._otp = otp
        self._cache = cache
        self._publisher = publisher
        self._settings = settings
        self._otp_settings = otp_settings
        self._ops = OperationService(session)

    # ---------- application ----------

    async def apply(
        self, user: User, account_id: uuid.UUID, amount: Decimal, term_months: int, purpose: str
    ) -> Credit:
        self._check_bounds(amount, term_months)
        account = await self._session.get(Account, account_id)
        if account is None or account.user_id != user.id:
            raise NotFoundError("Account not found")
        if account.currency != Currency.RUB:
            raise UnprocessableError("Credits are issued to RUB accounts only")
        self._require_active(account)

        credit_id = uuid.uuid4()
        try:
            scoring = await self._bki.score(
                application_id=credit_id,
                full_name=user.full_name,
                phone=user.phone,
                amount=amount,
                term_months=term_months,
                purpose=purpose,
            )
        except BkiUnavailableError as exc:
            raise ExternalServiceError("Credit bureau is unavailable, try again later") from exc

        approved = (
            scoring.decision is BkiDecision.APPROVE and scoring.score >= self._settings.min_score
        )
        rate = (
            self._settings.good_score_rate
            if scoring.score >= self._settings.good_score
            else self._settings.base_rate
        ).quantize(_RATE_STEP)
        credit = Credit(
            id=credit_id,
            user_id=user.id,
            account_id=account.id,
            amount=amount,
            rate=rate,
            term_months=term_months,
            purpose=purpose,
            status=CreditStatus.APPROVED if approved else CreditStatus.REJECTED,
            score=scoring.score,
            scoring_comment=scoring.comment,
            principal_outstanding=ZERO,
        )
        if approved:
            # Indicative schedule; the dates are fixed again from the signing day.
            credit.schedule = annuity_schedule(
                amount, rate, due_dates_from(_now().date(), term_months)
            )
            credit.monthly_payment = annuity_payment(amount, rate, term_months)
        self._session.add(credit)
        await self._session.commit()

        logger.info(
            "credit application decided",
            credit_id=str(credit.id),
            status=credit.status.value,
            score=scoring.score,
            decision=scoring.decision.value,
        )
        event = EventType.CREDIT_APPROVED if approved else EventType.CREDIT_REJECTED
        await self._publisher.publish_safely(event, _credit_payload(credit), key=str(user.id))
        if approved:
            await self._send_sign_otp_quietly(user, credit)
        return credit

    # ---------- read ----------

    async def list_for_user(self, user: User) -> list[Credit]:
        rows = await self._session.scalars(
            select(Credit)
            .where(Credit.user_id == user.id)
            .order_by(Credit.created_at.desc(), Credit.id)
        )
        return list(rows)

    async def get_for_user(
        self, user: User, credit_id: uuid.UUID, *, for_update: bool = False
    ) -> Credit:
        query = select(Credit).where(Credit.id == credit_id, Credit.user_id == user.id)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        credit: Credit | None = await self._session.scalar(query)
        if credit is None:
            raise NotFoundError("Credit not found")
        return credit

    # ---------- signing ----------

    async def send_sign_otp(self, user: User, credit_id: uuid.UUID) -> SignOtpSent:
        credit = await self.get_for_user(user, credit_id)
        self._require_status(credit, CreditStatus.APPROVED, "signed")
        await self._otp.ensure_can_send(user.phone, OtpPurpose.CREDIT_SIGN)
        await self._otp.send(user.phone, OtpPurpose.CREDIT_SIGN, user.id)
        return SignOtpSent(
            expires_in=self._otp_settings.ttl_seconds,
            resend_after=self._otp_settings.resend_cooldown_seconds,
        )

    async def sign(self, user: User, credit_id: uuid.UUID, otp_code: str) -> Credit:
        credit = await self.get_for_user(user, credit_id, for_update=True)
        self._require_status(credit, CreditStatus.APPROVED, "signed")
        account = await self._lock_account(credit)
        self._require_active(account)  # checked before the code is burned

        verification = await self._otp.check_codes([(user.phone, otp_code)], OtpPurpose.CREDIT_SIGN)
        if not verification.verified:
            raise InvalidOtpCodeError(
                f"OTP code is invalid or expired, attempts left: {verification.attempts_left}"
            )

        now = _now()
        credit.schedule = annuity_schedule(
            credit.amount, credit.rate, due_dates_from(now.date(), credit.term_months)
        )
        credit.monthly_payment = annuity_payment(credit.amount, credit.rate, credit.term_months)
        credit.principal_outstanding = credit.amount
        credit.status = CreditStatus.ACTIVE
        credit.signed_at = now
        account.balance += credit.amount
        self._ops.create_completed(
            account_id=account.id,
            user_id=user.id,
            type=OperationType.CREDIT_DISBURSEMENT,
            amount=credit.amount,
            currency=account.currency,
            description=f"Credit {credit.id}",
        )
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info("credit signed and disbursed", credit_id=str(credit.id))
        await self._publisher.publish_safely(
            EventType.CREDIT_SIGNED, _credit_payload(credit), key=str(user.id)
        )
        return credit

    # ---------- repayment ----------

    async def repay(
        self, user: User, credit_id: uuid.UUID, amount: Decimal, *, early: bool
    ) -> CreditRepayResult:
        credit = await self.get_for_user(user, credit_id, for_update=True)
        if credit.status not in _REPAYABLE:
            raise InvalidStateError(f"A {credit.status.value} credit cannot be repaid")
        account = await self._lock_account(credit)
        self._require_active(account)

        # Validates the amount against the debt first; on any error below nothing is
        # committed (the request's session is rolled back).
        if early:
            paid_interest, paid_principal = ZERO, self._repay_early(credit, amount)
        else:
            paid_interest, paid_principal = self._repay_installments(credit, amount)
        if account.balance < amount:
            raise UnprocessableError("Insufficient funds on the account")

        account.balance -= amount
        operation = self._ops.create_completed(
            account_id=account.id,
            user_id=user.id,
            type=OperationType.CREDIT_REPAYMENT,
            amount=amount,
            currency=account.currency,
            description=f"{'Early r' if early else 'R'}epayment of credit {credit.id}",
        )
        closed = all(is_paid(e) for e in credit.schedule)
        if closed:
            assert credit.principal_outstanding == 0, "repaid schedule with principal left"
            credit.status = CreditStatus.CLOSED
            credit.closed_at = _now()
        await self._session.commit()
        await self._cache.invalidate(user.id)

        logger.info(
            "credit repaid",
            credit_id=str(credit.id),
            amount=fmt(amount),
            early=early,
            principal_outstanding=fmt(credit.principal_outstanding),
            closed=closed,
        )
        if closed:
            await self._publisher.publish_safely(
                EventType.CREDIT_CLOSED, _credit_payload(credit), key=str(user.id)
            )
        return CreditRepayResult(
            credit=CreditRead.model_validate(credit),
            operation_id=operation.id,
            paid_interest=paid_interest,
            paid_principal=paid_principal,
        )

    def _repay_installments(self, credit: Credit, amount: Decimal) -> tuple[Decimal, Decimal]:
        """Pay installments in due order, interest before principal."""
        total_left = left_to_pay(credit)
        if amount > total_left:
            raise UnprocessableError(f"Amount exceeds what is left to pay: {fmt(total_left)}")

        now = _now().isoformat()
        rest, paid_interest, paid_principal = amount, ZERO, ZERO
        schedule = [dict(e) for e in credit.schedule]
        for entry in schedule:
            if rest == 0:
                break
            if is_paid(entry):
                continue
            interest_left, principal_left = installment_left(entry)
            to_interest = min(rest, interest_left)
            to_principal = min(rest - to_interest, principal_left)
            entry["paid_interest"] = fmt(Decimal(entry["paid_interest"]) + to_interest)
            entry["paid_principal"] = fmt(Decimal(entry["paid_principal"]) + to_principal)
            if to_interest == interest_left and to_principal == principal_left:
                entry["paid_at"] = now
            rest -= to_interest + to_principal
            paid_interest += to_interest
            paid_principal += to_principal

        credit.schedule = schedule  # a new list: JSONB changes are detected by assignment
        credit.principal_outstanding -= paid_principal
        self._update_monthly_payment(credit)
        return paid_interest, paid_principal

    def _repay_early(self, credit: Credit, amount: Decimal) -> Decimal:
        """Reduce the principal; the unpaid installments are recalculated (same dates,
        lower payment). Interest for the current period is not charged."""
        unpaid = [e for e in credit.schedule if not is_paid(e)]
        if any(not is_untouched(e) for e in unpaid):
            raise InvalidStateError(
                "An installment is partially paid: finish it with a regular repayment first"
            )
        if amount > credit.principal_outstanding:
            raise UnprocessableError(
                f"Amount exceeds the outstanding principal: {fmt(credit.principal_outstanding)}"
            )

        paid = [e for e in credit.schedule if is_paid(e)]
        credit.principal_outstanding -= amount
        if credit.principal_outstanding == 0:
            credit.schedule = paid  # nothing left: the remaining installments are dropped
        else:
            credit.schedule = paid + annuity_schedule(
                credit.principal_outstanding,
                credit.rate,
                [date.fromisoformat(e["due_date"]) for e in unpaid],
                first_number=unpaid[0]["n"],
            )
        self._update_monthly_payment(credit)
        return amount

    # ---------- internals ----------

    def _check_bounds(self, amount: Decimal, term_months: int) -> None:
        s = self._settings
        if not s.min_amount <= amount <= s.max_amount:
            raise UnprocessableError(f"Amount must be within {s.min_amount}..{s.max_amount}")
        if not s.min_term_months <= term_months <= s.max_term_months:
            raise UnprocessableError(
                f"Term must be within {s.min_term_months}..{s.max_term_months} months"
            )

    async def _lock_account(self, credit: Credit) -> Account:
        account = (await lock_accounts(self._session, {credit.account_id})).get(credit.account_id)
        if account is None:
            raise RuntimeError(f"Account of credit {credit.id} is missing")
        return account

    async def _send_sign_otp_quietly(self, user: User, credit: Credit) -> None:
        """The application is already decided: an SMS failure must not turn it into an
        error. The client requests the code again via ``/sign/otp``."""
        try:
            await self._otp.send(user.phone, OtpPurpose.CREDIT_SIGN, user.id)
        except ExternalServiceError:
            logger.warning("credit signing otp not sent", credit_id=str(credit.id))

    @staticmethod
    def _update_monthly_payment(credit: Credit) -> None:
        next_entry = next((e for e in credit.schedule if not is_paid(e)), None)
        credit.monthly_payment = Decimal(next_entry["payment"]) if next_entry else None

    @staticmethod
    def _require_status(credit: Credit, expected: CreditStatus, action: str) -> None:
        if credit.status is not expected:
            raise InvalidStateError(
                f"Only an {expected.value} credit can be {action}, it is {credit.status.value}"
            )

    @staticmethod
    def _require_active(account: Account) -> None:
        if account.status is not AccountStatus.ACTIVE:
            raise InvalidStateError(f"Account must be active, it is {account.status.value}")
