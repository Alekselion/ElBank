"""Demo data: staff and clients with known passwords, accounts and a month of operations.

Run (after ``alembic upgrade head``)::

    uv run python -m scripts.seed            # or: make seed
    docker compose run --rm elbank-app python -m scripts.seed

Idempotent: if the demo admin already exists nothing is written; if only some demo
rows exist (a phone, an email or the fraud rule name is taken) the seed stops and
names them. Everything is inserted in one transaction. Operations are booked directly
(completed), following the same rules as the services — paired postings, fees from
the current tariffs (back office, or the environment defaults), a fee posting only
when the fee is positive, the conversion credit ``amount * rate`` rounded down — so a
reconciliation of the period reports ``ok``.
Balances equal the sum of the booked operations.
"""

import asyncio
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, Decimal

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import Settings, get_settings
from elbank.core.security import hash_password
from elbank.db.models import (
    Account,
    AccountLimit,
    AccountStatus,
    AccountType,
    Currency,
    FraudRule,
    KycRequest,
    KycStatus,
    Operation,
    OperationStatus,
    OperationType,
    PaymentOrder,
    PaymentOrderStatus,
    PaymentProvider,
    Tariff,
    TariffCode,
    User,
    UserRole,
    UserStatus,
)
from elbank.db.session import create_engine, create_session_factory
from elbank.schemas.admin import FeeRule
from elbank.services.requisites import generate_account_number
from elbank.services.tariffs import resolve_rule

CENT = Decimal("0.01")
EMPLOYER_ACCOUNT = "40702810900000000017"  # pseudo payroll account of another bank


@dataclass(frozen=True, slots=True)
class DemoUser:
    key: str
    phone: str
    password: str
    email: str
    full_name: str
    role: UserRole


USERS = (
    DemoUser(
        "admin", "+79000000001", "Admin-pass1", "admin@elbank.local",
        "Администратор Системы", UserRole.ADMIN,
    ),
    DemoUser(
        "manager", "+79000000002", "Manager-pass1", "manager@elbank.local",
        "Менеджер Ольга", UserRole.MANAGER,
    ),
    DemoUser(
        "auditor", "+79000000003", "Auditor-pass1", "auditor@elbank.local",
        "Аудитор Павел", UserRole.AUDITOR,
    ),
    DemoUser(
        "ivan", "+79000000010", "Client-pass1", "ivan@elbank.local",
        "Петров Иван Сергеевич", UserRole.CLIENT,
    ),
    DemoUser(
        "maria", "+79000000011", "Client-pass2", "maria@elbank.local",
        "Иванова Мария Олеговна", UserRole.CLIENT,
    ),
)  # fmt: skip

DEMO_FRAUD_RULE_NAME = "Крупный перевод с нового счёта"

# Holds a large transfer from a freshly opened account (see README, «Антифрод»).
DEMO_FRAUD_RULE = {
    "all": [
        {"field": "amount", "op": ">=", "value": "10000"},
        {"field": "account_age_days", "op": "<", "value": "1"},
    ]
}


class Seeder:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self._session = session
        self._settings = settings
        self._now = datetime.now(UTC).replace(microsecond=0)
        self._fee_rules: dict[TariffCode, FeeRule] = {}

    def at(self, days_ago: int, hour: int = 12) -> datetime:
        return (self._now - timedelta(days=days_ago)).replace(hour=hour, minute=0, second=0)

    async def run(self) -> dict[str, User]:
        for code in TariffCode:
            tariff = await self._session.get(Tariff, code)
            self._fee_rules[code] = resolve_rule(code, tariff, self._settings.transfers)
        users = {u.key: self._user(u) for u in USERS}
        self._session.add_all(users.values())
        await self._session.flush()
        for key in ("ivan", "maria"):
            self._kyc(users[key])

        ivan_rub = self._account(users["ivan"], AccountType.CHECKING, Currency.RUB, days_ago=90)
        ivan_savings = self._account(users["ivan"], AccountType.SAVINGS, Currency.RUB, days_ago=60)
        ivan_usd = self._account(users["ivan"], AccountType.FOREIGN, Currency.USD, days_ago=45)
        maria_rub = self._account(users["maria"], AccountType.CHECKING, Currency.RUB, days_ago=80)
        await self._session.flush()

        for days_ago in (62, 32, 2):
            self._income(ivan_rub, Decimal("120000.00"), days_ago, "Зарплата ООО «Ромашка»")
        self._income(maria_rub, Decimal("85000.00"), 30, "Зарплата АО «Василёк»")
        ivan, maria = users["ivan"], users["maria"]
        utility = {
            "personal_account": "1002003004",
            "period": self.at(20).strftime("%Y-%m"),
            "service": "electricity",
        }
        for user, account, provider, amount, days_ago, payload in (
            (ivan, ivan_rub, PaymentProvider.MOBILE, "500.00", 25, {"phone": "+79161234567"}),
            (ivan, ivan_rub, PaymentProvider.UTILITY, "4300.00", 20, utility),
            (
                ivan,
                ivan_rub,
                PaymentProvider.INTERNET,
                "900.00",
                10,
                {"contract_number": "INET-104233"},
            ),
            (maria, maria_rub, PaymentProvider.MOBILE, "300.00", 12, {"phone": "+79167654321"}),
        ):
            self._payment(
                user=user,
                account=account,
                provider=provider,
                amount=Decimal(amount),
                days_ago=days_ago,
                payload=payload,
            )
        for user, source, target, amount, days_ago, text in (
            (ivan, ivan_rub, maria_rub, "5000.00", 7, "За билеты"),
            (ivan, ivan_rub, ivan_savings, "30000.00", 6, "В копилку"),
            (maria, maria_rub, ivan_rub, "1200.00", 3, "Возврат долга"),
        ):
            self._transfer(
                user=user,
                source=source,
                target=target,
                amount=Decimal(amount),
                days_ago=days_ago,
                text=text,
            )
        self._conversion(
            user=ivan,
            source=ivan_rub,
            target=ivan_usd,
            amount=Decimal("9000.00"),
            rate=Decimal("0.0125"),
            days_ago=5,
        )

        self._session.add(
            FraudRule(
                name=DEMO_FRAUD_RULE_NAME,
                condition=DEMO_FRAUD_RULE,
                description="Демо: перевод от 10 000 со счёта моложе суток уходит на проверку",
                enabled=True,
            )
        )
        await self._session.commit()
        return users

    # ---------- builders ----------

    def _user(self, demo: DemoUser) -> User:
        return User(
            id=uuid.uuid4(),
            phone=demo.phone,
            email=demo.email,
            password_hash=hash_password(demo.password),
            full_name=demo.full_name,
            status=UserStatus.ACTIVE,
            role=demo.role,
            created_at=self.at(120),
        )

    def _kyc(self, user: User) -> None:
        self._session.add(
            KycRequest(
                user_id=user.id,
                status=KycStatus.APPROVED,
                passport_series="4510",
                passport_number=f"{uuid.uuid4().int % 10**6:06d}",
                birth_date=date(1990, 5, 17),
                decided_at=self.at(100),
                created_at=self.at(100),
            )
        )

    def _account(
        self, user: User, account_type: AccountType, currency: Currency, *, days_ago: int
    ) -> Account:
        account = Account(
            id=uuid.uuid4(),
            user_id=user.id,
            number=generate_account_number(account_type, currency, self._settings.bank),
            type=account_type,
            currency=currency.value,
            status=AccountStatus.ACTIVE,
            balance=Decimal(0),
            created_at=self.at(days_ago),
        )
        self._session.add(account)
        self._session.add(
            AccountLimit(
                account_id=account.id,
                daily_limit=self._settings.accounts.default_daily_limit,
                monthly_limit=self._settings.accounts.default_monthly_limit,
            )
        )
        return account

    def _posting(
        self,
        account: Account,
        type_: OperationType,
        amount: Decimal,
        days_ago: int,
        **fields: object,
    ) -> Operation:
        operation = Operation(
            id=uuid.uuid4(),
            account_id=account.id,
            type=type_,
            amount=amount,
            currency=account.currency,
            status=OperationStatus.COMPLETED,
            created_at=self.at(days_ago),
            **fields,
        )
        self._session.add(operation)
        return operation

    def _key(self) -> str:
        return f"seed-{uuid.uuid4().hex}"

    def _income(self, account: Account, amount: Decimal, days_ago: int, text: str) -> None:
        self._posting(
            account,
            OperationType.TRANSFER_IN,
            amount,
            days_ago,
            counterparty_account=EMPLOYER_ACCOUNT,
            description=text,
        )
        account.balance += amount

    def _payment(
        self,
        *,
        user: User,
        account: Account,
        provider: PaymentProvider,
        amount: Decimal,
        days_ago: int,
        payload: dict[str, str],
    ) -> None:
        operation = self._posting(
            account,
            OperationType.PAYMENT,
            amount,
            days_ago,
            user_id=user.id,
            idempotency_key=self._key(),
            external_id=f"SEED-{uuid.uuid4().hex[:12].upper()}",
            description=f"Оплата: {provider.value}",
        )
        self._session.add(
            PaymentOrder(
                id=uuid.uuid4(),
                account_id=account.id,
                operation_id=operation.id,
                provider=provider,
                payload=payload,
                status=PaymentOrderStatus.COMPLETED,
                external_id=operation.external_id,
                provider_response={"status": "accepted", "transaction_id": operation.external_id},
                created_at=operation.created_at,
            )
        )
        account.balance -= amount

    def _transfer(
        self,
        *,
        user: User,
        source: Account,
        target: Account,
        amount: Decimal,
        days_ago: int,
        text: str,
    ) -> None:
        fee = self._fee_rules[TariffCode.INTERNAL_TRANSFER].fee(amount)
        main = self._posting(
            source,
            OperationType.TRANSFER_OUT,
            amount,
            days_ago,
            user_id=user.id,
            idempotency_key=self._key(),
            counterparty_account=target.number,
            description=text,
        )
        self._posting(
            target,
            OperationType.TRANSFER_IN,
            amount,
            days_ago,
            parent_operation_id=main.id,
            counterparty_account=source.number,
            description=text,
        )
        if fee > 0:
            self._posting(
                source,
                OperationType.FEE,
                fee,
                days_ago,
                parent_operation_id=main.id,
                description="Internal transfer fee",
            )
        source.balance -= amount + fee
        target.balance += amount

    def _conversion(
        self,
        *,
        user: User,
        source: Account,
        target: Account,
        amount: Decimal,
        rate: Decimal,
        days_ago: int,
    ) -> None:
        fee = self._fee_rules[TariffCode.CONVERSION].fee(amount)
        converted = (amount * rate).quantize(CENT, rounding=ROUND_DOWN)
        text = f"Conversion {source.currency} -> {target.currency}"
        main = self._posting(
            source,
            OperationType.CONVERSION_OUT,
            amount,
            days_ago,
            user_id=user.id,
            idempotency_key=self._key(),
            counterparty_account=target.number,
            description=text,
            exchange_rate=rate,
        )
        self._posting(
            target,
            OperationType.CONVERSION_IN,
            converted,
            days_ago,
            parent_operation_id=main.id,
            counterparty_account=source.number,
            description=text,
            exchange_rate=rate,
        )
        if fee > 0:
            self._posting(
                source,
                OperationType.FEE,
                fee,
                days_ago,
                parent_operation_id=main.id,
                description="Currency conversion fee",
            )
        source.balance -= amount + fee
        target.balance += converted


async def seed() -> int:
    settings = get_settings()
    engine = create_engine(settings.postgres)
    try:
        async with create_session_factory(engine)() as session:
            existing = await session.scalar(select(User.id).where(User.phone == USERS[0].phone))
            if existing is not None:
                print("Demo data already present: nothing to do.")
                _print_credentials()
                return 0
            conflicts = await _conflicts(session)
            if conflicts:
                print("Cannot seed: some demo rows already exist:", file=sys.stderr)
                for conflict in conflicts:
                    print(f"  {conflict}", file=sys.stderr)
                print("Remove them or start from scratch (make reset).", file=sys.stderr)
                return 1
            users = await Seeder(session, settings).run()
            accounts = list(
                await session.scalars(
                    select(Account)
                    .where(Account.user_id.in_([u.id for u in users.values()]))
                    .order_by(Account.created_at)
                )
            )
    finally:
        await engine.dispose()

    print("Demo data created.")
    _print_credentials()
    owners = {u.id: u.full_name for u in users.values()}
    print("\nAccounts:")
    for a in accounts:
        owner, kind, balance = owners[a.user_id], a.type.value, f"{a.balance} {a.currency}"
        print(f"  {owner:<26} {kind:<9} {a.number}  {balance:>16}  {a.id}")
    return 0


async def _conflicts(session: AsyncSession) -> list[str]:
    """Demo values that are taken by other rows (unique phone, email, fraud rule name)."""
    phones = {u.phone for u in USERS}
    emails = {u.email for u in USERS}
    found: list[str] = []
    rows = await session.execute(
        select(User.phone, User.email).where(or_(User.phone.in_(phones), User.email.in_(emails)))
    )
    for phone, email in rows:
        if phone in phones:
            found.append(f"user with phone {phone}")
        if email in emails:
            found.append(f"user with email {email}")
    rule = await session.scalar(select(FraudRule.id).where(FraudRule.name == DEMO_FRAUD_RULE_NAME))
    if rule is not None:
        found.append(f"fraud rule {DEMO_FRAUD_RULE_NAME!r}")
    return found


def _print_credentials() -> None:
    print("\nUsers (phone / password):")
    for u in USERS:
        print(f"  {u.role.value:<8} {u.phone}  {u.password:<14} {u.full_name}")


if __name__ == "__main__":
    sys.exit(asyncio.run(seed()))
