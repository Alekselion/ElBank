"""Auth-related FastAPI dependencies: current user, role checks, auth services."""

import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import Settings
from elbank.core.exceptions import AuthenticationError, PermissionDeniedError, UserInactiveError
from elbank.core.security import InvalidTokenError, TokenPayload, TokenType, decode_token
from elbank.db.models import User, UserRole, UserStatus
from elbank.deps import (
    BkiClientDep,
    BrokerClientDep,
    CardProcessorClientDep,
    CbrClientDep,
    DbSessionDep,
    EventPublisherDep,
    GosuslugiClientDep,
    InterbankClientDep,
    ProviderClientDep,
    RedisDep,
    ResourcesDep,
    SbpClientDep,
    SettingsDep,
    SmsClientDep,
)
from elbank.services.account_cache import AccountListCache
from elbank.services.accounts import AccountService
from elbank.services.admin_users import AdminUserService
from elbank.services.analytics import AnalyticsCache, AnalyticsService
from elbank.services.audit_search import AuditSearchService
from elbank.services.auth import AuthService
from elbank.services.budgets import BudgetAlerts, BudgetService
from elbank.services.cards import CardService
from elbank.services.conversion import ConversionService
from elbank.services.credits import CreditService
from elbank.services.deposits import DepositService
from elbank.services.exports import ExportJobStore, ExportService
from elbank.services.external_transfers import ExternalTransferService
from elbank.services.faq import FaqService
from elbank.services.fraud import FraudRuleService
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.investments import InvestmentService
from elbank.services.kyc import KycService
from elbank.services.ledger import OutgoingChecks
from elbank.services.notifications import NotificationService
from elbank.services.operations import OperationHistoryService
from elbank.services.otp import OtpService
from elbank.services.payments import PaymentService, TemplateService
from elbank.services.profile import ProfileService
from elbank.services.rates import RatesService
from elbank.services.reconciliation import ReconciliationService
from elbank.services.sessions import access_revoked_before
from elbank.services.statements import StatementJobStore, StatementService
from elbank.services.support import TicketService
from elbank.services.tariffs import TariffService
from elbank.services.transfers import TransferService

_bearer = HTTPBearer(auto_error=False, description="Access token from /api/v1/auth/login")


async def authenticate_access_token(
    token: str, session: AsyncSession, redis: Redis, settings: Settings
) -> tuple[User, TokenPayload]:
    """Resolve an access token to an active user (shared by HTTP and WebSocket auth)."""
    try:
        payload = decode_token(token, TokenType.ACCESS, settings.jwt)
        user_id = uuid.UUID(payload.sub)
    except (InvalidTokenError, ValueError) as exc:
        raise AuthenticationError from exc

    revoked_before = await access_revoked_before(redis, user_id)
    if revoked_before is not None and payload.iat < revoked_before:
        raise AuthenticationError("Token has been revoked")

    user = await session.get(User, user_id)
    if user is None:
        raise AuthenticationError
    if user.status is not UserStatus.ACTIVE:
        raise UserInactiveError
    return user, payload


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    session: DbSessionDep,
    redis: RedisDep,
    settings: SettingsDep,
) -> User:
    if credentials is None:
        raise AuthenticationError("Missing bearer token")
    user, _ = await authenticate_access_token(credentials.credentials, session, redis, settings)
    return user


CurrentUserDep = Annotated[User, Depends(get_current_user)]


def require_roles(*roles: UserRole) -> Callable[[User], Awaitable[User]]:
    """Dependency factory: the current user must have one of ``roles`` (else 403).

    Usage::

        @router.get("/admin", dependencies=[Depends(require_roles(UserRole.ADMIN))])
        async def handler(user: Annotated[User, Depends(require_roles(UserRole.MANAGER))]): ...
    """
    if not roles:
        raise ValueError("require_roles() needs at least one role")
    allowed = frozenset(roles)

    async def dependency(user: CurrentUserDep) -> User:
        if user.role not in allowed:
            raise PermissionDeniedError
        return user

    return dependency


def get_otp_service(
    redis: RedisDep, sms: SmsClientDep, publisher: EventPublisherDep, settings: SettingsDep
) -> OtpService:
    return OtpService(redis, sms, publisher, settings)


OtpServiceDep = Annotated[OtpService, Depends(get_otp_service)]


def get_auth_service(
    session: DbSessionDep,
    redis: RedisDep,
    otp: OtpServiceDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> AuthService:
    return AuthService(session, redis, otp, publisher, settings)


AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


def get_profile_service(
    session: DbSessionDep,
    redis: RedisDep,
    otp: OtpServiceDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> ProfileService:
    return ProfileService(session, redis, otp, publisher, settings)


ProfileServiceDep = Annotated[ProfileService, Depends(get_profile_service)]


def get_kyc_service(
    session: DbSessionDep,
    gosuslugi: GosuslugiClientDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> KycService:
    return KycService(session, gosuslugi, publisher, settings.kyc)


KycServiceDep = Annotated[KycService, Depends(get_kyc_service)]


def get_account_service(
    session: DbSessionDep,
    redis: RedisDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> AccountService:
    cache = AccountListCache(redis, settings.accounts.list_cache_ttl_seconds)
    return AccountService(session, cache, publisher, settings.bank, settings.accounts)


AccountServiceDep = Annotated[AccountService, Depends(get_account_service)]


def get_statement_service(
    redis: RedisDep, publisher: EventPublisherDep, settings: SettingsDep
) -> StatementService:
    store = StatementJobStore(redis, settings.statements.job_ttl_seconds)
    return StatementService(store, publisher, settings.statements)


StatementServiceDep = Annotated[StatementService, Depends(get_statement_service)]


def get_account_cache(redis: RedisDep, settings: SettingsDep) -> AccountListCache:
    return AccountListCache(redis, settings.accounts.list_cache_ttl_seconds)


AccountCacheDep = Annotated[AccountListCache, Depends(get_account_cache)]


def get_idempotency_guard(
    session: DbSessionDep, redis: RedisDep, settings: SettingsDep
) -> IdempotencyGuard:
    return IdempotencyGuard(session, redis, settings.transfers)


IdempotencyGuardDep = Annotated[IdempotencyGuard, Depends(get_idempotency_guard)]


def get_outgoing_checks(session: DbSessionDep, settings: SettingsDep) -> OutgoingChecks:
    return OutgoingChecks(session, settings.accounts)


OutgoingChecksDep = Annotated[OutgoingChecks, Depends(get_outgoing_checks)]


def get_tariff_service(session: DbSessionDep, settings: SettingsDep) -> TariffService:
    return TariffService(session, settings.transfers)


TariffServiceDep = Annotated[TariffService, Depends(get_tariff_service)]


def get_transfer_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    tariffs: TariffServiceDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> TransferService:
    return TransferService(
        session,
        guard,
        checks,
        tariffs=tariffs,
        cache=cache,
        publisher=publisher,
        settings=settings.transfers,
    )


TransferServiceDep = Annotated[TransferService, Depends(get_transfer_service)]


def get_external_transfer_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    interbank: InterbankClientDep,
    sbp: SbpClientDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> ExternalTransferService:
    return ExternalTransferService(
        session,
        guard,
        checks,
        interbank=interbank,
        sbp=sbp,
        cache=cache,
        publisher=publisher,
        bank=settings.bank,
        settings=settings.transfers,
    )


ExternalTransferServiceDep = Annotated[
    ExternalTransferService, Depends(get_external_transfer_service)
]


def get_rates_service(
    session: DbSessionDep, redis: RedisDep, cbr: CbrClientDep, settings: SettingsDep
) -> RatesService:
    return RatesService(session, redis, cbr, settings.cbr)


RatesServiceDep = Annotated[RatesService, Depends(get_rates_service)]


def get_conversion_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    rates: RatesServiceDep,
    tariffs: TariffServiceDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> ConversionService:
    return ConversionService(
        session,
        guard,
        checks,
        rates=rates,
        tariffs=tariffs,
        cache=cache,
        publisher=publisher,
        settings=settings.transfers,
    )


ConversionServiceDep = Annotated[ConversionService, Depends(get_conversion_service)]


def get_payment_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    providers: ProviderClientDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
) -> PaymentService:
    return PaymentService(
        session, guard, checks, providers=providers, cache=cache, publisher=publisher
    )


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]


def get_template_service(session: DbSessionDep) -> TemplateService:
    return TemplateService(session)


TemplateServiceDep = Annotated[TemplateService, Depends(get_template_service)]


def get_card_service(
    session: DbSessionDep,
    processor: CardProcessorClientDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> CardService:
    return CardService(session, processor, publisher, settings.cards)


CardServiceDep = Annotated[CardService, Depends(get_card_service)]


def get_credit_service(
    *,
    session: DbSessionDep,
    bki: BkiClientDep,
    otp: OtpServiceDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> CreditService:
    return CreditService(
        session,
        bki=bki,
        otp=otp,
        cache=cache,
        publisher=publisher,
        settings=settings.credits,
        otp_settings=settings.otp,
    )


CreditServiceDep = Annotated[CreditService, Depends(get_credit_service)]


def get_deposit_service(
    session: DbSessionDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> DepositService:
    return DepositService(session, cache=cache, publisher=publisher, settings=settings.deposits)


DepositServiceDep = Annotated[DepositService, Depends(get_deposit_service)]


def get_investment_service(
    session: DbSessionDep,
    broker: BrokerClientDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
) -> InvestmentService:
    return InvestmentService(session, broker=broker, cache=cache, publisher=publisher)


InvestmentServiceDep = Annotated[InvestmentService, Depends(get_investment_service)]

# Back-office role checks (see /api/v1/admin/* in the README for the full matrix).
StaffUserDep = Annotated[User, Depends(require_roles(UserRole.MANAGER, UserRole.ADMIN))]
AdminUserDep = Annotated[User, Depends(require_roles(UserRole.ADMIN))]
AuditorUserDep = Annotated[User, Depends(require_roles(UserRole.AUDITOR))]
# Read-only back-office views: staff plus the auditor.
BackOfficeReaderDep = Annotated[
    User, Depends(require_roles(UserRole.MANAGER, UserRole.ADMIN, UserRole.AUDITOR))
]
# Reconciliation: run by an admin or the auditor.
ReconcilerUserDep = Annotated[User, Depends(require_roles(UserRole.ADMIN, UserRole.AUDITOR))]


def get_operation_history_service(session: DbSessionDep) -> OperationHistoryService:
    return OperationHistoryService(session)


OperationHistoryServiceDep = Annotated[
    OperationHistoryService, Depends(get_operation_history_service)
]


def get_export_service(
    redis: RedisDep, publisher: EventPublisherDep, settings: SettingsDep
) -> ExportService:
    store = ExportJobStore(redis, settings.exports.job_ttl_seconds)
    return ExportService(store, publisher, settings.exports)


ExportServiceDep = Annotated[ExportService, Depends(get_export_service)]


def get_analytics_service(
    session: DbSessionDep, redis: RedisDep, settings: SettingsDep
) -> AnalyticsService:
    cache = AnalyticsCache(redis, settings.analytics.cache_ttl_seconds)
    return AnalyticsService(session, cache, settings.analytics)


AnalyticsServiceDep = Annotated[AnalyticsService, Depends(get_analytics_service)]


def get_budget_service(
    session: DbSessionDep, redis: RedisDep, settings: SettingsDep
) -> BudgetService:
    return BudgetService(session, BudgetAlerts(redis), settings.budgets)


BudgetServiceDep = Annotated[BudgetService, Depends(get_budget_service)]


def get_ticket_service(
    session: DbSessionDep, publisher: EventPublisherDep, settings: SettingsDep
) -> TicketService:
    return TicketService(session, publisher, settings.support)


TicketServiceDep = Annotated[TicketService, Depends(get_ticket_service)]


def get_faq_service(session: DbSessionDep, redis: RedisDep, settings: SettingsDep) -> FaqService:
    return FaqService(session, redis, settings.support.faq_cache_ttl_seconds)


FaqServiceDep = Annotated[FaqService, Depends(get_faq_service)]


def get_notification_service(session: DbSessionDep) -> NotificationService:
    return NotificationService(session)


NotificationServiceDep = Annotated[NotificationService, Depends(get_notification_service)]


def get_admin_user_service(
    session: DbSessionDep, redis: RedisDep, publisher: EventPublisherDep, settings: SettingsDep
) -> AdminUserService:
    return AdminUserService(session, redis, publisher, settings.jwt)


AdminUserServiceDep = Annotated[AdminUserService, Depends(get_admin_user_service)]


def get_fraud_rule_service(session: DbSessionDep) -> FraudRuleService:
    return FraudRuleService(session)


FraudRuleServiceDep = Annotated[FraudRuleService, Depends(get_fraud_rule_service)]


def get_audit_search_service(resources: ResourcesDep, settings: SettingsDep) -> AuditSearchService:
    return AuditSearchService(resources.elastic, settings.elastic)


AuditSearchServiceDep = Annotated[AuditSearchService, Depends(get_audit_search_service)]


def get_reconciliation_service(
    session: DbSessionDep, publisher: EventPublisherDep, settings: SettingsDep
) -> ReconciliationService:
    return ReconciliationService(session, publisher, settings.reconciliation)


ReconciliationServiceDep = Annotated[ReconciliationService, Depends(get_reconciliation_service)]
