"""Auth-related FastAPI dependencies: current user, role checks, auth services."""

import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from elbank.core.exceptions import AuthenticationError, PermissionDeniedError, UserInactiveError
from elbank.core.security import InvalidTokenError, TokenType, decode_token
from elbank.db.models import User, UserRole, UserStatus
from elbank.deps import (
    CbrClientDep,
    DbSessionDep,
    EventPublisherDep,
    GosuslugiClientDep,
    InterbankClientDep,
    RedisDep,
    SbpClientDep,
    SettingsDep,
    SmsClientDep,
)
from elbank.services.account_cache import AccountListCache
from elbank.services.accounts import AccountService
from elbank.services.auth import AuthService
from elbank.services.conversion import ConversionService
from elbank.services.external_transfers import ExternalTransferService
from elbank.services.idempotency import IdempotencyGuard
from elbank.services.kyc import KycService
from elbank.services.ledger import OutgoingChecks
from elbank.services.otp import OtpService
from elbank.services.profile import ProfileService
from elbank.services.rates import RatesService
from elbank.services.sessions import access_revoked_before
from elbank.services.statements import StatementJobStore, StatementService
from elbank.services.transfers import TransferService

_bearer = HTTPBearer(auto_error=False, description="Access token from /api/v1/auth/login")


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    session: DbSessionDep,
    redis: RedisDep,
    settings: SettingsDep,
) -> User:
    if credentials is None:
        raise AuthenticationError("Missing bearer token")
    try:
        payload = decode_token(credentials.credentials, TokenType.ACCESS, settings.jwt)
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


def get_transfer_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> TransferService:
    return TransferService(
        session,
        guard,
        checks,
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


def get_rates_service(redis: RedisDep, cbr: CbrClientDep, settings: SettingsDep) -> RatesService:
    return RatesService(redis, cbr, settings.cbr)


RatesServiceDep = Annotated[RatesService, Depends(get_rates_service)]


def get_conversion_service(
    *,
    session: DbSessionDep,
    guard: IdempotencyGuardDep,
    checks: OutgoingChecksDep,
    rates: RatesServiceDep,
    cache: AccountCacheDep,
    publisher: EventPublisherDep,
    settings: SettingsDep,
) -> ConversionService:
    return ConversionService(
        session,
        guard,
        checks,
        rates=rates,
        cache=cache,
        publisher=publisher,
        settings=settings.transfers,
    )


ConversionServiceDep = Annotated[ConversionService, Depends(get_conversion_service)]

# Managers and admins: back-office endpoints.
StaffUserDep = Annotated[User, Depends(require_roles(UserRole.MANAGER, UserRole.ADMIN))]
