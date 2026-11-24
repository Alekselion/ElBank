"""Authentication endpoints (``/api/v1/auth``)."""

from typing import Any

from fastapi import APIRouter, status

from elbank.api.deps import AuthServiceDep, CurrentUserDep, OtpServiceDep
from elbank.deps import SettingsDep
from elbank.schemas.auth import (
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    ResetPasswordRequest,
    SendOtpRequest,
    SendOtpResponse,
    TokenPairResponse,
    TwoFactorRequest,
    TwoFactorResponse,
    UserRead,
    VerifyOtpRequest,
    VerifyOtpResponse,
)
from elbank.services.auth import TokenPair

router = APIRouter(tags=["auth"])


type Responses = dict[int | str, dict[str, Any]]


def _error(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {"application/json": {"example": {"detail": "message", "code": "machine_code"}}},
    }


_UNAUTHORIZED: Responses = {
    status.HTTP_401_UNAUTHORIZED: _error("Missing, invalid or revoked token")
}


def _token_response(pair: TokenPair) -> TokenPairResponse:
    access_ttl = pair.access.expires_at - pair.access.issued_at
    refresh_ttl = pair.refresh.expires_at - pair.refresh.issued_at
    return TokenPairResponse(
        access_token=pair.access.token,
        refresh_token=pair.refresh.token,
        expires_in=int(access_ttl.total_seconds()),
        refresh_expires_in=int(refresh_ttl.total_seconds()),
    )


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    summary="Register a new client",
    responses={status.HTTP_409_CONFLICT: _error("Phone or email already registered")},
)
async def register(body: RegisterRequest, auth: AuthServiceDep) -> UserRead:
    user = await auth.register(body.phone, body.password, body.email)
    return UserRead.model_validate(user)


@router.post(
    "/login",
    summary="Log in with phone and password",
    responses={
        status.HTTP_401_UNAUTHORIZED: _error("Invalid credentials, or 2FA OTP token required"),
        status.HTTP_403_FORBIDDEN: _error("User is blocked or deleted"),
        status.HTTP_429_TOO_MANY_REQUESTS: _error("Too many login attempts"),
    },
)
async def login(body: LoginRequest, auth: AuthServiceDep) -> TokenPairResponse:
    pair = await auth.login(body.phone, body.password, body.otp_token)
    return _token_response(pair)


@router.post("/refresh", summary="Rotate the token pair", responses=_UNAUTHORIZED)
async def refresh(body: RefreshRequest, auth: AuthServiceDep) -> TokenPairResponse:
    pair = await auth.refresh(body.refresh_token)
    return _token_response(pair)


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke all sessions of the current user",
    responses=_UNAUTHORIZED,
)
async def logout(user: CurrentUserDep, auth: AuthServiceDep) -> None:
    await auth.logout(user)


@router.post(
    "/send-otp",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Send a one-time code by SMS",
    responses={
        status.HTTP_429_TOO_MANY_REQUESTS: _error("Code was requested too recently"),
        status.HTTP_503_SERVICE_UNAVAILABLE: _error("SMS gateway is unavailable"),
    },
)
async def send_otp(
    body: SendOtpRequest, auth: AuthServiceDep, settings: SettingsDep
) -> SendOtpResponse:
    await auth.send_otp(body.phone, body.purpose)
    return SendOtpResponse(
        expires_in=settings.otp.ttl_seconds,
        resend_after=settings.otp.resend_cooldown_seconds,
    )


@router.post("/verify-otp", summary="Check a one-time code and get an OTP token")
async def verify_otp(body: VerifyOtpRequest, otp: OtpServiceDep) -> VerifyOtpResponse:
    result = await otp.verify(body.phone, body.code, body.purpose)
    if result.token is None:
        return VerifyOtpResponse(verified=False, attempts_left=result.attempts_left)
    ttl = result.token.expires_at - result.token.issued_at
    return VerifyOtpResponse(
        verified=True,
        otp_token=result.token.token,
        otp_token_expires_in=int(ttl.total_seconds()),
        attempts_left=result.attempts_left,
    )


@router.post(
    "/reset-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Set a new password using an OTP token (purpose=reset)",
    responses={status.HTTP_401_UNAUTHORIZED: _error("OTP token invalid or already used")},
)
async def reset_password(body: ResetPasswordRequest, auth: AuthServiceDep) -> None:
    await auth.reset_password(body.phone, body.otp_token, body.new_password)


@router.post(
    "/2fa/enable",
    summary="Enable two-factor login (OTP token with purpose=two_factor)",
    responses=_UNAUTHORIZED,
)
async def enable_2fa(
    body: TwoFactorRequest, user: CurrentUserDep, auth: AuthServiceDep
) -> TwoFactorResponse:
    user = await auth.set_two_factor(user, body.otp_token, enabled=True)
    return TwoFactorResponse(is_2fa_enabled=user.is_2fa_enabled)


@router.post(
    "/2fa/disable",
    summary="Disable two-factor login (OTP token with purpose=two_factor)",
    responses=_UNAUTHORIZED,
)
async def disable_2fa(
    body: TwoFactorRequest, user: CurrentUserDep, auth: AuthServiceDep
) -> TwoFactorResponse:
    user = await auth.set_two_factor(user, body.otp_token, enabled=False)
    return TwoFactorResponse(is_2fa_enabled=user.is_2fa_enabled)
