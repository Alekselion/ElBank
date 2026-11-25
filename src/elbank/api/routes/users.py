"""Current user's profile (``/api/v1/users/me``)."""

from fastapi import APIRouter, status

from elbank.api.deps import CurrentUserDep, ProfileServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.api.routes.auth import token_pair_response
from elbank.deps import SettingsDep
from elbank.schemas.auth import TokenPairResponse, UserRead
from elbank.schemas.users import (
    ChangePasswordRequest,
    ChangePhoneConfirmRequest,
    ChangePhoneRequest,
    ChangePhoneRequestResponse,
    NotificationSettings,
    NotificationSettingsUpdate,
    ProfileUpdateRequest,
)

router = APIRouter(
    prefix="/me",
    tags=["profile"],
    responses={
        status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
        status.HTTP_403_FORBIDDEN: error_response("User is blocked or deleted"),
    },
)

_CONFLICT: Responses = {
    status.HTTP_409_CONFLICT: error_response("Email or phone belongs to another user")
}


@router.get("", summary="Current user's profile")
async def get_me(user: CurrentUserDep) -> UserRead:
    return UserRead.model_validate(user)


@router.patch("", summary="Update full name, email or address", responses=_CONFLICT)
async def update_me(
    body: ProfileUpdateRequest, user: CurrentUserDep, profile: ProfileServiceDep
) -> UserRead:
    user = await profile.update_profile(user, body.model_dump(exclude_unset=True))
    return UserRead.model_validate(user)


@router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete the account (soft delete, all sessions are revoked)",
)
async def delete_me(user: CurrentUserDep, profile: ProfileServiceDep) -> None:
    await profile.delete_account(user)


@router.post(
    "/change-password",
    summary="Change password; other sessions are revoked, a new token pair is returned",
    responses={status.HTTP_400_BAD_REQUEST: error_response("Current password is incorrect")},
)
async def change_password(
    body: ChangePasswordRequest, user: CurrentUserDep, profile: ProfileServiceDep
) -> TokenPairResponse:
    pair = await profile.change_password(user, body.old_password, body.new_password)
    return token_pair_response(pair)


@router.post(
    "/change-phone/request",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Send confirmation codes to the current and the new phone",
    responses={
        **_CONFLICT,
        status.HTTP_429_TOO_MANY_REQUESTS: error_response("Codes were requested too recently"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("SMS gateway is unavailable"),
    },
)
async def request_phone_change(
    body: ChangePhoneRequest,
    user: CurrentUserDep,
    profile: ProfileServiceDep,
    settings: SettingsDep,
) -> ChangePhoneRequestResponse:
    await profile.request_phone_change(user, body.new_phone)
    return ChangePhoneRequestResponse(
        expires_in=settings.otp.ttl_seconds,
        resend_after=settings.otp.resend_cooldown_seconds,
    )


@router.post(
    "/change-phone/confirm",
    summary="Confirm the phone change with both codes",
    responses={
        **_CONFLICT,
        status.HTTP_400_BAD_REQUEST: error_response("Invalid or expired codes"),
    },
)
async def confirm_phone_change(
    body: ChangePhoneConfirmRequest, user: CurrentUserDep, profile: ProfileServiceDep
) -> UserRead:
    user = await profile.confirm_phone_change(user, body.old_code, body.new_code)
    return UserRead.model_validate(user)


@router.get("/notification-settings", summary="Notification channels")
async def get_notification_settings(
    user: CurrentUserDep, profile: ProfileServiceDep
) -> NotificationSettings:
    return profile.get_notification_settings(user)


@router.patch("/notification-settings", summary="Switch notification channels on/off")
async def update_notification_settings(
    body: NotificationSettingsUpdate, user: CurrentUserDep, profile: ProfileServiceDep
) -> NotificationSettings:
    return await profile.update_notification_settings(user, body.model_dump(exclude_none=True))
