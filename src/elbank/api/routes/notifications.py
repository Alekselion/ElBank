"""Notifications (``/api/v1/notifications``): list, read marks, channel settings."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, status

from elbank.api.deps import CurrentUserDep, NotificationServiceDep, ProfileServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.notification import NotificationPage, NotificationRead, ReadAllResult
from elbank.schemas.users import NotificationSettings, NotificationSettingsUpdate

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}

router = APIRouter(tags=["notifications"], responses=_AUTH_ERRORS)


@router.get("", summary="Own notifications, newest first")
async def list_notifications(
    user: CurrentUserDep,
    notifications: NotificationServiceDep,
    unread: Annotated[bool, Query(description="Only unread notifications")] = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> NotificationPage:
    items, total, unread_count = await notifications.list_for_user(
        user, unread=unread, limit=limit, offset=offset
    )
    return NotificationPage(
        items=[NotificationRead.model_validate(n) for n in items],
        limit=limit,
        offset=offset,
        total=total,
        unread=unread_count,
    )


# Declared before "/{notification_id}/read" for readability; paths do not overlap.
@router.post("/read-all", summary="Mark all notifications as read")
async def read_all(user: CurrentUserDep, notifications: NotificationServiceDep) -> ReadAllResult:
    return ReadAllResult(updated=await notifications.mark_all_read(user))


@router.get("/settings", summary="Notification channels")
async def get_settings(user: CurrentUserDep, profile: ProfileServiceDep) -> NotificationSettings:
    return profile.get_notification_settings(user)


@router.patch(
    "/settings",
    summary="Switch notification channels on/off",
    description="A switched-off channel: notifications are stored but not sent.",
)
async def update_settings(
    body: NotificationSettingsUpdate, user: CurrentUserDep, profile: ProfileServiceDep
) -> NotificationSettings:
    return await profile.update_notification_settings(user, body.model_dump(exclude_none=True))


@router.post(
    "/{notification_id}/read",
    summary="Mark a notification as read (idempotent)",
    responses={status.HTTP_404_NOT_FOUND: error_response("Notification not found")},
)
async def read_notification(
    notification_id: uuid.UUID, user: CurrentUserDep, notifications: NotificationServiceDep
) -> NotificationRead:
    return NotificationRead.model_validate(await notifications.mark_read(user, notification_id))
