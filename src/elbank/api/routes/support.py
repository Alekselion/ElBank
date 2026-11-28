"""Support (``/api/v1/support``): tickets, FAQ."""

import uuid
from typing import Annotated

from fastapi import APIRouter, File, Form, Query, UploadFile, status
from fastapi.responses import FileResponse

from elbank.api.deps import CurrentUserDep, FaqServiceDep, StaffUserDep, TicketServiceDep
from elbank.api.openapi import Responses, error_response
from elbank.core.exceptions import UnprocessableError
from elbank.db.models import TicketCategory, TicketPriority, TicketStatus
from elbank.deps import SettingsDep
from elbank.schemas.kyc import Page
from elbank.schemas.support import (
    FaqArticleRead,
    MessageBody,
    MessageCreate,
    MessageRead,
    TicketDetail,
    TicketRate,
    TicketRead,
    TicketStatusChange,
    TicketSubject,
)
from elbank.services.support import UploadedFile

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Ticket not found")}
_WRONG_STATE: Responses = {
    status.HTTP_409_CONFLICT: error_response("Not allowed in the current ticket status")
}

router = APIRouter(tags=["support"], responses=_AUTH_ERRORS)
faq_router = APIRouter(tags=["support"])


# ---------- tickets ----------


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create a ticket with the first message (multipart/form-data)",
    description=(
        "Attachments: JPEG, PNG or PDF, up to SUPPORT_MAX_ATTACHMENTS files of "
        "SUPPORT_MAX_ATTACHMENT_BYTES each. Publishes `ticket.created`."
    ),
    responses={
        status.HTTP_413_CONTENT_TOO_LARGE: error_response("Attachment is too large"),
        status.HTTP_415_UNSUPPORTED_MEDIA_TYPE: error_response("Unsupported attachment type"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Invalid fields or too many files"),
    },
)
async def create_ticket(
    *,
    subject: Annotated[TicketSubject, Form()],
    category: Annotated[TicketCategory, Form()],
    body: Annotated[MessageBody, Form()],
    priority: Annotated[TicketPriority, Form()] = TicketPriority.NORMAL,
    attachments: Annotated[list[UploadFile] | None, File(description="Files to attach")] = None,
    user: CurrentUserDep,
    tickets: TicketServiceDep,
    settings: SettingsDep,
) -> TicketDetail:
    uploads = attachments or []
    if len(uploads) > settings.support.max_attachments:
        raise UnprocessableError(
            f"No more than {settings.support.max_attachments} attachments per ticket"
        )
    # Read one byte past the limit to detect oversized uploads without loading more.
    limit = settings.support.max_attachment_bytes
    files = [UploadedFile(u.filename, await u.read(limit + 1)) for u in uploads]
    ticket = await tickets.create(
        user, subject=subject, category=category, priority=priority, body=body, files=files
    )
    return TicketDetail.model_validate(ticket)


@router.get("", summary="Own tickets (staff: all tickets), latest activity first")
async def list_tickets(
    user: CurrentUserDep,
    tickets: TicketServiceDep,
    status_: Annotated[TicketStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[TicketRead]:
    items, total = await tickets.list_for_user(user, status=status_, limit=limit, offset=offset)
    return Page[TicketRead](
        items=[TicketRead.model_validate(t) for t in items], limit=limit, offset=offset, total=total
    )


@router.get("/{ticket_id}", summary="Ticket with its messages", responses=_NOT_FOUND)
async def get_ticket(
    ticket_id: uuid.UUID, user: CurrentUserDep, tickets: TicketServiceDep
) -> TicketDetail:
    return TicketDetail.model_validate(await tickets.get_detail(user, ticket_id))


@router.get(
    "/{ticket_id}/attachments/{attachment_id}",
    summary="Download an attachment",
    response_class=FileResponse,
    responses={
        status.HTTP_404_NOT_FOUND: error_response("Ticket or attachment not found"),
        status.HTTP_200_OK: {
            "content": {"image/jpeg": {}, "image/png": {}, "application/pdf": {}},
            "description": "Attachment",
        },
    },
)
async def download_attachment(
    ticket_id: uuid.UUID, attachment_id: uuid.UUID, user: CurrentUserDep, tickets: TicketServiceDep
) -> FileResponse:
    attachment, path = await tickets.attachment(user, ticket_id, attachment_id)
    return FileResponse(path, media_type=attachment.content_type, filename=attachment.file_name)


@router.post(
    "/{ticket_id}/messages",
    status_code=status.HTTP_201_CREATED,
    summary="Add a message; publishes ticket.updated",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def add_message(
    ticket_id: uuid.UUID, data: MessageCreate, user: CurrentUserDep, tickets: TicketServiceDep
) -> MessageRead:
    return MessageRead.model_validate(await tickets.add_message(user, ticket_id, data.body))


@router.post(
    "/{ticket_id}/close",
    summary="Close the ticket (author or manager)",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def close_ticket(
    ticket_id: uuid.UUID, user: CurrentUserDep, tickets: TicketServiceDep
) -> TicketRead:
    return TicketRead.model_validate(await tickets.close(user, ticket_id))


@router.post(
    "/{ticket_id}/rate",
    summary="Rate a resolved/closed ticket (CSAT 1..5), once, by the author",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def rate_ticket(
    ticket_id: uuid.UUID, data: TicketRate, user: CurrentUserDep, tickets: TicketServiceDep
) -> TicketRead:
    return TicketRead.model_validate(await tickets.rate(user, ticket_id, data.score, data.comment))


@router.post(
    "/{ticket_id}/status",
    summary="Change the status (manager, admin)",
    description=(
        "open → in_progress | closed; in_progress → waiting_client | resolved | closed; "
        "waiting_client → in_progress | resolved | closed; resolved → in_progress | closed."
    ),
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def change_ticket_status(
    ticket_id: uuid.UUID, data: TicketStatusChange, manager: StaffUserDep, tickets: TicketServiceDep
) -> TicketRead:
    return TicketRead.model_validate(await tickets.change_status(manager, ticket_id, data.status))


# ---------- FAQ ----------


@faq_router.get("", summary="FAQ articles (cached)")
async def list_faq(
    faq: FaqServiceDep, category: TicketCategory | None = None
) -> list[FaqArticleRead]:
    return await faq.list_articles(category)
