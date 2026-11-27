"""Cards (``/api/v1/cards``): issue, list, block/unblock, reissue, limits, MCC restrictions."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Body, status

from elbank.api.deps import CardServiceDep, CurrentUserDep
from elbank.api.openapi import Responses, error_response
from elbank.db.models import Card, Currency
from elbank.schemas.card import (
    CardBlockRequest,
    CardDetails,
    CardLimits,
    CardLimitsRead,
    CardMccRestrictionsRead,
    CardRead,
    MccRestrictions,
    PlasticCardOrderRequest,
    VirtualCardIssueRequest,
)

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Card not found")}
_WRONG_STATE: Responses = {
    status.HTTP_409_CONFLICT: error_response("Not allowed in the current card or account status")
}
_PROCESSOR_ERRORS: Responses = {
    status.HTTP_422_UNPROCESSABLE_CONTENT: error_response(
        "Invalid request or refused by the card processor (code CARD_PROCESSOR_REJECTED)"
    ),
    status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Card processor is unavailable"),
}
_ISSUE_ERRORS: Responses = {
    status.HTTP_404_NOT_FOUND: error_response("Account not found"),
    status.HTTP_409_CONFLICT: error_response("Account is not active or has too many cards"),
    **_PROCESSOR_ERRORS,
}

router = APIRouter(tags=["cards"], responses=_AUTH_ERRORS)


def _details(result: tuple[Card, Currency]) -> CardDetails:
    card, currency = result
    return CardDetails.from_card_and_currency(card, currency)


# ---------- issue ----------


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Issue a virtual card → card.issued",
    responses=_ISSUE_ERRORS,
)
async def issue_virtual_card(
    body: VirtualCardIssueRequest, user: CurrentUserDep, cards: CardServiceDep
) -> CardDetails:
    return _details(await cards.issue_virtual(user, body.account_id))


@router.post(
    "/plastic",
    status_code=status.HTTP_201_CREATED,
    summary="Order a plastic card with delivery → card.issued",
    responses=_ISSUE_ERRORS,
)
async def order_plastic_card(
    body: PlasticCardOrderRequest, user: CurrentUserDep, cards: CardServiceDep
) -> CardDetails:
    return _details(await cards.order_plastic(user, body.account_id, body.address))


# ---------- read ----------


@router.get("", summary="Current user's cards (all statuses)")
async def list_cards(user: CurrentUserDep, cards: CardServiceDep) -> list[CardRead]:
    return [CardRead.from_card(card) for card in await cards.list_for_user(user)]


@router.get("/{card_id}", summary="Card details", responses=_NOT_FOUND)
async def get_card(card_id: uuid.UUID, user: CurrentUserDep, cards: CardServiceDep) -> CardDetails:
    return _details(await cards.get_for_user(user, card_id))


# ---------- status ----------


@router.post(
    "/{card_id}/block",
    summary="Block an active card → card.blocked",
    responses={**_NOT_FOUND, **_WRONG_STATE, **_PROCESSOR_ERRORS},
)
async def block_card(
    card_id: uuid.UUID,
    user: CurrentUserDep,
    cards: CardServiceDep,
    body: Annotated[CardBlockRequest | None, Body()] = None,
) -> CardDetails:
    reason = (body or CardBlockRequest()).reason
    return _details(await cards.block(user, card_id, reason))


@router.post(
    "/{card_id}/unblock",
    summary="Unblock a blocked card → card.unblocked",
    responses={**_NOT_FOUND, **_WRONG_STATE, **_PROCESSOR_ERRORS},
)
async def unblock_card(
    card_id: uuid.UUID, user: CurrentUserDep, cards: CardServiceDep
) -> CardDetails:
    return _details(await cards.unblock(user, card_id))


@router.post(
    "/{card_id}/reissue",
    status_code=status.HTTP_201_CREATED,
    summary="Reissue a card: the old one becomes reissued → card.issued",
    responses={**_NOT_FOUND, **_WRONG_STATE, **_PROCESSOR_ERRORS},
)
async def reissue_card(
    card_id: uuid.UUID, user: CurrentUserDep, cards: CardServiceDep
) -> CardDetails:
    return _details(await cards.reissue(user, card_id))


# ---------- limits and MCC restrictions ----------


@router.get("/{card_id}/limits", summary="Spending limits", responses=_NOT_FOUND)
async def get_card_limits(
    card_id: uuid.UUID, user: CurrentUserDep, cards: CardServiceDep
) -> CardLimitsRead:
    return await cards.get_limits(user, card_id)


@router.put(
    "/{card_id}/limits",
    summary="Set daily/monthly/per-transaction limits (null = no limit)",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def update_card_limits(
    card_id: uuid.UUID, body: CardLimits, user: CurrentUserDep, cards: CardServiceDep
) -> CardLimitsRead:
    return await cards.update_limits(user, card_id, body)


@router.put(
    "/{card_id}/mcc-restrictions",
    summary="Set allowed/denied merchant category codes",
    responses={**_NOT_FOUND, **_WRONG_STATE},
)
async def update_card_mcc_restrictions(
    card_id: uuid.UUID, body: MccRestrictions, user: CurrentUserDep, cards: CardServiceDep
) -> CardMccRestrictionsRead:
    return await cards.update_mcc_restrictions(user, card_id, body)
