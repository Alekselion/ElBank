"""Monthly spending budgets (``/api/v1/budgets``)."""

import uuid

from fastapi import APIRouter, Response, status

from elbank.api.deps import BudgetServiceDep, CurrentUserDep
from elbank.api.openapi import Responses, error_response
from elbank.schemas.budget import BudgetCreate, BudgetRead, BudgetStatusReport, BudgetUpdate

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Not allowed"),
}
_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Budget not found")}

router = APIRouter(tags=["budgets"], responses=_AUTH_ERRORS)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create a budget for a category and currency",
    responses={
        status.HTTP_409_CONFLICT: error_response("Budget for the category already exists"),
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Too many budgets"),
    },
)
async def create_budget(
    data: BudgetCreate, user: CurrentUserDep, budgets: BudgetServiceDep
) -> BudgetRead:
    return BudgetRead.model_validate(await budgets.create(user, data))


@router.get("", summary="Own budgets")
async def list_budgets(user: CurrentUserDep, budgets: BudgetServiceDep) -> list[BudgetRead]:
    return [BudgetRead.model_validate(b) for b in await budgets.list_for_user(user)]


# Declared before "/{budget_id}" so "status" is not parsed as a budget id.
@router.get("/status", summary="Spent / limit / percent of every budget this month (UTC)")
async def budgets_status(user: CurrentUserDep, budgets: BudgetServiceDep) -> BudgetStatusReport:
    return await budgets.status(user)


@router.get("/{budget_id}", summary="Budget details", responses=_NOT_FOUND)
async def get_budget(
    budget_id: uuid.UUID, user: CurrentUserDep, budgets: BudgetServiceDep
) -> BudgetRead:
    return BudgetRead.model_validate(await budgets.get_for_user(user, budget_id))


@router.patch("/{budget_id}", summary="Change the monthly limit", responses=_NOT_FOUND)
async def update_budget(
    budget_id: uuid.UUID, data: BudgetUpdate, user: CurrentUserDep, budgets: BudgetServiceDep
) -> BudgetRead:
    return BudgetRead.model_validate(await budgets.update(user, budget_id, data))


@router.delete(
    "/{budget_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a budget",
    responses=_NOT_FOUND,
)
async def delete_budget(
    budget_id: uuid.UUID, user: CurrentUserDep, budgets: BudgetServiceDep
) -> Response:
    await budgets.delete(user, budget_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
