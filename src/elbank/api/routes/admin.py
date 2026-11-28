"""Back office: ``/api/v1/admin/*`` (users, tariffs, rates, audit, reconciliation, anti-fraud).

Roles:

=============================================== ==========================
GET /users, /tariffs, /rates, /fraud-rules      manager, admin, auditor
POST /users/{id}/block, /unblock                manager (clients only), admin
POST /users/{id}/role                           admin
PUT /tariffs, PUT/DELETE /rates/{pair}          admin
POST/PUT/DELETE /fraud-rules                    admin
GET /operations/fraud-cases                     manager, admin, auditor
POST /operations/{id}/confirm, /reject          manager, admin
GET /audit                                      auditor
POST /reconciliation/run, GET /reconciliation/* admin, auditor
=============================================== ==========================
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Request, Response, status
from pydantic import AwareDatetime

from elbank.api.deps import (
    AdminUserDep,
    AdminUserServiceDep,
    AuditorUserDep,
    AuditSearchServiceDep,
    BackOfficeReaderDep,
    FraudReviewServiceDep,
    FraudRuleServiceDep,
    RatesServiceDep,
    ReconcilerUserDep,
    ReconciliationServiceDep,
    StaffUserDep,
    TariffServiceDep,
)
from elbank.api.openapi import Responses, error_response
from elbank.core.exceptions import UnprocessableError
from elbank.db.models import FraudCase, FraudCaseStatus, Operation, UserRole, UserStatus
from elbank.schemas.admin import (
    AuditRecordRead,
    BlockUserRequest,
    ChangeRoleRequest,
    EffectiveRateRead,
    FraudCaseRead,
    FraudDecisionRequest,
    FraudRuleCreate,
    FraudRuleRead,
    FraudRuleUpdate,
    PageRead,
    RateOverrideRead,
    RateOverrideRequest,
    RatesRead,
    ReconciliationReport,
    ReconciliationRunRead,
    ReconciliationRunRequest,
    TariffRead,
    TariffsUpdateRequest,
    UserAdminRead,
)
from elbank.services.admin_users import UserFilter
from elbank.services.audit_search import MAX_RESULT_WINDOW, AuditQuery
from elbank.services.rates import parse_pair
from elbank.services.tariffs import TariffView

_AUTH_ERRORS: Responses = {
    status.HTTP_401_UNAUTHORIZED: error_response("Missing, invalid or revoked token"),
    status.HTTP_403_FORBIDDEN: error_response("Role not allowed"),
}
_USER_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("User not found")}
_RULE_NOT_FOUND: Responses = {status.HTTP_404_NOT_FOUND: error_response("Fraud rule not found")}
_BAD_PAIR: Responses = {
    status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Bad currency pair or rate")
}

PageNumber = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]

router = APIRouter(tags=["admin"], responses=_AUTH_ERRORS)


# ---------- users ----------


@router.get("/users", summary="Search users (manager/admin/auditor)")
async def list_users(
    *,
    _: BackOfficeReaderDep,
    users: AdminUserServiceDep,
    search: Annotated[
        str | None,
        Query(min_length=1, max_length=100, description="Part of phone, email or full name"),
    ] = None,
    status_filter: Annotated[UserStatus | None, Query(alias="status")] = None,
    role: UserRole | None = None,
    page: PageNumber = 1,
    size: PageSize = 20,
) -> PageRead[UserAdminRead]:
    items, total = await users.search(UserFilter(search, status_filter, role), page, size)
    return PageRead(
        items=[UserAdminRead.model_validate(u) for u in items], page=page, size=size, total=total
    )


@router.post(
    "/users/{user_id}/block",
    summary="Block a user: logs them out everywhere, publishes user.blocked (manager/admin)",
    responses={
        **_USER_NOT_FOUND,
        status.HTTP_409_CONFLICT: error_response("Already blocked or deleted"),
    },
)
async def block_user(
    user_id: uuid.UUID,
    staff: StaffUserDep,
    users: AdminUserServiceDep,
    body: BlockUserRequest | None = None,
) -> UserAdminRead:
    reason = body.reason if body is not None else None
    return UserAdminRead.model_validate(await users.block(staff, user_id, reason))


@router.post(
    "/users/{user_id}/unblock",
    summary="Unblock a user, publishes user.unblocked (manager/admin)",
    responses={**_USER_NOT_FOUND, status.HTTP_409_CONFLICT: error_response("Not blocked")},
)
async def unblock_user(
    user_id: uuid.UUID, staff: StaffUserDep, users: AdminUserServiceDep
) -> UserAdminRead:
    return UserAdminRead.model_validate(await users.unblock(staff, user_id))


@router.post(
    "/users/{user_id}/role",
    summary="Change a user's role (admin)",
    responses={**_USER_NOT_FOUND, status.HTTP_409_CONFLICT: error_response("User is deleted")},
)
async def change_user_role(
    user_id: uuid.UUID, body: ChangeRoleRequest, admin: AdminUserDep, users: AdminUserServiceDep
) -> UserAdminRead:
    return UserAdminRead.model_validate(await users.change_role(admin, user_id, body.role))


# ---------- tariffs ----------


def _tariff(view: TariffView) -> TariffRead:
    return TariffRead(
        code=view.code,
        rule=view.rule,
        is_default=view.is_default,
        updated_at=view.updated_at,
        updated_by_id=view.updated_by_id,
    )


@router.get("/tariffs", summary="Fees per operation (manager/admin/auditor)")
async def list_tariffs(_: BackOfficeReaderDep, tariffs: TariffServiceDep) -> list[TariffRead]:
    return [_tariff(v) for v in await tariffs.list_all()]


@router.put(
    "/tariffs",
    summary="Change fees; applies to operations created from now on (admin)",
)
async def update_tariffs(
    body: TariffsUpdateRequest, admin: AdminUserDep, tariffs: TariffServiceDep
) -> list[TariffRead]:
    return [_tariff(v) for v in await tariffs.update(admin, body.tariffs)]


# ---------- rates ----------


@router.get(
    "/rates",
    summary="Effective rates of every pair and the overrides (manager/admin/auditor)",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Bank of Russia unavailable")},
)
async def list_rates(_: BackOfficeReaderDep, rates: RatesServiceDep) -> RatesRead:
    effective = await rates.all_rates()
    overrides = await rates.list_overrides()
    return RatesRead(
        rates=[
            EffectiveRateRead(base=r.base, quote=r.quote, rate=r.rate, date=r.date, source=r.source)
            for r in effective
        ],
        overrides=[RateOverrideRead.model_validate(o) for o in overrides],
    )


@router.put(
    "/rates/{pair}",
    summary="Override a rate, e.g. PUT /rates/USD-RUB; drops the Redis cache of the pair (admin)",
    responses=_BAD_PAIR,
)
async def set_rate_override(
    pair: str, body: RateOverrideRequest, admin: AdminUserDep, rates: RatesServiceDep
) -> RateOverrideRead:
    base, quote = parse_pair(pair)
    override = await rates.set_override(admin, base, quote, body.rate)
    return RateOverrideRead.model_validate(override)


@router.delete(
    "/rates/{pair}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove an override: the Bank of Russia rate applies again (admin)",
    responses={
        **_BAD_PAIR,
        status.HTTP_404_NOT_FOUND: error_response("No override for the pair"),
    },
)
async def delete_rate_override(pair: str, admin: AdminUserDep, rates: RatesServiceDep) -> Response:
    base, quote = parse_pair(pair)
    await rates.delete_override(admin, base, quote)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------- audit ----------


@router.get(
    "/audit",
    summary="Audit trail from Elasticsearch (auditor)",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Bad period or page too deep"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Elasticsearch unavailable"),
    },
)
async def search_audit(
    *,
    _: AuditorUserDep,
    audit_search: AuditSearchServiceDep,
    user_id: Annotated[uuid.UUID | None, Query(description="Actor or subject")] = None,
    action: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=100,
            pattern=r"^[a-z0-9_.]+\*?$",
            description='Exact action, or a prefix with "*": admin.user.*',
        ),
    ] = None,
    date_from: Annotated[AwareDatetime | None, Query(alias="from")] = None,
    date_to: Annotated[AwareDatetime | None, Query(alias="to")] = None,
    page: PageNumber = 1,
    size: PageSize = 50,
) -> PageRead[AuditRecordRead]:
    if date_from is not None and date_to is not None and date_from >= date_to:
        raise UnprocessableError("from must be before to")
    if page * size > MAX_RESULT_WINDOW:
        raise UnprocessableError(f"Only the first {MAX_RESULT_WINDOW} records can be paged")
    records, total = await audit_search.search(
        AuditQuery(user_id=user_id, action=action, date_from=date_from, date_to=date_to),
        page,
        size,
    )
    return PageRead(
        items=[AuditRecordRead.model_validate(r.model_dump()) for r in records],
        page=page,
        size=size,
        total=total,
    )


# ---------- reconciliation ----------


@router.post(
    "/reconciliation/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Aggregate operations of a period and start a reconciliation (admin/auditor)",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: error_response("Bad period"),
        status.HTTP_503_SERVICE_UNAVAILABLE: error_response("Kafka unavailable"),
    },
)
async def run_reconciliation(
    body: ReconciliationRunRequest,
    request: Request,
    staff: ReconcilerUserDep,
    reconciliation: ReconciliationServiceDep,
) -> ReconciliationRunRead:
    run = await reconciliation.request(staff, body.date_from, body.date_to)
    return ReconciliationRunRead(
        run_id=run.run_id,
        date_from=run.date_from,
        date_to=run.date_to,
        requested_at=run.requested_at,
        totals=run.totals,
        report_url=str(request.url_for("get_reconciliation_report", run_id=run.run_id)),
    )


@router.get(
    "/reconciliation/{run_id}",
    summary="Reconciliation report (admin/auditor); 404 until the worker has written it",
    responses={status.HTTP_404_NOT_FOUND: error_response("Report not found or not ready yet")},
)
async def get_reconciliation_report(
    run_id: uuid.UUID, _: ReconcilerUserDep, reconciliation: ReconciliationServiceDep
) -> ReconciliationReport:
    return reconciliation.load_report(run_id)


# ---------- fraud rules ----------


@router.get("/fraud-rules", summary="Anti-fraud rules (manager/admin/auditor)")
async def list_fraud_rules(
    _: BackOfficeReaderDep, rules: FraudRuleServiceDep
) -> list[FraudRuleRead]:
    return [FraudRuleRead.model_validate(r) for r in await rules.list_all()]


@router.post(
    "/fraud-rules",
    status_code=status.HTTP_201_CREATED,
    summary="Create a rule; the condition is validated (admin)",
    responses={status.HTTP_409_CONFLICT: error_response("Name already used")},
)
async def create_fraud_rule(
    body: FraudRuleCreate, admin: AdminUserDep, rules: FraudRuleServiceDep
) -> FraudRuleRead:
    rule = await rules.create(
        admin,
        name=body.name,
        condition=body.condition,
        description=body.description,
        enabled=body.enabled,
    )
    return FraudRuleRead.model_validate(rule)


@router.put(
    "/fraud-rules/{rule_id}",
    summary="Update a rule: only sent fields change (admin)",
    responses={
        **_RULE_NOT_FOUND,
        status.HTTP_409_CONFLICT: error_response("Name already used"),
    },
)
async def update_fraud_rule(
    rule_id: uuid.UUID, body: FraudRuleUpdate, admin: AdminUserDep, rules: FraudRuleServiceDep
) -> FraudRuleRead:
    changes = {field: getattr(body, field) for field in body.model_fields_set}
    rule = await rules.update(admin, rule_id, changes)
    return FraudRuleRead.model_validate(rule)


@router.delete(
    "/fraud-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a rule (admin)",
    responses=_RULE_NOT_FOUND,
)
async def delete_fraud_rule(
    rule_id: uuid.UUID, admin: AdminUserDep, rules: FraudRuleServiceDep
) -> Response:
    await rules.delete(admin, rule_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------- fraud review ----------

_CASE_ERRORS: Responses = {
    status.HTTP_404_NOT_FOUND: error_response("No anti-fraud case for the operation"),
    status.HTTP_409_CONFLICT: error_response("Already decided or not held"),
}


def _case(case: FraudCase, operation: Operation) -> FraudCaseRead:
    return FraudCaseRead(
        id=case.id,
        status=case.status,
        operation_id=operation.id,
        operation_type=operation.type,
        operation_status=operation.status,
        error_code=operation.error_code,
        amount=operation.amount,
        currency=operation.currency,
        user_id=case.user_id,
        rules=case.rules,
        ticket_id=case.ticket_id,
        decided_by_id=case.decided_by_id,
        decided_at=case.decided_at,
        decision_reason=case.decision_reason,
        created_at=case.created_at,
    )


@router.get("/operations/fraud-cases", summary="Held operations, newest first (staff/auditor)")
async def list_fraud_cases(
    *,
    _: BackOfficeReaderDep,
    review: FraudReviewServiceDep,
    status_filter: Annotated[FraudCaseStatus | None, Query(alias="status")] = FraudCaseStatus.OPEN,
    page: PageNumber = 1,
    size: PageSize = 20,
) -> PageRead[FraudCaseRead]:
    items, total = await review.list_cases(status_filter, page, size)
    return PageRead(items=[_case(c, o) for c, o in items], page=page, size=size, total=total)


@router.post(
    "/operations/{operation_id}/confirm",
    summary="Release a held operation: it continues (manager/admin)",
    responses=_CASE_ERRORS,
)
async def confirm_operation(
    operation_id: uuid.UUID,
    manager: StaffUserDep,
    review: FraudReviewServiceDep,
    body: FraudDecisionRequest | None = None,
) -> FraudCaseRead:
    result = await review.confirm(manager, operation_id, body.reason if body else None)
    return _case(result.case, result.operation)


@router.post(
    "/operations/{operation_id}/reject",
    summary="Reject a held operation: failed, money returned (manager/admin)",
    responses=_CASE_ERRORS,
)
async def reject_operation(
    operation_id: uuid.UUID,
    manager: StaffUserDep,
    review: FraudReviewServiceDep,
    body: FraudDecisionRequest | None = None,
) -> FraudCaseRead:
    result = await review.reject(manager, operation_id, body.reason if body else None)
    return _case(result.case, result.operation)
