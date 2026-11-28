"""HTTP API: aggregates all routers."""

from fastapi import APIRouter

from elbank.api.routes import (
    accounts,
    analytics,
    auth,
    budgets,
    cards,
    credits,
    deposits,
    health,
    investments,
    kyc,
    operations,
    payments,
    transfers,
    users,
)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router, prefix="/api/v1/auth")
api_router.include_router(users.router, prefix="/api/v1/users")
api_router.include_router(kyc.router, prefix="/api/v1/kyc")
api_router.include_router(kyc.admin_router, prefix="/api/v1/admin/kyc")
api_router.include_router(accounts.router, prefix="/api/v1/accounts")
api_router.include_router(transfers.router, prefix="/api/v1/transfers")
api_router.include_router(payments.router, prefix="/api/v1/payments")
api_router.include_router(cards.router, prefix="/api/v1/cards")
api_router.include_router(credits.router, prefix="/api/v1/credits")
api_router.include_router(deposits.router, prefix="/api/v1/deposits")
api_router.include_router(deposits.admin_router, prefix="/api/v1/admin/deposits")
api_router.include_router(investments.router, prefix="/api/v1/investments")
api_router.include_router(operations.router, prefix="/api/v1/operations")
api_router.include_router(analytics.router, prefix="/api/v1/analytics")
api_router.include_router(budgets.router, prefix="/api/v1/budgets")

__all__ = ["api_router"]
