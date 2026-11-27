"""HTTP API: aggregates all routers."""

from fastapi import APIRouter

from elbank.api.routes import accounts, auth, health, kyc, transfers, users

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router, prefix="/api/v1/auth")
api_router.include_router(users.router, prefix="/api/v1/users")
api_router.include_router(kyc.router, prefix="/api/v1/kyc")
api_router.include_router(kyc.admin_router, prefix="/api/v1/admin/kyc")
api_router.include_router(accounts.router, prefix="/api/v1/accounts")
api_router.include_router(transfers.router, prefix="/api/v1/transfers")

__all__ = ["api_router"]
