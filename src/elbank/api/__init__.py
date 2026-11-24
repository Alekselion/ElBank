"""HTTP API: aggregates all routers."""

from fastapi import APIRouter

from elbank.api.routes import auth, health

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router, prefix="/api/v1/auth")

__all__ = ["api_router"]
