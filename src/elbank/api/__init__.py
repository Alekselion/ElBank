"""HTTP API: aggregates all routers."""

from fastapi import APIRouter

from elbank.api.routes import health

api_router = APIRouter()
api_router.include_router(health.router)

__all__ = ["api_router"]
