"""Liveness and readiness endpoints."""

from fastapi import APIRouter, Response, status

from elbank import __version__
from elbank.deps import ResourcesDep, SettingsDep
from elbank.schemas.health import HealthResponse, ReadinessResponse
from elbank.services.health import check_readiness

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness probe")
async def health() -> HealthResponse:
    return HealthResponse(version=__version__)


@router.get(
    "/ready",
    summary="Readiness probe (PostgreSQL, Redis, Kafka)",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def ready(
    resources: ResourcesDep, settings: SettingsDep, response: Response
) -> ReadinessResponse:
    result = await check_readiness(resources, settings.readiness_timeout_seconds)
    if result.status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return result
