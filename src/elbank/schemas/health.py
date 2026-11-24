"""Health/readiness response schemas."""

from typing import Literal

from pydantic import BaseModel

type Status = Literal["ok", "fail"]


class HealthResponse(BaseModel):
    status: Status = "ok"
    version: str


class DependencyStatus(BaseModel):
    status: Status
    latency_ms: float
    error: str | None = None


class ReadinessResponse(BaseModel):
    status: Status
    checks: dict[str, DependencyStatus]
