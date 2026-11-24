"""Readiness probes for infrastructure dependencies."""

import asyncio
import time
from collections.abc import Awaitable, Callable

from sqlalchemy import text

from elbank.core.logging import get_logger
from elbank.deps import Resources
from elbank.schemas.health import DependencyStatus, ReadinessResponse, Status

logger = get_logger(__name__)

type Probe = Callable[[], Awaitable[object]]


async def _run_probe(name: str, probe: Probe, probe_timeout: float) -> DependencyStatus:
    started = time.perf_counter()
    try:
        async with asyncio.timeout(probe_timeout):
            await probe()
    except Exception as exc:  # noqa: BLE001 - any failure means "not ready"
        latency = (time.perf_counter() - started) * 1000
        error = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        logger.warning("readiness probe failed", dependency=name, error=error)
        return DependencyStatus(status="fail", latency_ms=round(latency, 2), error=error)
    latency = (time.perf_counter() - started) * 1000
    return DependencyStatus(status="ok", latency_ms=round(latency, 2))


async def check_readiness(resources: Resources, probe_timeout: float) -> ReadinessResponse:
    async def postgres() -> None:
        async with resources.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def redis() -> None:
        await resources.redis.ping()

    probes: dict[str, Probe] = {
        "postgres": postgres,
        "redis": redis,
        "kafka": resources.kafka.ping,
    }
    results = await asyncio.gather(
        *(_run_probe(name, probe, probe_timeout) for name, probe in probes.items())
    )
    checks = dict(zip(probes, results, strict=True))
    overall: Status = "ok" if all(c.status == "ok" for c in checks.values()) else "fail"
    return ReadinessResponse(status=overall, checks=checks)
