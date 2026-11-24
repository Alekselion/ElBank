"""FastAPI application factory and lifespan."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from elbank import __version__
from elbank.api import api_router
from elbank.api.middleware import CorrelationIdMiddleware
from elbank.core.config import Settings, get_settings
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.deps import Resources
from elbank.kafka import KafkaProducer

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    kafka = KafkaProducer(settings.kafka)
    try:
        await kafka.start()
    except Exception as exc:  # noqa: BLE001 - app must boot; /ready reports Kafka as down
        logger.warning("kafka producer start deferred", error=str(exc) or type(exc).__name__)

    app.state.resources = Resources(
        engine=engine,
        session_factory=create_session_factory(engine),
        redis=redis,
        kafka=kafka,
    )
    logger.info("application started", version=__version__)
    try:
        yield
    finally:
        await kafka.stop()
        await redis.aclose()
        await engine.dispose()
        logger.info("application stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings)

    app = FastAPI(
        title="ElBank",
        version=__version__,
        debug=settings.app.debug,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.add_middleware(CorrelationIdMiddleware)
    app.include_router(api_router)
    return app


app = create_app()
