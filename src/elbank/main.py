"""FastAPI application factory and lifespan."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from elbank import __version__
from elbank.api import api_router
from elbank.api.middleware import CorrelationIdMiddleware
from elbank.clients import create_http_client
from elbank.core.config import Settings, get_settings
from elbank.core.exceptions import AppError
from elbank.core.logging import get_logger, setup_logging
from elbank.db.session import create_engine, create_session_factory
from elbank.deps import Resources
from elbank.kafka import EventPublisher, KafkaProducer
from elbank.services.operation_events import OperationEventRelay, bind_operation_events
from elbank.services.ticket_hub import TicketEventHub

logger = get_logger(__name__)

_EVENT_DRAIN_SECONDS = 5.0


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    engine = create_engine(settings.postgres)
    redis = Redis.from_url(str(settings.redis.url), decode_responses=True)
    kafka = KafkaProducer(settings.kafka)
    http = create_http_client(settings.wiremock.base_url, settings.wiremock.timeout_seconds)
    try:
        await kafka.start()
    except Exception as exc:  # noqa: BLE001 - app must boot; /ready reports Kafka as down
        logger.warning("kafka producer start deferred", error=str(exc) or type(exc).__name__)

    session_factory = create_session_factory(engine)
    relay = OperationEventRelay(EventPublisher(kafka))
    bind_operation_events(session_factory, relay)
    ticket_hub = TicketEventHub(settings.kafka, settings.support.ws_queue_size)
    ticket_hub.start()
    app.state.resources = Resources(
        engine=engine,
        session_factory=session_factory,
        redis=redis,
        kafka=kafka,
        http=http,
        ticket_hub=ticket_hub,
    )
    logger.info("application started", version=__version__)
    try:
        yield
    finally:
        await ticket_hub.stop()
        await relay.drain(_EVENT_DRAIN_SECONDS)
        await kafka.stop()
        await http.aclose()
        await redis.aclose()
        await engine.dispose()
        logger.info("application stopped")


async def app_error_handler(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AppError)
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message, "code": exc.code},
        headers=exc.headers,
    )


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
    app.add_exception_handler(AppError, app_error_handler)
    app.include_router(api_router)
    return app


app = create_app()
