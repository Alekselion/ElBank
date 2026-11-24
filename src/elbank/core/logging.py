"""Structured logging: structlog on top of stdlib logging, shipped to Logstash.

Every record (from structlog *and* third-party stdlib loggers) goes through the
same processor chain, is rendered to stdout (console or JSON) and, when enabled,
is sent asynchronously to Logstash over TCP as JSON.
"""

import logging
import logging.config
from typing import Any

import structlog
from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler

from elbank.core.config import Settings

_SHARED_PROCESSORS: list[structlog.typing.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.ExtraAdder(),
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
]


def _console_processors(settings: Settings) -> list[structlog.typing.Processor]:
    if settings.log_json:
        return [structlog.processors.dict_tracebacks, structlog.processors.JSONRenderer()]
    return [structlog.dev.ConsoleRenderer(colors=settings.app_env == "local")]


def _build_logstash_handler(settings: Settings) -> logging.Handler:
    handler: logging.Handler = AsynchronousLogstashHandler(
        host=settings.logstash_host,
        port=settings.logstash_port,
        database_path=None,  # in-memory queue; no SQLite buffer file in the container
        transport="logstash_async.transport.TcpTransport",
    )
    handler.setFormatter(
        LogstashFormatter(
            extra_prefix=None,
            extra={"application": settings.app_name, "environment": settings.app_env},
        )
    )
    return handler


def setup_logging(settings: Settings) -> None:
    """Configure stdlib logging and structlog. Safe to call more than once."""
    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            *_console_processors(settings),
        ],
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)

    handlers: list[logging.Handler] = [console_handler]
    if settings.logstash_enabled:
        handlers.append(_build_logstash_handler(settings))

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
        existing.close()
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(settings.log_level)

    # Route uvicorn's loggers through the root handlers instead of their own.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True

    for noisy in ("aiokafka", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # The Logstash shipper's own errors (e.g. Logstash is down) must not be fed back
    # into the Logstash queue: console only.
    worker_logger = logging.getLogger("LogProcessingWorker")
    worker_logger.handlers.clear()
    worker_logger.addHandler(console_handler)
    worker_logger.setLevel(logging.WARNING)
    worker_logger.propagate = False

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.StackInfoRenderer(),
            # Hands the event dict to stdlib as ``msg`` + ``extra`` (+ ``exc_info``):
            # ProcessorFormatter enriches and renders it for the console via the
            # shared pre-chain, LogstashFormatter ships the extras as JSON fields.
            structlog.stdlib.render_to_log_kwargs,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log_level]
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None, **initial_values: Any) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name, **initial_values)
    return logger
