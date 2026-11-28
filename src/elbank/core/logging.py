"""Structured JSON logging: structlog on top of stdlib logging, shipped to Logstash.

Every record — from structlog loggers *and* third-party stdlib loggers (uvicorn,
SQLAlchemy, ...) — goes through one processor chain and becomes a JSON object
with the mandatory fields::

    timestamp, level, logger, message, correlation_id, service, env

plus any extra key/values passed to the logger. PII is masked in every record
before it is rendered (:mod:`elbank.core.pii`): secrets/documents by key, phone and
card numbers anywhere in text. The same JSON is written to
stdout and sent asynchronously to Logstash over TCP (one object per line).

Logstash being unavailable never breaks or blocks the application: events are
queued in memory, retried in the background and dropped after
``LOGSTASH_EVENT_TTL_SECONDS``; the shipper's own errors go to stdout only.
"""

import logging
from collections.abc import MutableMapping
from typing import Any

import structlog
from logstash_async.handler import AsynchronousLogstashHandler

from elbank.core.config import Settings
from elbank.core.pii import redact

LOGSTASH_EVENT_TTL_SECONDS = 300

_REQUIRED_KEYS = ("timestamp", "level", "logger", "message", "correlation_id", "service", "env")

type EventDict = MutableMapping[str, Any]


def _static_fields(settings: Settings) -> structlog.typing.Processor:
    service, env = settings.app.name, settings.app.env

    def add_static_fields(_: Any, __: str, event_dict: EventDict) -> EventDict:
        event_dict["service"] = service
        event_dict["env"] = env
        event_dict.setdefault("correlation_id", None)
        return event_dict

    return add_static_fields


def _uppercase_level(_: Any, __: str, event_dict: EventDict) -> EventDict:
    if isinstance(level := event_dict.get("level"), str):
        event_dict["level"] = level.upper()
    return event_dict


_NOT_REDACTED = frozenset({"timestamp", "level", "logger", "correlation_id", "service", "env"})


def _redact_pii(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Last line of defence: mask PII that reached a log record (see core.pii)."""
    for key, value in list(event_dict.items()):
        if key not in _NOT_REDACTED:
            event_dict[key] = redact({key: value})[key]
    return event_dict


def _required_keys_first(_: Any, __: str, event_dict: EventDict) -> EventDict:
    ordered = {key: event_dict.pop(key, None) for key in _REQUIRED_KEYS}
    ordered.update(event_dict)
    return ordered


def _pre_chain(settings: Settings) -> list[structlog.typing.Processor]:
    """Enrichment shared by structlog loggers and foreign (stdlib) records."""
    return [
        structlog.contextvars.merge_contextvars,  # correlation_id, bound per request
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        _static_fields(settings),
    ]


def _json_formatter(settings: Settings) -> logging.Formatter:
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*_pre_chain(settings), structlog.stdlib.ExtraAdder()],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            _uppercase_level,
            structlog.processors.dict_tracebacks,
            _redact_pii,
            structlog.processors.EventRenamer("message"),
            _required_keys_first,
            structlog.processors.JSONRenderer(default=str),
        ],
    )


def _console_formatter(settings: Settings) -> logging.Formatter:
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*_pre_chain(settings), structlog.stdlib.ExtraAdder()],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            _redact_pii,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
    )


def _build_logstash_handler(settings: Settings, formatter: logging.Formatter) -> logging.Handler:
    handler: logging.Handler = AsynchronousLogstashHandler(
        host=settings.logstash.host,
        port=settings.logstash.port,
        database_path=None,  # in-memory queue; no SQLite buffer file in the container
        transport="logstash_async.transport.TcpTransport",
        event_ttl=LOGSTASH_EVENT_TTL_SECONDS,
    )
    # The handler appends "\n" itself, so the payload is exactly what json_lines expects.
    handler.setFormatter(formatter)
    return handler


def setup_logging(settings: Settings) -> None:
    """Configure stdlib logging and structlog. Safe to call more than once."""
    json_formatter = _json_formatter(settings)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        json_formatter if settings.log.json_format else _console_formatter(settings)
    )
    handlers: list[logging.Handler] = [console_handler]

    logstash_error: Exception | None = None
    if settings.logstash.enabled:
        try:
            handlers.append(_build_logstash_handler(settings, json_formatter))
        except Exception as exc:  # noqa: BLE001 - logging to stdout must keep working
            logstash_error = exc

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
        existing.close()
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(settings.log.level)

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
            *_pre_chain(settings),
            structlog.processors.StackInfoRenderer(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log.level]
        ),
        cache_logger_on_first_use=True,
    )

    if logstash_error is not None:
        get_logger(__name__).warning(
            "logstash handler disabled, logging to stdout only",
            error=f"{type(logstash_error).__name__}: {logstash_error}",
        )


def get_logger(name: str | None = None, **initial_values: Any) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name, **initial_values)
    return logger
