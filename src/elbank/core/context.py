"""Request-scoped context shared across layers (HTTP, logging, Kafka, outgoing HTTP)."""

from contextvars import ContextVar

CORRELATION_ID_HEADER = "X-Correlation-Id"

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str | None:
    return correlation_id_var.get()
