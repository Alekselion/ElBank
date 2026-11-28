"""Request-scoped context shared across layers (HTTP, logging, Kafka, outgoing HTTP)."""

from contextvars import ContextVar

CORRELATION_ID_HEADER = "X-Correlation-Id"

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str | None:
    return correlation_id_var.get()


# Set by the audit middleware for the duration of an HTTP request (audit events read them).
client_ip_var: ContextVar[str | None] = ContextVar("client_ip", default=None)
user_agent_var: ContextVar[str | None] = ContextVar("user_agent", default=None)
