"""OpenAPI helpers shared by routers."""

from typing import Any

type Responses = dict[int | str, dict[str, Any]]


def error_response(description: str) -> dict[str, Any]:
    """Document an error rendered by the ``AppError`` handler."""
    return {
        "description": description,
        "content": {"application/json": {"example": {"detail": "message", "code": "machine_code"}}},
    }
