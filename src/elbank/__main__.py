"""Entry point: ``python -m elbank`` or the ``elbank`` console script."""

import uvicorn

from elbank.core.config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "elbank.main:app",
        host=settings.app.host,
        port=settings.app.port,
        reload=settings.app.env == "local",
        log_config=None,  # logging is configured by elbank.core.logging
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
