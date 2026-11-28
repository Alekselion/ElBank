# syntax=docker/dockerfile:1.7
#
# Multi-stage build:
#   builder — uv resolves the locked dependencies into /app/.venv and installs the project
#             (non-editable); the venv is then trimmed (tests, type stubs, debug symbols).
#   runtime — python:slim + the venv only: no uv, no compilers, non-root user.
# Every service of docker-compose runs this image with a different command
# (uvicorn, alembic, elbank-*-worker, python -m scripts.seed).

ARG PYTHON_VERSION=3.15
ARG UV_VERSION=0.12.7

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# ---------- builder ----------
FROM python:${PYTHON_VERSION}-slim AS builder

COPY --from=uv /uv /usr/local/bin/uv

# binutils: `strip` for the trimming step below.
RUN apt-get update \
 && apt-get install --no-install-recommends -y binutils \
 && rm -rf /var/lib/apt/lists/*

ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Dependencies first: this layer is cached until pyproject.toml/uv.lock change.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Install the project itself as a regular (non-editable) package.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# Trim the venv: nothing below is used at runtime. No bytecode is shipped for the venv
# (it would add ~60 MB): each container compiles the dependencies it imports on start,
# into PYTHONPYCACHEPREFIX.
RUN set -eux; \
    find /app/.venv \( -name '__pycache__' -o -name 'tests' -o -name 'test' \) -type d -prune -exec rm -rf {} +; \
    find /app/.venv \( -name '*.pyi' -o -name '*.pyx' -o -name '*.pxd' -o -name '*.c' -o -name '*.h' \) -type f -delete; \
    find /app/.venv -name '*.so' -type f -exec strip --strip-unneeded {} +; \
    rm -f /app/.venv/bin/activate* /app/.venv/bin/*.bat /app/.venv/bin/*.ps1

# ---------- runtime ----------
FROM python:${PYTHON_VERSION}-slim AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

# DejaVu: TTF with Cyrillic glyphs for PDF exports (EXPORTS_PDF_FONT_PATH).
RUN apt-get update \
 && apt-get install --no-install-recommends -y fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid ${APP_GID} elbank \
 && useradd --system --uid ${APP_UID} --gid elbank --no-create-home --shell /usr/sbin/nologin elbank \
 # Mount point for the uploads volume; a fresh named volume inherits this ownership.
 && install -d -o elbank -g elbank -m 0750 /app/uploads

# Code is owned by root (read-only for elbank); the bytecode cache lives in /tmp for the
# lifetime of the container. Trade-off: with PYTHONPYCACHEPREFIX set, Python ignores
# every __pycache__ next to the sources, including the precompiled stdlib of the base
# image, so each container (app, workers, migrate, seed) compiles the stdlib and the
# dependencies it imports on its first start — a slower cold start and extra CPU in
# exchange for a ~60 MB smaller image. The healthcheck start-period allows for it.
ENV PYTHONPYCACHEPREFIX=/tmp/pycache \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY alembic ./alembic
# Only the Python helpers; the shell scripts (up.sh, reset.sh) run on the host.
COPY scripts/__init__.py scripts/seed.py ./scripts/

USER elbank

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status != 200)"]

CMD ["uvicorn", "elbank.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
