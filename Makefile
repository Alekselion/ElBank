# ElBank developer commands. `make help` lists them.
# migrate/seed run inside the application image, so no local Python is required;
# lint/fmt/typecheck use the local uv environment (`uv sync`).

COMPOSE ?= docker compose
SERVICE ?=

.DEFAULT_GOAL := help

.PHONY: help up down logs ps build migrate seed reset lint fmt typecheck

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

up: ## Build and start every service, wait until healthy
	scripts/up.sh

down: ## Stop and remove containers (data volumes are kept)
	$(COMPOSE) down

logs: ## Follow logs (all services, or SERVICE=elbank-app)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

ps: ## Service states and health
	$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

build: ## Build the application image
	$(COMPOSE) build elbank-app

migrate: ## alembic upgrade head (inside the app image)
	$(COMPOSE) run --rm migrate

seed: ## Load demo users, accounts and operations (idempotent)
	$(COMPOSE) run --rm --no-deps elbank-app python -m scripts.seed

reset: ## DELETE all data, start from scratch, migrate and seed
	scripts/reset.sh -y

lint: ## ruff check + format check + mypy (strict)
	uv run ruff check src scripts
	uv run ruff format --check src scripts
	uv run mypy

fmt: ## Format and autofix
	uv run ruff format src scripts
	uv run ruff check --fix src scripts

typecheck: ## mypy (strict) only
	uv run mypy
