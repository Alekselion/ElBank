#!/usr/bin/env bash
# Rebuild the whole environment from scratch: DELETES all data (volumes), starts every
# service, applies migrations and loads the demo data (scripts/seed.py).
#
#   scripts/reset.sh        # asks for confirmation
#   scripts/reset.sh -y     # no questions (CI, make reset)
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" != "-y" ]]; then
  read -r -p "This deletes ALL ElBank data (PostgreSQL, Kafka, Elasticsearch, uploads). Continue? [y/N] " answer
  [[ "${answer}" == "y" || "${answer}" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

if [[ ! -f .env ]]; then
  echo ".env not found: copy .env.example to .env and set the secrets first." >&2
  exit 1
fi

echo "==> docker compose down -v"
docker compose down -v --remove-orphans

echo "==> docker compose up (build, wait until healthy)"
scripts/up.sh

# The "migrate" service already ran on start; running it again is idempotent and makes
# the step explicit (alembic upgrade head inside the app image).
echo "==> alembic upgrade head"
docker compose run --rm migrate

echo "==> seed"
docker compose run --rm --no-deps elbank-app python -m scripts.seed

echo "==> done"
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
