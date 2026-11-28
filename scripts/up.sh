#!/usr/bin/env bash
# Build and start every service and wait until they are healthy.
#
# `docker compose up --wait` treats a one-shot container that exits (even with 0) as a
# failure unless another service waits for it, so the long-running services are waited
# for and the one-shots that nobody depends on (kibana-setup) run after them.
# migrate and kafka-init run as dependencies (service_completed_successfully).
#
# kibana-setup runs with `docker compose run --no-deps`, not `up --exit-code-from`:
# the latter implies --abort-on-container-exit and would stop kibana and elasticsearch
# as soon as the one-shot exits. Its dependency (kibana) is already healthy here.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo ".env not found: copy .env.example to .env and set the secrets first." >&2
  exit 1
fi

one_shots=(migrate kafka-init kibana-setup)
# A plain assignment so that a failing `docker compose config` stops the script (set -e
# does not see failures inside process substitution).
services="$(docker compose config --services)"
mapfile -t long_running < <(grep -vxF -f <(printf '%s\n' "${one_shots[@]}") <<<"${services}")
if [[ ${#long_running[@]} -eq 0 ]]; then
  echo "No long-running services found in docker-compose.yml." >&2
  exit 1
fi

docker compose up -d --build --wait --wait-timeout "${WAIT_TIMEOUT:-600}" "${long_running[@]}"
docker compose run --rm --no-deps kibana-setup
