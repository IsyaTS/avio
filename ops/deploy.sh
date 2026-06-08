#!/usr/bin/env bash
set -euo pipefail
cd /opt/avio
previous_app_image="$(docker compose images -q app 2>/dev/null || true)"
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git fetch --all
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git reset --hard origin/main
true # skip pull
docker compose build --pull
docker compose up -d app worker

for i in {1..45}; do
  if curl -fsS http://127.0.0.1:8000/health >/dev/null; then
    break
  fi
  if [[ "$i" == "45" ]]; then
    echo "app health failed after deploy" >&2
    docker compose logs --tail=200 app worker >&2 || true
    if [[ -n "$previous_app_image" ]]; then
      echo "previous app image was $previous_app_image" >&2
    fi
    exit 1
  fi
  sleep 2
done

if [[ -n "${ADMIN_TOKEN:-}" ]]; then
  python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants "${SMOKE_TENANTS:-1,3}"
else
  echo "ADMIN_TOKEN is not set; skipping protected critical smoke" >&2
fi

docker image prune -f || true
