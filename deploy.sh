#!/usr/bin/env bash
set -euo pipefail
cd /opt/avio
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git fetch --all
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git reset --hard origin/main
true # skip pull
docker compose build --pull
docker image prune -f || true
