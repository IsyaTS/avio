#!/usr/bin/env bash
set -euo pipefail
cd /opt/avio
git config --global --add safe.directory /opt/avio
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git fetch origin main
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git pull --rebase origin main
if [ -n "$(git status --porcelain)" ]; then
  git add -A
  git -c user.name="VPS Bot" -c user.email="vps@local" commit -m "vps: sync $(hostname) $(date -u +%F_%T)"
  GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' git push origin HEAD:main
fi
