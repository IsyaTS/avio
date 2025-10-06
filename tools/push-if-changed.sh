#!/usr/bin/env bash
set -euo pipefail
cd /opt/avio
git config --global --add safe.directory /opt/avio

# 1) если есть локальные изменения — закоммить сначала
if [ -n "$(git status --porcelain)" ]; then
  git add -A
  git -c user.name="VPS Bot" -c user.email="vps@local" \
      commit -m "vps: auto sync $(hostname) $(date -u +%F_%T)"
fi

# 2) подтянуть удалённые поверх (автостэш на случай новых незакоммиченных)
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' \
  git pull --rebase --autostash origin main

# 3) запушить
GIT_SSH_COMMAND='ssh -i /home/deploy/.ssh/id_ed25519_repo' \
  git push origin HEAD:main
