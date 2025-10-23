#!/usr/bin/env bash
# авто-коммит локальных правок и пуш в main, при конфликте — в запасную ветку
cd /opt/avio || return

git config user.name  "Avio Sync" >/dev/null
git config user.email "sync@avio.local" >/dev/null
git config push.default simple >/dev/null

REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
MSG_PREFIX="${MSG_PREFIX:-auto: vps sync}"

# known_hosts для github
mkdir -p ~/.ssh
[ -f ~/.ssh/known_hosts ] || touch ~/.ssh/known_hosts
ssh-keyscan -T 3 github.com 2>/dev/null | grep github.com >> ~/.ssh/known_hosts || true

# убедимся, что есть origin и нужная ветка
git remote get-url "$REMOTE" >/dev/null 2>&1 || git remote add "$REMOTE" git@github.com:IsyaTS/avio.git
git fetch "$REMOTE" --quiet || true
cur_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)"
if [ "$cur_branch" != "$BRANCH" ]; then
  git checkout -B "$BRANCH" "$REMOTE/$BRANCH" 2>/dev/null || git checkout "$BRANCH"
fi

# локальные изменения -> коммит
if ! git diff --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
  git add -A
  git commit -m "$MSG_PREFIX $(date -Iseconds)" || true
fi

# подтянуть удалённые изменения и запушить
if git pull --rebase "$REMOTE" "$BRANCH"; then
  git push "$REMOTE" "HEAD:$BRANCH"
else
  git rebase --abort 2>/dev/null || true
  side="vps-autosync-$(date +%Y%m%d-%H%M%S)"
  git push "$REMOTE" "HEAD:$side" && echo "diverged; pushed to $side"
fi
