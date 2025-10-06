#!/usr/bin/env bash
set -euo pipefail
cd /opt/avio
git config --global --add safe.directory /opt/avio
# следим за изменениями, игнорим .git и временные файлы
inotifywait -mr -e close_write,create,delete,move --exclude '(^|/)\.git($|/)|\.swp$|~$' /opt/avio | \
while read -r _; do
  /opt/avio/tools/push-if-changed.sh || true
done
