#!/usr/bin/env bash
set -euo pipefail
SVC="${1:-}"
if [ -z "$SVC" ]; then
  SVC="$(docker ps --format '{{.Names}} {{.Image}}' | grep -Ei 'waweb|whatsapp' | head -n1 | awk '{print $1}')"
fi
[ -z "$SVC" ] && { echo "$(date -u) waweb not found"; exit 1; }
RUN="$(docker inspect -f '{{.State.Running}}' "$SVC" 2>/dev/null || echo false)"
HEALTH="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$SVC" 2>/dev/null || true)"
if [ "$RUN" != "true" ]; then
  echo "$(date -u) $SVC running=$RUN -> restart"; docker restart "$SVC" >/dev/null 2>&1 || true; exit 0
fi
if [ -n "$HEALTH" ] && [ "$HEALTH" = "unhealthy" ]; then
  echo "$(date -u) $SVC health=$HEALTH -> restart"; docker restart "$SVC" >/dev/null 2>&1 || true; exit 0
fi
echo "$(date -u) $SVC ok (health=${HEALTH:-none})"
