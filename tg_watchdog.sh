#!/usr/bin/env bash
set -euo pipefail
SVC="${1:-}"
if [ -z "$SVC" ]; then
  SVC="$(docker ps --format '{{.Names}} {{.Image}}' | grep -Ei 'tgworker' | head -n1 | awk '{print $1}')"
fi
if [ -z "$SVC" ]; then
  echo "$(date -u) tgworker not found"
  exit 1
fi
RUN="$(docker inspect -f '{{.State.Running}}' "$SVC" 2>/dev/null || echo false)"
HEALTH="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$SVC" 2>/dev/null || true)"
if [ "$RUN" != "true" ]; then
  echo "$(date -u) $SVC running=$RUN -> restart"
  docker restart "$SVC" >/dev/null 2>&1 || true
  exit 0
fi
if [ -n "$HEALTH" ] && [ "$HEALTH" = "unhealthy" ]; then
  echo "$(date -u) $SVC health=$HEALTH -> restart"
  docker restart "$SVC" >/dev/null 2>&1 || true
  exit 0
fi
if docker logs --since=1h "$SVC" 2>&1 | grep -qi 'needs_2fa\|rpc_error'; then
  echo "$(date -u) $SVC warning recent errors detected"
fi
echo "$(date -u) $SVC ok (health=${HEALTH:-none})"
