#!/usr/bin/env bash
set -euo pipefail

APP_URL=${APP_URL:-http://127.0.0.1:8000}
WAWEB_URL=${WAWEB_URL:-http://waweb:9001}
ADMIN_TOKEN=${ADMIN_TOKEN:-${WAWEB_ADMIN_TOKEN:-}}
TENANT=${TENANT:-1}
WA_TEST_TO=${WA_TEST_TO:-79990000000}
WA_JID_OVERRIDE=${WA_JID:-}
WA_JID=${WA_JID_OVERRIDE:-${WA_TEST_TO}@c.us}
CURL_OPTS=(--silent --show-error --fail)

function curl_json() {
  local label=$1; shift
  echo "\n### ${label}"
  if ! curl "${CURL_OPTS[@]}" -w '\nHTTP %{http_code}\n' "$@"; then
    echo "curl failed (${label})" >&2
  fi
}

echo "== WhatsApp diagnostics =="

curl_json "app /health" "${APP_URL}/health"
curl_json "waweb /health" "${WAWEB_URL}/health"

echo "\n### Outbox flags"
echo "OUTBOX_ENABLED=${OUTBOX_ENABLED:-}"
echo "OUTBOX_WHITELIST=${OUTBOX_WHITELIST:-}"
echo "WAWEB_ADMIN_TOKEN set? $([[ -n ${WAWEB_ADMIN_TOKEN:-} ]] && echo yes || echo no)"

echo "\n### Test send (digits)"
curl -sS -w '\nHTTP %{http_code}\n' -X POST "${APP_URL}/send" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d @- <<JSON || true
{"tenant": ${TENANT}, "channel": "whatsapp", "to": "${WA_TEST_TO}", "text": "diag digits"}
JSON

echo "\n### Test send (jid)"
curl -sS -w '\nHTTP %{http_code}\n' -X POST "${APP_URL}/send" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d @- <<JSON || true
{"tenant": ${TENANT}, "channel": "whatsapp", "to": "${WA_JID}", "text": "diag jid"}
JSON

echo "\n### Logs (last 2m)"
docker compose logs --since=2m app waweb 2>&1 | tail -n +1
