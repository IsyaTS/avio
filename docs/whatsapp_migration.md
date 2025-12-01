# WhatsApp Web (waweb) integration notes

> This document captures the current HTTP contract between the Python stack (`app`, `worker`, public/admin views) and the legacy WhatsApp Web bridge (`waweb`). All changes in later steps should preserve this contract until every tenant is migrated to the new provider.

## Where waweb lives today
- Docker: `docker-compose.yml` sets `WA_WEB_URL`/`WAWEB_BASE_URL`, while `compose/waweb/docker-compose.yml` defines the standalone Node + Puppeteer container (one per tenant via `scripts/waweb_manage.py`).
- Node service: `apps/waweb/index.js` exposes the HTTP API, handles sessions, and forwards provider events to FastAPI via `/webhook`.
- Python entry points resolve the service base URL through `core.tenant_waweb_url()` → `apps/api/web/common.wa_base_url()` so the host can be overridden per tenant.

## HTTP calls from Python to waweb
The list below mirrors every waweb endpoint that `app` or `worker` currently hits. Unless noted otherwise, `apps/api/web/common.http()` injects `X-Auth-Token: <WA_INTERNAL_TOKEN|WA_WEB_TOKEN|WEBHOOK_SECRET>` and every request uses JSON bodies.

| Purpose | Call sites | HTTP request | Payload / semantics | Response expectations |
| --- | --- | --- | --- | --- |
| **Send message** | `app/main.py:210-520` (`send_transport_message`), `app/worker.py:1551-1710` (`send_whatsapp`) | `POST {wa_base}/send?tenant=<tenant_id>` with `X-Auth-Token` (and `X-Admin-Token` from worker) | JSON generated from `TransportMessage`: `channel="whatsapp"`, `tenant`/`tenant_id`, `to` (normalized JID), optional `text`, `meta`, and media payloads. Attachments are either `attachment` (single) or `attachments` list; each item contains `type`, `url` (normalized via `_normalize_internal_attachment_url`), optional `path`, `b64`, `mime`, `caption`, `sendMediaAsDocument`, nested `document/image/video/...` objects, etc. Worker may also send `document` block when large PDFs must be re-uploaded by waweb. | `200 OK` with `{ "ok": true }` on success. `409` + `X-Reauth: 1` triggers QR refresh. Other non-2xx bodies are proxied back to the caller. |
| **Prefetch/start session** | `apps/api/web/public.wa_start`, `_proxy_qr_with_fallbacks`, `wa_restart`, onboarding flows | `POST {wa_base}/session/<tenant>/start` (fallback `POST /session/start` when tenant host is unknown) | `{ "tenant_id": <int>, "webhook_url": common.webhook_url() }`. Used both for tenant-scoped and global start. | JSON snapshot from `sessionStatusPayload` (`{ ok, ready, qr, last, qr_id, state }`). Caller only needs `200` vs error. |
| **Restart session** | `apps/api/web/public.wa_restart` | `POST {wa_base}/session/<tenant>/restart` with same body as `/start`. If that fails, the code also POSTs `/session/<tenant>/logout` (empty `{}` body) and `/session/<tenant>/start`, finally `/session/restart` and `/session/start` without tenant in the path. | Same snapshot payloads. Non-2xx considered failure. |
| **Status polling** | `apps/api/web/public._wa_status_impl`, `apps/api/web/admin.admin_wa_status` | `GET {wa_base}/session/<tenant>/status` (fallback `GET /session/status`) | No body. Uses WA internal token header. | JSON from `sessionStatusPayload`: `{ ok, ready, connected, qr, state, last, qr_id }`. |
| **QR fetch** | `apps/api/web/public._build_qr_candidates`, `/pub/wa/qr.*`, admin QR view | `GET {wa_base}/session/<tenant>/qr.svg`, `/session/<tenant>/qr.png`, `GET /session/qr.(svg|png)` with cache-busting query params | None. Uses WA token header. | Raw SVG or PNG bytes (`X-QR-Id` header) for embedding in UI. |
| **Health** | Docker health-checks | `GET {wa_base}/health` | — | `{ "ok": true }` (waweb already serves it). |

### Message send payload example
```json
{
  "channel": "whatsapp",
  "tenant": 12,
  "tenant_id": 12,
  "to": "79991234567@c.us",
  "text": "Здравствуйте!",
  "meta": {"lead_id": 444001},
  "attachment": {
    "type": "document",
    "url": "http://app:8000/internal/files/abc.pdf?token=...",
    "name": "catalog.pdf",
    "mime": "application/pdf",
    "caption": "Каталог"
  },
  "attachments": [
    {
      "type": "image",
      "url": "https://cdn/preview.jpg",
      "caption": "Превью"
    }
  ]
}
```
The worker version may also add a `document` block with `{ "url"|"path"|"b64", "filename", "mime", "caption", "sendMediaAsDocument": true }` when `attachments` were collapsed into a single document upload.

## Webhook contract (waweb → app)
- waweb resolves the provider webhook URL from `APP_WEBHOOK` / `APP_BASE_URL` and always appends `token=<provider_token>` query parameter. Provider tokens are per-tenant secrets stored in `libs.core.repo.provider_tokens` and validated inside `apps/api/web/webhooks.provider_webhook`.
- Requests are `POST application/json`. Authentication may also be supplied via `Authorization: Bearer <token>` or `X-Provider-Token`, but waweb currently uses the query parameter style.

### Events
1. **`messages.incoming`** (produced in `apps/waweb/index.js` around `client.on('message', …)`):
   ```json
   {
     "event": "messages.incoming",
     "tenant": 12,
     "channel": "whatsapp",
     "provider": "whatsapp",
     "message_id": "AB12CDEF",
     "from": "79991234567",
     "from_jid": "79991234567@c.us",
     "to": "79998887766@c.us",
     "text": "Хочу каталог",
     "ts": 1718196400,
     "media": [
       {"type": "image", "url": "whatsapp://12/media-...", "name": "IMG-2024.jpg", "mime": "image/jpeg", "size": 34567}
     ],
     "provider_raw": { /* whatsapp-web.js message JSON */ }
   }
   ```
   The FastAPI webhook normalizes this via `_normalize_whatsapp_incoming()` (`apps/api/web/webhooks.py:608-704`) before queueing it. Messages without text but with media are currently ignored (media-only events log `webhook_skip_media_only`).

2. **`qr`** events (`apps/waweb/index.js:629-672`): `{ "event": "qr", "tenant": <id>, "channel": "whatsapp", "provider": "whatsapp", "qr_id": "...", "svg": "<svg ...>" }`. They are cached in Redis by `_cache_whatsapp_qr()` so `/pub/wa/qr.*` can reuse the latest QR.

3. **`ready`** events (`apps/waweb/index.js:2266-2287`): `{ "event": "ready", "tenant": <id>, "state": "ready", "ts": <epoch_ms> }`. The worker enqueues them via `_queue_incoming_event` for telemetry.

HTTP failures (401/5xx) cause waweb to refresh the provider token and retry up to three times; everything else is logged via `wa_to_app` counters.

## Where WhatsApp send logic lives in Python
These are the choke points to swap out when a new provider (wabaileys) lands:

- **`app/main.py::send_transport_message` (lines ~490-640)** – FastAPI endpoint `/send` that normalizes payloads, enforces whitelists, and ultimately POSTs to `_waweb_send_url(tenant)`. `_waweb_base_url()` and `_waweb_send_url()` are the only places where the waweb host/port is assembled for synchronous sends.
- **`app/worker.py::send_whatsapp` (lines ~1551-1705)** – Outbox worker implementation that prepares attachments, picks headers (`X-Auth-Token`, `X-Internal-Token`), handles retries, and hits `{wa_base}/send?tenant=`. All queue-based WhatsApp deliveries funnel through this function (see `do_send()` later in the same file).
- **`apps/api/web/common.wa_post()` and helpers** – Utility used by public/admin views to call `/session/*` endpoints. `common.wa_base_url()` and `core.tenant_waweb_url()` encapsulate tenant-specific hostnames (`waweb-<tenant>` by default) so provider resolution can be swapped per tenant config.
- **Incoming webhook handler `apps/api/web/webhooks.provider_webhook`** – Currently assumes waweb’s event shapes (`event == "messages.incoming"/"qr"/"ready"`) and performs validation/normalization. wabaileys should preserve this payload format to avoid touching Python consumers.

Keeping these touchpoints backward compatible means we can introduce a per-tenant provider switch later (see future steps) without rewriting the higher-level bot logic.

## Running the new `wabaileys` service
- `docker-compose.yml` now ships with a dedicated `wabaileys` service (`node:20-alpine`, port `9002`) that stores Baileys auth state under `./data/wabaileys`. Bring it up alongside `app`/`worker` via `docker compose up app worker wabaileys redis postgres`.
- Health check: `curl -fsS http://localhost:9002/health` → `{ "ok": true }`.
- Session lifecycle HTTP API (served by wabaileys):
  - `POST /sessions/start` `{ "tenant": 3 }` – starts/reattaches the Baileys session and emits QR/ready events via the existing FastAPI webhook.
  - `GET /sessions/status?tenant=3` – returns `{ status, connected, qr }`.
  - `POST /messages/send` – mirrors waweb’s `/send` contract for manual smoke tests; payload accepts `{ tenant, to, type, payload }` as described above.
- Configuration highlights:
  - `STATE_DIR` (default `/data/wabaileys`) – persisted auth files per tenant.
  - `APP_BASE_URL`/`APP_WEBHOOK_URL` – where webhook + internal `/internal/tenant/{tenant}/ensure` requests are sent; by default they target `http://app:8000`.
  - `WA_INTERNAL_TOKEN`/`WEBHOOK_SECRET`/`ADMIN_TOKEN` – one of these must be present so `wabaileys` can mint/refresh provider tokens before posting events.
- Logs (JSON via `pino`) call out `session_*`, `incoming_message`, and webhook delivery results to simplify troubleshooting during the migration.

## Enabling Baileys per tenant
- Every tenant config can now specify a `whatsapp.provider` field. Example:
  ```json
  {
    "channels": {"whatsapp": {"enabled": true}},
    "whatsapp": {"provider": "baileys"}
  }
  ```
  The default remains `waweb`; override globally via `WHATSAPP_PROVIDER_DEFAULT=baileys`.
- The selector is resolved through `core.tenant_whatsapp_provider` → `apps/api/web/common.whatsapp_provider`, so Python call sites automatically route to either waweb or wabaileys without branching logic in handlers.
- Updating the provider value requires a config reload (editing `data/tenants/<id>/tenant.json` or using the client settings form that persists the same file). No API restart is needed; the new value is read on the next request.

### Test tenant for Baileys smoke tests
- Repository ships with `data/tenants/9100/tenant.json` where `whatsapp.provider = "baileys"`. This tenant is reserved for staging checks and automated tests (`pytest tests/test_public_wa_qr.py -k 9100`).
- Flow to verify end-to-end:
  1. `docker compose up app worker wabaileys redis postgres`.
  2. Hit `POST http://localhost:9002/sessions/start` with `{ "tenant": 9100 }` (or call `/pub/wa/start?tenant=9100&k=<public_key>` for the public route) to trigger a QR.
  3. Fetch QR via `/pub/wa/qr.svg?tenant=9100&k=<public_key>`; the handler now proxies Baileys snapshots when the tenant is flagged for the new provider.
  4. After scanning, send a test message using the existing `/send` API (app or worker). The payload automatically lands in `wabaileys` through the new provider abstraction, and incoming replies arrive through the existing webhook path.
- Automated coverage: `tests/test_tenant_waweb.py::test_tenant_whatsapp_provider_reads_tenant_config` ensures the selector reads tenant configs from disk, while `test_wa_status_respects_baileys_tenant_config` in `tests/test_public_wa_qr.py` exercises the public QR/status flow without monkeypatching the provider.

### Re-authentication and QR refresh with wabaileys
- `POST /sessions/start` on the wabaileys service remains the canonical way to trigger a (re)login. The FastAPI proxy `/pub/wa/start` now detects Baileys tenants, forwards the call, and reuses the same JSON envelope it returns for waweb.
- `/sessions/status?tenant=<id>` surfaces `{ status, connected, qr }`. Public `/pub/wa/status` attaches cached QR IDs + URLs so the admin UI keeps working without changes.
- `/pub/wa/qr.(svg|png)` checks the tenant provider, proxies Baileys QR snapshots when needed, and falls back to waweb caching logic otherwise. This allows operators to reuse the same wizard for both providers.

### Monitoring, diagnostics, and rollback
- **wabaileys logs**: `docker compose logs -f wabaileys` prints JSON entries with keys such as `session_state`, `incoming_message`, `webhook_event_failed`. Use these to confirm socket state and webhook delivery.
- **App metrics/logs**:
  - `WEBHOOK_PROVIDER_COUNTER{provider="whatsapp", source="baileys"}` increments for every incoming event.
  - `wa_logger` (`apps/api/web/public.py`) produces `wa_qr_*`, `wa_status_*`, and `wabaileys_http_*` lines that highlight HTTP errors when public endpoints proxy the new service.
  - Worker logs continue to stream `send_whatsapp provider=...` so you can audit which tenants already migrated.
- **Redis/QR cache**: `wa:qr:last:<tenant>` remains the source of truth for the UI even for Baileys tenants. Expect entries with Baileys-generated IDs (e.g., `qr-tenant9100`).
- **Rollback**: set `whatsapp.provider` back to `waweb` (or unset it) and restart the waweb container for that tenant if needed. The FastAPI layer immediately switches back because provider resolution happens per request.
