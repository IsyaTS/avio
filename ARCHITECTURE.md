# Avio Architecture

Avio is a multi-tenant messaging and sales automation system. The active dev workspace is `/opt/avio-dev`; production is `/opt/avio` on `195.133.15.7`.

## Runtime Boundaries

- `apps/api` owns HTTP, public website, auth, client cabinet, webhooks, internal/public API, and startup wiring.
- `apps/worker` owns Redis queue consumers, smart reply, follow-ups, handoff, outbox scheduling, and integration loops.
- `apps/tgworker` owns Telegram session transport, 2FA/QR flows, media upload/download, and send API.
- `apps/waweb` and related WhatsApp transports own WhatsApp session bridges.
- `libs/core` owns shared domain logic: tenant config, repositories, response pipeline, integrations, learning, catalog, message envelope, and policy.

Route handlers and worker loops should stay thin: validate, authorize, route, call a service, map response. Business rules belong in `libs/core/*` or focused `apps/*/services/*` modules.

## Environment Separation

- Dev/staging: `72.56.87.229:/opt/avio-dev`, compose project `avio-dev`.
- Prod: `195.133.15.7:/opt/avio`, compose project `avio`.
- Old or backup directories are not active deploy targets unless explicitly verified.

Prod conclusions require prod-readonly evidence. Dev smoke is not prod evidence.

## Tenant Config Source of Truth

Tenant config reads must be deterministic:

```text
defaults -> DB-backed config -> file-backed fallback/tenant data -> runtime normalization
```

Config writes must be atomic and partial saves must not wipe unrelated fields. Critical protected areas:
- `integrations.avito` tokens and account metadata;
- `behavior.avito_smart_reply_enabled`;
- persona/brand settings;
- `follow_up`;
- learning settings.

Core modules:
- `libs/core/repo/tenant_configs.py`;
- `libs/core/services/tenant_config_merge.py`;
- tenant runtime helpers under `libs/core/sales_core/*`.

Verification:
- `tests/test_tenant_runtime_atomic.py`;
- `tests/test_tenant_config_merge.py`;
- `tests/test_client_settings.py`;
- `tests/test_public_settings.py`;
- `scripts/critical_smoke.py`;
- `scripts/restart_persistence_smoke.py`.

## Avito OAuth Flow

```text
client UI -> /v1/oauth/avito/authorize
  -> Redis state + signed state fallback
  -> Avito callback
  -> token exchange
  -> account metadata fetch
  -> tenant config persistence
```

The signed state fallback exists so OAuth does not fail solely because Redis state expired or was lost. Callback must persist tokens without overwriting unrelated tenant settings.

Verification:
- `tests/test_avito_oauth.py`;
- `tests/test_avito_oauth_state.py`;
- `tests/test_avito_oauth_tokens.py`;
- `tests/test_truth_critical_flows.py::test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss`.

## Avito Incoming to Worker to UI

```text
Avito webhook/API
  -> event normalization
  -> tenant resolution by account_id / user_id fallback
  -> Redis inbox queue
  -> worker incoming dispatcher
  -> message persistence
  -> response pipeline or static auto-reply
  -> outbox payload
  -> UI dialogs / external send loop
```

Unknown tenant events must not silently fall back to tenant `1`. They should be logged with provider/channel/stage and no raw customer payload.

Core modules:
- `libs/core/services/avito_webhook_events.py`;
- `libs/core/services/avito_incoming.py`;
- `libs/core/services/incoming_events.py`;
- `libs/core/services/queue_contract.py`;
- `libs/core/services/outbox_payloads.py`;
- focused runtime services under `apps/worker/services/*`.

Verification:
- `tests/test_avito_incoming.py`;
- `tests/test_avito_webhook_events.py`;
- `tests/test_worker_incoming.py`;
- `tests/test_worker_avito_send.py`;
- `tests/test_truth_critical_flows.py`;
- `scripts/inbox_worker_smoke.py`.

## Response Pipeline

The response path should be unified through `libs/core/response_pipeline.py` and `libs/core/sales_core/*`. Test dialog endpoints and worker smart replies should use the same production decision path where possible.

Verification:
- `tests/test_sales_policy_guard.py`;
- `tests/test_sales_engine.py`;
- `tests/test_brain_quality.py`;
- `scripts/message_pipeline_smoke.py`;
- `scripts/dialog_quality_runner.py`.

## Learning Pipeline

```text
dialog/message feedback
  -> capture hook
  -> example storage/scoring
  -> retrieval by tenant/context/similarity
  -> prompt examples block
  -> response pipeline answer
```

Learning must not blindly apply all examples. Negative feedback and low-similarity examples must not become strong positive guidance.

Core modules:
- `libs/core/learning/*`;
- `libs/core/training/*`;
- `libs/core/response_pipeline.py`.

Verification:
- `tests/test_learning_policy_v2.py`;
- `tests/test_learning_feedback.py`;
- `tests/test_learning_manager_capture_hooks.py`;
- `tests/test_truth_critical_flows.py::test_learning_examples_from_db_reach_response_pipeline_prompt`;
- `scripts/dialog_quality_runner.py`.

## Observability

Runtime logs should include useful context:
- tenant id;
- channel/provider;
- lead/chat/message id when safe;
- stage/event name;
- reason/fallback result.

Runtime logs must not include:
- access tokens;
- refresh tokens;
- admin/auth tokens;
- phone numbers;
- raw customer payloads.

Post-smoke log verification:

```bash
python scripts/runtime_log_guard.py \
  --compose-file docker-compose.yml \
  --compose-file compose/ci/docker-compose.yml \
  --service app \
  --service worker \
  --tail 1200 \
  --outbox-disabled
```

## Green Gates

The minimum senior-grade gate depends on changed surface:
- pure logic: relevant unit tests + lint;
- settings/OAuth: unit tests + critical smoke + restart persistence smoke;
- incoming/worker/outbox: worker tests + live stack inbox smoke + runtime log guard;
- learning/response: learning tests + dialog quality + prompt truth test;
- docs-only: consistency check and no runtime claim.

Never claim prod is green without prod-readonly checks on `195.133.15.7:/opt/avio`.
