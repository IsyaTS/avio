# Avio Runbook

This runbook is for production-like incidents. Prod is only `195.133.15.7:/opt/avio` with compose project `avio`; dev/staging is `/opt/avio-dev` on `72.56.87.229`.

Before any prod action:

```bash
pwd
hostname -I
docker inspect avio-app-1 --format '{{json .Config.Labels}}'
```

Do not run destructive cleanup, delete Redis keys, delete tenant configs, reset Avito tokens, or run write-smoke against prod tenants `1` and `3` unless the user explicitly approves that exact action.

## invalid_state / missing_state

Goal: determine whether Avito OAuth is failing because Redis state is missing, signed state fallback is broken, redirect host/proto is wrong, or tokens are not persisted.

Safe checks:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_public_settings.py tests/test_avito_oauth.py -q
python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode readonly
```

What to inspect:
- Avito app callback domain must match the public host used by `/v1/oauth/avito/authorize`.
- Callback must accept signed state even if Redis state is gone.
- Callback must persist `access_token`, `refresh_token`, `account_id`, and account metadata without wiping other tenant settings.
- Logs should not contain repeated `invalid_state` or `missing_state` after a fresh authorize URL.

Do not fix this by deleting all Redis state. That can temporarily change symptoms while hiding the real broken path.

## unknown_tenant

Goal: prove whether incoming events carry enough identity to resolve the tenant.

Safe checks:

```bash
.venv/bin/pytest tests/test_avito_incoming.py tests/test_avito_webhook_events.py tests/test_worker_incoming.py tests/test_truth_critical_flows.py -q
python scripts/runtime_log_guard.py --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml --service app --service worker --tail 1200
```

What to inspect:
- Avito events should resolve tenant by `account_id`; v3 payloads can fallback to `payload.value.user_id`.
- Telegram webhooks require explicit `tenant` or `tenant_id`.
- Worker must not default unknown events to tenant `1`.
- Unknown events should be logged with provider/channel/stage, without raw customer payloads.

## Worker Not Processing

Goal: distinguish queue starvation, worker crash, disabled outbox, and response pipeline failure.

Safe dev smoke:

```bash
docker compose -f docker-compose.yml -f compose/ci/docker-compose.yml up -d app worker redis postgres
python scripts/inbox_worker_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
python scripts/runtime_log_guard.py --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml --service app --service worker --tail 1200 --outbox-disabled
```

Expected result with `compose/ci/docker-compose.yml`:
- worker consumes `inbox:message_in`;
- worker creates `outbox:send` payload;
- outbox loop does not send or consume payload because `OUTBOX_ENABLED=0`.

If smoke fails:
- inspect `docker compose logs worker`;
- inspect Redis queue lengths for `inbox:message_in`, `outbox:send`, `outbox:dlq`;
- check for `unknown_tenant`, traceback, unhandled exception, token errors.

## Settings Disappeared

Goal: prove whether config save/load/restart path is wiping fields.

Safe checks:

```bash
.venv/bin/pytest tests/test_tenant_runtime_atomic.py tests/test_tenant_config_merge.py tests/test_client_settings.py tests/test_public_settings.py tests/test_tenant_configs_repo.py -q
python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode readonly
python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode test-tenant-write --write-tenant 999999 --public-key "$PUBLIC_KEY"
python scripts/restart_persistence_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --services app --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
```

Fields that must survive partial saves and restarts:
- `behavior.avito_smart_reply_enabled`;
- persona/brand fields;
- `follow_up`;
- learning settings;
- `integrations.avito.access_token`;
- `integrations.avito.refresh_token`;
- `integrations.avito.account_id`.

Do not restore settings by copying old backups over active tenant state unless you first identify which source of truth is wrong.

## Outbox Stuck

Goal: distinguish disabled sending, payload creation failure, external API failure, and DLQ.

Checks:
- Confirm `OUTBOX_ENABLED` and `AMOCRM_OUTBOX_ENABLED`.
- Inspect `outbox:send` and `outbox:dlq`.
- For Avito, confirm account tokens exist and refresh can work.
- For Telegram/WhatsApp/MAX, confirm transport service is healthy and auth/session exists.

When `OUTBOX_ENABLED=0`, queued payloads are expected and should not be consumed. Use `runtime_log_guard.py --outbox-disabled` after smoke to catch accidental consumption.

## Learning Not Applied

Goal: prove whether examples are captured, scored, retrieved, and injected into the response prompt.

Safe checks:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_learning_policy_v2.py tests/test_learning_feedback.py tests/test_learning_manager_capture_hooks.py -q
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
```

What to inspect:
- examples exist in DB/source;
- negative feedback examples are excluded from positive guidance;
- similarity threshold is high enough to avoid irrelevant examples;
- selected examples appear in the system prompt;
- answer quality runner does not show repeated bad CTA, unsupported claims, or irrelevant questions.

## Safe Reporting

When reporting incident evidence, include:
- environment identity;
- commands run and pass/fail result;
- sanitized log event names, not raw customer payloads;
- what was not checked;
- remaining risk.
