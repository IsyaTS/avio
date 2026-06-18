# Critical Verification Playbook

Use this playbook for critical runtime, queue, persistence, OAuth, worker, response-pipeline, learning, and production checks. Do not transfer dev evidence to prod.

## Environment Identity / Prod vs Dev

- True prod: server `195.133.15.7`, user `deploy`, working directory `/opt/avio`, compose project `avio`.
- Dev/staging: server `72.56.87.229` and/or directory `/opt/avio-dev`.
- Never treat `/opt/avio-dev` as prod, even if containers are named `avio-app-1`, `avio-worker-1`, or `avio-postgres-1`.
- Classify every task first as `dev-only`, `prod-readonly`, or `prod-deploy`.
- If the user did not ask for prod, default to `dev-only`.

Before any action the user calls prod, verify:

1. `hostname -I` or external IP includes `195.133.15.7`.
2. `pwd` is `/opt/avio`.
3. `docker inspect avio-app-1 --format '{{json .Config.Labels}}'` shows `com.docker.compose.project=avio` and `com.docker.compose.project.working_dir=/opt/avio`.
4. `docker ps` shows prod containers on that server.

If any item does not match, stop and say it is not prod. Do not make prod claims from `/opt/avio-dev`.

For prod checks and prod fixes, connect to `deploy@195.133.15.7` and work only in `/opt/avio`, unless the user explicitly gives another target.

## Tenant Settings / Avito Auth Persistence

Risk: tenant settings, `behavior.avito_smart_reply_enabled`, persona, follow-up settings, and Avito tokens can disappear after restarts, cleanup, deploys, or partial saves.

Required checks after changes in settings/client/public/tenant runtime:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_tenant_runtime_atomic.py tests/test_tenant_config_merge.py tests/test_client_settings.py tests/test_public_settings.py -q
python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3
python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode test-tenant-write --write-tenant 999999 --public-key "$PUBLIC_KEY"
python scripts/restart_persistence_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --services app --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
```

After `restart_persistence_smoke.py`, wait for:

```bash
docker compose ps app worker redis postgres
```

The relevant services should be healthy before reporting success.

## Avito OAuth

Risk: `invalid_state`, `missing_state`, wrong host/proto/cookie domain, or lost Redis state.

Required checks after OAuth changes:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_public_settings.py tests/test_avito_oauth.py -q
```

Key test: `test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss`.

Do not delete Redis state manually as a fix until the signed-state recovery path is checked.

## Avito Incoming / Worker / Outbox

Risk: webhook reaches Redis but worker does not create a reply, or tests only check that a mocked function was called.

Required checks after changes in `apps/worker/main.py`, Avito integration, queues, or outbox:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_worker_avito_send.py tests/test_worker_incoming.py -q
docker compose -f docker-compose.yml -f compose/ci/docker-compose.yml up -d app worker redis postgres
python scripts/inbox_worker_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
docker compose up -d app worker
```

`compose/ci/docker-compose.yml` sets `OUTBOX_ENABLED=0`; this proves worker payload creation in `outbox:send`, not real delivery to external APIs.

With `OUTBOX_ENABLED=0`, the outbox loop must not consume the queue. If smoke does not see a payload, inspect worker logs.

## Learning / Self-Training

Risk: learning is enabled but examples never reach responses, or stay only in shadow/evaluation paths.

Required checks after changes in `libs/core/learning/*`, `libs/core/training/*`, or `libs/core/response_pipeline.py`:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_learning_policy_v2.py tests/test_learning_feedback.py tests/test_learning_manager_capture_hooks.py -q
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
```

Key test: `test_learning_examples_from_db_reach_response_pipeline_prompt`.

Check not only capture/finalize, but also that the examples block reaches the actual system prompt.

## Message Pipeline / Answer Quality

After changes in sales core, response pipeline, prompt logic, or guard logic:

```bash
.venv/bin/pytest tests/test_sales_policy_guard.py tests/test_sales_engine.py tests/test_brain_quality.py tests/test_truth_critical_flows.py -q
python scripts/message_pipeline_smoke.py
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
```

For reply-quality work, record the actual reply behavior and distinguish `rule_fallback` from true `llm` output when metadata is available.

## Prod Checks

Never run write-smoke on prod tenants `1` or `3`.

Safe minimum after a prod deploy:

```bash
python scripts/critical_smoke.py --base-url https://avio.website --tenants 1,3 --mode readonly
```

Also check:

- `/internal/health/deep?tenants=1,3` with `X-Admin-Token`.
- UI-visible Avito connected/configured state for tenants `1` and `3`.
- Learning enabled/apply mode for tenants `1` and `3`.

Sanitize all logs before sharing: no access tokens, refresh tokens, phones, user ids, raw payloads, or raw customer messages.

## Definition of Green

### Narrow pure-code change

- Relevant unit tests pass.
- Lint target from `AGENTS.md` passes for touched surfaces.
- `python scripts/monolith_guard.py` passes if guarded surfaces were touched.

### Tenant settings / auth / OAuth change

- Required tenant settings and OAuth tests pass.
- Restart persistence smoke passes on a test tenant.
- No write-smoke is run on prod tenants `1` or `3`.

### Incoming / worker / outbox change

- Required worker tests pass.
- Live stack `inbox_worker_smoke.py` passes with `OUTBOX_ENABLED=0`.
- Worker logs show no `unknown_tenant`, unhandled exception loop, or consumed outbox when it should be disabled.

### Learning / response change

- Required learning tests pass.
- `dialog_quality_runner.py` passes.
- At least one test proves DB examples reach the system prompt.
- Bad or irrelevant examples are not applied as strong guidance.

### Test-suite or CI change

- Changed tests fail for the old broken behavior or are justified as coverage/gate improvements.
- `python scripts/test_truth_audit.py tests` reports no unreviewed critical mock-heavy tests, or each finding is documented with a follow-up.
- No critical runtime scenario is downgraded from smoke/integration to unit-only coverage.

### Production

- App and worker are healthy.
- `critical_smoke.py --mode readonly` passes for tenants `1,3`.
- `/internal/health/deep?tenants=1,3` is checked with `X-Admin-Token`.
- UI-visible Avito configured/connected and learning apply mode are manually or programmatically verified where possible.
- Final answer separates dev checks from prod checks.

### Documentation-only change

- Edited instructions are consistent with active repo layout and commands.
- No runtime green claim is made unless runtime checks were actually run.
- Final answer says runtime was not changed.
