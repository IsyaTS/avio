# Stabilization Roadmap

These rules define Codex behavior when improving stability, refactoring AI-generated code, auditing tests, or bringing the project toward a measurable green state.

## Mission

Avio is a production product with real tenants and real integrations. The task is not broad cosmetic refactoring. The task is to make the system measurably more stable without breaking existing behavior.

Primary goals:

- Tenant settings and Avito authorization must not disappear after restarts, cleanup, deploys, or partial failures.
- Avito OAuth must not fail with `invalid_state` or `missing_state` when signed-state fallback can recover the flow.
- Incoming messages must travel through the real route: webhook/API -> Redis/DB -> worker -> response pipeline -> messages/outbox/UI.
- Self-learning must affect responses only when relevant examples reach the prompt and must not silently degrade answer quality.
- Tests must prove real product behavior, not only mocked helper calls.
- Prod and dev must never be confused.

## Non-Negotiable Safety Rules

- Never treat `/opt/avio-dev` as prod. Prod is only `195.133.15.7:/opt/avio` with compose project `avio`.
- Do not edit prod unless the user explicitly asks to deploy or fix prod. Prod diagnosis is read-only by default.
- Do not run write-smoke against prod tenants `1` or `3`.
- Do not delete Redis keys, tenant files, Avito tokens, DB rows, Docker volumes, or upload data as a fix unless the user explicitly approves the exact destructive action.
- Do not use `git reset --hard`, `git checkout --`, or destructive cleanup commands to hide dirty state.
- Do not add new business logic to `apps/worker/main.py`, `apps/api/web/public.py`, `apps/api/web/client.py`, or `apps/api/web/webhooks.py` except for a minimal hotfix. Move new behavior into focused modules under `libs/core/services/*`, `libs/core/repo/*`, `libs/core/policies/*`, or `apps/*/services/*`.
- Do not declare green after only unit tests if the changed behavior is a runtime integration, queue, persistence, OAuth, or worker path.
- Do not claim prod works based on dev tests. Prod needs prod-readonly checks.
- Sanitize logs before reporting: no access/refresh tokens, phones, user ids, raw payloads, or raw customer messages.

## Required Working Style

Work without intermediate summaries when the user asks to do it end to end, but still give short progress updates during long work. Continue until one of these is true:

- Implementation is complete and all relevant checks are green.
- A real blocker exists and is documented with exact failing command/output summary.
- The user redirects the task.

Always start by establishing a baseline:

1. `pwd`, `hostname -I`, `git status --short`.
2. Identify whether the task is `dev-only`, `prod-readonly`, or `prod-deploy`.
3. Map the changed behavior to the required verification playbook.
4. Inspect existing modules/tests before adding anything new.

For large changes:

- Prefer vertical behavior-preserving slices.
- Batch compatible slices only when write scopes are clear and tests can isolate regressions.
- Never rewrite a whole monolith in one pass.
- Extract stable boundaries first, keep old entry points as thin adapters, then remove dead code after tests pass.
- If a refactor changes public behavior, call it a feature/fix, not a pure refactor, and run the corresponding runtime smoke.

## Senior-Grade Standard

The target is not more files or less AI-looking code. The target is a codebase a senior engineer can trust because:

- Critical product paths have explicit contracts and tests.
- Runtime behavior is proven by smoke/truth checks, not only mocks.
- Domain logic has clear ownership and is not hidden in route handlers or loops.
- Deploy and rollback steps are known.
- Logs are useful for diagnosis without leaking secrets.
- Prod evidence is separated from dev evidence.
- Remaining risks are documented instead of implied away.

Do not claim the project is senior-grade, stable, or fully fixed unless the relevant Definition of Green in `critical-verification-playbook.md` is satisfied and remaining risks are explicitly listed.

## Stabilization Roadmap

### Phase 1: Source-of-truth and environment hygiene

- Make the active workspace explicit in docs and final reports.
- Keep dev, prod, baseline, and backup directories clearly separated:
  - dev/staging: `72.56.87.229:/opt/avio-dev`;
  - prod: `195.133.15.7:/opt/avio`.
- Old/baseline copies are not active deploy targets unless the user says so.
- Do not make conclusions from `/opt/avio` on the dev server unless you first prove it is the active compose working directory.
- Before large changes, record dirty files and distinguish user changes from your own changes.

### Phase 2: Test truthfulness audit

- Classify each critical test as unit, integration, smoke, or prod_readonly.
- Flag tests that mock the system under test instead of external boundaries.
- For Avito incoming, ensure at least one test/smoke proves webhook -> queue -> worker -> message/outbox.
- For tenant settings, ensure restart persistence is verified against the real config merge path and DB/file fallback.
- For learning, ensure examples from DB enter the actual response pipeline prompt.
- For OAuth, ensure callback persists tokens even after Redis state loss through signed-state recovery.

### Phase 3: Critical persistence hardening

- Tenant config writes must be atomic.
- Tenant config reads must merge defaults, DB-backed config, and file-backed config deterministically.
- Avito tokens/settings must not be overwritten by partial UI saves.
- Follow-up settings, persona, smart reply toggles, and learning settings must survive app/worker restarts.
- Add focused regression tests before or with the fix.

### Phase 4: Runtime pipeline hardening

Split worker/webhook logic only along real behavioral boundaries:

- Avito incoming normalization and tenant resolution.
- Queue enqueue/dequeue contracts.
- Response pipeline invocation.
- Outbox send and status update.
- AmoCRM chat sync.
- Learning capture/finalize/retrieve.

Replace silent broad `except Exception` in critical paths with structured logging and explicit fallback results. Add tenant/channel/lead/stage fields to logs where useful, but do not log sensitive payloads.

### Phase 5: Answer quality and learning

- Keep learning retrieval separate from policy application.
- Do not mark all examples as good by default if there is negative feedback for the source message.
- Add or preserve tests that prove:
  - bad examples are excluded;
  - low-similarity examples do not drive answers;
  - selected examples are visible in the prompt;
  - answer quality runner catches irrelevant CTA, forbidden unsupported claims, and repeated useless questions.

### Phase 6: CI and deployment gates

- Keep CI fast but meaningful:
  - lint for touched Python surfaces;
  - full pytest when feasible;
  - dialog quality CI cases;
  - compose smoke for app/worker/redis/postgres.
- Keep architecture gates executable:
  - `python scripts/monolith_guard.py`;
  - `python scripts/test_truth_audit.py tests`;
  - `python scripts/release_scope_guard.py --strict`;
  - `python scripts/runtime_log_guard.py`;
  - `python scripts/ui_http_smoke.py`.
- Add or extend scripts when a repeated manual rule can be checked reliably.
- Deploy only after local/dev green.
- After deploy, run prod-readonly checks and report exact commands/results.
- If prod and dev differ, say so directly and do not infer.
