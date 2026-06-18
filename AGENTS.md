# Repository Guidelines

This file is the project law for Codex work in `/opt/avio-dev`. Keep it short,
enforceable, and linked to detailed runbooks instead of duplicating every
playbook inline.

Detailed Codex runbooks:
- Critical verification: `docs/codex/critical-verification-playbook.md`
- Stabilization roadmap: `docs/codex/stabilization-roadmap.md`
- Release hygiene: `docs/codex/release-hygiene.md`
- Testing truth rules: `docs/codex/testing-truth-rules.md`

## Default Codex Behavior

- Default scope is `dev-only` unless the user explicitly asks for prod.
- Never work directly on `main` unless explicitly instructed.
- Before code changes, use `/plan` and explain intended files.
- For broad investigation, use subagents only in read-only mode.
- After code changes, run relevant checks and use `/review`.
- Always explain `git diff` in simple language.
- Do not commit, push, merge, deploy, delete data, reset hard, or clean files without explicit user approval.
- If the worktree is dirty before starting, preserve existing user changes and report them.

## Project Structure

- `apps/api/` - FastAPI app: entrypoint `main.py`, routes in `web/`, templates and static assets.
- `apps/worker/` - background processors and queues (`main.py`, `http.py`, `outbox.py`).
- `apps/tgworker/` - separate Telegram transport (FastAPI + Telethon).
- `apps/waweb/` - WhatsApp bridge (Node.js + puppeteer).
- `apps/maxworker/` - Playwright-based personal WhatsApp worker (Node.js).
- `apps/frontend/client-portal/` - client SPA (Vite + React + TS + Tailwind).
- `apps/api/static/spa/client/` - built SPA static files.
- `libs/core/` - shared domain logic, integrations, repositories, services, policies.
- `db/init/` - PostgreSQL initialization SQL.
- `tests/` - `pytest` tests.
- `data/tenants/<id>/` - tenant configs, personas, catalogs.

## Build, Test, and Development Commands

- `docker-compose up app worker waweb redis postgres` - start the main local stack.
- `docker-compose.override.yml` mounts source files into containers for dev hot reload.
- Worker (`avio-worker-1`) runs in dual mode: `uvicorn apps.worker.http:app` for HTTP health/RPC and `python -m apps.worker.main` for the Redis loop.
- `uvicorn apps.api.main:app --reload --port 8000` - run only the API.
- `python -m apps.worker.main` - run the worker during API-only development.
- `cd apps/waweb && npm install && node index.js` - run the WhatsApp bridge.
- `pytest tests -q` - run Python tests.
- `.venv/bin/pytest -q` - preferred full local pytest in the prepared environment.
- `cd apps/frontend/client-portal && npm ci && npm run build` - build the client SPA into `apps/api/static/spa/client/`.
- `.venv/bin/ruff check --select E,F --ignore E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py` - CI-like lint.
- `.venv/bin/flake8 --select=E,F --extend-ignore=E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py` - CI-like flake8.
- `python scripts/monolith_guard.py` - guard against re-growing large runtime surfaces.
- `python scripts/test_truth_audit.py tests` - audit mock-heavy critical checks.
- `python scripts/release_scope_guard.py` - read-only release-scope report; use `--strict` before prod.
- `python scripts/runtime_log_guard.py --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml --service app --service worker --tail 1200 --outbox-disabled` - log gate after runtime smoke.
- `python scripts/ui_http_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY"` - HTTP smoke for key UI pages.

## Coding Style

- Python: 3.11, 4 spaces, `snake_case` functions, `PascalCase` classes, explicit type hints.
- Imports: stdlib -> third-party -> local.
- Redis keys: lowercase + `:` such as `handoff:silence:101:<lead_id>`.
- JavaScript in `apps/waweb/`: CommonJS, camelCase, 2 spaces.
- Keep comments sparse and useful; avoid comments that restate obvious code.

## Anti-Monolith Policy

- Do not add new business logic to `apps/worker/main.py`, `apps/api/web/public.py`, `apps/api/web/client.py`, or `apps/api/web/webhooks.py` except for a minimal hotfix. Move new behavior into focused modules under `libs/core/services/*`, `libs/core/repo/*`, `libs/core/policies/*`, or `apps/*/services/*`.
- Routes and loop handlers must stay thin: validation, auth, routing, service call, response mapping.
- Shared logic needed by multiple apps (`api`, `worker`, `tgworker`) belongs in `libs/core/*`.
- If a function grows past roughly 80 lines or pulls several external dependencies, extract a named service/repo/policy/adapter.
- Add a focused unit/smoke test for every non-trivial new function or scenario.
- After touching guarded surfaces, run `python scripts/monolith_guard.py`.

## Architecture Boundary Rules

- New behavior must have a clear boundary: service, repo, policy, normalizer, adapter, or transport.
- `apps/*` owns transport/runtime: HTTP, loops, dependency wiring, serialization.
- `libs/core/*` owns domain decisions, merges, persistence contracts, integrations, policies.
- Do not duplicate tenant resolution, config merge, queue payload shape, learning retrieval, or response invocation across apps.
- Broad `except Exception` in critical runtime paths is allowed only with structured logs, tenant/channel/context fields, and a safe fallback result.
- New project rules should have an executable guard where practical: test, smoke, lint script, or CI step.

## Testing Guidelines

- Use `pytest` and `pytest-asyncio` for async cases.
- Test files are named `test_*.py`.
- Mark tests with `unit`, `integration`, `e2e`, or `prod_readonly` from `pytest.ini`.
- Mock external boundaries such as Avito API, OpenAI, Telegram, and WA transport.
- Do not mock internal routing, config merge, Redis queue, or response pipeline in integration/truth tests unless necessary.
- Critical tests must assert observable product results: persisted config, queue item, DB row, outbox payload, prompt content, API response, or UI-visible state.
- Do not treat `pytest` alone as enough for critical runtime, queue, persistence, OAuth, worker, or response-pipeline changes; use the relevant playbook in `docs/codex/critical-verification-playbook.md`.
- Detailed testing honesty rules live in `docs/codex/testing-truth-rules.md`.

## Evidence & Reporting Rules

- Do not write "works", "green", "stable", or "verified" without exact command, environment, pass/fail result, and what it proves.
- Separate dev evidence from prod evidence.
- If prod was not checked on `195.133.15.7:/opt/avio`, explicitly say `prod не проверялся`.
- Report skipped checks and why.
- For audit/review tasks, list findings first by severity with file/line references, then assumptions, then short summary.
- For docs-only tasks, say runtime was not changed and do not claim runtime green.

## Git Workflow Rules

- Before changes, run and report `git status --short`.
- Prefer one task per branch.
- Do not work directly on `main` unless explicitly asked.
- Do not commit, push, merge, rebase, tag, or deploy without explicit user approval.
- After changes, explain changed files and diff in simple language.
- If the worktree was dirty before the task, do not modify or revert pre-existing user changes.
- Do not use destructive cleanup commands to hide dirty state.
- Detailed release and generated-artifact hygiene lives in `docs/codex/release-hygiene.md`.

## Codex CLI Workflow

Default feature/fix flow:

1. Use `/plan` before changing code.
2. For broad investigation, use subagents only in read-only mode.
3. Implement the smallest approved plan.
4. Run relevant tests/checks.
5. Use `/review` before commit.
6. Explain diff, risks, and rollback.
7. Do not commit/push/deploy without explicit user instruction.

Use `/goal` only for long-running tasks with clear success criteria, validation commands, constraints, and stop conditions.
Do not use `/goal` for vague tasks like "improve the bot", "make it stable", or "refactor everything".

## Scope Levels

### Docs-only
Do not run heavy runtime smoke unless docs affect commands, release instructions, or verification rules.
Final report must say runtime was not changed.

### Small code change
Run relevant unit tests and lint for touched surfaces.

### Critical runtime change
Use the relevant Critical Verification Playbook from `docs/codex/critical-verification-playbook.md`.

### Prod-readonly
Allowed only if user asks for prod observation. Must verify prod identity first.

### Prod-deploy
Allowed only with explicit user request, environment confirmation, release checklist, rollback plan, and prod-readonly checks.

## Security Rules

- Keep secrets only in `.env`; do not commit real keys or tokens.
- Do not print `.env`, OAuth callback `code/state`, Avito tokens, refresh tokens, amoCRM tokens, phone numbers, or raw customer messages.
- Sanitize logs before sharing: no access/refresh tokens, phones, user ids, or raw payloads.
- Do not reset integrations by manually deleting state files unless the user explicitly approves the exact destructive action.
- Do not run write-smoke against prod tenants `1` or `3`.
- Do not `source .env` blindly; extract only needed values such as `PUBLIC_KEY` or `ADMIN_TOKEN` with a safe parser.

## Prod and Dev Boundary

- Default workspace is dev/staging: `72.56.87.229:/opt/avio-dev`.
- True prod is only `deploy@195.133.15.7:/opt/avio` with compose project `avio`.
- Never treat `/opt/avio-dev` as prod.
- Never infer prod health from dev checks.
- Full prod identity and prod-check rules live in `docs/codex/critical-verification-playbook.md`.
