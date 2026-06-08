# Worktree Release Audit - 2026-05-13

Environment:
- workspace: `/opt/avio-dev`
- dev/staging IP observed: `72.56.87.229`
- compose project observed: `avio-dev`
- task class: `dev-only`
- prod was not touched

## Current State

The worktree is not a clean release candidate yet. It contains a large set of pre-existing dirty changes plus the latest stabilization/doc/gate changes.

Observed totals:
- tracked modified/added-like files: 100
- tracked deleted files: 7
- untracked files: 316
- tracked diff shortstat: `107 files changed, 12515 insertions(+), 31572 deletions(-)`

Current dev verification passed after the latest gate work:
- `.venv/bin/pytest -q` -> `677 passed`
- `ruff` target -> passed
- `flake8` target -> passed
- `python scripts/monolith_guard.py` -> `oversized_functions: none`
- `python scripts/test_truth_audit.py tests` -> `needs_truth_review: none`
- `python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1` -> `violations=0`
- `python scripts/message_pipeline_smoke.py` -> ok
- `critical_smoke.py` readonly tenants `1,3` -> ok
- `critical_smoke.py --mode test-tenant-write --write-tenant 999999` -> ok
- `ui_http_smoke.py --tenant 999999` -> ok
- `restart_persistence_smoke.py --tenant 999999` -> ok
- `inbox_worker_smoke.py --tenant 999999` -> ok
- `runtime_log_guard.py --outbox-disabled` -> ok
- `app`, `worker`, `redis`, `postgres` -> healthy after smoke
- `python scripts/release_scope_guard.py` -> report generated
- `python scripts/release_scope_guard.py --strict` -> expected fail while deploy blockers remain

## Release Slice A: Latest Stabilization Gates

These files are the latest explicit stabilization slice and are suitable to keep together:

- `.github/workflows/ci.yml`
- `AGENTS.md`
- `README.md`
- `ARCHITECTURE.md`
- `RUNBOOK.md`
- `scripts/critical_smoke.py`
- `scripts/runtime_log_guard.py`
- `scripts/ui_http_smoke.py`
- `tests/test_critical_smoke.py`
- `tests/test_runtime_log_guard.py`
- `tests/test_ui_http_smoke.py`

Purpose:
- make startup smoke resilient to app recreate/startup timing;
- add post-smoke runtime log guard;
- add lightweight HTTP UI smoke;
- wire those gates into CI;
- document architecture and incident runbooks.

Important caveat:
- `scripts/critical_smoke.py` is currently untracked in git, even though it is already referenced by CI/docs. If committing this slice, include the whole script and its test.

## Release Slice B: Existing Stabilization Architecture

These are large existing dirty areas that likely belong to previous stabilization/monolith-reduction work and should be reviewed as their own release slice, not mixed blindly with Slice A:

- `apps/api/web/services/*`
- `apps/worker/services/*`
- `libs/core/services/*` new service modules
- `libs/core/repo/tenant_configs.py`
- `libs/core/learning/*`
- many `libs/core/sales_core/*_runtime.py` modules
- new smoke scripts:
  - `scripts/critical_smoke.py`
  - `scripts/inbox_worker_smoke.py`
  - `scripts/monolith_guard.py`
  - `scripts/restart_persistence_smoke.py`
  - `scripts/smoke_lock.py`
  - `scripts/test_truth_audit.py`
  - `scripts/dialog_quality_runner.py`
- new critical tests under `tests/test_*runtime*.py`, `tests/test_*oauth*.py`, `tests/test_tenant_*`, `tests/test_truth_critical_flows.py`, `tests/test_learning_*`

This slice is probably required for the current full green result. Do not remove these files just because they are untracked.

Recommended handling:
- commit/review as the main stabilization refactor slice;
- keep CI smoke evidence attached;
- do not deploy if any of these files are accidentally omitted.

## Release Slice C: Product/UI/Transport Additions

These need product-level review before release:

- `apps/maxworker/*`
- `libs/core/transport/max_personal.py`
- `libs/core/services/max_personal_service.py`
- `docs/max_personal_transport.md`
- `apps/frontend/client-portal/*`
- `apps/api/static/landing/lovable/*`
- `apps/api/templates/marketing/home_lovable.html`
- `apps/api/static/spa/client/*`
- root `package.json` / `package-lock.json`
- `apps/api/static/js/*`
- auth/client templates under `apps/api/templates/*`

Recommended handling:
- review as separate MAX/frontend/landing slices;
- avoid mixing with backend stabilization gates unless the dependency is explicit.

## Release Slice D: Infrastructure and Data-Risk Changes

These are deploy-sensitive and require manual review before prod:

- `.dockerignore`
- `.gitignore`
- `.github/workflows/smoke.yml`
- `docker-compose.yml`
- `compose/ci/docker-compose.yml`
- `infra/caddy/Caddyfile`
- `ops/deploy.sh`
- `db/init/002_schema.sql`
- `db/init/003_contacts.sql`
- `db/migrations/20260413_learning_v2.sql`
- `db/migrations/20260421_contacts_tenant_scope.sql`
- `config/tenants/*`
- `libs/config/tenants/*`

Specific warnings:
- tracked deletions are intentionally not restored for this candidate and are recorded in `docs/release/2026-05-13/accepted-tracked-deletions.txt`.
- local/generated candidates are ignored in `.gitignore`/`.dockerignore`, not deleted.

Do not deploy if new unaccepted deletions appear.

## Local / Backup / Generated Candidates

These should not be part of a release unless explicitly needed:

- `.codex`
- `.env_recovery/*`
- `infra/caddy/Caddyfile.backup.*`
- `infra/caddy/Caddyfile.bak.*`
- `screens/*`
- `kabinet/*`
- `avio-connect-flow/`

Do not delete them without explicit approval. For release hygiene, either ignore/move them outside the repo or document why they are included.

## Current Deploy Blockers

1. The worktree has unrelated slices mixed together.
2. Several tracked files are deleted and need explicit acceptance/restoration.
3. Many files required by current green are untracked, so a partial commit/deploy can easily break CI/runtime.
4. There are local/generated/backup directories inside the worktree.
5. Prod was not checked and must not be inferred from dev green.
6. `python scripts/release_scope_guard.py --strict` fails until blockers are resolved.

## Recommended Next Steps

1. Create a clean release branch or patch set from the current worktree.
2. Review and commit Slice A separately: `docs/release/2026-05-13/slice-a-stabilization-gates.pathspec`.
3. Review and commit Slice B separately: `docs/release/2026-05-13/slice-b-backend-stabilization-candidates.pathspec`.
4. Commit Slice C only after product/UI review: `docs/release/2026-05-13/slice-c-product-ui-max-candidates.pathspec`.
5. Review Slice D manually before any prod deploy: `docs/release/2026-05-13/slice-d-infra-data-risk-candidates.pathspec`.
6. Exclude or move local/backup/generated candidates after explicit approval.
7. Run full dev green gate again after the final release candidate is assembled.
8. Only then request explicit prod deploy/read-only verification approval.
