# Release Candidate Checklist - 2026-05-13

This checklist is for dev-only release hygiene. It does not authorize prod deploy.

## 1. Environment

```bash
pwd
hostname -I
docker inspect avio-app-1 --format '{{json .Config.Labels}}' || true
git status --short
```

Expected:
- workspace is `/opt/avio-dev`;
- IP includes `72.56.87.229`;
- compose project is `avio-dev`;
- prod is not touched.

## 2. Slice A: Stabilization Gates

Pathspec:

```bash
docs/release/2026-05-13/slice-a-stabilization-gates.pathspec
```

Review diff:

```bash
git diff -- $(cat docs/release/2026-05-13/slice-a-stabilization-gates.pathspec)
```

Optional staging command, only after review:

```bash
git add --pathspec-from-file=docs/release/2026-05-13/slice-a-stabilization-gates.pathspec
git diff --cached --stat
```

Do not stage the whole repo.

## 3. Blockers Before Prod

Review:

```bash
cat docs/release/2026-05-13/deploy-blockers.txt
git status --short
python scripts/release_scope_guard.py --strict
```

Every tracked deletion must be explicitly accepted or restored before prod. Do not delete local/generated candidates without approval.
For this candidate, accepted tracked deletions are recorded in:

```bash
docs/release/2026-05-13/accepted-tracked-deletions.txt
```

Local/generated candidates are ignored in `.gitignore`/`.dockerignore`, not deleted.

## 4. Candidate Slice Manifests

The current dirty tree is split into candidate manifests:

- `slice-a-stabilization-gates.pathspec` - latest gates/docs/smoke slice.
- `slice-b-backend-stabilization-candidates.pathspec` - backend stabilization/refactor/tests/smoke suite candidates.
- `slice-c-product-ui-max-candidates.pathspec` - frontend, landing, MAX transport and product UI candidates.
- `slice-d-infra-data-risk-candidates.pathspec` - infra, compose, db, deploy and tenant config candidates.

Review each slice separately:

```bash
git status --short -- $(cat docs/release/2026-05-13/slice-b-backend-stabilization-candidates.pathspec)
git status --short -- $(cat docs/release/2026-05-13/slice-c-product-ui-max-candidates.pathspec)
git status --short -- $(cat docs/release/2026-05-13/slice-d-infra-data-risk-candidates.pathspec)
```

## 5. Dev Green Gate

```bash
.venv/bin/pytest -q
.venv/bin/ruff check --select E,F --ignore E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py scripts/dialog_regression.py scripts/test_truth_audit.py libs/core/repo/tenant_configs.py
.venv/bin/flake8 --select=E,F --extend-ignore=E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py scripts/dialog_regression.py scripts/test_truth_audit.py libs/core/repo/tenant_configs.py
python scripts/monolith_guard.py
python scripts/release_scope_guard.py --strict
python scripts/test_truth_audit.py tests
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
python scripts/message_pipeline_smoke.py
```

## 6. Runtime Smoke

```bash
PUBLIC_KEY=$(awk -F= '$1=="PUBLIC_KEY"{print $2; exit}' .env 2>/dev/null | tr -d '"\r')
ADMIN_TOKEN=$(awk -F= '$1=="ADMIN_TOKEN"{print $2; exit}' .env 2>/dev/null | tr -d '"\r')

docker compose -f docker-compose.yml -f compose/ci/docker-compose.yml up -d app worker redis postgres
ADMIN_TOKEN="$ADMIN_TOKEN" python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --health-timeout 90
ADMIN_TOKEN="$ADMIN_TOKEN" PUBLIC_KEY="$PUBLIC_KEY" python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode test-tenant-write --write-tenant 999999 --public-key "$PUBLIC_KEY" --health-timeout 90
PUBLIC_KEY="$PUBLIC_KEY" python scripts/ui_http_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY"
PUBLIC_KEY="$PUBLIC_KEY" python scripts/restart_persistence_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --services app --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
PUBLIC_KEY="$PUBLIC_KEY" python scripts/inbox_worker_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
python scripts/runtime_log_guard.py --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml --service app --service worker --tail 1200 --outbox-disabled
docker compose up -d app worker
docker compose ps app worker redis postgres
python scripts/runtime_log_guard.py --service app --service worker --tail 1200
```

## 7. Prod

Prod is not part of this checklist. Prod requires separate explicit approval and prod-readonly evidence on `195.133.15.7:/opt/avio`.

Before prod can be considered, the release scope guard must pass in strict mode:

```bash
python scripts/release_scope_guard.py --strict
```
