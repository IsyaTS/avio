# Avio

Multichannel sales assistant platform for Avito / Telegram / amoCRM with a web UI, background workers, and transport bridges.

## Stack
- Python 3.11 (FastAPI, async workers)
- PostgreSQL + Redis
- Node.js bridges (`waweb`, `wabaileys`)
- Docker Compose for local/dev runtime

## Repository layout
- `apps/api/` — HTTP API + web UI serving
- `apps/worker/` — outbox/inbox processors and automation logic
- `apps/tgworker/` — Telegram transport service
- `apps/waweb/` — WhatsApp Web bridge
- `apps/wabaileys/` — WhatsApp Baileys bridge
- `libs/core/` — shared domain/services/repositories/integrations
- `db/init/`, `db/migrations/` — DB bootstrap and migrations
- `tests/` — pytest suite
- `data/tenants/<id>/` — tenant runtime state/config

## Local run (Docker)
1. Prepare env:
```bash
cp .env.example .env
```
2. Start core services:
```bash
docker compose up -d app worker tgworker redis postgres
```
3. Optional bridges:
```bash
docker compose up -d waweb wabaileys
```
4. Health checks:
```bash
curl -fsS http://localhost:8000/health
```

## Local run (without full compose)
API:
```bash
uvicorn apps.api.main:app --reload --port 8000
```
Worker:
```bash
python -m apps.worker.main
```

## Tests and quality
Run all tests:
```bash
pytest -q
```
Run targeted tests:
```bash
pytest -q tests/test_main_webhook.py tests/test_worker_incoming.py
```
Lint and format check:
```bash
ruff check .
ruff format --check .
```

## Environment variables
Use `.env.example` as the only template.

Critical variables:
- `DATABASE_URL`, `REDIS_URL`
- `ADMIN_TOKEN`, `PUBLIC_KEY`, `WEBHOOK_SECRET`
- `OPENAI_API_KEY` (if AI features enabled)
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` (Telegram transport)

## Docker notes
- Main image is built from root `Dockerfile`.
- Compose definitions:
  - `docker-compose.yml` — main stack
  - `docker-compose.override.yml` — local source mounts
  - `compose/ci/docker-compose.yml` — CI smoke profile

## Operational notes
- Never commit real secrets or session files.
- Runtime volumes (`data/`, `tg-sessions/`) are local state, not source.
- Keep tenant-specific runtime artifacts under `data/tenants/`, not in source directories.

## CI
GitHub Actions workflow (`.github/workflows/ci.yml`) runs:
- lint (`ruff check`)
- format check (`ruff format --check`)
- tests (`pytest`)
- image build and compose smoke checks
