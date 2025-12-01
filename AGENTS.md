# Repository Guidelines

## Project Structure & Module Organization
- `apps/api/` contains the FastAPI entrypoint (`main.py`), routers under `web/`, and static/templates.
- `apps/worker/` hosts the Redis worker entrypoints (`main.py`, `http.py`, `outbox.py`).
- `apps/tgworker/` is the standalone Telegram transport service (FastAPI + Telethon).
- `db/init/` seeds PostgreSQL, while `migrations/` holds incremental SQL files with ascending numeric prefixes.
- `apps/waweb/` provides the WhatsApp bridge (Node.js + puppeteer).

## Build, Test, and Development Commands
- Start the full stack for parity: `docker-compose up app worker waweb redis postgres`.
- API-only iteration: run `uvicorn apps.api.main:app --reload --port 8000`; pair with `python -m apps.worker.main` if queue behavior matters.
- WhatsApp bridge: from `apps/waweb/`, run `npm install` once, then `node index.js` (set `STATE_DIR` when emulating multiple tenants).

## Coding Style & Naming Conventions
- Target Python 3.11 with 4-space indents, snake_case functions, PascalCase classes, and consistent type hints aligned with `libs/core/sales_core.py`.
- Order imports stdlib → third-party → local; prefer explicit JSON responses.
- Name Redis keys in lowercase with `:` separators (example: `session:tenant:status`).
- JavaScript in `apps/waweb/` uses CommonJS, camelCase helpers, and 2-space indentation.

## Testing Guidelines
- Use `pytest` under `tests/` with files named `test_*.py`; employ `pytest-asyncio` for async handlers and mock Redis/Postgres as needed.
- For integration smoke tests involving queues or migrations, run the relevant `docker-compose` services.
- Add lightweight mocks for `whatsapp-web.js` in `apps/waweb/` and execute via an `npm test` script.

## Commit & Pull Request Guidelines
- Write imperative commit subjects (e.g., `Add tenant QR reset`) and note env changes or migrations in the body.
- Pull requests should explain purpose, deployment steps, linked issues, and include UI or observability screenshots/logs when applicable.
- Before requesting review, run the relevant services or tests and call out any skipped checks explicitly.

## Security & Configuration Tips
- `.env` stores secrets—never commit raw credentials; share sanitized samples only and rotate if exposed.
- Scrub WhatsApp IDs, lead IDs, and queue payloads before sharing logs.
- Reset WhatsApp sessions via `POST /session/:tenant/restart` instead of deleting container state.
