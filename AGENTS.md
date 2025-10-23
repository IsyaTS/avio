# Repository Guidelines

## Project Structure & Module Organization
- `app/` contains the FastAPI entrypoint (`main.py`), Redis worker (`worker.py`), and routers under `web/`; shared prompts and catalog data live in `app/data/`.
- `db/init/` seeds PostgreSQL, while `migrations/` holds incremental SQL files with ascending numeric prefixes.
- `waweb/` provides the WhatsApp bridge (Node.js + puppeteer), `ops/` serves the operations dashboard, and `monitoring/` stores Prometheus/Grafana configs used by `docker-compose.yml`.

## Build, Test, and Development Commands
- Start the full stack for parity: `docker-compose up app worker waweb redis postgres`.
- API-only iteration: run `uvicorn main:app --reload --port 8000` from `app/`; pair with `python worker.py` if queue behavior matters.
- WhatsApp bridge: from `waweb/`, run `npm install` once, then `node index.js` (set `STATE_DIR` when emulating multiple tenants).
- Ops panel: `pip install -r ops/requirements.txt` then `uvicorn main:app --reload --port 8001` inside `ops/app/`.

## Coding Style & Naming Conventions
- Target Python 3.11 with 4-space indents, snake_case functions, PascalCase classes, and consistent type hints aligned with `core.py`.
- Order imports stdlib → third-party → local; prefer explicit JSON responses.
- Name Redis keys in lowercase with `:` separators (example: `session:tenant:status`).
- JavaScript in `waweb/` uses CommonJS, camelCase helpers, and 2-space indentation.

## Testing Guidelines
- Use `pytest` under `app/tests/` with files named `test_*.py`; employ `pytest-asyncio` for async handlers and mock Redis/Postgres as needed.
- For integration smoke tests involving queues or migrations, run the relevant `docker-compose` services.
- Add lightweight mocks for `whatsapp-web.js` in `waweb/` and execute via an `npm test` script.

## Commit & Pull Request Guidelines
- Write imperative commit subjects (e.g., `Add tenant QR reset`) and note env changes or migrations in the body.
- Pull requests should explain purpose, deployment steps, linked issues, and include UI or observability screenshots/logs when applicable.
- Before requesting review, run the relevant services or tests and call out any skipped checks explicitly.

## Security & Configuration Tips
- `.env` stores secrets—never commit raw credentials; share sanitized samples only and rotate if exposed.
- Scrub WhatsApp IDs, lead IDs, and queue payloads before sharing logs.
- Reset WhatsApp sessions via `POST /session/:tenant/restart` instead of deleting container state.
