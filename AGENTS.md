# Repository Guidelines

## Project Structure & Module Organization
- `app/` holds the FastAPI entrypoint (`main.py`), Redis worker (`worker.py`), and HTTP routers in `web/`; shared prompts and catalog data sit in `app/data/`.
- `db/init/` seeds Postgres, while incremental SQL lives in `migrations/`; add new files with an increasing numeric prefix.
- `waweb/` provides the WhatsApp bridge (Node.js + puppeteer), `ops/` serves the operations dashboard, and `monitoring/` contains Prometheus/Grafana configs used by `docker-compose.yml`.

## Build, Test, and Development Commands
- `docker-compose up app worker waweb redis postgres` brings up the core stack for local parity.
- API-only work: run `uvicorn main:app --reload --port 8000` inside `app/` and, if queue behavior matters, pair it with `python worker.py`.
- Inside `waweb/`, run `npm install` once and `node index.js` (export `STATE_DIR` when emulating multiple tenants).
- For the ops panel, `pip install -r ops/requirements.txt` then `uvicorn main:app --reload --port 8001` from `ops/app/`.

## Coding Style & Naming Conventions
- Target Python 3.11, use 4-space indents, snake_case for functions, PascalCase for classes, and keep type hints consistent with `core.py`.
- Order imports stdlib → third-party → local, prefer explicit JSON responses, and name Redis keys in lowercase with `:` separators.
- JavaScript in `waweb/` sticks to CommonJS and camelCase helpers with 2-space indentation; avoid introducing ESM without discussion.

## Testing Guidelines
- Adopt `pytest` under `app/tests/` (`test_*.py`); exercise async handlers with `pytest-asyncio` and mock Redis/Postgres where feasible.
- Use `docker-compose` for integration smoke tests before shipping changes that touch queues or migrations.
- For `waweb/`, add lightweight mocks around `whatsapp-web.js` and run them via an `npm test` script.

## Commit & Pull Request Guidelines
- Write imperative commit subjects (`Add tenant QR reset`) and include env or migration callouts in the body.
- Pull requests should outline purpose, deployment steps, linked issues, and screenshots/logs for UI or observability changes.
- Run the relevant services or tests before requesting review and note any skipped checks explicitly.

## Security & Configuration Tips
- `.env` carries secrets; share sanitized samples only and rotate credentials if committed accidentally.
- Scrub WhatsApp IDs, lead IDs, and queue payloads before sharing logs.
- Reset waweb sessions via API (`POST /session/:tenant/restart`) instead of deleting container state.
test 
