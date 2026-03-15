# Repository Guidelines

## Project Structure & Module Organization
- `apps/api/` — FastAPI приложение: точка входа `main.py`, роуты в `web/`, шаблоны и статика.
- `apps/worker/` — фоновые обработчики и очереди (`main.py`, `http.py`, `outbox.py`).
- `apps/tgworker/` — отдельный Telegram transport (FastAPI + Telethon).
- `apps/waweb/` — WhatsApp bridge (Node.js + puppeteer).
- `libs/core/` — общая доменная логика, интеграции, репозитории, сервисы.
- `db/init/` — SQL-инициализация PostgreSQL.
- `tests/` — `pytest`-тесты.
- `data/tenants/<id>/` — конфиги тенантов, персоны, каталоги.

## Build, Test, and Development Commands
- `docker-compose up app worker waweb redis postgres` — поднять основной стек локально.
- `uvicorn apps.api.main:app --reload --port 8000` — быстрый запуск только API.
- `python -m apps.worker.main` — запуск воркера при API-only разработке.
- `cd apps/waweb && npm install && node index.js` — запуск WhatsApp bridge.
- `pytest tests -q` — запуск Python-тестов.

## Coding Style & Naming Conventions
- Python: 3.11, 4 пробела, `snake_case` для функций, `PascalCase` для классов, явные type hints.
- Импорты: stdlib → third-party → local.
- Redis-ключи: lowercase + `:` (пример: `handoff:silence:101:<lead_id>`).
- JavaScript (`apps/waweb/`): CommonJS, camelCase, 2 пробела.

## Testing Guidelines
- Фреймворк: `pytest` (+ `pytest-asyncio` для async-кейсов).
- Имена файлов: `test_*.py`.
- Новые изменения в интеграциях покрывайте unit/smoke-тестами с моками внешних API.
- Для queue/db сценариев используйте поднятые `redis` и `postgres` из `docker-compose`.

## Commit & Pull Request Guidelines
- Коммиты: императивный стиль, коротко и предметно (например: `Fix amoCRM chat link reconciliation`).
- PR должен содержать:
  - цель изменений и затронутые модули;
  - шаги деплоя/миграции (если есть);
  - результаты проверок (`pytest`, smoke, логи);
  - скриншоты/логи для UI и интеграционных фиксов.

## Security & Configuration Tips
- Секреты храните только в `.env`; не коммитьте реальные ключи/токены.
- Перед публикацией логов удаляйте телефоны, user id, access tokens, payloads.
- Сбросы интеграций выполняйте через API/сервисные процедуры, а не удалением файлов состояния вручную.
