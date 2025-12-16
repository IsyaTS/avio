# Dialogs messenger worklog

- Branch: `feature/dialogs-messenger` from `dev-prod05`.
- Dev commands (existing project flow): `uvicorn apps.api.main:app --reload --port 8000` for API, `python -m apps.worker.main` for queue handling; full stack via `docker-compose up app worker waweb redis postgres`.

## Research
- Settings UI: route `/client/{tenant}/settings` in `apps/api/web/client.py` renders `apps/api/templates/client/settings.html` with tab buttons (`data-tab-target`). Inline JS at the bottom handles tab switching and behavior save; assets pulled from `/static/js/boot.js`, `catalog-upload.js`, `client-settings.js`, `followups-inline.js`.
- Tenant auth: key is read from `k` query or `client_key` cookie; `_auth` (`apps/api/web/client.py:222`) uses `C.valid_key`. Endpoints append `?k=<key>` for client access.
- Storage: schema in `db/init/002_schema.sql` — `leads` (tenant_id, channel, peer, contact, etc), `messages` (lead_id FK, direction 0/1, text, provider_msg_id, status, tenant_id, telegram_user_id, created_at), `outbox` for queued sends. DB helpers live in `libs/core/db.py` (`insert_message_in/out`, `ensure_outbox_queued`, etc.).
- Sending flows: outbound requests are queued to Redis `OUTBOX_QUEUE_KEY` (see `apps/api/main.py` enqueue with `origin="app.send"`). Worker loop in `apps/worker/main.py::process_queue` pops and dispatches via `do_send` → channel senders (`send_telegram`, `send_avito`, WhatsApp HTTP). Telegram-only DB outbox worker in `apps/worker/outbox.py` uses `ensure_outbox_queued`.
- Frontend stack: Jinja templates + vanilla JS; no React. Styles from `apps/api/static/css/portal.css`.

## Planned API shapes
- `GET /api/dialogs`: returns an array of dialogs  
  `[{ "id": lead_id, "channel": "avito|telegram", "title": "...", "contact": "...", "last_message": "...", "last_ts": "iso8601", "unread": 0 }]`
- `GET /api/dialogs/{lead_id}?limit=50&before=...`: returns  
  `{ "dialog_id": lead_id, "messages": [{ "id": msg_id, "direction": 0|1, "text": "...", "ts": "iso8601", "status": "...", "from_bot": true|false }] }`
- `POST /api/dialogs/{lead_id}/send` with `{ "text": "..." }` → `{ "ok": true, "message": {...} }` (or `{ "ok": true, "queued": true }`). Tenant ownership of lead must be enforced.
- `POST /api/feedback` with `{ "message_id": 123, "rating": "like" }` or `{ "message_id": 123, "rating": "dislike", "comment": "..." }`; dislike without comment => 400.

## Dialogs/feedback implementation
- Schema: added `messages.is_bot` (bool, default false) and new table `message_feedback` (tenant_id, message_id FK, rating like/dislike, comment, handled, created_at); migration `db/migrations/20241210_add_feedback_and_bot_flag.sql`, init schema updated.
- DAO helpers: `fetch_dialogs_for_tenant`, `list_messages_for_lead`, `get_lead_dialog_metadata`, `get_message_metadata`, `create_message_feedback` in `libs/core/db.py`.
- API (requires tenant + k): `GET /api/dialogs`, `GET /api/dialogs/{lead_id}`, `POST /api/dialogs/{lead_id}/send`, `POST /api/feedback`. Send endpoint writes queued message (is_bot=false), enqueues to `OUTBOX_QUEUE_KEY` with `origin="dialogs.ui"` and `_message_db_id` for status updates. Feedback allowed только для исходящих ботов (`is_bot=true`, direction=1); dislike требует comment.
- Frontend: tab «Диалоги» в `client/settings.html` — левый список диалогов, правая лента сообщений, кнопка «Обновить», ручная отправка ответов, лайк/дизлайк с комментарием на бот-ответах. Uses URLs from `state.urls.*`.

## Manual check (UI/API)
- Open `/client/{tenant}/settings#dialogs?k=<key>`, убедиться что список диалогов загружается без падений при пустых каналах.
- Выбрать диалог: история прогружается, скролл в конец, канал отображён бейджем.
- Отправить ответ из UI для Avito и Telegram: сообщение уходит в очередь, статус в UI не падает.
- Кнопка «Обновить» обновляет список и выбранный диалог.
- Лайк на бот-ответе уходит в `/api/feedback`; дизлайк без комментария не отправляется, с комментарием — записывает feedback.
- Ошибки отправки отображаются внизу формы, сообщение не исчезает.

## Tests
- Не запускались (предыдущие попытки завалились на тяжёлых зависимостях в sandbox); требуется ручная проверка по чек-листу выше.
