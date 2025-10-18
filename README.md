# Avio

## PUBLIC_KEY для фронта
- Публичные маршруты Telegram (`/pub/tg/*`) и WhatsApp (`/pub/wa/*`) принимают ключ только через параметр `?k=` и сравнивают его со значением `PUBLIC_KEY` из окружения.
- Значение `PUBLIC_KEY` обязательно и должно отличаться от `ADMIN_TOKEN`, чтобы не давать фронту доступ к административным операциям.
- При отсутствии `PUBLIC_KEY` система временно принимает `ADMIN_TOKEN` как запасной вариант, но это режим совместимости и рекомендуется задать отдельный ключ для фронта как можно раньше.

### Примеры `curl`

```bash
# Запуск логина по QR и получение ссылки на PNG
curl -G "https://api.avio.website/pub/tg/start" \
  --data-urlencode "tenant=1" \
  --data-urlencode "k=${PUBLIC_KEY}"

# Проверка статуса авторизации
curl -G "https://api.avio.website/pub/tg/status" \
  --data-urlencode "tenant=1" \
  --data-urlencode "k=${PUBLIC_KEY}"

# Получение PNG с QR-кодом (используйте qr_id из /start или /status)
curl -G "https://api.avio.website/pub/tg/qr.png" \
  --data-urlencode "tenant=1" \
  --data-urlencode "k=${PUBLIC_KEY}" \
  --data-urlencode "qr_id=<QR_ID>" \
  --output tg-qr.png

# Передача 2FA пароля
curl -X POST "https://api.avio.website/pub/tg/2fa?k=${PUBLIC_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"tenant": 1, "password": "<2FA>"}'
```

### Переменные окружения tgworker

| Переменная | Назначение |
|------------|------------|
| `TELEGRAM_API_ID` | идентификатор приложения Telegram | 
| `TELEGRAM_API_HASH` | hash приложения Telegram |
| `PUBLIC_KEY` | публичный ключ для доступа к `/pub/tg/*` и `/pub/wa/*` |
| `ADMIN_TOKEN` | админ-токен для приватных RPC эндпоинтов |
| `APP_BASE_URL` | внешний URL API (используется для обратных вызовов, по умолчанию `http://app:8000`) |
| `TGWORKER_BASE_URL` | внутренний URL Telegram worker (по умолчанию `http://tgworker:9000`) |
| `OUTBOX_ENABLED` | включает обработку очереди исходящих сообщений (по умолчанию `false`, чтобы ничего не отправлять без явного разрешения) |
| `OUTBOX_WHITELIST` | список разрешённых получателей (через пробелы или запятые), любая отправка вне списка будет пропущена (по умолчанию пустой список блокирует все исходящие) |
| `TG_SESSIONS_DIR` | каталог для хранения `.session` файлов (общий с `app`) |

Том сессий Telegram должен быть примонтирован к контейнерам `app` и `tgworker`, чтобы авторизация сохранялась между перезапусками.

### Outbox worker guards

- `ADMIN_TOKEN` обязателен для RPC-запросов к `tgworker:/send` — `app.worker` всегда отправляет заголовок `X-Admin-Token`.
- Если `OUTBOX_ENABLED=false`, воркер только логирует задачу (`status=skipped reason=outbox_disabled`).
- `OUTBOX_WHITELIST` фильтрует получателей по ID, username и телефону; пустое значение означает, что все отправки будут пропущены.
- Перед отправкой воркер проверяет наличие лида в БД и, при отсутствии, помечает результат как `err:no_lead` без попытки доставки.

### Database migrations

- Выполните `make migrate`, чтобы через контейнер `ops` применить Alembic-миграции и вывести структуру таблиц `leads`, `messages` и список колонок `contacts`. Перед запуском установите переменную окружения `DATABASE_URL`.

## Telegram Login Flow

### Стадии

- `need_qr` — QR сгенерирован и ждёт сканирования.
- `need_2fa` — аккаунт требует пароль второй факторной авторизации.
- `authorized` — сессия активирована, сообщения начинают поступать в `/webhook`.
- `failed` — QR истёк или поток авторизации завершился с ошибкой, требуется повторный запуск.

### Эндпоинты

| Маршрут | Описание | Успешные ответы | Коды ошибок |
|---------|----------|-----------------|-------------|
| `GET /pub/tg/start` | Запускает получение QR. Возвращает `qr_id`, `expires_at`, `state` и `qr_url`. | `200` | `409 already_authorized`, `502` при `qr_expired` |
| `GET /pub/tg/qr.png` | Отдаёт PNG текущего QR. Требует параметр `qr_id`. | `200` | `404 qr_not_found`, `410 qr_expired`, `502 tg_unavailable` |
| `GET /pub/tg/status` | Текущий статус и счётчики (`state`, `authorized`, `needs_2fa`, `qr_id`, `qr_url`). | `200` | `502 tg_unavailable` |
| `POST /pub/tg/2fa` | Передаёт 2FA пароль, когда `state=need_2fa`. | `200` | `401 bad_password`, `409 not_waiting_2fa`, `502 tg_unavailable` |

Все маршруты требуют обязательные параметры `tenant` и `k=<PUBLIC_KEY>`.

### Последовательность действий

1. Вызвать `GET /pub/tg/start?k=${PUBLIC_KEY}&tenant=<TENANT_ID>` и сохранить `qr_id`, `expires_at`, `state` и `qr_url` из ответа.
2. Отображать QR через `GET /pub/tg/qr.png?k=${PUBLIC_KEY}&tenant=<TENANT_ID>&qr_id=<QR_ID>`.
3. Параллельно опрашивать `GET /pub/tg/status` до смены `state` на `need_2fa` или `authorized`.
4. Если статус переходит в `need_2fa`, вызвать `POST /pub/tg/2fa` с JSON `{ "tenant": <TENANT_ID>, "password": "<2FA>" }`.
5. При статусе `failed` повторно вызвать `/pub/tg/start` для выпуска нового QR.

## Единый контракт

Единый транспортный контракт использует две структуры:

- **TransportMessage** — исходящее сообщение, которое отправляется в `POST /send` на приложении.
- **MessageIn** — входящее событие, которое провайдеры (Telegram/WhatsApp) публикуют в `POST /webhook`.

### Пример TransportMessage

```json
{
  "tenant": 1,
  "channel": "telegram",
  "to": "me",
  "text": "Привет!",
  "attachments": [
    {
      "type": "file",
      "url": "https://example.org/file.pdf",
      "name": "file.pdf",
      "mime": "application/pdf"
    }
  ],
  "meta": {
    "reply_to": "12345"
  }
}
```

`channel` выбирает воркер: `telegram` → `tgworker:/send`, `whatsapp` → `waweb:/send`. Алиас `to="me"` отправляет сообщение в сохранённые сообщения аккаунта. Ответы воркеров приводятся к формату `{"ok": true}` либо `{ "ok": false, "error": "..." }`.

## WhatsApp отправка

- Поддерживаемые значения `to`: `+E164`, строка из цифр (10–15 символов) или JID вида `1234567890@c.us`. Для российских номеров `8XXXXXXXXXX` автоматически приводится к `7XXXXXXXXXX`.
- Перед отправкой убедитесь, что с адресатом уже есть чат в WhatsApp — иначе доставка не состоится.
- Переменные окружения:
  - `OUTBOX_ENABLED` — включает REST-эндпойнт `/send` для исходящих сообщений. При значении `false` приложение отвечает `403 outbox_disabled`.
  - `OUTBOX_WHITELIST` — список разрешённых получателей (числа, `+E164`, JID). Любой другой номер вернёт `403 not_whitelisted`.
  - `WAWEB_ADMIN_TOKEN` — должен совпадать с `ADMIN_TOKEN` и используется для внутреннего API `waweb`.

### `curl`-примеры для `/send`

```bash
curl -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant": 1, "channel": "whatsapp", "to": "+79991234567", "text": "E164 demo"}'

curl -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant": 1, "channel": "whatsapp", "to": "79991234567", "text": "Digits demo"}'

curl -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant": 1, "channel": "whatsapp", "to": "79991234567@c.us", "text": "JID demo"}'
```

### Пример MessageIn

```json
{
  "event": "messages.incoming",
  "tenant": 1,
  "provider": "whatsapp",
  "channel": "whatsapp",
  "message_id": "wamid.123",
  "from": "+79991234567",
  "from_jid": "79991234567@c.us",
  "text": "Добрый день",
  "ts": 1715683200,
  "media": [
    {
      "type": "image",
      "mime_type": "image/jpeg",
      "url": "https://example.org/media/1"
    }
  ]
}
```

Каждое валидное входящее событие складывается в Redis по ключу `inbox:message_in` (LPUSH), что позволяет независимо подтверждать доставку. `provider_token` передаётся либо как query-параметр `token`, либо в заголовке `X-Provider-Token`. Все поля типа datetime сериализуются в формате ISO 8601 (UTC) либо в миллисекундах эпохи, чтобы не зависеть от часового пояса контейнеров.

## Inbound WhatsApp

### Provider token

- Для аутентификации событий `waweb → app` используется `provider_token`, закреплённый за каждым tenant.
- Генерация: `POST /internal/tenant/{tenant}/ensure` с заголовком `X-Auth-Token: ${WA_WEB_TOKEN}` (или `?token=`). Ответ:

  ```json
  {
    "ok": true,
    "tenant": 7,
    "provider_token": "a1b2c3d4..."
  }
  ```

- Токен сохраняется в таблицу `provider_tokens` (`tenant INT PRIMARY KEY`, `token TEXT UNIQUE NOT NULL`, `created_at TIMESTAMPTZ DEFAULT now()`) и переиспользуется при повторных вызовах.
- Админ-роут `/admin/keys/list?tenant=<id>` (с `X-Admin-Token`) возвращает текущий `provider_token` для выбранного tenant.

### Контракт `/webhook`

- Аутентификация: `provider_token` передаётся в `?token=<secret>` (стандартный путь для `waweb`) либо в заголовке `X-Provider-Token`. Токен должен совпадать с записью из таблицы `provider_tokens` для указанного tenant.
- Обязательное поле `tenant` в теле запроса.
- Поддерживаемые события:
  - `messages.incoming` — входящее сообщение WhatsApp. Требует `channel="whatsapp"`, поле `from` и хотя бы одно из `text` или `media`/`attachments`.
  - `qr` — свежий QR-код авторизации (`qr_id`, `svg`).
  - `ready` — сессия авторизована; можно передать `state` и `ts`.
- Успешный ответ — JSON `{"ok": true, "queued": true}` для сообщений, которые ставятся в очередь (`messages.incoming`, `ready`), и `{"ok": true, "queued": false, "event": "qr"}` для QR-событий. Ошибки аутентификации отвечают `401`, нарушения схемы — `422`.
- Все события `messages.incoming` дополнительно сохраняются в таблице `webhook_events` и попадают в Redis (`inbox:message_in`), откуда их обрабатывает воркер. Метрики `webhook_provider_total{status,channel}` и `wa_to_app_total{event,status}` отображают статусы доставки.

Пример валидного входящего сообщения:

```json
{
  "event": "messages.incoming",
  "tenant": 7,
  "channel": "whatsapp",
  "message_id": "ABCD123",
  "from": "79991234567",
  "text": "Привет!",
  "media": [
    { "type": "image", "url": "whatsapp://7/ABCD", "name": "photo.jpg" }
  ],
  "ts": 1716748800
}
```

QR события отправляются тем же маршрутом `POST /webhook?token=<provider_token>` с телом вида:

```json
{
  "provider": "whatsapp",
  "event": "qr",
  "tenant": 1,
  "qr_id": "1715940000000",
  "svg": "<?xml version=...>"
}
```

Если SVG отсутствует, обработчик вернёт `422 invalid_qr`. Валидные SVG кэшируются в Redis по ключам `wa:qr:{tenant}:{qr_id}:svg` и `wa:qr:last:{tenant}` (TTL ≥ 180 секунд), чтобы публичные маршруты `/pub/wa/status` и `/pub/wa/qr.svg` могли отдавать актуальный код без повторной генерации.

## Диагностика

- `make diag` — запускает основной скрипт `scripts/diag.sh` с минимальным выводом (передайте `AVIO_URL` и `ADMIN_TOKEN`).
- `make diag-verbose` — тот же скрипт, но с расширенным логированием (`DIAG_VERBOSE=1`).
- Проверка сервисов: `GET http://127.0.0.1:8000/health` (app) и `GET http://waweb:9001/health` (waweb).
- Тестирование канала: `POST /send` (app) и `POST /send` на `waweb` с `X-Auth-Token`.
- Публичные WA-эндпойнты: `GET /pub/wa/status?k=<PUBLIC_KEY>&tenant=<TENANT>` и `POST /pub/wa/start`.
- Скрипт `deploy/diag/wa.sh` автоматизирует health-check, проверку переменных `OUTBOX_*`, тестовые отправки (digits/JID) и сбор логов `app`/`waweb` за последние две минуты.
- Получение/создание provider_token: `curl -H "X-Admin-Token: ${ADMIN_TOKEN}" http://app:8000/admin/provider-token/7`.
- Проверка webhook-аутентификации: `curl -X POST "http://app:8000/webhook?token=${PROVIDER_TOKEN}" -H 'Content-Type: application/json' -d '{"event":"ready","tenant":7,"channel":"whatsapp"}'`.
- Поток обработки: `waweb → POST /webhook (?token=provider_token)` → HTTP `200` → запись в `webhook_events` → задача в Redis → `worker` логирует `send_success` → `waweb` метрика `wa_to_app_total{result=success}`.

### Ключи доступа

- `PUBLIC_KEY` используется только на публичных маршрутах (`/pub/tg/*`) и сверяется строго через параметр `?k=`.
- `ADMIN_TOKEN` остаётся приватным и не должен совпадать с `PUBLIC_KEY`.
- Если `PUBLIC_KEY` не задан, фронт временно может использовать `ADMIN_TOKEN`, но это режим совместимости — рекомендуем задать отдельный публичный ключ как можно раньше.

Исторические маршруты `/pub/tg/*` сохранены для обратной совместимости, но считаются **deprecated** — в логах выводится предупреждение не чаще одного раза в час.
