# Avio

## PUBLIC_KEY для фронта
- Публичные маршруты Telegram (`/pub/tg/*`) и WhatsApp (`/pub/wa/*`) принимают ключ только через параметр `?k=` и сравнивают его со значением `PUBLIC_KEY` из окружения.
- Значение `PUBLIC_KEY` обязательно и должно отличаться от `ADMIN_TOKEN`, чтобы не давать фронту доступ к административным операциям.
- При отсутствии `PUBLIC_KEY` система временно принимает `ADMIN_TOKEN` как запасной вариант, но это режим совместимости и рекомендуется задать отдельный ключ для фронта как можно раньше.

## Ключи арендатора
- На каждого арендатора приходится ровно один ключ доступа (`1 tenant = 1 key`).
- `GET /admin/key/get?tenant=<ID>` возвращает существующий ключ либо создаёт новый и сразу помечает его основным.
- В админке больше нет кнопки «сделать основным»: текущий ключ единственный, чтобы получить новый, сначала удалите действующий.
- Повторные попытки создания или сохранения ключа возвращают `409 key_already_exists`.
- Ссылки для клиентов формируются в виде `/connect/wa?tenant={ID}&k={TENANT_KEY}` — значение `k` передаётся без кавычек.

## Default persona fallback
- Если у арендатора отсутствует собственный файл `persona.md`, система автоматически подставляет дефолтную заготовку из `app/agents/presets.py`.
- Заготовка содержит плейсхолдеры `{AGENT_NAME}`, `{BRAND}`, `{CITY}`, `{CHANNEL}`, `{CATALOG_URL}`, `{CURRENCY}` — при рендеринге они заменяются данными паспорта бренда и ссылками канала.
- Валюта всегда подставляется как `RUB`; очистка текстового поля персоны в админке приводит к сохранению дефолтной заготовки, поэтому полностью пустой промпт не сохраняется.

## Паспорт бренда
- В админке редактируются только три поля: «Бренд», «Имя ассистента» и «Город» — этого достаточно для персонализации промптов.
- Валюта вычисляется автоматически (значение `RUB`), отдельное поле больше не требуется.
- Остальные настройки (тональность, CTA) перешли в дефолтные значения внутри приложения; кастомизация происходит через сам текст персоны.

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

`channel` выбирает воркер: `telegram` → `tgworker:/send`, `whatsapp` → `waweb:/send`. Алиас `to="me"` отправляет сообщение в сохранённые сообщения аккаунта. Ответы воркеров приводятся к формату `{ "ok": true }` либо `{ "ok": false, "error": "..." }`.

## Outbox: отправка

- Единственная точка отправки — `POST /send` на сервисе `app`.
- Авторизация строго через заголовок `X-Admin-Token: ${ADMIN_TOKEN}`.
- Тело должно содержать `tenant`, `channel`, `to` и хотя бы один из `text`/`attachments`.
- Поле `attachments[]` принимает объекты `{ type, url, name, mime }`; воркер скачивает файлы по `url` с таймаутом 15 секунд.
- `meta.reply_to` проксируется воркерам: Telegram поддерживает числовые и строковые ID, WhatsApp игнорирует параметр.

### Guard-правила

- `OUTBOX_ENABLED=false` — немедленный ответ `403 outbox_disabled` без постановки задачи.
- `OUTBOX_WHITELIST` фильтрует получателей **до** попытки доставки. Форматы:
  - Telegram: числовой ID (`peer_id`, `telegram_user_id`) или `username` без `@`.
  - WhatsApp: `+E164`, строка цифр 10–15 символов, либо JID `1234567890@c.us`.
- Воркер не создаёт лид «по пути»: если запись не найдена, результат фиксируется как `err:no_lead`.

### Telegram отправка

- Допустимые значения `to`:
  1. `peer_id` (int),
  2. `telegram_user_id` (int),
  3. `username` (str, без `@`),
  4. строка `"me"` — в «Избранное».
- Разрешение цели выполняется по порядку: `peer_id` → `telegram_user_id` → `username` → `"me"`.
- `meta.reply_to` поддерживается и может быть числом либо строкой (для внутренних ID провайдера).
- Если `text` ещё не отправлялся, подпись берётся из `attachments[].caption`.
- Требования среды: авторизованная сессия Telegram и общий том `TG_SESSIONS_DIR`, смонтированный в `app` и `tgworker`.

#### Примеры `curl`

```bash
curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"telegram","to":1564614169,"text":"ping"}'

curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"telegram","to":"someuser","text":"pong","meta":{"reply_to":"12345"}}'

curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"telegram","to":"me","text":"note"}'
```

### WhatsApp отправка

- Поддерживаемые значения `to`: `+E164`, строка из цифр (10–15 символов) или JID `1234567890@c.us`. Формат `8XXXXXXXXXX` автоматически нормализуется в `7XXXXXXXXXX`.
- Перед отправкой убедитесь, что с адресатом уже существует чат — иначе доставка не гарантируется.
- `meta.reply_to` игнорируется.
- Вложения скачиваются по `url` аналогично Telegram.
- Переменные окружения:
  - `OUTBOX_ENABLED` — включает REST-эндпойнт `/send`. При `false` возвращается `403 outbox_disabled`.
  - `OUTBOX_WHITELIST` — список разрешённых получателей (числа, `+E164`, JID). Иные значения приводят к `403 not_whitelisted`.
  - `WAWEB_ADMIN_TOKEN` — должен совпадать с `ADMIN_TOKEN` и используется для внутреннего API `waweb`.

#### `curl`-примеры для `/send`

```bash
curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"whatsapp","to":"+79991234567","text":"hello"}'

curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"whatsapp","to":"79991234567","text":"hi"}'

curl -sS -X POST "http://127.0.0.1:8000/send" \
  -H "Content-Type: application/json" -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"tenant":1,"channel":"whatsapp","to":"79991234567@c.us","text":"jid"}'
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

Общие правила входящих событий:

- Хотя бы одно из `text` или `attachments`/`media` обязательно.
- `ts` передаётся в UTC: секунды эпохи или ISO-8601.
- Валидные события пишутся в Redis `inbox:message_in` (LPUSH), что позволяет независимо подтверждать доставку.
- `provider_token` передаётся в query-параметре `token` либо заголовке `X-Provider-Token`.
- При наличии вложений обработчик не должен логировать `skip_no_text`.

## Inbound Telegram

- Минимальный контракт: `tenant`, `channel="telegram"`, `message_id`, `ts`.
- Идентификаторы отправителя: `peer`/`peer_id`, `telegram_user_id` и/или `username`.
- Если `peer` отсутствует, но есть `telegram_user_id`, парсер подставляет `peer` на основании `telegram_user_id`.
- `meta.reply_to` поддерживается: числовой ID или строковый идентификатор провайдера.
- Логи приложения: `webhook_received ch=telegram ... peer=...`, затем `incoming_enqueued` и `lead_upsert_ok`.

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
- Идентификаторы отправителя: `from` (цифры) и/или `from_jid` (`*@c.us`). Номер нормализуется до формата E164 без `+` и сохраняется в `contacts.whatsapp_phone`, а `lead_contacts.peer` получает цифры без `@c.us`.
- Успешный ответ — JSON `{"ok": true, "queued": true}` для сообщений, которые ставятся в очередь (`messages.incoming`, `ready`), и `{"ok": true, "queued": false, "event": "qr"}` для QR-событий. Ошибки аутентификации отвечают `401`, нарушения схемы — `422`.
- Все события `messages.incoming` дополнительно сохраняются в таблице `webhook_events` и попадают в Redis (`inbox:message_in`), откуда их обрабатывает воркер. Метрики `webhook_provider_total{status,channel}` и `wa_to_app_total{event,status}` отображают статусы доставки.
- Логи приложения: `webhook_received ch=whatsapp`, затем `incoming_enqueued` и `lead_upsert_ok`.

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

## Guardrails и самопроверки

- `skip_no_text` ≤ 5% за 15 минут для обоих каналов.
- Ошибки вебхука (`HTTP != 2xx`) < 1% за 5 минут.
- Telegram: доля событий без `peer` и без `telegram_user_id` ≈ 0%.
- WhatsApp: доля событий без валидного `from`/`from_jid` ≤ 1%.

## Проверки здоровья

- `GET http://app:8000/health` → `200`.
- `GET http://tgworker:8000/health` → `200` (авторизация активна).
- `GET http://waweb:9001/health` → `200`.

## Синтетика входящих

- Telegram: `POST /webhook/telegram` с `tenant`, `text`, `peer` **или** `telegram_user_id` → событие в БД, заполнение `lead_contacts.peer`, отсутствие `skip_no_text`.
- WhatsApp: `POST /webhook?token=<provider_token>` с валидными `tenant`, `from`/`from_jid`, `text` → событие в БД, связка контактов, отсутствие `skip_no_text`.

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
