# Avio

## PUBLIC_KEY для фронта
- Публичные маршруты Telegram (`/pub/tg/*`) принимают ключ только через параметр `?k=` и сравнивают его со значением `PUBLIC_KEY` из окружения.
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
| `PUBLIC_KEY` | публичный ключ для доступа к `/pub/tg/*` |
| `ADMIN_TOKEN` | админ-токен для приватных RPC эндпоинтов |
| `APP_BASE_URL` | внешний URL API (используется для обратных вызовов) |
| `TG_SESSIONS_DIR` | каталог для хранения `.session` файлов (общий с `app`) |

Том сессий Telegram должен быть примонтирован к контейнерам `app` и `tgworker`, чтобы авторизация сохранялась между перезапусками.

## Telegram Login Flow

### Стадии

- `need_qr` — QR сгенерирован и ждёт сканирования.
- `need_2fa` — аккаунт требует пароль второй факторной авторизации.
- `authorized` — сессия активирована, сообщения начинают поступать в `/webhook/provider`.
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
- **MessageIn** — входящее событие, которое провайдеры (Telegram/WhatsApp) публикуют в `POST /webhook/provider`.

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

### Пример MessageIn

```json
{
  "tenant": 1,
  "channel": "whatsapp",
  "from_id": "79001234567@c.us",
  "to": "my-biz@c.us",
  "text": "Добрый день",
  "attachments": [],
  "ts": 1714650000,
  "provider_raw": {
    "id": "ABCD",
    "type": "chat"
  }
}
```

Каждое валидное входящее событие складывается в Redis по ключу `inbox:message_in` (LPUSH), что позволяет независимо подтверждать доставку.

### Ключи доступа

- `PUBLIC_KEY` используется только на публичных маршрутах (`/pub/tg/*`) и сверяется строго через параметр `?k=`.
- `ADMIN_TOKEN` остаётся приватным и не должен совпадать с `PUBLIC_KEY`.
- Если `PUBLIC_KEY` не задан, фронт временно может использовать `ADMIN_TOKEN`, но это режим совместимости — рекомендуем задать отдельный публичный ключ как можно раньше.

Исторические маршруты `/pub/tg/*` сохранены для обратной совместимости, но считаются **deprecated** — в логах выводится предупреждение не чаще одного раза в час.
