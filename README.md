# Avio

## PUBLIC_KEY для фронта
- Публичные маршруты Telegram (`/pub/tg/*`) принимают ключ только через параметр `?k=` и сравнивают его со значением `PUBLIC_KEY` из окружения.
- Значение `PUBLIC_KEY` обязательно и должно отличаться от `ADMIN_TOKEN`, чтобы не давать фронту доступ к административным операциям.
- При отсутствии `PUBLIC_KEY` система временно принимает `ADMIN_TOKEN` как запасной вариант, но это режим совместимости и рекомендуется задать отдельный ключ для фронта как можно раньше.

### Примеры `curl`

```bash
# Запуск логина по QR
curl -G "https://api.avio.website/pub/tg/start" \
  --data-urlencode "tenant=1" \
  --data-urlencode "force=false" \
  --data-urlencode "k=${PUBLIC_KEY}"

# Проверка статуса авторизации
curl -G "https://api.avio.website/pub/tg/status" \
  --data-urlencode "tenant=1" \
  --data-urlencode "k=${PUBLIC_KEY}"

# Получение PNG с QR-кодом
curl -G "https://api.avio.website/pub/tg/qr.png" \
  --data-urlencode "tenant=1" \
  --data-urlencode "qr_id=<значение из /start>" \
  --data-urlencode "k=${PUBLIC_KEY}" \
  --output tg-qr.png
```

### Переменные окружения tgworker

| Переменная | Назначение |
|------------|------------|
| `TELEGRAM_API_ID` | идентификатор приложения Telegram | 
| `TELEGRAM_API_HASH` | hash приложения Telegram |
| `PUBLIC_KEY` | публичный ключ для доступа к `/pub/tg/*` |
| `ADMIN_TOKEN` | админ-токен для приватных RPC эндпоинтов |
| `APP_BASE_URL` | внешний URL API (используется для обратных вызовов) |

### Авторизация по QR для каждого арендатора

1. Вызвать `GET /pub/tg/start?k=${PUBLIC_KEY}&tenant=<TENANT_ID>`; при необходимости передать `force=true`.
2. Забрать `qr_id` из ответа и запросить `GET /pub/tg/qr.png?k=${PUBLIC_KEY}&tenant=<TENANT_ID>&qr_id=<QR_ID>` для отображения кода.
3. Параллельно опрашивать `GET /pub/tg/status?k=${PUBLIC_KEY}&tenant=<TENANT_ID>` до тех пор, пока поле `authorized` не станет `true`.
4. После успешной авторизации статус будет возвращать `authorized=true`, а QR перестанет быть активным.

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
