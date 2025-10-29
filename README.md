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

### Дефолтная персона
- Базовый шаблон хранится в `app/agents/persona_default_ru.md` и подставляется, если у арендатора нет собственного `persona.md`.
- Override на уровне арендатора находится в `tenants/<ID>/persona.md` и перекрывает дефолт после сохранения в клиентском кабинете.
- Плейсхолдеры: `{AGENT_NAME}`, `{BRAND}`, `{CITY}` берутся из паспорта бренда, `{CHANNEL}` — из фактического канала диалога (fallback `WhatsApp`), `{WHATSAPP_LINK}` и `{CATALOG_URL}` — из настроек арендатора (если пусто — подставляется пустая строка).
- `{CURRENCY}` всегда нормализуется в `₽`.

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

## Загрузка PDF каталога

### Эндпоинт
`POST /pub/catalog/upload?k=<PUBLIC_KEY>&tenant=<TENANT>`

- Формат: `multipart/form-data`
- Поле файла: **file**  *(также принимается `catalog`, но используйте `file`)*
- Допустимые расширения: `.pdf`, `.csv`, `.xlsx`, `.xls`
- Лимит размера: см. `MAX_UPLOAD_SIZE_BYTES` в коде

### Пример cURL
```bash
curl -F "file=@/path/to/catalog.pdf;type=application/pdf" \
  "https://api.avio.website/pub/catalog/upload?k=YOUR_PUBLIC_KEY&tenant=1"


Успешный ответ:

{ "ok": true, "job_id": "<uuid>", "state": "queued" }

Что делает бэкенд

Сохраняет загруженный файл:
/data/tenants/<TENANT>/uploads/<safe_name>.pdf

Создаёт CSV из PDF:
/data/tenants/<TENANT>/catalogs/<base_name>.csv

Пишет статус джобы:
/data/tenants/<TENANT>/catalog_jobs/<job_id>/status.json

Обновляет конфиг арендатора:
/data/tenants/<TENANT>/tenant.json → integrations.uploaded_catalog
Поля: path, original, uploaded_at, type, size, mime, csv_path, pipeline, index

Публичные настройки для фронтенда

GET /pub/settings/get?k=<PUBLIC_KEY>&tenant=<TENANT>

Ответ содержит:

{ "ok": true, "cfg": { "integrations": { "uploaded_catalog": { ... } }, ... } }


Фронтенд читает cfg.integrations.uploaded_catalog.
После загрузки PDF поле заполнится, а путь к CSV будет в csv_path.

Ошибки

401 {"detail":"invalid_key"} — неверный ключ

400 {"ok":false,"error":"empty_file"} — пустой файл

400 {"ok":false,"error":"unsupported_type"} — неподдерживаемое расширение

400 {"ok":false,"error":"file_too_large","max_size_bytes":...}

422 {"ok":false,"error":"invalid_payload","reason":"invalid_tenant|missing_file"}

Минимальный пример JS-загрузки
<input id="catFile" type="file" accept=".pdf,.csv,.xlsx,.xls">
<button id="uploadBtn">Загрузить</button>
<progress id="catProgress" max="100" value="0" style="width:100%"></progress>
<pre id="catStatus"></pre>
<script>
(() => {
  const pub = window.CLIENT_SETTINGS?.public_key;
  const ten = window.CLIENT_SETTINGS?.tenant || 1;
  const url = `https://api.avio.website/pub/catalog/upload?k=${pub}&tenant=${ten}`;
  const $f = document.getElementById('catFile');
  const $b = document.getElementById('uploadBtn');
  const $p = document.getElementById('catProgress');
  const $s = document.getElementById('catStatus');

  $b.addEventListener('click', async () => {
    if (!$f.files[0]) { $s.textContent = 'Выберите файл'; return; }
    $p.value = 0; $s.textContent = 'Загрузка...';
    const fd = new FormData(); fd.append('file', $f.files[0]);
    const r = await fetch(url, { method: 'POST', body: fd });
    const t = await r.text(); $s.textContent = t;
    try {
      const j = JSON.parse(t);
      if (j.ok) $s.textContent = `Принято. job_id=${j.job_id}`;
    } catch {}
    $p.value = 100;
    // После завершения фронтенд перечитывает /pub/settings/get и берёт cfg.integrations.uploaded_catalog
  });
})();
</script>

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

## Мультиарендный WhatsApp (waweb)

Чтобы каждый арендатор имел собственную сессию WhatsApp и не конфликтовал с остальными, используются отдельные контейнеры `waweb`. Управление вынесено в отдельный compose‑файл (`docker-compose.waweb.yml`) и утилиту `scripts/waweb_manage.py`.

### Конфигурация

- Реестр арендаторов хранится в `config/tenants.yml`:

  ```yaml
  tenants:
    - id: 1
      waweb:
        host: waweb-1
        port: 9001
    - id: 2
      waweb:
        host: waweb-2
        port: 9001
  ```

  `host` попадает в alias Docker‑сети `avio_default`, а `port` — внутрь контейнера (по умолчанию `9001`). При необходимости можно задать собственный `container_name` и `state_dir`.

- Каталоги сессий лежат в `data/wa_state/<TENANT>`. Для защиты создаётся файл `DO_NOT_DELETE.txt`; никакие скрипты не очищают эти каталоги.

- Основной `app` получает URL waweb через `app.core.tenant_waweb_url()`, поэтому после изменения конфига требуется `docker compose restart app`.

### Скрипт управления

```
scripts/waweb_manage.py <command> [--tenant <id>] [--all]

  up        – запустить (или пересобрать) контейнер
  down      – остановить контейнер
  restart   – перезапустить контейнер
  status    – показать `docker compose ps`
  logs      – вывести логи (с --follow для tail -f)
  purge     – попытка очистить state (всегда возвращает «нельзя удалять»)
```

Команды выполняются через `docker compose -f docker-compose.waweb.yml`, поэтому требуется сеть `avio_default` (создаётся основной `docker-compose.yml`).

### Добавление нового арендатора (пример для tenant=2)

1. **Конфиг** – добавить запись в `config/tenants.yml` (см. выше).
2. **Перезапустить `app`**, чтобы он перечитал конфигурацию:
   ```bash
   docker compose restart app
   ```
3. **Поднять контейнер waweb**:
   ```bash
   export ADMIN_TOKEN=sueta    # или ваш реальный токен
   ./scripts/waweb_manage.py up --tenant 2
   ./scripts/waweb_manage.py logs --tenant 2   # убедиться, что сервис поднялся
   ```
4. **Запросить старт сессии через app** (генерация QR):
   ```bash
   docker exec avio-app-1 curl -fsS \
     -H "X-Auth-Token: ${ADMIN_TOKEN}" \
     -H "Content-Type: application/json" \
     -d '{"tenant_id": 2, "webhook_url": "http://app:8000/webhook?token='${ADMIN_TOKEN}'"}' \
     -X POST http://waweb-2:9001/session/2/start
   ```
5. **Проверить статус**:
   ```bash
   docker exec avio-app-1 curl -fsS \
     -H "X-Auth-Token: ${ADMIN_TOKEN}" \
     http://waweb-2:9001/session/2/status
   ```
   Ответ `{"ready":false,"qr":true,...}` означает, что QR готов.
6. **Авторизоваться** – открыть `/pub/wa/start?tenant=2&k=<TENANT_KEY>` и сканировать QR. После подключения статус перейдёт в `ready=true`.

### Использование

- Один контейнер обслуживает одного арендатора. Для нескольких арендаторов запускаются `waweb-1`, `waweb-2` и т.д.
- Приложение `app` автоматически обращается к нужному контейнеру (никаких публичных переменных `WA_WEB_URL` не осталось).
- Очистка `data/wa_state/<TENANT>` недопустима: это приведёт к потере авторизации. При необходимости «сбросить» сессию используйте `POST /session/<tenant>/logout` или `restart`.

### Диагностика

```bash
# состояние контейнера
./scripts/waweb_manage.py status --tenant 1

# tail -f логов
./scripts/waweb_manage.py logs --tenant 1 -f

# проверка API из app
docker exec avio-app-1 curl -fsS -H "X-Auth-Token:${ADMIN_TOKEN}" http://waweb-1:9001/session/1/status

# экспорт SVG QR
docker exec avio-app-1 curl -fsS -H "X-Auth-Token:${ADMIN_TOKEN}" http://waweb-1:9001/session/1/qr.svg -o /tmp/wa-qr.svg
```

Если `/session/<tenant>/status` долго висит, смотрите лог контейнера (`SingletonLock`, `Failed to launch the browser process` и т.п.). Часто помогает `./scripts/waweb_manage.py restart --tenant <id>` с последующим `curl` через 20–30 секунд.


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

## Наблюдения и технические заметки (октябрь 2025)

- **LLM и промпты.** Ключи `OPENAI_API_KEY` и `OPENAI_MODEL` задаются через `.env`. В `app/core/__init__.py` реализован автоматический сброс `_PERSONA_HINTS_CACHE`, поэтому после обновления `persona.md` достаточно сохранить файл — перезапуск не нужен. Текущие параметры генерации: `temperature≈0.7`, `top_p=0.9`, `frequency_penalty=0.2`, `presence_penalty=0.05`, `max_tokens=260`.
- **Каталоги арендаторов.** Активный `TENANTS_DIR` — `data/tenants`. Все редакции `tenant.json`, `persona.md` и загруженных каталогов делаем тут; каталог `app/tenants` больше не используется.
- **Локальная проверка диалогов.** Команда `test` (обёртка над `scripts/chat_simulator.py`) работает из `.venv`. Полезные параметры: `--tenant`, `--contact`, `--channel`, `--reset`, `--show-messages`. Внутри сессии команда `reset` очищает состояние текущего контакта.
- **Состояния диалогов.** Redis-хранилище (`sales_state:<tenant>:<contact>`) монтируется на хост в `data/redis`. Для ручного сброса:  
  ```bash
  docker-compose exec -T redis redis-cli keys 'sales_state:1:*'
  docker-compose exec -T redis redis-cli del sales_state:1:<CONTACT_ID>
  ```  
  либо из Python:  
  ```bash
  .venv/bin/python - <<'PY'
  from app import core
  core.reset_sales_state(tenant=1, contact_id=<CONTACT_ID>)
  PY
  ```
- **Связка каналов.** `resolve_or_create_contact` ищет существующий контакт по `whatsapp_phone`, `avito_user_id`, `avito_login`, `telegram_user_id`. Если при переходе с Avito на WhatsApp передавать `leadId` от авито-чата или заранее сохранять номер телефона, бот продолжит диалог в рамках одного контакта.
- **Персонализация.** Плейсхолдеры `{BRAND}`, `{AGENT_NAME}`, `{CITY}` и др. берутся из `tenant.json`. При изменении бренда обновляйте паспорт, иначе в ответах останутся старые названия.

## Avito Messenger Интеграция

### OAuth и токены
- Ссылка авторизации (страница `/connect/avito`) всегда формируется со scope `messenger:read,messenger:write,user:read`.
- После успешной авторизации в `tenants/<ID>/tenant.json` автоматически сохраняются `access_token`, `refresh_token`, `expires_at`, `account_id`, `account_login`.
- Автоответ включается автоматически: `behavior.auto_reply = true`, `behavior.auto_reply_enabled = true`.
- Token refresh выполняется автоматически воркером при каждом запросе; при 401 выполняется повторный обмен по `refresh_token`.

### Webhook
- Avito требует активировать Messenger API в кабинете разработчика (подтверждение партнёра).
- Автоматическая регистрация: после OAuth и при нажатии «Обновить статус» вызывается `POST https://api.avito.ru/messenger/v3/webhook` с целью `https://hub.avio.website/webhook/avito` и типом `messages`.
- Если маршрут ещё недоступен (Avito возвращает 404), webhook можно зарегистрировать вручную:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v3/webhook        -H "Authorization: Bearer <ACCESS_TOKEN>"        -H "Content-Type: application/json"        -d '{"url":"https://hub.avio.website/webhook/avito","types":["messages"]}'
  ```
- Проверка текущих подписок:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v1/subscriptions        -H "Authorization: Bearer <ACCESS_TOKEN>"
  ```
- Снять подписку:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v1/webhook/unsubscribe        -H "Authorization: Bearer <ACCESS_TOKEN>"        -H "Content-Type: application/json"        -d '{"url":"https://hub.avio.website/webhook/avito"}'
  ```

### Структура входящих событий
Avito присылает JSON вида:
```json
{
  "id": "evt-…",
  "timestamp": "2024-…",
  "payload": {
    "type": "message",
    "value": {
      "account_id": 400040070,
      "chat_id": "987654",
      "type": "text",
      "content": { "text": "Здравствуйте" },
      "author_id": 123456,
      "published_at": "2024-…"
    }
  }
}
```
- Используется `payload.value.chat_id`, `payload.value.type`, `payload.value.content.*` для текста и вложений.
- `payload.value.author_id` — отправитель; `payload.value.user_id` совпадает с нашим аккаунтом.
- Мы создаём лиды с `avito.s table_lead_id(account_id, chat_id)` и обновляем поле `peer`.

### Логика webhook (/webhook/avito)
- Парсим `payload.value`: извлекаем `chat_id`, текст, вложения, `author_id`, `account_id`.
- Заполняем `incoming_body`: `peer`, `attachments`, `lead_contacts`, `account_id`, `auto_reply_handled = False`, чтобы воркер запустил автоответ.
- Контакты сохраняются через `resolve_or_create_contact` (поля `avito_user_id`, `avito_login`).
- В логах для входящих сообщений: `webhook_received ch=avito…`, `stage=incoming_enqueued…`, `lead_upsert_ok…`.

### Воркер (автоответ)
- Хранит кеш `AVITO_CHAT_CACHE` `{tenant: chat_id}`. После каждого webhook и успешной отправки `chat_id` обновляется.
- При ответе (`send_avito`) используем `chat_id` из payload (`item['chat_id']`/`peer`), иначе читаем из кеша. Если `chat_id` отсутствует и в кеше, доставка прерывается с `missing_chat`.
- Формат отправки (соответствует Avito API v1):
  ```json
  {
    "type": "text",
    "message": { "text": "Спасибо за обращение" }
  }
  ```
  Запрос: `POST https://api.avito.ru/messenger/v1/accounts/{account_id}/chats/{chat_id}/messages`.
- Отправка считаем успешной при `status 200`. Логи: `event=send_result status=sent reason=ok channel=avito…`.

### Ручные команды Avito (для отладки)
```bash
# зарегистрировать webhook
curl -X POST https://api.avito.ru/messenger/v3/webhook      -H "Authorization: Bearer $AT"      -H "Content-Type: application/json"      -d '{"url":"https://hub.avio.website/webhook/avito","types":["messages"]}'

# список подписок
curl -X POST https://api.avito.ru/messenger/v1/subscriptions      -H "Authorization: Bearer $AT"

# отписка
curl -X POST https://api.avito.ru/messenger/v1/webhook/unsubscribe      -H "Authorization: Bearer $AT"      -H "Content-Type: application/json"      -d '{"url":"https://hub.avio.website/webhook/avito"}'
```

### Типичные ошибки
| Сообщение в логах | Причина / решение |
|-------------------|--------------------|
| `avito_webhook_set_failed status=404 …` | Messenger API ещё не включён. Нужно дождаться подтверждения партнёра или активировать webhook вручную.
| `avito_webhook_skip reason=no_chat` | В событии не пришёл `chat_id` — теперь кеш используется, но если случится повторно, проверить payload или доступ accountants.
| `send_result status=skipped reason=missing_chat` | Кеш ещё не заполнен и нет `chat_id`. Проверь, что первый ответ прошёл успешно (2xx). |
| `send_result status=status_400` | Avito вернул ошибку (пустой текст, недоступный чат и т.п.). см. `body=` в логе. |
| `unauthorized` | `access_token` устарел или потерян — перепройти OAuth и провернуть регистрацию webhook. |

```bash
# тестовая посылка события в /webhook/avito
curl -X POST https://hub.avio.website/webhook/avito   -H 'Content-Type: application/json'   -d '{"id":"evt-1","timestamp":"2024-01-01T00:00:00Z","payload":{"type":"message","value":{"account_id":400040070,"chat_id":"987654","type":"text","content":{"text":"Здравствуйте"},"author_id":123456,"published_at":"2024-01-01T00:00:00Z"}}}'
```

После этих правок Avito бот автоматически отвечает на каждое входящее сообщение.

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
  - Для входящих событий `waweb` обязателен доступ либо к `ADMIN_TOKEN`, либо к `WA_WEB_TOKEN`/`WEBHOOK_SECRET`. Без токена сервис не сможет получить `provider_token`, в логах появится `provider_token_unauthorized`, и бот перестанет отвечать. При ручном запуске `node index.js` заранее экспортируйте нужный токен (например, `ADMIN_TOKEN` из `.env`).

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
- Токен хранится не только в БД, но и на диске: `app/tenants/<TENANT>/provider_token.json`. При отсутствии PostgreSQL приложение читает/создаёт файл автоматически, поэтому **важно монтировать каталог `app/tenants` для `app`, `worker` и `waweb`**.
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

### Автоответ и outbox

- Обязательно включите очереди: `INBOX_ENABLED=true`, `OUTBOX_ENABLED=true`. Без этого воркер воркера мгновенно пропускает сообщения со статусом `outbox_disabled`.
- `OUTBOX_WHITELIST` должен содержать `*` либо список разрешённых номеров (`+7999…`, `7999…`, `7999…@c.us`). Пустое значение означает «запретить все отправки» и приводит к `reason=whitelist_miss`.
- При недоступной БД воркер всё равно сгенерирует автоответ: lead_id берётся из номера отправителя, а проверка `lead_exists` переводится в предупреждение вместо жёсткого отказа. Поэтому записи вида `event=send_result status=warning reason=err:no_lead` допустимы при «офлайн»-режиме — сообщение всё равно ставится в очередь `outbox:send`.
- Проверьте, что WA token актуален: `curl -H "X-Admin-Token: ${ADMIN_TOKEN}" http://app:8000/admin/provider-token/<TENANT>` → ответ `{"ok": true, ...}`. Если `500`, подмонтируйте `app/tenants` и перезапустите `app`, чтобы пересоздался `provider_token.json`.
- `waweb` должен поднимать сессию с тем же токеном (`ADMIN_TOKEN` или `WA_WEB_TOKEN`). После обновления токена перезапустите `waweb`, иначе появится `provider_token_unauthorized`.
- Проверка цепочки:
  1. Написать тестовое сообщение → в логах `app` увидеть `incoming_enqueued`/`webhook_received`.
  2. В `worker` найти `event=smart_reply_generated` и `event=smart_reply_enqueued`.
  3. В `worker` после отправки должен появиться `event=send_result status=sent` (или `status=warning …` при деградированном режиме).
  4. В `waweb` — `event=message_out channel=whatsapp … result=success`.
- Для удобства диагностики есть скрипт `deploy/diag/wa.sh`, который проверяет токены, переменные `OUTBOX_*`, выполняет тестовую отправку и собирает свежие логи `app`/`waweb`.

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
vps test 2025-10-23T12:59:53+03:00
