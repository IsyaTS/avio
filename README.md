# Avio

## Project Layout
- `apps/api` — основной FastAPI сервис: публичный сайт, auth, клиентский кабинет, webhook-и и internal/public API.
- `apps/worker` — основной Redis worker: smart reply, follow-up, handoff, amoCRM/Avito/MAX/WhatsApp/Telegram синхронизация.
- `apps/tgworker` — отдельный Telegram transport/auth сервис (FastAPI + Telethon): QR/2FA, отправка, загрузка медиа.
- `apps/waweb`, `apps/wabaileys` — WhatsApp-транспорты.
- `libs/core` — общий код: БД, LLM/response pipeline, catalog flow, интеграции, message envelope, tenant helpers.
- `db/init`, `db/migrations` — инициализация и SQL-миграции.
- `apps/frontend/client-portal` — исходники клиентского SPA.
- `apps/api/static/spa/client` — собранная SPA-статика.
- `scripts` — smoke/regression-скрипты и локальная диагностика.

## Operational docs
- `AGENTS.md` — правила работы Codex/AI-агентов, prod/dev safety, green gates и anti-monolith policy.
- `ARCHITECTURE.md` — карта runtime boundaries и critical flows: tenant config, Avito OAuth, incoming worker path, learning, observability.
- `RUNBOOK.md` — пошаговая диагностика для `invalid_state`/`missing_state`, `unknown_tenant`, worker/outbox/settings/learning инцидентов.

## Public landing + Email Auth
- Новый публичный лендинг доступен по `/` (включается флагом `ENABLE_PUBLIC_LANDING=1`).
- Авторизация/регистрация по email включается флагом `ENABLE_EMAIL_AUTH=1`.
- Админка `/admin` остаётся внутренней (без ссылок на публичные страницы).
- Доступ к `/client/{tenant}/settings`:
  - по сессии (cookie) — без `k`
  - fallback на старую схему `?k=` и cookie `client_key` (если `AUTH_FALLBACK_MAGIC_LINK=1`, по умолчанию включён).
- Регистрация собирает `email`, `phone`, `password`, `contact`, `preferred_messenger`.
- После подтверждения email создаётся серверная сессия, пользователь попадает в свой tenant без `k`.
- Успешное подтверждение может отправлять уведомление в Telegram-чат через `NOTIFY_BOT_TOKEN` и настроенные chat ids.

### Новые эндпоинты
- `GET /login`, `GET /register` — формы входа/регистрации.
- `POST /auth/login`, `POST /auth/register`.
- `GET /auth/verify?token=...` — подтверждение email.
- `GET /forgot`, `POST /auth/forgot` — сброс пароля.
- `GET /reset?token=...`, `POST /auth/reset` — установка нового пароля.
- `POST /auth/logout`, `GET /dashboard`.

### Переменные окружения
- Флаги:
  - `ENABLE_PUBLIC_LANDING=1|0`
  - `ENABLE_EMAIL_AUTH=1|0`
  - `AUTH_FALLBACK_MAGIC_LINK=1|0` (по умолчанию `1`)
- Сессии:
  - `SESSION_COOKIE_NAME=avio_session`
  - `SESSION_TTL_DAYS=14`
  - `AUTH_COOKIE_SECURE=1` (если за прокси и https не виден в `request.url.scheme`)
- Email:
  - `PUBLIC_BASE_URL=https://<domain>`
  - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`
  - `SMTP_TLS=1` (по умолчанию), `SMTP_SSL=1` (если нужно SSL)

### Таблицы БД
- `auth_tenants`, `users`, `user_tokens`, `user_sessions` (создаются через `ensure_auth_schema()` на старте).
  - Жёсткое правило: `users.tenant_id` UNIQUE → 1 пользователь = 1 tenant.
  - Токены (`verify/reset`) хранятся как SHA‑256 хэши, сырые токены не логируются.
  - В `users` дополнительно хранятся `contact` и `preferred_messenger`.

### Безопасность
- rate‑limit на login/register/resend (Redis, fallback in‑memory в тестах).
- CSRF для форм (double submit cookie).
- cookie сессии: HttpOnly + SameSite=Lax, Secure в проде.

## Follow-ups и Avito автоответ

- Правила follow-up лежат в `data/tenants/<id>/tenant.json` → `follow_up` (или через UI `/client/{tenant}/follow-ups`).
- Воркер `apps/worker` ставит задачи при входящих сообщениях по каналу (avito/telegram/whatsapp) и вынимает их из Redis:
  - ZSET `followup:schedule`, HASH `followup:job:{id}`; очередь на отправку — `OUTBOX_QUEUE_KEY`.
  - Дедуп: `followup:scheduled:{tenant}:{lead}:{rule}` и `followup:sent:{tenant}:{lead}:{rule}` (TTL 24ч по умолчанию). Удалите их, чтобы вручную переотправить для лида.
  - Тюнинг: `FOLLOWUPS_ENABLED` (on/off), `FOLLOWUP_POLL_INTERVAL`, `FOLLOWUP_BATCH_LIMIT`, `FOLLOWUP_SCHEDULE_DEDUP_TTL`, `FOLLOWUP_SENT_DEDUP_TTL`.
- Avito автоответ:
  - Включить: `behavior.auto_reply=true`, заполнить `behavior.auto_reply_text` в конфиге арендатора.
  - Дедуп: `avito:auto_reply_sent:{tenant}:{lead}` (TTL `AVITO_AUTO_REPLY_TTL`, по умолчанию 86400).
  - Требуются валидные токены в `integrations.avito` (`account_id`, `refresh_token`, `access_token`); иначе будет `reason=token_unavailable`.
  - Диагностика: ищите в логах воркера `avito_auto_reply_enqueued`/`avito_auto_reply_skip`, для токенов — `token_unavailable` или ошибки Avito API.

### Follow-ups по условию: ответы vs молчание
- Факт ответа хранится в Redis и пишется только если сработал `capture` (ответ "да/нет").
- Если ответа не было, факта нет: условие `op: "not_exists"` срабатывает.
- Если нужен сценарий "ответил нет" → использовать `trigger_on_answer=true` + `condition: { op: "eq", value: "no" }`.
- Если нужен сценарий "молчание" → отдельный follow-up с `delay_minutes` и `condition: { op: "not_exists" }`.
- Важно: `delay_minutes` считается от первого входящего сообщения лида, а не от вопроса.

## Диалоги (Avito + Telegram) и обратная связь
- Вкладка «Диалоги» в кабинете клиента (`/client/{tenant}/settings#dialogs`) показывает список лидов слева и ленту сообщений справа; отправка ответов идёт через очереди воркера.
- Интерфейс автообновляется (polling ~5с) без необходимости жать «Обновить»; ручные кнопки остаются как fallback. При открытии диалога лента прокручивается к последнему сообщению.
- API под ключ клиента (`k` + `tenant`):
  - `GET /api/dialogs` — список диалогов с last_message/last_ts.
  - `GET /api/dialogs/{lead_id}` — история сообщений.
  - `POST /api/dialogs/{lead_id}/send` — отправка текста или фото (очередь OUTBOX). Поля: `text` и/или `photo_id`.
  - `POST /api/feedback` — лайк/дизлайк для ответов бота (`rating` = like|dislike, dislike требует `comment`).
- База данных: в `messages` есть флаг `is_bot` (по умолчанию `false`); таблица `message_feedback` используется для лайков/дизлайков. Если таблицы нет, диалоги продолжают работать, но фидбек не сохраняется.
- Сообщения менеджера:
- Avito: из UI сохраняются в `messages` как исходящие (`direction=1`, `is_bot=false`), видны в диалогах; дополнительно ставится тишина (`handoff:silence:<tenant>:<lead>`). Эхо бота игнорируется по ключу `avito:bot_echo:<tenant>:<chat_id>`.
- Telegram: сообщения из клиента сохраняются как исходящие и видны в диалогах; тишина ставится так же, как и для Avito.
- Avito фото-эхо: при отправке фото бот кэширует echo-маркер (`__image__`) вместе с текстом, а webhook сравнивает входящие события с этим списком. Это предотвращает ложную тишину от исходящих фото.
  - Для входящих событий `type=image` всегда проставляется `text="__image__"` (fallback), чтобы эхо распознавалось даже без текста.
  - При наличии нескольких вложений Avito отправляет каждую картинку отдельно (батч из нескольких фото).
- Сообщение об отписке после отложенных:
  - В правиле отложенного сообщения доступен флаг `stop_notice_after` (чекбокс в UI).
  - Если флаг стоит — после этого шага будет отправлено сообщение об отписке.
  - Если флаг не выбран ни в одном правиле — сообщение об отписке отправляется после первого отложенного сообщения (fallback).
- Для сброса тишины/отписок/дедуп-меток у лида используются ключи Redis:
  - `handoff:silence:<tenant>:<lead>`
  - `handoff:silence:meta:<tenant>:<lead>`
  - `followup:optout:<tenant>:<lead>`
  - `followup:stop_notice:<tenant>:<lead>`
  - `followup:scheduled:<tenant>:<lead>:*`
  - `followup:sent:<tenant>:<lead>:*`

## amoCRM Chats / Inbox
- Для Telegram ↔ amoCRM используется отдельный chat-layer:
  - `libs/core/services/amocrm_chat.py`
  - `libs/core/repo/crm_chat_links.py`
  - webhook-и: `POST /pub/integrations/amocrm/chat/webhook` и `POST /pub/integrations/amocrm/chat/webhook/{scope_id}`
- Отдельная таблица: `crm_chat_links` (миграция `db/migrations/20260303_amocrm_chat_links.sql`).
- Chat sync работает поверх обычной amoCRM OAuth-связки и не заменяет её:
  - входящее из Telegram создаёт/обновляет chat-link и может отправляться в amoCRM Chats/Inbox;
  - ответ менеджера из amoCRM возвращается в Telegram через `tgworker`;
  - медиа поддерживаются в обе стороны: текст, фото, видео, файлы, голосовые.
- Конфигурация берётся из `integrations.amocrm.chat` и/или env:
  - `AMOCRM_CHAT_ENABLED`
  - `AMOCRM_CHAT_SCOPE_ID`
  - `AMOCRM_CHAT_CHANNEL_ID`
  - `AMOCRM_CHAT_SECRET`
  - `AMOCRM_CHAT_BOT_ID`
  - `AMOCRM_CHAT_WEBHOOK_TOKEN`
  - `AMOCRM_CHAT_SOURCE_ID`
  - `AMOCRM_CHAT_BASE_URL`

## LLM pipeline (единый путь ответа)
- Боевая генерация ответа идёт через единый worker-pipeline; дополнительный orchestrator вынесен в `libs/core/response_pipeline.py` и включается флагом `RESPONSE_PIPELINE_ENABLED=1`.
- Для tenant доступен выбор режима мозга:
  - `smart` — текущий основной режим
  - `classic` — legacy/compatibility режим (`prod` и `legacy` тоже маппятся в `classic`)
- Тестовый диалог (`POST /api/dialogs/test`) использует тот же боевой пайплайн, включая:
  - split длинных ответов на короткие сообщения,
  - опциональную задержку ответа,
  - split-part delay между частями ответа.
- Основные env-параметры задержки и дробления:
  - `SMART_REPLY_DELAY_MIN_SECONDS` / `SMART_REPLY_DELAY_MAX_SECONDS`
  - `SMART_REPLY_BURST_ENABLED`
  - `SMART_REPLY_SPLIT_ENABLED`
  - `SMART_REPLY_SPLIT_MIN_LEN` / `SMART_REPLY_SPLIT_MAX_LEN`
  - `SMART_REPLY_SPLIT_PART_DELAY_ENABLED`
  - `SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS` / `SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS`
- Внутренний message pipeline нормализован через `libs/core/message_envelope.py`: единые `message_kind`, `attachments`, placeholders, fingerprints и trigger flags.

## Avito history export и dialog-level обучение

Цель этого слоя — дешёво подготовить корпус реальных Avito-диалогов менеджера и подключить его к ответам бота без массовой AI-разметки каждого ответа. Это не fine-tune, не запись в `training_examples` и не импорт в боевые `messages/leads`.

### Поток в ЛК
- Вкладка: `Обучение` в клиентском SPA (`apps/frontend/client-portal/src/pages/TrainingTab.tsx`).
- Клиент вводит количество хороших диалогов и нажимает `Подготовить файл диалогов`.
- Backend скачивает историю Avito через текущий OAuth/token flow, фильтрует мусор/автоответчик и сохраняет артефакты в tenant-local path:
  - `/data/tenants/{tenant_id}/uploads/dialogs/dialogs_*.md`
  - `/data/tenants/{tenant_id}/uploads/dialogs/dialog_dataset_*.jsonl`
  - `/data/tenants/{tenant_id}/uploads/dialogs/domain_schema_*.json`
  - `/data/tenants/{tenant_id}/uploads/dialogs/business_rules_draft_*.json`
  - `/data/tenants/{tenant_id}/uploads/dialogs/export_summary_*.json`
- `dialogs_*.md` нужен для ручного просмотра.
- `dialog_dataset_*.jsonl` — основной дешёвый обучающий корпус: одна строка = один accepted dialog целиком, без системных сообщений и без Avito raw ids.

### Почему это дешево
- Default pipeline больше не режет каждый диалог на тысячи `context -> ideal_reply` candidates.
- AI используется только для sample-level `domain_schema`/`business_rules_draft`, а не для массовой разметки каждого ответа.
- Default artifact для будущего обучения — `dialog_dataset_*.jsonl`, а legacy `contextual_cases_*.jsonl` не создаётся без явного legacy-флага.

### Backend modules
- Export orchestration: `libs/core/services/avito_history_export.py`.
- Фильтр диалогов: `libs/core/services/avito_dialog_filter.py`.
- Markdown writer: `libs/core/services/avito_dialog_export_writer.py`.
- Dialog-level JSONL writer: `libs/core/services/avito_dialog_dataset_writer.py`.
- Checkpoints: `libs/core/services/avito_export_checkpoint.py`.
- Job metadata repo: `libs/core/repo/avito_history_exports.py`.
- Client runtime/routes: `apps/api/web/services/client_avito_history_export_runtime.py`; `apps/api/web/client.py` остаётся тонким route-registration слоем.

### Подключение к ответам
- После завершения export в UI появляется кнопка `Подключить к ответам`.
- Endpoint: `POST /client/{tenant}/avito/history/export/{job_id}/activate-dataset`.
- Runtime строит TF-IDF индекс из `dialog_dataset_*.jsonl` через `libs/core/training/dialog_retriever.py`.
- Индекс сохраняется в `data/tenants/{tenant_id}/indexes/dialog_training_<sha1>.pkl`.
- В `tenant.json` пишется только metadata:
  - `learning.dialog_dataset.enabled=true`
  - `source_job_id`
  - `dialogs_count`
  - `index_sha1`
  - relative `index_path`
  - `dataset_file_name`
- Абсолютные пути не отдаются в API/UI.
- Кнопка `Отключить` вызывает `POST /client/{tenant}/avito/history/export/{job_id}/deactivate-dataset` и ставит `learning.dialog_dataset.enabled=false`. Файлы и индекс не удаляются, поэтому набор можно подключить снова.

### Как bot использует dataset
- `libs/core/response_pipeline.py` вызывает `dialog_retriever.build_dialog_examples_block(...)`, если `learning.dialog_dataset.enabled` не `false` и индекс существует.
- В prompt добавляются 1-3 похожих реальных диалога менеджера как стиль и сценарий общения.
- Guard в prompt прямо запрещает копировать цены, адреса, контакты и условия из похожего диалога, если они не подтверждены текущим клиентом.
- Если dialog-level index есть, legacy pair-training block не добавляется по умолчанию, чтобы две разные логики обучения не конфликтовали.
- Быстрый rollback без удаления файлов: поставить `learning.dialog_dataset.enabled=false` и перезапустить `app/worker` при необходимости.

### Проверки после изменений в этом слое
```bash
.venv/bin/pytest tests/test_avito_history_export.py tests/test_client_avito_history_export_runtime.py -q
.venv/bin/pytest tests/test_dialog_training_retriever.py tests/test_training_retriever_legacy_index.py tests/test_truth_critical_flows.py tests/test_learning_policy_v2.py tests/test_learning_feedback.py -q
.venv/bin/pytest tests/test_client_settings.py tests/test_public_settings.py -q
cd apps/frontend/client-portal && npm run build
python scripts/message_pipeline_smoke.py
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
python scripts/monolith_guard.py
```

## Каталог (CSV/XLSX/PDF)
- Импорт приводит названия к безопасному виду (`clean_title`) и не падает на единицах/скобках в title (например, `110 (110 ММ)`); если title всё ещё содержит запрещённые токены — импорт вернёт ошибку.

## Файлы: фото (Avito + Telegram)
- Фото хранятся в `data/tenants/<id>/uploads/photos/` и описываются в `manifest.json` рядом (без БД).
- Ограничения: только `jpg/jpeg/png/gif/bmp/heic`, размер до **24 MB** (лимит Avito).
- Метаданные фото (через UI «Файлы»): `title`, `tags`, `usage`, `channels`, `auto`, `priority`.
- UI нововведения:
  - Папка 📁 «Фото (N)» — список фото разворачивается по клику.
  - Мультизагрузка: можно выбрать несколько файлов за раз.
  - Предпросмотр: клик по фото открывает просмотр почти на весь экран.
- Публичные endpoints (требуют `tenant` + `k`):
  - `GET /pub/files/photos/list` — список фото (с URL для превью).
  - `POST /pub/files/photos/upload` — загрузка (multipart, поле `file`).
  - `DELETE /pub/files/photos/{photo_id}` — удаление.
  - `GET /pub/files/photos/{photo_id}` — отдача файла (используется для превью и Telegram).
  - `POST /pub/files/photos/{photo_id}/meta` — обновление метаданных фото.
- Отправка:
  - Telegram: через tgworker с attachment URL.
  - Avito: `uploadImages` → `messages/image` (отправка изображения без ссылок).
- Авто‑отправка (LLM):
- Включается флагом `behavior.auto_photo_enabled` и лимитом `behavior.auto_photo_max` (макс фото за ответ).
- Авто‑подбор фото работает только по тегам/usage (LLM‑подбор отключён).
  - Бот выбирает фото только из тех, где `auto=true` и канал входит в `channels`.
  - Для выбора используются `tags`/`usage` и текст клиента/ответ бота.

## Client SPA (redesign)
- Исходники: `apps/frontend/client-portal/` (Vite + React + TS + Tailwind).
- Сборка: `cd apps/frontend/client-portal && npm ci && npm run build`.
- Статика: `apps/api/static/spa/client/`, отдаётся по `/static/spa/client/`.
- Кабинет: `/client/{tenant}/settings` (legacy-страница доступна по `?legacy=1`).
- /connect/* редиректят на вкладку «Каналы» в SPA.
- Dev (опционально): поднять Vite и задать `VITE_DEV_SERVER_URL=http://localhost:5173`.
- Черновик настроек в SPA сохраняется в `sessionStorage` (переключение вкладок не сбрасывает чекбоксы/поля).
- Текущие вкладки SPA: `Настройки`, `Каналы`, `Файлы`, `Обучение`, `Статистика`.
- Вкладка «Обучение» содержит реальный тестовый диалог, который повторяет боевую логику ответа и умеет включать/выключать задержку.
- Вкладка «Каналы» показывает статусы Telegram, amoCRM OAuth и amoCRM chat sync.
## Avito вебхуки и multi-tenant
- Маршрутизация Avito-событий выполняется по `account_id`. В вебхуках v3, где `account_id` отсутствует, используется fallback на `payload.value.user_id`, после чего вызывается `find_tenant_by_account`.
- Если `account_id` не определён или не найден в конфиге арендатора, событие пропускается (нет дефолта на `TENANT/TENANT_ID`), чтобы не уезжать в чужой тенант.

## Telegram multi-tenant
- Вебхук `/webhook/telegram` требует явный tenant (`tenant` или `tenant_id`); при его отсутствии возвращает 400.
- Воркер игнорирует входящие Telegram-события без tenant, чтобы сообщения не попадали в дефолтный арендуемый контур.
- Telegram поддерживает multi-slot режим: у одного tenant может быть несколько авторизованных Telegram-слотов с отдельным включением/выключением.
- Для исходящих и входящих сообщений слот прокидывается через payload/контекст; UI позволяет выбирать активный слот в Telegram-диалоге.

## Telegram каталог (PDF)
- PDF берётся из `data/tenants/<id>/uploads/catalog.pdf` или `meta.catalog_pdf_path` и в Telegram отправляется файлом (без ссылок на viewer).
- tgworker проставляет имя/расширение из attachment (`filename`/`name`/`title`/`url`) при формировании `InputFile`, поэтому не должно быть `unnamed`.
- Вложения дедуплицируются по ключу (`url`, `name`, `mime`), чтобы один и тот же файл не улетал несколько раз.

## Telegram фото → handoff (тишина)
- В `/webhook/telegram` парсится `message.provider_raw`/`media`/`photo` (в т.ч. Telethon `MessageMediaPhoto`). Фото/любое вложение ставит `has_photo=True`.
- В `/webhook/avito` `content.image.sizes` может быть `dict` (а не список); URL берём из значений этого словаря, чтобы корректно проставлять `attachments` и `has_photo`.
- По умолчанию любое фото/вложение ставит флаг тишины: Redis ключ `handoff:silence:<tenant>:<lead_id>`, TTL `HANDOFF_SILENCE_TTL_SECONDS` (по умолчанию 86400). Smart reply/LLM не отправляются до истечения TTL.
- Если у арендатора включено ожидание фото (`behavior.photo_expected_markers`), то:
  - когда бот отправляет ответ с любым маркером, ставится `conv:state:<tenant>:<lead_id>=waiting_photo` (TTL `photo_expected_ttl` или TTL тишины по умолчанию);
  - если в этот момент клиент присылает фото/файл, бот отправляет `behavior.photo_expected_reply` (если задан), state очищается, тишина не ставится, но уведомление менеджеру отправляется;
  - любое другое фото без состояния работает по старой схеме (тишина + уведомление).
- Воркер при входящих в этот чат пишет `event=smart_reply_silenced`. Снять тишину: `redis-cli DEL handoff:silence:<tenant>:<lead_id>`.
- Каталог кешируется отдельно ключом `catalog:sent:<tenant>:tg:<peer>` (TTL `STATE_TTL_SECONDS`), он не влияет на тишину, только на повторную отправку каталога.

## Avito ответы
- Для Avito события воркер берёт автоответ из настроек арендатора `behavior.auto_reply_text` (UI: раздел «Поведение и триггеры»). Если текста нет или флаг `behavior.auto_reply` выключен, автоответ не отправляется. Персона больше не используется для Avito-автоответа.
- Если в Avito-сообщении найден российский номер телефона (формат `+7`/`7`/`8`, приводим к `+7XXXXXXXXXX`, строго 11 цифр), воркер один раз отправляет лидеру сообщение в Telegram от сессии этого же тенанта. Текст берётся из `behavior.avito_phone_tg_template` (UI вкладка «Поведение»); если пусто — падаем назад на `persona.meta.avito_phone_tg_template` (старый формат). Дедуп по ключу `avito:phone_tg_sent:<tenant>:<lead_id>` с TTL `AVITO_PHONE_TG_TTL` (по умолчанию 86400, отключается `AVITO_PHONE_TG_DEDUP_DISABLED=1`).
- Автоответ Avito отправляется один раз на lead/chat: при удачной постановке в очередь ставится ключ `avito:auto_reply_sent:<tenant>:<lead_id>` с TTL `AVITO_AUTO_REPLY_TTL` (по умолчанию 86400).
- Сбросить дедуп автоответа: `redis-cli DEL avito:auto_reply_sent:<tenant>:<lead_id>` (для dev `docker compose exec redis ...`).
- Триггеры тишины: в UI «Поведение и триггеры» можно задать фразы + каналы (TG/Avito/WA). При совпадении воркер ставит тишину и (по желанию) уведомляет менеджера; автоответчик/LLM не отвечают.
- Настройки поведения и триггеры лежат в `tenant.json` → `behavior` (`auto_reply`, `auto_reply_text`, `triggers`).
- Переключатели per-tenant:
  - `behavior.send_catalog_on_first_message` — отправлять ли PDF‑каталог первым сообщением в Telegram (по умолчанию `true`).
  - `behavior.avito_smart_reply_enabled` — разрешить смарт‑реплай (LLM) для Avito (по умолчанию `false`).

#### Авито → Telegram по номеру (подробно)
- Где включается: в UI «Поведение» поле «Текст для Telegram, если нашли номер в Avito» → сохраняется в `behavior.avito_phone_tg_template`. Старый вариант через `persona.meta.avito_phone_tg_template` остаётся как fallback.
- Как работает: воркер парсит номер из текста (`+7`/`7`/`8` → `+7XXXXXXXXXX`, 11 цифр), логирует `avito_phone_detected`, кладёт дедуп-ключ `avito:phone_tg_sent:<tenant>:<lead_id>` на `AVITO_PHONE_TG_TTL` (86400 по умолчанию). Без нового lead_id повторно не отправит, пока TTL не истёк.
- Отправка: вызывает tgworker `/send` с `phone`, добавляя заголовки `X-Auth-Token`/`X-Admin-Token`. Если Telegram не вернул peer (номер не зарегистрирован или недоступен), tgworker отвечает `peer_not_found` и воркер пишет `avito_phone_tg_fail status=404`.
- Диагностика: `avito_phone_detected`, `avito_phone_tg_sent`, `avito_phone_tg_skip` (dedup/empty_template), `avito_phone_tg_fail status=XXX body=...`. Tgworker выводит `event=tg_send_request … raw=...` в логах при вызове `/send`.
- Ручной сброс дедупа: `docker exec avio-redis-1 redis-cli DEL avito:phone_tg_sent:<tenant>:<lead_id>`.
- Сброс дедупа автоответа: `docker exec avio-redis-1 redis-cli DEL avito:auto_reply_sent:<tenant>:<lead_id>` (для dev-стенда используйте свой контейнер/команду `docker compose exec redis ...`).
- Ограничения Telegram: если номер не существует в Telegram или не резолвится через `ImportContacts`, отправка невозможна (получим `peer_not_found`). Обычные автоответы по известным peer/id не затрагиваются.

## Процесс разработки (dev → prod)
- Две ветки и два стенда:
  - `prod` → деплой в `/opt/avio` (прод).
  - `dev` → деплой в `/opt/avio-dev` (дев).
- Рабочий цикл:
  1) Фичи делаем в `dev` (или feature-ветка от `dev`), тестируем на дев-стенде: `docker compose -f docker-compose.yml -f docker-compose.override.test.yml up -d --build`.
  2) Готово к прод → `git checkout prod && git pull`; `git checkout dev && git pull`; merge `dev` → `prod` (исключая SPA файлы, если они не для прода). PR/merge приветствуется.
  3) Деплой прод: `/opt/avio`, `git checkout prod && git pull`, затем `docker compose up -d --build` (или ваш продовый пайплайн).
- Dev-override: в деве использовать `docker-compose.override.test.yml` + `.env.dev` (если есть) для портов/ENV, прод — без dev-override.

## Green contract: критичные проверки

Эти проверки добавлены после аудита тестов, потому что обычный `pytest` раньше часто был green, но реальные prod-сценарии ломались. Для критичных изменений green означает не только unit-тесты, а минимум набор ниже.

### Быстрый локальный green

```bash
.venv/bin/pytest -q
.venv/bin/ruff check --select E,F --ignore E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py
.venv/bin/flake8 --select=E,F --extend-ignore=E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py scripts/runtime_log_guard.py scripts/ui_http_smoke.py scripts/release_scope_guard.py
python scripts/monolith_guard.py
python scripts/test_truth_audit.py tests
python scripts/release_scope_guard.py --strict
python scripts/message_pipeline_smoke.py
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
```

`monolith_guard.py` проверяет два слоя: функции длиннее 80 строк и file-line budgets для бывших крупных entrypoint-файлов (`apps/worker/main.py`, `apps/api/main.py`, `apps/api/web/public.py`, `apps/api/web/client.py`, `apps/api/web/webhooks.py`, `apps/api/web/auth.py`, `apps/api/web/admin.py`). Эти файлы можно уменьшать, но нельзя снова раздувать.

### Tenant settings и Avito auth не должны слетать

Проверяется двумя слоями:

```bash
ADMIN_TOKEN="$ADMIN_TOKEN" python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3

PUBLIC_KEY="$PUBLIC_KEY" ADMIN_TOKEN="$ADMIN_TOKEN" \
  python scripts/critical_smoke.py \
    --base-url http://127.0.0.1:8000 \
    --tenants 1,3 \
    --mode test-tenant-write \
    --write-tenant 999999 \
    --public-key "$PUBLIC_KEY"
```

Для проверки переживания рестарта:

```bash
PUBLIC_KEY="$PUBLIC_KEY" \
  python scripts/restart_persistence_smoke.py \
    --base-url http://127.0.0.1:8000 \
    --tenant 999999 \
    --public-key "$PUBLIC_KEY" \
    --services app \
    --compose-file docker-compose.yml \
    --compose-file compose/ci/docker-compose.yml
```

Что должно сохраняться после save/restart: `passport`, `behavior.avito_smart_reply_enabled`, `behavior.brain_mode`, `integrations.avito.access_token`, `integrations.avito.refresh_token`, `integrations.avito.account_id`, `follow_up`.

### Avito OAuth state

Критичный тест: `tests/test_truth_critical_flows.py::test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss`.

Он проверяет, что callback принимает signed state даже после потери Redis state и сохраняет токены в tenant config. Если меняете OAuth, обязательно запускать:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_public_settings.py tests/test_avito_oauth.py -q
```

### Самообучение

Критичный тест: `tests/test_truth_critical_flows.py::test_learning_examples_from_db_reach_response_pipeline_prompt`.

Он проверяет реальный путь: DB-backed training example → `training_retriever` → examples block → `response_pipeline` system prompt. Unit-тесты learning остаются полезны, но сами по себе не доказывают, что обучение реально применяется в ответе.

Проверять после изменений в `libs/core/learning/*`, `libs/core/training/*`, `libs/core/response_pipeline.py`:

```bash
.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_learning_policy_v2.py tests/test_learning_feedback.py tests/test_learning_manager_capture_hooks.py -q
python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1
```

### Avito incoming → worker → outbox

Критичный unit/integration тест: `tests/test_truth_critical_flows.py::test_avito_incoming_worker_stores_message_and_enqueues_smart_reply`.

Критичный live-stack smoke через Redis и живой worker:

```bash
docker compose -f docker-compose.yml -f compose/ci/docker-compose.yml up -d app worker redis postgres

PUBLIC_KEY="$PUBLIC_KEY" \
  python scripts/inbox_worker_smoke.py \
    --base-url http://127.0.0.1:8000 \
    --tenant 999999 \
    --public-key "$PUBLIC_KEY" \
    --compose-file docker-compose.yml \
    --compose-file compose/ci/docker-compose.yml

python scripts/runtime_log_guard.py \
  --compose-file docker-compose.yml \
  --compose-file compose/ci/docker-compose.yml \
  --service app \
  --service worker \
  --tail 1200 \
  --outbox-disabled

docker compose up -d app worker
```

`compose/ci/docker-compose.yml` запускает worker с `OUTBOX_ENABLED=0`: worker должен обработать `inbox:message_in` и создать payload в `outbox:send`, но не отправлять его наружу. Важно: при `OUTBOX_ENABLED=0` outbox loop не должен потреблять очередь.
`runtime_log_guard.py` дополнительно проверяет, что после smoke в логах app/worker нет `unknown_tenant`, `invalid_state`, `missing_state`, traceback/unhandled exception и признаков потребления outbox при выключенной отправке.

### UI HTTP smoke

Минимальная проверка клиентского пути без браузера:

```bash
PUBLIC_KEY="$PUBLIC_KEY" \
  python scripts/ui_http_smoke.py \
    --base-url http://127.0.0.1:8000 \
    --tenant 999999 \
    --public-key "$PUBLIC_KEY"
```

Smoke проверяет, что открываются `/login`, `/register`, `/client/{tenant}/settings?k=...` и `/connect/avito?...`, а на страницах есть ключевые элементы форм, SPA bootstrap настроек и Avito authorize URL. Это не заменяет Playwright, но ловит самые грубые регрессии маршрутов и критичных entry points.

### CI compose smoke

CI поднимает `app worker mocktg redis` с `compose/ci/docker-compose.yml` и выполняет:

- `critical_smoke.py` readonly;
- `critical_smoke.py --mode test-tenant-write`;
- `ui_http_smoke.py`;
- `restart_persistence_smoke.py`;
- `inbox_worker_smoke.py`;
- `runtime_log_guard.py`;
- публичные TG smoke endpoints.

Если локально green, но CI падает именно в compose smoke, сначала смотреть `docker compose logs app worker redis postgres`.

### Prod readonly после деплоя

На prod нельзя использовать write-smoke на tenant `1` и `3`. После деплоя безопасный минимум:

```bash
ADMIN_TOKEN="$ADMIN_TOKEN" \
  python scripts/critical_smoke.py \
    --base-url https://avio.website \
    --tenants 1,3 \
    --mode readonly
```

Дополнительно вручную проверить статусы каналов в UI для tenant `1` и `3`: Avito auth connected/configured, learning enabled/apply mode, нет деградации `/internal/health/deep`.

## PUBLIC_KEY для фронта
- Публичные маршруты Telegram (`/pub/tg/*`) и WhatsApp (`/pub/wa/*`) принимают ключ через `?k=`. Для TG допускается ключ арендатора (tenant key) или `PUBLIC_KEY`, для WA — `PUBLIC_KEY`.
- Значение `PUBLIC_KEY` обязательно и должно отличаться от `ADMIN_TOKEN`, чтобы не давать фронту доступ к административным операциям.
- При отсутствии `PUBLIC_KEY` система временно принимает `ADMIN_TOKEN` как запасной вариант, но это режим совместимости и рекомендуется задать отдельный ключ для фронта как можно раньше.

## Ключи арендатора
- На каждого арендатора приходится ровно один ключ доступа (`1 tenant = 1 key`).
- `GET /admin/key/get?tenant=<ID>` возвращает существующий ключ либо создаёт новый и сразу помечает его основным.
- В админке больше нет кнопки «сделать основным»: текущий ключ единственный, чтобы получить новый, сначала удалите действующий.
- Повторные попытки создания или сохранения ключа возвращают `409 key_already_exists`.
- Ссылки для клиентов формируются в виде `/connect/wa?tenant={ID}&k={TENANT_KEY}` — значение `k` передаётся без кавычек.

### Дефолтная персона
- Базовый шаблон хранится в `libs/agents/persona_default_ru.md` и подставляется, если у арендатора нет собственного `persona.md`.
- Override на уровне арендатора находится в `data/tenants/<ID>/persona.md` и перекрывает дефолт после сохранения в клиентском кабинете.
- Плейсхолдеры: `{AGENT_NAME}`, `{BRAND}`, `{CITY}` берутся из паспорта бренда, `{CHANNEL}` — из фактического канала диалога (fallback `WhatsApp`), `{WHATSAPP_LINK}` и `{CATALOG_URL}` — из настроек арендатора (если пусто — подставляется пустая строка).
- `{CURRENCY}` всегда нормализуется в `₽`.
- В секции `meta` можно указать дополнительные артефакты:
- `meta.catalog_pdf_path` — относительный путь (от корня `data/tenants/<ID>/`) до PDF каталога. Этот файл автоматически подставляется при первом сообщении из WhatsApp/Telegram и при явном запросе клиента в любом канале.
  - `meta.catalog_csv_path` — путь к CSV, который должен использоваться каталожным поиском. При передаче CSV таким образом он имеет приоритет над конфигом `tenant.json`.
  - Пути должны быть внутри директории арендатора (`../` не допускается). Пример:
    ```yaml
    meta:
      catalog_pdf_path: "uploads/catalog.pdf"
      catalog_csv_path: "catalogs/catalog.csv"
    ```

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
```

Успешный ответ:
```json
{ "ok": true, "job_id": "<uuid>", "state": "queued" }
```

Что делает бэкенд:

- Сохраняет загруженный файл:
  ```text
  /data/tenants/<TENANT>/uploads/<safe_name>.pdf
  ```
- Создаёт CSV из PDF:
  ```text
  /data/tenants/<TENANT>/catalogs/<base_name>.csv
  ```
- Пишет статус джобы:
  ```text
  /data/tenants/<TENANT>/catalog_jobs/<job_id>/status.json
  ```
- Обновляет конфиг арендатора:
  ```text
  /data/tenants/<TENANT>/tenant.json → integrations.uploaded_catalog
  ```
  Поля: `path`, `original`, `uploaded_at`, `type`, `size`, `mime`, `csv_path`, `pipeline`, `index`

Публичные настройки для фронтенда:
```text
GET /pub/settings/get?k=<PUBLIC_KEY>&tenant=<TENANT>
```

Ответ содержит:
```json
{ "ok": true, "cfg": { "integrations": { "uploaded_catalog": { ... } }, ... } }
```

Фронтенд читает `cfg.integrations.uploaded_catalog`.
После загрузки PDF поле заполнится, а путь к CSV будет в `csv_path`.

Ошибки:
- `401 {"detail":"invalid_key"}` — неверный ключ
- `400 {"ok":false,"error":"empty_file"}` — пустой файл
- `400 {"ok":false,"error":"unsupported_type"}` — неподдерживаемое расширение
- `400 {"ok":false,"error":"file_too_large","max_size_bytes":...}`
- `422 {"ok":false,"error":"invalid_payload","reason":"invalid_tenant|missing_file"}`

Минимальный пример JS-загрузки:
```html
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
  });
})();
</script>
```

### Переменные окружения tgworker

| Переменная | Назначение |
|------------|------------|
| `TELEGRAM_API_ID` | идентификатор приложения Telegram |
| `TELEGRAM_API_HASH` | hash приложения Telegram |
| `PUBLIC_KEY` | публичный ключ для доступа к `/pub/tg/*` и `/pub/wa/*` |
| `ADMIN_TOKEN` | админ-токен для приватных RPC эндпоинтов |
| `APP_BASE_URL` | внешний URL API/приложения |
| `WORKER_BASE_URL` | базовый URL Telegram worker |
| `TGWORKER_BASE_URL` | алиас для обратной совместимости, использует `WORKER_BASE_URL` |
| `OUTBOX_ENABLED` | общий флаг разрешения исходящих сообщений |
| `OUTBOX_WHITELIST` | список разрешённых WhatsApp-получателей |
| `TG_SESSIONS_DIR` | каталог для хранения `.session` файлов (общий с `app`) |

Том сессий Telegram должен быть примонтирован к контейнерам `app` и `tgworker`, чтобы авторизация сохранялась между перезапусками.

## Dev → Prod мердж и деплой
- Фичи делаем в `dev` (или feature-ветке от `dev`). Проверяем на дев-стенде, прогоняем smoke/pytest.
- Перед выкладкой: `git checkout dev && git pull`, затем `git checkout prod && git pull`, после чего merge `dev` → `prod` (или PR). Dev-only файлы не тащим в прод.
- На прод-сервере `/opt/avio`: `git checkout prod && git pull`, затем `docker compose up -d --build`. Если есть миграции — выполнить их перед/после рестарта по чек-листу фичи.
- После деплоя проверяем health контейнеров, доступность API, критичные логи и ключевые канальные сценарии.

### Outbox worker guards

- `ADMIN_TOKEN` обязателен для RPC-запросов к `tgworker:/send` — воркер (`apps.worker.main`) всегда отправляет заголовок `X-Admin-Token`.
- Если `OUTBOX_ENABLED=false`, воркер только логирует задачу (`status=skipped reason=outbox_disabled`).
- `OUTBOX_WHITELIST` фильтрует получателей по ID, username и телефону; пустое значение или `*` означает `allow_all`.
- Перед отправкой воркер проверяет наличие лида в БД и, при отсутствии, помечает результат как `err:no_lead` без попытки доставки.

## Мультиарендный WhatsApp (waweb)

Чтобы каждый арендатор имел собственную сессию WhatsApp и не конфликтовал с остальными, используются отдельные контейнеры `waweb`. Управление вынесено в отдельный compose‑файл (`compose/waweb/docker-compose.yml`) и утилиту `scripts/waweb_manage.py`.

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

- Основной `app` получает URL waweb через `core.tenant_waweb_url()`, поэтому после изменения конфига требуется `docker compose restart app`.

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

Утилита сама передаёт в контейнеры секрет `WA_WEB_TOKEN` (и совместимый `WA_INTERNAL_TOKEN`), поэтому достаточно задать его один раз в `.env` — все арендаторы будут использовать согласованный токен без ручной синхронизации.

Команды выполняются через `docker compose -f compose/waweb/docker-compose.yml`, поэтому требуется сеть `avio_default` (создаётся основной `docker-compose.yml`).

## Умный ответ (Smart Reply)

- История контакта и последний CTA теперь запоминаются вместе с отпечатками уточняющих вопросов. Повтор одного и того же вопроса или призыва исключается, а повторный CTA допускается только спустя `CTA_COOLDOWN_SECONDS` (по умолчанию 180 с) и при нейтральном/положительном тоне беседы.
- Планировщик (LLM) работает в два этапа: сначала строится план, затем формируется ответ. Оба шага имеют таймаут; при задержке или ошибке система переходит на прямой `chat.completions`, далее — на rule-based fallback.
- При загрузке persona.md учитываются подсказки по максимально допустимому количеству вопросов (`max_questions`) и дружелюбному стилю — это влияет и на rule-based, и на LLM-ответы.
- Валидация плана следит за тем, чтобы вопросы не предлагали смену канала (например, «где удобнее общаться?») в уже выбранном WhatsApp/Telegram.

### Авторассылка каталога

- При первом сообщении в WhatsApp/Telegram бот отправляет PDF из `meta.catalog_pdf_path` (если он задан и каталог ещё не высылался). Повторные запросы «каталог», «pdf», «прайс» в любом канале немедленно сбрасывают TTL и пересылают документ заново.
- Telegram использует те же вложения, что и WhatsApp: в очередь подаётся сообщение с `attachments` и `telegram_user_id`/`peer`, дальнейшую доставку осуществляет воркер.
- Перед отправкой воркер скачивает `/internal/tenant/<TENANT>/catalog-file` с заголовком `X-Auth-Token: ${WA_WEB_TOKEN}` (alias `WA_INTERNAL_TOKEN`). Если приложение отвечает `401`/`403`, выполняется повторная попытка с `X-Internal-Token: ${WA_WEB_TOKEN}`. Сам маршрут также принимает `X-Admin-Token: ${ADMIN_TOKEN}`, `Authorization: Bearer ${ADMIN_TOKEN}` и `X-Webhook-Token: ${WEBHOOK_SECRET}`.
- Готовый документ отправляется через `POST http://<waweb-host>:9001/send` c заголовком `X-Auth-Token: ${WA_WEB_TOKEN}`; конкретный host берётся из `config/tenants.yml`.
- Для проверки доступа:
  - `curl -I -H "X-Auth-Token: $WA_WEB_TOKEN" "http://app:8000/internal/tenant/1/catalog-file?path=uploads/catalog.pdf"` → `200 OK`.
  - `curl -I "http://app:8000/internal/tenant/1/catalog-file?path=uploads/catalog.pdf"` → `403 Forbidden`.
- Если у арендатора задан `meta.catalog_csv_path`, поисковый движок каталога берёт данные прямо из этого файла — достаточно обновить CSV и перезапустить индекс.

### Тестирование

- Быстрая проверка новой логики:
  ```bash
  pytest tests/test_sales_engine.py tests/test_brain_quality.py
  ```
  Эти тесты покрывают CTA-кулдаун, хранение вопросных отпечатков, инициализацию каталогов из persona и фильтрацию вопросов планировщика.
- Для полного прогона используйте `pytest` в корне репозитория (потребуется больше времени).

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


- Выполните `make migrate`, чтобы применить Alembic-миграции и вывести структуру таблиц `leads`, `messages` и список колонок `contacts`. Перед запуском установите переменную окружения `DATABASE_URL`.

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

Единый транспортный контракт использует три уровня:

- **TransportMessage** — исходящее сообщение для `POST /send`.
- **MessageIn** — сырое входящее событие транспорта (`/webhook`, `/webhook/telegram`, amoCRM webhook и т.д.).
- **NormalizedMessage / envelope** — внутренний канонический формат из `libs/core/message_envelope.py`, который используют worker, webhook-и и amoCRM chat sync.

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

`channel` выбирает транспорт: `telegram` → `tgworker:/send`, `whatsapp` → соответствующий WA transport, `avito`/`max` — через worker integration layer. Алиас `to="me"` отправляет сообщение в сохранённые сообщения Telegram-аккаунта. Ответы транспорта приводятся к формату `{ "ok": true }` либо `{ "ok": false, "error": "..." }`.

## Outbox: отправка
- `POST /send` умеет отправлять сообщения напрямую в transport layer.
- Для WhatsApp есть два режима:
  - `SEND_STRATEGY=direct` — прямой transport POST;
  - `SEND_STRATEGY=redis` — постановка в outbox очередь.
- `OUTBOX_ENABLED` по умолчанию считается включённым, если env не задан; чтобы явно выключить исходящие, используйте `0|false|no|off|disabled`.
- `OUTBOX_WHITELIST` ограничивает исходящие WhatsApp-получатели; пустое значение или `*` означает `allow_all`.

## Наблюдения и технические заметки (октябрь 2025)

- **LLM и промпты.** Ключи `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_TEMPERATURE` задаются через `.env`. Точные runtime-параметры зависят от конкретной ветки вызова (`ask_llm`, semantic pass, planner, fallback), поэтому README не фиксирует одну общую комбинацию `max_tokens/penalty` на весь проект.
- **Каталоги арендаторов.** Активный `TENANTS_DIR` — `data/tenants`. Все редакции `tenant.json`, `persona.md`, uploads и generated artifacts живут там.
- **Правила каталога и нужд.** За бизнес-ограничения отвечают persona/meta и sales-policy слой:
  - `needs_mapping` — таблица ключевых слов/регулярок → значение нужды (`object_type`, `service`, `room` и т.д.). Пример:
    ```yaml
    needs_mapping:
      object_type:
        house:
          keywords: ["дом","частн"]
        apartment:
          keywords: ["кварт","подъезд"]
    ```
  - `catalog_tags`/`catalog_attributes` — декларативные правила, которые при загрузке CSV добавляют тег/атрибут позиции. Формат:
    ```yaml
    catalog_tags:
      - name: house_ready
        any:
          - field: "title"
            contains: ["термо","арктик"]
        tags: ["house_ready","thermo"]
        set:
          object_type: "house"
    ```
  - `sales_rules` — фильтрация перед ответом LLM. Пример:
    ```yaml
    sales_rules:
      - needs: {object_type: "house"}
        require_tags: ["house_ready"]
        forbid_tags: ["apartment_only"]
    ```
  - Эти блоки работают для любых категорий (не только двери). После сохранения `persona.md` перезапуск обычно не нужен: persona и hints перечитываются динамически.
- **Локальная проверка диалогов.** Команда `test` (обёртка над `scripts/chat_simulator.py`) работает из `.venv`. Полезные параметры: `--tenant`, `--contact`, `--channel`, `--reset`, `--show-messages`. Внутри сессии команда `reset` очищает состояние текущего контакта.
- **Состояния диалогов.** Redis-хранилище (`sales_state:<tenant>:<contact>`) монтируется на хост в `data/redis`. Для ручного сброса:
  ```bash
  docker-compose exec -T redis redis-cli keys 'sales_state:1:*'
  docker-compose exec -T redis redis-cli del sales_state:1:<CONTACT_ID>
  ```
  либо из Python:
  ```bash
  .venv/bin/python - <<'PY'
  from libs.core import sales_core as core
  core.reset_sales_state(tenant=1, contact_id=<CONTACT_ID>)
  PY
  ```
- **Связка каналов.** `resolve_or_create_contact` ищет существующий контакт по `whatsapp_phone`, `avito_user_id`, `avito_login`, `telegram_user_id`. Если при переходе с Avito на WhatsApp передавать `leadId` от авито-чата или заранее сохранять номер телефона, бот продолжит диалог в рамках одного контакта.
- **Персонализация.** Плейсхолдеры `{BRAND}`, `{AGENT_NAME}`, `{CITY}` и др. берутся из `tenant.json`. Если нужные данные не заполнены, часть плейсхолдеров может остаться пустой — это надо учитывать при сборке quickstart/persona.

## Avito Messenger Интеграция

### OAuth и токены
- Ссылка авторизации (страница `/connect/avito`) формируется со scope из `AVITO_SCOPE`; по умолчанию используется `messenger:read,messenger:write,user:read`.
- После успешной авторизации в `data/tenants/<ID>/tenant.json` автоматически сохраняются `access_token`, `refresh_token`, `expires_at`, `account_id`, `account_login`.
- При OAuth-callback UI-конфиг дополняется `behavior.auto_reply=true` и `behavior.auto_reply_enabled=true`, но фактический автоответ всё равно зависит от текущих tenant-настроек.
- Token refresh выполняется автоматически воркером при каждом запросе; при 401 выполняется повторный обмен по `refresh_token`.

### Webhook
- Avito требует активировать Messenger API в кабинете разработчика (подтверждение партнёра).
- Автоматическая регистрация: после OAuth и при нажатии «Обновить статус» вызывается `POST https://api.avito.ru/messenger/v3/webhook`, а URL берётся из текущего публичного домена приложения через `public_url(..., "/webhook/avito")`.
- Если маршрут ещё недоступен (Avito возвращает 404), webhook можно зарегистрировать вручную:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v3/webhook \
    -H "Authorization: Bearer <ACCESS_TOKEN>" \
    -H "Content-Type: application/json" \
    -d '{"url":"https://<PUBLIC_BASE_URL>/webhook/avito","types":["messages"]}'
  ```
- Проверка текущих подписок:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v1/subscriptions \
    -H "Authorization: Bearer <ACCESS_TOKEN>"
  ```
- Снять подписку:
  ```bash
  curl -X POST https://api.avito.ru/messenger/v1/webhook/unsubscribe \
    -H "Authorization: Bearer <ACCESS_TOKEN>" \
    -H "Content-Type: application/json" \
    -d '{"url":"https://<PUBLIC_BASE_URL>/webhook/avito"}'
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
- Мы создаём стабильный `lead_id` из пары `account_id + chat_id` и обновляем поле `peer`.

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
curl -X POST https://api.avito.ru/messenger/v3/webhook \
  -H "Authorization: Bearer $AT" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://<PUBLIC_BASE_URL>/webhook/avito","types":["messages"]}'

# список подписок
curl -X POST https://api.avito.ru/messenger/v1/subscriptions \
  -H "Authorization: Bearer $AT"

# отписка
curl -X POST https://api.avito.ru/messenger/v1/webhook/unsubscribe \
  -H "Authorization: Bearer $AT" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://<PUBLIC_BASE_URL>/webhook/avito"}'
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
curl -X POST https://<PUBLIC_BASE_URL>/webhook/avito \
  -H 'Content-Type: application/json' \
  -d '{"id":"evt-1","timestamp":"2024-01-01T00:00:00Z","payload":{"type":"message","value":{"account_id":400040070,"chat_id":"987654","type":"text","content":{"text":"Здравствуйте"},"author_id":123456,"published_at":"2024-01-01T00:00:00Z"}}}'
```

После регистрации webhook входящие события начинают попадать в Avio; сам автоответ дополнительно зависит от tenant-настроек `behavior.auto_reply` и `behavior.auto_reply_text`.

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
- Токен хранится не только в БД, но и на диске: `data/tenants/<TENANT>/provider_token.json`. Legacy-файл `provider_token.txt` мигрируется автоматически. При отсутствии PostgreSQL приложение читает/создаёт файл автоматически, поэтому **важно монтировать общий каталог `data/tenants` для `app`, `worker` и `waweb`**.
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
- `OUTBOX_WHITELIST` может содержать `*` либо список разрешённых номеров (`+7999…`, `7999…`, `7999…@c.us`). Пустое значение сейчас трактуется как `allow_all`.
- При недоступной БД воркер всё равно сгенерирует автоответ: lead_id берётся из номера отправителя, а проверка `lead_exists` переводится в предупреждение вместо жёсткого отказа. Поэтому записи вида `event=send_result status=warning reason=err:no_lead` допустимы при «офлайн»-режиме — сообщение всё равно ставится в очередь `outbox:send`.
- Проверьте, что WA token актуален: `curl -H "X-Admin-Token: ${ADMIN_TOKEN}" http://app:8000/admin/provider-token/<TENANT>` → ответ `{"ok": true, ...}`. Если `500`, проверьте mount `data/tenants` и перезапустите `app`, чтобы пересоздался `provider_token.json`.
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

- `make -f tools/Makefile diag` — запускает основной скрипт `scripts/diag.sh` с минимальным выводом (передайте `AVIO_URL` и `ADMIN_TOKEN`).
- `make -f tools/Makefile diag-verbose` — тот же скрипт, но с расширенным логированием (`DIAG_VERBOSE=1`).
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

## Канальные правила и каталоги

### WhatsApp
- Для WhatsApp каталог по умолчанию отправляется как публичная ссылка (`Каталог: <url>`), а не inline PDF.
- Viewer URL берётся из `integrations.catalog_url`, затем `passport.catalog_url`, затем `persona.meta.catalog_url`; если ничего не задано, используется fallback `/pub/catalog/file/<tenant>?v=<mtime>`.
- Постраничная отправка каталога сейчас принудительно отключена.
- Групповые чаты (`@g.us`) игнорируются: событие логируется как
  ```
  stage=incoming_skip_group ch=whatsapp tenant=<id> message_id=...@g.us...
  ```
  лиды, сообщения и ответы не создаются.
- Если ссылка приходит локальная (`http://app:8000/...`), проверьте `APP_BASE_URL` / `PUBLIC_BASE_URL` и tenant-config.

### Avito
- Автоматическая отправка каталога в Avito отключена: даже при forced catalog flow сам документ не уходит.
- Переключатель `behavior.avito_smart_reply_enabled` отдельно управляет smart reply в Avito.
- Для перевода клиента из Avito в Telegram используется `behavior.avito_phone_tg_template`.

### Telegram
- Telegram умеет отправлять каталог файлом, а не только ссылкой.
- Если файл слишком большой или нет корректного attachment, используется публичный fallback URL.
- Медиа и PDF дедуплицируются по ключу вложения.

### Настройка tenant.json
- `integrations.catalog_url` — основная viewer-ссылка.
- `behavior`:
  - `always_full_catalog` — участвует в catalog flow.
  - `send_catalog_as_pages` — сейчас хранится в конфиге, но постраничная отправка фактически отключена.
  - `brain_mode` — `smart` или `classic`.
  - `send_catalog_on_first_message`, `auto_photo_enabled`, `auto_photo_max`, `telegram_reply_enabled`, `max_reply_enabled`, `avito_smart_reply_enabled`.
  - `max_clarifying_questions`, `tone` и прочее остаются без изменений.

Пример фрагмента:
```json
{
  "integrations": {
    "catalog_url": "https://api.avio.website/pub/catalog/file/1",
    "uploaded_catalog": { ... }
  },
  "behavior": {
    "always_full_catalog": true,
    "send_catalog_as_pages": true,
    ...
  }
}
```

## Связка Avito ↔ Telegram и уведомления (AvioAlarm)
- Авито номер → ТГ: при детекте телефона в авито-сообщении воркер обновляет/создаёт контакт по номеру, линкует lead и кладёт номер в Redis (`cache:lead_phone:<tenant>:<lead_id>` и `cache:avito_phone:<tenant>:<chat_id>`, TTL 7 дней). Далее отправляет автоответ в ТГ через tgworker по этому номеру. При успешной отправке tgworker возвращает `peer_id`, воркер кеширует связку `peer_id → phone` в `cache:avito_phone` — это нужно для последующих входящих из ТГ.
- Входящий Telegram: после резолва lead воркер ищет телефон в БД; если нет — берёт из `cache:lead_phone` по lead_id, затем из `cache:avito_phone` по peer. Глобальный `last_phone` не используется. Если сообщение пришло от уведомительного бота, телефон не линкуется. Найденный телефон связывает существующий контакт либо создаёт новый и перелинкует lead на него, обновляя `telegram_user_id`/`username`.
- Уведомления handoff: при handoff (фото или явный вызов) воркер шлёт уведомление через бота, заданного `NOTIFY_BOT_TOKEN`, в настроенные chat ids. Формат: «Лид <телефон/username/peer> - <причина>». Ссылка на админку в тексте не добавляется.
- Диагностика: ключевые логи — `avito_phone_detected`, `avito_phone_tg_sent`, `telegram_contact_linked_by_phone`/`relinked_by_phone`, `notify_prepare`/`notify_send_success`. Убедитесь, что tgworker в статусе authorized, иначе вызовы `/send` вернут `authkey_unregistered`/`not_authorized`.
- Команда для получения `chat_id`:
  ```bash
  curl -s "https://api.telegram.org/bot${NOTIFY_BOT_TOKEN}/getUpdates"
  ```

## Очистка диска (runbook)
Ниже список безопасных действий для освобождения места. Выполнять от root.

1) Диагностика:
```bash
df -hT
du -xh / --max-depth=1 | sort -h
du -xh / --max-depth=2 | sort -h | tail -n 50
```

2) Docker (неиспользуемые образы/контейнеры/volume):
```bash
docker image prune -a -f
docker container prune -f
docker volume prune -f
```

3) Journald (уменьшить журналы):
```bash
journalctl --vacuum-time=7d
# или
journalctl --vacuum-size=200M
```

4) Архивные логи и кэши:
```bash
rm -f /var/log/*.gz /var/log/*.[0-9]
apt-get clean
rm -rf /var/cache/apt/archives/*
```

5) Пользовательские кэши (если не нужны):
```bash
rm -rf /home/deploy/.npm/_cacache
rm -rf /home/deploy/.cache
rm -rf /home/deploy/.vscode-server
```

6) Проверка "удаленные, но открытые" файлы:
```bash
lsof +L1 | awk '{print $7, $9}' | sort -n | tail -n 20
```

## Healthcheck
- Скрипт `avio-healthcheck.sh` теперь только логирует сбои (`/var/log/avio-healthcheck.log`) и не перезапускает контейнеры при ошибке внешнего `/health`.
- Cron-запись: `*/5 * * * * /usr/local/bin/avio-healthcheck.sh >/dev/null 2>&1`.
- Если увидели перезапуски каждые 5 минут, убедитесь, что никакие другие таймеры (`avio-autopull`, `avio-autopush`) не включены, и что cron-скрипт действительно в новой версии.
