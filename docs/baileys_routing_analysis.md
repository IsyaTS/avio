# Baileys routing analysis

Анализ делился на два слоя: Node‑сервис `wabaileys`, который держит Baileys‑сокет, и Python‑приложение Avio, которое получает вебхуки и ставит задачи воркеру.

## Кодовые точки (Step 1)
- **Создание сокета и управление сессиями**: `apps/wabaileys/src/session-manager.js`, функция `_createSession` внутри класса `SessionManager` (строки 124‑176) вызывает `makeWASocket` из `@whiskeysockets/baileys`.
- **Обработка входящих сообщений**: в том же файле `session-manager.js` подписка `socket.ev.on('messages.upsert', …)` (строки 232‑251) вызывает `_handleMessages`, затем `_normalizeIncomingMessage`.
- **HTTP API Baileys‑сервиса**: `apps/wabaileys/src/server.js`. Здесь описаны `/sessions/*`, `/messages/send` и др. Для отправки используется `SessionManager.sendMessage`.
- **Python‑слой, принимающий вебхуки**: `apps/api/web/webhooks.py`, эндпоинт `@router.post("/webhook")` → `provider_webhook`.
- **Python‑слой, который инициирует отправку**: REST `/send` в `app/main.py:504+` и воркер `app/worker.py` (функции `send_whatsapp_baileys`, `do_send`).

## INBOUND (Step 2)
### На стороне Baileys
- `_normalizeIncomingMessage` берет идентификаторы из:
  - `msg.key.remoteJid` → `remoteJid`. Это строка вроде `79273328311@s.whatsapp.net` или `41245158195227@lid`.
  - Для групп учитывается `msg.key.participant`.
  - Для собственного аккаунта берется `session.selfJid` (кешируется из `socket.user` или `auth.state.creds.me`).
- Итоговый payload (строки 253‑308):
  - `from` = цифры из `senderJid.replace(/\D/g, '')`.
  - `from_jid` и `from_raw` = исходный `senderJid` (включая `@lid`).
  - `to` = `selfJid` (для личных чатов) или `remoteJid` (для групп).
  - `tenant`, `message_id`, `text`, `media`, `provider_raw`.
- `_handleMessages` отправляет этот payload через `_sendWebhookEvent`, который использует `apps/wabaileys/src/webhook-client.js`. Клиент вызывает `POST {APP_WEBHOOK_URL}?token=provider_token`.

### На стороне Python
- Эндпоинт `provider_webhook` (`apps/api/web/webhooks.py:1235+`) проверяет токен и тип события.
- Для `event == "messages.incoming"` вызывается `_normalize_whatsapp_incoming` (строки 944‑1016):
  - `sender_raw` берется из `payload["from"]` / `from_jid` / `from_raw`.
  - `sender_digits = _digits(sender_str)` — удаляет все нецифровые символы, поэтому для `41245158195227@lid` остается `41245158195227`.
  - `sender_jid`: если строка заканчивается на `@c.us`, `@s.whatsapp.net`, `@lid` или `@g.us`, она сохраняется как есть. Иначе добавляется `@c.us`.
  - В итоговом JSON, который уходит дальше, сохраняются оба поля: `"from"` (цифры) и `"from_jid"` (полный JID).
- После нормализации событие сохраняется в БД/Redis (`insert_webhook_event`, `_queue_incoming_event`). Дополнительно `_remember_whatsapp_jid` (строки 175‑208) кладет `from_jid` в Redis‑хеш `wa:jid:{tenant}[lead_id]`. Именно там запоминается `41245158195227@lid`.

**Вывод по inbound:** единственный идентификатор, который отдает Baileys, — `remoteJid`. Если WhatsApp прислал `@lid`, именно он идет дальше по стэку и сохраняется как `from_jid`/lead id. Реальный MSISDN не передается.

## OUTBOUND (Step 3)
### Python → Baileys
- Сообщения попадают в воркер через Redis (`OUTBOX_QUEUE_KEY`). В `app/worker.py:2085-2345` функция `do_send` подготавливает `recipient_value`:
  1. Если в задаче есть `to_jid` (из `_queue_text_reply` или из кэша `wa:jid`), то `recipient_value = to_jid`.
  2. Иначе, если кэш `wa:jid` вернул запись по lead_id, используется она.
  3. В противном случае берется “телефон” (`raw_to`, цифры) и позже нормализуется до `@s.whatsapp.net`.
- Для провайдера `baileys` вызывается `send_whatsapp_baileys` (`app/worker.py:1739-1797`):
  - Строится payload `{"tenant": ..., "to": recipient}`.
  - Если строка `recipient` уже содержит `@`, она передается как есть (может быть `@lid`, `@s.whatsapp.net`, `@g.us` и т.д.).
  - Иначе вызывается `normalize_whatsapp_recipient` для получения `digits@s.whatsapp.net`.
- REST‑вызов идет на `POST {BAILEYS_URL}/messages/send`.

### Baileys‑сервис → WhatsApp
- В `apps/wabaileys/src/server.js` эндпоинт `/messages/send` (строки 57‑115) разбирает JSON, вызывает `SessionManager.sendMessage`.
- `sendMessage` (строки 364‑414):
  - `jid = normalizeJid(request.to)`.
  - `normalizeJid` (строки 481‑534) пропускает любые строки, которые уже заканчиваются на `@s.whatsapp.net`, `@c.us`, `@g.us` или `@lid`.
  - Если `to` — просто цифры, добавляется `@s.whatsapp.net`.
- В итоге `session.socket.sendMessage` вызывается с тем JID, который пришел от Python (LID или обычный JID).

## Почему видим `41245158195227@lid`
- В режиме multi-device WhatsApp выдаёт **Long-lived ID (LID)** вместо MSISDN. Его формат `<digits>@lid`.
- Baileys возвращает ровно этот LID в `msg.key.remoteJid`. У нас он сохраняется:
  - в вебхуке (`from_jid` = `41245158195227@lid`);
  - в Redis‑кэше `wa:jid:{tenant}[lead_id]`;
  - в outbox‑записях (поле `to_jid`).
- Когда бот отвечает, воркер вытаскивает `to_jid` и передает его обратно в Baileys. Тот вызывает `sendMessage` с `to = 41245158195227@lid`. WhatsApp по этому LID знает, как доставить сообщение реальному контакту `+7 927 332‑83‑11`.
- Мы нигде не конвертируем LID обратно в MSISDN — WhatsApp просто не дает этой информации. Поэтому в логах и в UI номер лида теперь выглядит как `41245158195227`, хотя фактически это “внешний ID контакта”.

## Резюме
1. **INBOUND:** Baileys → `_normalizeIncomingMessage` → Webhook → `_normalize_whatsapp_incoming`. Идентификатор чата = `remoteJid` (`...@lid`), он передается как `from_jid` и кешируется в `wa:jid`.
2. **OUTBOUND:** Воркер берет `to_jid` (LID) из входящего события/кэша, отправляет его в `send_whatsapp_baileys`. Baileys не модифицирует JID, просто вызывает `socket.sendMessage(to_jid, …)`.
3. **Почему `41245158195227`:** это LID, который WhatsApp назначил контакту `+7 927 332‑83‑11`. Baileys никогда не сообщает реальный номер, поэтому система логично оперирует LID. Ответ уходит по этому же LID и достигает нужного абонента.

Логика отправки/приема не нуждается в правках: бот уже отвечает тому, кто написал, просто идентификаторы выглядят иначе после перехода на Baileys.
