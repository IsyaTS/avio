Telegram handoff (менеджерский режим)
======================================

Что должно происходить
----------------------
- Любое сообщение, отправленное вручную с подключённого к tgworker Telegram-аккаунта, помечается как `manager=true, out=true` и ставит флаг тишины (handoff) для связки `tenant + lead_id`.
- Пока флаг тишины установлен, smart_reply/LLM не отвечает в этот чат. Сообщения руками продолжают проходить.
- Тишина действует точечно на конкретный чат (lead_id). В других чатах автоответы продолжают работать.
- Тишина не снимается автоматически (авторазморозка по входящему от лида отключена). Снимать её нужно вручную.

Как работает
------------
- tgworker:
  - Telethon отдаёт исходящие сообщения с `out=True`, `sender_id=self_id`; tgworker проставляет `manager=true, out=true` и `origin=telegram:manager` в вебхук.
  - Webhook отправляется в `app` с этими флагами.
- app (`/webhook/telegram`):
  - Читает верхнеуровневые `manager/out`, а также `message.manager/message.out`.
  - Если `manager_flag` → записывает `manager`/`out` в `normalized_event` для worker и сразу отвечает `{"handoff": true}` без smart_reply.
- worker:
  - При получении события с `manager=true` ставит Redis-ключ `handoff:silence:<tenant>:<lead_id>`.
  - При последующих входящих в этот чат пишет `event=smart_reply_silenced` и не отвечает.

Как снять тишину вручную
------------------------
Удалить Redis-ключ:
```
docker compose exec redis redis-cli del handoff:silence:<tenant>:<lead_id>
```
Например, для tenant=2 и lead_id=1694250181:
```
docker compose exec redis redis-cli del handoff:silence:2:1694250181
```

Диагностика
-----------
- tgworker логи: `tg_diag:new_message`, `stage=manager_detect`, `webhook_request` показывают `out`/`sender_id`/`manager`.
- app логи: `manager_diag_raw` и `manager_diag` показывают, дошли ли `manager/out` во входящем вебхуке.
- worker логи: `event=smart_reply_silenced` означают, что чат в тишине.

Важно
-----
- Поведение handoff сейчас без авторазморозки. Если нужно вернуть авторазморозку (включать smart_reply после ответа лида), придётся изменить логику в worker и app.

Фото = автоматический handoff
-----------------------------
- `/webhook/telegram` теперь парсит `message.provider_raw`/`media`/`photo` (в т.ч. Telethon `MessageMediaPhoto`) и ставит `has_photo=True`, даже если в attachments пусто.
- Если `has_photo=True`, API сразу ставит флаг тишины в Redis и отвечает `{"handoff": true}` без smart_reply.
- Ключ в Redis: `handoff:silence:<tenant>:<lead_id>`, TTL = `HANDOFF_SILENCE_TTL_SECONDS` (по умолчанию 86400; читается из `.env`).
- Логи для фото: в app `webhook_photo_probe ... has_photo_initial=1 ...`; в worker `event=handoff_marked ... reason=photo_received`, затем при входящих `event=smart_reply_silenced`.

Как снять тишину вручную (фото/manager)
---------------------------------------
Удалить ключ:
```
docker compose exec redis redis-cli del handoff:silence:<tenant>:<lead_id>
```
Проверить TTL:
```
docker compose exec redis redis-cli ttl handoff:silence:<tenant>:<lead_id>
```
В k8s/других окружениях ключ тот же, меняется только способ доступа к Redis.

Каталог vs тишина
-----------------
- Любое вложение (включая фото) может запустить catalog_flow: отправка каталога + кэш `catalog:sent:<tenant>:tg:<peer>` с TTL `STATE_TTL_SECONDS` (по умолчанию 600 или значение из `.env`).
- Этот кэш влияет только на повторную отправку каталога. Тишину smart_reply даёт именно `handoff:silence:*`.
- Если после фото нужно вернуть автоответы, чистите `handoff:silence:<tenant>:<lead_id>`. Кэш каталога можно удалять отдельно при необходимости.

Где менять поведение
--------------------
- Детект фото и установка handoff: `apps/api/web/webhooks.py` (`has_photo`, `handoff_silence_key`).
- Проверка/установка тишины на воркере: `apps/worker/main.py` (`_mark_handoff_silence`, `_is_handoff_silenced`).
- TTL для тишины: переменная `HANDOFF_SILENCE_TTL_SECONDS` в `.env`.
