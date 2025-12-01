Телеграм-диагностика (tgworker)
===============================

Цель: понять, почему собственные сообщения аккаунта, подключённого к tgworker, приезжают как входящие (out=0) и не ставят handoff.

Что сделать для воспроизведения
-------------------------------
1. Перезапустить `tgworker` с текущим кодом (диагностические логи уже включены).
2. В Telegram открыть личный чат, где воспроизводится проблема, и отправить с телефона два сообщения: `DIAG_SELF_1` и `DIAG_SELF_2`.
3. Собрать логи `tgworker` и `app`:
   - `docker logs -f avio-tgworker-1 | grep -E "tg_diag|stage=manager_detect"`
   - `docker logs -f avio-app-1 | grep manager_diag`

Что смотрим в логах
-------------------
- `tg_diag:self_identity` — какой `self_id` вернул Telethon при старте.
- `tg_diag:new_message` — все личные сообщения, поля `out`, `sender_id`, `peer_id`, `self_id`.
- `tg_diag:outgoing_handler` — какие сообщения попадают в обработчик outgoing.
- `tg_diag:raw_update` — сырые обновления (UpdateShortMessage/UpdateNewMessage), поля `out`, `user_id/from_id`, `peer_id`.
- `stage=manager_detect` — решает, менеджерское ли сообщение, показывает `sender_id`, `self`, `manager`, `peer_id`.
- `manager_diag` (app) — что пришло в вебхук: ключи `manager/out/origin` и поля сообщения.

Далее по этим логам сравниваем, откуда берутся `out=0` и `sender_id != self` для собственных сообщений, и какой сигнал можно использовать для handoff.

