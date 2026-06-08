# MAX Personal Transport

## Что реализовано
- Отдельный канал `max_personal` (не смешивается с текущим `max` Bot API).
- Отдельный сервис `apps/maxworker/` (control/data plane).
- QR flow, статусная модель сессии, outbound/inbound private text.
- Self-echo suppression, manager-outgoing detection, dedupe.
- Reconnect/stale handling, per-tenant state, feature flags и kill switch.
- Интеграция в API/webhook/worker/runtime и клиентский портал.

## Состояния сессии
- `idle`
- `waiting_qr`
- `authorizing`
- `authorized`
- `stale`
- `reauth_required`
- `disconnected`
- `error`

## Включение per-tenant
1. В ЛК открыть вкладку `Каналы` и блок `MAX Personal`.
2. Нажать `Подключить` (запускает `/v1/max-personal/connect`).
3. Сканировать QR.
4. Проверить статус (`Подключено`).

В tenant-конфиге используется секция:
`integrations.max_personal`.

## API ручки
- `GET /v1/max-personal/status`
- `POST /v1/max-personal/connect`
- `POST /v1/max-personal/session/start`
- `GET /v1/max-personal/session/qr`
- `POST /v1/max-personal/session/logout`
- `POST /v1/max-personal/disconnect`
- `POST /v1/max-personal/send`

## maxworker внутренние ручки
- `POST /session/start`
- `GET /session/qr`
- `GET /session/status`
- `POST /session/logout`
- `POST /send`
- `POST /events/inbound`
- `GET /health`
- `GET /metrics`

`/events/inbound` принимает нормализованные transport-события и разделяет:
- клиентский inbound (`kind=inbound`);
- self echo (подавляется);
- manager outgoing (`kind=manager_outgoing`), чтобы корректно срабатывал handoff/аналитика.

## Shadow/risky флаги и rollback
- Глобальный kill switch: `MAX_PERSONAL_KILL_SWITCH=1`
- Глобальный запрет outbound: `MAX_PERSONAL_OUTBOUND_DISABLED=1`
- URL web-клиента MAX: `MAX_PERSONAL_WEB_URL` (по умолчанию `https://max.ru/web`)
- Пер-тенант отключение канала: `integrations.max_personal.enabled=false`
- Пер-тенант отключение outbound: `integrations.max_personal.outbound_enabled=false`

Для быстрого отката:
1. Выключить глобально `MAX_PERSONAL_KILL_SWITCH=1`.
2. Или выключить tenant в `/v1/max-personal/disconnect`.

## Ограничения текущего MVP
- Только private text.
- Без групп/медиа/voice/video.
- В dev по умолчанию включён `MAX_PERSONAL_MOCK=1`.
