# Telegram (QR)

## Требования

- Аккаунт Telegram с доступом к [my.telegram.org](https://my.telegram.org) для получения `api_id` и `api_hash`.
- Открытый доступ tgworker к внутреннему API Avio (`APP_INTERNAL_URL`).
- Общий том для хранения сессий: `${TG_SESSIONS_DIR}` (по умолчанию `/app/tg_sessions`).

## Переменные окружения

| Переменная | Описание |
|------------|----------|
| `TELEGRAM_API_ID` | Идентификатор приложения Telegram. |
| `TELEGRAM_API_HASH` | Хэш приложения Telegram. |
| `TG_SESSIONS_DIR` | Путь до каталога с `.session` файлами (общий том для `app` и `tgworker`). |

## Порядок подключения

1. Добавьте `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TG_SESSIONS_DIR` в `.env` и `ops/.env`.
2. Пересоберите контейнеры `app`, `worker`, `tgworker` (`docker compose build app worker tgworker`).
3. Откройте `/connect/tg?tenant={id}&k={ключ}` и дождитесь статуса `waiting_qr`.
4. Отсканируйте QR в приложении Telegram → статус сменится на `authorized`.
5. При необходимости выполните `POST /pub/tg/logout` (через UI или API), чтобы сбросить сессию.

## Ограничения и поведение

- Сессии сохраняются в `.session` файлах (`SQLiteSession`). Один контейнер `tgworker` обслуживает все арендаторы.
- При включённом 2FA статус сменится на `needs_2fa` — пароль вводится вручную в Telegram.
- Входящие сообщения пробрасываются вебхуком `POST /webhook/telegram` и создают лид с каналом `telegram`.
- Исходящие сообщения отправляются через `POST tgworker:/send` с `peer_id` (user_id) либо `username`.
- Метрики доступны в Grafana дашборде *Avio Integrations* и алертах Prometheus (`tgworker_sessions_*`, `tgworker_events_errors_total`).
