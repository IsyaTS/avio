# Public Avito Analytics

## URL
- UI: `/pub/analytics/avito?tenant=...&k=...`
- API:
  - `/v1/analytics/avito/report?tenant=...&k=...&period=7&sla=15&fast=1`
  - `/v1/analytics/avito/items`
  - `/v1/analytics/avito/stats`
  - `/v1/analytics/avito/messenger`
  - `/v1/analytics/avito/spend`
  - `/v1/analytics/avito/calls`
  - OAuth analytics:
    - `/v1/oauth/avito-analytics/authorize`
    - `/v1/oauth/avito-analytics/status`
    - `/v1/oauth/avito-analytics/callback`

## Авторизация
Все эндпоинты защищены `_authorize_public_settings_request` и требуют `tenant` + `k`.
OAuth аналитики хранится отдельно от бота.

## Параметры
- `period`: 7/30/90
- `sla`: 5/15/60
- `fast`: 1 — быстрый режим (выборка)
- `force`: 1 — игнорировать кэш
- `avg_check`, `close_rate_chat`, `gross_margin`, `loss_factor_slow_response`

## Примечания
- Быстрый режим ограничивает страницы items, количество чатов и сообщений.
- При rate-limit 429 выставляются предупреждения в `meta.warnings`.
- Убедитесь, что `AVITO_ANALYTICS_REDIRECT_URI` указывает на `/v1/oauth/avito-analytics/callback`.
