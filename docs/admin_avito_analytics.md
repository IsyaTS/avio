# Avito Analytics (OAuth) — админ-раздел

Новый инструмент для аналитики Avito доступен только по админ-токену.

## URL
- Страница: `/admin/avito-analytics`
- OAuth старт: `/admin/avito-analytics/oauth/start`
- Callback (зарегистрируйте в Avito): `/admin/avito-analytics/oauth/callback`
- API: `/admin/avito-analytics/api/report`, `/api/export.json`, `/api/export.csv`, `/api/refresh`

## Требуемые переменные окружения
- `ADMIN_TOKEN` — уже используется в админке, обязателен.
- `AVITO_CLIENT_ID`, `AVITO_CLIENT_SECRET` — креды приложения Avito.
- `AVITO_ANALYTICS_REDIRECT_URI` — тот же URL, что и callback выше.
- `AVITO_ANALYTICS_SCOPES` — опционально, по умолчанию запрашивается максимум:
  ```
  user:read items:info stats:read user_balance:read user_operations:read messenger:read
  autoload:reports autoteka:previews autoteka:reports job:write job:negotiations job:cv short_term_rent:read
  ```
- `AVITO_TOKEN_ENCRYPTION_KEY` — ключ Fernet (base64). Без него токены не сохраняются.
- `AVITO_ANALYTICS_CACHE_TTL` — TTL кеша отчётов в Redis (секунды, по умолчанию 900).

## Что делает страница
1) Админ нажимает «Авторизоваться в Avito» → OAuth → callback сохраняет токены в Postgres (таблица `avito_analytics_tokens`) в зашифрованном виде.
2) Поддерживается несколько аккаунтов, переключение в выпадающем списке.
3) Отчёт: карточки метрик, таблица объявлений, операции/списания, raw JSON со всеми ответами Avito.
4) Экспорты: JSON и CSV (отдельные эндпоинты).
5) Кэш отчёта в Redis на 5–15 минут, ручное обновление кнопкой «Обновить».

## Ограничения и совместимость
- Не трогаются существующие Avito маршруты для тенантов (`/v1/oauth/avito/*`).
- Вся авторизация — только через `ADMIN_TOKEN`.
- При отсутствии Redis или ключа шифрования страница вернёт понятную ошибку.
