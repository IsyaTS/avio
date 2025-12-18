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
- `AVITO_ANALYTICS_REDIRECT_URI` — тот же URL, что и callback выше. Если переменная не задана, используется основной redirect из `AVITO_REDIRECT_URL` (общий callback `/v1/oauth/avito/callback`), чтобы можно было работать на одном зарегистрированном redirect.
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
3) Отчёт: карточки метрик, таблица объявлений, операции/списания, блок “Работа: отклики” (job:applications) и “VAS/продвижение” (items:apply_vas), raw JSON со всеми ответами Avito.
4) Экспорты: JSON и CSV (items / operations / job_applications / vas_prices / vas_packages).
5) Кэш отчёта в Redis на 5–15 минут, ручное обновление кнопкой «Обновить».

## Ограничения и совместимость
- Не трогаются существующие Avito маршруты для тенантов (`/v1/oauth/avito/*`).
- Вся авторизация — только через `ADMIN_TOKEN`.
- При отсутствии Redis или ключа шифрования страница вернёт понятную ошибку.

## Job applications (job:applications)
- Основной путь: `job/v1|v2 applications` (pull). Если Avito не даёт list — используем хранилище IDs.
- Таблица `avito_job_application_events` собирает application_id из webhooks (если в payload есть application_id/applyId) или через admin API `POST /admin/avito-analytics/api/job/application/add` (ручной ввод).
- Обогащение: `job/v1/applications/get_by_ids` (батчи до 200), + best-effort `job/v2/resumes/{id}` и `job/v2/vacancies/{id}` для топ-резюме/вакансий.
- KPI: total, unique_applicants, разбивка по статусам, raw блок с list/by_ids/resumes/vacancies.

## VAS (items:apply_vas)
- Прайсы: `POST /core/v1/accounts/{id}/price/vas` (services) и `.../price/vas_packages` (packages). При валидационной ошибке сохраняется raw error и выдаётся предупреждение, отчёт не падает.
- Derived: top-10 самых дешёвых услуг; операции можно сверять через экспорт operations.csv.

## Лимиты/батчи
- job get_by_ids: до 200 ID за запрос, берём не более 200 за отчёт (период 7/30/90).
- enrich resume/vacancy: до 10 уникальных ID каждого типа.
- vas: один запрос на цены и один на пакеты (payload пустой, только диагностируем доступность).
