# amoCRM интеграция (Avio → amoCRM)

## Что это делает
- Односторонняя синхронизация: входящие сообщения в Avio создают/обновляют сделку в amoCRM.
- Движение по стадиям и обновление кастомных полей управляются правилами тенанта.
- Изменения в amoCRM обратно в Avio не подтягиваются.

## Настройка в amoCRM
1) Создайте интеграцию в amoCRM и получите `client_id` и `client_secret`.
2) Укажите redirect URL в интеграции amoCRM:
   - `https://<ваш_домен>/pub/integrations/amocrm/oauth/callback`
3) Убедитесь, что доступ к amoCRM API v4 разрешён для вашей интеграции.

## Настройка в Avio (UI → Каналы → amoCRM)
1) Нажмите `Подключить` и пройдите авторизацию.
2) Система автоматически выберет первую воронку amoCRM и сохранит её стадии.
3) Правила продвижения по умолчанию:
   - 0: `on_first_inbound`
   - 1: `on_inbound_count` (min 2)
   - 2: `on_inbound_count` (min 4)
   - остальные: `manual_only`

## Режим "через env" (одна кнопка для клиента)
UI показывает только кнопку `Подключить`, все параметры берутся из env:
- `AMOCRM_CLIENT_ID`
- `AMOCRM_CLIENT_SECRET`
- `AMOCRM_REDIRECT_URL` (опционально)
- `AMOCRM_BASE_URL` или `AMOCRM_SUBDOMAIN`

В этом режиме клиенту достаточно нажать «Подключить».

Если у каждого арендатора свой amoCRM-домен, используйте per-tenant переменные окружения:
- `AMOCRM_SUBDOMAIN_TENANT_<id>` или `AMOCRM_BASE_URL_TENANT_<id>`
- `AMOCRM_CLIENT_ID_TENANT_<id>`
- `AMOCRM_CLIENT_SECRET_TENANT_<id>`
- `AMOCRM_REDIRECT_URL_TENANT_<id>` (опционально)

## Как узнать pipeline_id / stage_id / custom_field_id
- В amoCRM откройте нужную воронку → настройки стадий.
- В DevTools amoCRM API v4:
  - `GET /api/v4/leads/pipelines` вернёт список воронок и `id`.
  - `GET /api/v4/leads/pipelines/{pipeline_id}` вернёт `status_id` стадий.
- Для кастомных полей используйте раздел полей сущности в amoCRM или API:
  - `GET /api/v4/leads/custom_fields`

## Правила стадий (stage rules)
- `on_first_inbound` — первый входящий (обычно стадия 0)
- `on_inbound_count` — при `min_inbound_messages`
- `on_keyword` — ключевые слова в последнем сообщении
- `on_field_present` — когда извлечено поле (`field_key`)
- `manual_only` — остановка автоматического продвижения

Дефолты:
- stage[0] → `on_first_inbound`
- stage[1] → `on_inbound_count` (min=2)
- stage[2] → `on_inbound_count` (min=4)

## Извлечение полей
Пример правила:
```
{ "key": "phone", "regex": "(\\+?\\d{11,})", "amo_field_id": 123, "apply_mode": "last_inbound" }
```

- `apply_mode=last_inbound` — только последнее сообщение
- `apply_mode=any_history` — по последним входящим сообщениям (склейка)
- Значение берётся из первой capturing group или полного совпадения
- `phone` нормализуется (оставляем `+` и цифры)

## Проверка подключения
- В UI нажмите `Проверить подключение`
- Или вручную:
  - `GET /pub/integrations/amocrm/status?tenant=<id>&k=<key>`
  - `POST /pub/integrations/amocrm/test?tenant=<id>&k=<key>`

## Отключение
- Универсальный webhook для amoCRM (в настройках интеграции):
  - `https://dev.avio.website/pub/integrations/amocrm/uninstall`
- Ручное отключение (по ключу):
  - `POST /pub/integrations/amocrm/disconnect?tenant=<id>&k=<key>`
  - `POST /pub/integrations/amocrm/uninstall?tenant=<id>&k=<key>`

## Логи и отладка
- События складываются в `crm_outbox`
- Ключевые логи:
  - `amocrm_outbox_enqueued`
  - `amocrm_event_done`
  - `amocrm_event_retry`

## Безопасность
- Токены хранятся в БД в зашифрованном виде (ключ `AVITO_TOKEN_ENCRYPTION_KEY`)
- В API ответы не возвращают `client_secret` и токены в открытом виде
