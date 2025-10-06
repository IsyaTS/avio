# Diagnostics Toolkit

Этот модуль позволяет запускать полный набор проверок после деплоя Avio.
Скрипт `scripts/diagnostics.py` автоматически обнаруживает функции `check_*`,
выполняет их по порядку и формирует цветной отчёт с логом в каталоге
`diagnostics/`.

## Запуск через Make

```bash
export AVIO_URL="https://stage.your-avio.example"
export ADMIN_TOKEN="<админский токен>"
make diag
```

Для дополнительного логирования установите переменную `DIAG_VERBOSE` или
используйте цель:

```bash
make diag-verbose
```

## Запуск в Docker Compose

```bash
docker compose run --rm \
  -e ADMIN_TOKEN="$ADMIN_TOKEN" \
  diagnostics
```

Сервис `diagnostics` подключён к сети `app` и по умолчанию обращается к
`http://app:8000`.

## Ручной запуск

```bash
export AVIO_URL="http://localhost:8000"
export ADMIN_TOKEN="<админский токен>"
python scripts/diagnostics.py
```

При успешном выполнении отчёт будет сохранён в
`diagnostics/report-<timestamp>.txt`. Для сохранения артефактов установите
`KEEP_ARTIFACTS=1`.
