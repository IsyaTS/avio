# Telegram API credentials

1. Откройте [my.telegram.org](https://my.telegram.org), авторизуйтесь по номеру и SMS/код из приложения.
2. Создайте (или переиспользуйте) приложение *Telegram API* в разделе **API development tools**.
3. Скопируйте `api_id` и `api_hash`; передайте их владельцу инфраструктуры через защищённый канал (1Password/Hashicorp Vault).
4. Обновите `.env` и `ops/.env` значениями `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`.
5. После ротации обязательно перезапустите `tgworker` (через `docker compose restart tgworker`).
