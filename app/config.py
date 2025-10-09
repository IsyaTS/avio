"""Compatibility wrapper for legacy ``app.config`` imports."""
from __future__ import annotations

from config import TelegramConfig, telegram_config, tg_worker_url  # re-export

__all__ = ["TelegramConfig", "telegram_config", "tg_worker_url"]
