from __future__ import annotations

from prometheus_client import Counter


TG_QR_START_TOTAL = Counter(
    "tg_qr_start_total", "Total number of QR login sessions initiated"
)
TG_QR_EXPIRED_TOTAL = Counter(
    "tg_qr_expired_total",
    "Total number of Telegram QR codes that expired before authorization",
)
TG_2FA_REQUIRED_TOTAL = Counter(
    "tg_2fa_required_total", "Total number of login flows that required 2FA"
)
TG_LOGIN_SUCCESS_TOTAL = Counter(
    "tg_login_success_total", "Total number of successful Telegram authorizations"
)
TG_LOGIN_FAIL_TOTAL = Counter(
    "tg_login_fail_total",
    "Total number of failed Telegram authorizations",
    ["reason"],
)

__all__ = [
    "TG_QR_START_TOTAL",
    "TG_QR_EXPIRED_TOTAL",
    "TG_2FA_REQUIRED_TOTAL",
    "TG_LOGIN_SUCCESS_TOTAL",
    "TG_LOGIN_FAIL_TOTAL",
]
