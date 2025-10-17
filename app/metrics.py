from __future__ import annotations

from prometheus_client import Counter

MESSAGE_IN_COUNTER = Counter(
    "message_in_total",
    "Normalized incoming messages",
    labelnames=("channel",),
)
MESSAGE_OUT_COUNTER = Counter(
    "messages_out_total",
    "Normalized outgoing messages grouped by channel and status",
    labelnames=("channel", "status"),
)
DB_ERRORS_COUNTER = Counter(
    "db_errors_total",
    "Database errors grouped by operation",
    labelnames=("operation",),
)
SEND_FAIL_COUNTER = Counter(
    "send_fail_total",
    "Failed send attempts",
    labelnames=("channel", "reason"),
)

WA_QR_RECEIVED_COUNTER = Counter(
    "wa_qr_received_total",
    "WhatsApp QR payloads accepted",
    labelnames=("tenant",),
)

WA_QR_CALLBACK_ERRORS_COUNTER = Counter(
    "wa_qr_callback_errors_total",
    "WhatsApp QR callback processing errors",
    labelnames=("reason",),
)

WEBHOOK_PROVIDER_COUNTER = Counter(
    "webhook_provider_total",
    "Provider webhook requests grouped by status and channel",
    labelnames=("status", "channel"),
)

__all__ = [
    "MESSAGE_IN_COUNTER",
    "MESSAGE_OUT_COUNTER",
    "DB_ERRORS_COUNTER",
    "SEND_FAIL_COUNTER",
    "WA_QR_RECEIVED_COUNTER",
    "WA_QR_CALLBACK_ERRORS_COUNTER",
    "WEBHOOK_PROVIDER_COUNTER",
]
