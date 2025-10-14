from __future__ import annotations

from prometheus_client import Counter

MESSAGE_IN_COUNTER = Counter(
    "message_in_total",
    "Normalized incoming messages",
    labelnames=("channel",),
)
MESSAGE_OUT_COUNTER = Counter(
    "message_out_total",
    "Normalized outgoing messages",
    labelnames=("channel",),
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

__all__ = [
    "MESSAGE_IN_COUNTER",
    "MESSAGE_OUT_COUNTER",
    "DB_ERRORS_COUNTER",
    "SEND_FAIL_COUNTER",
]
