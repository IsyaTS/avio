from __future__ import annotations

"""Shared application-level constants and utilities."""

OUTBOX_QUEUE_KEY = "outbox:send"
OUTBOX_DLQ_KEY = "outbox:dlq"

__all__ = ["OUTBOX_QUEUE_KEY", "OUTBOX_DLQ_KEY"]
