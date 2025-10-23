"""Application package exposing shared modules and runtime patches."""

# Ensure httpx clients inside the app use sane defaults (timeouts, retries).
from . import httpx as _httpx_defaults  # noqa: F401

__all__ = [
    "db",
    "core",
    "metrics",
]
