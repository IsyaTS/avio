from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(slots=True)
class AvitoAnalyticsToken:
    account_id: int
    display_name: str | None
    scopes: str | None
    token_type: str | None
    access_token: str | None
    refresh_token: str | None
    expires_at: datetime | None
    obtained_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None
    last_error: str | None
    raw_payload: Mapping[str, Any] | None
