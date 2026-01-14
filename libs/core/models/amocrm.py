from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(slots=True)
class AmoCRMToken:
    tenant_id: int
    access_token: str | None
    refresh_token: str | None
    expires_at: datetime | None
    obtained_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None
    last_error: str | None
    raw_payload: Mapping[str, Any] | None
