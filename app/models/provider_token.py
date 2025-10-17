from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class ProviderToken:
    tenant_id: int
    token: str
    created_at: datetime
