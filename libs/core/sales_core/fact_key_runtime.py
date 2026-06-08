from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class FactKeyRuntimeDeps:
    fact_canonical_aliases: Dict[str, set[str]]


class FactKeyRuntime:
    def __init__(self, deps: FactKeyRuntimeDeps) -> None:
        self.deps = deps

    @staticmethod
    def is_plausible_contact_phone(token: str) -> bool:
        raw = str(token or "").strip()
        if not raw:
            return False
        digits = re.sub(r"\D+", "", raw)
        if len(digits) < 10 or len(digits) > 15:
            return False
        if re.fullmatch(r"\d+", raw):
            return len(digits) == 11
        if re.fullmatch(r"\+\d+", raw):
            return 10 <= len(digits) <= 15
        return True

    @staticmethod
    def safe_short_text(value: str, limit: int = 120) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 1)].rstrip() + "…"

    @staticmethod
    def normalize_fact_key(value: str) -> str:
        key = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        key = re.sub(r"[^a-z0-9а-яё_]+", "", key, flags=re.IGNORECASE)
        key = re.sub(r"_+", "_", key).strip("_")
        return key[:48]

    def canonical_fact_key(self, value: str) -> str:
        key = self.normalize_fact_key(value)
        if not key:
            return ""
        for canonical, aliases in self.deps.fact_canonical_aliases.items():
            if key == canonical or key in aliases:
                return canonical
        return key
