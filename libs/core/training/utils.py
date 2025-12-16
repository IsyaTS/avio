from __future__ import annotations

import re
from typing import Optional

from libs.core.training import exporter


_WHITESPACE_RE = re.compile(r"\s+")


def sanitize_text(text: Optional[str]) -> str:
    """Lightweight PII scrub + whitespace normalization."""
    cleaned = exporter.scrub(text or "")
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned
