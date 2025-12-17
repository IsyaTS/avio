from __future__ import annotations

import re
from typing import Optional

from libs.core.training import exporter


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)


def sanitize_text(text: Optional[str]) -> str:
    """Lightweight PII scrub + whitespace normalization."""
    cleaned = exporter.scrub(text or "")
    cleaned = _PUNCT_RE.sub(" ", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned
