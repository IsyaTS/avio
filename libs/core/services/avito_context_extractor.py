from __future__ import annotations

from typing import Any, Mapping

from libs.core.services.avito_contextual_case_builder import AvitoContextualCaseCandidate
from libs.core.services.avito_domain_context_extractor import (
    extract_context as _extract_domain_context,
    extract_reply_facts,
)


def extract_context(
    candidate: AvitoContextualCaseCandidate,
    domain_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _extract_domain_context(candidate, domain_schema=domain_schema)


__all__ = ["extract_context", "extract_reply_facts"]
