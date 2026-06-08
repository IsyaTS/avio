from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class ContextGuardRuntimeDeps:
    normalize_model_alias: Callable[[str], str]
    fact_token_re: Any
    generic_fact_stopwords: Sequence[str]


class ContextGuardRuntime:
    def __init__(self, deps: ContextGuardRuntimeDeps) -> None:
        self.deps = deps

    def rewrite_loses_context_anchors(
        self,
        candidate: str,
        rewrite: str,
        dialogue_tail: Sequence[Mapping[str, Any]],
    ) -> bool:
        cand = str(candidate or "").strip()
        rew = str(rewrite or "").strip()
        if not cand or not rew:
            return False
        cand_norm = self.deps.normalize_model_alias(cand)
        rew_norm = self.deps.normalize_model_alias(rew)
        if not cand_norm or not rew_norm:
            return False

        cand_nums = set(re.findall(r"\d{2,}", cand))
        rew_nums = set(re.findall(r"\d{2,}", rew))
        if cand_nums and not (cand_nums & rew_nums):
            return True

        anchor_tokens: set[str] = set()
        for item in dialogue_tail or []:
            if str(item.get("role") or "").strip().lower() != "user":
                continue
            content = str(item.get("content") or "")
            for token in self.deps.fact_token_re.findall(content.lower().replace("ё", "е")):
                if len(token) < 4:
                    continue
                if token in self.deps.generic_fact_stopwords:
                    continue
                anchor_tokens.add(token)
        if not anchor_tokens:
            return False
        cand_hits = {token for token in anchor_tokens if token in cand_norm}
        if not cand_hits:
            return False
        rew_hits = {token for token in anchor_tokens if token in rew_norm}
        return not rew_hits
