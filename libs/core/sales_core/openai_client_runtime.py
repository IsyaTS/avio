from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OpenAIClientRuntimeDeps:
    openai_module: Any
    logger: Any


class OpenAIClientRuntime:
    def __init__(self, deps: OpenAIClientRuntimeDeps) -> None:
        self.deps = deps
        self._openai_client: Any | None = None
        self._openai_client_key: str | None = None

    def resolve_chat_completion_callable(self, obj: Any):
        chat = getattr(obj, "chat", None)
        if chat is None:
            return None
        completions = getattr(chat, "completions", None)
        if completions is None:
            return None
        create_fn = getattr(completions, "create", None)
        if not callable(create_fn):
            return None
        return create_fn

    def get_openai_client(self, *, api_key: str) -> Any | None:
        """Return an OpenAI client compatible with chat.completions.create."""

        openai = self.deps.openai_module
        logger = self.deps.logger
        key = str(api_key or "").strip()

        if not (openai and key):
            return None

        if hasattr(openai, "OpenAI"):
            if self._openai_client is None or self._openai_client_key != key:
                try:
                    self._openai_client = openai.OpenAI(api_key=key)  # type: ignore[attr-defined]
                except TypeError:
                    self._openai_client = openai.OpenAI()  # type: ignore[attr-defined]
                except Exception as exc:  # pragma: no cover - сетевые/валидационные ошибки
                    logger.warning("openai client init failed: %s", exc)
                    self._openai_client = None
                    return None
                self._openai_client_key = key

            if self._openai_client is None:
                return None

            if self.resolve_chat_completion_callable(self._openai_client) is None:
                logger.warning("openai client missing chat.completions.create")
                return None

            return self._openai_client

        if not hasattr(openai, "OpenAI"):
            self._openai_client = None
            self._openai_client_key = None

        try:
            setattr(openai, "api_key", key)  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - старые клиенты без api_key
            logger.warning("failed to set openai api_key: %s", exc)
            return None

        if self.resolve_chat_completion_callable(openai) is None:
            logger.warning("openai module missing chat.completions.create")
            return None

        return openai
