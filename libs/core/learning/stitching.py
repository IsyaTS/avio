from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class StitchedTurn:
    role: str
    text: str
    start_at: str
    end_at: str
    raw_count: int = 1
    is_stitched: bool = False
    message_ids: list[int] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "text": self.text,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "raw_count": self.raw_count,
            "is_stitched": self.is_stitched,
            "message_ids": list(self.message_ids),
            "sources": list(self.sources),
        }


_EPOCH = datetime.fromtimestamp(0)


def _as_timestamp(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, datetime):
        return value.timestamp()
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _normalize_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value or "").strip()
    if text:
        return text
    return _EPOCH.isoformat()


def resolve_role(item: Mapping[str, Any]) -> str:
    try:
        direction = int(item.get("direction") or 0)
    except Exception:
        direction = 0
    source = str(item.get("source") or "").strip().lower()
    is_bot = bool(item.get("is_bot"))
    if direction == 0:
        return "user"
    if source == "manager" or source.startswith("manager:"):
        return "manager"
    if not is_bot and source and source != "bot":
        return "manager"
    return "assistant"


def stitch_messages(messages: Sequence[Mapping[str, Any]], *, within_seconds: int = 45) -> list[StitchedTurn]:
    turns: list[StitchedTurn] = []
    if within_seconds <= 0:
        within_seconds = 45
    for row in messages or []:
        text = str(row.get("text") or row.get("content") or "").strip()
        if not text:
            continue
        role = resolve_role(row)
        created_at = _normalize_dt(row.get("created_at") or row.get("ts") or row.get("end_at"))
        ts = _as_timestamp(row.get("created_at") or row.get("ts") or row.get("end_at"))
        source = str(row.get("source") or "").strip().lower()
        message_id = row.get("id")
        try:
            parsed_id = int(message_id)
        except Exception:
            parsed_id = 0
        if turns:
            last = turns[-1]
            last_ts = _as_timestamp(last.end_at)
            if role == last.role and ts >= last_ts and ts - last_ts <= float(within_seconds):
                merged_text = f"{last.text}\n{text}" if text not in last.text.splitlines() else last.text
                merged_ids = list(last.message_ids)
                if parsed_id > 0:
                    merged_ids.append(parsed_id)
                merged_sources = list(last.sources)
                if source:
                    merged_sources.append(source)
                turns[-1] = StitchedTurn(
                    role=last.role,
                    text=merged_text.strip(),
                    start_at=last.start_at,
                    end_at=created_at,
                    raw_count=last.raw_count + 1,
                    is_stitched=True,
                    message_ids=merged_ids,
                    sources=merged_sources,
                )
                continue
        turns.append(
            StitchedTurn(
                role=role,
                text=text,
                start_at=created_at,
                end_at=created_at,
                raw_count=1,
                is_stitched=False,
                message_ids=[parsed_id] if parsed_id > 0 else [],
                sources=[source] if source else [],
            )
        )
    return turns


def stitch_runtime_history(history: Sequence[Mapping[str, Any]], *, current_user_text: str = "") -> list[StitchedTurn]:
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(history or []):
        role_raw = str(item.get("role") or "").strip().lower()
        role = "assistant" if role_raw == "assistant" else "user"
        rows.append(
            {
                "id": idx + 1,
                "direction": 0 if role == "user" else 1,
                "is_bot": role == "assistant",
                "source": "bot" if role == "assistant" else "user",
                "text": str(item.get("content") or item.get("text") or "").strip(),
                "created_at": item.get("created_at") or f"1970-01-01T00:00:{idx:02d}+00:00",
            }
        )
    if current_user_text.strip():
        rows.append(
            {
                "id": len(rows) + 1,
                "direction": 0,
                "is_bot": False,
                "source": "user",
                "text": current_user_text.strip(),
                "created_at": f"1970-01-01T00:01:{len(rows):02d}+00:00",
            }
        )
    return stitch_messages(rows, within_seconds=30)
