from __future__ import annotations

import re
from dataclasses import dataclass


_GREETING_PREFIX_RE = re.compile(
    r"^\s*(здравствуйте|добрый(?:й|е)|доброго|привет|салам|доброе утро|добрый вечер)\b",
    re.IGNORECASE,
)
_QUESTION_START_RE = re.compile(
    r"\b(в каком|какой|какая|какие|где|когда|сколько|что|как|подскажите|уточните|нужен ли|нужна ли)\b",
    re.IGNORECASE,
)
_SEGMENT_CONNECTOR_RE = re.compile(
    r"\s+(?:но|а|если|когда|чтобы|потом|также|при этом|после этого)\s+",
    re.IGNORECASE,
)
_URL_TOKEN_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_TG_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_PHONE_TOKEN_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")
_STYLE_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_EOS_MARKER = "<<eos>>"
_ACK_CAP_NEXT_WORD_RE = re.compile(
    r"(?iu)\b(ок|понял|принял|услышал|ладно|хорошо)\s+([А-ЯЁA-Z][А-Яа-яЁёA-Za-z\-]{0,40})\b"
)


@dataclass(frozen=True)
class ReplySplitConfig:
    enabled: bool
    min_len: int
    max_len: int
    max_parts: int
    channels: set[str]


def apply_custom_punctuation_style(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""
    parts: list[str] = []
    pos = 0
    comma_idx = 0
    for match in _STYLE_URL_RE.finditer(candidate):
        if match.start() > pos:
            segment, comma_idx = _punct_style_segment(candidate[pos:match.start()], comma_idx)
            parts.append(segment)
        parts.append(match.group(0))
        pos = match.end()
    if pos < len(candidate):
        tail, comma_idx = _punct_style_segment(candidate[pos:], comma_idx)
        parts.append(tail)
    styled = "".join(parts)
    styled = re.sub(r"[ \t]{2,}", " ", styled)
    styled = re.sub(r"[ \t]+\n", "\n", styled)
    styled = re.sub(r"\n{3,}", "\n\n", styled)
    styled = re.sub(r"\s+([,?])", r"\1", styled)
    styled = re.sub(r",{2,}", ",", styled)
    styled = re.sub(r"\?{2,}", "?", styled)
    styled = _lowercase_after_removed_sentence_endings(styled)
    styled = _lowercase_after_acknowledgement(styled)
    return styled.strip()


def _punct_style_segment(text: str, comma_index: int) -> tuple[str, int]:
    out_chars: list[str] = []
    idx = int(comma_index or 0)
    eos_pending = False
    for ch in text:
        if ch in {".", "!"}:
            if not eos_pending:
                out_chars.append(_EOS_MARKER)
                eos_pending = True
            continue
        if ch == ",":
            idx += 1
            if idx % 2 == 0:
                continue
        if not ch.isspace():
            eos_pending = False
        out_chars.append(ch)
    return "".join(out_chars), idx


def _lowercase_after_removed_sentence_endings(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""
    if _EOS_MARKER not in candidate:
        return candidate
    parts = candidate.split(_EOS_MARKER)
    merged = parts[0].rstrip()
    for part in parts[1:]:
        chunk = part.lstrip()
        if chunk:
            first = chunk[0]
            if first.isalpha():
                chunk = first.lower() + chunk[1:]
        if merged and chunk:
            merged = f"{merged} {chunk}"
        elif chunk:
            merged = chunk
    return merged.strip()


def _lowercase_after_acknowledgement(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""

    def _repl(match: re.Match[str]) -> str:
        head = match.group(1)
        word = match.group(2)
        if not word:
            return match.group(0)
        return f"{head} {word[0].lower()}{word[1:]}"

    return _ACK_CAP_NEXT_WORD_RE.sub(_repl, candidate)


def split_reply_for_test_send(reply_text: str, channel: str, config: ReplySplitConfig) -> list[str]:
    clean = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not clean:
        return []
    ch = str(channel or "").strip().lower()
    if not config.enabled or ch not in config.channels:
        return [clean]
    greeting_combo = _split_greeting_question_combo(clean)
    has_multi_questions = clean.count("?") > 1
    has_paragraphs = "\n\n" in clean
    if (
        len(clean) < config.min_len
        and len(greeting_combo) <= 1
        and not has_multi_questions
        and not has_paragraphs
    ):
        return [clean]

    parts: list[str] = []
    blocks = [blk.strip() for blk in re.split(r"\n{2,}", clean) if blk.strip()]
    if not blocks:
        blocks = [clean]

    for block in blocks:
        greeting_split = _split_greeting_question_combo(block)
        for segment in greeting_split:
            seg = segment.strip()
            if not seg:
                continue
            dash_chunks = [part.strip() for part in re.split(r"\s*[—–;]\s*", seg) if part.strip()]
            if not dash_chunks:
                dash_chunks = [seg]
            q_chunks: list[str] = []
            for dash_chunk in dash_chunks:
                q_chunks.extend(
                    [q.strip() for q in re.findall(r"[^?]+(?:\?|$)", dash_chunk) if q.strip()]
                )
            if not q_chunks:
                q_chunks = [seg]
            for chunk in q_chunks:
                parts.extend(_split_long_segment(chunk, config.max_len))

    deduped: list[str] = []
    prev_norm = ""
    for part in parts:
        line = re.sub(r"\s+", " ", part).strip(" ,")
        if not line:
            continue
        if re.fullmatch(r"[.!,;:()\-\s]+", line):
            continue
        norm = line.casefold()
        if norm == prev_norm:
            continue
        deduped.append(line)
        prev_norm = norm
    if not deduped:
        return [clean]
    tokenized: list[str] = []
    for part in deduped:
        tokenized.extend(_extract_standalone_tokens(part))
    deduped = tokenized or deduped
    deduped = _merge_short_split_parts(deduped, config.max_len)
    if len(deduped) <= config.max_parts:
        return deduped
    head = deduped[: config.max_parts - 1]
    tail = " ".join(deduped[config.max_parts - 1:]).strip()
    if tail:
        head.append(tail)
    return [part for part in head if part]


def _extract_standalone_tokens(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    tokens: list[tuple[int, int, str]] = []
    for rx in (_URL_TOKEN_RE, _TG_HANDLE_RE, _PHONE_TOKEN_RE):
        for match in rx.finditer(candidate):
            token = str(match.group(0) or "").strip()
            if token:
                tokens.append((match.start(), match.end(), token))
    if not tokens:
        return [candidate]
    tokens.sort(key=lambda item: (item[0], item[1]))
    merged: list[tuple[int, int, str]] = []
    for start, end, token in tokens:
        if merged and start < merged[-1][1]:
            continue
        merged.append((start, end, token))
    out: list[str] = []
    cursor = 0
    for start, end, token in merged:
        prefix = candidate[cursor:start].strip(" ,")
        if prefix:
            out.append(prefix)
        out.append(token)
        cursor = end
    tail = candidate[cursor:].strip(" ,")
    if tail:
        out.append(tail)
    return [item for item in out if item]


def _split_long_segment_by_words(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]
    words = clean.split(" ")
    out: list[str] = []
    current = ""
    for word in words:
        if not word:
            continue
        candidate = f"{current} {word}".strip() if current else word
        if current and len(candidate) > max_len:
            if re.match(r"^\d", word) and len(candidate) <= max_len + 14:
                current = candidate
                continue
            out.append(current.strip())
            current = word
        else:
            current = candidate
    if current.strip():
        out.append(current.strip())
    if len(out) >= 2 and len(out[-1]) < 12:
        combined = f"{out[-2]} {out[-1]}".strip()
        if len(combined) <= max_len + 20:
            out[-2] = combined
            out.pop()
    return [part for part in out if part]


def _split_long_segment_by_connectors(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]

    out: list[str] = []
    remaining = clean
    min_cut = max(48, int(max_len * 0.5))
    while len(remaining) > max_len:
        window = remaining[: max_len + 1]
        split_pos = -1
        for match in _SEGMENT_CONNECTOR_RE.finditer(window):
            if match.start() >= min_cut:
                split_pos = int(match.start())
        if split_pos <= 0:
            break
        head = remaining[:split_pos].strip(" ,")
        if len(head) < min_cut:
            break
        if head:
            out.append(head)
        remaining = remaining[split_pos:].strip(" ,")
        if not remaining:
            break
    if remaining:
        out.append(remaining)
    return [part for part in out if part]


def _split_greeting_question_combo(text: str) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return [clean] if clean else []
    if not _GREETING_PREFIX_RE.search(clean):
        return [clean]
    match = _QUESTION_START_RE.search(clean)
    if not match:
        return [clean]
    split_at = int(match.start())
    if split_at <= 6:
        return [clean]
    head = clean[:split_at].strip(" ,")
    tail = clean[split_at:].strip(" ,")
    if not head or not tail:
        return [clean]
    if len(head) > 56:
        return [clean]
    if "?" not in clean and len(tail) < 10:
        return [clean]
    return [head, tail]


def _merge_short_split_parts(parts: list[str], max_len: int) -> list[str]:
    if not parts:
        return []

    def _is_atomic_contact_or_link(chunk: str) -> bool:
        raw = re.sub(r"\s+", " ", str(chunk or "")).strip(" ,")
        if not raw:
            return False
        if re.fullmatch(r"https?://\S+", raw, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"@[\w\d_]{4,}", raw):
            return True
        if re.fullmatch(r"(?:\+?\d[\d\-\s()]{8,}\d)", raw):
            return True
        return False

    merged: list[str] = []
    min_part = max(36, int(max_len * 0.33))
    for part in parts:
        candidate = re.sub(r"\s+", " ", str(part or "")).strip(" ,")
        if not candidate:
            continue
        if _is_atomic_contact_or_link(candidate):
            merged.append(candidate)
            continue
        if merged and len(candidate) < min_part:
            prev = merged[-1]
            if _is_atomic_contact_or_link(prev):
                merged.append(candidate)
                continue
            if _GREETING_PREFIX_RE.match(prev) and _QUESTION_START_RE.match(candidate):
                merged.append(candidate)
                continue
            combined = f"{merged[-1]} {candidate}".strip()
            if len(combined) <= max_len + 6:
                merged[-1] = combined
                continue
        merged.append(candidate)
    if len(merged) >= 2 and len(merged[0]) < min_part:
        if not (_GREETING_PREFIX_RE.match(merged[0]) and _QUESTION_START_RE.match(merged[1])):
            combined = f"{merged[0]} {merged[1]}".strip()
            if len(combined) <= max_len + 6:
                merged[1] = combined
                merged = merged[1:]

    tail_connectors = ("и", "но", "а", "или", "если", "чтобы", "потом", "также")
    idx = 0
    while idx < len(merged) - 1:
        last_word = merged[idx].split(" ")[-1].lower()
        if last_word in tail_connectors:
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 6:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        if re.search(r'[»"]\s*$', merged[idx]) and re.match(r"^\d", merged[idx + 1]):
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 20:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        if "—" in merged[idx] and re.match(r"^\d", merged[idx + 1]):
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 20:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        idx += 1
    return merged


def _split_long_segment(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]

    comma_parts = [part.strip() for part in clean.split(",") if part.strip()]
    if len(comma_parts) > 1 and any(len(part) < 8 for part in comma_parts):
        conn_parts = _split_long_segment_by_connectors(clean, max_len)
        out_parts: list[str] = []
        for part in conn_parts:
            out_parts.extend(_split_long_segment_by_words(part, max_len))
        normalized = [part for part in out_parts if part]
        if len(normalized) >= 2:
            return normalized
    if len(comma_parts) <= 1:
        dash_parts = [part.strip() for part in re.split(r"\s*[—–;]\s*", clean) if part.strip()]
        if len(dash_parts) > 1:
            expanded: list[str] = []
            for part in dash_parts:
                expanded.extend(_split_long_segment_by_connectors(part, max_len))
            out_parts: list[str] = []
            for part in expanded:
                out_parts.extend(_split_long_segment_by_words(part, max_len))
            return [part for part in out_parts if part]
        conn_parts = _split_long_segment_by_connectors(clean, max_len)
        out_parts: list[str] = []
        for part in conn_parts:
            out_parts.extend(_split_long_segment_by_words(part, max_len))
        return [part for part in out_parts if part]

    out: list[str] = []
    current = ""
    for idx, part in enumerate(comma_parts):
        suffix = "," if idx < len(comma_parts) - 1 else ""
        piece = f"{part}{suffix}".strip()
        candidate = f"{current} {piece}".strip() if current else piece
        if current and len(candidate) > max_len:
            out.extend(_split_long_segment_by_words(current, max_len))
            current = piece
        else:
            current = candidate
    if current.strip():
        out.extend(_split_long_segment_by_words(current, max_len))
    return [part.strip() for part in out if part.strip()]
