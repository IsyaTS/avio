from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable


SyncFn = Callable[..., object]


@dataclass(frozen=True)
class ReplySplitConfig:
    enabled: bool
    channels: set[str]
    min_len: int
    max_len: int
    max_parts: int


@dataclass(frozen=True)
class ReplySplitDeps:
    url_token_re: re.Pattern[str]
    tg_handle_re: re.Pattern[str]
    phone_token_re: re.Pattern[str]
    split_greeting_question_combo_fn: SyncFn
    split_long_segment_fn: SyncFn
    extract_standalone_tokens_fn: SyncFn
    force_isolate_contact_tokens_fn: SyncFn
    merge_short_split_parts_fn: SyncFn
    stitch_orphan_fragments_fn: SyncFn
    is_punctuation_only_chunk_fn: SyncFn


def _default_deps() -> ReplySplitDeps:
    return ReplySplitDeps(
        url_token_re=_URL_TOKEN_RE,
        tg_handle_re=_TG_HANDLE_RE,
        phone_token_re=_PHONE_TOKEN_RE,
        split_greeting_question_combo_fn=_split_greeting_question_combo,
        split_long_segment_fn=_split_long_segment,
        extract_standalone_tokens_fn=_extract_standalone_tokens,
        force_isolate_contact_tokens_fn=_force_isolate_contact_tokens,
        merge_short_split_parts_fn=_merge_short_split_parts,
        stitch_orphan_fragments_fn=_stitch_orphan_fragments,
        is_punctuation_only_chunk_fn=_is_punctuation_only_chunk,
    )


_URL_RE = re.compile(
    r"(?:https?://\S+|(?:disk\.yandex\.ru|yadi\.sk|t\.me|telegram\.me)/\S+)",
    re.IGNORECASE,
)
_EOS_MARKER = "<<eos>>"
_ACK_CAP_NEXT_WORD_RE = re.compile(
    r"(?iu)\b(ок|понял|принял|услышал|ладно|хорошо)\s+([А-ЯЁA-Z][А-Яа-яЁёA-Za-z\-]{0,40})\b"
)


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
            # Remove each second comma -> 50% commas removed.
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


def apply_custom_punctuation_style(text: str, *, punct_style_enabled: bool = True) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""
    if not punct_style_enabled:
        return candidate.strip()

    parts: list[str] = []
    pos = 0
    comma_idx = 0
    for match in _URL_RE.finditer(candidate):
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


def _is_atomic_contact_or_link_chunk(chunk: str) -> bool:
    raw = re.sub(r"\s+", " ", str(chunk or "")).strip(" ,")
    if not raw:
        return False
    if _URL_TOKEN_RE.fullmatch(raw):
        return True
    if re.fullmatch(r"@[\w\d_]{4,}", raw):
        return True
    if re.fullmatch(r"(?:\+?\d[\d\-\s()]{8,}\d)", raw):
        return True
    return False


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
            # Avoid splitting model name and its price into different bubbles.
            if re.match(r"^\d", word) and len(candidate) <= max_len + 14:
                current = candidate
                continue
            out.append(current.strip())
            current = word
        else:
            current = candidate
    if current.strip():
        out.append(current.strip())
    # Avoid tiny tail fragments ("установку") when long sentence is split by length.
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


def _merge_short_split_parts(parts: list[str], max_len: int) -> list[str]:
    if not parts:
        return []

    merged: list[str] = []
    min_part = max(36, int(max_len * 0.33))
    for part in parts:
        candidate = re.sub(r"\s+", " ", str(part or "")).strip(" ,")
        if not candidate:
            continue
        if _is_atomic_contact_or_link_chunk(candidate):
            merged.append(candidate)
            continue
        if merged and len(candidate) < min_part:
            prev = merged[-1]
            if _is_atomic_contact_or_link_chunk(prev):
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
            if _is_atomic_contact_or_link_chunk(merged[1]):
                return merged
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
        # Avoid dangling message tails like "... подскажите" where question body
        # was split into the next bubble and first bubble looks unfinished.
        if re.search(
            r"(?iu)\b(подскажите|уточните|скажите|напишите|укажите|выбираете)\s*$",
            merged[idx],
        ):
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 16:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        # Keep "модель ... 33 900 ₽" in one message for readability.
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


def _stitch_orphan_fragments(parts: list[str], max_len: int) -> list[str]:
    if len(parts) <= 1:
        return parts
    out: list[str] = []
    idx = 0
    while idx < len(parts):
        cur = re.sub(r"\s+", " ", str(parts[idx] or "")).strip(" ,")
        if not cur:
            idx += 1
            continue
        if (
            idx + 1 < len(parts)
            and len(cur) < 34
            and "?" not in cur
            and not _GREETING_PREFIX_RE.match(cur)
            and not _is_atomic_contact_or_link_chunk(cur)
        ):
            nxt = re.sub(r"\s+", " ", str(parts[idx + 1] or "")).strip(" ,")
            if nxt and (not _is_atomic_contact_or_link_chunk(nxt)):
                combined = f"{cur} {nxt}".strip()
                if len(combined) <= max_len + 16:
                    out.append(combined)
                    idx += 2
                    continue
        out.append(cur)
        idx += 1
    return out


def _split_long_segment(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]

    comma_parts = [part.strip() for part in clean.split(",") if part.strip()]
    if len(comma_parts) > 1 and any(len(part) < 8 for part in comma_parts):
        # Tiny comma prefixes like "ок," degrade split quality; prefer connector-based split.
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
    # For no-question-mark phrasing ("Здравствуйте в каком городе..."), still split.
    if "?" not in clean and len(tail) < 10:
        return [clean]
    return [head, tail]


_URL_TOKEN_RE = re.compile(
    r"(?:https?://\S+|(?:disk\.yandex\.ru|yadi\.sk|t\.me|telegram\.me)/\S+)",
    re.IGNORECASE,
)
_TG_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_PHONE_TOKEN_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")


def _cleanup_contact_token(token: str) -> str:
    raw = str(token or "").strip()
    if not raw:
        return ""
    cleaned = raw.strip(" \t\r\n<>\"'")
    if _URL_TOKEN_RE.fullmatch(cleaned):
        cleaned = cleaned.rstrip(").,;:!?")
    elif _TG_HANDLE_RE.fullmatch(cleaned):
        cleaned = cleaned.rstrip(").,;:!?")
    elif _PHONE_TOKEN_RE.fullmatch(cleaned):
        cleaned = cleaned.rstrip(").,;:!?")
    return cleaned.strip()


def _is_punctuation_only_chunk(text: str) -> bool:
    return bool(re.fullmatch(r"[.!,;:()\-`'\"«»\s]+", str(text or "")))


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
        prefix = candidate[cursor:start].strip(" ,.;:!?")
        if prefix:
            out.append(prefix)
        cleaned_token = _cleanup_contact_token(token)
        if cleaned_token:
            out.append(cleaned_token)
        cursor = end
    tail = candidate[cursor:].strip(" ,.;:!?")
    if tail:
        out.append(tail)
    return [item for item in out if item and not _is_punctuation_only_chunk(item)]


def _force_isolate_contact_tokens(parts: list[str]) -> list[str]:
    if not parts:
        return []
    out: list[str] = []
    for raw in parts:
        part = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not part:
            continue
        if not (
            _URL_TOKEN_RE.search(part) or _TG_HANDLE_RE.search(part) or _PHONE_TOKEN_RE.search(part)
        ):
            out.append(part)
            continue
        expanded = _extract_standalone_tokens(part)
        if expanded:
            out.extend(expanded)
        else:
            out.append(part)
    return [item for item in out if item and not _is_punctuation_only_chunk(item)]


def _has_contact_intro(parts: list[str]) -> bool:
    if not parts:
        return False
    combined = " ".join(str(p or "") for p in parts).lower()
    markers = (
        "контакт",
        "для связи",
        "напишите",
        "пишите",
        "позвон",
        "связаться",
        "telegram",
        "телеграм",
        "whatsapp",
        "ватсап",
        "вотсап",
    )
    return any(marker in combined for marker in markers)


def split_reply_for_send(
    reply_text: str,
    channel: str,
    *,
    config: ReplySplitConfig,
    deps: ReplySplitDeps | None = None,
) -> list[str]:
    deps = deps or _default_deps()
    clean = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not clean:
        return []
    ch = str(channel or "").strip().lower()
    has_contact_tokens = _has_contact_tokens(clean, deps)
    if not config.enabled or ch not in config.channels:
        return _unsplit_parts(clean, has_contact_tokens=has_contact_tokens, deps=deps)

    greeting_combo = list(deps.split_greeting_question_combo_fn(clean) or [])
    has_multi_questions = clean.count("?") > 1
    has_paragraphs = "\n\n" in clean
    if (
        len(clean) < config.min_len
        and len(greeting_combo) <= 1
        and not has_multi_questions
        and not has_paragraphs
        and not has_contact_tokens
    ):
        return [clean]

    parts = _build_split_parts(clean, config=config, deps=deps)
    deduped = _dedupe_adjacent_parts(parts, deps=deps)
    if not deduped:
        return [clean]
    final_parts = _finalize_parts(
        deduped,
        has_contact_tokens=has_contact_tokens,
        config=config,
        deps=deps,
    )
    return [part for part in final_parts if part and not deps.is_punctuation_only_chunk_fn(part)]


def _unsplit_parts(clean: str, *, has_contact_tokens: bool, deps: ReplySplitDeps) -> list[str]:
    if has_contact_tokens:
        tokenized = list(deps.extract_standalone_tokens_fn(clean) or [])
        if tokenized:
            return list(deps.force_isolate_contact_tokens_fn(tokenized) or [])
    return [clean]


def _build_split_parts(clean: str, *, config: ReplySplitConfig, deps: ReplySplitDeps) -> list[str]:
    parts: list[str] = []
    blocks = [blk.strip() for blk in re.split(r"\n{2,}", clean) if blk.strip()] or [clean]
    for block in blocks:
        greeting_split = list(deps.split_greeting_question_combo_fn(block) or [])
        for segment in greeting_split:
            parts.extend(_split_segment(segment, config=config, deps=deps))
    return parts


def _split_segment(segment: str, *, config: ReplySplitConfig, deps: ReplySplitDeps) -> list[str]:
    seg = str(segment or "").strip()
    if not seg:
        return []
    dash_chunks = [part.strip() for part in re.split(r"\s*[—–;]\s*", seg) if part.strip()] or [seg]
    q_chunks: list[str] = []
    for dash_chunk in dash_chunks:
        if deps.url_token_re.search(dash_chunk):
            q_chunks.append(dash_chunk.strip())
            continue
        q_chunks.extend(
            [q.strip() for q in re.findall(r"[^.?!]+(?:[.?!]|$)", dash_chunk) if q.strip()]
        )
    out: list[str] = []
    for chunk in q_chunks or [seg]:
        out.extend(list(deps.split_long_segment_fn(chunk, config.max_len) or []))
    return out


def _dedupe_adjacent_parts(parts: list[str], *, deps: ReplySplitDeps) -> list[str]:
    deduped: list[str] = []
    prev_norm = ""
    for part in parts:
        line = re.sub(r"\s+", " ", str(part or "")).strip(" ,")
        if not line or deps.is_punctuation_only_chunk_fn(line):
            continue
        norm = line.casefold()
        norm_cmp = _cmp_text(norm)
        prev_cmp = _cmp_text(prev_norm)
        if norm_cmp and prev_cmp and norm_cmp == prev_cmp:
            continue
        if deduped and _is_near_duplicate(norm_cmp, _cmp_text(deduped[-1].casefold())):
            if len(norm) > len(deduped[-1].casefold()):
                deduped[-1] = line
                prev_norm = norm
            continue
        deduped.append(line)
        prev_norm = norm
    return deduped


def _finalize_parts(
    deduped: list[str],
    *,
    has_contact_tokens: bool,
    config: ReplySplitConfig,
    deps: ReplySplitDeps,
) -> list[str]:
    tokenized: list[str] = []
    for part in deduped:
        tokenized.extend(list(deps.extract_standalone_tokens_fn(part) or []))
    final_parts = tokenized or deduped
    final_parts = list(deps.merge_short_split_parts_fn(final_parts, config.max_len) or [])
    final_parts = list(deps.stitch_orphan_fragments_fn(final_parts, config.max_len) or [])
    if len(final_parts) > config.max_parts:
        head = final_parts[: config.max_parts - 1]
        tail = " ".join(final_parts[config.max_parts - 1:]).strip()
        final_parts = head + ([tail] if tail else [])
    if has_contact_tokens:
        final_parts = list(deps.force_isolate_contact_tokens_fn(final_parts) or [])
    return final_parts


def _has_contact_tokens(clean: str, deps: ReplySplitDeps) -> bool:
    return bool(
        deps.url_token_re.search(clean)
        or deps.tg_handle_re.search(clean)
        or deps.phone_token_re.search(clean)
    )


def _cmp_text(value: str) -> str:
    return re.sub(r"(?u)[^0-9a-zа-яё]+", " ", str(value or "")).strip()


def _is_near_duplicate(norm_cmp: str, prev_cmp: str) -> bool:
    if not norm_cmp or not prev_cmp:
        return False
    if not (norm_cmp.startswith(prev_cmp) or prev_cmp.startswith(norm_cmp)):
        return False
    return min(len(norm_cmp), len(prev_cmp)) >= 24


def split_long_segment(text: str, max_len: int) -> list[str]:
    return _split_long_segment(text, max_len)


def merge_short_split_parts(parts: list[str], max_len: int) -> list[str]:
    return _merge_short_split_parts(parts, max_len)


def is_punctuation_only_chunk(text: str) -> bool:
    return _is_punctuation_only_chunk(text)


def clip_text(value: str, limit: int = 1200) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def compose_burst_user_text(parts: list[str]) -> str:
    cleaned: list[str] = []
    last_norm = ""
    for raw in parts:
        text = str(raw or "").strip()
        if not text:
            continue
        norm = re.sub(r"\s+", " ", text).strip().lower()
        if norm == last_norm:
            continue
        cleaned.append(clip_text(text, 700))
        last_norm = norm
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return "\n".join(cleaned)
