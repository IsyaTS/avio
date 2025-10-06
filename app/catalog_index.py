"""PDF catalog indexing utilities.

This module parses uploaded catalog files (currently PDF) into structured
chunks so the bot can search within catalogs without manual conversion
into CSV.  Indices are stored alongside tenant data to keep uploads and
metadata colocated.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import re
import json as _json
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    from catalog.pipeline import (
        PipelineReport,
        clean_title as _clean_title,
        finalize_catalog_rows as _finalize_catalog_rows,
        normalize_price_value as _normalize_price_value,
        sanitize_value as _sanitize_value,
        title_contains_forbidden as _title_contains_forbidden,
    )
except ImportError:  # pragma: no cover - fallback for tooling imports
    import sys

    from app import catalog as _catalog
    from app.catalog import pipeline as _pipeline

    sys.modules.setdefault("catalog", _catalog)
    sys.modules.setdefault("catalog.pipeline", _pipeline)
    from app.catalog.pipeline import (
        PipelineReport,
        clean_title as _clean_title,
        finalize_catalog_rows as _finalize_catalog_rows,
        normalize_price_value as _normalize_price_value,
        sanitize_value as _sanitize_value,
        title_contains_forbidden as _title_contains_forbidden,
    )

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - fallback when dependency missing
    PdfReader = None  # type: ignore[assignment]


class CatalogIndexError(RuntimeError):
    """Raised when catalog indexing fails."""


logger = logging.getLogger(__name__)

def _decode_pdf_text(raw: bytes) -> str:
    mapping = {
        ord('n'): '\n',
        ord('r'): '\r',
        ord('t'): '\t',
        ord('b'): '\b',
        ord('f'): '\f',
        ord('('): '(',
        ord(')'): ')',
        ord('\\'): '\\',
    }
    result_chars: list[str] = []
    idx = 0
    length = len(raw)
    while idx < length:
        if raw[idx] == 0x5C and idx + 1 < length:
            mapped = mapping.get(raw[idx + 1])
            if mapped is not None:
                result_chars.append(mapped)
                idx += 2
                continue
        result_chars.append(chr(raw[idx]))
        idx += 1
    return "".join(result_chars)

def _extract_pdf_pages(source: Path) -> list[tuple[int, str]]:
    if PdfReader is not None:
        reader = PdfReader(str(source))
        pages = []
        for idx, page in enumerate(reader.pages, start=1):
            try:
                extracted = page.extract_text() or ""
            except Exception as exc:  # pragma: no cover - parity with real parser
                raise CatalogIndexError(f"failed to extract text from page {idx}: {exc}")
            pages.append((idx, extracted))
        return pages

    import re

    try:
        data = source.read_bytes()
    except Exception as exc:  # pragma: no cover - file access issues
        raise CatalogIndexError(f"failed to read PDF: {exc}")

    streams = re.findall(rb"stream\s*(.*?)\s*endstream", data, re.DOTALL)
    pages: list[tuple[int, str]] = []
    for idx, stream in enumerate(streams, start=1):
        fragments: list[str] = []
        for match in re.finditer(rb"\((.*?)(?<!\\)\)", stream, re.DOTALL):
            text_piece = _decode_pdf_text(match.group(1)).strip()
            if text_piece:
                fragments.append(text_piece)
        if fragments:
            pages.append((idx, "\n".join(fragments)))
    return pages


@dataclass(frozen=True)
class CatalogChunk:
    chunk_id: str
    page: int
    title: str
    text: str
    identifiers: Sequence[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.chunk_id,
            "page": self.page,
            "title": self.title,
            "text": self.text,
            "identifiers": list(self.identifiers),
        }


def _normalize_whitespace(value: str) -> str:
    collapsed = re.sub(r"[\u00a0\t]+", " ", value)
    collapsed = re.sub(r"\s*\n\s*", "\n", collapsed)
    collapsed = re.sub(r"\n{3,}", "\n\n", collapsed)
    return collapsed.strip()


def _chunk_text(text: str, *, max_chars: int = 700, overlap: int = 120) -> Iterable[str]:
    """Yield reasonably sized chunks for retrieval.

    We keep overlap so token-based retrieval has context around the split.
    """

    if not text:
        return []
    clean = _normalize_whitespace(text)
    if not clean:
        return []

    # Split by paragraphs first to preserve semantic units.
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", clean) if p.strip()]
    if not paragraphs:
        paragraphs = [clean]

    buffer: List[str] = []
    buffer_len = 0

    def flush_buffer() -> Iterable[str]:
        nonlocal buffer, buffer_len
        if not buffer:
            return []
        merged = "\n\n".join(buffer)
        buffer = []
        buffer_len = 0
        yield merged.strip()

    for para in paragraphs:
        para_len = len(para)
        if para_len + buffer_len <= max_chars:
            buffer.append(para)
            buffer_len += para_len + (2 if buffer else 0)
            continue

        if buffer:
            for chunk in flush_buffer():
                yield from _split_long_chunk(chunk, max_chars=max_chars, overlap=overlap)

        if para_len <= max_chars:
            buffer = [para]
            buffer_len = para_len
            continue

        # Paragraph is longer than window => split directly.
        yield from _split_long_chunk(para, max_chars=max_chars, overlap=overlap)
        buffer = []
        buffer_len = 0

    for chunk in flush_buffer():
        yield from _split_long_chunk(chunk, max_chars=max_chars, overlap=overlap)


def _split_long_chunk(text: str, *, max_chars: int, overlap: int) -> Iterable[str]:
    if not text:
        return []
    cleaned = text.strip()
    if not cleaned:
        return []

    start = 0
    length = len(cleaned)
    max_chars = max(200, max_chars)
    overlap = max(0, min(overlap, max_chars // 2))

    while start < length:
        end = min(length, start + max_chars)
        candidate = cleaned[start:end]
        if end < length:
            pivot = max(candidate.rfind("\n"), candidate.rfind("."))
            if pivot >= max_chars * 0.4:
                end = start + pivot + 1
                candidate = cleaned[start:end]
        yield candidate.strip()
        if end >= length:
            break
        start = max(end - overlap, 0)
        if start == end:
            start += max_chars


_IDENTIFIER_RE = re.compile(r"\b([A-ZА-Я0-9]{2,}(?:[-_/][A-ZА-Я0-9]{2,})+)\b")


def _extract_identifiers(text: str) -> Sequence[str]:
    if not text:
        return []
    values = {match.strip() for match in _IDENTIFIER_RE.findall(text)}
    # Also capture numeric article codes like 123-45 or 9001
    for token in re.findall(r"\b\d{3,}[^\s]*\b", text):
        cleaned = token.strip()
        if cleaned:
            values.add(cleaned)
    return sorted(values)


def _guess_title(text: str, *, page: int) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines[:5]:
        if 4 <= len(line) <= 120:
            return line
    if lines:
        first = lines[0]
        return first[:117] + "…" if len(first) > 118 else first
    return f"Страница {page}"


@dataclass
class BlockLine:
    raw: str
    text: str
    clean: str
    leading_space: bool


@dataclass
class ParsedPair:
    key: str
    value: str
    line_index: int


@dataclass
class PriceCandidate:
    raw: str
    normalized: str
    weight: float


@dataclass
class ProductBlock:
    chunk_id: str
    chunk_page: int
    chunk_title: str
    chunk_title_candidate: str
    index: int
    lines: List[BlockLine]
    text: str
    pairs: List[ParsedPair]
    consumed_indices: Set[int]
    score: float
    price: Optional[PriceCandidate]
    title_candidates: List[Tuple[int, str]]


_INVISIBLE_RE = re.compile(r"[\u200b\u200c\u200d\u2028\u2029\u2060]")
_HYPHEN_LINEBREAK_RE = re.compile(r"-\s*\n\s*")
_LETTER_RUN_RE = re.compile(r"(?:(?<=\s)|^)([A-ZА-ЯЁ])(?:\s+([A-ZА-ЯЁ])){2,}(?=(?:\s|$))")
_TRAILING_LAST_LETTER_RE = re.compile(r"([A-Za-zА-Яа-яЁё])\s+([A-Za-zА-Яа-яЁё])\b")
_MULTI_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"\S+")
_NUMBER_RE = re.compile(r"\d[\d\s.,]*")
def _load_stop_phrases() -> List[str]:
    candidates: List[str] = [
        # built-ins
        "watermark", "icecream", "editor", "trial", "demo",
        "отредактирован", "активируйте", "оглавление", "контакты",
        "гарантия", "доставка", "акция", "реклама",
        "водяной знак", "убрать водяной знак", "pro версию",
        # common catalog section headers (keep generic)
        "памятка", "как замерить", "как узнать",
        "нас рекомендуют близким",
    ]
    try:
        path = Path("data/stop_phrases.json")
        if path.exists():
            data = _json.loads(path.read_text(encoding="utf-8"))
            phrases = data.get("phrases") or []
            for p in phrases:
                if isinstance(p, str) and p.strip():
                    candidates.append(p.strip())
    except Exception:
        pass
    # Deduplicate and sort by length (longer first for substrings)
    uniq = []
    seen = set()
    for p in sorted(set(candidates), key=lambda s: (-len(s), s)):
        low = p.lower()
        if low not in seen:
            seen.add(low)
            uniq.append(low)
    return uniq

_STOP_PHRASES = _load_stop_phrases()

_STOP_KEYWORDS_RE = re.compile(
    r"|".join(re.escape(p) for p in _STOP_PHRASES),
    re.IGNORECASE,
)
_CURRENCY_TOKENS = {
    "₽",
    "р",
    "р.",
    "rub",
    "руб",
    "руб.",
    "рублей",
    "рубля",
    "$",
    "usd",
    "eur",
    "€",
    "byn",
    "kzt",
    "uah",
}
_PRICE_WORDS = {"цена", "стоимость", "price"}
_UNIT_TOKENS = {"мм", "см", "cm", "mm", "kg", "кг"}
_CURRENCY_REGEX = re.compile(r"(?:₽|руб\.?|\bр\.?\b|\$|€|usd|eur|byn|kzt|uah)", re.IGNORECASE)
_UNIT_REGEX = re.compile(r"\b(?:мм|см|cm|mm|kg|кг)\b", re.IGNORECASE)
_LONG_DIGITS_RE = re.compile(r"\d{4,}")
_PRICE_WINDOW = 2

def _latin_to_cyrillic_lookalikes(s: str) -> str:
    if PdfReader is None:
        return s
    mapping = str.maketrans({
        "A": "А", "a": "а",
        "B": "В", "b": "в",
        "C": "С", "c": "с",
        "E": "Е", "e": "е",
        "H": "Н", "h": "н",
        "K": "К", "k": "к",
        "M": "М", "m": "м",
        "O": "О", "o": "о",
        "P": "Р", "p": "р",
        "T": "Т", "t": "т",
        "X": "Х", "x": "х",
        "Y": "У", "y": "у",
    })
    return s.translate(mapping)


def _collapse_spaced_letters(text: str) -> str:
    if not text:
        return ""

    def repl(match: re.Match[str]) -> str:
        chunk = match.group(0)
        return re.sub(r"\s+", "", chunk)

    return _LETTER_RUN_RE.sub(repl, text)


def _prepare_block_text(text: str) -> str:
    if not text:
        return ""
    prepared = text.replace("\r\n", "\n").replace("\r", "\n")
    prepared = prepared.replace("_", " ")
    prepared = prepared.replace("\u00ad", "")
    prepared = _INVISIBLE_RE.sub("", prepared)
    prepared = _collapse_spaced_letters(prepared)
    prepared = _HYPHEN_LINEBREAK_RE.sub("", prepared)
    # Insert line breaks before obvious key/value starts or "Word 123" patterns,
    # but avoid splitting inside an existing word (require non-letter before).
    prepared = re.sub(
        r"(?<!\n)(?<![^\W\d_])([^\W\d_]{2,}\s*[:–—-]\s+)",
        r"\n\1",
        prepared,
    )
    prepared = re.sub(
        r"(?<!\n)(?<![^\W\d_])(?=[^\W\d_]{2,}\s+\d)",
        "\n",
        prepared,
    )
    prepared = re.sub(r"\n{3,}", "\n\n", prepared)
    return prepared.strip("\n")


def _clean_line_preserve_indent(line: str) -> str:
    if not line:
        return ""
    text = line.replace("\t", " ")
    text = text.replace("\u00ad", "")
    text = text.replace("_", " ")
    text = _INVISIBLE_RE.sub("", text)
    text = _collapse_spaced_letters(text)
    # Map Latin lookalikes to Cyrillic to fix keys like 'BEC' -> 'ВЕС'
    text = _latin_to_cyrillic_lookalikes(text)
    return text.rstrip("\n\r")


def _build_block_lines(text: str) -> List[BlockLine]:
    prepared = _prepare_block_text(text)
    lines: List[BlockLine] = []
    for raw_line in prepared.splitlines():
        cleaned = _clean_line_preserve_indent(raw_line)
        leading_space = bool(cleaned[:1].isspace())
        stripped = cleaned.rstrip()
        clean_value = _sanitize_value(stripped)
        lines.append(BlockLine(raw=raw_line, text=stripped, clean=clean_value, leading_space=leading_space))
    return lines


def _split_blocks(lines: List[BlockLine]) -> List[List[BlockLine]]:
    blocks: List[List[BlockLine]] = []
    current: List[BlockLine] = []
    for line in lines:
        if not line.clean:
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        blocks.append(current)
    return blocks


def _iter_pair_segments(text: str) -> Iterable[str]:
    normalized = (
        text.replace("•", ";")
        .replace("·", ";")
        .replace("∙", ";")
        .replace("●", ";")
        .replace("|", ";")
    )
    for segment in re.split(r"\s*;\s*", normalized):
        cleaned = segment.strip()
        if cleaned:
            yield cleaned


_PAIR_COLON_RE = re.compile(r"^\s*(?P<key>.+?)\s*:\s*(?P<value>.+)$")
_PAIR_DASH_RE = re.compile(r"^\s*(?P<key>.+?)\s*[–—-]\s+(?P<value>.+)$")
_PAIR_EQ_RE = re.compile(r"^\s*(?P<key>.+?)\s*=\s*(?P<value>.+)$")
_PAIR_LEADER_RE = re.compile(r"^\s*(?P<key>[^:–—=]{2,}?)\s*[\.·•]{2,}\s*(?P<value>.+)$")
_PAIR_SPACE_NUM_RE = re.compile(r"^\s*(?P<key>[A-Za-zА-Яа-яЁё\s]{2,}?)\s+(?P<value>\d.+)$")
_EMBEDDED_PAIR_RE = re.compile(r"([^-\W\d_]{2,}(?:\s+[^-\W\d_]{2,})*)\s+(\d.+)")


def _split_embedded_pairs(value: str) -> Tuple[str, List[Tuple[str, str]]]:
    extras: List[Tuple[str, str]] = []
    remaining = value
    base = value
    base_set = False
    while True:
        match = _EMBEDDED_PAIR_RE.search(remaining)
        if not match:
            break
        if not base_set:
            prefix = remaining[: match.start()].rstrip()
            if prefix:
                base = prefix
            base_set = True
        extras.append((match.group(1).strip(), match.group(2).strip().rstrip(".;,*•")))
        remaining = remaining[match.end():].lstrip()
        if not remaining:
            break
    return base.strip(), extras
_EMBEDDED_PAIR_RE = re.compile(r"([^-\W\d_]{2,}(?:\s+[^-\W\d_]{2,})*)\s+(\d.+)")


def _match_pair(segment: str) -> Optional[Tuple[str, str]]:
    for pattern in (_PAIR_COLON_RE, _PAIR_DASH_RE, _PAIR_EQ_RE, _PAIR_LEADER_RE):
        match = pattern.match(segment)
        if match:
            key = re.sub(r"\s+", " ", match.group("key").strip())
            value = match.group("value").strip()
            value = value.rstrip(".;,*•")
            return key, value
    space_match = _PAIR_SPACE_NUM_RE.match(segment)
    if space_match:
        key = re.sub(r"\s+", " ", space_match.group("key").strip())
        value = space_match.group("value").strip().rstrip(".;,*•")
        # Guard against treating titles like "ГАРДА 8" as key/value.
        # Accept this relaxed form only when the numeric value looks like a
        # characteristic: contains units/currency or a dimension separator.
        if not (
            re.search(r"\b(мм|см|mm|cm|kg|кг)\b", value, re.IGNORECASE)
            or re.search(r"[x×*/]", value)
            or re.search(r"(?:₽|руб\.?|\bр\.?\b|\$|€|usd|eur|byn|kzt|uah)", value, re.IGNORECASE)
        ):
            return None
        return key, value
    return None


def _line_has_pair(text: str) -> bool:
    for segment in _iter_pair_segments(text):
        if _match_pair(segment):
            return True
    return False


def _parse_pairs(block: List[BlockLine]) -> Tuple[List[ParsedPair], Set[int]]:
    pairs: List[ParsedPair] = []
    consumed: Set[int] = set()
    idx = 0
    while idx < len(block):
        line = block[idx]
        segments = list(_iter_pair_segments(line.text))
        raw_matches = [match for match in (_match_pair(seg) for seg in segments) if match]
        matches: List[Tuple[str, str]] = []
        for key_raw, value_raw in raw_matches:
            base_value, extra_pairs = _split_embedded_pairs(value_raw)
            if base_value:
                matches.append((key_raw, base_value))
            else:
                matches.append((key_raw, value_raw))
            for extra_key, extra_value in extra_pairs:
                matches.append((extra_key, extra_value))
        if not matches:
            # Handle dangling keys like "Описание:" or "Описание —"
            dangling = re.match(r"^\s*(?P<key>.+?)\s*[:–—-]\s*$", line.text)
            if dangling:
                base_idx = idx
                consumed.add(base_idx)
                key_raw = re.sub(r"\s+", " ", dangling.group("key").strip())
                value_parts: List[str] = []
                look_idx = base_idx + 1
                while look_idx < len(block):
                    next_line = block[look_idx]
                    if not next_line.clean:
                        break
                    if _line_has_pair(next_line.text):
                        break
                    # Stop on obvious new title
                    if _is_title_candidate(next_line.clean) and not next_line.leading_space:
                        break
                    consumed.add(look_idx)
                    value_parts.append(next_line.clean)
                    look_idx += 1
                if value_parts:
                    pairs.append(ParsedPair(key=key_raw, value=_sanitize_value(" ".join(value_parts)), line_index=base_idx))
                    idx = look_idx
                    continue
            idx += 1
            continue
        base_idx = idx
        consumed.add(base_idx)
        if len(matches) == 1:
            key_raw, value_raw = matches[0]
            value_parts = [value_raw]
            look_idx = base_idx + 1
            while look_idx < len(block):
                next_line = block[look_idx]
                if not next_line.clean:
                    break
                # Allow continuation lines even without indentation if
                # they do not introduce a new pair and look like a
                # sentence continuation (start with lowercase or punctuation).
                stripped = next_line.text.lstrip()
                if _line_has_pair(next_line.text):
                    break
                if not (next_line.leading_space or (stripped and (not stripped[0].isalpha() or stripped[0].islower()))):
                    break
                consumed.add(look_idx)
                value_parts.append(next_line.clean)
                look_idx += 1
            matches = [(key_raw, " ".join(value_parts))]
            idx = look_idx
        else:
            idx = base_idx + 1
        for key_raw, value_raw in matches:
            key = re.sub(r"\s+", " ", key_raw).strip()
            value = _sanitize_value(value_raw)
            if not key and not value:
                continue
            pairs.append(ParsedPair(key=key, value=value, line_index=base_idx))
    return pairs, consumed


def _is_currency_token(token: str) -> bool:
    cleaned = token.strip().lower().strip('.').strip(',')
    return cleaned in _CURRENCY_TOKENS


def _is_unit_token(token: str) -> bool:
    cleaned = token.strip().lower().strip('.').strip(',')
    return cleaned in _UNIT_TOKENS


def _normalize_price_value(raw: str) -> str:
    text = raw.strip()
    if not text:
        return ""
    # Keep only digits, separators and whitespace to split possible merged tokens
    cleaned = re.sub(r"[^0-9.,\s]", "", text.replace("\u00a0", " "))
    if not cleaned:
        return ""
    parts = [p for p in re.split(r"\s+", cleaned) if p]
    if len(parts) > 1:
        # If tokens look like grouped thousands (e.g., "45 000" or "1 200 000"), join them.
        digit_tokens = [re.sub(r"\D", "", p) for p in parts]
        if all(t.isdigit() for t in digit_tokens):
            if len(digit_tokens) >= 2 and all(len(t) == 3 for t in digit_tokens[1:]) and 1 <= len(digit_tokens[0]) <= 3:
                cleaned = "".join(digit_tokens)
            else:
                # Otherwise take the longest standalone token
                cleaned = max(digit_tokens, key=len)
        else:
            # Mixed tokens: pick the one with most digits
            def score_part(p: str) -> tuple[int, int]:
                digits = re.sub(r"\D", "", p)
                return (len(digits), int(bool(re.search(r"[,\.]", p))))
            best = max(parts, key=score_part)
            cleaned = best
    cleaned = cleaned.replace(" ", "").replace(",", ".")
    # Drop everything but digits and a single dot
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if not cleaned:
        return ""
    if cleaned.count('.') > 1:
        parts = cleaned.split('.')
        integer = parts[0] + "".join(parts[1:-1])
        fraction = parts[-1]
        cleaned = integer + (f".{fraction}" if fraction else "")
    elif cleaned.count('.') == 1:
        integer, fraction = cleaned.split('.')
        if not fraction:
            cleaned = integer
        elif len(fraction) == 3:
            cleaned = integer + fraction
    return cleaned


def _select_price_candidate(block: List[BlockLine]) -> Optional[PriceCandidate]:
    joined = " ".join(line.text.strip() for line in block if line.text.strip())
    if not joined:
        return None
    tokens = list(_TOKEN_RE.finditer(joined))
    if not tokens:
        return None
    best: Optional[PriceCandidate] = None
    hi_candidates: List[PriceCandidate] = []

    def _is_dimension_context(start: int, end: int) -> bool:
        # Inspect a small window around the number for dimension patterns.
        window_start = max(0, start - 24)
        window_end = min(len(joined), end + 24)
        window = joined[window_start:window_end]
        # Patterns like 860*2050, 960/860*1900, 860×2050, 860x2050
        if re.search(r"\b\d{2,4}\s*[x×*]\s*\d{3,4}\b", window):
            return True
        if re.search(r"\b\d{3,4}\s*/\s*\d{3,4}\s*[x×*]\s*\d{3,4}\b", window):
            return True
        # Common vertical dimensions (e.g., 1700–2400) near separators imply size, not price
        try:
            num = int(re.sub(r"\D", "", joined[start:end]))
            if 1700 <= num <= 2400 and re.search(r"[x×*/]", window):
                return True
        except Exception:
            pass
        return False
    # Add explicit big-number candidates (e.g., "33 200") to avoid fragmented matches
    BIG_NUM_RE = re.compile(r"\b(?:\d{1,3}(?:[\u00a0\s]\d{3})+|\d{4,6})\b")
    for match in list(_NUMBER_RE.finditer(joined)) + list(BIG_NUM_RE.finditer(joined)):
        raw = match.group().strip()
        if not raw:
            continue
        start, end = match.start(), match.end()
        # Skip numbers that are part of obvious dimension patterns
        if _is_dimension_context(start, end):
            continue
        token_indices = [i for i, tok in enumerate(tokens) if tok.start() < end and tok.end() > start]
        if not token_indices:
            continue
        token_start = token_indices[0]
        token_end = token_indices[-1]
        context_range = range(max(0, token_start - _PRICE_WINDOW), min(len(tokens), token_end + _PRICE_WINDOW + 1)) if tokens else range(0, 0)
        context_tokens = [tokens[i].group() for i in context_range] if tokens else []
        lower_tokens = [tok.lower() for tok in context_tokens]
        weight = 0.0
        has_currency = any(_is_currency_token(tok) for tok in context_tokens)
        if has_currency:
            weight += 2.0
        if any(tok in _PRICE_WORDS for tok in lower_tokens):
            weight += 1.5
        digits = re.sub(r"\D", "", raw)
        if len(digits) >= 4 or any(sep in raw for sep in (" ", "'", ".", ",")):
            weight += 1.0
        if any(_is_unit_token(tok) for tok in context_tokens):
            weight -= 2.0
        if not has_currency and digits and digits.isdigit() and int(digits) < 300:
            weight -= 2.0
        # Penalize suspicious 4-digit numbers likely to be sizes when no currency context
        if not has_currency and digits.isdigit():
            try:
                val = int(digits)
                if 1700 <= val <= 2400:
                    weight -= 3.0
            except Exception:
                pass
        normalized = _normalize_price_value(raw)
        if not normalized:
            continue
        # Penalize unrealistic outliers to avoid picking concatenated numbers
        try:
            if float(normalized) > 1_000_000:
                weight -= 5.0
        except Exception:
            pass
        candidate = PriceCandidate(raw=raw, normalized=normalized, weight=weight)
        try:
            if float(candidate.normalized) >= 1000:
                hi_candidates.append(candidate)
        except Exception:
            pass
        if best is None or candidate.weight > best.weight or (
            math.isclose(candidate.weight, best.weight)
            and float(candidate.normalized) > float(best.normalized)
        ):
            best = candidate
    # If the chosen candidate is suspiciously small (<1000) but we saw plausible
    # larger numbers, fallback to the largest plausible price.
    try:
        if (best is None or float(best.normalized) < 1000) and hi_candidates:
            best = max(hi_candidates, key=lambda c: float(c.normalized))
    except Exception:
        pass
    return best


def _is_title_candidate(text: str) -> bool:
    if not text:
        return False
    if ":" in text:
        return False
    if _CURRENCY_REGEX.search(text) or _UNIT_REGEX.search(text):
        return False
    if _STOP_KEYWORDS_RE.search(text):
        return False
    # Avoid generic section headers
    lowered = text.strip().lower()
    if any(token in lowered for token in (
        "характеристик",
        "технические данные",
        "параметр",
        "количество",
        "толщина",
        "размер",
        "ширина",
        "высота",
        "диаметр",
        "цвет",
        "замки",
        "муар",
        "кварц",
        "материал",
        "наполн",
        "в наличии",
    )):
        return False
    normalized = re.sub(r"[^0-9A-Za-zА-Яа-яЁё]", "", text)
    digits = sum(ch.isdigit() for ch in normalized)
    letters = sum(ch.isalpha() for ch in normalized)
    if digits and letters == 0:
        return False
    # Relax numeric allowance to keep common model markers in titles
    if digits > 4:
        effective_len = digits + letters
        if effective_len and digits / effective_len > 0.35:
            return False
    # Avoid picking very short single-word nouns (likely characteristics) as titles
    tokens = [t for t in re.split(r"\s+", text.strip()) if t]
    if len(tokens) <= 1 and digits == 0:
        # Allow collapsed uppercase brand-like words (e.g., from spaced letters)
        stripped = text.strip()
        if len(stripped) <= 12 and not (stripped.isupper() and len(stripped) >= 6):
            return False
    length = len(text.strip())
    if length < 3 or length > 80:
        return False
    return True


def _find_title_candidates(block: List[BlockLine], consumed: Set[int]) -> List[Tuple[int, str]]:
    candidates: List[Tuple[int, str]] = []
    for idx, line in enumerate(block):
        if idx in consumed:
            continue
        candidate = line.clean
        if not candidate:
            continue
        if not _is_title_candidate(candidate):
            continue
        cleaned = _clean_title(candidate)
        if cleaned:
            candidates.append((idx, cleaned))
    return candidates


def _compute_block_score(
    price: Optional[PriceCandidate],
    pairs: List[ParsedPair],
    title_candidates: List[Tuple[int, str]],
    text: str,
    chunk_title_candidate: str,
) -> float:
    score = 0.0
    if price and price.weight > 0:
        score += 2.0
    if len(pairs) >= 2:
        score += 1.0
    if title_candidates:
        score += 1.0
    has_stop = bool(_STOP_KEYWORDS_RE.search(text))
    if has_stop:
        score -= 2.0
    if score < 2 and chunk_title_candidate and pairs and ":" in text and not has_stop:
        score = max(score, 2.0)
    return score


def _build_product_block(chunk: CatalogChunk, block_lines: List[BlockLine], index: int) -> ProductBlock:
    pairs, consumed = _parse_pairs(block_lines)
    title_candidates = _find_title_candidates(block_lines, consumed)
    price_candidate = _select_price_candidate(block_lines)
    text = "\n".join(line.text.strip() or line.text for line in block_lines)
    chunk_title_candidate = _clean_title(chunk.title)
    if chunk_title_candidate:
        normalized_existing = {cand.lower() for _, cand in title_candidates}
        if chunk_title_candidate.lower() not in normalized_existing and _is_title_candidate(chunk_title_candidate):
            title_candidates.append((-1, chunk_title_candidate))
        else:
            # Heuristic: salvage a leading Latin model marker when the chunk title
            # is contaminated by concatenated attribute text (e.g., "Model ALPHA-100Цвет:")
            m = re.match(r"^[A-Za-z0-9][A-Za-z0-9\-\s]{3,}", chunk.title.strip())
            if m:
                prefix = _clean_title(m.group(0))
                if prefix and prefix.lower() not in normalized_existing and _is_title_candidate(prefix):
                    title_candidates.append((-1, prefix))
    score = _compute_block_score(price_candidate, pairs, title_candidates, text, chunk_title_candidate)
    return ProductBlock(
        chunk_id=chunk.chunk_id,
        chunk_page=chunk.page,
        chunk_title=chunk.title,
        chunk_title_candidate=chunk_title_candidate,
        index=index,
        lines=block_lines,
        text=text,
        pairs=pairs,
        consumed_indices=consumed,
        score=score,
        price=price_candidate,
        title_candidates=title_candidates,
    )


def _select_block_title(block: ProductBlock) -> str:
    attr_names = {pair.key.strip().lower() for pair in block.pairs if pair.key}
    # Also avoid picking attribute VALUES as titles (e.g., color codes like "9005 муар" or
    # single-word characteristics like "Царга").
    attr_values = {_clean_title(pair.value).strip().lower() for pair in block.pairs if pair.value}

    scored: List[Tuple[float, str]] = []
    for idx, candidate in block.title_candidates:
        if idx >= 0 and idx in block.consumed_indices:
            continue
        cleaned = _clean_title(candidate)
        if not cleaned:
            continue
        if _title_contains_forbidden(cleaned):
            continue
        lower = cleaned.lower()
        # Skip obvious attribute headers/values
        if lower in attr_names or lower in attr_values:
            continue
        # Heuristic scoring: prefer multi-word, alnum-mixed, longer titles
        tokens = [t for t in re.split(r"\s+", cleaned) if t]
        has_letters = any(ch.isalpha() for ch in cleaned)
        has_digits = any(ch.isdigit() for ch in cleaned)
        score = 0.0
        if len(tokens) >= 2:
            score += 3.0
        if has_letters and has_digits:
            score += 2.0
        if len(cleaned) >= 6:
            score += 1.0
        if len(tokens) == 1:
            score -= 2.0
        if cleaned.isupper() and len(tokens) == 1:
            score -= 1.0
        # Penalize single-word colors/materials as titles
        if len(tokens) == 1:
            low = cleaned.lower()
            bad_single = {
                "черный", "чёрный", "белый", "серый", "графит", "венге", "дуб",
                "бук", "букле", "муар", "кварц", "лиственница", "лиственничная",
                "царга", "панель", "панно", "зеркало",
            }
            if low in bad_single:
                score -= 4.0
        # Slightly prefer in-block candidates over chunk-level guesses
        if idx >= 0:
            score += 3.0
            if cleaned.isupper() and len(cleaned) >= 6:
                score += 5.0
        scored.append((score, cleaned))

        if scored:
            # Try to avoid returning penalized single-word colors/materials when a
            # better fallback exists (use chunk title instead).
            best = max(scored, key=lambda s: (s[0], len(s[1])))
            candidate = best[1]
            cand_tokens = [t for t in re.split(r"\s+", candidate) if t]
            bad_single = {
                "черный", "чёрный", "белый", "серый", "графит", "венге", "дуб",
                "бук", "букле", "муар", "кварц", "лиственница", "лиственничная",
                "царга", "панель", "панно", "зеркало",
            }
            if len(cand_tokens) == 1 and candidate.lower() in bad_single:
                chunk_title = _clean_title(block.chunk_title)
                if (
                    chunk_title
                    and _is_title_candidate(chunk_title)
                    and chunk_title.lower() not in attr_names
                    and chunk_title.lower() not in attr_values
                    and not _title_contains_forbidden(chunk_title)
                ):
                    return chunk_title
            if best[0] > -1.5:  # ensure we don't pick very weak single-token values
                return candidate

    if block.pairs:
        first_pair_idx = min(pair.line_index for pair in block.pairs)
        for idx in range(first_pair_idx - 1, -1, -1):
            if idx in block.consumed_indices:
                continue
            candidate = _clean_title(block.lines[idx].clean)
            if not candidate:
                continue
            if candidate.lower() in attr_names:
                continue
            if _title_contains_forbidden(candidate):
                continue
            return candidate

    chunk_title = _clean_title(block.chunk_title)
    if (
        chunk_title
        and _is_title_candidate(chunk_title)
        and chunk_title.lower() not in attr_names
        and chunk_title.lower() not in attr_values
        and not _title_contains_forbidden(chunk_title)
    ):
        return chunk_title

    return f"Позиция {block.index + 1} (стр. {block.chunk_page})"


def _block_to_item(block: ProductBlock) -> Dict[str, str]:
    attributes: Dict[str, str] = {}
    for pair in block.pairs:
        if not pair.key:
            continue
        if pair.key not in attributes or not attributes[pair.key]:
            attributes[pair.key] = pair.value

    title = _select_block_title(block)
    price_value = ""
    if block.price and block.price.weight > 0:
        price_value = block.price.normalized

    def _normalize_attr_key(key: str, value: str) -> str:
        raw = str(key or "").strip()
        # keep Russian letters/digits only, collapse to single token
        raw = _latin_to_cyrillic_lookalikes(raw)
        raw = raw.replace("ё", "е")
        cleaned = re.sub(r"[^0-9A-Za-zА-Яа-яЁё]+", "", raw)
        lowered = cleaned.lower()
        if not lowered:
            return "attr"
        # Primary semantic prefixes to strip → leave object as key
        qty_prefixes = (
            "количество",
            "толщина",
            "размеры",
            "размер",
            "диаметр",
            "ширина",
            "высота",
            "длина",
            "вес",
            "масса",
        )
        # Descriptive prefixes to keep (prepend to avoid collision)
        desc_prefixes = ("тип", "цвет", "материал", "наполнение")

        for pref in desc_prefixes:
            if lowered.startswith(pref) and len(lowered) > len(pref):
                suffix = lowered[len(pref):]
                return f"{pref}{suffix}"
        for pref in qty_prefixes:
            if lowered.startswith(pref) and len(lowered) > len(pref):
                suffix = lowered[len(pref):]
                # Drop common glue like 'вналичиипо' or 'по'
                for glue in ("вналичиипо", "вналичии", "по"):
                    if suffix.startswith(glue):
                        suffix = suffix[len(glue):]
                return suffix or lowered
        # Special handling for lock-related keys: collapse to either 'замков' or 'типзамков'
        if ("замк" in lowered) or ("замок" in lowered) or ("замки" in lowered):
            # If value looks numeric → quantity of locks
            val = str(value or "").strip()
            digits = re.sub(r"\D", "", val)
            if digits:
                return "замков"
            # Otherwise treat as type/model, normalize to 'типзамков'
            return "типзамков"
        return lowered

    # Normalize attribute keys to single-token, disambiguating 'тип...' vs 'количество...'
    normalized_attrs: Dict[str, str] = {}
    for key, value in attributes.items():
        norm_key = _normalize_attr_key(key, value)
        if norm_key not in normalized_attrs or not normalized_attrs[norm_key]:
            normalized_attrs[norm_key] = value

    item: Dict[str, str] = {
        "title": title,
        "price": price_value,
        "page": str(block.chunk_page),
    }

    def _pretty_attr_key(norm: str) -> str:
        mapping = {
            "материал": "Материал",
            "цвет": "Цвет",
            "описание": "Описание",
        }
        return mapping.get(norm, norm)

    for key, value in normalized_attrs.items():
        disp_key = _pretty_attr_key(key)
        if value:
            item[disp_key] = value

    for key, value in list(item.items()):
        if key == "price":
            continue
        item[key] = _sanitize_value(value)

    return item


def _write_csv(index_path: Path, header: List[str], rows: List[Dict[str, str]]) -> Path:
    csv_path = index_path.with_suffix(".csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    # Standard CSV for index export: UTF-8 (no BOM) and comma delimiter
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=header,
            quoting=csv.QUOTE_MINIMAL,
            delimiter=",",
        )
        writer.writeheader()
        for row in rows:
            # Flatten newlines/tabs to spaces for CSV consumers
            cleaned = {}
            for key in header:
                val = row.get(key, "")
                text = str(val or "")
                if text:
                    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
                    text = re.sub(r"\s+", " ", text).strip()
                cleaned[key] = text
            writer.writerow(cleaned)
    return csv_path


def _write_manifest(index_path: Path, data: Dict[str, Any]) -> Path:
    manifest_path = index_path.with_suffix(".manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path

@dataclass(frozen=True)
class CatalogIndex:
    catalog_id: str
    source_path: str
    original_name: str | None
    generated_at: int
    sha1: str
    page_count: int
    chunk_count: int
    chunks: Sequence[CatalogChunk]
    index_path: Path

    def to_dict(self) -> Dict[str, Any]:
        return {
            "catalog_id": self.catalog_id,
            "source_path": self.source_path,
            "original_name": self.original_name,
            "generated_at": self.generated_at,
            "sha1": self.sha1,
        "page_count": self.page_count,
        "chunk_count": self.chunk_count,
        "chunks": [chunk.to_dict() for chunk in self.chunks],
        "index_path": str(self.index_path),
    }


def _hash_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_pdf_index(
    source: Path,
    *,
    output_dir: Path,
    source_relpath: str,
    original_name: str | None = None,
    chunk_chars: int = 700,
    overlap: int = 120,
) -> CatalogIndex:
    source = source.resolve()
    if not source.exists():
        raise CatalogIndexError(f"source file not found: {source}")

    pages = _extract_pdf_pages(source)
    page_count = max((idx for idx, _ in pages), default=0)

    chunks: List[CatalogChunk] = []
    for idx, extracted in pages:
        text = _normalize_whitespace(extracted)
        if not text:
            continue
        for part in _chunk_text(text, max_chars=chunk_chars, overlap=overlap):
            chunk_id = uuid.uuid4().hex
            identifiers = _extract_identifiers(part)
            title = _guess_title(part, page=idx)
            chunks.append(
                CatalogChunk(
                    chunk_id=chunk_id,
                    page=idx,
                    title=title,
                    text=part,
                    identifiers=identifiers,
                )
            )

    if not chunks:
        raise CatalogIndexError("catalog did not produce any text chunks")

    sha1 = _hash_file(source)
    generated_at = int(time.time())
    catalog_id = uuid.uuid4().hex

    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / f"catalog_{catalog_id}.json"

    payload = {
        "format": 1,
        "catalog_id": catalog_id,
        "source_path": source_relpath,
        "original_name": original_name,
        "generated_at": generated_at,
        "sha1": sha1,
        "page_count": page_count,
        "chunk_count": len(chunks),
        "chunks": [chunk.to_dict() for chunk in chunks],
    }

    index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return CatalogIndex(
        catalog_id=catalog_id,
        source_path=source_relpath,
        original_name=original_name,
        generated_at=generated_at,
        sha1=sha1,
        page_count=page_count,
        chunk_count=len(chunks),
        chunks=chunks,
        index_path=index_path,
    )


def load_index(path: Path) -> CatalogIndex:
    data = json.loads(path.read_text(encoding="utf-8"))
    chunks = [
        CatalogChunk(
            chunk_id=chunk.get("id", uuid.uuid4().hex),
            page=int(chunk.get("page", 0) or 0),
            title=str(chunk.get("title") or ""),
            text=str(chunk.get("text") or ""),
            identifiers=tuple(chunk.get("identifiers") or ()),
        )
        for chunk in data.get("chunks", [])
    ]
    if not chunks:
        raise CatalogIndexError(f"index {path} does not contain chunks")
    return CatalogIndex(
        catalog_id=data.get("catalog_id") or "",
        source_path=data.get("source_path") or "",
        original_name=data.get("original_name"),
        generated_at=int(data.get("generated_at", 0) or 0),
        sha1=data.get("sha1") or "",
        page_count=int(data.get("page_count", 0) or 0),
        chunk_count=len(chunks),
        chunks=chunks,
        index_path=path,
    )


def index_to_catalog_items(index: CatalogIndex) -> List[Dict[str, Any]]:
    blocks: List[ProductBlock] = []
    for chunk in index.chunks:
        if not chunk.text:
            continue
        lines = _build_block_lines(chunk.text)
        for idx, block_lines in enumerate(_split_blocks(lines)):
            if not block_lines:
                continue
            block = _build_product_block(chunk, block_lines, idx)
            blocks.append(block)

    total_blocks = len(blocks)
    def _has_inblock_title(block: ProductBlock) -> bool:
        return any(idx >= 0 for idx, _ in block.title_candidates)

    kept_blocks: List[ProductBlock] = []
    for block in blocks:
        has_price = bool(block.price and block.price.weight > 0)
        # Drop obvious non-product sections without price
        if not has_price and _STOP_KEYWORDS_RE.search(block.text):
            continue
        # Keep any reasonably product-like block (score>=2) or any with price
        if has_price or block.score >= 2:
            kept_blocks.append(block)
    if not kept_blocks and blocks:
        # Fallback: pick the most attribute-rich non-stop block to avoid
        # returning an empty catalog for slim PDFs without explicit prices.
        candidates = [b for b in blocks if not _STOP_KEYWORDS_RE.search(b.text)]
        if candidates:
            best = max(candidates, key=lambda b: (len(b.pairs), int(bool(b.title_candidates))))
            if len(best.pairs) >= 1:
                kept_blocks = [best]
    dropped_blocks = [block for block in blocks if block.score < 2]

    log_entries = [
        {
            "block_id": f"{block.chunk_id}:{block.index}",
            "chunk_id": block.chunk_id,
            "page": block.chunk_page,
            "score": block.score,
            "reason": "non_product",
            "text": block.text,
        }
        for block in dropped_blocks
    ]

    raw_items = [_block_to_item(block) for block in kept_blocks]

    items: List[Dict[str, Any]] = []
    if raw_items:
        # 1) Run normalization to get unique titles and merged column map
        try:
            finalized_rows, pipeline_header, report = _finalize_catalog_rows(raw_items, keep_existing_ids=False)
        except Exception:
            pipeline_header = ["id", "title", "price"]
            report = PipelineReport(items=len(raw_items), columns=pipeline_header)
            finalized_rows = [{"title": r.get("title", "")} for r in raw_items]

        # 2) Build rich items preserving parsed characteristics and page
        items = []
        merged_map = getattr(report, "merged_columns_map", {}) or {}
        for i, row in enumerate(raw_items, start=1):
            item = dict(row)
            item.setdefault("id", str(i))
            # Apply title updates (uniqueness fixes)
            try:
                finalized_title = finalized_rows[i - 1].get("title")
                if finalized_title:
                    item["title"] = finalized_title
            except Exception:
                pass
            # Apply merged column aliases (e.g., Материалы -> Материал)
            for alias, canonical in merged_map.items():
                if alias in item and (canonical not in item or not item.get(canonical)):
                    item[canonical] = item.pop(alias)
            items.append(item)

        # 3) Write index CSV with full set of characteristics (unlike pipeline CSV)
        attr_keys: set[str] = set()
        for it in items:
            for k in it.keys():
                if k not in {"id", "title", "price", "page"}:
                    attr_keys.add(k)
        header_rich = ["id", "title", "price", *sorted(attr_keys)]
        _write_csv(index.index_path, header_rich, items)
    else:
        header_rich = ["id", "title", "price"]
        report = PipelineReport(items=0, columns=header_rich)
        _write_csv(index.index_path, header_rich, [])

    missing_price_blocks = [block for block in kept_blocks if not (block.price and block.price.weight > 0)]
    price_examples: List[Dict[str, Any]] = []
    if kept_blocks:
        missing_ratio = len(missing_price_blocks) / max(len(kept_blocks), 1)
        if missing_ratio > 0.2:
            for block in missing_price_blocks[:5]:
                price_examples.append(
                    {
                        "block_id": f"{block.chunk_id}:{block.index}",
                        "text": block.text,
                    }
                )

    # Present merged_columns_map with display-friendly keys (capitalize if needed)
    merged_map_display = {}
    try:
        for k, v in (report.merged_columns_map or {}).items():
            dk = k[0].upper() + k[1:] if isinstance(k, str) and k.islower() else k
            merged_map_display[dk] = v
    except Exception:
        merged_map_display = report.merged_columns_map

    manifest: Dict[str, Any] = {
        "items_total": total_blocks,
        "kept": len(kept_blocks),
        "dropped_non_product": len(dropped_blocks),
        "columns": header_rich,
        "pipeline": report.to_dict(),
        "merged_columns_map": merged_map_display,
        "logs": log_entries,
    }
    if report.duplicate_titles_fixed:
        manifest["duplicate_titles_fixed"] = [
            {"from": original, "to": updated}
            for original, updated in report.duplicate_titles_fixed
        ]
    if price_examples:
        manifest["price_missing_examples"] = price_examples

    _write_manifest(index.index_path, manifest)

    return items
