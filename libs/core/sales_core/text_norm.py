from __future__ import annotations

import re
from typing import Any, List


_LAT_TO_LAT = {chr(c): chr(c) for c in range(ord("a"), ord("z") + 1)}
_CYR_TO_LAT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "i",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "c",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


def normalize_text(value: Any) -> str:
    text = str(value or "")
    return text.casefold().replace("ё", "е")


def match_key(value: Any) -> str:
    text = normalize_text(value)
    out: List[str] = []
    for ch in text:
        if ch in _LAT_TO_LAT:
            out.append(_LAT_TO_LAT[ch])
            continue
        mapped = _CYR_TO_LAT.get(ch)
        if mapped is not None:
            out.append(mapped)
            continue
        if ch.isdigit():
            out.append(ch)
            continue
        if ch in {" ", "-", "_", "/"}:
            out.append(" ")
    return re.sub(r"\s+", " ", "".join(out)).strip()
