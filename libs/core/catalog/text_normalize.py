from __future__ import annotations

"""Text normalization helpers for catalog parsing.

References:
    - Unicode Normalization Form KC (NFKC): https://unicode.org/reports/tr15/
    - Unicode security guidelines on confusables: https://unicode.org/reports/tr39/
"""

from typing import Dict
import re
import unicodedata

_SPACE_TRANSLATION = {
    ord("\u00A0"): " ",  # no-break space
    ord("\u202F"): " ",  # narrow no-break space
    ord("\u2009"): " ",  # thin space
}

_DASH_TRANSLATION = {
    ord("\u2010"): "-",
    ord("\u2011"): "-",
    ord("\u2012"): "-",
    ord("\u2013"): "-",
    ord("\u2014"): "-",
    ord("\u2015"): "-",
    ord("\u2212"): "-",
    ord("\u2043"): "-",
}

_CONFUSABLES_MAP: Dict[int, str] = str.maketrans(
    {
        "A": "А",  # Latin A -> Cyrillic А
        "a": "а",
        "B": "В",
        "E": "Е",
        "e": "е",
        "K": "К",
        "k": "к",
        "M": "М",
        "H": "Н",
        "O": "О",
        "o": "о",
        "P": "Р",
        "p": "р",
        "C": "С",
        "c": "с",
        "T": "Т",
        "X": "Х",
        "x": "х",
        "Y": "У",
        "y": "у",
    }
)

_DECIMAL_PATTERN = re.compile(r"(\d)\s*([.,])\s*(\d)")


def normalize_unicode_nfkc(text: str | None) -> str:
    """Return text normalized to NFKC (https://unicode.org/reports/tr15/)."""

    if text is None:
        return ""
    return unicodedata.normalize("NFKC", text)


def collapse_spaces(text: str | None) -> str:
    """Collapse thin/nbsp characters into ASCII spaces and squeeze whitespace."""

    if not text:
        return ""
    normalized = normalize_unicode_nfkc(text).translate(_SPACE_TRANSLATION)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def unify_dashes_and_decimals(text: str | None) -> str:
    """Convert dash variants to '-' and normalize spaced decimals like '7 . 5' -> '7.5'."""

    if not text:
        return ""
    collapsed = normalize_unicode_nfkc(text)
    collapsed = collapsed.translate({**_SPACE_TRANSLATION, **_DASH_TRANSLATION})

    def _decimal_repl(match: re.Match[str]) -> str:
        left, _, right = match.groups()
        return f"{left}.{right}"

    collapsed = _DECIMAL_PATTERN.sub(_decimal_repl, collapsed)
    # Collapse duplicated spaces introduced by replacements
    collapsed = re.sub(r"\s+", " ", collapsed)
    return collapsed.strip()


def strip_confusables(text: str | None) -> str:
    """Normalize obvious Latin/Cyrillic confusables (UTS #39 https://unicode.org/reports/tr39/)."""

    if text is None:
        return ""
    normalized = normalize_unicode_nfkc(text)
    return normalized.translate(_CONFUSABLES_MAP)

