from __future__ import annotations

import re
from typing import Any, Dict, List


def format_items_for_prompt(items: List[Dict[str, Any]], currency: str = "₽") -> str:
    if not items:
        return ""
    out = []
    for idx, it in enumerate(items, start=1):
        title = (
            it.get("title") or it.get("name") or it.get("sku") or it.get("id") or f"Позиция {idx}"
        )
        raw_price = str(it.get("price") or "").strip()
        price_match = re.search(r"\d[\d\s.,]*", raw_price)
        digits = re.sub(r"\D", "", price_match.group(0)) if price_match else ""
        if digits:
            try:
                price_fmt = f"{int(digits):,}".replace(",", " ")
            except Exception:
                price_fmt = raw_price
        else:
            price_fmt = raw_price or "цена по запросу"

        details: List[str] = []
        if it.get("brand"):
            details.append(str(it.get("brand")).strip())
        if it.get("width"):
            details.append(f"{it['width']} см")
        if it.get("color"):
            details.append(str(it.get("color")).strip())
        stock = it.get("stock")
        if stock is not None and str(stock).strip():
            try:
                stock_val = int(str(stock).strip())
                if stock_val > 0:
                    details.append("в наличии")
            except Exception:
                details.append(str(stock))
        url = (it.get("url") or "").strip()
        meta = f" ({', '.join(details)})" if details else ""
        line = f"{idx}. {title} — {price_fmt} {currency}{meta}"
        rag_score = it.get("_rag_score")
        if isinstance(rag_score, (int, float)) and rag_score > 0:
            line += f" (релевантность {rag_score:.2f})"
        if url:
            line += f" · {url}"
        excerpt = str(it.get("_match_excerpt") or "").strip()
        if excerpt:
            line = f"{line}\n   ↳ {excerpt}"
        out.append(line)
    return "\n".join(out)


def format_needs_for_prompt(needs: Dict[str, Any]) -> str:
    if not needs:
        return ""
    parts = []
    for k in ["type", "width", "color", "budget_max"]:
        if k in needs:
            parts.append(f"{k}={needs[k]}")
    return ", ".join(parts) if parts else ""
