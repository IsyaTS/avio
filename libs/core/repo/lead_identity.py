from __future__ import annotations

import re
from typing import Any

from libs.core import db as db_module


_AVITO_FALLBACK_RE = re.compile(r"^\s*avito\s*[·\-]\s*клиент", re.IGNORECASE)


def is_placeholder_display_name(value: Any, *, peer: Any = None) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    if text.isdigit():
        return True
    if _AVITO_FALLBACK_RE.match(text):
        return True
    peer_text = str(peer or "").strip()
    return bool(peer_text and text == peer_text)


async def update_avito_lead_contact_if_placeholder(
    tenant_id: int,
    lead_id: int,
    name: str,
) -> bool:
    clean_name = str(name or "").strip()
    if not clean_name or clean_name.isdigit():
        return False
    fetchrow = getattr(db_module, "_fetchrow", None)
    exec_fn = getattr(db_module, "_exec", None)
    if not fetchrow or not exec_fn:
        return False
    row = await fetchrow(
        """
        SELECT contact, title, peer
        FROM leads
        WHERE tenant_id = $1 AND id = $2
        LIMIT 1
        """,
        int(tenant_id),
        int(lead_id),
    )
    if not row:
        return False
    contact = row.get("contact") if hasattr(row, "get") else None
    title = row.get("title") if hasattr(row, "get") else None
    peer = row.get("peer") if hasattr(row, "get") else None
    update_contact = is_placeholder_display_name(contact, peer=peer)
    update_title = is_placeholder_display_name(title, peer=peer)
    if not update_contact and not update_title:
        return False
    await exec_fn(
        """
        UPDATE leads
        SET contact = CASE WHEN $3 THEN $5 ELSE contact END,
            title = CASE WHEN $4 THEN $5 ELSE title END,
            updated_at = now()
        WHERE tenant_id = $1 AND id = $2
        """,
        int(tenant_id),
        int(lead_id),
        bool(update_contact),
        bool(update_title),
        clean_name,
    )
    return True


__all__ = ["is_placeholder_display_name", "update_avito_lead_contact_if_placeholder"]
