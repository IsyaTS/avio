#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.core import db
from libs.core.integrations import avito as avito_integration
from libs.core.repo import lead_identity
from libs.core.services import avito_contact_identity_resolver


async def _noop(*_args: Any, **_kwargs: Any) -> None:
    return None


async def _noop_update_lead(*_args: Any, **_kwargs: Any) -> bool:
    return False


async def _load_candidates(tenant_id: int, limit: int) -> list[dict[str, Any]]:
    fetch = getattr(db, "_fetch", None)
    if not fetch:
        return []
    rows = await fetch(
        """
        SELECT l.id AS lead_id,
               l.contact,
               l.peer,
               l.source_real_id AS account_id,
               lc.contact_id,
               c.avito_user_id,
               c.avito_login
        FROM leads l
        LEFT JOIN lead_contacts lc ON lc.lead_id = l.id
        LEFT JOIN contacts c ON c.id = lc.contact_id
        WHERE l.tenant_id = $1
          AND l.channel = 'avito'
          AND NULLIF(trim(COALESCE(l.peer, '')), '') IS NOT NULL
          AND l.source_real_id IS NOT NULL
          AND (
                NULLIF(trim(COALESCE(l.contact, '')), '') IS NULL
             OR trim(COALESCE(l.contact, '')) ~ '^[0-9]+$'
             OR NULLIF(trim(COALESCE(c.avito_login, '')), '') IS NULL
             OR trim(COALESCE(c.avito_login, '')) ~ '^[0-9]+$'
          )
        ORDER BY l.updated_at DESC
        LIMIT $2
        """,
        int(tenant_id),
        int(limit),
    )
    return [dict(row) for row in rows or []]


async def _run(args: argparse.Namespace) -> int:
    await db.init_db()
    candidates = await _load_candidates(int(args.tenant), int(args.limit))
    resolved = 0
    failed = 0
    skipped = 0
    for row in candidates:
        lead_id = int(row.get("lead_id") or 0)
        account_id = int(row.get("account_id") or 0)
        chat_id = str(row.get("peer") or "").strip()
        contact_id = row.get("contact_id")
        author_id = row.get("avito_user_id")
        if not lead_id or not account_id or not chat_id:
            skipped += 1
            continue
        deps = avito_contact_identity_resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=avito_integration.resolve_chat_participant_profile,
            update_contact_avito_login_fn=db.update_contact_avito_login if args.apply else _noop,
            update_lead_contact_fn=lead_identity.update_avito_lead_contact_if_placeholder
            if args.apply
            else _noop_update_lead,
            redis_client=None,
            log_fn=lambda *_a, **_kw: None,
        )
        result = await avito_contact_identity_resolver.resolve_and_store_avito_contact_identity(
            avito_contact_identity_resolver.AvitoContactIdentityInput(
                tenant_id=int(args.tenant),
                lead_id=lead_id,
                contact_id=int(contact_id) if contact_id is not None else None,
                account_id=account_id,
                chat_id=chat_id,
                author_id=int(author_id) if author_id is not None else None,
                current_login=row.get("avito_login"),
                current_contact=row.get("contact"),
            ),
            deps=deps,
        )
        if result.resolved and result.name:
            resolved += 1
        elif result.reason == "missing_context":
            skipped += 1
        else:
            failed += 1
        if args.sleep > 0:
            await asyncio.sleep(float(args.sleep))
    mode = "apply" if args.apply else "dry_run"
    print(
        "avito_identity_backfill "
        f"mode={mode} tenant={int(args.tenant)} scanned={len(candidates)} "
        f"resolved={resolved} skipped={skipped} failed={failed}"
    )
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Avito contact display names.")
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run(_parse_args())))
