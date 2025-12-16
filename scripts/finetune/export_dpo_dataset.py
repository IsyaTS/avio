#!/usr/bin/env python
"""
Export per-tenant DPO-style dataset from dislikes + corrections.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import List, Dict

from libs.core import db


async def _load_corrections(tenant: int) -> List[Dict]:
    rows = await db._fetch(  # type: ignore[attr-defined]
        """
        SELECT te.q_text,
               te.a_text AS chosen,
               m.text AS rejected,
               te.id AS training_id,
               mf.id AS feedback_id
        FROM training_examples te
        JOIN message_feedback mf ON mf.id = te.source_feedback_id
        LEFT JOIN messages m ON m.id = mf.message_id
        WHERE te.tenant_id = $1
          AND te.source = 'correction'
          AND te.is_bad = FALSE
          AND te.is_active = TRUE
        ORDER BY te.updated_at DESC, te.id DESC;
        """,
        tenant,
    )
    out: List[Dict] = []
    for row in rows or []:
        record = dict(row)
        if not record.get("q_text") or not record.get("chosen") or not record.get("rejected"):
            continue
        out.append(record)
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    corrections = await _load_corrections(args.tenant)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        for row in corrections:
            payload = {
                "prompt": row.get("q_text") or "",
                "chosen": row.get("chosen") or "",
                "rejected": row.get("rejected") or "",
                "meta": {
                    "training_id": row.get("training_id"),
                    "feedback_id": row.get("feedback_id"),
                },
            }
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    print(f"written {len(corrections)} preference pairs to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
