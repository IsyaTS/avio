#!/usr/bin/env python
"""
Export per-tenant training examples (SFT-ready JSONL).
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import List, Dict

from libs.core import db


async def _load_examples(tenant: int) -> List[Dict]:
    rows = await db.get_training_examples_for_retrieval(tenant, limit=10000)
    usable = []
    for row in rows:
        if row.get("is_bad"):
            continue
        if (row.get("source") or "") not in {"like", "correction", "manual"}:
            continue
        usable.append(row)
    return usable


def _to_sft_row(example: Dict) -> Dict:
    return {
        "messages": [
            {"role": "user", "content": example.get("q_text") or ""},
            {"role": "assistant", "content": example.get("a_text") or ""},
        ],
        "meta": {
            "source": example.get("source"),
            "id": example.get("id"),
        },
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", type=int, required=True, help="Tenant id")
    parser.add_argument("--output", type=Path, required=True, help="Path to JSONL file")
    args = parser.parse_args()

    examples = await _load_examples(args.tenant)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        for ex in examples:
            fh.write(json.dumps(_to_sft_row(ex), ensure_ascii=False) + "\n")
    print(f"written {len(examples)} examples to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
