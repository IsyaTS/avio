#!/usr/bin/env python
"""
Minimal stub for launching an OpenAI finetune job (per-tenant).

This script is intentionally a no-op unless explicitly invoked with a dataset path.
Fine-tune stays DISABLED by default; toggle via tenant_models.use_finetune when ready.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    import openai  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    openai = None

from libs.core import db


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--dataset", type=Path, required=True, help="Path to JSONL dataset for finetune")
    parser.add_argument("--base-model", default=os.getenv("OPENAI_MODEL") or "gpt-4.1")
    args = parser.parse_args()

    if openai is None:
        raise SystemExit("openai package is missing; install to run finetune")
    if not args.dataset.exists():
        raise SystemExit(f"dataset not found: {args.dataset}")

    # Placeholder: load dataset and print summary. Real fine-tune submission is left to ops.
    total = sum(1 for _ in args.dataset.open("r", encoding="utf-8"))
    print(f"[dry-run] tenant={args.tenant} base_model={args.base_model} examples={total}")
    print("Upload the dataset to OpenAI manually and store the resulting model id in tenant_models.finetune_model")

    # Ensure tenant_models row exists
    await db._exec(  # type: ignore[attr-defined]
        """
        INSERT INTO tenant_models(tenant_id, base_model, finetune_model, use_finetune)
        VALUES($1, $2, NULL, FALSE)
        ON CONFLICT (tenant_id) DO UPDATE
          SET base_model = EXCLUDED.base_model,
              updated_at = now();
        """,
        args.tenant,
        args.base_model,
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
