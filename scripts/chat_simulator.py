#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
from dataclasses import dataclass

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

if load_dotenv:
    load_dotenv(ROOT_DIR / ".env")

from app import core


@dataclass
class SessionConfig:
    tenant: int
    contact: int
    channel: str
    show_messages: bool


def parse_args() -> SessionConfig:
    parser = argparse.ArgumentParser(
        description="Локальная имитация чата с ботом без подключения к внешним каналам."
    )
    parser.add_argument("--tenant", type=int, default=1, help="ID арендатора (по умолчанию 1)")
    parser.add_argument("--contact", type=int, default=9999, help="ID контакта для хранения состояния диалога")
    parser.add_argument(
        "--channel",
        type=str,
        default="whatsapp",
        help="Название канала (whatsapp/telegram/avito и т.д.)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Сбросить состояние диалога перед началом сессии.",
    )
    parser.add_argument(
        "--show-messages",
        action="store_true",
        help="Печатать системный промпт и историю, которые отправляются в LLM.",
    )
    args = parser.parse_args()

    if args.reset:
        core.reset_sales_state(args.tenant, args.contact)

    return SessionConfig(
        tenant=args.tenant,
        contact=args.contact,
        channel=args.channel.strip() or "whatsapp",
        show_messages=args.show_messages,
    )


async def ask_bot(cfg: SessionConfig, text: str) -> str:
    messages = await core.build_llm_messages(
        cfg.contact,
        text,
        channel=cfg.channel,
        tenant=cfg.tenant,
    )
    if cfg.show_messages:
        print("--- LLM messages ---")
        print(json.dumps(messages, ensure_ascii=False, indent=2))
        print("--- end ---")
    reply = await core.ask_llm(
        messages,
        tenant=cfg.tenant,
        contact_id=cfg.contact,
        channel=cfg.channel,
    )
    return reply


def main() -> None:
    cfg = parse_args()
    print(
        f"Имитация чата. Tenant={cfg.tenant}, contact={cfg.contact}, канал={cfg.channel}. "
        "Введите сообщение, 'reset' для сброса, 'quit' или Ctrl+C для выхода."
    )

    while True:
        try:
            user_input = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nЗавершено.")
            return

        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit"}:
            print("Завершено.")
            return
        if user_input.lower() == "reset":
            core.reset_sales_state(cfg.tenant, cfg.contact)
            print("Состояние диалога сброшено.")
            continue

        try:
            reply = asyncio.run(ask_bot(cfg, user_input))
        except Exception as exc:  # noqa: BLE001
            print(f"[ошибка] {exc}")
            continue

        print(f"Бот: {reply}")


if __name__ == "__main__":
    main()
