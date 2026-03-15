#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

if load_dotenv:
    load_dotenv(ROOT_DIR / ".env")

from libs.core import sales_core as core


@dataclass
class Violation:
    case_name: str
    iteration: int
    turn: int
    rule: str
    user_text: str
    bot_text: str


_DEFAULT_HUMAN_RULES: Dict[str, Any] = {
    "forbidden_substrings": [
        "ваш запрос принят",
        "отвечаю по запросу",
        "благодарим за обращение",
        "готов предоставить",
        "в рамках вашего запроса",
    ],
    "max_questions_per_reply": 1,
    "max_sentences_per_reply": 4,
    "max_exclamations_per_reply": 1,
    "forbid_placeholder_tokens": True,
    "forbid_meta_terms": True,
    "forbid_city_echo_ponyal": True,
    "forbid_city_echo_ack": True,
    "forbid_repeated_questions": True,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Массовый прогон диалоговых сценариев с проверками качества ответов."
    )
    parser.add_argument(
        "--cases",
        default=str(ROOT_DIR / "scripts" / "dialog_regression_cases.json"),
        help="Путь к JSON с кейсами регрессии.",
    )
    parser.add_argument("--iterations", type=int, default=1, help="Сколько прогонов каждого кейса.")
    parser.add_argument(
        "--contact-base",
        type=int,
        default=int(time.time()) % 100000000 + 70000000,
        help="Базовый contact_id для изоляции стейта.",
    )
    parser.add_argument("--tenant", type=int, default=None, help="Принудительный tenant для всех кейсов.")
    parser.add_argument("--channel", type=str, default=None, help="Принудительный канал для всех кейсов.")
    parser.add_argument("--show-ok", action="store_true", help="Показывать логи и для успешных кейсов.")
    return parser.parse_args()


def _load_cases(path: pathlib.Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("В файле кейсов отсутствует непустой массив 'cases'.")
    return cases


def _looks_like_address(text: str) -> bool:
    low = (text or "").strip().lower()
    if not low:
        return False
    if re.search(r"\d+\s*/\s*\d+", low):
        return True
    has_street_word = any(
        token in low
        for token in (
            "ул",
            "улиц",
            "просп",
            "пр-кт",
            "дом",
            "д.",
            "корп",
            "кв",
            "стр",
            "шоссе",
            "переул",
            "набереж",
            "бульвар",
            "авеню",
        )
    )
    return bool(has_street_word and re.search(r"\d", low))


def _sentence_count(text: str) -> int:
    chunks = [part.strip() for part in re.split(r"[.!?]+", text or "") if part.strip()]
    return len(chunks)


def _question_count(text: str) -> int:
    return (text or "").count("?")


def _validate_turn(
    *,
    case_name: str,
    iteration: int,
    turn: int,
    user_text: str,
    bot_text: str,
    rules: Dict[str, Any],
    seen_address: bool,
) -> List[Violation]:
    out: List[Violation] = []
    reply = (bot_text or "").strip()
    reply_low = reply.lower()

    if rules.get("forbid_placeholder_tokens", False):
        if re.search(r"\{[^{}]{1,80}\}", reply):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule="forbid_placeholder_tokens",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    for needle in rules.get("forbidden_substrings", []) or []:
        item = str(needle or "").strip().lower()
        if item and item in reply_low:
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule=f"forbidden_substrings:{item}",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    if rules.get("forbid_meta_terms", False):
        meta_terms = ("intent", "pipeline", "persona", "state machine", "json")
        if any(term in reply_low for term in meta_terms):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule="forbid_meta_terms",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    for pattern in rules.get("forbidden_regex", []) or []:
        rx = str(pattern or "").strip()
        if rx and re.search(rx, reply, flags=re.IGNORECASE):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule=f"forbidden_regex:{rx}",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    turn_rules_root = rules.get("turn_rules") or {}
    turn_rules: Dict[str, Any] = {}
    if isinstance(turn_rules_root, dict):
        by_str = turn_rules_root.get(str(turn))
        by_int = turn_rules_root.get(turn)
        if isinstance(by_str, dict):
            turn_rules = by_str
        elif isinstance(by_int, dict):
            turn_rules = by_int

    for needle in turn_rules.get("forbidden_substrings", []) or []:
        item = str(needle or "").strip().lower()
        if item and item in reply_low:
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule=f"turn_forbidden_substrings:{item}",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    must_all = [str(item or "").strip().lower() for item in (turn_rules.get("must_include_substrings") or [])]
    for item in must_all:
        if item and item not in reply_low:
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule=f"turn_must_include_substrings:{item}",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    must_any = [str(item or "").strip().lower() for item in (turn_rules.get("must_include_any") or [])]
    if must_any and not any(item and item in reply_low for item in must_any):
        out.append(
            Violation(
                case_name=case_name,
                iteration=iteration,
                turn=turn,
                rule=f"turn_must_include_any:{'|'.join(must_any)}",
                user_text=user_text,
                bot_text=bot_text,
            )
        )

    for pattern in turn_rules.get("must_match_regex", []) or []:
        rx = str(pattern or "").strip()
        if rx and not re.search(rx, reply, flags=re.IGNORECASE):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule=f"turn_must_match_regex:{rx}",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    if rules.get("forbid_city_echo_ponyal", False):
        if re.search(r"\b[А-ЯЁA-Z][А-ЯЁA-Za-z0-9\-\s]{1,30},\s*понял", reply):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule="forbid_city_echo_ponyal",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    if rules.get("forbid_city_echo_ack", False):
        if re.search(r"\b[А-ЯЁA-Z][А-ЯЁA-Za-z0-9\-\s]{1,30},\s*(принял|принято|услышал)", reply):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule="forbid_city_echo_ack",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    if rules.get("forbid_neighbor_claim_without_address", False):
        if not seen_address and any(
            phrase in reply_low
            for phrase in ("соседнем доме", "ставили рядом", "недавно ставили", "соседнем подъезде")
        ):
            out.append(
                Violation(
                    case_name=case_name,
                    iteration=iteration,
                    turn=turn,
                    rule="forbid_neighbor_claim_without_address",
                    user_text=user_text,
                    bot_text=bot_text,
                )
            )

    max_q = rules.get("max_questions_per_reply")
    if max_q is not None and _question_count(reply) > int(max_q):
        out.append(
            Violation(
                case_name=case_name,
                iteration=iteration,
                turn=turn,
                rule=f"max_questions_per_reply:{max_q}",
                user_text=user_text,
                bot_text=bot_text,
            )
            )

    max_excl = rules.get("max_exclamations_per_reply")
    if max_excl is not None and (reply.count("!") > int(max_excl)):
        out.append(
            Violation(
                case_name=case_name,
                iteration=iteration,
                turn=turn,
                rule=f"max_exclamations_per_reply:{max_excl}",
                user_text=user_text,
                bot_text=bot_text,
            )
        )

    max_sent = rules.get("max_sentences_per_reply")
    if max_sent is not None and _sentence_count(reply) > int(max_sent):
        out.append(
            Violation(
                case_name=case_name,
                iteration=iteration,
                turn=turn,
                rule=f"max_sentences_per_reply:{max_sent}",
                user_text=user_text,
                bot_text=bot_text,
            )
        )

    return out


def _extract_questions(text: str) -> List[str]:
    chunks = re.findall(r"[^?]*\?", text or "", flags=re.MULTILINE)
    out: List[str] = []
    for item in chunks:
        normalized = re.sub(r"\s+", " ", item.strip().lower())
        if normalized:
            out.append(normalized)
    return out


async def _run_case(
    *,
    case: Dict[str, Any],
    iteration: int,
    contact_base: int,
    tenant_override: int | None,
    channel_override: str | None,
) -> tuple[List[tuple[str, str]], List[Violation]]:
    case_name = str(case.get("name") or f"case_{iteration}")
    tenant = int(tenant_override if tenant_override is not None else case.get("tenant", 1))
    channel = str(channel_override if channel_override is not None else case.get("channel", "telegram"))
    messages = case.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError(f"{case_name}: messages должен быть непустым массивом.")

    rules = case.get("rules") or {}
    if not isinstance(rules, dict):
        rules = {}
    merged_rules: Dict[str, Any] = json.loads(json.dumps(_DEFAULT_HUMAN_RULES, ensure_ascii=False))
    for key, value in rules.items():
        if key == "forbidden_substrings":
            base = [str(x) for x in (merged_rules.get(key) or [])]
            extra = [str(x) for x in (value or [])]
            merged_rules[key] = list(dict.fromkeys(base + extra))
        elif key == "turn_rules" and isinstance(value, dict):
            merged_rules[key] = value
        else:
            merged_rules[key] = value
    rules = merged_rules

    contact_id = int(contact_base + (hash(case_name) % 10000) + iteration * 100000)
    core.reset_sales_state(tenant, contact_id)

    convo: List[tuple[str, str]] = []
    violations: List[Violation] = []
    seen_address = False
    seen_questions: set[str] = set()
    extra_system = str(case.get("extra_system") or "").strip()
    extra_mode = str(case.get("extra_system_mode") or "append").strip().lower()

    for idx, user_text in enumerate(messages, start=1):
        text = str(user_text or "").strip()
        if not text:
            continue
        seen_address = seen_address or _looks_like_address(text)
        llm_messages = await core.build_llm_messages(
            contact_id=contact_id,
            last_user_text=text,
            channel=channel,
            tenant=tenant,
        )
        if extra_system:
            if llm_messages and str(llm_messages[0].get("role") or "") == "system":
                if extra_mode == "replace":
                    llm_messages[0]["content"] = extra_system
                else:
                    llm_messages[0]["content"] = (
                        str(llm_messages[0].get("content") or "").strip() + "\n\n" + extra_system
                    ).strip()
            else:
                llm_messages.insert(0, {"role": "system", "content": extra_system})
        reply = str(
            await core.ask_llm(
                llm_messages,
                tenant=tenant,
                contact_id=contact_id,
                channel=channel,
            )
        ).strip()
        convo.append((text, reply))
        violations.extend(
            _validate_turn(
                case_name=case_name,
                iteration=iteration,
                turn=idx,
                user_text=text,
                bot_text=reply,
                rules=rules,
                seen_address=seen_address,
            )
        )
        if rules.get("forbid_repeated_questions", False):
            for question in _extract_questions(reply):
                if question in seen_questions:
                    violations.append(
                        Violation(
                            case_name=case_name,
                            iteration=iteration,
                            turn=idx,
                            rule=f"forbid_repeated_questions:{question}",
                            user_text=text,
                            bot_text=reply,
                        )
                    )
                else:
                    seen_questions.add(question)
    return convo, violations


def _print_case_log(case_name: str, iteration: int, convo: Sequence[tuple[str, str]], violations: Sequence[Violation]) -> None:
    print(f"\n=== CASE {case_name} / iteration {iteration} ===")
    for turn, pair in enumerate(convo, start=1):
        user_text, bot_text = pair
        print(f"{turn:02d}. U: {user_text}")
        print(f"    A: {bot_text}")
    if not violations:
        print("STATUS: OK")
        return
    print(f"STATUS: FAIL ({len(violations)} violations)")
    for item in violations:
        print(f" - turn {item.turn}: {item.rule}")


async def _main_async(args: argparse.Namespace) -> int:
    cases_path = pathlib.Path(args.cases).resolve()
    cases = _load_cases(cases_path)

    total_runs = 0
    total_violations = 0
    all_violations: List[Violation] = []

    for case in cases:
        case_name = str(case.get("name") or "unnamed")
        for i in range(1, int(args.iterations) + 1):
            total_runs += 1
            convo, violations = await _run_case(
                case=case,
                iteration=i,
                contact_base=int(args.contact_base),
                tenant_override=args.tenant,
                channel_override=args.channel,
            )
            total_violations += len(violations)
            all_violations.extend(violations)
            if args.show_ok or violations:
                _print_case_log(case_name, i, convo, violations)

    print("\n=== SUMMARY ===")
    print(f"runs={total_runs}")
    print(f"violations={total_violations}")
    if all_violations:
        grouped: Dict[str, int] = {}
        for item in all_violations:
            grouped[item.rule] = grouped.get(item.rule, 0) + 1
        for rule, count in sorted(grouped.items(), key=lambda x: (-x[1], x[0])):
            print(f" - {rule}: {count}")
        return 1
    return 0


def main() -> None:
    args = _parse_args()
    rc = asyncio.run(_main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
