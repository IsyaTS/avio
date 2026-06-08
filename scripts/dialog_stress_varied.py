#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import re
import time
from dataclasses import dataclass

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
project_data_dir = ROOT_DIR / "data"
project_tenants_dir = project_data_dir / "tenants"
current_tenants = pathlib.Path(os.getenv("TENANTS_DIR") or "").expanduser()
if (not str(current_tenants)) or str(current_tenants) == "/data/tenants" or (not current_tenants.exists()):
    os.environ["APP_DATA_DIR"] = str(project_data_dir)
    os.environ["TENANTS_DIR"] = str(project_tenants_dir)

from libs.core import sales_core as core


logging.basicConfig(
    level=logging.WARNING, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s"
)
for _name in ("openai", "openai._base_client", "httpx", "httpcore", "asyncio"):
    logging.getLogger(_name).setLevel(logging.WARNING)


@dataclass
class Case:
    name: str
    tenant: int
    channel: str
    persona: str
    messages: list[str]


_LEAK_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"после приветствия последовательно уточни", re.IGNORECASE),
    re.compile(r"диалог-скрипт", re.IGNORECASE),
    re.compile(r"шаблон реплик", re.IGNORECASE),
    re.compile(r"собрал коротк\w*\s+шорт[-\s]?лист", re.IGNORECASE),
    re.compile(r"\{[^{}]{1,80}\}"),
)


def _question_fp(text: str) -> str:
    low = re.sub(r"[^a-zа-яё0-9\s]", " ", (text or "").lower())
    tokens = [tok for tok in low.split() if len(tok) >= 3]
    return " ".join(tokens[:16])


def _extract_questions(text: str) -> list[str]:
    if not text:
        return []
    out: list[str] = []
    for chunk in re.findall(r"[^?]+(?:\?|$)", text):
        q = chunk.strip()
        if "?" in q and len(q) >= 5:
            out.append(q)
    return out


def _build_cases() -> list[Case]:
    return [
        Case(
            name="doors_b2c",
            tenant=6201,
            channel="telegram",
            persona=(
                "Вы менеджер по входным дверям\n"
                "Тон: спокойный, деловой, без канцелярита\n"
                "После приветствия последовательно уточни:\n"
                "1) город\n"
                "2) тип объекта\n"
                "3) что из каталога приглянулось\n"
                "Если клиент сравнивает цены — спокойно объясни разницу и предложи альтернативу\n"
            ),
            messages=[
                "здравствуйте",
                "уфа",
                "квартира",
                "мне нужна дверь с зеркалом",
                "у конкурентов дешевле",
                "а завтра с утра сможете?",
            ],
        ),
        Case(
            name="furniture_custom",
            tenant=6202,
            channel="telegram",
            persona=(
                "Вы консультант по корпусной мебели на заказ\n"
                "Стиль: коротко и живо\n"
                "Скрипт диалога:\n"
                "- уточни город\n"
                "- уточни помещение и размеры\n"
                "- предложи 2 сценария по бюджету\n"
            ),
            messages=[
                "добрый день",
                "екатеринбург",
                "шкаф в спальню",
                "размер примерно 240 на 60",
                "бюджет до 80 тысяч",
                "как быстро изготовите?",
            ],
        ),
        Case(
            name="auto_service",
            tenant=6203,
            channel="telegram",
            persona=(
                "Вы администратор автосервиса\n"
                "Общение на Вы, без робота\n"
                "Уточни марку авто, проблему, желаемое время визита\n"
            ),
            messages=[
                "привет",
                "нужен ремонт подвески",
                "kia rio",
                "стук спереди на кочках",
                "когда можно подъехать?",
            ],
        ),
        Case(
            name="dentistry",
            tenant=6204,
            channel="telegram",
            persona=(
                "Вы координатор стоматологии\n"
                "Тон: аккуратный и спокойный\n"
                "Сначала уточни запрос, потом предложи ближайшее окно\n"
            ),
            messages=[
                "здравствуйте",
                "болит зуб",
                "сегодня можно попасть?",
                "вечером после 19",
            ],
        ),
        Case(
            name="real_estate_rent",
            tenant=6205,
            channel="telegram",
            persona=(
                "Вы менеджер по аренде квартир\n"
                "Уточняй район, бюджет и срок аренды\n"
                "Если нет точного варианта — предложи похожие\n"
            ),
            messages=[
                "добрый день",
                "ищу однушку в казани",
                "до 35 тысяч",
                "нужно с конца месяца",
                "желательно рядом с метро",
            ],
        ),
        Case(
            name="catering",
            tenant=6206,
            channel="telegram",
            persona=(
                "Вы менеджер кейтеринга\n"
                "Уточни формат события, число гостей, дату, локацию\n"
                "Давай конкретные предложения пакетами\n"
            ),
            messages=[
                "здравствуйте",
                "нужен кейтеринг на корпоратив",
                "60 человек",
                "дата 28 числа",
                "площадка в центре уфы",
            ],
        ),
        Case(
            name="logistics_b2b",
            tenant=6207,
            channel="telegram",
            persona=(
                "Вы менеджер логистики для B2B\n"
                "Уточни маршрут, тип груза, сроки и объём\n"
                "Если срочно — сразу предлагай ближайший слот\n"
            ),
            messages=[
                "добрый день",
                "нужна доставка москва - казань",
                "груз 4 палеты",
                "завтра отгрузка",
                "какая цена и сроки?",
            ],
        ),
        Case(
            name="education",
            tenant=6208,
            channel="telegram",
            persona=(
                "Вы консультант онлайн-школы\n"
                "Уточни цель обучения, текущий уровень, удобный формат\n"
                "Без лишней воды и без повторов\n"
            ),
            messages=[
                "здравствуйте",
                "хочу выучить английский",
                "уровень начальный",
                "интересуют занятия вечером",
                "сколько стоит?",
            ],
        ),
        Case(
            name="beauty_salon",
            tenant=6209,
            channel="telegram",
            persona=(
                "Вы администратор салона красоты\n"
                "Уточняй услугу и желаемое время\n"
                "Предлагай ближайшие свободные окна\n"
            ),
            messages=[
                "добрый вечер",
                "нужна стрижка и окрашивание",
                "когда есть запись на выходных?",
            ],
        ),
        Case(
            name="home_appliance",
            tenant=6210,
            channel="telegram",
            persona=(
                "Вы консультант по бытовой технике\n"
                "Уточни тип техники, задачи и бюджет\n"
                "Дай 2-3 варианта и один следующий шаг\n"
            ),
            messages=[
                "здравствуйте",
                "нужен холодильник",
                "для семьи из 4 человек",
                "бюджет 70-90",
                "что посоветуете?",
            ],
        ),
        Case(
            name="windows_install",
            tenant=6211,
            channel="telegram",
            persona=(
                "Вы менеджер по окнам ПВХ\n"
                "Уточни город, тип объекта и количество окон\n"
                "Если нет замеров — предложи бесплатный замер\n"
            ),
            messages=[
                "здравствуйте",
                "пермь",
                "частный дом",
                "нужно 6 окон",
                "замеров пока нет",
            ],
        ),
        Case(
            name="it_integrator",
            tenant=6212,
            channel="telegram",
            persona=(
                "Вы пресейл инженер по CRM интеграции\n"
                "Уточни текущую систему, цель проекта и срок запуска\n"
                "Тон профессиональный, но живой\n"
            ),
            messages=[
                "добрый день",
                "нужна интеграция crm с телефонией",
                "amo + asterisk",
                "хотим запустить за 2 недели",
                "с чего начать?",
            ],
        ),
    ]


async def _run_case(
    case: Case, iteration: int, contact_id: int
) -> tuple[list[str], list[tuple[str, str]]]:
    core.ensure_tenant_files(case.tenant)
    core.write_persona(case.tenant, case.persona, channel=case.channel)
    core.reset_sales_state(case.tenant, contact_id)

    violations: list[str] = []
    transcript: list[tuple[str, str]] = []
    asked_fps: set[str] = set()
    early_questions = 0

    for turn, user_text in enumerate(case.messages, start=1):
        llm_messages = await core.build_llm_messages(
            contact_id=contact_id,
            last_user_text=user_text,
            channel=case.channel,
            tenant=case.tenant,
        )
        answer = await core.ask_llm(
            llm_messages,
            tenant=case.tenant,
            contact_id=contact_id,
            channel=case.channel,
        )
        answer_text = str(answer or "").strip()
        transcript.append((user_text, answer_text))

        if not answer_text:
            violations.append(f"iter={iteration} turn={turn}: empty_reply")
            continue

        for pattern in _LEAK_PATTERNS:
            if pattern.search(answer_text):
                violations.append(f"iter={iteration} turn={turn}: leak:{pattern.pattern}")

        if re.search(r"\b[А-ЯЁA-Z][А-ЯЁA-Za-z\-]{2,25},\s*(понял|принял)\b", answer_text):
            violations.append(f"iter={iteration} turn={turn}: city_echo_ack")

        if len(answer_text) > 440:
            violations.append(f"iter={iteration} turn={turn}: too_long_reply:{len(answer_text)}")

        if turn <= 3 and "?" in answer_text:
            early_questions += 1

        for q in _extract_questions(answer_text):
            fp = _question_fp(q)
            if not fp:
                continue
            if fp in asked_fps:
                violations.append(f"iter={iteration} turn={turn}: repeated_question:{fp}")
            asked_fps.add(fp)

    if early_questions == 0:
        violations.append(f"iter={iteration}: no_questions_in_first_three_turns")

    return violations, transcript


async def _run_split_delay_checks() -> list[str]:
    import apps.worker.main as worker_main  # local import to avoid changing global log config during dialog run
    from redis import exceptions as redis_ex

    violations: list[str] = []

    delay_samples = [worker_main._split_part_delay_seconds_value() for _ in range(200)]
    if min(delay_samples) < worker_main.SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS:
        violations.append("split_delay_below_min")
    if max(delay_samples) > worker_main.SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS:
        violations.append("split_delay_above_max")

    long_text = (
        "здравствуйте отправил каталог чтобы было удобнее посмотреть модели "
        "с зеркалом сейчас в наличии гарда зеркало и эмалит зеркало 9см "
        "если хотите могу коротко сравнить их по утеплению и замкам и сразу предложить "
        "вариант под ваш бюджет и сроки установки"
    )
    parts = worker_main._split_reply_for_send(long_text, "telegram")
    if len(parts) < 2:
        violations.append(f"split_parts_too_few:{len(parts)}")

    test_queue = f"outbox:test:split-delay:{int(time.time())}"
    old_queue = worker_main.OUTBOX_QUEUE_KEY
    worker_main.OUTBOX_QUEUE_KEY = test_queue
    try:
        await worker_main.r.delete(test_queue)
        ok = await worker_main._enqueue_channel_reply_payload(
            tenant_id=7777,
            lead_id=5555,
            channel="whatsapp",
            reply_text=long_text,
            user_text="нужны двери с зеркалом",
            context={"to": "79001234567"},
        )
        if not ok:
            violations.append("enqueue_split_payload_failed")
            return violations
        raw_items = await worker_main.r.lrange(test_queue, 0, -1)
        items = [json.loads(raw) for raw in reversed(raw_items)]
        if len(items) < 2:
            violations.append(f"queue_split_items_too_few:{len(items)}")
            return violations
        prev_ts = 0.0
        for idx, item in enumerate(items, start=1):
            if idx == 1:
                if "send_not_before_ts" in item:
                    violations.append("first_part_has_delay_unexpected")
                continue
            ts = float(item.get("send_not_before_ts") or 0.0)
            if ts <= 0:
                violations.append(f"part_{idx}_missing_delay_ts")
                continue
            if prev_ts and ts <= prev_ts:
                violations.append(f"part_{idx}_delay_not_increasing")
            prev_ts = ts
    except redis_ex.ConnectionError:
        return []
    finally:
        worker_main.OUTBOX_QUEUE_KEY = old_queue
        try:
            await worker_main.r.delete(test_queue)
        except Exception:
            pass
        try:
            await worker_main.r.aclose()
        except Exception:
            pass

    return violations


async def main() -> int:
    cases = _build_cases()
    iterations = 1
    contact_seed = int(time.time()) % 100000 + 900000

    all_violations: list[str] = []
    total_turns = 0

    print(f"Running varied dialog stress: cases={len(cases)} iterations={iterations}")
    for iteration in range(1, iterations + 1):
        for idx, case in enumerate(cases):
            contact_id = contact_seed + iteration * 1000 + idx
            violations, transcript = await _run_case(case, iteration, contact_id)
            total_turns += len(transcript)
            if violations:
                print(f"[FAIL] {case.name} iter={iteration} violations={len(violations)}")
                for item in violations[:8]:
                    print(f"  - {item}")
                if transcript:
                    print("  Last 2 turns:")
                    for u, a in transcript[-2:]:
                        print(f"    U: {u}")
                        print(f"    A: {a}")
                all_violations.extend([f"{case.name}: {v}" for v in violations])
            else:
                print(f"[OK] {case.name} iter={iteration}")

    split_violations = await _run_split_delay_checks()
    if split_violations:
        print("[FAIL] split/delay checks")
        for item in split_violations:
            print(f"  - {item}")
        all_violations.extend([f"split-delay: {v}" for v in split_violations])
    else:
        print("[OK] split/delay checks")

    print(
        f"SUMMARY: cases={len(cases)} iterations={iterations} turns={total_turns} violations={len(all_violations)}"
    )
    return 1 if all_violations else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
