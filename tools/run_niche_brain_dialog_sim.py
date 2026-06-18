#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import hashlib
import json
import os
import pathlib
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

MODE_CHOICES = ("old", "v2", "compare")
NICHE_MARKER = "NICHE BRAIN V2"
DEFAULT_TENANT_ID = 101
_ENV_KEYS = ("APP_DATA_DIR", "TENANTS_DIR", "TENANT_CONFIG_DB_ENABLED")
_RESPONSE_PIPELINE: Any | None = None
_SALES_CORE: Any | None = None

_TOPIC_MARKERS: dict[str, tuple[str, ...]] = {
    "availability": ("налич", "провер", "модель", "вариант"),
    "budget": ("бюджет", "цен", "стоим", "диапазон"),
    "catalog": ("каталог", "вариант", "модель", "подбор", "фото"),
    "category": ("квартир", "дом", "входн", "межкомнат", "категор"),
    "city": ("город", "уфа", "достав", "зон"),
    "delivery": ("достав", "привез", "самовывоз", "город"),
    "details": ("детал", "характерист", "цвет", "фото", "модель"),
    "installation": ("установ", "монтаж", "замер", "под ключ"),
    "model": ("модель", "вариант", "двер"),
    "photo": ("фото", "цвет", "пример", "показ"),
    "price": ("цен", "стоим", "бюджет", "расчет"),
    "product_details": ("размер", "модель", "замк", "открыв", "характерист"),
    "size": ("размер", "проем", "ширин", "высот", "2050", "900"),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline multi-turn Avito-like dialog simulator for old vs niche-brain-v2 replies."
    )
    parser.add_argument("--cases", required=True, help="Path to JSONL cases.")
    parser.add_argument("--tenant-id", type=int, default=None)
    parser.add_argument("--channel", default="avito")
    parser.add_argument("--mode", choices=MODE_CHOICES, default="compare")
    parser.add_argument("--run-id", default=None, help="Optional stable run namespace for reproducible debug.")
    parser.add_argument("--out", required=True, help="Path to report JSON.")
    parser.add_argument("--timeout-seconds", type=float, default=12.0)
    return parser.parse_args()


def load_cases(path: pathlib.Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"{path}:{line_no}: case must be a JSON object")
        turns = payload.get("turns")
        if not isinstance(turns, list) or not turns:
            raise ValueError(f"{path}:{line_no}: case.turns must be a non-empty list")
        cases.append(payload)
    return cases


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(base or {}))
    for key, value in dict(overlay or {}).items():
        current = result.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            result[key] = _deep_merge(current, value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def get_response_pipeline() -> Any:
    global _RESPONSE_PIPELINE
    if _RESPONSE_PIPELINE is None:
        from libs.core import response_pipeline as pipeline

        _RESPONSE_PIPELINE = pipeline
    return _RESPONSE_PIPELINE


def get_sales_core() -> Any:
    global _SALES_CORE
    if _SALES_CORE is None:
        from libs.core import sales_core as core

        _SALES_CORE = core
    return _SALES_CORE


@contextlib.contextmanager
def scoped_local_env() -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in _ENV_KEYS}
    project_data_dir = ROOT_DIR / "data"
    project_tenants_dir = project_data_dir / "tenants"
    tenants_env = os.getenv("TENANTS_DIR")
    current_tenants = pathlib.Path(tenants_env).expanduser() if tenants_env else None
    if current_tenants is None or str(current_tenants) == "/data/tenants" or not current_tenants.exists():
        os.environ["APP_DATA_DIR"] = str(project_data_dir)
        os.environ["TENANTS_DIR"] = str(project_tenants_dir)
    os.environ["TENANT_CONFIG_DB_ENABLED"] = "0"
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _resolve_tenant_id(case: Mapping[str, Any], tenant_override: int | None) -> int:
    if tenant_override is not None:
        return int(tenant_override)
    if case.get("tenant_id") is not None:
        return int(case.get("tenant_id"))
    return DEFAULT_TENANT_ID


def _niche_override(*, mode: str, tenant_id: int, channel: str) -> dict[str, Any]:
    enabled = mode == "v2"
    return {
        "behavior": {
            "niche_brain_v2": {
                "enabled": enabled,
                "apply_mode": enabled,
                "tenant_allowlist": [int(tenant_id)],
                "allowed_channels": [str(channel).strip().lower()],
            }
        }
    }


@contextlib.contextmanager
def scoped_pipeline_overrides(
    *,
    mode: str,
    tenant_id: int,
    channel: str,
    prompt_capture: dict[str, Any],
) -> Iterator[None]:
    response_pipeline = get_response_pipeline()
    original_read_tenant_config = response_pipeline.read_tenant_config
    original_ask_llm = response_pipeline.ask_llm
    override = _niche_override(mode=mode, tenant_id=tenant_id, channel=channel)

    def read_tenant_config_with_override(requested_tenant: int) -> dict[str, Any]:
        base = original_read_tenant_config(int(requested_tenant))
        if int(requested_tenant) != int(tenant_id):
            return base
        return _deep_merge(base if isinstance(base, Mapping) else {}, override)

    async def ask_llm_with_prompt_capture(messages: Sequence[Mapping[str, Any]], **kwargs: Any) -> Any:
        system_text = ""
        if messages and isinstance(messages[0], Mapping):
            system_text = str(messages[0].get("content") or "")
        prompt_capture["prompt_has_niche_brain_v2"] = NICHE_MARKER in system_text
        if NICHE_MARKER in system_text:
            idx = system_text.find(NICHE_MARKER)
            prompt_capture["prompt_preview"] = _compact(system_text[idx : idx + 320])
        else:
            prompt_capture["prompt_preview"] = ""
        return await original_ask_llm(messages, **kwargs)

    response_pipeline.read_tenant_config = read_tenant_config_with_override
    response_pipeline.ask_llm = ask_llm_with_prompt_capture
    try:
        yield
    finally:
        response_pipeline.read_tenant_config = original_read_tenant_config
        response_pipeline.ask_llm = original_ask_llm


def _generate_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:16]}"


def _safe_id_part(value: Any) -> str:
    raw = str(value or "").strip()
    result = []
    for char in raw:
        if char.isalnum() or char in {"-", "_"}:
            result.append(char)
        else:
            result.append("_")
    cleaned = "".join(result).strip("_")
    return cleaned[:80] or "item"


def _fake_numeric_contact_id(*, run_id: str, case_id: str, case_index: int, mode: str) -> int:
    digest = hashlib.sha256(f"{run_id}:{case_index}:{case_id}:{mode}".encode("utf-8")).hexdigest()
    return 900_000_000 + (int(digest[:10], 16) % 80_000_000)


def _fake_id_prefix(*, run_id: str, case_id: str, case_index: int, mode: str) -> str:
    return "offline-%s-case%04d-%s-%s" % (
        _safe_id_part(run_id),
        int(case_index),
        _safe_id_part(case_id),
        _safe_id_part(mode),
    )


def _reset_fake_sales_state(
    *,
    tenant_id: int,
    contact_id: int,
    warnings: list[dict[str, Any]],
    phase: str,
) -> None:
    try:
        get_sales_core().reset_sales_state(int(tenant_id), int(contact_id))
    except Exception as exc:
        warnings.append(
            {
                "phase": phase,
                "tenant_id": int(tenant_id),
                "contact_id": int(contact_id),
                "warning": "reset_fake_sales_state_failed",
                "error": str(exc),
            }
        )


def _normalize_seed_history(seed_history: Any) -> list[dict[str, str]]:
    if not isinstance(seed_history, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in seed_history:
        if not isinstance(item, Mapping):
            continue
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or item.get("text") or "").strip()
        if role in {"user", "assistant"} and content:
            normalized.append({"role": role, "content": content})
    return normalized


def _manual_score(case: Mapping[str, Any]) -> dict[str, Any]:
    score = case.get("manual_score")
    if not isinstance(score, Mapping):
        score = {}
    return {
        "old": score.get("old"),
        "v2": score.get("v2"),
        "winner": score.get("winner"),
        "notes": str(score.get("notes") or ""),
    }


def _compact(text: str, limit: int = 320) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def check_turn_violations(*, user_text: str, reply_text: str, expected: Mapping[str, Any] | None) -> list[str]:
    expected = expected if isinstance(expected, Mapping) else {}
    reply_low = str(reply_text or "").lower()
    violations: list[str] = []
    if not str(reply_text or "").strip():
        violations.append("empty_reply")
        return violations
    for needle in expected.get("must_not_contain") or []:
        item = str(needle or "").strip()
        if item and item.lower() in reply_low:
            violations.append(f"must_not_contain:{item}")
    for topic in expected.get("must_answer_about") or []:
        topic_key = str(topic or "").strip().lower()
        markers = _TOPIC_MARKERS.get(topic_key)
        if markers and not any(marker in reply_low for marker in markers):
            violations.append(f"missing_topic:{topic_key}")
    return sorted(set(violations))


async def _run_mode_for_case(
    *,
    case: Mapping[str, Any],
    mode: str,
    run_id: str,
    case_index: int,
    fallback_tenant_id: int | None,
    fallback_channel: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    case_id = str(case.get("case_id") or case.get("id") or case.get("name") or "case").strip()
    tenant_id = _resolve_tenant_id(case, fallback_tenant_id)
    channel = str(case.get("channel") or fallback_channel).strip().lower()
    turns = case.get("turns")
    if not isinstance(turns, list):
        turns = []
    pipeline_contact_id = _fake_numeric_contact_id(
        run_id=run_id,
        case_id=case_id,
        case_index=case_index,
        mode=mode,
    )
    fake_id_prefix = _fake_id_prefix(run_id=run_id, case_id=case_id, case_index=case_index, mode=mode)
    history = _normalize_seed_history(case.get("seed_history"))
    out_turns: list[dict[str, Any]] = []
    reset_warnings: list[dict[str, Any]] = []

    _reset_fake_sales_state(
        tenant_id=tenant_id,
        contact_id=pipeline_contact_id,
        warnings=reset_warnings,
        phase="before_case_mode",
    )
    try:
        for idx, turn in enumerate(turns, start=1):
            if not isinstance(turn, Mapping):
                continue
            user_text = str(turn.get("user") or turn.get("user_text") or "").strip()
            if not user_text:
                continue
            prompt_capture: dict[str, Any] = {
                "prompt_has_niche_brain_v2": False,
                "prompt_preview": "",
            }
            with scoped_pipeline_overrides(
                mode=mode,
                tenant_id=tenant_id,
                channel=channel,
                prompt_capture=prompt_capture,
            ):
                pipeline_result = await get_response_pipeline().run_response_pipeline(
                    tenant_id=tenant_id,
                    channel=channel,
                    user_text=user_text,
                    history=history,
                    contact_id=pipeline_contact_id,
                    enable_photos=False,
                    timeout_seconds=timeout_seconds,
                    log_fn=None,
                )
            reply_text = str(pipeline_result.reply_text or "").strip()
            expected = turn.get("expected") if isinstance(turn.get("expected"), Mapping) else {}
            out_turns.append(
                {
                    "turn": idx,
                    "eval_id": f"{fake_id_prefix}-turn{idx:03d}",
                    "user": user_text,
                    "reply": reply_text,
                    "source": str(pipeline_result.source or ""),
                    "trace_id": getattr(pipeline_result.trace, "trace_id", None),
                    "violations": check_turn_violations(
                        user_text=user_text,
                        reply_text=reply_text,
                        expected=expected,
                    ),
                    "prompt_has_niche_brain_v2": bool(prompt_capture.get("prompt_has_niche_brain_v2")),
                    "prompt_preview": str(prompt_capture.get("prompt_preview") or ""),
                }
            )
            history.append({"role": "user", "content": user_text})
            history.append({"role": "assistant", "content": reply_text})
    finally:
        _reset_fake_sales_state(
            tenant_id=tenant_id,
            contact_id=pipeline_contact_id,
            warnings=reset_warnings,
            phase="after_case_mode",
        )

    return {
        "mode": mode,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "channel": channel,
        "contact_id": f"{fake_id_prefix}-contact",
        "lead_id": f"{fake_id_prefix}-lead",
        "conversation_id": f"{fake_id_prefix}-conversation",
        "pipeline_contact_id": pipeline_contact_id,
        "reset_warnings": reset_warnings,
        "turns": out_turns,
    }


def _empty_turn(mode: str, index: int, user_text: str = "") -> dict[str, Any]:
    return {
        "turn": index,
        "eval_id": f"{mode}-missing-turn{index:03d}",
        "user": user_text,
        "reply": None,
        "source": None,
        "trace_id": None,
        "violations": [],
        "prompt_has_niche_brain_v2": False,
        "prompt_preview": "",
        "mode": mode,
    }


def _combine_turns(case: Mapping[str, Any], old_run: Mapping[str, Any] | None, v2_run: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    old_turns = list(old_run.get("turns") or []) if isinstance(old_run, Mapping) else []
    v2_turns = list(v2_run.get("turns") or []) if isinstance(v2_run, Mapping) else []
    raw_turns = case.get("turns") if isinstance(case.get("turns"), list) else []
    total = max(len(old_turns), len(v2_turns), len(raw_turns))
    manual_score = _manual_score(case)
    combined: list[dict[str, Any]] = []
    for idx in range(total):
        raw_user = ""
        if idx < len(raw_turns) and isinstance(raw_turns[idx], Mapping):
            raw_user = str(raw_turns[idx].get("user") or raw_turns[idx].get("user_text") or "")
        old_turn = old_turns[idx] if idx < len(old_turns) else _empty_turn("old", idx + 1, raw_user)
        v2_turn = v2_turns[idx] if idx < len(v2_turns) else _empty_turn("v2", idx + 1, raw_user)
        user = str(old_turn.get("user") or v2_turn.get("user") or raw_user)
        combined.append(
            {
                "turn": idx + 1,
                "user": user,
                "old_reply": old_turn.get("reply"),
                "v2_reply": v2_turn.get("reply"),
                "old_source": old_turn.get("source"),
                "v2_source": v2_turn.get("source"),
                "old_trace_id": old_turn.get("trace_id"),
                "v2_trace_id": v2_turn.get("trace_id"),
                "old_eval_id": old_turn.get("eval_id"),
                "v2_eval_id": v2_turn.get("eval_id"),
                "old_prompt_has_niche_brain_v2": bool(old_turn.get("prompt_has_niche_brain_v2")),
                "v2_prompt_has_niche_brain_v2": bool(v2_turn.get("prompt_has_niche_brain_v2")),
                "prompt_has_niche_brain_v2": {
                    "old": bool(old_turn.get("prompt_has_niche_brain_v2")),
                    "v2": bool(v2_turn.get("prompt_has_niche_brain_v2")),
                },
                "prompt_preview": {
                    "old": old_turn.get("prompt_preview") or "",
                    "v2": v2_turn.get("prompt_preview") or "",
                },
                "violations": {
                    "old": list(old_turn.get("violations") or []),
                    "v2": list(v2_turn.get("violations") or []),
                },
                "manual_score": copy.deepcopy(manual_score),
            }
        )
    return combined


async def run_cases(
    cases: Sequence[Mapping[str, Any]],
    *,
    mode: str,
    tenant_id: int | None,
    channel: str,
    run_id: str | None = None,
    timeout_seconds: float = 12.0,
) -> dict[str, Any]:
    effective_run_id = str(run_id or _generate_run_id()).strip() or _generate_run_id()
    with scoped_local_env():
        report_cases: list[dict[str, Any]] = []
        for case_index, case in enumerate(cases, start=1):
            case_id = str(case.get("case_id") or case.get("id") or case.get("name") or f"case_{len(report_cases) + 1}")
            old_run = None
            v2_run = None
            if mode in {"old", "compare"}:
                old_run = await _run_mode_for_case(
                    case=case,
                    mode="old",
                    run_id=effective_run_id,
                    case_index=case_index,
                    fallback_tenant_id=tenant_id,
                    fallback_channel=channel,
                    timeout_seconds=timeout_seconds,
                )
            if mode in {"v2", "compare"}:
                v2_run = await _run_mode_for_case(
                    case=case,
                    mode="v2",
                    run_id=effective_run_id,
                    case_index=case_index,
                    fallback_tenant_id=tenant_id,
                    fallback_channel=channel,
                    timeout_seconds=timeout_seconds,
                )
            turns = _combine_turns(case, old_run, v2_run)
            old_violations = sum(len(turn["violations"]["old"]) for turn in turns)
            v2_violations = sum(len(turn["violations"]["v2"]) for turn in turns)
            effective_tenant = _resolve_tenant_id(case, tenant_id)
            effective_channel = str(case.get("channel") or channel).strip().lower()
            report_cases.append(
                {
                    "case_id": case_id,
                    "tenant_id": effective_tenant,
                    "channel": effective_channel,
                    "ids": {
                        "old": {
                            "lead_id": old_run.get("lead_id") if old_run else None,
                            "contact_id": old_run.get("contact_id") if old_run else None,
                            "conversation_id": old_run.get("conversation_id") if old_run else None,
                            "pipeline_contact_id": old_run.get("pipeline_contact_id") if old_run else None,
                        },
                        "v2": {
                            "lead_id": v2_run.get("lead_id") if v2_run else None,
                            "contact_id": v2_run.get("contact_id") if v2_run else None,
                            "conversation_id": v2_run.get("conversation_id") if v2_run else None,
                            "pipeline_contact_id": v2_run.get("pipeline_contact_id") if v2_run else None,
                        },
                    },
                    "reset_warnings": {
                        "old": list(old_run.get("reset_warnings") or []) if old_run else [],
                        "v2": list(v2_run.get("reset_warnings") or []) if v2_run else [],
                    },
                    "turns": turns,
                    "manual_score": _manual_score(case),
                    "summary": {
                        "old_violations_count": old_violations,
                        "v2_violations_count": v2_violations,
                        "winner": None,
                    },
                }
            )
        report_tenant = int(tenant_id) if tenant_id is not None else (
            _resolve_tenant_id(cases[0], None) if cases else DEFAULT_TENANT_ID
        )
        return {
            "run_id": effective_run_id,
            "tenant_id": report_tenant,
            "channel": str(channel).strip().lower(),
            "mode": mode,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cases_total": len(report_cases),
            "warnings": [
                warning
                for case in report_cases
                for mode_warnings in case.get("reset_warnings", {}).values()
                for warning in mode_warnings
            ],
            "cases": report_cases,
        }


def write_report(path: pathlib.Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


async def main_async() -> int:
    args = _parse_args()
    cases_path = pathlib.Path(args.cases)
    if not cases_path.exists():
        raise SystemExit(f"cases file not found: {cases_path}")
    cases = load_cases(cases_path)
    if not cases:
        raise SystemExit("No cases found")
    report = await run_cases(
        cases,
        mode=args.mode,
        tenant_id=int(args.tenant_id) if args.tenant_id is not None else None,
        channel=str(args.channel),
        run_id=args.run_id,
        timeout_seconds=float(args.timeout_seconds),
    )
    out_path = pathlib.Path(args.out)
    write_report(out_path, report)
    print(
        "mode=%s cases=%s report=%s"
        % (
            report["mode"],
            report["cases_total"],
            out_path,
        )
    )
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
