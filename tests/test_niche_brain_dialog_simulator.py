from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from libs.core import response_pipeline
from tools import run_niche_brain_dialog_sim as simulator


pytestmark = pytest.mark.unit


def _case() -> dict[str, Any]:
    return {
        "case_id": "test_case",
        "tenant_id": 101,
        "channel": "avito",
        "turns": [
            {"user": "доставка есть?", "expected": {"must_answer_about": ["delivery"]}},
            {"user": "Уфа, до подъезда", "expected": {"must_answer_about": ["city"]}},
        ],
        "manual_score": {"old": None, "v2": None, "winner": None, "notes": ""},
    }


def _write_cases(path: Path, cases: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(case, ensure_ascii=False) for case in cases), encoding="utf-8")


class _FakeSalesCore:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.reset_calls: list[tuple[int, int]] = []

    def reset_sales_state(self, tenant_id: int, contact_id: int) -> None:
        self.reset_calls.append((int(tenant_id), int(contact_id)))
        if self.fail:
            raise RuntimeError("cleanup unavailable")


def _patch_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    *,
    env_reads: list[str | None] | None = None,
    fake_core: _FakeSalesCore | None = None,
) -> tuple[list[dict[str, Any]], _FakeSalesCore]:
    calls: list[dict[str, Any]] = []
    fake_core = fake_core or _FakeSalesCore()

    async def _build_llm_messages(*_args: Any, **_kwargs: Any) -> list[dict[str, str]]:
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages: list[dict[str, str]], **_kwargs: Any) -> str:
        calls.append({"messages": messages, "kwargs": dict(_kwargs)})
        return f"reply-{len(calls)} доставка город размер модель"

    async def _contextual(**_kwargs: Any) -> dict[str, Any]:
        return {"enabled": False, "applied": False, "block": ""}

    async def _policy(**_kwargs: Any) -> dict[str, Any]:
        return {"enabled": False, "policy_block": ""}

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "build_contextual_cases_block_for_runtime", _contextual)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy)
    def _read_tenant_config(_tenant: int) -> dict[str, Any]:
        if env_reads is not None:
            env_reads.append(os.environ.get("TENANT_CONFIG_DB_ENABLED"))
        return {"behavior": {}}

    monkeypatch.setattr(response_pipeline, "read_tenant_config", _read_tenant_config)
    monkeypatch.setattr(simulator, "get_sales_core", lambda: fake_core)
    return calls, fake_core


@pytest.mark.asyncio
async def test_multi_turn_history_includes_previous_assistant_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, _fake_core = _patch_pipeline(monkeypatch)

    await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    assert len(calls) == 2
    second_messages = calls[1]["messages"]
    assert {"role": "user", "content": "доставка есть?"} in second_messages
    assert {"role": "assistant", "content": "reply-1 доставка город размер модель"} in second_messages


@pytest.mark.asyncio
async def test_old_mode_does_not_add_niche_brain_block(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases([_case()], mode="old", tenant_id=101, channel="avito", timeout_seconds=1)

    turn = report["cases"][0]["turns"][0]
    assert turn["old_prompt_has_niche_brain_v2"] is False
    assert turn["v2_reply"] is None


@pytest.mark.asyncio
async def test_v2_mode_adds_block_for_allowed_tenant_and_avito(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    turn = report["cases"][0]["turns"][0]
    assert turn["v2_prompt_has_niche_brain_v2"] is True
    assert "NICHE BRAIN V2" in turn["prompt_preview"]["v2"]
    assert turn["old_reply"] is None


@pytest.mark.asyncio
async def test_compare_mode_isolates_fake_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases([_case()], mode="compare", tenant_id=101, channel="avito", timeout_seconds=1)

    ids = report["cases"][0]["ids"]
    assert ids["old"]["contact_id"]
    assert ids["v2"]["contact_id"]
    assert ids["old"]["contact_id"] != ids["v2"]["contact_id"]
    assert ids["old"]["conversation_id"] != ids["v2"]["conversation_id"]


@pytest.mark.asyncio
async def test_cli_tenant_override_wins_over_case_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, _fake_core = _patch_pipeline(monkeypatch)

    report = await simulator.run_cases([_case()], mode="v2", tenant_id=7, channel="avito", timeout_seconds=1)

    assert report["tenant_id"] == 7
    assert report["cases"][0]["tenant_id"] == 7
    assert {call["kwargs"]["tenant"] for call in calls} == {7}


@pytest.mark.asyncio
async def test_two_runs_without_run_id_produce_different_fake_contact_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    first = await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)
    second = await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    first_id = first["cases"][0]["ids"]["v2"]["contact_id"]
    second_id = second["cases"][0]["ids"]["v2"]["contact_id"]
    assert first["run_id"] != second["run_id"]
    assert first_id != second_id


@pytest.mark.asyncio
async def test_fixed_run_id_is_in_fake_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case()],
        mode="v2",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    ids = report["cases"][0]["ids"]["v2"]
    assert report["run_id"] == "fixed-run"
    assert "fixed-run" in ids["contact_id"]
    assert "fixed-run" in ids["lead_id"]
    assert "fixed-run" in ids["conversation_id"]
    assert "fixed-run" in report["cases"][0]["turns"][0]["v2_eval_id"]


@pytest.mark.asyncio
async def test_compare_mode_run_id_still_uses_different_old_and_v2_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case()],
        mode="compare",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    ids = report["cases"][0]["ids"]
    assert ids["old"]["contact_id"] != ids["v2"]["contact_id"]
    assert ids["old"]["pipeline_contact_id"] != ids["v2"]["pipeline_contact_id"]
    assert "old" in ids["old"]["contact_id"]
    assert "v2" in ids["v2"]["contact_id"]


@pytest.mark.asyncio
async def test_reset_runs_before_and_after_exact_fake_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    _calls, fake_core = _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case()],
        mode="v2",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    fake_contact_id = report["cases"][0]["ids"]["v2"]["pipeline_contact_id"]
    assert fake_core.reset_calls == [(101, fake_contact_id), (101, fake_contact_id)]
    assert report["warnings"] == []


@pytest.mark.asyncio
async def test_compare_mode_resets_old_and_v2_fake_contacts_separately(monkeypatch: pytest.MonkeyPatch) -> None:
    _calls, fake_core = _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case()],
        mode="compare",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    old_id = report["cases"][0]["ids"]["old"]["pipeline_contact_id"]
    v2_id = report["cases"][0]["ids"]["v2"]["pipeline_contact_id"]
    assert old_id != v2_id
    assert fake_core.reset_calls == [(101, old_id), (101, old_id), (101, v2_id), (101, v2_id)]


def test_reset_source_does_not_use_broad_cleanup() -> None:
    source = Path(simulator.__file__).read_text(encoding="utf-8")
    assert "flushdb" not in source.lower()
    assert "sales_state:*" not in source
    assert "delete(" not in source


@pytest.mark.asyncio
async def test_fixed_run_id_repeated_run_resets_same_fake_contact_each_time(monkeypatch: pytest.MonkeyPatch) -> None:
    _calls, fake_core = _patch_pipeline(monkeypatch)

    first = await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", run_id="fixed-run", timeout_seconds=1)
    second = await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", run_id="fixed-run", timeout_seconds=1)

    fake_contact_id = first["cases"][0]["ids"]["v2"]["pipeline_contact_id"]
    assert second["cases"][0]["ids"]["v2"]["pipeline_contact_id"] == fake_contact_id
    assert fake_core.reset_calls == [
        (101, fake_contact_id),
        (101, fake_contact_id),
        (101, fake_contact_id),
        (101, fake_contact_id),
    ]


@pytest.mark.asyncio
async def test_reset_failure_does_not_fail_run_and_writes_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    failing_core = _FakeSalesCore(fail=True)
    _patch_pipeline(monkeypatch, fake_core=failing_core)

    report = await simulator.run_cases(
        [_case()],
        mode="v2",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    assert report["cases"][0]["turns"][0]["v2_reply"]
    assert len(report["warnings"]) == 2
    assert {warning["phase"] for warning in report["warnings"]} == {"before_case_mode", "after_case_mode"}


@pytest.mark.asyncio
async def test_repeated_case_id_entries_do_not_share_fake_contact_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case(), _case()],
        mode="v2",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    first_id = report["cases"][0]["ids"]["v2"]["contact_id"]
    second_id = report["cases"][1]["ids"]["v2"]["contact_id"]
    assert first_id != second_id
    assert "case0001" in first_id
    assert "case0002" in second_id


@pytest.mark.asyncio
async def test_report_contains_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases(
        [_case()],
        mode="v2",
        tenant_id=101,
        channel="avito",
        run_id="fixed-run",
        timeout_seconds=1,
    )

    assert report["run_id"] == "fixed-run"


@pytest.mark.asyncio
async def test_cli_without_tenant_id_uses_case_tenant(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls, _fake_core = _patch_pipeline(monkeypatch)
    cases_path = tmp_path / "cases.jsonl"
    out_path = tmp_path / "report.json"
    _write_cases(cases_path, [_case()])
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_niche_brain_dialog_sim.py",
            "--cases",
            str(cases_path),
            "--channel",
            "avito",
            "--mode",
            "v2",
            "--out",
            str(out_path),
        ],
    )

    assert await simulator.main_async() == 0

    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["tenant_id"] == 101
    assert report["cases"][0]["tenant_id"] == 101
    assert {call["kwargs"]["tenant"] for call in calls} == {101}


@pytest.mark.asyncio
async def test_cli_without_tenant_id_uses_default_when_case_missing_tenant(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls, _fake_core = _patch_pipeline(monkeypatch)
    case = _case()
    case.pop("tenant_id")
    cases_path = tmp_path / "cases.jsonl"
    out_path = tmp_path / "report.json"
    _write_cases(cases_path, [case])
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_niche_brain_dialog_sim.py",
            "--cases",
            str(cases_path),
            "--channel",
            "avito",
            "--mode",
            "v2",
            "--out",
            str(out_path),
        ],
    )

    assert await simulator.main_async() == 0

    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["tenant_id"] == simulator.DEFAULT_TENANT_ID
    assert report["cases"][0]["tenant_id"] == simulator.DEFAULT_TENANT_ID
    assert {call["kwargs"]["tenant"] for call in calls} == {simulator.DEFAULT_TENANT_ID}


@pytest.mark.asyncio
async def test_cli_tenant_id_override_still_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls, _fake_core = _patch_pipeline(monkeypatch)
    cases_path = tmp_path / "cases.jsonl"
    out_path = tmp_path / "report.json"
    _write_cases(cases_path, [_case()])
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_niche_brain_dialog_sim.py",
            "--cases",
            str(cases_path),
            "--tenant-id",
            "7",
            "--channel",
            "avito",
            "--mode",
            "v2",
            "--out",
            str(out_path),
        ],
    )

    assert await simulator.main_async() == 0

    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["tenant_id"] == 7
    assert report["cases"][0]["tenant_id"] == 7
    assert {call["kwargs"]["tenant"] for call in calls} == {7}


@pytest.mark.asyncio
async def test_report_contains_required_manual_review_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)

    report = await simulator.run_cases([_case()], mode="compare", tenant_id=101, channel="avito", timeout_seconds=1)

    item = report["cases"][0]
    turn = item["turns"][0]
    assert item["case_id"] == "test_case"
    assert "old_reply" in turn
    assert "v2_reply" in turn
    assert "violations" in turn
    assert set(turn["violations"]) == {"old", "v2"}
    assert turn["manual_score"] == {"old": None, "v2": None, "winner": None, "notes": ""}


def test_simulator_does_not_import_worker_runtime() -> None:
    source = Path(simulator.__file__).read_text(encoding="utf-8")
    assert "apps.worker" not in source
    assert "OUTBOX_QUEUE_KEY" not in source


def test_importing_module_does_not_mutate_tenant_env(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = ("APP_DATA_DIR", "TENANTS_DIR", "TENANT_CONFIG_DB_ENABLED")
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    importlib.reload(simulator)

    for key in keys:
        assert key not in os.environ


@pytest.mark.asyncio
async def test_scoped_env_setup_restores_missing_values_after_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)
    keys = ("APP_DATA_DIR", "TENANTS_DIR", "TENANT_CONFIG_DB_ENABLED")
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    for key in keys:
        assert key not in os.environ


@pytest.mark.asyncio
async def test_tenant_config_db_forced_off_inside_scoped_env(monkeypatch: pytest.MonkeyPatch) -> None:
    env_reads: list[str | None] = []
    _patch_pipeline(monkeypatch, env_reads=env_reads)
    monkeypatch.setenv("TENANT_CONFIG_DB_ENABLED", "1")

    await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    assert env_reads
    assert set(env_reads) == {"0"}
    assert os.environ["TENANT_CONFIG_DB_ENABLED"] == "1"


@pytest.mark.asyncio
async def test_scoped_env_setup_restores_previous_values_after_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)
    monkeypatch.setenv("APP_DATA_DIR", "/tmp/custom-data")
    monkeypatch.setenv("TENANTS_DIR", "/tmp/custom-tenants")
    monkeypatch.setenv("TENANT_CONFIG_DB_ENABLED", "1")

    await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    assert os.environ["APP_DATA_DIR"] == "/tmp/custom-data"
    assert os.environ["TENANTS_DIR"] == "/tmp/custom-tenants"
    assert os.environ["TENANT_CONFIG_DB_ENABLED"] == "1"


@pytest.mark.asyncio
async def test_config_override_is_restored_after_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch)
    patched_reader = response_pipeline.read_tenant_config

    await simulator.run_cases([_case()], mode="v2", tenant_id=101, channel="avito", timeout_seconds=1)

    assert response_pipeline.read_tenant_config is patched_reader


def test_sample_cases_jsonl_is_valid() -> None:
    cases = simulator.load_cases(Path("evals/niche_brain_v2/sample_cases.jsonl"))

    assert len(cases) >= 5
    assert all(case.get("case_id") for case in cases)
    assert all(case.get("channel") == "avito" for case in cases)
