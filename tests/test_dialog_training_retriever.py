from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.training import dialog_retriever


pytestmark = pytest.mark.unit


def test_dialog_retriever_builds_index_from_markdown_and_masks_unsafe_fact(tmp_path: Path) -> None:
    md = tmp_path / "dialogs.md"
    md.write_text(
        "\n".join(
            [
                "# Client 1",
                "Клиент: Где можно посмотреть двери?",
                "Менеджер: В Уфе магазин находится по адресу Менделеева 80",
                "",
                "# Client 2",
                "Клиент: Нужна дверь с терморазрывом",
                "Менеджер: Уточните размер проема и город установки",
            ]
        ),
        encoding="utf-8",
    )

    index = dialog_retriever.build_index_from_markdown(md)
    assert index is not None
    tenant_dir = tmp_path / "tenant"
    dialog_retriever.save_dialog_training_index(index, tenant_dir=tenant_dir)
    dialog_retriever._CACHE.clear()

    block = dialog_retriever.build_dialog_examples_block(
        101,
        "где можно посмотреть двери?",
        tenant_dir_fn=lambda _tenant: tenant_dir,
        top_k=1,
        min_score=0.01,
    )

    assert "Похожие реальные диалоги менеджера" in block
    assert "Клиент:" in block
    assert "Менеджер:" in block
    assert "город клиента не определён" in block
    assert "не копируй факт дословно" in block
    assert "Менделеева 80" not in block


def test_dialog_retriever_keeps_contextual_fact_when_query_has_city(tmp_path: Path) -> None:
    dialogs = [
        [
            {"role": "client", "text": "Я из Уфы, где магазин?"},
            {"role": "manager", "text": "В Уфе магазин находится по адресу Менделеева 80"},
        ]
    ]
    index = dialog_retriever.build_index_from_dialogs(dialogs)
    assert index is not None
    tenant_dir = tmp_path / "tenant"
    dialog_retriever.save_dialog_training_index(index, tenant_dir=tenant_dir)
    dialog_retriever._CACHE.clear()

    block = dialog_retriever.build_dialog_examples_block(
        101,
        "Я из Уфы, где магазин?",
        tenant_dir_fn=lambda _tenant: tenant_dir,
        top_k=1,
        min_score=0.01,
    )

    assert "Менделеева 80" in block
    assert "город клиента не определён" not in block


def test_dialog_retriever_uses_latest_manifest_not_lexicographic_sha(tmp_path: Path) -> None:
    old = dialog_retriever.build_index_from_dialogs(
        [[{"role": "client", "text": "старый вопрос"}, {"role": "manager", "text": "старый ответ"}]]
    )
    new = dialog_retriever.build_index_from_dialogs(
        [
            [{"role": "client", "text": "новый вопрос"}, {"role": "manager", "text": "новый ответ"}],
            [{"role": "client", "text": "ещё новый вопрос"}, {"role": "manager", "text": "ещё новый ответ"}],
        ]
    )
    assert old is not None
    assert new is not None

    tenant_dir = tmp_path / "tenant"
    idx_dir = tenant_dir / "indexes"
    idx_dir.mkdir(parents=True)

    old_path = idx_dir / "dialog_training_zzzz_old.pkl"
    new_path = idx_dir / "dialog_training_aaaa_new.pkl"
    old.save(old_path)
    new.save(new_path)
    old_path.with_suffix(".manifest.json").write_text(
        json.dumps({"type": "dialog_training", "created_at": 100, "dialogs": 1}),
        encoding="utf-8",
    )
    new_path.with_suffix(".manifest.json").write_text(
        json.dumps({"type": "dialog_training", "created_at": 200, "dialogs": 2}),
        encoding="utf-8",
    )
    dialog_retriever._CACHE.clear()

    loaded = dialog_retriever.ensure_dialog_index(101, tenant_dir_fn=lambda _tenant: tenant_dir)

    assert loaded is not None
    assert loaded.sha1 == new.sha1
    assert len(loaded.items) == 2
