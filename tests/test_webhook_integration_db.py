import importlib
import os
from pathlib import Path
from typing import Any, Tuple

import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:  # pragma: no cover - environment guard
    pytest.skip("TEST_DATABASE_URL is not configured", allow_module_level=True)


def _alembic_config() -> Config:
    cfg_path = Path(__file__).resolve().parents[1] / "app" / "ops" / "alembic.ini"
    config = Config(str(cfg_path))
    config.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)
    return config


def _upgrade_and_clean() -> None:
    engine = sa.create_engine(TEST_DATABASE_URL)
    config = _alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
        connection.execute(
            sa.text(
                "TRUNCATE lead_contacts, messages, outbox, contacts, leads RESTART IDENTITY CASCADE"
            )
        )


_upgrade_and_clean()
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

# Reload database module with the test DSN and reset connection pool
import app.db as db_module

db_module = importlib.reload(db_module)
db_module.DATABASE_URL = TEST_DATABASE_URL.replace(
    "postgresql+asyncpg://", "postgresql://"
)
db_module._pool = None  # type: ignore[attr-defined]

# Reload main app to bind to the refreshed db module
import app.main as main

main = importlib.reload(main)


class _RedisCapture:
    def __init__(self) -> None:
        self.items: list[Tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> None:
        self.items.append((key, value))

    async def incrby(self, key: str, value: int) -> None:  # pragma: no cover - compatibility
        return None

    async def setnx(self, key: str, value: int) -> int:  # pragma: no cover - compatibility
        return 1

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - compatibility
        return None


@pytest.fixture(autouse=True)
def _cleanup_db() -> None:
    yield
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE lead_contacts, messages, outbox, contacts, leads RESTART IDENTITY CASCADE"
            )
        conn.commit()


@pytest.fixture
def webhook_client(monkeypatch: pytest.MonkeyPatch) -> Tuple[TestClient, _RedisCapture]:
    redis_stub = _RedisCapture()
    if hasattr(main, "_webhooks_mod"):
        monkeypatch.setattr(main._webhooks_mod, "_redis_queue", redis_stub)
        monkeypatch.setattr(main._webhooks_mod.settings, "WEBHOOK_SECRET", "webhook-secret", raising=False)

        async def _dummy_build(*args: Any, **kwargs: Any) -> list[Any]:
            return []

        async def _dummy_ask(*args: Any, **kwargs: Any) -> str:
            return "Спасибо за сообщение"

        monkeypatch.setattr(main._webhooks_mod, "build_llm_messages", _dummy_build)
        monkeypatch.setattr(main._webhooks_mod, "ask_llm", _dummy_ask)
        monkeypatch.setattr(main._webhooks_mod.core, "record_bot_reply", lambda *a, **k: None)

    main._transport_clients.clear()
    monkeypatch.setattr(main.settings, "WEBHOOK_SECRET", "webhook-secret", raising=False)
    with TestClient(main.app) as client:
        yield client, redis_stub


def test_webhook_persists_message(webhook_client: Tuple[TestClient, _RedisCapture]) -> None:
    client, redis_stub = webhook_client
    payload = {
        "tenant": 1,
        "channel": "telegram",
        "from_id": 45678,
        "text": "integration hello",
        "attachments": [],
        "ts": 1_700_100_000,
    }
    response = client.post("/webhook/telegram?token=webhook-secret", json=payload)
    assert response.status_code in (200, 202)
    assert any(key == main._webhooks_mod.INCOMING_QUEUE_KEY for key, _ in redis_stub.items)

    with psycopg.connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, telegram_user_id FROM leads ORDER BY id DESC LIMIT 1")
            lead_row = cur.fetchone()
            assert lead_row is not None
            lead_id, telegram_user_id = lead_row
            assert telegram_user_id == 45678

            cur.execute(
                "SELECT telegram_user_id FROM messages WHERE lead_id = %s", (lead_id,)
            )
            message_row = cur.fetchone()
            assert message_row is not None
            assert message_row[0] == 45678
