from __future__ import annotations

import pytest

from apps.worker.services import incoming_dispatcher


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_incoming_dispatcher_skips_unknown_channel() -> None:
    logs: list[str] = []

    async def _unexpected(_event):
        raise AssertionError("should not be called")

    await incoming_dispatcher.handle_incoming_event(
        {"channel": "unknown", "text": "hello"},
        deps=incoming_dispatcher.IncomingDispatcherDeps(
            handlers={"whatsapp": _unexpected},
            log_fn=logs.append,
        ),
    )

    assert logs == ["event=incoming_skip_handler channel=unknown"]
