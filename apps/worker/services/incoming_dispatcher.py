from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.services import incoming_events


LogFn = Callable[[str], None]
HandlerFn = Callable[[Mapping[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class IncomingDispatcherDeps:
    handlers: Mapping[str, HandlerFn]
    log_fn: LogFn


async def handle_incoming_event(
    event: Mapping[str, Any],
    *,
    deps: IncomingDispatcherDeps,
) -> None:
    route = incoming_events.build_incoming_event_route(event)
    handler = deps.handlers.get(route.channel)
    if handler is None:
        deps.log_fn(f"event=incoming_skip_handler channel={route.channel or '-'}")
        return
    await handler(event)
