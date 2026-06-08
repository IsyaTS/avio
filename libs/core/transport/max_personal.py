from __future__ import annotations

from typing import Any, Mapping

import httpx

from libs.core.services import max_personal_service


def _auth_headers() -> dict[str, str]:
    token = max_personal_service.max_personal_worker_token()
    if not token:
        return {}
    return {"X-Auth-Token": token}


def _base_url() -> str:
    return max_personal_service.max_personal_worker_url().rstrip("/")


async def _request(
    method: str,
    path: str,
    *,
    params: Mapping[str, Any] | None = None,
    json_body: Mapping[str, Any] | None = None,
    timeout: float = 10.0,
) -> tuple[int, Any]:
    url = f"{_base_url()}{path}"
    headers = _auth_headers()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method.upper(),
                url,
                params=dict(params or {}),
                json=dict(json_body or {}) if json_body is not None else None,
                headers=headers,
            )
    except httpx.HTTPError as exc:
        return 0, {"error": str(exc)}
    try:
        payload = response.json()
    except Exception:
        payload = {"body": response.text}
    return int(response.status_code or 0), payload


async def start_session(
    tenant_id: int,
    *,
    callback_url: str,
    webhook_token: str | None = None,
    force: bool = False,
) -> tuple[int, Any]:
    body: dict[str, Any] = {
        "tenant": int(tenant_id),
        "callback_url": str(callback_url or "").strip(),
        "force": bool(force),
    }
    if webhook_token:
        body["webhook_token"] = str(webhook_token).strip()
    return await _request("POST", "/session/start", json_body=body, timeout=120.0)


async def get_qr(tenant_id: int) -> tuple[int, Any]:
    return await _request("GET", "/session/qr", params={"tenant": int(tenant_id)}, timeout=10.0)


async def get_status(tenant_id: int) -> tuple[int, Any]:
    return await _request(
        "GET",
        "/session/status",
        params={"tenant": int(tenant_id)},
        timeout=10.0,
    )


async def logout_session(tenant_id: int) -> tuple[int, Any]:
    return await _request(
        "POST",
        "/session/logout",
        json_body={"tenant": int(tenant_id)},
        timeout=12.0,
    )


async def send_message(
    tenant_id: int,
    *,
    chat_id: str | int,
    text: str,
    attachments: list[Mapping[str, Any]] | None = None,
    dedupe_key: str | None = None,
    idempotency_key: str | None = None,
) -> tuple[int, Any]:
    body: dict[str, Any] = {
        "tenant": int(tenant_id),
        "to": chat_id,
        "text": str(text or ""),
    }
    if attachments:
        body["attachments"] = [dict(item) for item in attachments if isinstance(item, Mapping)]
    if dedupe_key:
        body["dedupe_key"] = str(dedupe_key)
    if idempotency_key:
        body["idempotency_key"] = str(idempotency_key)
    return await _request("POST", "/send", json_body=body, timeout=60.0)


__all__ = [
    "get_qr",
    "get_status",
    "logout_session",
    "send_message",
    "start_session",
]
