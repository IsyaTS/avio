from __future__ import annotations

from typing import Any

from fastapi import Request


async def resolve_tenant_and_key(
    request: Request | None,
    raw_tenant: int | str | None,
    raw_key: str | None,
    *,
    query_keys: tuple[str, ...] = ("key", "k"),
    allow_body: bool = True,
    json_module: Any,
) -> tuple[int | str | None, str | None]:
    tenant_candidate: int | str | None = raw_tenant
    key_candidate: str | None = raw_key

    if request is not None:
        if tenant_candidate is None:
            tenant_candidate = request.query_params.get("tenant")
        if not key_candidate:
            key_candidate = _first_mapping_value(request.query_params, query_keys)
        if not key_candidate:
            cookies = getattr(request, "cookies", None) or {}
            key_candidate = cookies.get("client_key")

        needs_body = allow_body and request.method.upper() in {"POST", "PUT", "PATCH"}
        if needs_body and (tenant_candidate is None or not key_candidate):
            payload = await _request_payload(request, json_module=json_module)
            if tenant_candidate is None:
                tenant_candidate = payload.get("tenant")
            if not key_candidate:
                key_candidate = _first_mapping_value(payload, query_keys)

    return tenant_candidate, key_candidate


async def _request_payload(request: Request, *, json_module: Any) -> dict[str, Any]:
    try:
        raw_body = await request.body()
    except Exception:
        raw_body = b""

    payload: dict[str, Any] = {}
    if raw_body:
        decoded = _decode_body(raw_body)
        if decoded:
            try:
                data = json_module.loads(decoded)
            except json_module.JSONDecodeError:
                data = {}
            if isinstance(data, dict):
                payload.update(data)

    if payload:
        return payload

    try:
        form = await request.form()
    except Exception:
        form = None
    if form is None:
        return {}
    payload = {}
    for form_key, value in form.multi_items():
        if form_key not in payload:
            payload[form_key] = value
    return payload


def _decode_body(raw_body: bytes) -> str:
    try:
        return raw_body.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def _first_mapping_value(mapping: Any, keys: tuple[str, ...]) -> str | None:
    for query_key in keys:
        value = mapping.get(query_key)
        if value:
            return value
    return None
