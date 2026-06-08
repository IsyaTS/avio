from __future__ import annotations

import base64
import io
import json
import math
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping

import qrcode
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from qrcode.image.svg import SvgImage


SyncFn = Any


@dataclass(frozen=True)
class WaQrDeps:
    common_module: Any
    settings: Any
    client_config_module: Any
    redis_error_type: type[Exception]
    logger: Any
    no_store_headers_fn: SyncFn
    qr_cache_ttl_fn: SyncFn


def normalize_qr_id(value: Any) -> str | None:
    if value is None:
        return None
    candidate = value
    if isinstance(candidate, (bytes, bytearray)):
        try:
            candidate = candidate.decode("utf-8", errors="ignore")
        except Exception:
            candidate = bytes(candidate).decode("utf-8", errors="ignore")
    if isinstance(candidate, bool):
        candidate = int(candidate)
    if isinstance(candidate, int):
        return str(candidate)
    if isinstance(candidate, float):
        if not math.isfinite(candidate):
            return None
        return str(int(candidate))
    text = str(candidate).strip()
    if not text:
        return None
    return text


def derive_wa_state(data: Mapping[str, Any] | None) -> tuple[str | None, bool]:
    if not isinstance(data, Mapping):
        return None, False
    state_value = data.get("state")
    if state_value is not None:
        state_value = str(state_value)
    ready_flag = _truthy_flag(data.get("ready"))
    need_qr_flag = _truthy_flag(data.get("need_qr"))
    qr_flag = _truthy_flag(data.get("qr"))
    if state_value is None:
        if ready_flag:
            state_value = "ready"
        elif need_qr_flag or qr_flag:
            state_value = "qr"
        elif data.get("last") is not None:
            state_value = str(data.get("last"))
    if not need_qr_flag:
        need_qr_flag = not ready_flag and (qr_flag or state_value == "qr")
    return state_value, need_qr_flag


def fetch_qr_bytes(url: str, deps: WaQrDeps, timeout: float = 6.0) -> tuple[int, str, bytes]:
    req = urllib.request.Request(url, method="GET")
    try:
        token = (
            getattr(deps.client_config_module, "WA_WEB_TOKEN", "")
            or getattr(deps.client_config_module, "WA_INTERNAL_TOKEN", "")
            or ""
        )
        if token:
            req.add_header("X-Auth-Token", token)
    except Exception:
        pass
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            try:
                deps.logger.info(
                    "qr_upstream ok code=%s ctype=%s len=%s",
                    getattr(resp, "status", 200),
                    ctype,
                    len(body or b""),
                )
            except Exception:
                pass
            return resp.status, ctype, body
    except urllib.error.HTTPError as exc:
        try:
            data = exc.read()
        except Exception:
            data = b""
        try:
            deps.logger.info(
                "qr_upstream http_error code=%s len=%s",
                getattr(exc, "code", 0),
                len(data or b""),
            )
        except Exception:
            pass
        return exc.code, "", data
    except Exception as exc:  # pragma: no cover
        try:
            deps.logger.exception("qr_upstream failed: %s", exc)
        except Exception:
            pass
        return 0, "", b""


def build_qr_candidates(tenant: int, cache_bust: int, deps: WaQrDeps) -> list[tuple[str, str]]:
    base = deps.common_module.wa_base_url(int(tenant) if tenant is not None else None).rstrip("/")
    ts_param = f"ts={cache_bust}"
    return [
        (f"{base}/session/{tenant}/qr?format=svg&{ts_param}", "tenant_query_svg"),
        (f"{base}/session/{tenant}/qr.svg?{ts_param}", "tenant_ext_svg"),
        (f"{base}/session/{tenant}/qr.png?{ts_param}", "tenant_ext_png"),
        (f"{base}/session/qr?format=svg&{ts_param}", "global_query_svg"),
        (f"{base}/session/qr.svg?{ts_param}", "global_ext_svg"),
        (f"{base}/session/qr?format=png&{ts_param}", "global_query_png"),
        (f"{base}/session/qr.png?{ts_param}", "global_ext_png"),
    ]


def proxy_qr_with_fallbacks(tenant: int, deps: WaQrDeps) -> Response:
    if deps.common_module.whatsapp_provider(int(tenant)) == "baileys":
        return proxy_baileys_qr(tenant, deps)
    deps.logger.info("qr_fetch start tenant=%s", tenant)
    prefetch_qr_session_start(tenant, deps)
    attempts, retry_delay = qr_fetch_retry_settings(deps)
    last_status, last_stage, last_body_present, last_content_type = 0, "", False, ""
    for attempt in range(attempts):
        result = try_fetch_qr_candidate(tenant, attempt, deps)
        last_status = result["status"]
        last_stage = result["stage"]
        last_body_present = result["body_present"]
        last_content_type = result["content_type"]
        if result["response"] is not None:
            return result["response"]
        if attempt + 1 < attempts and retry_delay:
            deps.logger.info("qr_fetch_retry sleep=%s attempt=%s", retry_delay, attempt + 1)
            try:
                time.sleep(retry_delay)
            except Exception:
                deps.logger.info("qr_fetch_retry_sleep_failed attempt=%s", attempt + 1)
    return qr_fetch_error_response(
        last_status,
        last_stage,
        last_body_present,
        last_content_type,
        deps,
    )


def prefetch_qr_session_start(tenant: int, deps: WaQrDeps) -> None:
    if not getattr(deps.settings, "WA_PREFETCH_START", True):
        return
    try:
        hook = deps.common_module.webhook_url()
        payload = json.dumps({"tenant_id": int(tenant), "webhook_url": hook}, ensure_ascii=False).encode("utf-8")
        code, _ = deps.common_module.http(
            "POST",
            f"{deps.common_module.wa_base_url(int(tenant))}/session/{int(tenant)}/start",
            body=payload,
            timeout=4.0,
        )
        deps.logger.info("qr_prefetch_start code=%s", code)
    except Exception:
        deps.logger.info("qr_prefetch_start_failed")


def qr_fetch_retry_settings(deps: WaQrDeps) -> tuple[int, float]:
    attempts_raw = getattr(deps.settings, "WA_QR_FETCH_ATTEMPTS", 1) or 1
    try:
        attempts = max(1, int(attempts_raw))
    except (TypeError, ValueError):
        attempts = 1
    delay_raw = getattr(deps.settings, "WA_QR_FETCH_RETRY_DELAY", 0.0) or 0.0
    try:
        retry_delay = max(0.0, float(delay_raw))
    except (TypeError, ValueError):
        retry_delay = 0.0
    return attempts, retry_delay


def try_fetch_qr_candidate(tenant: int, attempt: int, deps: WaQrDeps) -> dict[str, Any]:
    cache_bust = int(time.time() * 1000)
    result: dict[str, Any] = {
        "status": 0,
        "stage": "",
        "body_present": False,
        "content_type": "",
        "response": None,
    }
    for url, stage in build_qr_candidates(tenant, cache_bust, deps):
        deps.logger.info("qr_fetch url=%s stage=%s attempt=%s", url, stage, attempt + 1)
        status, ctype, body = fetch_qr_bytes(url, deps)
        result.update(
            status=status,
            stage=stage,
            body_present=bool(body),
            content_type=(ctype or "").lower(),
        )
        deps.logger.info("upstream status=%s stage=%s attempt=%s", status, stage, attempt + 1)
        if int(status or 0) == 200 and result["content_type"].startswith("image/") and body:
            headers = {"Cache-Control": "no-store", "X-Debug-Stage": f"served_qr:{stage}"}
            deps.logger.info(
                "return=200 len=%s ctype=%s stage=%s attempt=%s",
                len(body or b""),
                ctype,
                stage,
                attempt + 1,
            )
            result["response"] = StreamingResponse(io.BytesIO(body), media_type=ctype, headers=headers)
            return result
    return result


def qr_fetch_error_response(
    last_status: int,
    last_stage: str,
    last_body_present: bool,
    last_content_type: str,
    deps: WaQrDeps,
) -> Response:
    headers = deps.no_store_headers_fn()
    headers["Cache-Control"] = "no-store"
    if int(last_status or 0) in (204, 404) or (
        int(last_status or 0) == 200
        and (not last_body_present or not last_content_type.startswith("image/"))
    ):
        stage_label = last_stage or "unknown"
        headers["X-Debug-Stage"] = f"no_content:{stage_label}"
        deps.logger.info("return=204 stage=%s status=%s", last_stage, last_status)
        return Response(status_code=204, headers=headers)

    headers["X-Debug-Stage"] = f"bad_gateway:{last_stage}" if last_stage else "bad_gateway"
    deps.logger.info("return=502 stage=%s status=%s", last_stage, last_status)
    return JSONResponse({"error": "wa_unavailable"}, status_code=502, headers=headers)


def get_last_qr_id(tenant: int, deps: WaQrDeps) -> tuple[str | None, bool]:
    key = f"wa:qr:last:{tenant}"
    try:
        client = deps.common_module.redis_client()
        value = client.get(key)
    except deps.redis_error_type:
        return None, True
    if not value:
        return None, False
    return normalize_qr_id(value), False


def load_cached_qr_entry(tenant: int, qr_id: str, deps: WaQrDeps) -> tuple[dict[str, Any] | None, bool]:
    key = f"wa:qr:{tenant}:{qr_id}"
    try:
        client = deps.common_module.redis_client()
        raw = client.get(key)
    except deps.redis_error_type:
        return None, True
    entry: dict[str, Any] | None = None
    if raw is None:
        try:
            svg_value, png_value, txt_value = client.mget(
                f"{key}:svg",
                f"{key}:png",
                f"{key}:txt",
            )
        except deps.redis_error_type:
            return None, True
        interim: dict[str, Any] = {}
        if svg_value:
            interim["qr_svg"] = svg_value
        if png_value:
            interim["qr_png"] = png_value
        if txt_value:
            interim["qr_text"] = txt_value
        if interim:
            entry = interim
        else:
            return None, False
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    if entry is None:
        entry = parse_qr_cache_entry(raw)
    if entry is None:
        return None, False
    result: dict[str, Any] = dict(entry)
    result.setdefault("tenant", tenant)
    result.setdefault("ts", qr_id)
    _drop_empty_qr_cache_values(result)
    return result, False


def parse_qr_cache_entry(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            return parsed
        if "<svg" in stripped.lower():
            return {"qr_svg": stripped}
        return {"qr_text": stripped}
    if isinstance(raw, dict):
        return raw
    return None


def _drop_empty_qr_cache_values(result: dict[str, Any]) -> None:
    if isinstance(result.get("qr_svg"), str) and not result["qr_svg"].strip():
        result.pop("qr_svg", None)
    if isinstance(result.get("qr_png"), str) and not result["qr_png"].strip():
        result.pop("qr_png", None)
    if isinstance(result.get("qr_text"), str) and not result["qr_text"].strip():
        result.pop("qr_text", None)


def resolve_cached_qr(tenant: int, deps: WaQrDeps) -> tuple[str | None, dict[str, Any] | None, bool]:
    qr_id, redis_failed = get_last_qr_id(tenant, deps)
    if redis_failed:
        return None, None, True
    if not qr_id:
        return None, None, False
    entry, entry_failed = load_cached_qr_entry(tenant, qr_id, deps)
    if entry_failed:
        return None, None, True
    if entry is None:
        return None, None, False
    return qr_id, entry, False


def load_cached_svg(tenant: int, qr_id: str, deps: WaQrDeps) -> tuple[str | None, bool]:
    key = f"wa:qr:{tenant}:{qr_id}:svg"
    try:
        client = deps.common_module.redis_client()
        cached = client.get(key)
    except deps.redis_error_type:
        return None, True
    if cached:
        candidate = cached
        if isinstance(candidate, (bytes, bytearray)):
            candidate = bytes(candidate).decode("utf-8", errors="ignore")
        candidate_str = str(candidate).strip()
        if candidate_str:
            return candidate_str, False
    entry, failed = load_cached_qr_entry(tenant, qr_id, deps)
    if failed:
        return None, True
    if entry is None:
        return None, False
    svg_value = entry.get("qr_svg") if isinstance(entry, dict) else None
    if isinstance(svg_value, str) and svg_value.strip():
        return svg_value, False
    qr_text = entry.get("qr_text") if isinstance(entry, dict) else None
    if isinstance(qr_text, str) and qr_text.strip():
        rendered = render_qr_svg_from_text(qr_text.strip())
        if rendered:
            try:
                cache_qr_payload(
                    tenant,
                    qr_id,
                    {"qr_svg": rendered, "qr_text": qr_text.strip()},
                    deps,
                    include_last=False,
                )
            except Exception:
                deps.logger.info("wa_qr_cache_update_failed tenant=%s qr_id=%s", tenant, qr_id)
            return rendered, False
    return None, False


def qr_expired_response(no_store_headers_fn: SyncFn, qr_id: str | None = None) -> JSONResponse:
    headers = no_store_headers_fn()
    if qr_id:
        headers["X-WA-QR-ID"] = str(qr_id)
    return JSONResponse({"error": "qr_expired"}, status_code=410, headers=headers)


def as_head_response(response: Response, request: Request) -> Response:
    if request.method.upper() != "HEAD":
        return response

    headers = dict(response.headers.items())
    media_type = response.media_type or headers.get("content-type") or headers.get("Content-Type")
    return Response(status_code=response.status_code, headers=headers, media_type=media_type)


def render_qr_svg_from_text(qr_text: str) -> str | None:
    if not qr_text:
        return None
    qr = qrcode.QRCode(
        error_correction=qrcode.constants.ERROR_CORRECT_Q,
        box_size=8,
        border=2,
    )
    qr.add_data(qr_text)
    qr.make(fit=True)
    img = qr.make_image(image_factory=SvgImage)
    buf = io.BytesIO()
    img.save(buf)
    return buf.getvalue().decode("utf-8")


def render_qr_png_bytes(qr_text: str) -> bytes | None:
    if not qr_text:
        return None
    qr = qrcode.QRCode(
        error_correction=qrcode.constants.ERROR_CORRECT_Q,
        box_size=10,
        border=2,
    )
    qr.add_data(qr_text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#000000", back_color="#FFFFFF").convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def cache_qr_payload(
    tenant: int,
    qr_id: str,
    entry: Mapping[str, Any],
    deps: WaQrDeps,
    *,
    include_last: bool = True,
) -> None:
    if not qr_id:
        return
    serialisable, svg_value, png_value, txt_value = normalize_qr_cache_values(tenant, qr_id, entry)
    try:
        json_payload = json.dumps(serialisable, ensure_ascii=False)
    except (TypeError, ValueError):
        json_payload = None
    write_qr_cache_values(
        tenant,
        qr_id,
        json_payload=json_payload,
        svg_value=svg_value,
        png_value=png_value,
        txt_value=txt_value,
        include_last=include_last,
        deps=deps,
    )


def normalize_qr_cache_values(
    tenant: int,
    qr_id: str,
    entry: Mapping[str, Any],
) -> tuple[dict[str, Any], str | None, str | None, str | None]:
    data = dict(entry or {})
    data.setdefault("tenant", tenant)
    data.setdefault("qr_id", qr_id)
    data.setdefault("ts", data.get("ts") or data.get("timestamp") or qr_id)
    svg_value = data.get("qr_svg")
    if isinstance(svg_value, str):
        svg_value = svg_value.strip() or None
    else:
        svg_value = None

    png_value = data.get("qr_png") or data.get("qr_png_base64")
    raw_png_bytes = data.get("qr_png_bytes")
    if png_value is None and isinstance(raw_png_bytes, (bytes, bytearray)):
        png_value = base64.b64encode(raw_png_bytes).decode("utf-8")
    if isinstance(png_value, (bytes, bytearray)):
        png_value = base64.b64encode(png_value).decode("utf-8")
    elif isinstance(png_value, str):
        png_value = png_value.strip() or None
    else:
        png_value = None

    txt_value = data.get("qr_text") or data.get("txt")
    if isinstance(txt_value, str):
        txt_value = txt_value.strip() or None
    else:
        txt_value = None

    serialisable = dict(data)
    if svg_value is None:
        serialisable.pop("qr_svg", None)
    else:
        serialisable["qr_svg"] = svg_value
    serialisable.pop("qr_png_bytes", None)
    if png_value is None:
        serialisable.pop("qr_png", None)
    else:
        serialisable["qr_png"] = png_value
    if txt_value is None:
        serialisable.pop("qr_text", None)
    else:
        serialisable["qr_text"] = txt_value
    return serialisable, svg_value, png_value, txt_value


def write_qr_cache_values(
    tenant: int,
    qr_id: str,
    *,
    json_payload: str | None,
    svg_value: str | None,
    png_value: str | None,
    txt_value: str | None,
    include_last: bool,
    deps: WaQrDeps,
) -> None:
    try:
        client = deps.common_module.redis_client()
        pipe = client.pipeline()
        wrote = False
        ttl = deps.qr_cache_ttl_fn()
        if json_payload is not None:
            pipe.setex(f"wa:qr:{tenant}:{qr_id}", ttl, json_payload)
            wrote = True
        if svg_value is not None:
            pipe.setex(f"wa:qr:{tenant}:{qr_id}:svg", ttl, svg_value)
            wrote = True
        if png_value is not None:
            pipe.setex(f"wa:qr:{tenant}:{qr_id}:png", ttl, png_value)
            wrote = True
        if txt_value is not None:
            pipe.setex(f"wa:qr:{tenant}:{qr_id}:txt", ttl, txt_value)
            wrote = True
        if include_last:
            pipe.setex(f"wa:qr:last:{tenant}", ttl, qr_id)
            wrote = True
        if wrote:
            pipe.execute()
    except deps.redis_error_type:
        deps.logger.info("wa_qr_cache_write_skip tenant=%s qr_id=%s", tenant, qr_id)


def persist_qr_entry(tenant: int, qr_id: str, entry: Mapping[str, Any], deps: WaQrDeps) -> None:
    cache_qr_payload(tenant, qr_id, entry, deps, include_last=True)


def wa_qr_png_response(
    *,
    request: Request,
    tenant: int,
    key: str,
    qr_id: str | None,
    ensure_valid_qr_request_fn: SyncFn,
    invalid_key_response_fn: SyncFn,
    deps: WaQrDeps,
) -> Response:
    ok = ensure_valid_qr_request_fn(tenant, key, request, query_param_only=True)
    if ok is None:
        return invalid_key_response_fn()
    tenant_id, _ = ok

    requested_id = (qr_id or "").strip()
    redis_failed = False
    if not requested_id:
        requested_id, redis_failed = get_last_qr_id(tenant_id, deps)
    if redis_failed:
        return JSONResponse({"error": "wa_cache_unavailable"}, status_code=503)
    if not requested_id:
        return qr_expired_response(deps.no_store_headers_fn)

    entry, redis_failed = load_cached_qr_entry(tenant_id, requested_id, deps)
    if redis_failed:
        return JSONResponse({"error": "wa_cache_unavailable"}, status_code=503)

    binary, mutated = _qr_png_binary_from_entry(entry, tenant_id, requested_id, deps)
    if binary is None:
        return qr_expired_response(deps.no_store_headers_fn, requested_id)

    if mutated:
        try:
            persist_qr_entry(tenant_id, requested_id, dict(entry or {}), deps)
        except Exception:
            deps.logger.info(
                "wa_qr_cache_update_failed tenant=%s qr_id=%s format=png",
                tenant_id,
                requested_id,
            )

    headers = deps.no_store_headers_fn()
    headers["X-WA-QR-ID"] = requested_id
    return Response(content=binary, media_type="image/png", headers=headers)


def _qr_png_binary_from_entry(
    entry: Mapping[str, Any] | None,
    tenant_id: int,
    requested_id: str,
    deps: WaQrDeps,
) -> tuple[bytes | None, bool]:
    png_value = entry.get("qr_png") if isinstance(entry, Mapping) else None
    binary: bytes | None = None
    mutated = False
    if isinstance(png_value, str) and png_value.strip():
        normalized = png_value.split(",")[-1].strip()
        try:
            binary = base64.b64decode(normalized, validate=False)
        except Exception:
            binary = None
            deps.logger.warning("wa_qr_cache_invalid_png tenant=%s qr_id=%s", tenant_id, requested_id)

    if binary is None:
        qr_text = entry.get("qr_text") if isinstance(entry, Mapping) else None
        if isinstance(qr_text, str) and qr_text.strip():
            binary = render_qr_png_bytes(qr_text.strip())
            if binary and isinstance(entry, dict):
                entry["qr_png"] = base64.b64encode(binary).decode("ascii")
                mutated = True
    return binary, mutated


def proxy_baileys_qr(tenant: int, deps: WaQrDeps) -> Response:
    deps.logger.info("qr_fetch start tenant=%s provider=baileys", tenant)
    code, raw = deps.common_module.wabaileys_http("GET", f"/sessions/status?tenant={int(tenant)}", timeout=3.0)
    if int(code or 0) < 200 or int(code or 0) >= 300:
        return Response(b"", media_type="image/svg+xml", status_code=int(code or 0) or 502)
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    session = data.get("session") if isinstance(data, Mapping) else None
    if not isinstance(session, Mapping):
        session = {}
    qr_block = session.get("qr") if isinstance(session.get("qr"), Mapping) else {}
    if not qr_block:
        return Response(b"", media_type="image/svg+xml", status_code=404)
    qr_id = str(qr_block.get("id") or qr_block.get("raw") or "")
    headers = deps.no_store_headers_fn()
    if qr_id:
        headers["X-WA-QR-ID"] = qr_id
    svg_blob = qr_block.get("svg")
    if isinstance(svg_blob, str) and svg_blob.strip():
        return Response(svg_blob.encode("utf-8"), media_type="image/svg+xml", headers=headers)
    png_blob = qr_block.get("png")
    if isinstance(png_blob, str) and png_blob.strip():
        try:
            binary = base64.b64decode(png_blob, validate=True)
        except Exception:
            binary = b""
        if binary:
            return Response(binary, media_type="image/png", headers=headers)
    return Response(b"", media_type="image/svg+xml", status_code=404)


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "ready", "connected", "qr"}
