from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import httpx

from libs.core.models.amocrm import AmoCRMToken
from libs.core.repo import amocrm_tokens

logger = logging.getLogger(__name__)

AMOCRM_STATE_PREFIX = "amo1."
AMOCRM_STATE_TTL = 600
AMOCRM_HISTORY_LIMIT = 5

DEFAULT_STAGE_RULES: dict[int, dict[str, Any]] = {
    0: {"type": "on_first_inbound", "params": {}},
    1: {"type": "on_inbound_count", "params": {"min_inbound_messages": 2}},
    2: {"type": "on_inbound_count", "params": {"min_inbound_messages": 4}},
}


class AmoCRMError(RuntimeError):
    """Raised when AmoCRM requests fail."""


def normalize_phone(value: str) -> str:
    if value is None:
        return ""
    raw = str(value)
    cleaned = re.sub(r"[^\d+]", "", raw)
    digits = re.sub(r"\D", "", cleaned)
    if "+" in cleaned:
        return f"+{digits}" if digits else ""
    return digits


def build_history_text(texts: Sequence[str], limit: int = AMOCRM_HISTORY_LIMIT) -> str:
    if not texts:
        return ""
    safe_limit = limit if isinstance(limit, int) and limit > 0 else AMOCRM_HISTORY_LIMIT
    chunk = list(texts)[-safe_limit:]
    return "\n".join([str(item) for item in chunk if item is not None])


def extract_fields(
    rules: Sequence[Mapping[str, Any]] | None,
    *,
    last_text: str,
    history_text: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not rules:
        return results
    last_text_val = last_text or ""
    history_text_val = history_text or ""
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        key = str(rule.get("key") or "").strip()
        regex = rule.get("regex")
        if not key or not regex:
            continue
        apply_mode = str(rule.get("apply_mode") or "last_inbound").strip().lower()
        text = history_text_val if apply_mode == "any_history" else last_text_val
        if not text:
            continue
        try:
            pattern = re.compile(str(regex))
        except re.error:
            logger.warning("amocrm_regex_invalid key=%s regex=%s", key, regex)
            continue
        match = pattern.search(text)
        if not match:
            continue
        if match.groups():
            value = match.group(1)
        else:
            value = match.group(0)
        if value is None:
            continue
        value_str = str(value).strip()
        if not value_str:
            continue
        if key.lower() == "phone":
            value_str = normalize_phone(value_str)
        if not value_str:
            continue
        result = {
            "key": key,
            "value": value_str,
            "amo_field_id": rule.get("amo_field_id"),
        }
        results.append(result)
    return results


def _normalize_stage_rule(rule: Mapping[str, Any] | None, index: int) -> dict[str, Any]:
    default_rule = DEFAULT_STAGE_RULES.get(index) or {"type": "manual_only", "params": {}}
    if not isinstance(rule, Mapping):
        return dict(default_rule)
    rule_type = str(rule.get("type") or "").strip() or str(default_rule.get("type") or "manual_only")
    params = rule.get("params") if isinstance(rule.get("params"), Mapping) else None
    if not params and isinstance(default_rule.get("params"), Mapping):
        params = default_rule.get("params")
    normalized = {
        "type": rule_type or "manual_only",
        "params": params if isinstance(params, Mapping) else {},
    }
    return normalized


def _rule_satisfied(
    rule: Mapping[str, Any],
    inbound_count: int,
    last_text: str,
    extracted_fields: Mapping[str, Any],
) -> bool:
    rule_type = str(rule.get("type") or "")
    params = rule.get("params") if isinstance(rule.get("params"), Mapping) else {}
    if rule_type == "on_first_inbound":
        return int(inbound_count) <= 1
    if rule_type == "on_inbound_count":
        try:
            min_count = int(params.get("min_inbound_messages") or 0)
        except Exception:
            min_count = 0
        return int(inbound_count) >= min_count if min_count > 0 else False
    if rule_type == "on_keyword":
        keywords = params.get("keywords") or []
        if not isinstance(keywords, Iterable):
            return False
        text = (last_text or "").lower()
        return any(isinstance(word, str) and word.strip().lower() in text for word in keywords)
    if rule_type == "on_field_present":
        field_key = str(params.get("field_key") or "").strip()
        if not field_key:
            return False
        return bool(extracted_fields.get(field_key))
    if rule_type == "manual_only":
        return False
    return False


def decide_next_stage(
    stages: Sequence[Mapping[str, Any]] | None,
    current_stage_index: int,
    inbound_count: int,
    last_text: str,
    extracted_fields: Mapping[str, Any],
) -> int | None:
    if not stages:
        return None
    try:
        current_index = int(current_stage_index)
    except Exception:
        current_index = 0
    if current_index < 0:
        current_index = 0
    if current_index >= len(stages) - 1:
        return None
    current_rule = _normalize_stage_rule(
        stages[current_index].get("rule") if isinstance(stages[current_index], Mapping) else None,
        current_index,
    )
    if current_rule.get("type") == "manual_only":
        return None
    next_index = current_index + 1
    next_stage = stages[next_index] if next_index < len(stages) else None
    next_rule = _normalize_stage_rule(
        next_stage.get("rule") if isinstance(next_stage, Mapping) else None,
        next_index,
    )
    if _rule_satisfied(next_rule, inbound_count, last_text, extracted_fields):
        return next_index
    return None


def build_oauth_state(payload: Mapping[str, Any], secret: str) -> str:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    body_b64 = base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")
    return f"{AMOCRM_STATE_PREFIX}{body_b64}.{sig}"


def verify_oauth_state(state: str, secret: str, *, ttl: int = AMOCRM_STATE_TTL) -> Mapping[str, Any] | None:
    if not isinstance(state, str) or not state.startswith(AMOCRM_STATE_PREFIX):
        return None
    raw = state[len(AMOCRM_STATE_PREFIX):]
    parts = raw.split(".")
    if len(parts) != 2:
        return None
    body_b64, sig = parts
    pad = "=" * (-len(body_b64) % 4)
    try:
        body = base64.urlsafe_b64decode(body_b64 + pad)
    except Exception:
        return None
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    ts = payload.get("ts")
    if isinstance(ts, (int, float)) and ttl > 0:
        if int(time.time()) - int(ts) > int(ttl):
            return None
    return payload


class AmoCRMClient:
    def __init__(
        self,
        *,
        tenant_id: int,
        base_url: str,
        client_id: str | None = None,
        client_secret: str | None = None,
        redirect_url: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.tenant_id = int(tenant_id)
        self.base_url = (base_url or "").strip().rstrip("/")
        self.client_id = (client_id or "").strip() if client_id else ""
        self.client_secret = (client_secret or "").strip() if client_secret else ""
        self.redirect_url = (redirect_url or "").strip() if redirect_url else ""
        self.timeout = timeout if timeout and timeout > 0 else 10.0

    async def _load_tokens(self) -> AmoCRMToken | None:
        return await amocrm_tokens.get(self.tenant_id)

    async def _refresh_tokens(self, refresh_token: str) -> AmoCRMToken:
        if not self.client_id or not self.client_secret:
            raise AmoCRMError("amocrm_oauth_missing")
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        if self.redirect_url:
            payload["redirect_uri"] = self.redirect_url
        url = f"{self.base_url}/oauth2/access_token"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload)
        if response.status_code >= 400:
            raise AmoCRMError(f"amocrm_refresh_failed:{response.status_code}")
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise AmoCRMError("amocrm_refresh_invalid_json") from exc
        access_token = str(data.get("access_token") or "").strip()
        refresh_token_new = str(data.get("refresh_token") or "").strip()
        expires_in = data.get("expires_in")
        expires_at = None
        if isinstance(expires_in, (int, float)):
            expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=int(expires_in))
        obtained_at = datetime.now(tz=timezone.utc)
        return await amocrm_tokens.update_tokens(
            self.tenant_id,
            access_token=access_token or None,
            refresh_token=refresh_token_new or None,
            expires_at=expires_at,
            obtained_at=obtained_at,
        )

    async def refresh_tokens(self) -> AmoCRMToken:
        token_entry = await self._load_tokens()
        refresh_token = token_entry.refresh_token if token_entry else None
        if not refresh_token:
            raise AmoCRMError("amocrm_refresh_missing")
        return await self._refresh_tokens(refresh_token)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if not self.base_url:
            raise AmoCRMError("amocrm_base_url_missing")
        token_entry = await self._load_tokens()
        access_token = token_entry.access_token if token_entry else None
        if not access_token:
            raise AmoCRMError("amocrm_token_missing")
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method,
                url,
                json=json_body,
                params=params,
                headers=headers,
            )
        if response.status_code == 401 and token_entry and token_entry.refresh_token:
            refreshed = await self._refresh_tokens(token_entry.refresh_token)
            if not refreshed or not refreshed.access_token:
                raise AmoCRMError("amocrm_refresh_failed")
            headers["Authorization"] = f"Bearer {refreshed.access_token}"
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.request(
                    method,
                    url,
                    json=json_body,
                    params=params,
                    headers=headers,
                )
        if response.status_code >= 400:
            raise AmoCRMError(f"amocrm_http_error:{response.status_code}")
        if response.status_code == 204:
            return {}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {}

    async def get_pipelines(self) -> Mapping[str, Any]:
        return await self._request("GET", "/api/v4/leads/pipelines")

    async def get_pipeline_stages(self, pipeline_id: int) -> Mapping[str, Any]:
        return await self._request("GET", f"/api/v4/leads/pipelines/{int(pipeline_id)}")

    async def get_lead_custom_fields(self) -> Mapping[str, Any]:
        return await self._request("GET", "/api/v4/leads/custom_fields")

    async def create_lead_custom_field(self, *, name: str) -> int | None:
        payload = [{"name": name, "type": "text"}]
        created = await self._request("POST", "/api/v4/leads/custom_fields", json_body=payload)
        embedded = created.get("_embedded") if isinstance(created, Mapping) else None
        fields = embedded.get("custom_fields") if isinstance(embedded, Mapping) else None
        if isinstance(fields, list) and fields:
            field_id = fields[0].get("id")
            try:
                return int(field_id)
            except Exception:
                return None
        return None

    async def get_account(self, *, with_drive_url: bool = False) -> Mapping[str, Any]:
        params = {"with": "drive_url"} if with_drive_url else None
        return await self._request("GET", "/api/v4/account", params=params)

    async def upsert_contact(self, *, phone: str | None, name: str | None) -> int | None:
        contact_id = None
        phone_value = (phone or "").strip()
        if phone_value:
            data = await self._request("GET", "/api/v4/contacts", params={"query": phone_value})
            embedded = data.get("_embedded") if isinstance(data, Mapping) else None
            contacts = embedded.get("contacts") if isinstance(embedded, Mapping) else None
            if isinstance(contacts, list) and contacts:
                contact_id = contacts[0].get("id")
        if contact_id:
            try:
                return int(contact_id)
            except Exception:
                return None
        payload: dict[str, Any] = {}
        if name:
            payload["name"] = name
        if phone_value:
            payload["custom_fields_values"] = [
                {
                    "field_code": "PHONE",
                    "values": [{"value": phone_value}],
                }
            ]
        if not payload:
            return None
        created = await self._request("POST", "/api/v4/contacts", json_body=[payload])
        embedded = created.get("_embedded") if isinstance(created, Mapping) else None
        contacts = embedded.get("contacts") if isinstance(embedded, Mapping) else None
        if isinstance(contacts, list) and contacts:
            contact_id = contacts[0].get("id")
            try:
                return int(contact_id)
            except Exception:
                return None
        return None

    async def create_lead(
        self,
        *,
        pipeline_id: int,
        status_id: int,
        name: str,
        contact_id: int | None,
        custom_fields: list[dict[str, Any]] | None = None,
    ) -> int | None:
        payload: dict[str, Any] = {
            "name": name,
            "pipeline_id": int(pipeline_id),
            "status_id": int(status_id),
        }
        if custom_fields:
            payload["custom_fields_values"] = custom_fields
        if contact_id:
            payload["_embedded"] = {"contacts": [{"id": int(contact_id)}]}
        created = await self._request("POST", "/api/v4/leads", json_body=[payload])
        embedded = created.get("_embedded") if isinstance(created, Mapping) else None
        leads = embedded.get("leads") if isinstance(embedded, Mapping) else None
        if isinstance(leads, list) and leads:
            lead_id = leads[0].get("id")
            try:
                return int(lead_id)
            except Exception:
                return None
        return None

    async def update_lead_fields(
        self,
        lead_id: int,
        *,
        custom_fields: list[dict[str, Any]],
    ) -> None:
        if not custom_fields:
            return
        payload = [{"id": int(lead_id), "custom_fields_values": custom_fields}]
        await self._request("PATCH", "/api/v4/leads", json_body=payload)

    async def update_contact_fields(
        self,
        contact_id: int,
        *,
        custom_fields: list[dict[str, Any]],
    ) -> None:
        if not custom_fields:
            return
        payload = [{"id": int(contact_id), "custom_fields_values": custom_fields}]
        await self._request("PATCH", "/api/v4/contacts", json_body=payload)

    async def get_lead_contact_id(self, lead_id: int) -> int | None:
        payload = await self._request("GET", f"/api/v4/leads/{int(lead_id)}")
        embedded = payload.get("_embedded") if isinstance(payload, Mapping) else None
        contacts = embedded.get("contacts") if isinstance(embedded, Mapping) else None
        if isinstance(contacts, list) and contacts:
            contact_id = contacts[0].get("id")
            try:
                return int(contact_id)
            except Exception:
                return None
        return None

    async def move_lead_stage(
        self,
        lead_id: int,
        *,
        status_id: int,
        pipeline_id: int | None = None,
    ) -> None:
        payload: dict[str, Any] = {"id": int(lead_id), "status_id": int(status_id)}
        if pipeline_id:
            payload["pipeline_id"] = int(pipeline_id)
        await self._request("PATCH", "/api/v4/leads", json_body=[payload])

    async def add_lead_note(self, lead_id: int, text: str) -> None:
        note = {
            "note_type": "common",
            "params": {"text": text},
        }
        await self._request("POST", f"/api/v4/leads/{int(lead_id)}/notes", json_body=[note])

    async def _resolve_drive_url(self) -> str:
        payload = await self.get_account(with_drive_url=True)
        drive_url = str(payload.get("drive_url") or "").strip()
        if not drive_url:
            links = payload.get("_links") if isinstance(payload, Mapping) else None
            if isinstance(links, Mapping):
                drive_link = links.get("drive_url") if isinstance(links.get("drive_url"), Mapping) else None
                if isinstance(drive_link, Mapping):
                    drive_url = str(drive_link.get("href") or "").strip()
        return drive_url.rstrip("/")

    async def upload_file(
        self,
        *,
        filename: str,
        content: bytes,
        content_type: str | None = None,
    ) -> str | None:
        if not filename or not content:
            return None
        if not self.base_url:
            raise AmoCRMError("amocrm_base_url_missing")
        token_entry = await self._load_tokens()
        access_token = token_entry.access_token if token_entry else None
        if not access_token:
            raise AmoCRMError("amocrm_token_missing")
        drive_url = await self._resolve_drive_url()
        if not drive_url:
            raise AmoCRMError("amocrm_drive_url_missing")
        headers = {"Authorization": f"Bearer {access_token}"}
        session_payload = {
            "file_name": filename,
            "file_size": len(content),
        }
        if content_type:
            session_payload["content_type"] = content_type
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            session_resp = await client.post(
                f"{drive_url}/v1.0/sessions",
                json=session_payload,
                headers=headers,
            )
        if session_resp.status_code >= 400:
            raise AmoCRMError(f"amocrm_http_error:{session_resp.status_code}")
        try:
            session_data = session_resp.json()
        except json.JSONDecodeError as exc:
            raise AmoCRMError("amocrm_upload_session_invalid_json") from exc
        upload_url = str(session_data.get("upload_url") or "").strip()
        max_part_size = int(session_data.get("max_part_size") or 0)
        if not upload_url:
            raise AmoCRMError("amocrm_upload_url_missing")
        part_size = max_part_size if max_part_size > 0 else len(content)
        offset = 0
        next_url = upload_url
        uploaded_uuid: str | None = None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while offset < len(content):
                chunk = content[offset : offset + part_size]
                resp = await client.post(next_url, content=chunk, headers=headers)
                if resp.status_code >= 400:
                    raise AmoCRMError(f"amocrm_http_error:{resp.status_code}")
                try:
                    data = resp.json()
                except json.JSONDecodeError as exc:
                    raise AmoCRMError("amocrm_upload_chunk_invalid_json") from exc
                if "uuid" in data:
                    uploaded_uuid = str(data.get("uuid") or "").strip() or None
                    break
                next_url = str(data.get("next_url") or "").strip()
                if not next_url:
                    break
                offset += part_size
        return uploaded_uuid

    async def attach_file_to_lead(self, lead_id: int, file_uuid: str) -> None:
        if not file_uuid:
            return
        payload = [{"file_uuid": file_uuid}]
        await self._request("PUT", f"/api/v4/leads/{int(lead_id)}/files", json_body=payload)


__all__ = [
    "AmoCRMClient",
    "AmoCRMError",
    "normalize_phone",
    "extract_fields",
    "decide_next_stage",
    "build_oauth_state",
    "verify_oauth_state",
    "build_history_text",
    "AMOCRM_STATE_TTL",
    "AMOCRM_STATE_PREFIX",
]
