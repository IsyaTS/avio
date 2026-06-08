from __future__ import annotations

import json
import os
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional


@dataclass(frozen=True)
class TenantRuntimeDeps:
    settings: Any
    logger: Any
    yaml_module: Any
    root_dir: pathlib.Path
    data_dir: pathlib.Path
    tenants_dir: pathlib.Path
    tenant_config_dir: pathlib.Path
    default_tenant_json: Dict[str, Any]
    default_persona_md: str
    persona_md_fallback: str
    tenant_config_cache: MutableMapping[int, tuple[float, float, dict]]
    tenant_persona_cache: MutableMapping[tuple[int, str], tuple[float, str]]
    persona_hints_cache: MutableMapping[Any, Any]
    clear_persona_hints_cache: Callable[[int], None]
    coerce_bool: Callable[[Any, bool], bool]
    tenant_config_db_get: Callable[[int], tuple[dict[str, Any], float] | None] | None = None
    tenant_config_db_upsert: Callable[[int, Mapping[str, Any]], bool] | None = None


class TenantRuntime:
    def __init__(self, deps: TenantRuntimeDeps) -> None:
        self.deps = deps

    def ensure_passport_public_key(self, cfg: dict[str, Any] | None) -> bool:
        if not isinstance(cfg, dict):
            return False

        public_key = str(getattr(self.deps.settings, "PUBLIC_KEY", "") or "").strip()
        if not public_key:
            return False

        passport = cfg.get("passport")
        mutated = False
        if not isinstance(passport, dict):
            passport = {}
            cfg["passport"] = passport
            mutated = True

        current_raw = passport.get("public_key")
        current_value = str(current_raw).strip() if current_raw else ""
        if current_value:
            if isinstance(current_raw, str) and current_raw == current_value:
                return mutated
            passport["public_key"] = current_value
            return True

        passport["public_key"] = public_key
        return True

    def _write_json_atomic(self, path: pathlib.Path, cfg: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as fh:
                tmp_name = fh.name
                json.dump(cfg, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
            tmp_name = ""
            try:
                dir_fd = os.open(path.parent, os.O_RDONLY)
            except Exception:
                dir_fd = None
            if dir_fd is not None:
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        finally:
            if tmp_name:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass

    def tenant_dir(self, tenant: int) -> pathlib.Path:
        tenant_key = int(tenant)
        env_value = str(os.getenv("TENANTS_DIR") or "").strip()
        if env_value:
            return pathlib.Path(env_value) / str(tenant_key)

        primary = self.deps.tenants_dir / str(tenant_key)
        if primary.exists():
            return primary

        repo_fallback = self.deps.root_dir.parent / "data" / "tenants" / str(tenant_key)
        if repo_fallback.exists():
            return repo_fallback

        data_fallback = self.deps.data_dir / "tenants" / str(tenant_key)
        if data_fallback.exists():
            return data_fallback

        return primary

    def ensure_tenant_files(self, tenant: int) -> pathlib.Path:
        td = self.tenant_dir(tenant)
        td.mkdir(parents=True, exist_ok=True)
        tenant_json = td / "tenant.json"
        persona_md = td / "persona.md"

        if not tenant_json.exists() or tenant_json.stat().st_size == 0:
            cfg = json.loads(json.dumps(self.deps.default_tenant_json, ensure_ascii=False))
            cfg.setdefault("passport", {})["tenant_id"] = int(tenant)
            self.ensure_passport_public_key(cfg)
            with open(tenant_json, "w", encoding="utf-8") as fh:
                json.dump(cfg, fh, ensure_ascii=False, indent=2)
        else:
            try:
                with open(tenant_json, "r", encoding="utf-8") as fh:
                    existing_cfg = json.load(fh)
            except Exception:
                existing_cfg = {}

            if not isinstance(existing_cfg, dict):
                existing_cfg = {}
            channels = existing_cfg.get("channels") if isinstance(existing_cfg, dict) else None
            mutated = False
            if not isinstance(channels, dict):
                channels = {"whatsapp": {"enabled": True}}
                existing_cfg["channels"] = channels
                mutated = True
            else:
                whatsapp_cfg = channels.get("whatsapp")
                if not isinstance(whatsapp_cfg, dict):
                    channels["whatsapp"] = {"enabled": True}
                    mutated = True
                elif "enabled" not in whatsapp_cfg:
                    whatsapp_cfg["enabled"] = True
                    mutated = True

            if self.ensure_passport_public_key(existing_cfg):
                mutated = True

            if mutated:
                try:
                    self._write_json_atomic(tenant_json, existing_cfg)
                except Exception:
                    pass

        if not persona_md.exists() or persona_md.stat().st_size == 0:
            with open(persona_md, "w", encoding="utf-8") as fh:
                fh.write(self.deps.default_persona_md)

        return td

    def merge_dicts(self, base: Mapping[str, Any] | dict, overlay: Mapping[str, Any] | dict) -> dict:
        result = dict(base or {})
        for key, value in dict(overlay or {}).items():
            base_value = result.get(key)
            if isinstance(base_value, dict) and isinstance(value, dict):
                result[key] = self.merge_dicts(base_value, value)
            else:
                result[key] = value
        return result

    def load_external_tenant_config(self, tenant: int) -> tuple[float, dict]:
        directory = self.deps.tenant_config_dir
        if not directory.exists():
            return 0.0, {}
        tenant_str = str(int(tenant))
        candidates = (
            directory / f"{tenant_str}.yaml",
            directory / f"{tenant_str}.yml",
            directory / f"{tenant_str}.json",
        )
        for path in candidates:
            if not path.exists():
                continue
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
            try:
                if path.suffix.lower() == ".json":
                    with open(path, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                else:
                    with open(path, "r", encoding="utf-8") as fh:
                        data = self.deps.yaml_module.safe_load(fh)
            except Exception:
                self.deps.logger.warning(
                    "failed to load tenant override path=%s",
                    path,
                    exc_info=True,
                )
                return mtime, {}
            if isinstance(data, dict):
                return mtime, data
            self.deps.logger.warning("tenant override not a mapping path=%s", path)
            return mtime, {}
        return 0.0, {}

    def normalize_tenant_config(self, cfg: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(cfg or {})
        behavior_raw = normalized.get("behavior")
        behavior: dict[str, Any] = {}
        if isinstance(behavior_raw, dict):
            behavior.update(behavior_raw)

        auto_flag = behavior.get("auto_reply")
        if auto_flag is None:
            auto_flag = behavior.get("auto_reply_enabled")
        behavior["auto_reply"] = bool(auto_flag)
        behavior["auto_reply_enabled"] = behavior["auto_reply"]

        explain_flag = self.deps.coerce_bool(behavior.get("explain"), False)
        behavior["explain"] = explain_flag

        text_raw = behavior.get("auto_reply_text")
        if isinstance(text_raw, str):
            text_value = text_raw
        elif text_raw is None:
            text_value = ""
        else:
            text_value = str(text_raw)
        behavior["auto_reply_text"] = text_value

        triggers_raw = behavior.get("triggers")
        triggers: list[dict[str, Any]] = []
        if isinstance(triggers_raw, list):
            for item in triggers_raw:
                if not isinstance(item, Mapping):
                    continue
                phrases_raw = item.get("phrases") or item.get("keywords") or []
                phrases: list[str] = []
                if isinstance(phrases_raw, (list, tuple, set)):
                    for phrase in phrases_raw:
                        if isinstance(phrase, str) and phrase.strip():
                            phrases.append(phrase.strip())
                elif isinstance(phrases_raw, str) and phrases_raw.strip():
                    for phrase in phrases_raw.split(","):
                        if phrase.strip():
                            phrases.append(phrase.strip())
                if not phrases:
                    continue

                channels_raw = item.get("channels") or [
                    "telegram",
                    "avito",
                    "whatsapp",
                    "max",
                    "max_personal",
                ]
                channels: list[str] = []
                if isinstance(channels_raw, (list, tuple, set)):
                    for channel in channels_raw:
                        if isinstance(channel, str) and channel.strip():
                            channels.append(channel.strip().lower())
                elif isinstance(channels_raw, str) and channels_raw.strip():
                    channels.append(channels_raw.strip().lower())
                if not channels:
                    channels = ["telegram", "avito", "whatsapp", "max", "max_personal"]

                silence_flag = self.deps.coerce_bool(item.get("silence"), True)
                notify_flag = self.deps.coerce_bool(item.get("notify"), False)
                triggers.append(
                    {
                        "phrases": phrases,
                        "channels": channels,
                        "silence": silence_flag,
                        "notify": notify_flag,
                    }
                )
        behavior["triggers"] = triggers

        photo_markers_raw = (
            behavior.get("photo_expected_markers") or behavior.get("photo_markers") or []
        )
        photo_markers: list[str] = []
        if isinstance(photo_markers_raw, (list, tuple, set)):
            for phrase in photo_markers_raw:
                if isinstance(phrase, str) and phrase.strip():
                    photo_markers.append(phrase.strip())
        elif isinstance(photo_markers_raw, str) and photo_markers_raw.strip():
            for phrase in photo_markers_raw.split(","):
                if phrase.strip():
                    photo_markers.append(phrase.strip())
        behavior["photo_expected_markers"] = photo_markers
        photo_reply_raw = behavior.get("photo_expected_reply") or behavior.get("photo_reply") or ""
        behavior["photo_expected_reply"] = (
            photo_reply_raw if isinstance(photo_reply_raw, str) else str(photo_reply_raw or "")
        )
        try:
            ttl_value = int(behavior.get("photo_expected_ttl") or 0)
        except Exception:
            ttl_value = 0
        behavior["photo_expected_ttl"] = ttl_value if ttl_value > 0 else 0

        send_catalog_flag = behavior.get("send_catalog_on_first_message")
        if send_catalog_flag is None:
            behavior["send_catalog_on_first_message"] = True
        else:
            behavior["send_catalog_on_first_message"] = self.deps.coerce_bool(send_catalog_flag, True)

        avito_ai_flag = behavior.get("avito_smart_reply_enabled")
        behavior["avito_smart_reply_enabled"] = self.deps.coerce_bool(avito_ai_flag, False)

        whatsapp_cfg = normalized.get("whatsapp")
        whatsapp: dict[str, Any] = {}
        if isinstance(whatsapp_cfg, dict):
            whatsapp.update(whatsapp_cfg)
        provider_value = str(whatsapp.get("provider") or "").strip().lower()
        default_provider = getattr(self.deps.settings, "WHATSAPP_PROVIDER_DEFAULT", "waweb")
        if provider_value not in {"waweb", "baileys"}:
            provider_value = default_provider
        whatsapp["provider"] = provider_value
        normalized["whatsapp"] = whatsapp

        notifications_raw = normalized.get("notifications")
        if isinstance(notifications_raw, dict):
            notifications = dict(notifications_raw)
        else:
            notifications = {}
        normalized["notifications"] = notifications

        normalized["behavior"] = behavior
        return normalized

    def read_tenant_config(self, tenant: int) -> dict:
        self.ensure_tenant_files(tenant)
        path = self.tenant_dir(tenant) / "tenant.json"
        db_payload = None
        if self.deps.tenant_config_db_get is not None:
            try:
                db_payload = self.deps.tenant_config_db_get(int(tenant))
            except Exception:
                db_payload = None
        try:
            primary_mtime = path.stat().st_mtime
        except Exception:
            primary_mtime = 0.0
        if db_payload is not None:
            primary_mtime = -abs(float(db_payload[1] or 1.0))

        overlay_mtime, overlay_cfg = self.load_external_tenant_config(tenant)
        cached = self.deps.tenant_config_cache.get(int(tenant))
        if cached and cached[0] == primary_mtime and cached[1] == overlay_mtime:
            return cached[2]

        if db_payload is not None:
            data = dict(db_payload[0] or {})
        elif path.exists():
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        else:
            data = {}

        merged = self.merge_dicts(data, overlay_cfg)
        normalized = self.normalize_tenant_config(merged)
        if db_payload is None and data and self.deps.tenant_config_db_upsert is not None:
            try:
                self.deps.tenant_config_db_upsert(int(tenant), data)
            except Exception:
                pass
        self.deps.tenant_config_cache[int(tenant)] = (primary_mtime, overlay_mtime, normalized)
        return normalized

    def write_tenant_config(self, tenant: int, cfg: dict) -> None:
        self.ensure_tenant_files(tenant)
        path = self.tenant_dir(tenant) / "tenant.json"
        self.ensure_passport_public_key(cfg)
        db_written = False
        if self.deps.tenant_config_db_upsert is not None:
            try:
                db_written = bool(self.deps.tenant_config_db_upsert(int(tenant), cfg))
            except Exception:
                db_written = False
        try:
            self._write_json_atomic(path, cfg)
        except Exception:
            if not db_written:
                raise
            self.deps.logger.warning(
                "tenant_config_file_cache_write_failed tenant=%s path=%s",
                tenant,
                path,
                exc_info=True,
            )
        try:
            mtime = path.stat().st_mtime
        except Exception:
            mtime = 0.0
        if db_written:
            db_payload = None
            if self.deps.tenant_config_db_get is not None:
                try:
                    db_payload = self.deps.tenant_config_db_get(int(tenant))
                except Exception:
                    db_payload = None
            if db_payload is not None:
                mtime = -abs(float(db_payload[1] or 1.0))
        overlay_mtime, overlay_cfg = self.load_external_tenant_config(tenant)
        merged = self.merge_dicts(cfg, overlay_cfg)
        normalized = self.normalize_tenant_config(merged)
        try:
            self.deps.tenant_config_cache[int(tenant)] = (mtime, overlay_mtime, normalized)
        except Exception:
            self.deps.tenant_config_cache.pop(int(tenant), None)

    def persist_pdf_index_metadata(
        self,
        tenant: int,
        source_key: str,
        rel_index_path: str,
        index_meta: Dict[str, Any],
    ) -> None:
        try:
            cfg = self.read_tenant_config(tenant)
        except Exception:
            return

        catalogs = cfg.get("catalogs")
        catalogs = catalogs if isinstance(catalogs, list) else []
        source_candidates = {source_key.strip()}
        source_candidates.add(index_meta.get("source_path", ""))
        resolved: set[str] = set()
        for token in list(source_candidates):
            if not token:
                continue
            resolved.add(token)
            try:
                abs_path = str((self.tenant_dir(tenant) / token).resolve())
                resolved.add(abs_path)
            except Exception:
                pass
        resolved = {value for value in resolved if value}

        for entry in catalogs:
            entry_path = str(entry.get("path") or "").strip()
            if not entry_path:
                continue
            candidate_set = {entry_path}
            try:
                candidate_set.add(str((self.tenant_dir(tenant) / entry_path).resolve()))
            except Exception:
                pass
            if candidate_set & resolved:
                entry["index_path"] = rel_index_path
                entry["indexed_at"] = index_meta.get("generated_at")
                entry["chunk_count"] = index_meta.get("chunk_count")
                entry["sha1"] = index_meta.get("sha1")
                break

        integrations = cfg.setdefault("integrations", {})
        uploaded = integrations.get("uploaded_catalog")
        if isinstance(uploaded, dict) and (uploaded.get("path") in resolved):
            uploaded["index"] = {
                "path": rel_index_path,
                "generated_at": index_meta.get("generated_at"),
                "chunks": index_meta.get("chunk_count"),
                "pages": index_meta.get("page_count"),
                "sha1": index_meta.get("sha1"),
            }

        try:
            self.write_tenant_config(tenant, cfg)
        except Exception:
            pass

    @staticmethod
    def persona_cache_key(tenant: int, channel: str | None) -> tuple[int, str]:
        return int(tenant), (channel or "").strip().lower()

    @staticmethod
    def persona_fallback_channels(channel: str | None) -> tuple[str, ...]:
        channel_name = (channel or "").strip().lower()
        if channel_name == "max_personal":
            return ("telegram", "")
        if channel_name:
            return ("",)
        return ()

    def persona_path(self, tenant: int, channel: str | None) -> pathlib.Path:
        base = self.tenant_dir(tenant)
        channel_name = (channel or "").strip().lower()
        if channel_name:
            return base / f"persona_{channel_name}.md"
        return base / "persona.md"

    def _clear_persona_hints(self, tenant: int) -> None:
        try:
            self.deps.clear_persona_hints_cache(int(tenant))
        except Exception:
            self.deps.persona_hints_cache.clear()

    def read_persona(self, tenant: int, channel: str | None = None) -> str:
        self.ensure_tenant_files(tenant)
        path = self.persona_path(tenant, channel)
        if channel and not path.exists():
            for fallback_channel in self.persona_fallback_channels(channel):
                fallback_path = self.persona_path(tenant, fallback_channel or None)
                if fallback_path.exists():
                    path = fallback_path
                    break
        try:
            mtime = path.stat().st_mtime
            cached = self.deps.tenant_persona_cache.get(self.persona_cache_key(int(tenant), channel))
            if cached and cached[0] == mtime:
                return cached[1]
        except Exception:
            mtime = 0.0
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        try:
            self.deps.tenant_persona_cache[self.persona_cache_key(int(tenant), channel)] = (mtime, text)
        except Exception:
            pass
        self._clear_persona_hints(int(tenant))
        return text

    def write_persona(self, tenant: int, text: str, channel: str | None = None) -> None:
        self.ensure_tenant_files(tenant)
        path = self.persona_path(tenant, channel)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text or "")
        try:
            mtime = path.stat().st_mtime
            self.deps.tenant_persona_cache[self.persona_cache_key(int(tenant), channel)] = (
                mtime,
                text or "",
            )
        except Exception:
            self.deps.tenant_persona_cache.pop(self.persona_cache_key(int(tenant), channel), None)
        self._clear_persona_hints(int(tenant))

    def load_tenant(self, tenant: int) -> dict:
        try:
            return self.read_tenant_config(tenant)
        except Exception:
            cfg = json.loads(json.dumps(self.deps.default_tenant_json, ensure_ascii=False))
            cfg.setdefault("passport", {})["tenant_id"] = int(tenant)
            return cfg

    def branding_for_tenant(
        self,
        tenant: int | None = None,
        channel: str | None = None,
    ) -> Dict[str, str]:
        passport: Dict[str, Any] = {}
        integrations: Dict[str, Any] = {}
        if tenant is not None:
            try:
                cfg = self.read_tenant_config(tenant)
            except Exception:
                cfg = {}
        else:
            cfg = {}

        if isinstance(cfg, dict):
            raw_passport = cfg.get("passport")
            if isinstance(raw_passport, dict):
                passport = raw_passport
            raw_integrations = cfg.get("integrations")
            if isinstance(raw_integrations, dict):
                integrations = raw_integrations

        agent_name = str(passport.get("agent_name") or "").strip()
        brand = str(passport.get("brand") or "").strip()
        city = str(passport.get("city") or "").strip()
        whatsapp_link = str(
            passport.get("whatsapp_link") or integrations.get("whatsapp_link") or ""
        ).strip()
        catalog_url = str(
            integrations.get("catalog_url")
            or integrations.get("pdf_catalog_url")
            or passport.get("catalog_url")
            or ""
        ).strip()

        if tenant is None:
            agent_name = agent_name or getattr(self.deps.settings, "AGENT_NAME", "")
            brand = brand or getattr(self.deps.settings, "BRAND_NAME", "")
            city = city or getattr(self.deps.settings, "CITY", "")
            whatsapp_link = whatsapp_link or getattr(self.deps.settings, "WHATSAPP_LINK", "")

        currency = str(passport.get("currency") or "₽").strip() or "₽"
        resolved_channel = str(channel or passport.get("channel") or "").strip()
        if not resolved_channel:
            resolved_channel = "WhatsApp"

        return {
            "AGENT_NAME": agent_name,
            "BRAND": brand,
            "BRAND_NAME": brand,
            "WHATSAPP_LINK": whatsapp_link,
            "CATALOG_URL": catalog_url,
            "CITY": city,
            "CHANNEL": resolved_channel,
            "CURRENCY": currency,
        }

    def load_persona(self, tenant: int | None = None, channel: str | None = None) -> str:
        if tenant is not None:
            try:
                persona = self.read_persona(tenant, channel)
                if not persona.strip():
                    persona = self.deps.default_persona_md
            except Exception:
                persona = self.deps.default_persona_md
        else:
            try:
                with open(self.deps.settings.PERSONA_MD, "r", encoding="utf-8") as fh:
                    persona = fh.read()
            except Exception:
                persona = self.deps.persona_md_fallback

        tokens = self.branding_for_tenant(tenant, channel)
        for key, value in tokens.items():
            persona = persona.replace(f"{{{key}}}", value or "")
        return persona

    def load_persona_structured(self, tenant: int | None = None) -> Dict[str, Any]:
        text = self.load_persona(tenant)
        if not text.strip():
            return {}
        try:
            parsed = self.deps.yaml_module.safe_load(text)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def persona_meta_config(self, tenant: int | None = None) -> Dict[str, Any]:
        structured = self.load_persona_structured(tenant)
        meta = structured.get("meta") if isinstance(structured, dict) else {}
        if isinstance(meta, dict) and meta:
            return meta

        try:
            raw = self.load_persona(tenant)
            lines = raw.splitlines()
            meta_lines: list[str] = []
            in_meta = False
            for line in lines:
                if line.strip().startswith("meta:"):
                    in_meta = True
                if in_meta:
                    if line and not line.startswith(" "):
                        if not line.strip().startswith("meta:"):
                            break
                    meta_lines.append(line)
            if meta_lines:
                parsed = self.deps.yaml_module.safe_load("\n".join(meta_lines))
                if isinstance(parsed, dict):
                    block = parsed.get("meta") if "meta" in parsed else parsed
                    if isinstance(block, dict):
                        return block
        except Exception:
            pass
        return {}

    def resolve_persona_relative_path(self, tenant: int, raw_path: str) -> Optional[pathlib.Path]:
        candidate = (raw_path or "").strip()
        if not candidate:
            return None
        candidate = candidate.replace("\\", "/")
        candidate = candidate.lstrip("/")
        if ".." in candidate.split("/"):
            return None
        tenant_root = self.tenant_dir(tenant)
        target = tenant_root / candidate
        if target.exists() and target.is_file():
            return target
        return None

    def persona_catalog_pdf(self, tenant: int) -> Optional[Dict[str, Any]]:
        meta = self.persona_meta_config(tenant)
        raw_path = meta.get("catalog_pdf_path") if isinstance(meta, dict) else None
        if not isinstance(raw_path, str):
            return None
        target = self.resolve_persona_relative_path(tenant, raw_path)
        if not target:
            return None
        return {
            "type": "pdf",
            "path": str(target.relative_to(self.tenant_dir(tenant))),
            "original": target.name,
            "mime": "application/pdf",
        }

    def persona_catalog_csv(self, tenant: int) -> Optional[pathlib.Path]:
        meta = self.persona_meta_config(tenant)
        raw_path = meta.get("catalog_csv_path") if isinstance(meta, dict) else None
        if not isinstance(raw_path, str):
            return None
        return self.resolve_persona_relative_path(tenant, raw_path)

    def normalize_catalog_pdf_candidate(
        self,
        tenant: int,
        candidate: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:
        path_value = candidate.get("path") or candidate.get("relative_path")
        if not isinstance(path_value, str):
            return None
        cleaned = path_value.replace("\\", "/").strip()
        if not cleaned:
            return None
        try:
            safe = pathlib.PurePosixPath(cleaned)
        except Exception:
            return None
        if safe.is_absolute() or ".." in safe.parts:
            return None
        tenant_root = self.tenant_dir(tenant)
        target = tenant_root / str(safe)
        if not target.exists() or not target.is_file():
            return None
        try:
            stat = target.stat()
        except OSError:
            return None

        type_hint = str(candidate.get("type") or candidate.get("kind") or "").strip().lower()
        mime_hint = (
            str(candidate.get("mime") or candidate.get("mime_type") or candidate.get("mimetype") or "")
            .strip()
            .lower()
        )
        extension = safe.suffix.lower()
        if type_hint and type_hint not in {"pdf", "document"}:
            return None
        if not type_hint and extension not in {".pdf", ".pdfx"} and "pdf" not in mime_hint:
            return None

        filename = str(candidate.get("original") or candidate.get("filename") or safe.name)
        mime = (
            str(
                candidate.get("mime")
                or candidate.get("mime_type")
                or candidate.get("mimetype")
                or "application/pdf"
            ).strip()
            or "application/pdf"
        )
        return {
            "relative_path": str(safe),
            "absolute_path": str(target),
            "filename": filename,
            "mime": mime,
            "size": stat.st_size,
            "updated_at": int(stat.st_mtime),
        }

    def resolve_catalog_pdf_meta(
        self,
        tenant: int,
        cfg: Optional[dict] = None,
    ) -> Optional[Dict[str, Any]]:
        if cfg is None:
            try:
                cfg = self.load_tenant(tenant)
            except Exception:
                cfg = {}

        candidates: list[Mapping[str, Any]] = []
        if isinstance(cfg, dict):
            integrations = cfg.get("integrations")
            if isinstance(integrations, Mapping):
                uploaded = integrations.get("uploaded_catalog")
                if isinstance(uploaded, Mapping):
                    candidates.append(uploaded)
                for alt_key in ("uploaded_catalog_pdf", "catalog_pdf", "pdf_catalog"):
                    alt_meta = integrations.get(alt_key)
                    if isinstance(alt_meta, Mapping):
                        candidates.append(alt_meta)
            raw_catalogs = cfg.get("catalogs")
            if isinstance(raw_catalogs, list):
                for entry in raw_catalogs:
                    if not isinstance(entry, Mapping):
                        continue
                    entry_type = str(entry.get("type") or "").strip().lower()
                    if entry_type == "pdf":
                        candidates.append(entry)

        for candidate in candidates:
            normalized = self.normalize_catalog_pdf_candidate(tenant, candidate)
            if normalized:
                return normalized

        persona_meta = self.persona_catalog_pdf(tenant)
        if persona_meta:
            normalized = self.normalize_catalog_pdf_candidate(tenant, persona_meta)
            if normalized:
                return normalized

        default_path = self.tenant_dir(tenant) / "uploads" / "catalog.pdf"
        if default_path.exists() and default_path.is_file():
            try:
                stat = default_path.stat()
            except OSError:
                stat = None
            if stat:
                return {
                    "relative_path": "uploads/catalog.pdf",
                    "absolute_path": str(default_path),
                    "filename": default_path.name,
                    "mime": "application/pdf",
                    "size": stat.st_size,
                    "updated_at": int(stat.st_mtime),
                }
        return None
