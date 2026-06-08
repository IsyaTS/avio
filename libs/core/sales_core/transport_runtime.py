from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class TransportRuntimeDeps:
    settings: Any


class TransportRuntime:
    def __init__(self, deps: TransportRuntimeDeps) -> None:
        self.deps = deps

    def tenant_waweb_url(
        self,
        tenant: int | None,
        *,
        tenant_config_fn: Callable[[int], dict],
    ) -> str:
        """
        Return waweb base URL for a tenant. Falls back to a generated host name or default settings.
        """
        if tenant is None:
            return self.deps.settings.WA_WEB_URL
        try:
            tenant_key = int(tenant)
        except Exception:
            return self.deps.settings.WA_WEB_URL

        cfg = tenant_config_fn(tenant_key)
        waweb_cfg = cfg.get("waweb") if isinstance(cfg.get("waweb"), dict) else {}

        url_value = ""
        if waweb_cfg:
            url_value = str(waweb_cfg.get("url") or "").strip()
            if not url_value:
                host_value = str(waweb_cfg.get("host") or "").strip()
                port_value = waweb_cfg.get("port")
                if host_value:
                    if port_value:
                        try:
                            port_int = int(str(port_value).strip())
                        except Exception:
                            port_int = None
                        if port_int:
                            url_value = f"http://{host_value}:{port_int}"
                    if not url_value:
                        url_value = f"http://{host_value}"

        if url_value:
            return url_value.rstrip("/")

        default_host = f"waweb-{tenant_key}"
        return f"http://{default_host}:9001"

    def tenant_whatsapp_provider(
        self,
        tenant: int | None,
        *,
        tenant_config_fn: Callable[[int], dict],
        read_tenant_config_fn: Callable[[int], dict],
    ) -> str:
        """
        Resolve the WhatsApp transport provider for a tenant ("waweb" or "baileys").
        """
        default_provider = getattr(self.deps.settings, "WHATSAPP_PROVIDER_DEFAULT", "waweb")
        if tenant is None:
            return default_provider
        try:
            tenant_key = int(tenant)
        except Exception:
            return default_provider

        cfg = tenant_config_fn(tenant_key)
        whatsapp_cfg = cfg.get("whatsapp") if isinstance(cfg, Mapping) else {}
        if not isinstance(whatsapp_cfg, Mapping):
            whatsapp_cfg = {}
        provider = str(whatsapp_cfg.get("provider") or "").strip().lower()
        if provider in {"waweb", "baileys"}:
            return provider

        try:
            cfg = read_tenant_config_fn(tenant_key)
        except Exception:
            cfg = {}
        whatsapp_cfg = cfg.get("whatsapp") if isinstance(cfg, Mapping) else {}
        if not isinstance(whatsapp_cfg, Mapping):
            whatsapp_cfg = {}
        provider = str(whatsapp_cfg.get("provider") or "").strip().lower()
        if provider in {"waweb", "baileys"}:
            return provider

        return default_provider
