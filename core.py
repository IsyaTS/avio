import importlib as _importlib
import sys as _sys

from config import settings as _config_settings

_reload = getattr(_importlib, "reload")
if not getattr(_reload, "_avio_wrapped", False):
    _ORIG_RELOAD = _reload

    def _avio_reload(module):
        name = getattr(getattr(module, "__spec__", None), "name", None) or getattr(module, "__name__", None)
        if name:
            _sys.modules[name] = module
        try:
            return _ORIG_RELOAD(module)
        except ImportError as exc:
            if name and getattr(exc, "name", None) == name:
                _sys.modules[name] = module
                return _ORIG_RELOAD(module)
            raise

    _avio_reload._avio_wrapped = True
    _importlib.reload = _avio_reload

_app_core_mod = _importlib.import_module("app.core")
_app_core_mod = _importlib.reload(_app_core_mod)

ADMIN_COOKIE = "admin_session"
settings = _config_settings
get_tenant_pubkey = _app_core_mod.get_tenant_pubkey
set_tenant_pubkey = _app_core_mod.set_tenant_pubkey

for _name, _value in _app_core_mod.__dict__.items():
    if _name in {
        "__name__",
        "__package__",
        "__loader__",
        "__spec__",
        "__file__",
        "__path__",
        "ADMIN_COOKIE",
        "settings",
    }:
        continue
    globals()[_name] = _value

__all__ = sorted(set(getattr(_app_core_mod, "__all__", [])) | {
    "ADMIN_COOKIE",
    "settings",
    "get_tenant_pubkey",
    "set_tenant_pubkey",
})

_sys.modules.setdefault("core", _sys.modules[__name__])

del (
    _app_core_mod,
    _name,
    _value,
    _config_settings,
)
