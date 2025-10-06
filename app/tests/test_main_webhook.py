import asyncio
import json
import pathlib
import sys
import time
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import core


def _install_fastapi_stub() -> None:
    if "fastapi" in sys.modules:
        return

    class FakeRouter:
        def __init__(self, *_, **__):
            pass

        def post(self, *_a, **_k):
            def decorator(func):
                return func

            return decorator

        def get(self, *_a, **_k):
            def decorator(func):
                return func

            return decorator

        def include_router(self, *_a, **_k):
            pass

    class FakeFastAPI(FakeRouter):
        def mount(self, *_a, **_k):
            pass

    class FakeHTTPException(Exception):
        def __init__(self, status_code: int = 400, detail: str | None = None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi_mod = types.ModuleType("fastapi")
    fastapi_mod.FastAPI = FakeFastAPI
    fastapi_mod.APIRouter = FakeRouter
    fastapi_mod.Request = object
    fastapi_mod.HTTPException = FakeHTTPException
    sys.modules["fastapi"] = fastapi_mod

    responses_mod = types.ModuleType("fastapi.responses")

    class FakeJSONResponse:
        def __init__(self, data: dict, status_code: int = 200):
            self.body = data
            self.status_code = status_code

    class FakeRedirectResponse(FakeJSONResponse):
        def __init__(self, url: str, status_code: int = 303):
            super().__init__({"url": url}, status_code)
            self.headers = {"location": url}

    class FakeFileResponse(FakeJSONResponse):
        def __init__(self, path: pathlib.Path | str, media_type: str | None = None, filename: str | None = None):
            super().__init__({"path": str(path), "filename": filename, "media_type": media_type})

    responses_mod.JSONResponse = FakeJSONResponse
    responses_mod.RedirectResponse = FakeRedirectResponse
    responses_mod.FileResponse = FakeFileResponse
    sys.modules["fastapi.responses"] = responses_mod

    staticfiles_mod = types.ModuleType("fastapi.staticfiles")

    class FakeStaticFiles:
        def __init__(self, *_, **__):
            pass

    staticfiles_mod.StaticFiles = FakeStaticFiles
    sys.modules["fastapi.staticfiles"] = staticfiles_mod


def _install_web_stubs() -> None:
    if "web" in sys.modules:
        return

    try:
        import importlib

        importlib.import_module("web.common")
        importlib.import_module("web.admin")
        importlib.import_module("web.public")
        importlib.import_module("web.client")
        return
    except Exception:
        pass

    web_pkg = types.ModuleType("web")
    web_pkg.__path__ = []  # mark as package so importlib.reload works
    sys.modules["web"] = web_pkg

    class FakeRouter:
        def __init__(self):
            self.routes = []
            self.on_startup = []
            self.on_shutdown = []
            self.dependencies = []
            self.lifespan_context = None

        def _register(self, func):
            self.routes.append(func)
            return func

        def get(self, *_a, **_k):
            return self._register

        def post(self, *_a, **_k):
            return self._register

        def include_router(self, *_a, **_k):
            return None

    fake_router = FakeRouter()

    common_mod = types.ModuleType("web.common")
    common_mod.ensure_tenant_files = core.ensure_tenant_files
    common_mod.tenant_dir = core.tenant_dir
    common_mod.read_tenant_config = core.read_tenant_config
    common_mod.write_tenant_config = core.write_tenant_config
    common_mod.read_persona = core.read_persona
    common_mod.write_persona = core.write_persona
    sys.modules["web.common"] = common_mod
    setattr(web_pkg, "common", common_mod)

    for name in ("admin", "public", "client"):
        mod = types.ModuleType(f"web.{name}")
        mod.router = FakeRouter()
        sys.modules[f"web.{name}"] = mod
        setattr(web_pkg, name, mod)


_install_fastapi_stub()
_install_web_stubs()


def _install_db_stub() -> None:
    if "db" in sys.modules:
        return

    db_mod = types.ModuleType("db")

    async def _resolve_or_create_contact(**_kw):
        return 0

    async def _link_lead_contact(*_a, **_k):
        return None

    async def _insert_message_in(*_a, **_k):
        return None

    async def _init_db():
        return None

    async def _upsert_lead(*_a, **_k):
        return None

    async def _insert_message_out(*_a, **_k):
        return None

    db_mod.resolve_or_create_contact = _resolve_or_create_contact
    db_mod.link_lead_contact = _link_lead_contact
    db_mod.insert_message_in = _insert_message_in
    db_mod.init_db = _init_db
    db_mod.upsert_lead = _upsert_lead
    db_mod.insert_message_out = _insert_message_out

    sys.modules["db"] = db_mod


_install_db_stub()

import main


class _QueryParams:
    def __init__(self, data: dict):
        self._data = data

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._data.get(key, default)


class DummyRequest:
    def __init__(self, body: dict, base_url: str = "http://app:8000"):
        self._body = body
        self._base_url = base_url.rstrip("/")
        self.query_params = _QueryParams({})

    async def json(self) -> dict:
        return self._body

    def url_for(self, name: str, **params: str) -> str:
        if name != "internal_catalog_file":
            raise KeyError(name)
        tenant = params.get("tenant")
        return f"{self._base_url}/internal/tenant/{tenant}/catalog-file"


def test_webhook_returns_pdf_attachment(monkeypatch, tmp_path):
    tenant = 9
    core.ensure_tenant_files(tenant)
    core.reset_sales_state(tenant, 123)
    main._catalog_sent_cache.clear()

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)

    pdf_rel = "uploads/catalog.pdf"
    pdf_path = uploads / "catalog.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 test")

    cfg = core.read_tenant_config(tenant)
    cfg.setdefault("passport", {})["tenant_id"] = tenant
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": pdf_rel,
            "type": "pdf",
            "encoding": "utf-8",
        }
    ]
    cfg.setdefault("integrations", {})["uploaded_catalog"] = {
        "path": pdf_rel,
        "original": "catalog.pdf",
        "uploaded_at": int(time.time()),
        "type": "pdf",
        "size": pdf_path.stat().st_size,
        "mime": "application/pdf",
    }
    core.write_tenant_config(tenant, cfg)

    pushed: list[tuple[str, dict]] = []

    class FakeQueue:
        async def lpush(self, key: str, value: str) -> None:
            pushed.append((key, json.loads(value)))

    async def fail(*_a, **_k):  # should not be called for PDF flow
        raise AssertionError("LLM should be skipped when PDF catalog is available")

    monkeypatch.setattr(main, "_r", FakeQueue())
    monkeypatch.setattr(main, "build_llm_messages", fail)
    monkeypatch.setattr(main, "ask_llm", fail)
    monkeypatch.setattr(main.settings, "WEBHOOK_SECRET", "", raising=False)

    payload = {
        "source": {"type": "whatsapp", "tenant": tenant},
        "message": {"from": "79001234567@c.us", "body": "нужен каталог"},
        "leadId": 123,
    }

    request = DummyRequest(payload)
    response = asyncio.run(main._handle(request))

    assert response.status_code == 200
    assert pushed, "expected message queued in Redis"
    assert len(pushed) == 1

    key, queued = pushed[0]
    assert key == "outbox:send"
    assert "attachment" in queued
    assert queued["attachment"]["filename"] == "catalog.pdf"
    assert queued["attachment"]["mime_type"] == "application/pdf"
    assert queued["text"].startswith("Каталог")


def test_webhook_skips_pdf_after_first_send(monkeypatch, tmp_path):
    tenant = 11
    lead_id = 987
    core.ensure_tenant_files(tenant)
    core.reset_sales_state(tenant, lead_id)
    main._catalog_sent_cache.clear()

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)

    pdf_rel = "uploads/catalog.pdf"
    pdf_path = uploads / "catalog.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 test")

    cfg = core.read_tenant_config(tenant)
    cfg.setdefault("passport", {})["tenant_id"] = tenant
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": pdf_rel,
            "type": "pdf",
            "encoding": "utf-8",
        }
    ]
    cfg.setdefault("integrations", {})["uploaded_catalog"] = {
        "path": pdf_rel,
        "original": "catalog.pdf",
        "uploaded_at": int(time.time()),
        "type": "pdf",
        "size": pdf_path.stat().st_size,
        "mime": "application/pdf",
    }
    core.write_tenant_config(tenant, cfg)

    class FakeQueue:
        def __init__(self) -> None:
            self.pushed: list[tuple[str, dict]] = []

        async def lpush(self, key: str, value: str) -> None:
            self.pushed.append((key, json.loads(value)))

    calls = {"build": 0, "ask": 0}

    async def stub_build_llm_messages(*_a, **_k):
        calls["build"] += 1
        return [{"role": "system", "content": "stub"}]

    async def stub_ask_llm(*_a, **_k):
        calls["ask"] += 1
        return "Ответ по запросу"

    queue = FakeQueue()

    monkeypatch.setattr(main, "_r", queue)
    monkeypatch.setattr(main, "build_llm_messages", stub_build_llm_messages)
    monkeypatch.setattr(main, "ask_llm", stub_ask_llm)
    monkeypatch.setattr(main.settings, "WEBHOOK_SECRET", "", raising=False)

    payload = {
        "source": {"type": "whatsapp", "tenant": tenant},
        "message": {"from": "79001234567@c.us", "body": "привет"},
        "leadId": lead_id,
    }

    first = DummyRequest(payload)
    response_first = asyncio.run(main._handle(first))
    assert response_first.status_code == 200
    assert len(queue.pushed) == 1
    assert "attachment" in queue.pushed[0][1]
    assert calls == {"build": 0, "ask": 0}

    payload_second = {
        "source": {"type": "whatsapp", "tenant": tenant},
        "message": {"from": "79001234567@c.us", "body": "а теперь ответь"},
        "leadId": lead_id,
    }

    second = DummyRequest(payload_second)
    response_second = asyncio.run(main._handle(second))
    assert response_second.status_code == 200
    assert len(queue.pushed) == 2

    _, second_msg = queue.pushed[1]
    assert "attachment" not in second_msg
    assert second_msg["text"] == "Ответ по запросу"
    assert calls == {"build": 1, "ask": 1}


@pytest.mark.anyio
async def test_ask_llm_uses_planner(monkeypatch):
    import core
    from brain import planner as planner_mod

    class DummyChat:
        def __init__(self):
            self.completions = self

        def create(self, **_kwargs):
            raise AssertionError("fallback call not expected")

    class DummyOpenAI:
        api_key: str | None = None

        def __init__(self):
            self.chat = DummyChat()

    dummy_openai = DummyOpenAI()

    monkeypatch.setattr(core, "openai", dummy_openai, raising=False)
    monkeypatch.setattr(core.settings, "OPENAI_API_KEY", "test-key", raising=False)

    generated = planner_mod.GeneratedPlan(
        analysis="analysis",
        stage="stage",
        next_questions=["Какой бюджет?"],
        cta="Оставьте заявку",
        tone="tone",
        raw={},
    )

    async def fake_generate(messages, **_kwargs):
        return generated, "Ответ"

    monkeypatch.setattr(planner_mod, "generate_sales_reply", fake_generate)
    monkeypatch.setattr(core.planner, "generate_sales_reply", fake_generate)
    monkeypatch.setattr(core.quality, "enforce_plan_alignment", lambda reply, *_a, **_k: reply)

    saved_state: dict[str, dict] = {}
    state = core.SalesState(tenant=1, contact_id=2)

    def fake_load_state(tenant, contact):
        assert int(tenant or 0) == state.tenant
        assert int(contact or 0) == state.contact_id
        return state

    def fake_save_state(updated_state):
        saved_state["plan"] = dict(updated_state.last_plan)

    recorded_replies: list[tuple[int, int | None, str, str]] = []

    def fake_record_bot_reply(contact, tenant, channel, text):
        recorded_replies.append((contact, tenant, channel, text))

    monkeypatch.setattr(core, "load_persona_hints", lambda *_a, **_k: core.PersonaHints(language="ru"))
    monkeypatch.setattr(core, "load_sales_state", fake_load_state)
    monkeypatch.setattr(core, "save_sales_state", fake_save_state)
    monkeypatch.setattr(core, "record_bot_reply", fake_record_bot_reply)

    reply = await core.ask_llm(
        [
            {"role": "system", "content": "persona"},
            {"role": "user", "content": "Привет"},
        ],
        tenant=1,
        contact_id=2,
        channel="whatsapp",
    )

    assert reply.startswith("Ответ")
    assert saved_state["plan"]["cta"] == "Оставьте заявку"
    assert recorded_replies == [(2, 1, "whatsapp", reply)]
