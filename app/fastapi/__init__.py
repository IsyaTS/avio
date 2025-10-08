"""Minimal FastAPI-compatible stubs for local testing."""

from __future__ import annotations

import asyncio
import inspect
import json as _json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl

from starlette.responses import FileResponse, JSONResponse, RedirectResponse, Response
from .staticfiles import StaticFiles


class HTTPException(Exception):
    """Exception carrying an HTTP status code."""

    def __init__(self, status_code: int = 400, detail: Any = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class BackgroundTasks:
    """Very small BackgroundTasks replacement used in tests."""

    def __init__(self) -> None:
        self._tasks: List[tuple[Callable[..., Any], tuple[Any, ...], Dict[str, Any]]] = []

    def add_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self._tasks.append((func, args, kwargs))

    async def run(self) -> None:
        for func, args, kwargs in list(self._tasks):
            try:
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    await result  # type: ignore[arg-type]
            except Exception:
                continue
        self._tasks.clear()


class UploadFile:
    """Simple in-memory UploadFile replacement."""

    def __init__(
        self,
        filename: str | None = None,
        content: bytes | None = None,
        content_type: str | None = None,
    ) -> None:
        self.filename = filename or ""
        self.content_type = content_type or "application/octet-stream"
        self._buffer = content or b""

    async def read(self) -> bytes:
        return self._buffer

    async def write(self, data: bytes) -> None:
        self._buffer += data

    async def seek(self, _pos: int) -> None:
        return None


class _HeaderMap(dict):
    def __init__(self, data: Dict[str, Any]):
        super().__init__()
        for key, value in data.items():
            self[key] = value

    def __setitem__(self, key: str, value: Any) -> None:  # type: ignore[override]
        super().__setitem__(str(key).lower(), str(value))

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return super().get(str(key).lower(), default)

    def __getitem__(self, key: str) -> Any:  # type: ignore[override]
        return super().__getitem__(str(key).lower())


class _QueryParams:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = {str(k): v for k, v in data.items()}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()


class _URL:
    def __init__(self, base: str, path: str, query: str = "") -> None:
        self._base = base.rstrip("/")
        self.path = path
        self.query = query

    @property
    def full(self) -> str:
        if self.query:
            return f"{self._base}{self.path}?{self.query}"
        return f"{self._base}{self.path}"


class Request:
    def __init__(
        self,
        *,
        app: "FastAPI",
        method: str,
        path: str,
        query_params: Dict[str, Any],
        headers: Dict[str, Any],
        cookies: Dict[str, Any],
        body: bytes,
        base_url: str,
    ) -> None:
        self.app = app
        self.method = method.upper()
        self.path = path
        self.base_url = base_url.rstrip('/')
        self.query_params = _QueryParams(query_params)
        self.headers = _HeaderMap(headers)
        self.cookies = {str(k): v for k, v in cookies.items()}
        self.path_params: Dict[str, Any] = {}
        self._body = body
        self.url = _URL(base_url, path, _encode_query(query_params))

    async def json(self) -> Any:
        import json

        if not self._body:
            return {}
        return json.loads(self._body.decode("utf-8"))

    async def body(self) -> bytes:
        return self._body

    def url_for(self, name: str, **params: Any) -> str:
        path = self.app.url_path_for(name, **params)
        return f"{self.url.full.split(self.url.path)[0]}{path}"


@dataclass
class _Route:
    path: str
    methods: List[str]
    endpoint: Callable[..., Any]
    name: str

    def match(self, target: str) -> Optional[Dict[str, str]]:
        template = [part for part in self.path.strip("/").split("/") if part]
        actual = [part for part in target.strip("/").split("/") if part]
        if len(template) != len(actual):
            return None
        params: Dict[str, str] = {}
        for expected, value in zip(template, actual):
            if expected.startswith("{") and expected.endswith("}"):
                key = expected[1:-1]
                params[key] = value
            elif expected != value:
                return None
        return params


def _encode_query(data: Dict[str, Any]) -> str:
    if not data:
        return ""
    from urllib.parse import urlencode

    flat = {k: v for k, v in data.items() if v is not None}
    return urlencode(flat, doseq=True)


class APIRouter:
    def __init__(self, *, prefix: str = "") -> None:
        self.prefix = prefix.rstrip("/")
        self.routes: List[_Route] = []

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        methods: Iterable[str],
        name: Optional[str] = None,
    ) -> None:
        final_path = _join_paths(self.prefix, path)
        route = _Route(path=final_path, methods=[m.upper() for m in methods], endpoint=endpoint, name=name or endpoint.__name__)
        self.routes.append(route)

    def _route(
        self,
        path: str,
        *,
        methods: Iterable[str],
        name: Optional[str],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.add_api_route(path, func, methods=methods, name=name)
            return func

        return decorator

    def get(self, path: str, *, name: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self._route(path, methods=["GET"], name=name)

    def post(self, path: str, *, name: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self._route(path, methods=["POST"], name=name)

    def include_router(self, router: "APIRouter", *, prefix: str = "") -> None:
        for route in router.routes:
            combined = _join_paths(prefix, route.path)
            self.routes.append(_Route(path=combined, methods=route.methods, endpoint=route.endpoint, name=route.name))


class FastAPI:
    def __init__(self, *, title: str | None = None) -> None:
        self.title = title or "FastAPI"
        self.router = APIRouter()
        self._middlewares: List[Callable[[Request, Callable[[Request], Awaitable[Any]]], Awaitable[Any]]] = []
        self._mounts: Dict[str, Tuple[str, Any]] = {}
        self._static_mounts: List[Tuple[str, StaticFiles]] = []

    def add_api_route(self, path: str, endpoint: Callable[..., Any], *, methods: Iterable[str], name: Optional[str] = None) -> None:
        self.router.add_api_route(path, endpoint, methods=methods, name=name)

    def get(self, path: str, *, name: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.router.get(path, name=name)

    def post(self, path: str, *, name: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.router.post(path, name=name)

    def include_router(self, router: APIRouter, *, prefix: str = "") -> None:
        self.router.include_router(router, prefix=prefix)

    def mount(self, path: str, app: Any, *, name: Optional[str] = None) -> None:  # pragma: no cover - unused in tests
        final = path if path.startswith('/') else f'/{path}'
        prefix = '/' if final == '/' else final.rstrip('/')
        if name:
            self._mounts[name] = (prefix, app)
        if isinstance(app, StaticFiles):
            self._static_mounts.append((prefix, app))
        return None

    def middleware(self, name: str) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        if name.lower() != "http":
            raise ValueError("Only 'http' middleware supported in shim")

        def decorator(func: Callable[[Request, Callable[[Request], Awaitable[Any]]], Awaitable[Any]]):
            self._middlewares.append(func)
            return func

        return decorator

    def url_path_for(self, name: str, **params: Any) -> str:
        if name in self._mounts:
            mount_path, _ = self._mounts[name]
            sub_path = str(params.get('path', '')).strip('/')
            if mount_path == '/':
                return f"/{sub_path}" if sub_path else '/'
            return f"{mount_path}/{sub_path}" if sub_path else mount_path
        for route in self.router.routes:
            if route.name == name:
                return _apply_params(route.path, params)
        raise KeyError(name)

    async def _call(self, request: Request, endpoint: Callable[..., Any], params: Dict[str, Any]) -> Any:
        request.path_params = params
        kwargs = await _build_kwargs(request, endpoint, params)
        result = endpoint(**kwargs)
        if inspect.isawaitable(result):
            result = await result  # type: ignore[assignment]
        return result

    async def _handle(self, request: Request) -> Any:
        route, params = _match_route(self.router.routes, request.path)
        if not route:
            raise HTTPException(status_code=404, detail="Not Found")

        async def call_endpoint(req: Request) -> Any:
            return await self._call(req, route.endpoint, params)

        return await self._dispatch_with_middlewares(request, call_endpoint)

    async def _dispatch_with_middlewares(
        self,
        request: Request,
        handler: Callable[[Request], Awaitable[Any]],
    ) -> Any:
        call_next = handler
        for middleware in reversed(self._middlewares):
            prev = call_next

            async def wrapper(req: Request, mw=middleware, nxt=prev):
                return await mw(req, nxt)

            call_next = wrapper
        return await call_next(request)

    async def __call__(self, scope: Dict[str, Any], receive, send) -> None:
        if scope.get("type") != "http":
            await send({"type": "http.response.start", "status": 500, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return

        body = bytearray()
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                if chunk:
                    body.extend(chunk)
                more_body = message.get("more_body", False)
            elif message["type"] == "http.disconnect":
                return

        headers: Dict[str, str] = {}
        for key, value in scope.get("headers", []):
            headers[key.decode("latin1")] = value.decode("latin1")

        cookies: Dict[str, str] = {}
        raw_cookie = headers.get("cookie", "")
        if raw_cookie:
            parts = [item.strip() for item in raw_cookie.split(";") if item.strip()]
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    cookies[k.strip()] = v.strip()

        query_bytes: bytes = scope.get("query_string", b"")
        query_params = dict(parse_qsl(query_bytes.decode("latin1"))) if query_bytes else {}

        server = scope.get("server") or ("localhost", 80)
        scheme = scope.get("scheme", "http")

        def _first_value(header_name: str) -> str:
            raw = headers.get(header_name, "")
            if not raw:
                return ""
            return raw.split(",")[0].strip()

        forwarded_proto = _first_value("x-forwarded-proto") or _first_value("x-forwarded-scheme")
        host_header = _first_value("x-forwarded-host") or _first_value("host")

        if host_header:
            proto = forwarded_proto or scheme
            base_url = f"{proto}://{host_header.rstrip('/')}"
        else:
            host, port = server
            base_url = f"{scheme}://{host}:{port}"
        path_val = scope.get("path") or scope.get("raw_path") or "/"

        request = Request(
            app=self,
            method=str(scope.get("method", "GET")),
            path=path_val,
            query_params=query_params,
            headers=headers,
            cookies=cookies,
            body=bytes(body),
            base_url=base_url,
        )

        handled_by_mount = False
        result: Any
        for mount_prefix, static_app in self._static_mounts:
            if mount_prefix == '/':
                rel_path = path_val.lstrip('/')
            elif path_val == mount_prefix:
                rel_path = ''
            elif path_val.startswith(f"{mount_prefix}/"):
                rel_path = path_val[len(mount_prefix) + 1 :]
            else:
                continue

            handled_by_mount = True
            try:
                result = static_app.get_response(rel_path, request.method)
            except FileNotFoundError:
                result = Response("Not Found", status_code=404, media_type="text/plain")
            except Exception:
                detail = _json.dumps({"detail": "internal_error"}, ensure_ascii=False)
                result = Response(detail, status_code=500, media_type="application/json")
            break

        if not handled_by_mount:
            try:
                result = await self._handle(request)
            except HTTPException as exc:
                payload = {"detail": exc.detail or ""}
                result = JSONResponse(payload, status_code=exc.status_code)
            except Exception:
                detail = _json.dumps({"detail": "internal_error"}, ensure_ascii=False)
                result = Response(detail, status_code=500, media_type="application/json")

        response = _coerce_response(result)

        if "content-length" not in response.headers:
            response.headers["content-length"] = str(len(response.body or b""))

        await send({"type": "http.response.start", "status": response.status_code, "headers": [
            (str(key).encode("latin1"), str(value).encode("latin1")) for key, value in response.headers.items()
        ]})
        await send({"type": "http.response.body", "body": response.body, "more_body": False})

        background = getattr(response, "background", None)
        if isinstance(background, BackgroundTasks):
            await background.run()



def _join_paths(prefix: str, path: str) -> str:
    base = prefix.rstrip("/")
    tail = path if path.startswith("/") else f"/{path}"
    combined = f"{base}{tail}" if base else tail
    return combined or "/"


def _apply_params(template: str, params: Dict[str, Any]) -> str:
    segments = []
    for part in template.split("/"):
        if part.startswith("{") and part.endswith("}"):
            key = part[1:-1]
            value = params.get(key)
            if value is None:
                raise KeyError(key)
            segments.append(str(value))
        else:
            segments.append(part)
    result = "/".join(segments)
    if not result.startswith("/"):
        result = "/" + result
    return result


def _match_route(routes: List[_Route], target: str) -> tuple[Optional[_Route], Dict[str, Any]]:
    for route in routes:
        params = route.match(target)
        if params is not None:
            return route, params
    return None, {}


def _coerce_response(result: Any) -> Response:
    if isinstance(result, Response):
        return result
    if isinstance(result, dict):
        return JSONResponse(result)
    if isinstance(result, str):
        return Response(result, media_type="text/plain")
    if result is None:
        return Response(status_code=204)
    return Response(str(result), media_type="text/plain")


async def _build_kwargs(
    request: Request,
    endpoint: Callable[..., Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    import inspect

    signature = inspect.signature(endpoint)
    kwargs: Dict[str, Any] = {}
    query = dict(request.query_params.items())
    files = getattr(request, "_files", {})
    form = getattr(request, "_form", {})
    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if name == "request":
            kwargs[name] = request
            continue
        if annotation is BackgroundTasks or name == "background_tasks":
            kwargs[name] = BackgroundTasks()
            continue
        if name in params:
            kwargs[name] = _convert_type(annotation, params[name])
            continue
        if name in files:
            kwargs[name] = files[name]
            continue
        if name in form:
            kwargs[name] = form[name]
            continue
        if name in query:
            kwargs[name] = _convert_type(annotation, query[name])
            continue
        if parameter.default is not inspect._empty:
            kwargs[name] = parameter.default
            continue
        kwargs[name] = None
    return kwargs


def _convert_type(annotation: Any, value: Any) -> Any:
    if annotation in (inspect._empty, Any) or value is None:
        return value
    try:
        if annotation in (int, Optional[int]):
            return int(value)
        if annotation in (float, Optional[float]):
            return float(value)
        if annotation in (bool, Optional[bool]):
            if isinstance(value, bool):
                return value
            lowered = str(value).lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            return bool(value)
        return annotation(value)  # type: ignore[call-arg]
    except Exception:
        return value


def File(*_args: Any, **_kwargs: Any) -> None:
    return None


__all__ = [
    "APIRouter",
    "FastAPI",
    "Request",
    "HTTPException",
    "BackgroundTasks",
    "UploadFile",
    "File",
    "FileResponse",
    "JSONResponse",
    "RedirectResponse",
    "Response",
]
