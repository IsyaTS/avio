"""Very small synchronous test client used in unit tests."""

from __future__ import annotations

import asyncio
import json as json_module
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlencode

from . import BackgroundTasks, FastAPI, HTTPException, Request, UploadFile
from .responses import JSONResponse, Response


class _ClientResponse:
    def __init__(self, response: Response) -> None:
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self._content = response.content

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        return self._response.text

    def json(self) -> Any:
        return self._response.json()


class TestClient:
    def __init__(self, app: FastAPI, *, base_url: str = "http://testserver") -> None:
        self.app = app
        self.base_url = base_url.rstrip("/")
        self._loop = asyncio.new_event_loop()

    # Context management -------------------------------------------------
    def __enter__(self) -> "TestClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if not self._loop.is_closed():
            self._loop.close()

    # HTTP verbs ---------------------------------------------------------
    def get(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, Any]] = None,
        follow_redirects: bool = True,
    ) -> _ClientResponse:
        return self.request("GET", url, params=params, headers=headers, follow_redirects=follow_redirects)

    def post(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, Any]] = None,
        json: Any = None,
        data: Any = None,
        files: Optional[Mapping[str, Tuple[str, Any, Optional[str]]]] = None,
        follow_redirects: bool = True,
    ) -> _ClientResponse:
        return self.request(
            "POST",
            url,
            params=params,
            headers=headers,
            json=json,
            data=data,
            files=files,
            follow_redirects=follow_redirects,
        )

    # Core request dispatch ---------------------------------------------
    def request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, Any]] = None,
        json: Any = None,
        data: Any = None,
        files: Optional[Mapping[str, Tuple[str, Any, Optional[str]]]] = None,
        follow_redirects: bool = True,
    ) -> _ClientResponse:
        target_url = urljoin(f"{self.base_url}/", url.lstrip("/"))
        parsed = urlparse(target_url)
        query: Dict[str, Any] = dict(parse_qsl(parsed.query))
        if params:
            query.update({k: v for k, v in params.items() if v is not None})

        body_bytes = b""
        content_type = None
        uploads: Dict[str, UploadFile] = {}
        form_data: Dict[str, Any] = {}

        json_payload = json
        if json_payload is not None:
            body_bytes = json_module.dumps(json_payload, ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
        elif data is not None:
            if isinstance(data, (dict, list, tuple)):
                body_bytes = urlencode(data).encode("utf-8")
                content_type = "application/x-www-form-urlencoded"
            else:
                body_bytes = str(data).encode("utf-8")
        if files:
            uploads, form_data = _prepare_files(files)

        headers_dict = {str(k): str(v) for k, v in (headers or {}).items()}
        if content_type and "content-type" not in {k.lower() for k in headers_dict}:
            headers_dict["content-type"] = content_type

        request = Request(
            app=self.app,
            method=method,
            path=parsed.path or "/",
            query_params=query,
            headers=headers_dict,
            cookies={},
            body=body_bytes,
            base_url=f"{parsed.scheme}://{parsed.netloc}",
        )
        request._files = uploads  # type: ignore[attr-defined]
        request._form = form_data  # type: ignore[attr-defined]

        response = self._run(self._dispatch(request))

        if follow_redirects and response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location")
            if location:
                next_method = "GET" if response.status_code in {301, 302, 303} else method
                return self.request(next_method, location, follow_redirects=False)

        background = getattr(response._response, "background", None)  # type: ignore[attr-defined]
        if isinstance(background, BackgroundTasks):
            self._run(background.run())

        return response

    async def _dispatch(self, request: Request) -> _ClientResponse:
        try:
            raw = await self.app._handle(request)  # type: ignore[attr-defined]
        except HTTPException as exc:
            payload = {"detail": exc.detail or ""}
            raw = Response(json_module.dumps(payload), status_code=exc.status_code, media_type="application/json")
        response = _coerce_response(raw)
        return _ClientResponse(response)

    def _run(self, coro):
        return self._loop.run_until_complete(coro)


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


def _prepare_files(files: Mapping[str, Tuple[str, Any, Optional[str]]]) -> tuple[Dict[str, UploadFile], Dict[str, Any]]:
    uploads: Dict[str, UploadFile] = {}
    extra: Dict[str, Any] = {}
    for field, value in files.items():
        if not isinstance(value, tuple) or len(value) < 2:
            continue
        filename, content = value[0], value[1]
        content_type = value[2] if len(value) > 2 else None
        if hasattr(content, "read"):
            raw = content.read()  # type: ignore[attr-defined]
        elif isinstance(content, str):
            raw = content.encode("utf-8")
        else:
            raw = bytes(content)
        uploads[field] = UploadFile(filename=filename, content=raw, content_type=content_type)
    return uploads, extra


__all__ = ["TestClient"]
