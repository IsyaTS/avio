from __future__ import annotations

from typing import Any, Callable

from fastapi import Request


def _runtime() -> Any:
    from apps.api.web.services import avito_oauth_runtime

    return avito_oauth_runtime


def _status(deps_builder: Callable[[], Any]):
    async def handler(request: Request, tenant: int | None = None, k: str | None = None):
        return await _runtime().oauth_status(request, tenant=tenant, key=k, deps=deps_builder())

    return handler


def _authorize(deps_builder: Callable[[], Any]):
    async def handler(
        request: Request,
        tenant: int | None = None,
        k: str | None = None,
        redirect: bool = False,
    ):
        return await _runtime().oauth_authorize(
            request, tenant=tenant, key=k, redirect=redirect, deps=deps_builder()
        )

    return handler


def _callback(deps_builder: Callable[[], Any]):
    async def handler(
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
    ):
        return await _runtime().oauth_callback(
            request, code=code, state=state, error=error, deps=deps_builder()
        )

    return handler


def _disconnect(deps_builder: Callable[[], Any]):
    async def handler(request: Request, tenant: int | None = None, k: str | None = None):
        return await _runtime().oauth_disconnect(request, tenant=tenant, key=k, deps=deps_builder())

    return handler


def _webhook(deps_builder: Callable[[], Any]):
    async def handler(request: Request, tenant: int | None = None, k: str | None = None):
        return await _runtime().oauth_webhook(request, tenant=tenant, key=k, deps=deps_builder())

    return handler


def _accounts(deps_builder: Callable[[], Any]):
    async def handler(request: Request, tenant: int | None = None, k: str | None = None):
        return await _runtime().oauth_accounts(request, tenant=tenant, key=k, deps=deps_builder())

    return handler


def _account_action(deps_builder: Callable[[], Any], action: str):
    async def handler(
        account_id: int,
        request: Request,
        tenant: int | None = None,
        k: str | None = None,
    ):
        runtime = _runtime()
        target = {
            "primary": runtime.oauth_account_primary,
            "rename": runtime.oauth_account_rename,
            "disconnect": runtime.oauth_account_disconnect,
            "webhook": runtime.oauth_account_webhook,
        }[action]
        return await target(
            request,
            tenant=tenant,
            key=k,
            account_id=account_id,
            deps=deps_builder(),
        )

    return handler


def register_routes(router: Any, deps_builder: Callable[[], Any]) -> None:
    routes = (
        ("/status", ["GET"], "avito_oauth_status", _status(deps_builder)),
        ("/authorize", ["GET"], "avito_oauth_authorize", _authorize(deps_builder)),
        ("/callback", ["GET"], "avito_oauth_callback", _callback(deps_builder)),
        ("/disconnect", ["POST"], "avito_oauth_disconnect", _disconnect(deps_builder)),
        ("/webhook", ["POST"], "avito_oauth_webhook", _webhook(deps_builder)),
        ("/accounts", ["GET"], "avito_oauth_accounts", _accounts(deps_builder)),
        (
            "/accounts/{account_id}/primary",
            ["POST"],
            "avito_oauth_account_primary",
            _account_action(deps_builder, "primary"),
        ),
        (
            "/accounts/{account_id}/rename",
            ["POST"],
            "avito_oauth_account_rename",
            _account_action(deps_builder, "rename"),
        ),
        (
            "/accounts/{account_id}/disconnect",
            ["POST"],
            "avito_oauth_account_disconnect",
            _account_action(deps_builder, "disconnect"),
        ),
        (
            "/accounts/{account_id}/webhook",
            ["POST"],
            "avito_oauth_account_webhook",
            _account_action(deps_builder, "webhook"),
        ),
    )
    for path, methods, name, handler in routes:
        router.add_api_route(path, handler, methods=methods, name=name)
