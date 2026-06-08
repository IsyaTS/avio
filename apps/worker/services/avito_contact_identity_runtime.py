from __future__ import annotations

from typing import Any, Callable

from libs.core.integrations import avito as avito_integration
from libs.core.repo import lead_identity
from libs.core.services import avito_contact_identity_resolver


async def resolve_avito_contact_identity(
    *,
    redis_client: Any,
    update_contact_avito_login_fn: Callable[..., Any],
    log_fn: Callable[..., None],
    **kwargs: Any,
) -> avito_contact_identity_resolver.AvitoContactIdentityResult:
    return await avito_contact_identity_resolver.resolve_and_store_avito_contact_identity(
        avito_contact_identity_resolver.AvitoContactIdentityInput(**kwargs),
        deps=avito_contact_identity_resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=avito_integration.resolve_chat_participant_profile,
            update_contact_avito_login_fn=update_contact_avito_login_fn,
            update_lead_contact_fn=lead_identity.update_avito_lead_contact_if_placeholder,
            redis_client=redis_client,
            log_fn=log_fn,
        ),
    )


__all__ = ["resolve_avito_contact_identity"]
