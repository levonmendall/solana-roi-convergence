from __future__ import annotations

import asyncio
import os
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import quote, urlparse

from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_production_ws_transport as production_transport
from . import robinhood_provider_budget_transport as provider_budget
from . import robinhood_usage_bounded_transport as bounded_transport
from .robinhood_adaptive_lane_controller import (
    install_robinhood_adaptive_lane_controller,
    status as adaptive_lane_controller_status,
)
from .robinhood_alchemy_budget_guard import (
    install_robinhood_alchemy_budget_guard,
    status as alchemy_budget_guard_status,
)
from .robinhood_event_driven_settlement import (
    install_robinhood_event_driven_settlement,
    status as event_driven_settlement_status,
)
from .robinhood_getlogs_provider_guard import (
    install_robinhood_getlogs_provider_guard,
    status as getlogs_provider_guard_status,
)
from .robinhood_provider_budget_transport import (
    install_robinhood_provider_budget_transport,
    status as provider_budget_transport_status,
)
from .robinhood_provider_failover import (
    install_robinhood_provider_failover,
    status as provider_failover_status,
)
from .robinhood_usage_bounded_transport import (
    install_robinhood_usage_bounded_transport,
    status as usage_bounded_transport_status,
)


FINALIZER_VERSION = "robinhood-production-provider-finalizer-v10-drpc-secondary"
DRPC_NETWORK_SLUG = "robinhood"
DRPC_ENDPOINT_HOST = "lb.drpc.live"
DRPC_KEY_ENV_NAMES = (
    "DRPC_API_KEY",
    "ROBINHOOD_DRPC_API_KEY",
    "DRPC_KEY",
)
_INSTALLED = False
_DRPC_BACKUP_BOOTSTRAPPED = False
_LEGACY_FRESH_READY: Callable[[Any], Awaitable[bool]] | None = None


def _normalized_endpoint(value: str) -> str:
    return value.rstrip("/").lower()


def _resolved_production_ws_url() -> str:
    """Resolve production WSS without promoting public or insecure RPC transport.

    An explicit WebSocket setting is authoritative and is returned unchanged for the
    production transport's existing validation. Only when that setting is absent may
    an explicitly configured private HTTPS Robinhood RPC be transformed to the
    provider-equivalent ``wss://`` URL. The built-in public fallback and plain HTTP
    are deliberately ineligible, so missing/unsafe configuration remains fail-closed.
    """
    explicit_ws = (os.getenv("ROBINHOOD_WS_URL") or "").strip()
    if explicit_ws:
        return explicit_ws

    configured_rpc = (os.getenv("ROBINHOOD_RPC_URL") or "").strip()
    if not configured_rpc:
        return ""
    if _normalized_endpoint(configured_rpc) == _normalized_endpoint(
        production_transport.runtime.ROBINHOOD_PUBLIC_RPC
    ):
        return ""

    try:
        parsed = urlparse(configured_rpc)
    except Exception:
        return ""
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        return ""
    return parsed._replace(scheme="wss").geturl()


def _drpc_api_key() -> str:
    """Return the first configured dRPC credential without exposing it to telemetry."""
    for name in DRPC_KEY_ENV_NAMES:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _drpc_backup_pair_urls() -> tuple[str, str] | None:
    """Build a dRPC Robinhood pair only as a secondary to a valid private primary.

    Explicit provider-pool JSON remains authoritative. Likewise, any explicit backup
    setting (including a partial pair) is never overwritten: a half-configured backup
    must remain visibly fail-closed instead of being silently repaired by a secret.
    This helper only turns a dRPC key into the provider's documented paired HTTPS/WSS
    endpoint when the existing Robinhood primary is already a valid private pair.
    """
    if (os.getenv("ROBINHOOD_RPC_ENDPOINTS_JSON") or "").strip():
        return None

    explicit_backup_http = (os.getenv("ROBINHOOD_BACKUP_RPC_URL") or "").strip()
    explicit_backup_ws = (os.getenv("ROBINHOOD_BACKUP_WS_URL") or "").strip()
    if explicit_backup_http or explicit_backup_ws:
        return None

    api_key = _drpc_api_key()
    if not api_key:
        return None

    primary_http = (os.getenv("ROBINHOOD_RPC_URL") or "").strip()
    primary_ws = _resolved_production_ws_url()
    if not primary_http or not primary_ws:
        return None
    if _normalized_endpoint(primary_http) == _normalized_endpoint(
        production_transport.runtime.ROBINHOOD_PUBLIC_RPC
    ):
        return None
    if _normalized_endpoint(primary_ws) == _normalized_endpoint(
        production_transport.PUBLIC_SEQUENCER_FEED
    ):
        return None

    try:
        http_parts = urlparse(primary_http)
        ws_parts = urlparse(primary_ws)
    except Exception:
        return None
    if http_parts.scheme.lower() != "https" or not http_parts.netloc:
        return None
    if ws_parts.scheme.lower() != "wss" or not ws_parts.netloc:
        return None

    key_path = quote(api_key, safe="")
    path = f"{DRPC_NETWORK_SLUG}/{key_path}"
    return (
        f"https://{DRPC_ENDPOINT_HOST}/{path}",
        f"wss://{DRPC_ENDPOINT_HOST}/{path}",
    )


def _install_drpc_backup_from_key() -> bool:
    """Install a process-local dRPC backup pair without persisting or logging its key."""
    global _DRPC_BACKUP_BOOTSTRAPPED
    pair = _drpc_backup_pair_urls()
    if pair is None:
        return False
    os.environ["ROBINHOOD_BACKUP_RPC_URL"] = pair[0]
    os.environ["ROBINHOOD_BACKUP_WS_URL"] = pair[1]
    _DRPC_BACKUP_BOOTSTRAPPED = True
    return True


def _install_private_https_wss_derivation() -> None:
    current = production_transport._ws_url
    if bool(getattr(current, "_roi_private_https_wss_derivation", False)):
        return
    setattr(_resolved_production_ws_url, "_roi_private_https_wss_derivation", True)
    production_transport._ws_url = _resolved_production_ws_url


async def _final_fresh_ready(self: Any) -> bool:
    """Use production provider authority only for the real running worker.

    Isolated unit/regression calls that never start the production worker retain the
    historical fresh-head helper. Once ``run()`` starts, every entry decision is
    governed by the production RPC/WebSocket transport and the canonical event-age
    ceiling. This keeps compatibility tests meaningful without allowing public
    research transport to authorize actual production paper entries.
    """
    if bool(getattr(self, "_roi_production_provider_enforce", False)):
        return await production_transport._production_fresh_ready(self)
    if _LEGACY_FRESH_READY is None:
        return False
    return await _LEGACY_FRESH_READY(self)


def _enforcing_run(original: Callable[[Any, asyncio.Event], Awaitable[None]]) -> Callable[[Any, asyncio.Event], Awaitable[None]]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        setattr(self, "_roi_production_provider_enforce", True)
        try:
            await original(self, stop)
        finally:
            # Keep the instance fail-closed after shutdown; it must never fall back to
            # test/legacy freshness semantics after having been a production worker.
            setattr(self, "_roi_production_provider_enforce", True)

    setattr(wrapped, "_roi_robinhood_production_provider_finalizer", True)
    return wrapped


def _preserve_bounded_transport_aliases() -> None:
    """Keep the bounded module's canonical reader aliases on the final wrappers.

    The bounded transport historically exposes the exact reader/readiness functions
    installed on the production transport module. Provider failover adds one final
    generation-aware wrapper around those functions. Mirror the final callables back
    into the bounded module so callers and architecture checks see one canonical
    reader identity while the generation fail-closed semantics remain intact.
    """
    bounded_transport._reader_async = production_transport._reader_async
    bounded_transport._reader_ready = production_transport._reader_ready


def install_robinhood_production_provider_finalizer(
    plane_cls: type[Any],
    *,
    legacy_fresh_ready: Callable[[Any], Awaitable[bool]],
) -> None:
    """Install the final production provider authority chain.

    Broad discovery remains on the research-only public plane while bounded private
    WebSocket subscriptions carry only the prospective live shortlist and open
    positions. The hard Alchemy budget guard coalesces duplicate ``eth_call`` work,
    budgets noncritical reads, and reserves open-position settlement as critical.

    Provider failover is deliberately installed *after* that guard so quota/budget
    exhaustion, 429s, provider 5xx/transport failures, or repeated WebSocket failures
    can move the complete private HTTP/WSS pair to a configured backup. A switch
    invalidates the old provider generation immediately; paper-entry readiness stays
    false until the replacement WebSocket has verified Robinhood chain id 4663 and
    re-established the bounded subscription. Public RPC/sequencer transport never
    enters the authoritative provider pool. Strategy economics and v5.2 authority are
    unchanged, and signing/submission/live-money capability remains absent.
    """
    global _INSTALLED, _LEGACY_FRESH_READY
    if _INSTALLED:
        return

    _LEGACY_FRESH_READY = legacy_fresh_ready
    install_robinhood_getlogs_provider_guard()
    install_robinhood_provider_budget_transport()
    # The budget installer patches the bounded module before it is installed. Restore
    # the two-stage wrapper *function* here (not the already-bound wrapper factory), so
    # bounded installation can compose it around the production status wrapper without
    # invoking a status method at import time.
    bounded_transport._augment_status_wrapper = provider_budget._augment_status_wrapper
    install_robinhood_usage_bounded_transport()
    # Keep the production transport's existing validation as final authority. This
    # resolver only supplies the provider-equivalent WSS when a private HTTPS RPC was
    # explicitly configured and no explicit WSS override exists.
    _install_private_https_wss_derivation()
    # A Render-held dRPC key may supply the secondary Robinhood pair, but only behind
    # an already-valid private primary and never over explicit JSON/backup settings.
    _install_drpc_backup_from_key()
    production_transport.install_robinhood_production_ws_transport(plane_cls)
    install_robinhood_event_driven_settlement(plane_cls)
    install_robinhood_adaptive_lane_controller(plane_cls)
    install_robinhood_alchemy_budget_guard(plane_cls)
    # Outermost provider wrapper: catches provider/budget failures emitted by the
    # guarded RPC path and coordinates the HTTP + WSS generation switch.
    install_robinhood_provider_failover()
    _preserve_bounded_transport_aliases()

    current_run = plane_cls.run
    if not bool(getattr(current_run, "_roi_robinhood_production_provider_finalizer", False)):
        plane_cls.run = _enforcing_run(current_run)  # type: ignore[method-assign]

    frontier._fresh_head_ready = _final_fresh_ready  # type: ignore[assignment]
    setattr(plane_cls, "_roi_robinhood_production_provider_finalizer_version", FINALIZER_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "finalizer_version": FINALIZER_VERSION,
        "installed": _INSTALLED,
        "instance_scoped_production_enforcement": True,
        "public_transport_can_authorize_running_worker": False,
        "private_https_rpc_wss_derivation": True,
        "explicit_websocket_precedence": True,
        "public_rpc_wss_derivation_allowed": False,
        "plain_http_rpc_wss_derivation_allowed": False,
        "drpc_secret_secondary_supported": True,
        "drpc_secret_secondary_bootstrapped": _DRPC_BACKUP_BOOTSTRAPPED,
        "drpc_secondary_network": DRPC_NETWORK_SLUG,
        "getlogs_provider_guard": getlogs_provider_guard_status(),
        "provider_budget_transport": provider_budget_transport_status(),
        "provider_transport": usage_bounded_transport_status(),
        "event_driven_settlement": event_driven_settlement_status(),
        "adaptive_lane_controller": adaptive_lane_controller_status(),
        "alchemy_budget_guard": alchemy_budget_guard_status(),
        "provider_failover": provider_failover_status(),
        "canonical_latency_hard_max_seconds": production_transport.canonical_latency_hard_max_seconds(),
        "legacy_two_block_gate_has_production_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "FINALIZER_VERSION",
    "_drpc_api_key",
    "_drpc_backup_pair_urls",
    "_install_drpc_backup_from_key",
    "_final_fresh_ready",
    "_install_private_https_wss_derivation",
    "_preserve_bounded_transport_aliases",
    "_resolved_production_ws_url",
    "install_robinhood_production_provider_finalizer",
    "status",
]
