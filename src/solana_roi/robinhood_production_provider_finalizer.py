from __future__ import annotations

import asyncio
import os
from functools import wraps
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

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
from .robinhood_drpc_block_number_compat import (
    install_robinhood_drpc_block_number_compat,
    status as drpc_block_number_compat_status,
)
from .robinhood_drpc_http_failure_diagnostic import (
    install_robinhood_drpc_http_failure_diagnostic,
)
from .robinhood_event_driven_settlement import (
    install_robinhood_event_driven_settlement,
    status as event_driven_settlement_status,
)
from .robinhood_getlogs_capability_repair import (
    install_robinhood_getlogs_capability_repair,
    status as getlogs_capability_repair_status,
)
from .robinhood_getlogs_provider_guard import (
    install_robinhood_getlogs_provider_guard,
    status as getlogs_provider_guard_status,
)
from .robinhood_provider_budget_transport import (
    install_robinhood_provider_budget_transport,
    status as provider_budget_transport_status,
)
from .robinhood_provider_capacity_budget import (
    install_robinhood_provider_capacity_budget,
    status as provider_capacity_budget_status,
)
from .robinhood_provider_failover import (
    install_robinhood_provider_failover,
    status as provider_failover_status,
)
from .robinhood_provider_pool_throughput_repair import (
    install_robinhood_provider_pool_throughput_repair,
    status as provider_pool_throughput_status,
)
from .robinhood_provider_runtime_proof import install_robinhood_provider_runtime_proof
from .robinhood_usage_bounded_transport import (
    install_robinhood_usage_bounded_transport,
    status as usage_bounded_transport_status,
)


FINALIZER_VERSION = "robinhood-production-provider-finalizer-v15-capability-specific-getlogs"
_INSTALLED = False
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
            setattr(self, "_roi_production_provider_enforce", True)

    setattr(wrapped, "_roi_robinhood_production_provider_finalizer", True)
    return wrapped


def _preserve_bounded_transport_aliases() -> None:
    """Keep the bounded module's canonical reader aliases on the final wrappers."""
    bounded_transport._reader_async = production_transport._reader_async
    bounded_transport._reader_ready = production_transport._reader_ready


def install_robinhood_production_provider_finalizer(
    plane_cls: type[Any],
    *,
    legacy_fresh_ready: Callable[[Any], Awaitable[bool]],
) -> None:
    """Install the final production provider authority chain.

    Broad discovery remains promotion-only. Healthy private providers may carry the
    broad screening workload, while the provider-capacity layer enforces provider
    burst limits and durable calendar-month request allowances before failover. The
    current operational budget is 3M Chainstack + 3M Alchemy requests per UTC month,
    with a combined 6M ceiling and a protected reserve for decision-critical quotes
    and settlement. Candidate discovery remains complete; budget pacing changes only
    acquisition cadence and never grants or removes paper-entry authority.

    dRPC compatibility remains installed for safe historical/future recovery but has
    no special authority. Provider generation switching, Robinhood chain-id 4663
    verification, fresh-event authority, paper-only operation, and the absence of
    signing/submission/live-money capability are unchanged. ``eth_getLogs`` is
    capability-specific: basic EVM reads cannot make a provider fully healthy when
    mandatory log retrieval is unavailable.
    """
    global _INSTALLED, _LEGACY_FRESH_READY
    if _INSTALLED:
        return

    _LEGACY_FRESH_READY = legacy_fresh_ready
    install_robinhood_provider_pool_throughput_repair()
    install_robinhood_getlogs_provider_guard()
    install_robinhood_provider_budget_transport()
    bounded_transport._augment_status_wrapper = provider_budget._augment_status_wrapper
    install_robinhood_usage_bounded_transport()
    _install_private_https_wss_derivation()
    production_transport.install_robinhood_production_ws_transport(plane_cls)
    install_robinhood_event_driven_settlement(plane_cls)
    install_robinhood_adaptive_lane_controller(plane_cls)
    install_robinhood_alchemy_budget_guard(plane_cls)

    # Provider-specific compatibility remains immediately inside capacity/failover.
    # The capacity guard then counts every actual private-provider attempt, including
    # bounded WSS control requests, before failover decides whether another provider
    # should be tried.
    install_robinhood_drpc_block_number_compat(production_transport.runtime.RobinhoodRpc)
    install_robinhood_provider_capacity_budget()
    install_robinhood_provider_failover()
    install_robinhood_provider_runtime_proof()
    install_robinhood_drpc_http_failure_diagnostic()

    # Install after the provider pool has captured its inner RPC seam. This lets the
    # getLogs repair probe/reroute one capability without changing the paired basic
    # HTTP/WSS provider or bypassing the existing capacity/budget wrappers.
    install_robinhood_getlogs_capability_repair()
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
        "drpc_block_number_compat": drpc_block_number_compat_status(),
        "provider_pool_throughput": provider_pool_throughput_status(),
        "provider_capacity_budget": provider_capacity_budget_status(),
        "getlogs_provider_guard": getlogs_provider_guard_status(),
        "getlogs_capability_repair": getlogs_capability_repair_status(),
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
    "_final_fresh_ready",
    "_install_private_https_wss_derivation",
    "_preserve_bounded_transport_aliases",
    "_resolved_production_ws_url",
    "install_robinhood_production_provider_finalizer",
    "status",
]
