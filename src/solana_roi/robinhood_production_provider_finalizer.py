from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_production_ws_transport as production_transport
from . import robinhood_provider_budget_transport as provider_budget
from . import robinhood_usage_bounded_transport as bounded_transport
from .robinhood_adaptive_lane_controller import (
    install_robinhood_adaptive_lane_controller,
    status as adaptive_lane_controller_status,
)
from .robinhood_event_driven_settlement import (
    install_robinhood_event_driven_settlement,
    status as event_driven_settlement_status,
)
from .robinhood_provider_budget_transport import (
    install_robinhood_provider_budget_transport,
    status as provider_budget_transport_status,
)
from .robinhood_usage_bounded_transport import (
    install_robinhood_usage_bounded_transport,
    status as usage_bounded_transport_status,
)


FINALIZER_VERSION = "robinhood-production-provider-finalizer-v5-adaptive-lanes"
_INSTALLED = False
_LEGACY_FRESH_READY: Callable[[Any], Awaitable[bool]] | None = None


async def _final_fresh_ready(self: Any) -> bool:
    """Use production provider authority only for the real running worker.

    Isolated unit/regression calls that never start the production worker retain the
    historical fresh-head helper. Once ``run()`` starts, every entry decision is
    governed by the production RPC/WebSocket transport and frozen v5.1 event-age
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


def install_robinhood_production_provider_finalizer(
    plane_cls: type[Any],
    *,
    legacy_fresh_ready: Callable[[Any], Awaitable[bool]],
) -> None:
    """Install the provider transport after every sequencer/legacy wrapper.

    The provider-budget plane patches only acquisition mechanics before the bounded
    production transport is installed: all persisted candidates are screened on a
    research-only public plane, known factories stay continuously discoverable, and
    Alchemy is reserved for a small prospective live shortlist plus open positions.
    Event-driven settlement removes redundant exact provider quotes. The adaptive lane
    controller then varies only prospective Alchemy capacity from 1-4 lanes according
    to locally metered provider load and ranked simultaneous demand; all open positions
    remain forced live outside that cap. Strategy economics and frozen v5.1 entry
    authority remain downstream and unchanged.
    """
    global _INSTALLED, _LEGACY_FRESH_READY
    if _INSTALLED:
        return

    _LEGACY_FRESH_READY = legacy_fresh_ready
    install_robinhood_provider_budget_transport()
    # The budget installer patches the bounded module before it is installed. Restore
    # the two-stage wrapper *function* here (not the already-bound wrapper factory), so
    # bounded installation can compose it around the production status wrapper without
    # invoking a status method at import time.
    bounded_transport._augment_status_wrapper = provider_budget._augment_status_wrapper
    install_robinhood_usage_bounded_transport()
    production_transport.install_robinhood_production_ws_transport(plane_cls)
    install_robinhood_event_driven_settlement(plane_cls)
    install_robinhood_adaptive_lane_controller(plane_cls)

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
        "provider_budget_transport": provider_budget_transport_status(),
        "provider_transport": usage_bounded_transport_status(),
        "event_driven_settlement": event_driven_settlement_status(),
        "adaptive_lane_controller": adaptive_lane_controller_status(),
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
    "install_robinhood_production_provider_finalizer",
    "status",
]
