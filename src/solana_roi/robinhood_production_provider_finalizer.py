from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_production_ws_transport as production_transport


FINALIZER_VERSION = "robinhood-production-provider-finalizer-v1"
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

    ``production_transport`` wraps the final sequencer run/status implementation.
    This finalizer then makes enforcement instance-scoped and restores a composite
    module-level freshness function so old direct tests keep their historical helper
    while the actual running production plane cannot use it for authority.
    """
    global _INSTALLED, _LEGACY_FRESH_READY
    if _INSTALLED:
        return

    _LEGACY_FRESH_READY = legacy_fresh_ready
    production_transport.install_robinhood_production_ws_transport(plane_cls)

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
