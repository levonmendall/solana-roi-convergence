from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import funding_provenance_repair as funding
from . import launch_chain_timing_repair as chain_timing
from . import launch_reference_timing_repair as reference
from . import launch_ws_frontier_timing_repair as frontier
from . import raw_receipt_dispatch_repair as raw_receipt
from .coverage_completeness_repair import _launch_contexts
from .launch_funding import DexScreenerLaunchCollector


_V7_COLLECT = frontier._launch_collect_with_ws_frontier
_ORIGINAL_FUNDING_STATUS_FACTORY = funding._status_with_funding_provenance


async def _collect_with_offline_timing_compatibility(
    self: DexScreenerLaunchCollector,
    mint: str,
    at: Any,
) -> bool:
    """Keep published v4/v5 offline fixtures while production remains v7-only.

    Production launch receipts always persist a v7 frontier row, including an
    explicit missing/stale row. A hand-seeded direct/offline fixture can have an
    older v4 timing row but no v7 frontier row; delegate only that case to the
    established reference collector so prior helper/test contracts remain stable.
    """

    context = _launch_contexts(self).get(mint)
    if isinstance(context, dict):
        signature = str(context.get("launch_signature") or "")
        if (
            signature
            and frontier._frontier_row(self.store, signature) is None
            and chain_timing._timing_row(self.store, signature) is not None
        ):
            return await reference._launch_collect_with_reference_timing(self, mint, at)
    return await _V7_COLLECT(self, mint, at)


setattr(_collect_with_offline_timing_compatibility, "_roi_ws_frontier_compatibility", True)


def _safe_funding_status_factory(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    wrapped = _ORIGINAL_FUNDING_STATUS_FACTORY(original)

    def status(self: Any) -> dict[str, Any]:
        # Several intrinsic status regressions deliberately construct a minimal
        # ingestion plane without the runtime service/collector graph. The funding
        # telemetry is additive and must not make the core status endpoint require
        # that optional graph.
        if not hasattr(self, "service"):
            return original(self)
        return wrapped(self)

    try:
        status.__dict__.update(getattr(wrapped, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_funding_provenance_repair", True)
    setattr(status, "_roi_funding_status_intrinsic_safe", True)
    return status


def install_production_boundary_compatibility() -> None:
    DexScreenerLaunchCollector.collect = _collect_with_offline_timing_compatibility  # type: ignore[method-assign]
    # Funding is installed immediately after this shim in __init__.py. Replace only
    # its status-wrapper factory so minimal intrinsic status callers stay supported;
    # production funding collection logic is untouched.
    funding._status_with_funding_provenance = _safe_funding_status_factory  # type: ignore[assignment]

    # The v7 production cohort proved that timing evidence was still stamped only
    # after each WebSocket reader waited for synchronous durable dispatch. Install
    # the bounded raw-receipt dispatcher here, after v7 timing is active and before
    # the runtime is constructed, so every supported entrypoint observes socket-read
    # arrival time without changing launch, funding, continuity, or safety gates.
    raw_receipt.install_raw_receipt_dispatch_repair()

    # `_launch_like` already carries the program-scoped detector marker contract
    # installed by runtime_guards. The sentinel wrapper changes transport shape,
    # not detection scope, so preserve every existing marker for production.py and
    # the intrinsic regression suite.
    try:
        raw_receipt._launch_like_with_sentinel.__dict__.update(
            getattr(raw_receipt._ORIGINAL_LAUNCH_LIKE, "__dict__", {})
        )
    except Exception:
        pass


__all__ = [
    "install_production_boundary_compatibility",
    "_collect_with_offline_timing_compatibility",
    "_safe_funding_status_factory",
]
