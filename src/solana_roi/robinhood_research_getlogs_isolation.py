from __future__ import annotations

from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_chain_core as core
from . import robinhood_getlogs_provider_guard as getlogs_guard


REPAIR_VERSION = "robinhood-research-getlogs-isolation-v2-dispatch-seam"
_INSTALLED = False
_ORIGINAL_DISPATCH: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None


def _normalized(value: str) -> str:
    return str(value or "").strip().rstrip("/").lower()


def _explicit_public_research_rpc(self: Any) -> bool:
    return _normalized(getattr(self, "rpc_url", "")) == _normalized(core.ROBINHOOD_PUBLIC_RPC)


async def _research_isolated_dispatch(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None,
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Preserve the intended public/private Robinhood provider boundary.

    The broad research screener deliberately constructs a RobinhoodRpc pointed at the
    official public endpoint. The global provider guard previously preferred
    Validation Cloud for every getLogs dispatch whenever VC was configured, including
    that explicitly public client.

    Patch the guard's dispatch seam rather than RobinhoodRpc.get_logs itself. This is
    safe regardless of import/install order: the guard calls this seam after it is
    installed, while pre-guard imports remain untouched. Public research requests call
    the guard's captured pre-dispatch read implementation using the already-explicit
    public rpc_url. Every non-public request delegates to the original governed
    Validation Cloud/range/failover dispatch unchanged.
    """
    if _explicit_public_research_rpc(self):
        public_get_logs = getlogs_guard._ORIGINAL_GET_LOGS
        if public_get_logs is None:
            raise RuntimeError("robinhood_public_research_getlogs_base_unavailable")
        return await public_get_logs(
            self,
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )

    original = _ORIGINAL_DISPATCH
    if original is None:
        raise RuntimeError("robinhood_governed_getlogs_dispatch_unavailable")
    return await original(
        self,
        from_block=from_block,
        to_block=to_block,
        addresses=addresses,
        topics=topics,
    )


setattr(_research_isolated_dispatch, "_roi_robinhood_research_getlogs_isolation", True)


def install_robinhood_research_getlogs_isolation() -> None:
    global _INSTALLED, _ORIGINAL_DISPATCH
    current = getlogs_guard._dispatch_range
    if bool(getattr(current, "_roi_robinhood_research_getlogs_isolation", False)):
        _INSTALLED = True
        return
    _ORIGINAL_DISPATCH = current
    wrapped = wraps(current)(_research_isolated_dispatch)
    setattr(wrapped, "_roi_robinhood_research_getlogs_isolation", True)
    getlogs_guard._dispatch_range = wrapped  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "composition_order_independent": True,
        "public_research_getlogs_uses_official_public_rpc": True,
        "public_research_getlogs_uses_validation_cloud": False,
        "private_production_getlogs_preserves_governed_guard": True,
        "candidate_universe_reduced": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "_explicit_public_research_rpc",
    "_research_isolated_dispatch",
    "install_robinhood_research_getlogs_isolation",
    "status",
]
