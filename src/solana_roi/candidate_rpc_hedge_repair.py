from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import production_capacity_repair as capacity
from . import rpc_workload_governor as governor
from .solana_rpc import SolanaRpcPool


_ORIGINAL_CALL_WITH_META: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


async def _candidate_hedged_call_with_meta(
    self: SolanaRpcPool,
    method: str,
    params: list[Any],
    *,
    hedge: bool = False,
) -> tuple[Any, str, float]:
    """Restore latency hedging only for frozen-scout candidate reads.

    The production capacity repair intentionally disables proactive hedging to the
    official public endpoint for routine reads because doing so on every request
    generated sustained 429s. Candidate hydration is different: it is already a
    bounded, noncritical workload inside the unchanged endpoint governor and must
    finish inside the unchanged 20-second entry window. Production telemetry proved
    that serial fallback sent every candidate read to PublicNode and produced no
    latency samples before scout expiry.

    For an already-hedged candidate read, call the pool's pre-capacity hedging
    implementation. Endpoint calls still pass through the installed capacity and
    workload-governor wrappers, so 429 cooldowns and the continuity-critical reserve
    remain authoritative. All other workloads keep the existing sequential official
    public fallback policy.
    """
    if (
        hedge
        and governor.current_rpc_workload() == "candidate"
        and capacity._official_pair_requires_sequential_fallback(self)
    ):
        return await capacity._ORIGINAL_RPC_CALL_WITH_META(self, method, params, hedge=True)
    if _ORIGINAL_CALL_WITH_META is None:
        raise RuntimeError("candidate RPC hedge repair is not installed")
    return await _ORIGINAL_CALL_WITH_META(self, method, params, hedge=hedge)


setattr(_candidate_hedged_call_with_meta, "_roi_candidate_official_public_hedge", True)


def _status_with_candidate_hedge(self: SolanaRpcPool) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("candidate RPC hedge repair is not installed")
    payload = _ORIGINAL_STATUS(self)
    control = payload.get("capacity_control")
    if isinstance(control, dict):
        control["candidate_official_public_proactive_hedge_enabled"] = True
        control["candidate_hedge_preserves_429_cooldown"] = True
        control["candidate_hedge_preserves_critical_reservation"] = True
        control["routine_official_public_proactive_hedge_disabled"] = True
    return payload


setattr(_status_with_candidate_hedge, "_roi_candidate_official_public_hedge", True)


def install_candidate_rpc_hedge_repair() -> None:
    global _ORIGINAL_CALL_WITH_META, _ORIGINAL_STATUS

    current_call = SolanaRpcPool.call_with_meta
    if not bool(getattr(current_call, "_roi_candidate_official_public_hedge", False)):
        _ORIGINAL_CALL_WITH_META = current_call
        try:
            _candidate_hedged_call_with_meta.__dict__.update(getattr(current_call, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.call_with_meta = _candidate_hedged_call_with_meta  # type: ignore[method-assign]

    current_status = SolanaRpcPool.status
    if not bool(getattr(current_status, "_roi_candidate_official_public_hedge", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_candidate_hedge.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.status = _status_with_candidate_hedge  # type: ignore[method-assign]


__all__ = [
    "_candidate_hedged_call_with_meta",
    "install_candidate_rpc_hedge_repair",
]
