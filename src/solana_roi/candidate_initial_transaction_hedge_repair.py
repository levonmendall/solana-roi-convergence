from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from . import candidate_certification_hotpath_repair as hotpath
from . import candidate_rpc_priority_repair as priority
from . import production_capacity_repair as capacity
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import SolanaRpcPool


_CANDIDATE_INITIAL_TRANSACTION_HEDGE: ContextVar[bool] = ContextVar(
    "roi_candidate_initial_transaction_hedge",
    default=False,
)
_PREVIOUS_GET_TRANSACTION_READY: Callable[..., Any] | None = None
_PREVIOUS_CALL_WITH_META: Callable[..., Any] | None = None
_PREVIOUS_RPC_STATUS: Callable[..., dict[str, Any]] | None = None
_PREVIOUS_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None


def _increment(pool: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_initial_hedge_{name}"
    setattr(pool, attr, int(getattr(pool, attr, 0) or 0) + int(amount))


def _winner_counts(pool: Any) -> dict[str, int]:
    value = getattr(pool, "_roi_candidate_initial_hedge_winners", None)
    if not isinstance(value, dict):
        value = {}
        setattr(pool, "_roi_candidate_initial_hedge_winners", value)
    return value


async def _get_transaction_ready_with_candidate_hedge(
    self: DirectSolanaIngestionPlane,
    signature: str,
    *,
    hedge: bool,
    attempts: int,
) -> tuple[Any, str | None, float | None]:
    if _PREVIOUS_GET_TRANSACTION_READY is None:
        raise RuntimeError("candidate initial transaction hedge repair is not installed")

    reason = str(hotpath._CURRENT_HYDRATION_REASON.get() or "")
    candidate_initial_read = bool(
        hedge
        and reason in hotpath.SCOUT_REASONS
        and governor.current_rpc_workload() == priority.WORKLOAD_CANDIDATE
    )
    if not candidate_initial_read:
        return await _PREVIOUS_GET_TRANSACTION_READY(
            self,
            signature,
            hedge=hedge,
            attempts=attempts,
        )

    token = _CANDIDATE_INITIAL_TRANSACTION_HEDGE.set(True)
    try:
        return await _PREVIOUS_GET_TRANSACTION_READY(
            self,
            signature,
            hedge=hedge,
            attempts=attempts,
        )
    finally:
        _CANDIDATE_INITIAL_TRANSACTION_HEDGE.reset(token)


setattr(_get_transaction_ready_with_candidate_hedge, "_roi_candidate_initial_transaction_hedge", True)


async def _call_with_candidate_initial_hedge(
    self: SolanaRpcPool,
    method: str,
    params: list[Any],
    *,
    hedge: bool = False,
) -> tuple[Any, str, float]:
    if _PREVIOUS_CALL_WITH_META is None:
        raise RuntimeError("candidate initial transaction hedge repair is not installed")

    candidate_initial = bool(
        hedge
        and method == "getTransaction"
        and _CANDIDATE_INITIAL_TRANSACTION_HEDGE.get()
        and governor.current_rpc_workload() == priority.WORKLOAD_CANDIDATE
        and capacity._official_pair_requires_sequential_fallback(self)
    )
    if not candidate_initial:
        return await _PREVIOUS_CALL_WITH_META(self, method, params, hedge=hedge)

    # PR #64 intentionally disabled proactive official-public hedging for the
    # routine two-provider pair after broad certification traffic caused sustained
    # 429s. Frozen-scout transaction availability is a different workload: it has
    # the unchanged 2s ingestion/5s end-to-end certification target and is already
    # isolated by PR #90's noncritical candidate reservation. Reuse the canonical
    # original hedge only for this exact initial transaction read. Endpoint
    # cooldowns and the process-wide governor remain enforced because the original
    # hedger calls the currently composed self._ordered/self._call_endpoint methods.
    _increment(self, "attempts")
    try:
        result, provider, latency = await capacity._ORIGINAL_RPC_CALL_WITH_META(
            self,
            method,
            params,
            hedge=True,
        )
    except Exception:
        _increment(self, "errors")
        raise
    winners = _winner_counts(self)
    winners[str(provider)] = int(winners.get(str(provider), 0) or 0) + 1
    return result, provider, latency


setattr(_call_with_candidate_initial_hedge, "_roi_candidate_initial_transaction_hedge", True)


def _rpc_status_with_candidate_initial_hedge(self: SolanaRpcPool) -> dict[str, Any]:
    if _PREVIOUS_RPC_STATUS is None:
        raise RuntimeError("candidate initial transaction hedge repair is not installed")
    payload = _PREVIOUS_RPC_STATUS(self)
    payload["candidate_initial_transaction_hedge"] = {
        "installed": True,
        "scope": "frozen-scout-initial-getTransaction-only",
        "official_pair_proactive_hedge_enabled_for_candidate_initial_read": True,
        "routine_certification_research_sequential_fallback_unchanged": True,
        "hedge_delay_ms": float(getattr(self, "hedge_delay_seconds", 0.0) or 0.0) * 1000.0,
        "attempts": int(getattr(self, "_roi_candidate_initial_hedge_attempts", 0) or 0),
        "errors": int(getattr(self, "_roi_candidate_initial_hedge_errors", 0) or 0),
        "winner_counts": dict(_winner_counts(self)),
        "endpoint_cooldown_preserved": True,
        "candidate_remains_noncritical": True,
        "critical_continuity_reserve_unchanged": True,
        "read_only_only": True,
        "signing_or_submission_available": False,
    }
    capacity_status = payload.get("capacity_control")
    if isinstance(capacity_status, dict):
        capacity_status["official_public_proactive_hedge_disabled_for_routine"] = True
        capacity_status["candidate_initial_get_transaction_hedge_exception"] = True
    return payload


setattr(_rpc_status_with_candidate_initial_hedge, "_roi_candidate_initial_transaction_hedge", True)


def _direct_status_with_candidate_initial_hedge(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _PREVIOUS_DIRECT_STATUS is None:
        raise RuntimeError("candidate initial transaction hedge repair is not installed")
    payload = _PREVIOUS_DIRECT_STATUS(self)
    candidate = payload.get("candidate_rpc_priority")
    if isinstance(candidate, dict):
        candidate.update(
            {
                "initial_get_transaction_hedge_enabled": True,
                "initial_get_transaction_hedge_only": True,
                "routine_rpc_hedging_policy_unchanged": True,
            }
        )
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_initial_transaction_hedge_enabled": True,
                "candidate_initial_transaction_hedge_scope": "frozen-scout-initial-getTransaction-only",
                "routine_official_public_proactive_hedge_disabled": True,
                "candidate_rpc_cannot_consume_continuity_reserve": True,
                "rpc_total_endpoint_capacity_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_direct_status_with_candidate_initial_hedge, "_roi_candidate_initial_transaction_hedge", True)


def install_candidate_initial_transaction_hedge_repair() -> None:
    global _PREVIOUS_GET_TRANSACTION_READY, _PREVIOUS_CALL_WITH_META
    global _PREVIOUS_RPC_STATUS, _PREVIOUS_DIRECT_STATUS

    current_ready = DirectSolanaIngestionPlane._get_transaction_ready
    if not bool(getattr(current_ready, "_roi_candidate_initial_transaction_hedge", False)):
        _PREVIOUS_GET_TRANSACTION_READY = current_ready
        try:
            _get_transaction_ready_with_candidate_hedge.__dict__.update(getattr(current_ready, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane._get_transaction_ready = _get_transaction_ready_with_candidate_hedge  # type: ignore[method-assign]

    current_call = SolanaRpcPool.call_with_meta
    if not bool(getattr(current_call, "_roi_candidate_initial_transaction_hedge", False)):
        _PREVIOUS_CALL_WITH_META = current_call
        try:
            _call_with_candidate_initial_hedge.__dict__.update(getattr(current_call, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.call_with_meta = _call_with_candidate_initial_hedge  # type: ignore[method-assign]

    current_rpc_status = SolanaRpcPool.status
    if not bool(getattr(current_rpc_status, "_roi_candidate_initial_transaction_hedge", False)):
        _PREVIOUS_RPC_STATUS = current_rpc_status
        try:
            _rpc_status_with_candidate_initial_hedge.__dict__.update(getattr(current_rpc_status, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.status = _rpc_status_with_candidate_initial_hedge  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_candidate_initial_transaction_hedge", False)):
        _PREVIOUS_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_candidate_initial_hedge.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _direct_status_with_candidate_initial_hedge  # type: ignore[method-assign]


__all__ = [
    "_CANDIDATE_INITIAL_TRANSACTION_HEDGE",
    "_call_with_candidate_initial_hedge",
    "_get_transaction_ready_with_candidate_hedge",
    "install_candidate_initial_transaction_hedge_repair",
]
