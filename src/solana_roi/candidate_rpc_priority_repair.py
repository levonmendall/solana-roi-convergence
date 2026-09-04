from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import candidate_certification_hotpath_repair as hotpath
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane
from .solana_rpc import RpcEndpoint, SolanaRpcPool


WORKLOAD_CANDIDATE = "candidate"
_ORIGINAL_HYDRATE: Callable[..., Any] | None = None
_ORIGINAL_RPC_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_CANDIDATE_WAITERS: dict[tuple[int, str], int] = {}


def _state_key(state: Any) -> tuple[int, str]:
    return int(state.loop_id), str(state.endpoint_key)


def _candidate_waiters(state: Any) -> int:
    return int(_CANDIDATE_WAITERS.get(_state_key(state), 0) or 0)


def _change_candidate_waiters(state: Any, delta: int) -> None:
    key = _state_key(state)
    value = max(0, int(_CANDIDATE_WAITERS.get(key, 0) or 0) + int(delta))
    if value:
        _CANDIDATE_WAITERS[key] = value
    else:
        _CANDIDATE_WAITERS.pop(key, None)


def _noncritical_active(state: Any) -> int:
    return sum(
        int(state.active_by_workload.get(name, 0) or 0)
        for name in (
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
            WORKLOAD_CANDIDATE,
        )
    )


def _allowed_with_candidate_priority(
    state: Any,
    workload: str,
    policy: dict[str, float | int],
) -> tuple[bool, float]:
    """Use the configured 3-slot endpoint budget without sacrificing its reserve.

    The previous governor compared ``active_total`` with the two-slot noncritical
    ceiling. When one critical request was active, one background certification
    request therefore blocked the third configured slot. The reserve is a category
    ceiling, not a lower total-capacity ceiling: up to two noncritical requests may
    coexist with one critical request, while total endpoint concurrency remains 3.

    Frozen-scout work is still noncritical. When a candidate is waiting, at least
    one of those two noncritical slots is withheld from background certification or
    research until the candidate can start. Candidate work never consumes the
    continuity-critical reservation.
    """

    total = int(policy["total_per_endpoint"])
    noncritical_ceiling = int(policy["noncritical_ceiling_per_endpoint"])
    research_max = int(policy["research_max_per_endpoint"])
    now = time.monotonic()

    if workload == governor.WORKLOAD_CRITICAL:
        return state.active_total < total, 0.0

    noncritical_active = _noncritical_active(state)
    if state.active_total >= total or noncritical_active >= noncritical_ceiling:
        return False, 0.02

    if workload != WORKLOAD_CANDIDATE and _candidate_waiters(state) > 0:
        # Keep one noncritical slot available for the latency-critical paper
        # candidate path. This changes scheduling only, never endpoint capacity.
        background_ceiling = max(0, noncritical_ceiling - 1)
        if noncritical_active >= background_ceiling:
            return False, 0.01

    if workload == governor.WORKLOAD_RESEARCH:
        if int(state.active_by_workload.get(governor.WORKLOAD_RESEARCH, 0) or 0) >= research_max:
            return False, 0.05
        interval = float(policy["research_min_interval_seconds"])
        remaining = max(0.0, state.last_research_started_monotonic + interval - now)
        if remaining > 0.0:
            return False, min(0.10, remaining)

    return True, 0.0


async def _acquire_with_candidate_priority(endpoint: RpcEndpoint, workload: str) -> Any:
    state = governor._state_for(endpoint)
    policy = governor._policy()
    candidate_waiting = workload == WORKLOAD_CANDIDATE
    if candidate_waiting:
        _change_candidate_waiters(state, 1)
    registered = candidate_waiting
    try:
        while True:
            sleep_for = 0.02
            async with state.lock:
                allowed, suggested = _allowed_with_candidate_priority(state, workload, policy)
                if allowed:
                    if registered:
                        _change_candidate_waiters(state, -1)
                        registered = False
                    state.active_total += 1
                    state.active_by_workload[workload] = int(state.active_by_workload.get(workload, 0) or 0) + 1
                    state.requests_by_workload[workload] = int(state.requests_by_workload.get(workload, 0) or 0) + 1
                    state.max_active_total = max(state.max_active_total, state.active_total)
                    if workload == governor.WORKLOAD_RESEARCH:
                        state.last_research_started_monotonic = time.monotonic()
                    return state
                state.waits_by_workload[workload] = int(state.waits_by_workload.get(workload, 0) or 0) + 1
                if suggested > 0.0:
                    sleep_for = suggested
            await asyncio.sleep(max(0.005, min(0.10, sleep_for)))
    finally:
        if registered:
            _change_candidate_waiters(state, -1)


async def _hydrate_with_candidate_rpc_priority(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    if _ORIGINAL_HYDRATE is None:
        raise RuntimeError("candidate RPC priority repair is not installed")
    reason = str(row.get("reason") or "")
    try:
        priority = int(row.get("priority") or 0)
    except (TypeError, ValueError):
        priority = 0
    if reason in hotpath.SCOUT_REASONS and priority <= 2:
        with governor.rpc_workload(WORKLOAD_CANDIDATE):
            await _ORIGINAL_HYDRATE(self, row)
        return
    await _ORIGINAL_HYDRATE(self, row)


setattr(_hydrate_with_candidate_rpc_priority, "_roi_candidate_rpc_priority", True)


def _rpc_status_with_candidate_priority(self: SolanaRpcPool) -> dict[str, Any]:
    if _ORIGINAL_RPC_STATUS is None:
        raise RuntimeError("candidate RPC priority repair is not installed")
    payload = _ORIGINAL_RPC_STATUS(self)
    workload = payload.get("workload_governor")
    if isinstance(workload, dict):
        workload["workloads"] = [
            governor.WORKLOAD_CRITICAL,
            WORKLOAD_CANDIDATE,
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
        ]
        workload["candidate_priority_installed"] = True
        workload["candidate_is_noncritical"] = True
        workload["candidate_cannot_consume_critical_reserve"] = True
        workload["noncritical_capacity_accounted_independently_of_active_critical"] = True
        workload["total_per_endpoint_unchanged"] = int(governor._policy()["total_per_endpoint"])
        workload["candidate_waiters"] = sum(
            count
            for (loop_id, endpoint_key), count in _CANDIDATE_WAITERS.items()
            if any(
                endpoint_key == str(endpoint.http_url)
                for endpoint in self.endpoints
            )
        )
        endpoints = workload.get("endpoints")
        if isinstance(endpoints, list):
            for row in endpoints:
                if not isinstance(row, dict):
                    continue
                endpoint_key = None
                host = str(row.get("http_host") or "")
                for endpoint in self.endpoints:
                    if endpoint.http_url.split("/", 3)[2] == host:
                        endpoint_key = str(endpoint.http_url)
                        break
                if endpoint_key is not None:
                    row["candidate_waiters"] = sum(
                        count
                        for (_loop_id, key), count in _CANDIDATE_WAITERS.items()
                        if key == endpoint_key
                    )
    return payload


setattr(_rpc_status_with_candidate_priority, "_roi_candidate_rpc_priority", True)


def _direct_status_with_candidate_rpc_priority(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate RPC priority repair is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    payload["candidate_rpc_priority"] = {
        "installed": True,
        "workload": WORKLOAD_CANDIDATE,
        "candidate_is_noncritical": True,
        "candidate_cannot_consume_critical_reserve": True,
        "candidate_reserves_one_noncritical_slot_while_waiting": True,
        "endpoint_total_concurrency_unchanged": int(governor._policy()["total_per_endpoint"]),
        "endpoint_noncritical_ceiling_unchanged": int(governor._policy()["noncritical_ceiling_per_endpoint"]),
        "candidate_hydration_worker_count_unchanged": int(getattr(self, "worker_count", 0) or 0),
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_rpc_priority_noncritical": True,
                "candidate_rpc_cannot_consume_continuity_reserve": True,
                "rpc_total_endpoint_capacity_unchanged": True,
                "rpc_noncritical_capacity_slot_accounting_repaired": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_direct_status_with_candidate_rpc_priority, "_roi_candidate_rpc_priority", True)


def install_candidate_rpc_priority_repair() -> None:
    global _ORIGINAL_HYDRATE, _ORIGINAL_RPC_STATUS, _ORIGINAL_DIRECT_STATUS

    # _normalize_workload reads this set dynamically, and governor state maps accept
    # additional string keys. No endpoint or configured concurrency is added.
    governor.WORKLOAD_CANDIDATE = WORKLOAD_CANDIDATE  # type: ignore[attr-defined]
    governor._VALID_WORKLOADS = frozenset(set(governor._VALID_WORKLOADS) | {WORKLOAD_CANDIDATE})
    governor._allowed = _allowed_with_candidate_priority  # type: ignore[assignment]
    governor._acquire = _acquire_with_candidate_priority  # type: ignore[assignment]

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_candidate_rpc_priority", False)):
        _ORIGINAL_HYDRATE = current_hydrate
        try:
            _hydrate_with_candidate_rpc_priority.__dict__.update(getattr(current_hydrate, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane._hydrate_one = _hydrate_with_candidate_rpc_priority  # type: ignore[method-assign]

    current_rpc_status = SolanaRpcPool.status
    if not bool(getattr(current_rpc_status, "_roi_candidate_rpc_priority", False)):
        _ORIGINAL_RPC_STATUS = current_rpc_status
        try:
            _rpc_status_with_candidate_priority.__dict__.update(getattr(current_rpc_status, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.status = _rpc_status_with_candidate_priority  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_candidate_rpc_priority", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_candidate_rpc_priority.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _direct_status_with_candidate_rpc_priority  # type: ignore[method-assign]


__all__ = [
    "WORKLOAD_CANDIDATE",
    "_acquire_with_candidate_priority",
    "_allowed_with_candidate_priority",
    "_hydrate_with_candidate_rpc_priority",
    "install_candidate_rpc_priority_repair",
]
