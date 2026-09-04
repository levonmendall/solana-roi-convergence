from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import candidate_rpc_priority_repair as candidate
from . import continuity_high_volume_poll_affinity_repair as affinity
from . import continuity_storage_capacity_repair as storage
from . import live_poll_redundancy as live_poll
from . import poll_watermark_repair as watermark
from . import rpc_workload_governor as governor
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .solana_rpc import RpcEndpoint, SolanaRpcPool


WORKLOAD_STANDBY = "standby"
_ORIGINAL_SHARDED_PAGE: Callable[..., Any] | None = None
_ORIGINAL_RPC_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_STANDBY_WAITERS: dict[tuple[int, str], int] = {}


def _state_key(state: Any) -> tuple[int, str]:
    return int(state.loop_id), str(state.endpoint_key)


def _standby_waiters(state: Any) -> int:
    return int(_STANDBY_WAITERS.get(_state_key(state), 0) or 0)


def _change_standby_waiters(state: Any, delta: int) -> None:
    key = _state_key(state)
    value = max(0, int(_STANDBY_WAITERS.get(key, 0) or 0) + int(delta))
    if value:
        _STANDBY_WAITERS[key] = value
    else:
        _STANDBY_WAITERS.pop(key, None)


def _noncritical_active(state: Any) -> int:
    return sum(
        int(state.active_by_workload.get(name, 0) or 0)
        for name in (
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
            candidate.WORKLOAD_CANDIDATE,
            WORKLOAD_STANDBY,
        )
    )


def _allowed_with_standby_priority(
    state: Any,
    workload: str,
    policy: dict[str, float | int],
) -> tuple[bool, float]:
    """Keep standby cursors current without consuming the critical reservation.

    Production on release 8ae1cebd proved high-volume routine poll reads were still
    classified as ordinary certification work. PublicNode accumulated more than
    125k certification wait iterations while PUMP_FUN/PUMP_AMM standby cursors did
    not advance after startup. Once stale, the unchanged 3x1000 recovery window can
    no longer bridge a real WebSocket loss.

    Standby work remains noncritical and shares the existing two noncritical slots.
    A waiting candidate still has first claim on one of those slots. When no
    candidate is waiting, a waiting standby read withholds one slot from background
    certification/research so the fixed four-second standby cadence can actually
    execute. Endpoint concurrency and the one-slot critical reserve are unchanged.
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

    candidate_waiters = candidate._candidate_waiters(state)
    standby_waiters = _standby_waiters(state)

    # Candidate latency remains ahead of standby maintenance. If a candidate is
    # queued, standby/background may occupy at most one noncritical slot.
    if workload != candidate.WORKLOAD_CANDIDATE and candidate_waiters > 0:
        if noncritical_active >= max(0, noncritical_ceiling - 1):
            return False, 0.01

    # In the absence of a waiting candidate, reserve one noncritical slot for a
    # stale high-volume standby poll ahead of ordinary certification/research.
    if (
        workload not in {candidate.WORKLOAD_CANDIDATE, WORKLOAD_STANDBY}
        and candidate_waiters <= 0
        and standby_waiters > 0
    ):
        if noncritical_active >= max(0, noncritical_ceiling - 1):
            return False, 0.01

    if workload == governor.WORKLOAD_RESEARCH:
        if int(state.active_by_workload.get(governor.WORKLOAD_RESEARCH, 0) or 0) >= research_max:
            return False, 0.05
        interval = float(policy["research_min_interval_seconds"])
        remaining = max(0.0, state.last_research_started_monotonic + interval - now)
        if remaining > 0.0:
            return False, min(0.10, remaining)

    return True, 0.0


async def _acquire_with_standby_priority(endpoint: RpcEndpoint, workload: str) -> Any:
    state = governor._state_for(endpoint)
    policy = governor._policy()
    candidate_waiting = workload == candidate.WORKLOAD_CANDIDATE
    standby_waiting = workload == WORKLOAD_STANDBY

    if candidate_waiting:
        candidate._change_candidate_waiters(state, 1)
    if standby_waiting:
        _change_standby_waiters(state, 1)
    candidate_registered = candidate_waiting
    standby_registered = standby_waiting

    try:
        while True:
            sleep_for = 0.02
            async with state.lock:
                allowed, suggested = _allowed_with_standby_priority(state, workload, policy)
                if allowed:
                    if candidate_registered:
                        candidate._change_candidate_waiters(state, -1)
                        candidate_registered = False
                    if standby_registered:
                        _change_standby_waiters(state, -1)
                        standby_registered = False
                    state.active_total += 1
                    state.active_by_workload[workload] = int(
                        state.active_by_workload.get(workload, 0) or 0
                    ) + 1
                    state.requests_by_workload[workload] = int(
                        state.requests_by_workload.get(workload, 0) or 0
                    ) + 1
                    state.max_active_total = max(state.max_active_total, state.active_total)
                    if workload == governor.WORKLOAD_RESEARCH:
                        state.last_research_started_monotonic = time.monotonic()
                    return state
                state.waits_by_workload[workload] = int(
                    state.waits_by_workload.get(workload, 0) or 0
                ) + 1
                if suggested > 0.0:
                    sleep_for = suggested
            await asyncio.sleep(max(0.005, min(0.10, sleep_for)))
    finally:
        if candidate_registered:
            candidate._change_candidate_waiters(state, -1)
        if standby_registered:
            _change_standby_waiters(state, -1)


async def _sharded_page_with_standby_priority(
    self: Any,
    target: WatchTarget,
    *,
    before: str | None = None,
    min_context_slot: int | None = None,
    limit: int,
) -> tuple[list[dict[str, Any]], str | None, float | None]:
    if _ORIGINAL_SHARDED_PAGE is None:
        raise RuntimeError("continuity standby RPC priority repair is not installed")
    if affinity._is_high_volume_target(target):
        with governor.rpc_workload(WORKLOAD_STANDBY):
            return await _ORIGINAL_SHARDED_PAGE(
                self,
                target,
                before=before,
                min_context_slot=min_context_slot,
                limit=limit,
            )
    return await _ORIGINAL_SHARDED_PAGE(
        self,
        target,
        before=before,
        min_context_slot=min_context_slot,
        limit=limit,
    )


setattr(_sharded_page_with_standby_priority, "_roi_high_volume_standby_priority", True)


def _rpc_status_with_standby_priority(self: SolanaRpcPool) -> dict[str, Any]:
    if _ORIGINAL_RPC_STATUS is None:
        raise RuntimeError("continuity standby RPC priority repair is not installed")
    payload = _ORIGINAL_RPC_STATUS(self)
    workload = payload.get("workload_governor")
    if isinstance(workload, dict):
        workload["workloads"] = [
            governor.WORKLOAD_CRITICAL,
            candidate.WORKLOAD_CANDIDATE,
            WORKLOAD_STANDBY,
            governor.WORKLOAD_CERTIFICATION,
            governor.WORKLOAD_RESEARCH,
        ]
        workload["high_volume_standby_priority_installed"] = True
        workload["standby_is_noncritical"] = True
        workload["standby_cannot_consume_critical_reserve"] = True
        workload["candidate_priority_over_standby"] = True
        workload["standby_reserves_one_noncritical_slot_while_waiting"] = True
        workload["standby_waiters"] = sum(
            count
            for (_loop_id, endpoint_key), count in _STANDBY_WAITERS.items()
            if any(endpoint_key == str(endpoint.http_url) for endpoint in self.endpoints)
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
                    row["standby_waiters"] = sum(
                        count
                        for (_loop_id, key), count in _STANDBY_WAITERS.items()
                        if key == endpoint_key
                    )
    return payload


setattr(_rpc_status_with_standby_priority, "_roi_high_volume_standby_priority", True)


def _direct_status_with_standby_priority(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("continuity standby RPC priority repair is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    payload["high_volume_standby_rpc_priority"] = {
        "installed": True,
        "workload": WORKLOAD_STANDBY,
        "sources": sorted(affinity.HIGH_VOLUME_ROUTINE_SOURCES),
        "standby_is_noncritical": True,
        "standby_cannot_consume_critical_reserve": True,
        "candidate_priority_over_standby": True,
        "endpoint_total_concurrency_unchanged": int(governor._policy()["total_per_endpoint"]),
        "endpoint_noncritical_ceiling_unchanged": int(
            governor._policy()["noncritical_ceiling_per_endpoint"]
        ),
        "poll_interval_seconds_unchanged": live_poll.POLL_INTERVAL_SECONDS,
        "hard_page_limit_unchanged": live_poll.POLL_CURSOR_MAX_PAGES,
        "hard_page_size_unchanged": live_poll.POLL_LIMIT,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "high_volume_standby_rpc_priority_noncritical": True,
                "high_volume_standby_cannot_consume_continuity_reserve": True,
                "candidate_rpc_priority_preserved_over_standby": True,
                "routine_poll_interval_unchanged": True,
                "live_poll_hard_delta_bound_unchanged": True,
                "rpc_total_endpoint_capacity_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_direct_status_with_standby_priority, "_roi_high_volume_standby_priority", True)


def install_continuity_standby_rpc_priority_repair() -> None:
    global _ORIGINAL_SHARDED_PAGE, _ORIGINAL_RPC_STATUS, _ORIGINAL_DIRECT_STATUS

    governor.WORKLOAD_STANDBY = WORKLOAD_STANDBY  # type: ignore[attr-defined]
    governor._VALID_WORKLOADS = frozenset(set(governor._VALID_WORKLOADS) | {WORKLOAD_STANDBY})
    governor._allowed = _allowed_with_standby_priority  # type: ignore[assignment]
    governor._acquire = _acquire_with_standby_priority  # type: ignore[assignment]

    current_page = watermark._slot_poll_page
    if not bool(getattr(current_page, "_roi_high_volume_standby_priority", False)):
        _ORIGINAL_SHARDED_PAGE = current_page
        try:
            _sharded_page_with_standby_priority.__dict__.update(getattr(current_page, "__dict__", {}))
        except Exception:
            pass
        setattr(_sharded_page_with_standby_priority, "_roi_high_volume_standby_priority", True)
        storage._sharded_slot_poll_page = _sharded_page_with_standby_priority  # type: ignore[assignment]
        watermark._slot_poll_page = _sharded_page_with_standby_priority  # type: ignore[assignment]
        live_poll._poll_page = _sharded_page_with_standby_priority  # type: ignore[assignment]

    current_rpc_status = SolanaRpcPool.status
    if not bool(getattr(current_rpc_status, "_roi_high_volume_standby_priority", False)):
        _ORIGINAL_RPC_STATUS = current_rpc_status
        try:
            _rpc_status_with_standby_priority.__dict__.update(getattr(current_rpc_status, "__dict__", {}))
        except Exception:
            pass
        SolanaRpcPool.status = _rpc_status_with_standby_priority  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_high_volume_standby_priority", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_standby_priority.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _direct_status_with_standby_priority  # type: ignore[method-assign]


__all__ = [
    "WORKLOAD_STANDBY",
    "_acquire_with_standby_priority",
    "_allowed_with_standby_priority",
    "_sharded_page_with_standby_priority",
    "install_continuity_standby_rpc_priority_repair",
]
