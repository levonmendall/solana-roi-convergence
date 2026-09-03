from __future__ import annotations

import asyncio
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from .solana_rpc import RpcEndpoint, SolanaRpcPool


WORKLOAD_CRITICAL = "critical"
WORKLOAD_CERTIFICATION = "certification"
WORKLOAD_RESEARCH = "research"
_VALID_WORKLOADS = frozenset({WORKLOAD_CRITICAL, WORKLOAD_CERTIFICATION, WORKLOAD_RESEARCH})

_DEFAULT_TOTAL_PER_ENDPOINT = 3
_DEFAULT_CRITICAL_RESERVED_PER_ENDPOINT = 1
_DEFAULT_RESEARCH_MAX_PER_ENDPOINT = 1
_DEFAULT_RESEARCH_MIN_INTERVAL_SECONDS = 1.0

_RPC_WORKLOAD: ContextVar[str] = ContextVar(
    "solana_roi_rpc_workload",
    default=WORKLOAD_CERTIFICATION,
)


@dataclass(slots=True)
class _EndpointGovernorState:
    loop_id: int
    endpoint_key: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_total: int = 0
    active_by_workload: dict[str, int] = field(
        default_factory=lambda: {
            WORKLOAD_CRITICAL: 0,
            WORKLOAD_CERTIFICATION: 0,
            WORKLOAD_RESEARCH: 0,
        }
    )
    requests_by_workload: dict[str, int] = field(
        default_factory=lambda: {
            WORKLOAD_CRITICAL: 0,
            WORKLOAD_CERTIFICATION: 0,
            WORKLOAD_RESEARCH: 0,
        }
    )
    waits_by_workload: dict[str, int] = field(
        default_factory=lambda: {
            WORKLOAD_CRITICAL: 0,
            WORKLOAD_CERTIFICATION: 0,
            WORKLOAD_RESEARCH: 0,
        }
    )
    max_active_total: int = 0
    last_research_started_monotonic: float = 0.0


_STATES: dict[tuple[int, str], _EndpointGovernorState] = {}
_ORIGINAL_CALL_ENDPOINT: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _env_int(name: str, default: int, *, minimum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _policy() -> dict[str, float | int]:
    total = _env_int(
        "SOLANA_ROI_RPC_GOVERNOR_TOTAL_PER_ENDPOINT",
        _DEFAULT_TOTAL_PER_ENDPOINT,
        minimum=3,
    )
    critical_reserved = _env_int(
        "SOLANA_ROI_RPC_GOVERNOR_CRITICAL_RESERVED_PER_ENDPOINT",
        _DEFAULT_CRITICAL_RESERVED_PER_ENDPOINT,
        minimum=1,
    )
    critical_reserved = min(critical_reserved, total - 2)
    research_max = _env_int(
        "SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MAX_PER_ENDPOINT",
        _DEFAULT_RESEARCH_MAX_PER_ENDPOINT,
        minimum=1,
    )
    research_max = min(research_max, max(1, total - critical_reserved - 1))
    research_interval = _env_float(
        "SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MIN_INTERVAL_SECONDS",
        _DEFAULT_RESEARCH_MIN_INTERVAL_SECONDS,
        minimum=0.0,
    )
    return {
        "total_per_endpoint": total,
        "critical_reserved_per_endpoint": critical_reserved,
        "noncritical_ceiling_per_endpoint": total - critical_reserved,
        "research_max_per_endpoint": research_max,
        "research_min_interval_seconds": research_interval,
    }


def _normalize_workload(value: str | None) -> str:
    text = str(value or "").strip().lower()
    return text if text in _VALID_WORKLOADS else WORKLOAD_CERTIFICATION


def current_rpc_workload() -> str:
    return _normalize_workload(_RPC_WORKLOAD.get())


@contextmanager
def rpc_workload(workload: str) -> Iterator[None]:
    token = _RPC_WORKLOAD.set(_normalize_workload(workload))
    try:
        yield
    finally:
        _RPC_WORKLOAD.reset(token)


def _endpoint_key(endpoint: RpcEndpoint) -> str:
    return str(endpoint.http_url)


def _state_for(endpoint: RpcEndpoint) -> _EndpointGovernorState:
    loop = asyncio.get_running_loop()
    key = (id(loop), _endpoint_key(endpoint))
    state = _STATES.get(key)
    if state is None:
        state = _EndpointGovernorState(loop_id=id(loop), endpoint_key=key[1])
        _STATES[key] = state
    return state


def _task_implied_workload() -> str | None:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    name = task.get_name()
    if name.startswith("isolated-immediate-gap-recovery:"):
        return WORKLOAD_CRITICAL
    return None


def _effective_workload() -> str:
    implied = _task_implied_workload()
    if implied is not None:
        return implied
    return current_rpc_workload()


def _allowed(state: _EndpointGovernorState, workload: str, policy: dict[str, float | int]) -> tuple[bool, float]:
    total = int(policy["total_per_endpoint"])
    noncritical_ceiling = int(policy["noncritical_ceiling_per_endpoint"])
    research_max = int(policy["research_max_per_endpoint"])
    now = time.monotonic()

    if workload == WORKLOAD_CRITICAL:
        return state.active_total < total, 0.0

    if state.active_total >= noncritical_ceiling:
        return False, 0.02

    if workload == WORKLOAD_RESEARCH:
        if int(state.active_by_workload.get(WORKLOAD_RESEARCH, 0)) >= research_max:
            return False, 0.05
        interval = float(policy["research_min_interval_seconds"])
        remaining = max(0.0, state.last_research_started_monotonic + interval - now)
        if remaining > 0.0:
            return False, min(0.10, remaining)

    return True, 0.0


async def _acquire(endpoint: RpcEndpoint, workload: str) -> _EndpointGovernorState:
    state = _state_for(endpoint)
    policy = _policy()
    while True:
        sleep_for = 0.02
        async with state.lock:
            allowed, suggested = _allowed(state, workload, policy)
            if allowed:
                state.active_total += 1
                state.active_by_workload[workload] = int(state.active_by_workload.get(workload, 0)) + 1
                state.requests_by_workload[workload] = int(state.requests_by_workload.get(workload, 0)) + 1
                state.max_active_total = max(state.max_active_total, state.active_total)
                if workload == WORKLOAD_RESEARCH:
                    state.last_research_started_monotonic = time.monotonic()
                return state
            state.waits_by_workload[workload] = int(state.waits_by_workload.get(workload, 0)) + 1
            if suggested > 0.0:
                sleep_for = suggested
        await asyncio.sleep(max(0.005, min(0.10, sleep_for)))


async def _release(state: _EndpointGovernorState, workload: str) -> None:
    async with state.lock:
        state.active_total = max(0, state.active_total - 1)
        state.active_by_workload[workload] = max(
            0,
            int(state.active_by_workload.get(workload, 0)) - 1,
        )


async def _governed_call_endpoint(
    self: SolanaRpcPool,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> tuple[Any, str, float]:
    if _ORIGINAL_CALL_ENDPOINT is None:
        raise RuntimeError("RPC workload governor is not installed")
    workload = _effective_workload()
    state = await _acquire(endpoint, workload)
    try:
        return await _ORIGINAL_CALL_ENDPOINT(self, endpoint, method, params)
    finally:
        await _release(state, workload)


def _aggregate_endpoint_snapshot(endpoint: RpcEndpoint) -> dict[str, Any]:
    key = _endpoint_key(endpoint)
    matching = [state for (_loop_id, endpoint_key), state in _STATES.items() if endpoint_key == key]
    requests = {name: 0 for name in _VALID_WORKLOADS}
    waits = {name: 0 for name in _VALID_WORKLOADS}
    active = {name: 0 for name in _VALID_WORKLOADS}
    max_active = 0
    for state in matching:
        max_active = max(max_active, int(state.max_active_total))
        for name in _VALID_WORKLOADS:
            requests[name] += int(state.requests_by_workload.get(name, 0))
            waits[name] += int(state.waits_by_workload.get(name, 0))
            active[name] += int(state.active_by_workload.get(name, 0))
    return {
        "name": endpoint.name,
        "http_host": endpoint.http_url.split("/", 3)[2],
        "active_by_workload": active,
        "requests_by_workload": requests,
        "wait_iterations_by_workload": waits,
        "max_active_total": max_active,
    }


def _governed_status(self: SolanaRpcPool) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("RPC workload governor is not installed")
    payload = _ORIGINAL_STATUS(self)
    policy = _policy()
    payload["workload_governor"] = {
        "installed": True,
        "scope": "process-wide-across-all-SolanaRpcPool-instances",
        "workloads": [WORKLOAD_CRITICAL, WORKLOAD_CERTIFICATION, WORKLOAD_RESEARCH],
        **policy,
        "critical_capacity_reserved": True,
        "research_cannot_consume_critical_reserve": True,
        "research_is_background_rate_limited": True,
        "read_only_only": True,
        "signing_or_submission_available": False,
        "endpoints": [_aggregate_endpoint_snapshot(endpoint) for endpoint in self.endpoints],
    }
    return payload


def install_rpc_workload_governor() -> None:
    global _ORIGINAL_CALL_ENDPOINT, _ORIGINAL_STATUS

    current_call = SolanaRpcPool._call_endpoint
    if not bool(getattr(current_call, "_roi_rpc_workload_governor", False)):
        _ORIGINAL_CALL_ENDPOINT = current_call
        try:
            _governed_call_endpoint.__dict__.update(getattr(current_call, "__dict__", {}))
        except Exception:
            pass
        setattr(_governed_call_endpoint, "_roi_rpc_workload_governor", True)
        SolanaRpcPool._call_endpoint = _governed_call_endpoint  # type: ignore[method-assign]

    current_status = SolanaRpcPool.status
    if not bool(getattr(current_status, "_roi_rpc_workload_governor", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _governed_status.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_governed_status, "_roi_rpc_workload_governor", True)
        SolanaRpcPool.status = _governed_status  # type: ignore[method-assign]


__all__ = [
    "WORKLOAD_CERTIFICATION",
    "WORKLOAD_CRITICAL",
    "WORKLOAD_RESEARCH",
    "current_rpc_workload",
    "install_rpc_workload_governor",
    "rpc_workload",
]
