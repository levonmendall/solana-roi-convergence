from __future__ import annotations

import asyncio
import sys
import threading
from typing import Any, Awaitable, Callable

from .solana_rpc import RpcEndpoint, SolanaRpcPool


REPAIR_VERSION = "rpc-endpoint-task-terminal-ownership-v3-captured-capacity-reference"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_CallEndpoint = Callable[[SolanaRpcPool, RpcEndpoint, str, list[Any]], Awaitable[tuple[Any, str, float]]]
_ORIGINAL_CALL_ENDPOINT: _CallEndpoint | None = None
_ORIGINAL_CAPACITY_CALL_ENDPOINT: _CallEndpoint | None = None
_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "installed": False,
    "owned_endpoint_tasks": 0,
    "owned_capacity_root_tasks": 0,
    "captured_capacity_references_rebound": 0,
    "terminal_successes_observed": 0,
    "terminal_failures_observed": 0,
    "terminal_cancellations_observed": 0,
}


def _increment(name: str, amount: int = 1) -> None:
    with _STATE_LOCK:
        _STATE[name] = int(_STATE.get(name, 0) or 0) + int(amount)


def _observe_terminal_state(task: asyncio.Task[Any]) -> None:
    """Retrieve a detached endpoint task's terminal state without changing it.

    Calling ``Task.exception()`` marks a finished exception as observed, but it does
    not alter later ``Task.result()``/``await`` behavior. The normal RPC caller
    therefore still receives the exact same success, failure or cancellation while
    the event loop can no longer emit an orphaned ``Task exception was never
    retrieved`` warning if an outer wrapper/cancellation path loses ownership.
    """

    try:
        exc = task.exception()
    except asyncio.CancelledError:
        _increment("terminal_cancellations_observed")
        return
    except BaseException:
        # ``Task.exception`` should only raise cancellation here, but keep this
        # observer fail-safe: observability must never add execution authority.
        _increment("terminal_failures_observed")
        return
    if exc is None:
        _increment("terminal_successes_observed")
    else:
        _increment("terminal_failures_observed")


def _task_root_is(task: asyncio.Task[Any], callback: Callable[..., Any]) -> bool:
    """Return true only when ``callback`` is the distinct Task's root coroutine."""

    try:
        coro = task.get_coro()
    except BaseException:
        return False
    return getattr(coro, "cr_code", None) is getattr(callback, "__code__", None)


def _claim_current_root_task(callback: Callable[..., Any], *, capacity_root: bool = False) -> None:
    """Attach one terminal observer only to a distinct endpoint child Task.

    A sequential/direct ``await`` executes inside its parent's Task, whose root code
    is not the endpoint wrapper, so no observer is attached to the parent. This
    preserves ordinary exception ownership while making detached hedge/cancellation
    tasks terminally owned even when a late production-capacity composition layer
    becomes the root coroutine.
    """

    task = asyncio.current_task()
    if task is None or not _task_root_is(task, callback):
        return
    if bool(getattr(task, "_roi_endpoint_terminal_observer", False)):
        return
    setattr(task, "_roi_endpoint_terminal_observer", True)
    task.add_done_callback(_observe_terminal_state)
    _increment("owned_endpoint_tasks")
    if capacity_root:
        _increment("owned_capacity_root_tasks")


async def _owned_call_endpoint(
    self: SolanaRpcPool,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> tuple[Any, str, float]:
    _claim_current_root_task(_owned_call_endpoint)

    original = _ORIGINAL_CALL_ENDPOINT
    if original is None:
        raise RuntimeError("RPC endpoint task ownership guard missing delegated endpoint call")
    return await original(self, endpoint, method, params)


setattr(_owned_call_endpoint, "_roi_rpc_task_terminal_ownership", True)


async def _owned_capacity_call_endpoint(
    self: SolanaRpcPool,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> tuple[Any, str, float]:
    """Own a detached Task whose root is the production-capacity wrapper."""

    _claim_current_root_task(_owned_capacity_call_endpoint, capacity_root=True)
    original = _ORIGINAL_CAPACITY_CALL_ENDPOINT
    if original is None:
        raise RuntimeError("RPC capacity task ownership guard missing delegated capacity call")
    return await original(self, endpoint, method, params)


setattr(_owned_capacity_call_endpoint, "_roi_rpc_task_terminal_ownership", True)


def _rebind_loaded_capacity_references(
    original: _CallEndpoint,
    replacement: _CallEndpoint,
) -> int:
    """Replace already-captured module globals that still point at capacity v1.

    Production proved that a late wrapper can report itself active while an earlier
    repair module still holds the exact pre-ownership capacity function object in a
    module-global delegate. A Task created from that stale reference never enters the
    class-level ownership wrapper. Rebind only exact object-identity matches inside
    already-loaded ``solana_roi.*`` modules. Do not inspect closures, instances,
    external packages, or arbitrary callables, and never rewrite this module's own
    original delegate because that delegate is required to preserve call semantics.
    """

    rebound = 0
    for module_name, module in tuple(sys.modules.items()):
        if not module_name.startswith("solana_roi.") or module_name == __name__ or module is None:
            continue
        namespace = getattr(module, "__dict__", None)
        if not isinstance(namespace, dict):
            continue
        for name, value in tuple(namespace.items()):
            if value is not original:
                continue
            namespace[name] = replacement
            rebound += 1
    return rebound


def _install_capacity_root_guard() -> None:
    """Own both the capacity module global and stale loaded module captures."""

    global _ORIGINAL_CAPACITY_CALL_ENDPOINT
    try:
        from . import production_capacity_repair as capacity
    except Exception:
        # The generic ownership wrapper is still valid in compositions that do not
        # include production capacity control.
        return

    current = capacity._capacity_call_endpoint
    if bool(getattr(current, "_roi_rpc_task_terminal_ownership", False)):
        original = _ORIGINAL_CAPACITY_CALL_ENDPOINT
        if original is not None:
            _increment(
                "captured_capacity_references_rebound",
                _rebind_loaded_capacity_references(original, _owned_capacity_call_endpoint),
            )
        return

    _ORIGINAL_CAPACITY_CALL_ENDPOINT = current
    try:
        _owned_capacity_call_endpoint.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_owned_capacity_call_endpoint, "_roi_rpc_task_terminal_ownership", True)
    capacity._capacity_call_endpoint = _owned_capacity_call_endpoint
    _increment(
        "captured_capacity_references_rebound",
        _rebind_loaded_capacity_references(current, _owned_capacity_call_endpoint),
    )


def install_rpc_task_ownership_repair() -> None:
    """Guarantee terminal observation for every distinct endpoint task.

    The final ownership layer covers three production composition shapes:

    1. Capacity is already installed: the generic endpoint wrapper captures it.
    2. Capacity is installed/reinstalled later: the capacity module global points at
       ``_owned_capacity_call_endpoint``.
    3. A previously loaded repair module captured the original capacity function
       object before ownership installation: exact in-package module-global captures
       are rebound to the owned capacity wrapper.

    No retry, provider ordering, timeout, strategy, certification, signing, evidence,
    or live-money behavior is changed.
    """

    global _ORIGINAL_CALL_ENDPOINT
    _install_capacity_root_guard()

    current = SolanaRpcPool._call_endpoint
    if not bool(getattr(current, "_roi_rpc_task_terminal_ownership", False)):
        _ORIGINAL_CALL_ENDPOINT = current
        try:
            _owned_call_endpoint.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        setattr(_owned_call_endpoint, "_roi_rpc_task_terminal_ownership", True)
        SolanaRpcPool._call_endpoint = _owned_call_endpoint  # type: ignore[method-assign]

    with _STATE_LOCK:
        _STATE["installed"] = True


def _active_guard_status() -> tuple[str, bool, str | None, bool | None]:
    current = SolanaRpcPool._call_endpoint
    active_name = str(getattr(current, "__name__", type(current).__name__))
    active_owned = bool(getattr(current, "_roi_rpc_task_terminal_ownership", False))
    try:
        from . import production_capacity_repair as capacity

        capacity_current = capacity._capacity_call_endpoint
        capacity_name = str(getattr(capacity_current, "__name__", type(capacity_current).__name__))
        capacity_owned: bool | None = bool(
            getattr(capacity_current, "_roi_rpc_task_terminal_ownership", False)
        )
    except Exception:
        capacity_name = None
        capacity_owned = None
    return active_name, active_owned, capacity_name, capacity_owned


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    active_name, active_owned, capacity_name, capacity_owned = _active_guard_status()
    return {
        **state,
        "repair_version": REPAIR_VERSION,
        "active_call_endpoint_name": active_name,
        "active_call_endpoint_terminally_owned": active_owned,
        "capacity_call_endpoint_name": capacity_name,
        "capacity_call_endpoint_terminally_owned": capacity_owned,
        "task_terminal_state_retrieved_without_changing_result_semantics": True,
        "sequential_parent_tasks_observed": False,
        "late_capacity_composition_guarded": True,
        "preowned_capacity_module_captures_rebound": True,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "economic_thresholds_changed": ECONOMIC_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


def _reset_state_for_tests() -> None:
    global _ORIGINAL_CALL_ENDPOINT
    _ORIGINAL_CALL_ENDPOINT = None
    # Do not clear the capacity delegate here. The composed regression process may
    # already have installed the capacity-root guard globally; clearing its delegate
    # would poison unrelated tests after a fixture restores the class method.
    with _STATE_LOCK:
        _STATE.update(
            {
                "installed": False,
                "owned_endpoint_tasks": 0,
                "owned_capacity_root_tasks": 0,
                "captured_capacity_references_rebound": 0,
                "terminal_successes_observed": 0,
                "terminal_failures_observed": 0,
                "terminal_cancellations_observed": 0,
            }
        )


__all__ = [
    "REPAIR_VERSION",
    "install_rpc_task_ownership_repair",
    "status",
]
