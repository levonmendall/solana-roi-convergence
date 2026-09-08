from __future__ import annotations

import asyncio
import threading
from typing import Any, Awaitable, Callable

from .solana_rpc import RpcEndpoint, SolanaRpcPool


REPAIR_VERSION = "rpc-endpoint-task-terminal-ownership-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_CallEndpoint = Callable[[SolanaRpcPool, RpcEndpoint, str, list[Any]], Awaitable[tuple[Any, str, float]]]
_ORIGINAL_CALL_ENDPOINT: _CallEndpoint | None = None
_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "installed": False,
    "owned_endpoint_tasks": 0,
    "terminal_successes_observed": 0,
    "terminal_failures_observed": 0,
    "terminal_cancellations_observed": 0,
}


def _increment(name: str) -> None:
    with _STATE_LOCK:
        _STATE[name] = int(_STATE.get(name, 0) or 0) + 1


def _observe_terminal_state(task: asyncio.Task[Any]) -> None:
    """Retrieve a detached endpoint task's terminal state without changing it.

    Calling ``Task.exception()`` marks a finished exception as observed, but it does
    not alter later ``Task.result()``/``await`` behavior.  The normal RPC caller
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


def _current_task_is_endpoint_task(task: asyncio.Task[Any]) -> bool:
    """True only when this wrapper itself is the Task's root coroutine.

    Sequential RPC calls merely ``await self._call_endpoint(...)`` inside their
    existing owner task.  We do not attach observers to those parent tasks.  Hedged
    RPC calls create a distinct task whose root coroutine is this endpoint wrapper;
    those are the tasks that require an independent terminal-state ownership guard.
    """

    try:
        coro = task.get_coro()
    except BaseException:
        return False
    return getattr(coro, "cr_code", None) is _owned_call_endpoint.__code__


async def _owned_call_endpoint(
    self: SolanaRpcPool,
    endpoint: RpcEndpoint,
    method: str,
    params: list[Any],
) -> tuple[Any, str, float]:
    task = asyncio.current_task()
    if task is not None and _current_task_is_endpoint_task(task):
        if not bool(getattr(task, "_roi_endpoint_terminal_observer", False)):
            setattr(task, "_roi_endpoint_terminal_observer", True)
            task.add_done_callback(_observe_terminal_state)
            _increment("owned_endpoint_tasks")

    original = _ORIGINAL_CALL_ENDPOINT
    if original is None:
        raise RuntimeError("RPC endpoint task ownership guard missing delegated endpoint call")
    return await original(self, endpoint, method, params)


setattr(_owned_call_endpoint, "_roi_rpc_task_terminal_ownership", True)


def install_rpc_task_ownership_repair() -> None:
    """Guarantee terminal observation for every distinct hedged endpoint task.

    The installer captures the *current* endpoint implementation at install time so
    it composes after capacity/cooldown wrappers instead of bypassing them.  It is
    intentionally safe to call again if another production composition layer has
    replaced ``_call_endpoint`` since an earlier test/import.
    """

    global _ORIGINAL_CALL_ENDPOINT
    current = SolanaRpcPool._call_endpoint
    if bool(getattr(current, "_roi_rpc_task_terminal_ownership", False)):
        with _STATE_LOCK:
            _STATE["installed"] = True
        return

    _ORIGINAL_CALL_ENDPOINT = current
    try:
        _owned_call_endpoint.__dict__.update(getattr(current, "__dict__", {}))
    except Exception:
        pass
    setattr(_owned_call_endpoint, "_roi_rpc_task_terminal_ownership", True)
    SolanaRpcPool._call_endpoint = _owned_call_endpoint  # type: ignore[method-assign]
    with _STATE_LOCK:
        _STATE["installed"] = True


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    return {
        **state,
        "repair_version": REPAIR_VERSION,
        "task_terminal_state_retrieved_without_changing_result_semantics": True,
        "sequential_parent_tasks_observed": False,
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
    with _STATE_LOCK:
        _STATE.update(
            {
                "installed": False,
                "owned_endpoint_tasks": 0,
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
