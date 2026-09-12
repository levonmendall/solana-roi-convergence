from __future__ import annotations

"""Serialize logical-bootstrap page lifecycles through post-send cache cleanup.

Production telemetry proved that successful bounded pages can refault hundreds of MiB
of clean SQLite file cache while a large reclaimable anonymous-heap baseline remains.
The response reaches the certifier before Starlette runs the existing post-response
cache cleanup, so the next request can enter the unchanged 94% raw-cgroup guard while
the prior page's cache is still resident and fail closed again.

The repair deliberately does *not* move cleanup before response serialization/send.
Instead, one page owns an asyncio gate from route entry until the already-registered
post-send cleanup has completed. A following page can arrive immediately, but it may
not start SQLite/guard work until that cleanup releases the gate. This preserves the
ASGI memory-lifecycle contract, pagination, historical identity, the 94% guard, and all
paper-only/certification authority.
"""

import asyncio
from functools import wraps
from typing import Any, Callable

from fastapi import BackgroundTasks, HTTPException
from starlette.background import BackgroundTask

REPAIR_VERSION = "logical-bootstrap-page-lifecycle-gate-v2"
PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"
PAGE_GATE_WAIT_SECONDS = 30.0

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False
RAW_CRITICAL_FRACTION_CHANGED = False

_INSTALLED = False
_LAST_STATE: dict[str, Any] | None = None


def _route(app: Any) -> Any:
    route = next(
        (candidate for candidate in app.routes if getattr(candidate, "path", None) == PAGE_PATH),
        None,
    )
    dependant = getattr(route, "dependant", None) if route is not None else None
    if route is None or dependant is None or not callable(getattr(dependant, "call", None)):
        raise RuntimeError(f"certification bootstrap page route unavailable: {PAGE_PATH}")
    return route


def _release_gate(gate: asyncio.Lock, state: dict[str, Any], generation: int) -> None:
    if gate.locked():
        gate.release()
    if int(state.get("generation", 0)) == generation:
        state["active"] = False
    state["releases"] = int(state.get("releases", 0)) + 1


async def _complete_page_lifecycle(
    tasks: tuple[BackgroundTask, ...],
    gate: asyncio.Lock,
    state: dict[str, Any],
    generation: int,
) -> None:
    """Run the original post-send tasks in order and always unblock the next page."""

    try:
        for task in tasks:
            await task()
    finally:
        # A cleanup failure is still allowed to propagate after the response, but it
        # must not strand the lifecycle gate forever. The unchanged next-page raw
        # cgroup guard remains the fail-closed authority if cache pressure persists.
        _release_gate(gate, state, generation)


def install_logical_bootstrap_page_cache_repair(app: Any) -> None:
    """Install one app-scoped lifecycle gate around the fully composed page route."""

    global _INSTALLED, _LAST_STATE
    marker = "roi_logical_bootstrap_page_lifecycle_gate"
    if bool(getattr(app.state, marker, False)):
        return

    route = _route(app)
    original_page: Callable[..., Any] = route.dependant.call
    if bool(getattr(original_page, "_roi_logical_bootstrap_page_lifecycle_gate", False)):
        setattr(app.state, marker, True)
        return

    gate = asyncio.Lock()
    state: dict[str, Any] = {
        "gate": gate,
        "active": False,
        "generation": 0,
        "acquisitions": 0,
        "releases": 0,
        "timeouts": 0,
        "cancelled_waiters": 0,
    }

    @wraps(original_page)
    async def page_with_lifecycle_gate(
        background_tasks: BackgroundTasks,
        table: str,
        epoch: str,
        schema_fingerprint: str,
        cursor: str | None = None,
        limit: int = 250,
        x_certification_token: str | None = None,
    ) -> Any:
        try:
            await asyncio.wait_for(gate.acquire(), timeout=PAGE_GATE_WAIT_SECONDS)
        except TimeoutError as exc:
            state["timeouts"] = int(state.get("timeouts", 0)) + 1
            raise HTTPException(
                status_code=503,
                detail="certification logical bootstrap deferred: previous page cleanup still active",
            ) from exc
        except asyncio.CancelledError:
            state["cancelled_waiters"] = int(state.get("cancelled_waiters", 0)) + 1
            raise

        state["generation"] = int(state.get("generation", 0)) + 1
        generation = int(state["generation"])
        state["active"] = True
        state["acquisitions"] = int(state.get("acquisitions", 0)) + 1
        tasks_before = len(background_tasks.tasks)

        try:
            payload = await original_page(
                background_tasks=background_tasks,
                table=table,
                epoch=epoch,
                schema_fingerprint=schema_fingerprint,
                cursor=cursor,
                limit=limit,
                x_certification_token=x_certification_token,
            )
        except BaseException:
            # The existing logical route performs immediate off-loop cache cleanup on
            # failed page construction because no successful response will be sent.
            # Once that call unwinds it is safe to let a retry enter the gate.
            _release_gate(gate, state, generation)
            raise

        appended = tuple(background_tasks.tasks[tasks_before:])
        if not appended:
            _release_gate(gate, state, generation)
            raise HTTPException(
                status_code=503,
                detail="certification logical bootstrap deferred: post-response cleanup unavailable",
            )

        # Preserve exact post-send timing and task order. Starlette runs BackgroundTasks
        # only after the final response body is sent. Replacing just the tasks appended
        # by this page with one composite makes gate release a finally-action *after*
        # those original tasks, even when cleanup itself raises.
        background_tasks.tasks[tasks_before:] = [
            BackgroundTask(_complete_page_lifecycle, appended, gate, state, generation)
        ]
        return payload

    setattr(page_with_lifecycle_gate, "_roi_logical_bootstrap_page_lifecycle_gate", True)
    setattr(page_with_lifecycle_gate, "_roi_original_endpoint", original_page)
    route.endpoint = page_with_lifecycle_gate
    route.dependant.call = page_with_lifecycle_gate

    app.state.roi_logical_bootstrap_page_lifecycle_gate = True
    app.state.roi_logical_bootstrap_page_lifecycle_gate_version = REPAIR_VERSION
    app.state.roi_logical_bootstrap_page_lifecycle_gate_state = state
    _LAST_STATE = state
    _INSTALLED = True


def status(app: Any | None = None) -> dict[str, Any]:
    state = None
    if app is not None:
        state = getattr(app.state, "roi_logical_bootstrap_page_lifecycle_gate_state", None)
    if state is None:
        state = _LAST_STATE
    return {
        "repair_version": REPAIR_VERSION,
        "installed": bool(
            _INSTALLED
            if app is None
            else getattr(app.state, "roi_logical_bootstrap_page_lifecycle_gate", False)
        ),
        "scope": "logical_bootstrap_page_through_post_send_cleanup",
        "post_response_cleanup_preserved": True,
        "pre_response_cleanup_added": False,
        "next_page_waits_for_prior_cleanup": True,
        "bounded_wait_seconds": PAGE_GATE_WAIT_SECONDS,
        "active": bool(state.get("active", False)) if isinstance(state, dict) else False,
        "acquisitions": int(state.get("acquisitions", 0)) if isinstance(state, dict) else 0,
        "releases": int(state.get("releases", 0)) if isinstance(state, dict) else 0,
        "timeouts": int(state.get("timeouts", 0)) if isinstance(state, dict) else 0,
        "cancelled_waiters": int(state.get("cancelled_waiters", 0)) if isinstance(state, dict) else 0,
        "raw_critical_fraction_changed": RAW_CRITICAL_FRACTION_CHANGED,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "continuity_semantics_changed": CONTINUITY_SEMANTICS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "install_logical_bootstrap_page_cache_repair",
    "status",
]
