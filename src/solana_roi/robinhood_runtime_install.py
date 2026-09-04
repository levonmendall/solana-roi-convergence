from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap
from .robinhood_chain_paper import RobinhoodChainPaperPlane


_ORIGINAL_RUNTIME_WORKERS: Callable[[Any, asyncio.Event], Any] | None = None
_PLANE: RobinhoodChainPaperPlane | None = None
_STARTUP_ERROR: str | None = None
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "attempts": 0,
}


async def _runtime_workers_with_robinhood(runtime: Any, stop: asyncio.Event) -> None:
    """Run Robinhood beside canonical workers after durable runtime bootstrap.

    The canonical `_render_handoff_lifespan` and its exact object identity are left
    untouched. This function is reached only after that bootstrap has acquired the
    persistent SQLite runtime, so Robinhood cannot delay Render liveness or compete
    with the blue/green handoff for initial database ownership.
    """
    global _PLANE, _STARTUP_ERROR
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("Robinhood production worker composition is not installed")

    _STATE["attempts"] = int(_STATE["attempts"]) + 1
    robinhood_task: asyncio.Task[None] | None = None
    try:
        try:
            _PLANE = RobinhoodChainPaperPlane(runtime.store)
            _STARTUP_ERROR = None
            _STATE["state"] = "running" if _PLANE.enabled else "disabled"
            if _PLANE.enabled:
                robinhood_task = asyncio.create_task(
                    _PLANE.run(stop),
                    name="robinhood-chain-paper",
                )
        except Exception as exc:
            # Robinhood is additive. Initialization failure must never terminate
            # the existing Solana/FOMO workers; only Robinhood fails closed.
            _PLANE = None
            _STARTUP_ERROR = f"{type(exc).__name__}: {exc}"
            _STATE["state"] = "failed_closed"

        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        if robinhood_task is not None:
            if not stop.is_set():
                robinhood_task.cancel()
            with suppress(asyncio.CancelledError):
                await robinhood_task
        if _PLANE is not None:
            with suppress(Exception):
                await _PLANE.close()
        if stop.is_set():
            _STATE["state"] = "stopped"


def _status() -> dict[str, Any]:
    if _PLANE is not None:
        return {**_PLANE.status(), "runtime_ready": True, "production_install": dict(_STATE)}
    return {
        "enabled": True,
        "chain": "ROBINHOOD_CHAIN",
        "chain_id": 4663,
        "paper_only": True,
        "paper_trading_authority": False,
        "shadow_only": False,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "runtime_ready": False,
        "failed_closed": True,
        "error": _STARTUP_ERROR or "canonical_runtime_not_ready",
        "production_install": dict(_STATE),
    }


def install_robinhood_chain_paper_runtime(app: Any, runtime_provider: Callable[[], Any]) -> None:
    """Add Robinhood worker/telemetry without replacing canonical ASGI lifespan."""
    del runtime_provider  # canonical bootstrap passes the ready runtime to workers
    global _ORIGINAL_RUNTIME_WORKERS
    if bool(getattr(app.state, "roi_robinhood_chain_paper_runtime", False)):
        return

    current_workers = render_bootstrap._run_runtime_workers
    if not bool(getattr(current_workers, "_roi_robinhood_chain_paper", False)):
        _ORIGINAL_RUNTIME_WORKERS = current_workers
        setattr(_runtime_workers_with_robinhood, "_roi_robinhood_chain_paper", True)
        render_bootstrap._run_runtime_workers = _runtime_workers_with_robinhood

    routes = {getattr(route, "path", None) for route in app.routes}
    if "/v1/robinhood-chain/status" not in routes:
        app.add_api_route(
            "/v1/robinhood-chain/status",
            _status,
            methods=["GET"],
            name="robinhood_chain_status",
        )
    app.state.roi_robinhood_chain_paper_runtime = True
    _STATE["installed"] = True
    _STATE["state"] = "installed_waiting_for_canonical_runtime"


__all__ = ["install_robinhood_chain_paper_runtime"]
