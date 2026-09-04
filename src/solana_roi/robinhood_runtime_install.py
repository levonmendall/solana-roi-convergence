from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any, Callable

from .robinhood_chain_paper import RobinhoodChainPaperPlane


_RUNTIME_PROVIDER: Callable[[], Any] | None = None
_PLANE: RobinhoodChainPaperPlane | None = None
_STARTUP_ERROR: str | None = None
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "attempts": 0,
}


async def _run_when_runtime_ready(stop: asyncio.Event) -> None:
    global _PLANE, _STARTUP_ERROR
    if _RUNTIME_PROVIDER is None:
        _STARTUP_ERROR = "runtime_provider_unavailable"
        _STATE["state"] = "failed_closed"
        return

    while not stop.is_set():
        _STATE["attempts"] = int(_STATE["attempts"]) + 1
        try:
            runtime = _RUNTIME_PROVIDER()
        except Exception as exc:
            # The canonical Render bootstrap intentionally returns 503 until the
            # durable Solana runtime owns the SQLite file. Robinhood waits behind
            # that same handoff instead of delaying constant-time web liveness.
            _STARTUP_ERROR = f"{type(exc).__name__}: runtime_not_ready"
            _STATE["state"] = "waiting_for_canonical_runtime"
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            return

        try:
            plane = RobinhoodChainPaperPlane(runtime.store)
            _PLANE = plane
            _STARTUP_ERROR = None
            _STATE["state"] = "running" if plane.enabled else "disabled"
            if plane.enabled:
                await plane.run(stop)
            else:
                await stop.wait()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _STARTUP_ERROR = f"{type(exc).__name__}: {exc}"
            _STATE["state"] = "failed_closed"
            return
        finally:
            if _PLANE is not None:
                with suppress(Exception):
                    await _PLANE.close()
        return


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
        "error": _STARTUP_ERROR or "runtime_not_initialized",
        "production_install": dict(_STATE),
    }


def install_robinhood_chain_paper_runtime(app: Any, runtime_provider: Callable[[], Any]) -> None:
    global _RUNTIME_PROVIDER
    if bool(getattr(app.state, "roi_robinhood_chain_paper_runtime", False)):
        return
    _RUNTIME_PROVIDER = runtime_provider
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def combined_lifespan(app_instance: Any):
        async with original_lifespan(app_instance):
            stop = asyncio.Event()
            task = asyncio.create_task(_run_when_runtime_ready(stop), name="robinhood-chain-paper")
            try:
                yield
            finally:
                stop.set()
                with suppress(asyncio.CancelledError):
                    await task
                _STATE["state"] = "stopped"

    app.router.lifespan_context = combined_lifespan
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
    _STATE["state"] = "installed_waiting_for_lifespan"


__all__ = ["install_robinhood_chain_paper_runtime"]
