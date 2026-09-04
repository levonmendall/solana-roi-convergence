from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any

from .production import app
from .api import ingestion_runtime
from .robinhood_chain_paper import RobinhoodChainPaperPlane


_robinhood_plane: RobinhoodChainPaperPlane | None = None
_robinhood_startup_error: str | None = None
_original_lifespan = app.router.lifespan_context


@asynccontextmanager
async def _combined_lifespan(app_instance: Any):
    global _robinhood_plane, _robinhood_startup_error
    async with _original_lifespan(app_instance):
        stop: asyncio.Event | None = None
        task: asyncio.Task[None] | None = None
        try:
            _robinhood_plane = RobinhoodChainPaperPlane(ingestion_runtime().store)
            _robinhood_startup_error = None
            if _robinhood_plane.enabled:
                stop = asyncio.Event()
                task = asyncio.create_task(
                    _robinhood_plane.run(stop),
                    name="robinhood-chain-paper",
                )
        except Exception as exc:
            # Robinhood is additive. Its initialization must never take down the
            # existing Solana paper service; the Robinhood status surface fails
            # closed until its own runtime can initialize on a later release.
            _robinhood_plane = None
            _robinhood_startup_error = f"{type(exc).__name__}: {exc}"
        try:
            yield
        finally:
            if stop is not None:
                stop.set()
            if task is not None:
                with suppress(asyncio.CancelledError):
                    await task
            if _robinhood_plane is not None:
                with suppress(Exception):
                    await _robinhood_plane.close()


app.router.lifespan_context = _combined_lifespan


@app.get("/v1/robinhood-chain/status")
def robinhood_chain_status() -> dict[str, Any]:
    if _robinhood_plane is None:
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
            "error": _robinhood_startup_error or "runtime_not_initialized",
        }
    return {**_robinhood_plane.status(), "runtime_ready": True}


__all__ = ["app", "robinhood_chain_status"]
