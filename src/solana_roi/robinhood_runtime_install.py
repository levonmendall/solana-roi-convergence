from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap
from .risk_conditioned_alpha_v5 import (
    _fomo_classify_v5,
    install_risk_conditioned_alpha_v5,
)

# The Robinhood installer is imported by production only after canonical Solana/FOMO
# modules have been composed and before the ASGI lifespan starts background workers.
# Install v5 here so existing adapter instances pick up class-method wrappers without
# altering Render startup, RPC continuity, signing/submission, or live-money authority.
install_risk_conditioned_alpha_v5()

# fomo_runtime_install imports classify_fomo_state by value, so rebind that local
# production reference as well as the source module patched by the installer.
from . import fomo_runtime_install as _fomo_runtime_install

_fomo_runtime_install.classify_fomo_state = _fomo_classify_v5

from .robinhood_chain_paper import RobinhoodChainPaperPlane
from .robinhood_chain_profit_maximizer import ROBINHOOD_V5_VERSION


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
        payload = _PLANE.status()
        try:
            with _PLANE.store._lock:
                contexts = int(_PLANE.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_v5_trial_context WHERE release_commit=?",
                    (_PLANE.release_commit,),
                ).fetchone()[0])
                challengers = int(_PLANE.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_v5_trial_context WHERE release_commit=? AND threshold_challenger=1",
                    (_PLANE.release_commit,),
                ).fetchone()[0])
                marks = int(_PLANE.store.db.execute(
                    "SELECT COUNT(*) FROM robinhood_v5_marks WHERE release_commit=?",
                    (_PLANE.release_commit,),
                ).fetchone()[0])
        except Exception:
            contexts = challengers = marks = 0
        payload.update(
            {
                "strategy_version": ROBINHOOD_V5_VERSION,
                "wallet_authority_key": "chain_x_entity_x_role_x_venue_x_lifecycle_x_regime_x_risk_signature_x_flow_state",
                "risk_conditioned_v5": {
                    "active_paper_authority": True,
                    "lanes": [
                        "elite_entity_continuation",
                        "creator_deployer_continuation",
                        "entity_flow_accumulation",
                        "fomo_continuation",
                        "lifecycle_transition_continuation",
                        "hazard_continuation",
                    ],
                    "deployer_is_automatic_veto": False,
                    "deployer_counts_as_independent_confirmation": False,
                    "pons_v2_85pct_progress_is_automatic_veto": False,
                    "snipe_tax_above_500bps_is_automatic_veto": False,
                    "promotion_requires_50pct_hit_rate": False,
                    "promotion_objective": "robust_forward_expected_log_growth_with_tail_and_drawdown_constraints",
                    "learned_exit_policy": "forward_MFE_MAE_after_30_closed_context_outcomes",
                    "context_rows": contexts,
                    "threshold_challenger_rows": challengers,
                    "mark_to_market_rows": marks,
                },
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
                "runtime_ready": True,
                "production_install": dict(_STATE),
            }
        )
        return payload
    return {
        "enabled": True,
        "chain": "ROBINHOOD_CHAIN",
        "chain_id": 4663,
        "strategy_version": ROBINHOOD_V5_VERSION,
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
