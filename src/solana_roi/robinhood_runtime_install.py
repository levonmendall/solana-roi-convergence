from __future__ import annotations

import asyncio
import copy
import os
import sqlite3
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from . import render_runtime_bootstrap_repair as render_bootstrap
from . import risk_conditioned_alpha_v5 as _risk_v5
from .risk_conditioned_alpha_v5 import (
    _fomo_classify_v5,
    install_risk_conditioned_alpha_v5,
)

# The Robinhood installer is imported by production only after canonical Solana/FOMO
# modules have been composed and before the ASGI lifespan starts background workers.
# Install v5 here so existing adapter instances pick up class-method wrappers without
# altering Render startup, RPC continuity, signing/submission, or live-money authority.
install_risk_conditioned_alpha_v5()

# Preserve the pre-v5 forward-maturity boundary. The strategy redesign intentionally
# removes the 50% hit-rate / positive-median veto; it does not silently raise the
# already-governed number of forward samples required before a FOMO wallet can be
# promoted. Robust expected log growth and tail constraints apply at the same mature
# forward boundary used by the existing wallet/entity system.
from . import fomo_paper_strategy as _fomo_paper_strategy
from .wallet_entity_universe_v4 import MIN_MATURE_FORWARD_SAMPLES


def _fomo_profile_with_existing_forward_maturity(values: list[float]) -> dict[str, Any]:
    profile = _risk_v5.robust_return_profile(
        values,
        grid=_risk_v5.FOMO_ACTIVE_GRID,
        max_fraction=0.05,
        min_samples=MIN_MATURE_FORWARD_SAMPLES,
    )
    challenger = {
        fraction: _risk_v5._expected_log_growth(values, fraction)
        for fraction in _risk_v5.FOMO_CHALLENGER_GRID
    }
    if profile.sample_count < MIN_MATURE_FORWARD_SAMPLES:
        state = "bootstrap_forward_evidence"
    elif profile.state == "promoted_positive_log_growth":
        state = "promoted_fomo_wallet"
    elif profile.state == "demoted_nonpositive_log_growth":
        state = "demoted_fomo_wallet"
    else:
        state = "observe_mixed_fomo_wallet"
    return {
        "sample_count": profile.sample_count,
        "mean_residual_roi_pct": profile.mean_return * 100.0 if profile.mean_return is not None else None,
        "median_residual_roi_pct": profile.median_return * 100.0 if profile.median_return is not None else None,
        "trimmed_mean_residual_roi_ex_best_1_pct": profile.trimmed_mean_ex_best * 100.0 if profile.trimmed_mean_ex_best is not None else None,
        "positive_rate_pct": profile.hit_rate * 100.0 if profile.hit_rate is not None else None,
        "mature": profile.sample_count >= MIN_MATURE_FORWARD_SAMPLES,
        "state": state,
        "best_paper_position_fraction": min(0.05, profile.best_fraction),
        "best_expected_log_growth": profile.best_expected_log_growth,
        "expected_shortfall_20_pct": profile.expected_shortfall_20 * 100.0 if profile.expected_shortfall_20 is not None else None,
        "winner_concentration_pct": profile.winner_concentration * 100.0 if profile.winner_concentration is not None else None,
        "max_drawdown_at_best_fraction_pct": profile.max_drawdown_at_best_fraction * 100.0 if profile.max_drawdown_at_best_fraction is not None else None,
        "challenger_expected_log_growth": challenger,
        "hit_rate_is_promotion_veto": False,
        "historical_evidence_used_for_promotion": False,
    }


_risk_v5._fomo_profile_v5 = _fomo_profile_with_existing_forward_maturity
_fomo_paper_strategy.classify_fomo_wallet_returns = _fomo_profile_with_existing_forward_maturity

# fomo_runtime_install imports classify_fomo_state by value, so rebind that local
# production reference as well as the source module patched by the installer.
from . import fomo_runtime_install as _fomo_runtime_install

_fomo_runtime_install.classify_fomo_state = _fomo_classify_v5

# V5.1 must be installed before the regime proof wraps the active buy path. This
# ensures the proof records the final entity-exact, amount-specific converged paper
# fraction rather than the preliminary v5 quote fraction. The installer remains
# paper-only and adds no signer, submission path or live-money authority.
from .risk_conditioned_alpha_v51 import install_risk_conditioned_alpha_v51

install_risk_conditioned_alpha_v51()

# PR120 makes risk signature a first-class strategy dimension. Apply the matching
# wallet allocator only after v5/FOMO/v5.1 composition is complete: specialist
# coverage is reserved by strategy + regime, then unused challenger capacity is still
# earned globally by forward ROI. High-risk and hazard-FOMO wallets retain observation
# coverage without gaining paper authority or weakening mechanical hard stops.
from .strategy_specialist_wallet_allocator import (
    install_strategy_specialist_wallet_allocator,
)

install_strategy_specialist_wallet_allocator()

# Make the fairness repair part of actual production composition and tighten the
# selection rule to regime-specific robust ROI percentage. Assigned leaders can use
# normal v5.1 paper sizing; lower-ranked/nonleader wallets remain bounded challengers
# so a new wallet can still dethrone an incumbent. No dollar-P/L ranking is used.
from .regime_roi_wallet_authority import (
    install_regime_roi_wallet_authority,
    robinhood_regime_entity_authority_status,
)

install_regime_roi_wallet_authority()

# Later touches from dynamically tracked wallets must never disappear merely because
# the in-memory strategy task set is full. Install a durable paper-strategy handoff
# after v5.1/regime authority composition, and partition only observed same-venue age
# into later-life contexts without fabricating a launch or pool timestamp.
from .later_activity_execution_repair import install_later_activity_execution_repair

install_later_activity_execution_repair()

# The unified status contract is installed at the same final production-composition
# boundary so it can observe Solana, FOMO and Robinhood without replacing any data
# plane. The regime probe now wraps the already-installed v5.1 buy path and reuses its
# final exact entry/immediate-exit snapshot; it never issues an extra quote or RPC
# request and has no promotion, portfolio-allocation, signing, submission or live authority.
from .unified_strategy_status import (
    install_regime_paper_e2e_probe,
    install_unified_ingestion_status,
)

install_regime_paper_e2e_probe()

# The first production all-strategy status exposed two continuity truth gaps: a
# failed direct-Solana gap recovery could erase its own retry boundary, while
# Robinhood could be reported E2E-achievable before it was caught up enough to make
# paper decisions. Install the fail-closed continuity/readiness repair before the
# ASGI routes are wrapped and before background workers start.
from .continuity_e2e_readiness_repair import install_continuity_e2e_readiness_repair

install_continuity_e2e_readiness_repair()

# The current strategy is wallet/context driven rather than a first-slot firehose
# sniper. Preserve broad raw program discovery and the unchanged program-coverage
# certification gate, but make lossless execution continuity authoritative only for
# strategy-relevant scout transport. Program-stream gaps remain durable discovery
# degradation and cannot consume the critical recovery lane or poison a release's
# strategy continuity by themselves.
from .strategy_relevant_continuity import install_strategy_relevant_continuity

install_strategy_relevant_continuity()

from .observation_store import ObservationEventStore
from .robinhood_chain_paper import RobinhoodChainPaperPlane
from .robinhood_chain_profit_maximizer import ROBINHOOD_V5_VERSION


ROBINHOOD_WORKER_ISOLATION_VERSION = "robinhood-dedicated-worker-v2"
ROBINHOOD_STATUS_REFRESH_SECONDS = 1.0
ROBINHOOD_WORKER_RESTART_SECONDS = 1.0

_ORIGINAL_RUNTIME_WORKERS: Callable[[Any, asyncio.Event], Any] | None = None
_PLANE: RobinhoodChainPaperPlane | None = None
_STARTUP_ERROR: str | None = None
_STATUS_LOCK = threading.Lock()
_STATUS_SNAPSHOT: dict[str, Any] | None = None
_STATE: dict[str, Any] = {
    "installed": False,
    "state": "not_started",
    "attempts": 0,
    "worker_restarts": 0,
    "dedicated_thread_alive": False,
    "dedicated_store_path": None,
    "canonical_cursor_seeded": False,
    "canonical_cursor_seed_error": None,
}


def _dedicated_store_path(canonical_store_path: str | Path) -> Path:
    explicit = os.getenv("ROBINHOOD_ISOLATED_STORE_PATH", "").strip()
    if explicit:
        return Path(explicit)
    canonical = Path(canonical_store_path)
    if canonical.suffix:
        return canonical.with_name(f"{canonical.stem}-robinhood{canonical.suffix}")
    return canonical.with_name(f"{canonical.name}-robinhood.sqlite3")


def _seed_cursor_from_canonical(
    plane: RobinhoodChainPaperPlane,
    canonical_store_path: str | Path,
) -> tuple[bool, str | None]:
    if plane._cursor is not None:
        return False, None
    source = Path(canonical_store_path)
    try:
        uri = f"file:{source.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=0.25)
        try:
            row = connection.execute(
                "SELECT value FROM robinhood_chain_state WHERE key='cursor_block'"
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if row is None:
        return False, None
    try:
        plane._set_cursor(int(row[0]))
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    return True, None


def _worker_isolation_status() -> dict[str, Any]:
    return {
        "version": ROBINHOOD_WORKER_ISOLATION_VERSION,
        "dedicated_thread": True,
        "dedicated_asyncio_event_loop": True,
        "dedicated_sqlite_file": True,
        "canonical_sqlite_shared": False,
        "uvicorn_event_loop_runs_robinhood_polling": False,
        "uvicorn_event_loop_runs_robinhood_sqlite": False,
        "status_served_from_cached_snapshot": True,
        "canonical_cursor_seed_is_read_only": True,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _fallback_status(error: str | None = None) -> dict[str, Any]:
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
        "error": error or _STARTUP_ERROR or "canonical_runtime_not_ready",
        "worker_isolation": _worker_isolation_status(),
        "production_install": dict(_STATE),
    }


def _build_plane_status(plane: RobinhoodChainPaperPlane) -> dict[str, Any]:
    payload = plane.status()
    try:
        with plane.store._lock:
            contexts = int(plane.store.db.execute(
                "SELECT COUNT(*) FROM robinhood_v5_trial_context WHERE release_commit=?",
                (plane.release_commit,),
            ).fetchone()[0])
            challengers = int(plane.store.db.execute(
                "SELECT COUNT(*) FROM robinhood_v5_trial_context WHERE release_commit=? AND threshold_challenger=1",
                (plane.release_commit,),
            ).fetchone()[0])
            marks = int(plane.store.db.execute(
                "SELECT COUNT(*) FROM robinhood_v5_marks WHERE release_commit=?",
                (plane.release_commit,),
            ).fetchone()[0])
    except Exception:
        contexts = challengers = marks = 0
    try:
        regime_authority = robinhood_regime_entity_authority_status(plane)
    except Exception as exc:
        regime_authority = {
            "authority_version": "regime-roi-wallet-authority-v2",
            "failed_closed": True,
            "error": f"{type(exc).__name__}: Robinhood regime entity authority unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
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
            "regime_roi_entity_authority": regime_authority,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "runtime_ready": True,
            "worker_isolation": _worker_isolation_status(),
            "production_install": dict(_STATE),
        }
    )
    return payload


def _publish_status(payload: dict[str, Any]) -> None:
    global _STATUS_SNAPSHOT
    with _STATUS_LOCK:
        _STATUS_SNAPSHOT = copy.deepcopy(payload)


async def _snapshot_loop(plane: RobinhoodChainPaperPlane, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            _publish_status(_build_plane_status(plane))
        except Exception as exc:
            _publish_status(_fallback_status(f"{type(exc).__name__}: Robinhood status snapshot failed"))
        try:
            await asyncio.wait_for(stop.wait(), timeout=ROBINHOOD_STATUS_REFRESH_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _thread_cycle(canonical_store_path: str, thread_stop: threading.Event) -> None:
    global _PLANE, _STARTUP_ERROR
    dedicated_path = _dedicated_store_path(canonical_store_path)
    dedicated_path.parent.mkdir(parents=True, exist_ok=True)
    _STATE["dedicated_store_path"] = str(dedicated_path)

    store = ObservationEventStore(dedicated_path)
    plane: RobinhoodChainPaperPlane | None = None
    async_stop = asyncio.Event()

    async def bridge_stop() -> None:
        while not thread_stop.is_set():
            await asyncio.sleep(0.10)
        async_stop.set()

    bridge_task: asyncio.Task[None] | None = None
    snapshot_task: asyncio.Task[None] | None = None
    try:
        plane = RobinhoodChainPaperPlane(store)
        _PLANE = plane
        seeded, seed_error = _seed_cursor_from_canonical(plane, canonical_store_path)
        _STATE["canonical_cursor_seeded"] = seeded
        _STATE["canonical_cursor_seed_error"] = seed_error
        _STARTUP_ERROR = None
        _STATE["state"] = "running" if plane.enabled else "disabled"
        _STATE["dedicated_thread_alive"] = True
        _publish_status(_build_plane_status(plane))

        bridge_task = asyncio.create_task(bridge_stop(), name="robinhood-worker-stop-bridge")
        snapshot_task = asyncio.create_task(
            _snapshot_loop(plane, async_stop),
            name="robinhood-worker-status-snapshot",
        )
        if plane.enabled:
            await plane.run(async_stop)
        else:
            await bridge_task
    finally:
        async_stop.set()
        if snapshot_task is not None:
            snapshot_task.cancel()
            with suppress(asyncio.CancelledError):
                await snapshot_task
        if bridge_task is not None and not bridge_task.done():
            bridge_task.cancel()
            with suppress(asyncio.CancelledError):
                await bridge_task
        if plane is not None:
            with suppress(Exception):
                await plane.close()
        store.close()
        _PLANE = None
        _STATE["dedicated_thread_alive"] = False


def _thread_entry(canonical_store_path: str, thread_stop: threading.Event) -> None:
    global _STARTUP_ERROR
    while not thread_stop.is_set():
        try:
            asyncio.run(_thread_cycle(canonical_store_path, thread_stop))
        except Exception as exc:
            _STARTUP_ERROR = f"{type(exc).__name__}: {exc}"
            _STATE["state"] = "failed_closed"
            _STATE["dedicated_thread_alive"] = False
            _publish_status(_fallback_status(_STARTUP_ERROR))
        if thread_stop.is_set():
            break
        _STATE["worker_restarts"] = int(_STATE["worker_restarts"]) + 1
        _STATE["state"] = "restarting_worker"
        if thread_stop.wait(ROBINHOOD_WORKER_RESTART_SECONDS):
            break
    _STATE["dedicated_thread_alive"] = False


async def _runtime_workers_with_robinhood(runtime: Any, stop: asyncio.Event) -> None:
    """Run Robinhood outside Uvicorn's event loop and canonical SQLite file.

    Production proved cooperative yields were insufficient because one synchronous
    SQLite/CPU section could still occupy the web process beyond Render's five-second
    health deadline. Robinhood now owns a dedicated thread, asyncio loop and SQLite
    file. The only canonical-store contact is a one-time read-only cursor seed when
    the isolated store has no cursor yet. Solana/FOMO worker composition is unchanged.
    """
    if _ORIGINAL_RUNTIME_WORKERS is None:
        raise RuntimeError("Robinhood production worker composition is not installed")

    _STATE["attempts"] = int(_STATE["attempts"]) + 1
    canonical_store_path = str(runtime.store.path)
    thread_stop = threading.Event()
    worker_thread = threading.Thread(
        target=_thread_entry,
        args=(canonical_store_path, thread_stop),
        name="robinhood-chain-paper-worker",
        daemon=True,
    )
    worker_thread.start()
    try:
        await _ORIGINAL_RUNTIME_WORKERS(runtime, stop)
    finally:
        thread_stop.set()
        await asyncio.to_thread(worker_thread.join, 5.0)
        if worker_thread.is_alive():
            _STATE["state"] = "worker_thread_still_stopping"
        elif stop.is_set():
            _STATE["state"] = "stopped"
        _STATE["dedicated_thread_alive"] = worker_thread.is_alive()


def _status() -> dict[str, Any]:
    with _STATUS_LOCK:
        snapshot = copy.deepcopy(_STATUS_SNAPSHOT)
    if snapshot is not None:
        snapshot["production_install"] = dict(_STATE)
        snapshot["worker_isolation"] = _worker_isolation_status()
        return snapshot
    return _fallback_status()


def install_robinhood_chain_paper_runtime(app: Any, runtime_provider: Callable[[], Any]) -> None:
    """Add Robinhood worker/telemetry without replacing canonical ASGI lifespan."""
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

    install_unified_ingestion_status(
        app,
        runtime_provider=runtime_provider,
        robinhood_status_provider=_status,
    )

    app.state.roi_robinhood_chain_paper_runtime = True
    _STATE["installed"] = True
    _STATE["state"] = "installed_waiting_for_canonical_runtime"


__all__ = [
    "ROBINHOOD_WORKER_ISOLATION_VERSION",
    "_dedicated_store_path",
    "_seed_cursor_from_canonical",
    "_status",
    "install_robinhood_chain_paper_runtime",
]
