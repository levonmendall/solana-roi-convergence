from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from . import direct_transaction as tx
from . import economic_signal_continuation_repair as economic
from . import post177_forward_pipeline_bottleneck_repair as post177
from . import release_bound_scout_classification_repair as release_bound
from . import robinhood_forward_only_runtime_repair as forward
from . import robinhood_live_frontier_verification_repair as frontier
from . import scout_candidate_continuity_repair as scout
from . import unified_strategy_status as unified_status


REPAIR_VERSION = "post178-e2e-residual-v1"
LEGACY_ROBINHOOD_BLOCKER = "robinhood_not_caught_up_for_paper_decisions"
FORWARD_ROBINHOOD_BLOCKER = "robinhood_forward_frontier_not_ready"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_NORMALIZE: Callable[..., Any] | None = None
_ORIGINAL_UNIFIED_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ROBINHOOD_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post178_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _header_aware_account_entries(result: Any) -> list[tuple[str, bool, int]]:
    """Infer signers from the canonical message header for string account keys.

    Standard JSON-RPC commonly returns ``accountKeys`` as strings. In that shape the
    signer bit lives in ``header.numRequiredSignatures`` rather than on each key.
    Treating every string key as a non-signer can make one real scout signer look
    ambiguous whenever another tracked wallet is merely an account in the same tx.
    """

    if not isinstance(result, dict):
        return []
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    rows = message.get("accountKeys") if isinstance(message, dict) else None
    if not isinstance(rows, list):
        return []
    header = message.get("header") if isinstance(message, dict) else None
    try:
        required_signatures = int(header.get("numRequiredSignatures") or 0) if isinstance(header, dict) else 0
    except (TypeError, ValueError):
        required_signatures = 0

    entries: list[tuple[str, bool, int]] = []
    for index, row in enumerate(rows):
        header_signer = index < max(0, required_signatures)
        if isinstance(row, str):
            pubkey = row
            signer = header_signer
        elif isinstance(row, dict):
            pubkey = str(row.get("pubkey") or "")
            signer = bool(row.get("signer")) or header_signer
        else:
            continue
        if pubkey:
            entries.append((pubkey, signer, index))
    return entries


async def _observe_robinhood_head_without_false_generation(self: Any, stop: asyncio.Event) -> None:
    """Keep head freshness telemetry without turning poll jitter into data loss.

    A failed or slow ``eth_blockNumber`` sample does not itself create an event gap:
    the authoritative worker subsequently reads a bounded exact block/log window.
    Readiness is false while the head is stale, but observer generation is not
    advanced. A real stale backlog is skipped by the rolling-current-window advance
    below, so no historical interval gains retrospective entry authority.
    """

    while not stop.is_set():
        started = time.monotonic()
        prior_success = float(getattr(self, "_roi_post177_head_observer_last_success_monotonic", 0.0) or 0.0)
        try:
            latest = int(await self.rpc.block_number())
            now = time.monotonic()
            if prior_success > 0.0 and now - prior_success > post177.ROBINHOOD_HEAD_OBSERVER_MAX_GAP_SECONDS:
                setattr(self, "_roi_post177_head_observer_last_gap_reason", "observer_heartbeat_stall_no_reanchor")
                _inc(self, "head_observer_stalls")
            setattr(self, "_roi_post177_head_observed_block", latest)
            setattr(self, "_roi_post177_head_observer_last_success_monotonic", now)
            setattr(self, "_roi_post177_head_observer_last_success_at", _utcnow().isoformat())
            setattr(self, "_roi_post177_head_observer_last_error_type", None)
            setattr(self, "_roi_post177_head_observer_continuity_ok", True)
            self._latest_block = latest
            post177._inc(self, "head_observer_successes")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            setattr(self, "_roi_post177_head_observer_continuity_ok", False)
            setattr(self, "_roi_post177_head_observer_last_gap_reason", "observer_rpc_stall_no_reanchor")
            setattr(self, "_roi_post177_head_observer_last_error_type", type(exc).__name__)
            post177._inc(self, "head_observer_failures")
            _inc(self, "head_observer_rpc_stalls")

        elapsed = max(0.0, time.monotonic() - started)
        delay = max(0.05, post177.ROBINHOOD_HEAD_OBSERVER_INTERVAL_SECONDS - elapsed)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


async def _advance_robinhood_current_window(self: Any) -> None:
    """Advance from the current bounded window instead of draining historical lag.

    The previous post-177 repair correctly separated observation from processing,
    but it still drained every queued block before decisions could become ready.
    That recreated a practical catch-up requirement. This implementation retains the
    existing bounded 64-block acquisition window and exact factory/log reads, skips
    any older stale backlog with zero entry authority, and evaluates only markets
    touched in the current bounded window after a fresh-head readiness check.
    """

    frontier._inc(self, "polls")
    post177._schedule_rwa_refresh(self)

    if not bool(getattr(self, "_roi_forward_only_chain_id_verified", False)):
        chain_id = await self.rpc.chain_id()
        if chain_id != forward.runtime.ROBINHOOD_CHAIN_ID:
            raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
        setattr(self, "_roi_forward_only_chain_id_verified", True)

    latest = await post177._current_observed_head(self)
    self._latest_block = latest

    if not frontier._live_epoch_active(self):
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="startup_current_frontier_metadata",
        )
        post177._start_observed_epoch(self, latest=latest, reason="forward_only_current_window")
        return

    live_cursor = frontier._live_cursor(self)
    assert live_cursor is not None

    if latest < live_cursor:
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="chain_head_regressed_reanchor",
        )
        post177._start_observed_epoch(self, latest=latest, reason="chain_head_regressed")
        frontier._inc(self, "epoch_resets")
        _inc(self, "true_chain_regression_reanchors")
        return

    gap = max(0, latest - live_cursor)
    if gap == 0:
        ready = post177._observer_head_fresh(self)
        setattr(self, "_roi_live_epoch_ready", ready)
        if ready:
            setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        return

    max_window = int(frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS)
    stale_skipped = max(0, gap - max_window)
    from_block = max(live_cursor + 1, latest - max_window + 1)
    to_block = latest
    if stale_skipped:
        post177._clear_pending_markets(self)
        _inc(self, "stale_backlog_windows_skipped")
        _inc(self, "stale_backlog_blocks_skipped", stale_skipped)

    observed_at = frontier._utcnow()
    await frontier._sync_factory_state(self, from_block=from_block, to_block=to_block)
    market_logs = await frontier._fetch_market_logs(self, from_block=from_block, to_block=to_block)

    touched_v3: dict[str, tuple[Any, int]] = {}
    touched_v2: dict[str, Any] = {}
    setattr(self, "_roi_live_epoch_suppress_entries", True)
    try:
        for kind, market, log in market_logs:
            block = int(str(log.get("blockNumber") or "0x0"), 16)
            if kind == "v3":
                await self._process_v3_swap(market, log, live=True, observed_at=observed_at)
                previous = touched_v3.get(market.pool)
                if previous is None or block > previous[1]:
                    touched_v3[market.pool] = (market, block)
            else:
                await self._process_v2_curve_log(market, log, live=True, observed_at=observed_at)
                touched_v2[market.curve] = market
    finally:
        setattr(self, "_roi_live_epoch_suppress_entries", False)

    setattr(self, "_roi_live_epoch_cursor", to_block)
    setattr(self, "_roi_live_epoch_factory_verified_through", to_block)
    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    frontier._inc(self, "ranges_completed")
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {
            "from_block": from_block,
            "to_block": to_block,
            "market_logs": len(market_logs),
            "forward_only": True,
            "rolling_current_window": True,
            "historical_backlog_blocks_skipped": stale_skipped,
            "retrospective_entry_authority": False,
        },
    )

    ready = bool(post177._observer_head_fresh(self) and await frontier._fresh_head_ready(self))
    setattr(self, "_roi_live_epoch_ready", ready)
    if not ready:
        return

    for market, block in touched_v3.values():
        await self._maybe_open_v3(market, current_block=block)
    for market in touched_v2.values():
        await self._maybe_open_v2(market)


setattr(_advance_robinhood_current_window, "_roi_post178_current_window", True)


def _normalize_with_terminal_noncopyable(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    source_hint: str | None = None,
) -> Any:
    if _ORIGINAL_NORMALIZE is None:
        raise RuntimeError("post-178 terminal scout classification is not installed")
    swap = _ORIGINAL_NORMALIZE(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        source_hint=source_hint,
    )
    if swap is not None or source_hint is not None or not isinstance(result, dict):
        return swap

    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is None:
        return swap
    if release_bound._terminal_classification(plane.store, signature) is not None:
        return swap

    wallet, _wallet_error = scout._tracked_scout_wallet(
        result,
        tuple(getattr(plane, "scout_wallets", ()) or ()),
    )
    if wallet is None:
        return swap
    movement, movement_error = economic._economic_movement(result, wallet)
    if movement is None or movement_error is not None:
        return swap
    if movement.get("native_amount_sol") is not None:
        return swap

    # A proven token movement with no attributable quote flow is useful continuation
    # evidence but is not copyable at this observation. Terminally classify it so it
    # does not remain an anonymous certification failure. No synthetic price, quote,
    # candidate, or paper entry is created.
    release_bound._record_terminal_non_candidate(
        plane.store,
        signature=signature,
        trigger_received_at=trigger_received_at,
        reason="economic_movement_price_unresolved_noncopyable",
    )
    _inc(plane, "economic_movement_noncopyable_classifications")
    return swap


setattr(_normalize_with_terminal_noncopyable, "_roi_post178_terminal_noncopyable", True)


def _replace_legacy_blocker(value: Any) -> Any:
    if isinstance(value, str):
        return FORWARD_ROBINHOOD_BLOCKER if value == LEGACY_ROBINHOOD_BLOCKER else value
    if isinstance(value, list):
        return [_replace_legacy_blocker(item) for item in value]
    if isinstance(value, dict):
        return {key: _replace_legacy_blocker(item) for key, item in value.items()}
    return value


def _remove_blocker(values: list[Any], blocker: str) -> list[Any]:
    return [value for value in values if value != blocker]


def _unified_status_with_current_frontier(
    base_status: dict[str, Any], runtime: Any, robinhood_status: dict[str, Any]
) -> dict[str, Any]:
    if _ORIGINAL_UNIFIED_STATUS is None:
        raise RuntimeError("post-178 unified status repair is not installed")
    payload = _replace_legacy_blocker(_ORIGINAL_UNIFIED_STATUS(base_status, runtime, robinhood_status))
    robinhood = payload.get("robinhood") if isinstance(payload.get("robinhood"), dict) else {}

    raw_ready = robinhood_status.get("paper_decision_transport_ready")
    if raw_ready is None:
        raw_ready = robinhood_status.get("forward_frontier_ready")
    if raw_ready is None:
        raw_ready = robinhood_status.get("caught_up_for_paper_decisions")
    ready = bool(
        raw_ready
        and robinhood_status.get("runtime_ready")
        and robinhood_status.get("paper_trading_authority", True)
        and not robinhood_status.get("failed_closed", False)
    )
    robinhood["paper_decision_transport_ready"] = ready
    blockers = _remove_blocker(list(robinhood.get("blockers") or []), FORWARD_ROBINHOOD_BLOCKER)
    if not ready:
        blockers.append(FORWARD_ROBINHOOD_BLOCKER)
    robinhood["blockers"] = list(dict.fromkeys(blockers))

    regimes = robinhood.get("regimes") if isinstance(robinhood.get("regimes"), dict) else {}
    for regime in regimes.values():
        if not isinstance(regime, dict):
            continue
        regime_blockers = _remove_blocker(list(regime.get("blockers") or []), FORWARD_ROBINHOOD_BLOCKER)
        if not ready:
            regime_blockers.append(FORWARD_ROBINHOOD_BLOCKER)
            regime["e2e_achievable"] = False
        elif not regime_blockers and bool(regime.get("paper_capable", True)):
            regime["e2e_achievable"] = True
        regime["blockers"] = list(dict.fromkeys(regime_blockers))
    if regimes:
        robinhood["all_regimes_e2e_achievable"] = all(
            bool(regime.get("e2e_achievable")) for regime in regimes.values() if isinstance(regime, dict)
        )
    robinhood["readiness_semantics"] = {
        "historical_catchup_required": False,
        "historical_cursor_has_decision_authority": False,
        "forward_frontier_authoritative": True,
        "rolling_current_window": True,
        "stale_backlog_has_retrospective_entry_authority": False,
        "repair_version": REPAIR_VERSION,
    }
    payload["robinhood"] = robinhood

    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    overall_blockers = _remove_blocker(list(overall.get("blocking_components") or []), FORWARD_ROBINHOOD_BLOCKER)
    overall_blockers.extend(robinhood.get("blockers") or [])
    overall["blocking_components"] = list(dict.fromkeys(overall_blockers))
    overall["all_paper_planes_e2e_achievable"] = bool(
        payload.get("solana", {}).get("all_regimes_e2e_achievable")
        and payload.get("fomo", {}).get("all_regimes_e2e_achievable")
        and robinhood.get("all_regimes_e2e_achievable")
    )
    overall["post178_e2e_residual_repair"] = REPAIR_VERSION
    payload["overall"] = overall
    return payload


setattr(_unified_status_with_current_frontier, "_roi_post178_e2e_residual", True)


def _status_with_post178(self: Any) -> dict[str, Any]:
    if _ORIGINAL_ROBINHOOD_STATUS is None:
        raise RuntimeError("post-178 Robinhood status repair is not installed")
    payload = _ORIGINAL_ROBINHOOD_STATUS(self)
    forward_readiness = payload.get("forward_frontier_readiness")
    if isinstance(forward_readiness, dict):
        forward_readiness.update(
            {
                "rolling_current_window": True,
                "processing_backlog_requires_catchup": False,
                "head_poll_stall_reanchors_epoch": False,
                "stale_backlog_has_retrospective_entry_authority": False,
                "stale_backlog_windows_skipped_session": int(
                    getattr(self, "_roi_post178_stale_backlog_windows_skipped", 0) or 0
                ),
                "stale_backlog_blocks_skipped_session": int(
                    getattr(self, "_roi_post178_stale_backlog_blocks_skipped", 0) or 0
                ),
                "head_observer_stalls_session": int(getattr(self, "_roi_post178_head_observer_stalls", 0) or 0),
                "head_observer_rpc_stalls_session": int(
                    getattr(self, "_roi_post178_head_observer_rpc_stalls", 0) or 0
                ),
                "post178_repair_version": REPAIR_VERSION,
            }
        )
    payload["historical_catchup_required_for_paper_decisions"] = False
    return payload


setattr(_status_with_post178, "_roi_post178_e2e_residual", True)


def install_post178_e2e_residual_repair(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_NORMALIZE, _ORIGINAL_UNIFIED_STATUS, _ORIGINAL_ROBINHOOD_STATUS
    if _INSTALLED:
        return

    # Robinhood: current-window acquisition is authoritative. Historical backlog is
    # neither drained nor promoted to a decision prerequisite.
    post177._observe_robinhood_head = _observe_robinhood_head_without_false_generation  # type: ignore[assignment]
    frontier._advance_live_epoch = _advance_robinhood_current_window  # type: ignore[assignment]

    # Solana scout identity: use the message header for string-form account keys.
    scout._account_entries = _header_aware_account_entries  # type: ignore[assignment]

    current_normalize = tx.normalize_standard_transaction
    if not bool(getattr(current_normalize, "_roi_post178_terminal_noncopyable", False)):
        _ORIGINAL_NORMALIZE = current_normalize
        try:
            _normalize_with_terminal_noncopyable.__dict__.update(getattr(current_normalize, "__dict__", {}))
        except Exception:
            pass
        setattr(_normalize_with_terminal_noncopyable, "_roi_post178_terminal_noncopyable", True)
        tx.normalize_standard_transaction = _normalize_with_terminal_noncopyable

    current_unified = unified_status.build_unified_strategy_status
    if not bool(getattr(current_unified, "_roi_post178_e2e_residual", False)):
        _ORIGINAL_UNIFIED_STATUS = current_unified
        try:
            _unified_status_with_current_frontier.__dict__.update(getattr(current_unified, "__dict__", {}))
        except Exception:
            pass
        unified_status.build_unified_strategy_status = _unified_status_with_current_frontier

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_post178_e2e_residual", False)):
        _ORIGINAL_ROBINHOOD_STATUS = current_status
        try:
            _status_with_post178.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        plane_cls.status = _status_with_post178  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_post178_e2e_residual_repair_installed", True)
    setattr(plane_cls, "_roi_post178_e2e_residual_repair_version", REPAIR_VERSION)
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_advance_robinhood_current_window",
    "_header_aware_account_entries",
    "_normalize_with_terminal_noncopyable",
    "_unified_status_with_current_frontier",
    "install_post178_e2e_residual_repair",
]
