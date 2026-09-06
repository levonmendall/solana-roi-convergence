from __future__ import annotations

from typing import Any

from . import post177_forward_pipeline_bottleneck_repair as post177
from . import robinhood_forward_only_runtime_repair as forward
from . import robinhood_live_frontier_verification_repair as frontier
from . import robinhood_chain_runtime as runtime
from .v51_counterfactual_extension import refresh_all_rejected_counterfactuals  # compatibility symbol only

REPAIR_VERSION = "robinhood-current-decision-tail-v2-no-proof-work"
METADATA_WINDOW_BLOCKS = int(frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS)
DECISION_WINDOW_BLOCKS = int(runtime.LIVE_LAG_BLOCKS)
_INSTALLED = False


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_decision_tail_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


async def _advance_current_decision_tail(self: Any) -> None:
    """Keep metadata bounded while evaluating only the decision-current trade tail.

    Older continuously observed blocks cannot satisfy the existing two-block paper
    frontier by the time a wide market-log scan completes. They therefore have no
    immediate entry authority. No proof/counterfactual analytics run on this latency-
    critical path; those are published separately by the isolated proof worker.
    """
    frontier._inc(self, "polls")
    post177._schedule_rwa_refresh(self)

    if not bool(getattr(self, "_roi_forward_only_chain_id_verified", False)):
        chain_id = await self.rpc.chain_id()
        if chain_id != runtime.ROBINHOOD_CHAIN_ID:
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
        post177._start_observed_epoch(self, latest=latest, reason="current_decision_tail")
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
        return

    gap = max(0, latest - live_cursor)
    if gap == 0:
        provisional = bool(post177._observer_head_fresh(self))
        setattr(self, "_roi_live_epoch_ready", provisional)
        ready = bool(provisional and await frontier._fresh_head_ready(self))
        setattr(self, "_roi_live_epoch_ready", ready)
        if ready:
            setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        return

    metadata_from = max(live_cursor + 1, latest - METADATA_WINDOW_BLOCKS + 1)
    decision_from = max(live_cursor + 1, latest - DECISION_WINDOW_BLOCKS + 1)
    stale_skipped = max(0, decision_from - (live_cursor + 1))
    if stale_skipped:
        post177._clear_pending_markets(self)
        _inc(self, "stale_trade_blocks_skipped", stale_skipped)
        _inc(self, "stale_trade_windows_skipped")

    observed_at = frontier._utcnow()
    await frontier._sync_factory_state(self, from_block=metadata_from, to_block=latest)
    market_logs = await frontier._fetch_market_logs(
        self,
        from_block=decision_from,
        to_block=latest,
    )

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

    setattr(self, "_roi_live_epoch_cursor", latest)
    setattr(self, "_roi_live_epoch_factory_verified_through", latest)
    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    frontier._inc(self, "ranges_completed")
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {
            "metadata_from_block": metadata_from,
            "decision_from_block": decision_from,
            "to_block": latest,
            "market_logs": len(market_logs),
            "decision_tail_blocks": DECISION_WINDOW_BLOCKS,
            "metadata_window_blocks": METADATA_WINDOW_BLOCKS,
            "stale_trade_blocks_skipped": stale_skipped,
            "stale_trade_blocks_have_retrospective_entry_authority": False,
            "forward_only": True,
        },
    )

    provisional = bool(post177._observer_head_fresh(self))
    setattr(self, "_roi_live_epoch_ready", provisional)
    ready = bool(provisional and await frontier._fresh_head_ready(self))
    setattr(self, "_roi_live_epoch_ready", ready)

    if not ready:
        return
    for market, block in touched_v3.values():
        await self._maybe_open_v3(market, current_block=block)
    for market in touched_v2.values():
        await self._maybe_open_v2(market)


setattr(_advance_current_decision_tail, "_roi_current_decision_tail", True)


def install_robinhood_decision_tail_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    frontier._advance_live_epoch = _advance_current_decision_tail  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "metadata_window_blocks": METADATA_WINDOW_BLOCKS,
        "decision_window_blocks": DECISION_WINDOW_BLOCKS,
        "proof_or_counterfactual_work_on_decision_loop": False,
        "readiness_gate_blocks_changed": False,
        "stale_trade_blocks_have_retrospective_entry_authority": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "DECISION_WINDOW_BLOCKS",
    "METADATA_WINDOW_BLOCKS",
    "REPAIR_VERSION",
    "_advance_current_decision_tail",
    "install_robinhood_decision_tail_repair",
    "status",
]
