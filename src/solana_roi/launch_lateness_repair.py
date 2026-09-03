from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector
from .live_collectors import _fresh
from .risk import LaunchEvidence, RiskDimension


async def _launch_collect_with_one_sided_lateness(
    self: DexScreenerLaunchCollector,
    mint: str,
    at: datetime,
) -> bool:
    """Treat near-creation as late-arrival latency, not absolute clock disagreement.

    The launch creation time is a Solana cluster blockTime while the live receipt
    is a Render host timestamp. A negative signed difference means the chain clock
    was ahead of the host clock; it is not evidence that observation arrived late.
    Positive lateness remains subject to the unchanged three-second threshold.
    Missing timestamps still fail closed.
    """

    if _fresh(self.risk, mint, RiskDimension.LAUNCH, at):
        return True
    created_at = await self._created_at(mint)
    if created_at is None:
        return False
    if at < created_at + timedelta(seconds=self.policy.launch_window_seconds):
        return False

    rows = self._early_rows(
        mint,
        start=created_at - timedelta(seconds=1),
        end=created_at + timedelta(seconds=self.policy.launch_window_seconds),
        decision_at=at,
    )
    buys = [row for row in rows if row["side"] == "buy"]
    buyers = {str(row["wallet"]) for row in buys}

    context = _launch_contexts(self).get(mint)
    if isinstance(context, dict):
        observed_at = context.get("observed_at")
        if not isinstance(observed_at, datetime):
            signed_lag_seconds = None
            lag_seconds = None
        else:
            signed_lag_seconds = (observed_at - created_at).total_seconds()
            # Only positive delay is transport/observer lateness. Negative values
            # are cross-clock skew and cannot prove that the launch arrived late.
            lag_seconds = max(0.0, signed_lag_seconds)
            if signed_lag_seconds < 0.0:
                setattr(
                    self,
                    "_roi_negative_launch_clock_skew_count",
                    int(getattr(self, "_roi_negative_launch_clock_skew_count", 0) or 0) + 1,
                )
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        early_complete = bool(context.get("complete"))
        source = "program-wide-swaps+confirmed-launch-context:launch-window-v3"
    else:
        # Preserve legacy/offline semantics when there is no live launch receipt
        # attestation. This repair does not manufacture prospective evidence.
        earliest = min(
            (datetime.fromisoformat(str(row["observed_at"])) for row in rows),
            default=None,
        )
        signed_lag_seconds = None
        lag_seconds = (
            abs((earliest - created_at).total_seconds())
            if earliest is not None
            else None
        )
        near_creation = (
            lag_seconds is not None
            and lag_seconds <= self.policy.max_pair_stream_lag_seconds
        )
        early_complete = (
            len(buys) >= self.policy.min_launch_buys
            and len(buyers) >= self.policy.min_launch_buyers
        )
        source = "program-wide-swaps+dexscreener:launch-window-v1"

    if hasattr(self.store, "record_program_coverage"):
        self.store.record_program_coverage(
            token_mint=mint,
            pair_created_at=created_at.isoformat(),
            assessed_at=at.isoformat(),
            launch_lag_ms=lag_seconds * 1000.0 if lag_seconds is not None else None,
            launch_near_creation=near_creation,
            early_buy_count=len(buys),
            early_buyer_count=len(buyers),
            early_buyers_complete=early_complete,
        )
    if not early_complete or not near_creation:
        return False

    slot_buyers: dict[int, set[str]] = {}
    buyer_sol: dict[str, float] = {}
    total_sol = 0.0
    for row in buys:
        slot_buyers.setdefault(int(row["slot"]), set()).add(str(row["wallet"]))
        amount = float(row["native_amount_sol"])
        total_sol += amount
        buyer_sol[str(row["wallet"])] = buyer_sol.get(str(row["wallet"]), 0.0) + amount
    bundled = max((len(value) for value in slot_buyers.values()), default=0) >= self.policy.bundled_same_slot_buyers
    top_two = sum(sorted(buyer_sol.values(), reverse=True)[:2])
    sniper_heavy = total_sol > 0 and top_two / total_sol >= self.policy.sniper_top_two_buy_share
    self.risk.record_launch(
        mint,
        LaunchEvidence(bundled_launch=bundled, sniper_heavy=sniper_heavy),
        observed_at=at,
        received_at=at,
        source=source,
    )
    return True


def _status_with_launch_lateness(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        launch_bridge = payload.get("launch_coverage_bridge")
        if isinstance(launch_bridge, dict):
            launch_bridge.update(
                {
                    "coverage_semantics": "live-arrival-lateness+immutable-window-acquisition-v3",
                    "near_creation_uses_live_launch_receipt": True,
                    "near_creation_uses_one_sided_lateness": True,
                    "negative_chain_clock_skew_does_not_count_as_late": True,
                    "near_creation_threshold_unchanged": True,
                    "early_buyer_completeness_is_acquisition_completeness": True,
                    "candidate_activation_from_launch_bridge": False,
                    "latency_samples_from_launch_bridge": False,
                    "quote_samples_from_launch_bridge": False,
                    "paper_authority": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_launch_lateness_repair", True)
    return status


def install_launch_lateness_repair() -> None:
    DexScreenerLaunchCollector.collect = _launch_collect_with_one_sided_lateness  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_launch_lateness_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_launch_lateness(current_status)  # type: ignore[method-assign]

    # Production proved that absolute host-wall-clock versus Solana blockTime still
    # rejects most otherwise complete launch observations. Install the v4 proof only
    # after v3 so direct/offline callers keep v3 compatibility while production
    # launch-bridge contexts use a first-receipt confirmed-chain-head comparison.
    from .launch_chain_timing_repair import install_launch_chain_timing_repair

    install_launch_chain_timing_repair()


__all__ = [
    "install_launch_lateness_repair",
    "_launch_collect_with_one_sided_lateness",
]
