from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .fomo_continuation_shadow import (
    FomoContinuationShadow,
    FomoOutcome,
    build_fomo_features,
    classify_fomo_state,
)
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter


INSTALL_VERSION = "fomo-runtime-install-v1"
_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _safe_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _venue_lifecycle(row: dict[str, Any]) -> tuple[str, str]:
    venue = str(row.get("venue") or row.get("venue_name") or row.get("platform") or "unknown")
    lifecycle = str(row.get("lifecycle") or row.get("lifecycle_stage") or row.get("market_phase") or "unknown")
    return venue, lifecycle


def _window_stats(adapter: FinalProfitFirstResearchAdapter, token_mint: str, at: datetime) -> dict[str, Any]:
    short_start = (at - timedelta(seconds=5)).isoformat()
    long_start = (at - timedelta(seconds=20)).isoformat()
    end = at.isoformat()
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT wallet,side,price_sol,amount_raw,copyable,metadata_json,observed_at,received_at "
            "FROM wallet_discovery_forward_observations "
            "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at",
            (token_mint, long_start, end),
        ).fetchall()

    short_rows = []
    long_rows = []
    for item in rows:
        received = str(item["received_at"] or item["observed_at"] or "")
        if not received:
            continue
        long_rows.append(item)
        if received >= short_start:
            short_rows.append(item)

    def stats(items: list[Any]) -> dict[str, Any]:
        buyers: set[str] = set()
        buys = sells = 0
        buy_volume = sell_volume = 0.0
        momentum_wallets = 0
        for item in items:
            side = str(item["side"] or "").lower()
            wallet = str(item["wallet"] or "")
            price = float(item["price_sol"] or 0.0)
            amount = float(item["amount_raw"] or 0.0)
            notional = max(0.0, price * amount)
            metadata = _safe_json(item["metadata_json"])
            role = str(metadata.get("wallet_role") or metadata.get("role") or "").lower()
            if side == "buy":
                buys += 1
                buy_volume += notional
                if wallet:
                    buyers.add(wallet)
                if role in {"momentum_alpha", "confirmation_alpha", "momentum", "confirmation"}:
                    momentum_wallets += 1
            elif side == "sell":
                sells += 1
                sell_volume += notional
        return {
            "buyers": len(buyers),
            "buys": buys,
            "sells": sells,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "momentum_wallets": momentum_wallets,
        }

    return {"short": stats(short_rows), "long": stats(long_rows)}


def _trial_for_signature(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,token_mint,observed_at,regime,opportunity_json,decision_json,signal_to_entry_seconds "
            "FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature),
        ).fetchone()
    return dict(row) if row else None


def _latest_final_outcome(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,entry_observed_at,signal_to_entry_seconds,net_return,context_json "
            "FROM profit_first_final_outcomes WHERE epoch_id=? AND source_signature=? ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature),
        ).fetchone()
    return dict(row) if row else None


def _shadow(adapter: FinalProfitFirstResearchAdapter) -> FomoContinuationShadow:
    current = getattr(adapter, "_roi_fomo_continuation_shadow", None)
    if isinstance(current, FomoContinuationShadow):
        return current
    value = FomoContinuationShadow(adapter.store, release_commit=adapter.release_commit)
    setattr(adapter, "_roi_fomo_continuation_shadow", value)
    return value


async def _observe_with_fomo(self: FinalProfitFirstResearchAdapter, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("FOMO runtime install missing original observe")
    await _ORIGINAL_OBSERVE(self, signature)

    trial = _trial_for_signature(self, signature)
    if not trial:
        return
    opportunity = _safe_json(trial.get("opportunity_json"))
    observed_at = str(trial.get("observed_at") or datetime.now(timezone.utc).isoformat())
    try:
        at = datetime.fromisoformat(observed_at)
    except Exception:
        at = datetime.now(timezone.utc)
    token_mint = str(trial.get("token_mint") or "")
    if not token_mint:
        return
    windows = _window_stats(self, token_mint, at)
    short = windows["short"]
    long = windows["long"]
    venue, lifecycle = _venue_lifecycle(opportunity)
    creator_flow_state = str(opportunity.get("creator_flow_state") or "neutral")
    features = build_fomo_features(
        token_mint=token_mint,
        observed_at=observed_at,
        venue=venue,
        lifecycle=lifecycle,
        regime=str(trial.get("regime") or opportunity.get("regime") or "unknown"),
        independent_buyers_short=int(short["buyers"]),
        independent_buyers_long=int(long["buyers"]),
        buys_short=int(short["buys"]),
        buys_long=int(long["buys"]),
        sells_short=int(short["sells"]),
        sells_long=int(long["sells"]),
        buy_volume_short=float(short["buy_volume"]),
        buy_volume_long=float(long["buy_volume"]),
        sell_volume_short=float(short["sell_volume"]),
        sell_volume_long=float(long["sell_volume"]),
        momentum_wallet_participation=int(short["momentum_wallets"]),
        creator_accumulating=creator_flow_state == "accumulating",
        creator_distributing=creator_flow_state == "distributing",
        early_holder_exit_fraction=float(opportunity.get("early_buyer_exit_fraction") or 0.0),
        chase_fraction=opportunity.get("chase_fraction"),
        signal_to_entry_seconds=trial.get("signal_to_entry_seconds"),
        quote_deterioration_fraction=opportunity.get("quote_deterioration_fraction"),
        depth_growth_fraction=opportunity.get("depth_growth_fraction"),
        exit_slippage_deterioration_fraction=opportunity.get("exit_slippage_deterioration_fraction"),
        risk_complete=not bool(opportunity.get("risk_incomplete", False)),
    )
    state = classify_fomo_state(features)
    shadow = _shadow(self)
    shadow.record_observation(source_signature=signature, features=features, state=state)

    outcome = _latest_final_outcome(self, signature)
    if outcome is not None:
        context = _safe_json(outcome.get("context_json"))
        shadow.record_outcome(
            FomoOutcome(
                source_signature=signature,
                observed_at=str(outcome.get("entry_observed_at") or observed_at),
                venue=venue,
                lifecycle=lifecycle,
                regime=str(context.get("regime") or trial.get("regime") or "unknown"),
                fomo_state=state.state,
                signal_to_entry_seconds=float(outcome.get("signal_to_entry_seconds") or trial.get("signal_to_entry_seconds") or 0.0),
                net_return=float(outcome.get("net_return") or 0.0),
                release_commit=self.release_commit,
            )
        )


def _status_with_fomo(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("FOMO runtime install missing original status")
    payload = _ORIGINAL_STATUS(self)
    payload["fomo_continuation_shadow"] = _shadow(self).status()
    return payload


def _manifest_with_fomo(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("FOMO runtime install missing original manifest")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "fomo_research_version": "fomo-continuation-shadow-v1",
            "fomo_lane": "fomo_continuation_shadow",
            "fomo_feature_family": [
                "independent_buyer_acceleration",
                "transaction_frequency_acceleration",
                "net_buy_flow_acceleration",
                "buy_sell_imbalance",
                "independent_demand_persistence",
                "momentum_wallet_participation",
                "creator_accumulation_or_distribution",
                "early_holder_distribution",
                "chase_distance",
                "observation_latency",
                "quote_deterioration",
                "depth_growth",
                "exit_slippage_deterioration",
            ],
            "fomo_states": ["no_fomo", "pre_fomo", "active_fomo", "late_or_inaccessible_fomo"],
            "fomo_strategy_authority": False,
            "fomo_historical_promotion_authority": False,
            "fomo_paper_only": True,
        }
    )
    return payload


def install_fomo_runtime() -> None:
    global _ORIGINAL_OBSERVE, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    current_observe = FinalProfitFirstResearchAdapter.observe
    if not bool(getattr(current_observe, "_roi_fomo_runtime", False)):
        _ORIGINAL_OBSERVE = current_observe
        setattr(_observe_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter.observe = _observe_with_fomo  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_fomo_runtime", False)):
        _ORIGINAL_STATUS = current_status
        setattr(_status_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter.status = _status_with_fomo  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_fomo_runtime", False)):
        _ORIGINAL_MANIFEST = current_manifest
        setattr(_manifest_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_fomo  # type: ignore[method-assign]


__all__ = ["INSTALL_VERSION", "install_fomo_runtime"]
