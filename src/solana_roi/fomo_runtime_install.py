from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .fomo_continuation_shadow import (
    FomoContinuationShadow,
    FomoOutcome,
    build_fomo_features,
    classify_fomo_state,
)
from .profit_first_entity_final import UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import (
    MIN_MATURE_FORWARD_SAMPLES,
    SEED_BY_ADDRESS,
    TokenScopedEntityResolver,
    WalletRole,
    _universe,
)
from .wallet_venue_lifecycle_research import lifecycle_stage, venue_from_source


INSTALL_VERSION = "fomo-runtime-install-v2-point-in-time"
_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _inherit_markers(wrapper: Callable[..., Any], wrapped: Callable[..., Any]) -> None:
    try:
        wrapper.__dict__.update(getattr(wrapped, "__dict__", {}))
    except Exception:
        pass


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


def _set_error(adapter: FinalProfitFirstResearchAdapter, message: str | None) -> None:
    setattr(adapter, "_roi_fomo_last_error", message)


def _shadow(adapter: FinalProfitFirstResearchAdapter) -> FomoContinuationShadow:
    current = getattr(adapter, "_roi_fomo_continuation_shadow", None)
    if isinstance(current, FomoContinuationShadow):
        return current
    value = FomoContinuationShadow(adapter.store, release_commit=adapter.release_commit)
    setattr(adapter, "_roi_fomo_continuation_shadow", value)
    return value


def _observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,"
            "copyable_price_sol,chase_fraction,copyable,observation_lag_ms,risk_complete,source "
            "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
            (signature,),
        ).fetchone()
    return dict(row) if row else None


def _trial(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT source_signature,token_mint,trigger_wallet,observed_at,received_at,regime,opportunity_json,"
            "signal_to_entry_seconds FROM profit_first_final_trials "
            "WHERE epoch_id=? AND source_signature=? AND lane=? ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature, UNIFIED_LANE),
        ).fetchone()
    return dict(row) if row else None


def _prior_pump_evidence(adapter: FinalProfitFirstResearchAdapter, token_mint: str, at: datetime) -> bool:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT source FROM wallet_discovery_forward_observations "
            "WHERE token_mint=? AND received_at<=? ORDER BY received_at DESC LIMIT 100",
            (token_mint, at.isoformat()),
        ).fetchall()
    return any(venue_from_source(str(row["source"] or "")) in {"PUMP_FUN", "PUMP_AMM"} for row in rows)


def _venue_lifecycle(adapter: FinalProfitFirstResearchAdapter, row: dict[str, Any], at: datetime) -> tuple[str, str]:
    venue = venue_from_source(str(row.get("source") or ""))
    prior_pump = bool(venue == "RAYDIUM" and _prior_pump_evidence(adapter, str(row.get("token_mint") or ""), at))
    return venue or "UNKNOWN", lifecycle_stage(venue, prior_pump_evidence=prior_pump)


def _qualified_momentum_wallets(adapter: FinalProfitFirstResearchAdapter) -> set[str]:
    now = time.monotonic()
    cached = getattr(adapter, "_roi_fomo_wallet_role_cache", None)
    if isinstance(cached, tuple) and len(cached) == 2 and now - float(cached[0]) <= 30.0:
        return set(cached[1])

    qualified: set[str] = set()
    target_roles = {WalletRole.MOMENTUM_ALPHA, WalletRole.CONFIRMATION_ALPHA}
    for address, seed in SEED_BY_ADDRESS.items():
        if any(role in target_roles for role in seed.initial_roles):
            qualified.add(address)
    try:
        for wallet, scores in _universe(adapter.discovery).role_scores().items():
            for role in target_roles:
                score = scores.get(role)
                if (
                    score is not None
                    and score.score is not None
                    and float(score.score) > 0.0
                    and int(score.sample_count) >= MIN_MATURE_FORWARD_SAMPLES
                ):
                    qualified.add(str(wallet))
                    break
    except Exception:
        pass
    setattr(adapter, "_roi_fomo_wallet_role_cache", (now, frozenset(qualified)))
    return qualified


def _window_rows(adapter: FinalProfitFirstResearchAdapter, token_mint: str, at: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    short_start = (at - timedelta(seconds=5)).isoformat()
    long_start = (at - timedelta(seconds=20)).isoformat()
    end = at.isoformat()
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT wallet,side,token_amount,wallet_price_sol,copyable_price_sol,observed_at,received_at "
            "FROM wallet_discovery_forward_observations "
            "WHERE token_mint=? AND received_at>=? AND received_at<=? ORDER BY received_at,id",
            (token_mint, long_start, end),
        ).fetchall()
    long_rows = [dict(row) for row in rows]
    short_rows = [row for row in long_rows if str(row.get("received_at") or "") >= short_start]
    return short_rows, long_rows


def _entity_counts(
    adapter: FinalProfitFirstResearchAdapter,
    token_mint: str,
    at: datetime,
    trigger_wallet: str,
    creator_wallet: str | None,
    short_rows: list[dict[str, Any]],
    long_rows: list[dict[str, Any]],
) -> tuple[int, int, dict[str, str]]:
    short_buyers = {str(row.get("wallet") or "") for row in short_rows if str(row.get("side") or "").lower() == "buy"}
    long_buyers = {str(row.get("wallet") or "") for row in long_rows if str(row.get("side") or "").lower() == "buy"}
    addresses = {wallet for wallet in long_buyers | {trigger_wallet, creator_wallet or ""} if wallet}
    resolver = getattr(adapter, "_roi_fomo_entity_resolver", None)
    if not isinstance(resolver, TokenScopedEntityResolver):
        resolver = TokenScopedEntityResolver(adapter.discovery)
        setattr(adapter, "_roi_fomo_entity_resolver", resolver)
    mapping, _ = resolver.components(token_mint, addresses, as_of=at, creator_wallet=creator_wallet)
    excluded = {mapping.get(trigger_wallet)}
    if creator_wallet:
        excluded.add(mapping.get(creator_wallet))
    excluded.discard(None)

    def count(wallets: set[str]) -> int:
        entities = {mapping.get(wallet, f"address:{wallet}") for wallet in wallets if wallet}
        return len({entity for entity in entities if entity not in excluded})

    return count(short_buyers), count(long_buyers), mapping


def _window_stats(rows: list[dict[str, Any]], qualified_wallets: set[str]) -> dict[str, Any]:
    buys = sells = 0
    buy_volume = sell_volume = 0.0
    momentum_wallets: set[str] = set()
    for row in rows:
        side = str(row.get("side") or "").lower()
        amount = max(0.0, float(row.get("token_amount") or 0.0))
        price = row.get("copyable_price_sol")
        if price is None:
            price = row.get("wallet_price_sol")
        notional = amount * max(0.0, float(price or 0.0))
        wallet = str(row.get("wallet") or "")
        if side == "buy":
            buys += 1
            buy_volume += notional
            if wallet in qualified_wallets:
                momentum_wallets.add(wallet)
        elif side == "sell":
            sells += 1
            sell_volume += notional
    return {
        "buys": buys,
        "sells": sells,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "momentum_wallets": len(momentum_wallets),
    }


def _record_fomo_observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> None:
    trial = _trial(adapter, signature)
    row = _observation(adapter, signature)
    if trial is None or row is None or str(row.get("side") or "").lower() != "buy":
        return

    at_raw = str(trial.get("received_at") or row.get("received_at") or "")
    at = datetime.fromisoformat(at_raw) if at_raw else datetime.now(timezone.utc)
    token_mint = str(trial.get("token_mint") or row.get("token_mint") or "")
    trigger_wallet = str(trial.get("trigger_wallet") or row.get("wallet") or "")
    creator_wallet = adapter.execution._deployer(token_mint, at)
    short_rows, long_rows = _window_rows(adapter, token_mint, at)
    short_entities, long_entities, _ = _entity_counts(
        adapter,
        token_mint,
        at,
        trigger_wallet,
        creator_wallet,
        short_rows,
        long_rows,
    )
    qualified = _qualified_momentum_wallets(adapter)
    short = _window_stats(short_rows, qualified)
    long = _window_stats(long_rows, qualified)
    opportunity = _safe_json(trial.get("opportunity_json"))
    venue, lifecycle = _venue_lifecycle(adapter, row, at)
    creator_flow_state = str(opportunity.get("creator_flow_state") or "neutral")

    features = build_fomo_features(
        token_mint=token_mint,
        observed_at=str(trial.get("observed_at") or row.get("observed_at") or at.isoformat()),
        venue=venue,
        lifecycle=lifecycle,
        regime=str(trial.get("regime") or opportunity.get("regime") or "unknown"),
        independent_buyers_short=short_entities,
        independent_buyers_long=long_entities,
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
        chase_fraction=opportunity.get("chase_fraction", row.get("chase_fraction")),
        signal_to_entry_seconds=trial.get("signal_to_entry_seconds"),
        quote_deterioration_fraction=opportunity.get("quote_deterioration_fraction"),
        depth_growth_fraction=opportunity.get("depth_growth_fraction"),
        exit_slippage_deterioration_fraction=opportunity.get("exit_slippage_deterioration_fraction"),
        risk_complete=bool(row.get("risk_complete")),
        trigger_is_proven_wallet=trigger_wallet in qualified,
    )
    _shadow(adapter).record_observation(
        source_signature=signature,
        features=features,
        state=classify_fomo_state(features),
    )


def _sync_outcomes(adapter: FinalProfitFirstResearchAdapter) -> int:
    shadow = _shadow(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.entry_observed_at,o.signal_to_entry_seconds,o.net_return,"
            "s.venue,s.lifecycle,s.regime,s.state_json "
            "FROM profit_first_final_outcomes o "
            "JOIN fomo_shadow_observations s ON s.release_commit=? AND s.source_signature=o.source_signature "
            "LEFT JOIN fomo_shadow_outcomes f ON f.release_commit=? AND f.source_signature=o.source_signature "
            "WHERE o.epoch_id=? AND o.lane=? AND o.evidence_phase='forward' AND f.id IS NULL ORDER BY o.id LIMIT 500",
            (adapter.release_commit, adapter.release_commit, adapter.epoch_id, UNIFIED_LANE),
        ).fetchall()
    inserted = 0
    for row in rows:
        state = _safe_json(row["state_json"])
        shadow.record_outcome(
            FomoOutcome(
                source_signature=str(row["source_signature"]),
                observed_at=str(row["entry_observed_at"]),
                venue=str(row["venue"]),
                lifecycle=str(row["lifecycle"]),
                regime=str(row["regime"]),
                fomo_state=str(state.get("state") or "unknown"),
                signal_to_entry_seconds=float(row["signal_to_entry_seconds"]),
                net_return=float(row["net_return"]),
                release_commit=adapter.release_commit,
            )
        )
        inserted += 1
    return inserted


async def _observe_with_fomo(self: FinalProfitFirstResearchAdapter, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("FOMO runtime install missing original observe")
    await _ORIGINAL_OBSERVE(self, signature)
    try:
        _record_fomo_observation(self, signature)
        _set_error(self, None)
    except Exception as exc:
        _set_error(self, f"{type(exc).__name__}: {exc}")


async def _sell_with_fomo(self: FinalProfitFirstResearchAdapter, row: dict[str, Any]) -> None:
    if _ORIGINAL_SELL is None:
        raise RuntimeError("FOMO runtime install missing original sell")
    await _ORIGINAL_SELL(self, row)
    try:
        _sync_outcomes(self)
        _set_error(self, None)
    except Exception as exc:
        _set_error(self, f"{type(exc).__name__}: outcome sync failed: {exc}")


def _status_with_fomo(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("FOMO runtime install missing original status")
    payload = _ORIGINAL_STATUS(self)
    try:
        _sync_outcomes(self)
        status = _shadow(self).status()
        status["last_error"] = getattr(self, "_roi_fomo_last_error", None)
        status["collector_failure_isolated_from_strategy"] = True
        payload["fomo_continuation_shadow"] = status
    except Exception as exc:
        payload["fomo_continuation_shadow"] = {
            "research_version": "fomo-continuation-shadow-v1",
            "paper_only": True,
            "live_money_authority": False,
            "active_strategy_mutation_allowed": False,
            "collector_failure_isolated_from_strategy": True,
            "last_error": f"{type(exc).__name__}: {exc}",
        }
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
                "point_in_time_independent_entity_buyer_acceleration",
                "transaction_frequency_acceleration",
                "net_buy_flow_acceleration",
                "buy_sell_imbalance",
                "independent_demand_persistence",
                "dynamic_momentum_wallet_participation",
                "creator_accumulation_or_distribution",
                "early_holder_distribution",
                "chase_distance",
                "observation_latency",
                "quote_deterioration_when_available",
                "depth_growth_when_available",
                "exit_slippage_deterioration_when_available",
            ],
            "fomo_states": [
                "no_fomo",
                "pre_fomo",
                "active_fomo",
                "fomo_exhaustion",
                "late_or_inaccessible_fomo",
            ],
            "fomo_experiments": [
                "wallet_signal_only",
                "wallet_plus_entity_confirmation",
                "wallet_plus_fomo_acceleration",
                "pure_entity_flow_fomo",
            ],
            "fomo_venue_semantics": "market_state_overlay_not_platform",
            "fomo_outcome_source": "same_release_unified_forward_settlement",
            "fomo_strategy_authority": False,
            "fomo_historical_promotion_authority": False,
            "fomo_paper_only": True,
            "fomo_collector_failure_isolated_from_strategy": True,
        }
    )
    return payload


def install_fomo_runtime() -> None:
    global _ORIGINAL_OBSERVE, _ORIGINAL_SELL, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    current_observe = FinalProfitFirstResearchAdapter.observe
    if not bool(getattr(current_observe, "_roi_fomo_runtime", False)):
        _ORIGINAL_OBSERVE = current_observe
        _inherit_markers(_observe_with_fomo, current_observe)
        setattr(_observe_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter.observe = _observe_with_fomo  # type: ignore[method-assign]

    current_sell = FinalProfitFirstResearchAdapter._sell
    if not bool(getattr(current_sell, "_roi_fomo_runtime", False)):
        _ORIGINAL_SELL = current_sell
        _inherit_markers(_sell_with_fomo, current_sell)
        setattr(_sell_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter._sell = _sell_with_fomo  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_fomo_runtime", False)):
        _ORIGINAL_STATUS = current_status
        _inherit_markers(_status_with_fomo, current_status)
        setattr(_status_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter.status = _status_with_fomo  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_fomo_runtime", False)):
        _ORIGINAL_MANIFEST = current_manifest
        _inherit_markers(_manifest_with_fomo, current_manifest)
        setattr(_manifest_with_fomo, "_roi_fomo_runtime", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_fomo  # type: ignore[method-assign]


__all__ = ["INSTALL_VERSION", "install_fomo_runtime"]
