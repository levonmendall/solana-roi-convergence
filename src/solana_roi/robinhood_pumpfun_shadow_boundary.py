from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Callable

from .risk_conditioned_alpha_v5 import chase_band, latency_band, robust_return_profile
from .robinhood_chain_core import (
    HARVEST_FRACTION,
    KNOWN_NON_ACTORS,
    MAX_HOLD_SECONDS,
    MAX_IMMEDIATE_ROUND_TRIP_COST,
    PONS_V2_MEME_HOOK,
    STOP_LOSS_FRACTION,
    WETH,
    V2Curve,
    V3Pool,
    _clean_address,
    _finite,
    _utcnow,
)


SHADOW_BOUNDARY_VERSION = "robinhood-chain-pumpfun-shadow-boundary-v1"
MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY = 30
MIN_EXACT_CONTEXT_SAMPLES = 20
MIN_RELAXED_SAME_ENTITY_SAMPLES = 45
MAX_COPYABLE_CHASE_FRACTION = 0.15
MAX_COPYABLE_OBSERVATION_LATENCY_SECONDS = 20.0
SHADOW_PROBE_GRID = (0.005, 0.01, 0.02, 0.05)
ROBINHOOD_MAX_POSITION = 0.05
ROBINHOOD_MAX_OPEN_EXPOSURE = 0.20

_ORIGINAL_CHOOSE: Callable[..., Any] | None = None
_ORIGINAL_POLL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., Any] | None = None
_ORIGINAL_V2: Callable[..., Any] | None = None
_ORIGINAL_V3: Callable[..., Any] | None = None


def _ensure_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_robinhood_shadow_schema_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_v5_shadow_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_key TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "trigger_actor TEXT NOT NULL, trigger_entity TEXT NOT NULL, lane TEXT NOT NULL, role TEXT NOT NULL, regime TEXT NOT NULL, "
            "flow_state TEXT NOT NULL, risk_signature TEXT NOT NULL, risk_severity REAL NOT NULL, risk_json TEXT NOT NULL, "
            "context_key TEXT NOT NULL, candidate_lanes_json TEXT NOT NULL, probe_fraction REAL NOT NULL, "
            "entry_quote_in_wei TEXT NOT NULL, entry_token_raw TEXT NOT NULL, entry_gas_wei TEXT NOT NULL, "
            "entry_total_cost_wei TEXT NOT NULL, entry_price_eth REAL NOT NULL, immediate_exit_wei TEXT NOT NULL, "
            "round_trip_cost_fraction REAL NOT NULL, signal_price_eth REAL NOT NULL, executable_chase_fraction REAL NOT NULL, "
            "observation_latency_seconds REAL NOT NULL, opened_at TEXT NOT NULL, settled_at TEXT, exit_reason TEXT, "
            "paper_allocation_fraction REAL NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "paper_promotion_authority INTEGER NOT NULL, UNIQUE(release_commit,source_key,lane))"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_v5_shadow_open ON "
            "robinhood_v5_shadow_trials(release_commit,settled_at,id)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_v5_shadow_context ON "
            "robinhood_v5_shadow_trials(release_commit,context_key,lane,id)"
        )
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_v5_shadow_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, shadow_trial_id INTEGER NOT NULL UNIQUE, release_commit TEXT NOT NULL, "
            "strategy_version TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "trigger_entity TEXT NOT NULL, lane TEXT NOT NULL, regime TEXT NOT NULL, risk_signature TEXT NOT NULL, "
            "context_key TEXT NOT NULL, probe_fraction REAL NOT NULL, net_return REAL NOT NULL, exit_quote_out_wei TEXT NOT NULL, "
            "exit_gas_wei TEXT NOT NULL, exit_reason TEXT NOT NULL, settled_at TEXT NOT NULL, paper_allocation_fraction REAL NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, paper_promotion_authority INTEGER NOT NULL)"
        )
    setattr(self, "_roi_robinhood_shadow_schema_ready", True)


def _source_key(*, venue: str, token: str, actor: str, observed_ts: float, flow_state: str) -> str:
    raw = f"{venue}|{token}|{actor}|{observed_ts:.6f}|{flow_state}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _probe_fraction(source_key: str) -> float:
    index = int(hashlib.sha256(source_key.encode()).hexdigest()[:8], 16) % len(SHADOW_PROBE_GRID)
    return float(SHADOW_PROBE_GRID[index])


def _signal_reference(swaps: Any, actor: str) -> tuple[float, float] | None:
    actor = _clean_address(actor)
    for row in reversed(list(swaps or [])):
        if str(row.get("side") or "") != "buy":
            continue
        if _clean_address(str(row.get("actor") or "")) != actor:
            continue
        price = _finite(row.get("price_eth"))
        observed_ts = _finite(row.get("observed_ts"))
        if price is not None and price > 0.0 and observed_ts is not None and observed_ts > 0.0:
            return float(price), float(observed_ts)
    return None


def _execution_cost_band(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "unknown"
    numeric = max(0.0, float(value))
    if numeric <= 0.03:
        return "le_3pct"
    if numeric <= 0.07:
        return "3_7pct"
    if numeric <= 0.15:
        return "7_15pct"
    return "gt_15pct"


def _shadow_context_key(
    *,
    entity: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    role: str,
    risk_signature: str,
    flow_state: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> str:
    return "|".join(
        (
            str(entity),
            str(lane),
            str(venue),
            str(lifecycle),
            str(regime),
            str(role),
            str(risk_signature),
            str(flow_state),
            chase_band(chase_fraction),
            latency_band(latency_seconds),
            _execution_cost_band(round_trip_cost_fraction),
        )
    )


def copyability_assessment(
    quote: dict[str, Any] | None,
    *,
    signal_price_eth: float | None,
    signal_observed_ts: float | None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    if quote is None:
        return {
            "copyable": False,
            "blockers": ["entry_or_exit_quote_unavailable"],
            "executable_chase_fraction": None,
            "observation_latency_seconds": None,
            "round_trip_cost_fraction": None,
        }
    entry_price = _finite(quote.get("entry_price_eth"))
    immediate_exit = int(quote.get("immediate_exit_wei") or 0)
    round_trip = _finite(quote.get("round_trip_cost_fraction"))
    if entry_price is None or entry_price <= 0.0:
        blockers.append("entry_quote_unavailable")
    if immediate_exit <= 0:
        blockers.append("exit_quote_unavailable")
    if round_trip is None or round_trip > MAX_IMMEDIATE_ROUND_TRIP_COST:
        blockers.append("round_trip_cost_not_copyable")
    chase: float | None = None
    if signal_price_eth is None or signal_price_eth <= 0.0 or entry_price is None:
        blockers.append("signal_price_unavailable")
    else:
        chase = max(0.0, float(entry_price) / float(signal_price_eth) - 1.0)
        if chase > MAX_COPYABLE_CHASE_FRACTION:
            blockers.append("chase_above_15pct")
    latency: float | None = None
    if signal_observed_ts is None or signal_observed_ts <= 0.0:
        blockers.append("signal_observation_time_unavailable")
    else:
        latency = max(0.0, float(now_ts if now_ts is not None else time.time()) - float(signal_observed_ts))
        if latency > MAX_COPYABLE_OBSERVATION_LATENCY_SECONDS:
            blockers.append("observation_latency_above_20s")
    return {
        "copyable": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
        "executable_chase_fraction": chase,
        "observation_latency_seconds": latency,
        "round_trip_cost_fraction": round_trip,
    }


def _context_returns_shadow(
    self: Any,
    *,
    entity: str,
    role: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    flow_state: str,
    chase_fraction: float | None,
    latency_seconds: float | None,
    round_trip_cost_fraction: float | None,
) -> tuple[list[float], str]:
    _ensure_schema(self)
    key = _shadow_context_key(
        entity=entity,
        lane=lane,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        role=role,
        risk_signature=risk_signature,
        flow_state=flow_state,
        chase_fraction=chase_fraction,
        latency_seconds=latency_seconds,
        round_trip_cost_fraction=round_trip_cost_fraction,
    )
    with self.store._lock:
        exact = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_v5_shadow_outcomes o "
            "JOIN robinhood_v5_shadow_trials t ON t.id=o.shadow_trial_id "
            "WHERE t.release_commit=? AND t.strategy_version=? AND t.context_key=? ORDER BY o.id",
            (self.release_commit, SHADOW_BOUNDARY_VERSION, key),
        ).fetchall()
        if len(exact) >= MIN_EXACT_CONTEXT_SAMPLES:
            return [float(row["net_return"]) for row in exact], "shadow_exact_entity_context"
        same = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_v5_shadow_outcomes o "
            "JOIN robinhood_v5_shadow_trials t ON t.id=o.shadow_trial_id "
            "WHERE t.release_commit=? AND t.strategy_version=? AND t.trigger_entity=? AND t.role=? AND t.lane=? "
            "AND t.venue=? AND t.lifecycle=? AND t.regime=? AND t.risk_signature=? ORDER BY o.id",
            (
                self.release_commit,
                SHADOW_BOUNDARY_VERSION,
                entity,
                role,
                lane,
                venue,
                lifecycle,
                regime,
                risk_signature,
            ),
        ).fetchall()
        if len(same) >= MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY:
            return [float(row["net_return"]) for row in same], "shadow_same_entity_lane_venue_lifecycle_regime_risk"
        relaxed = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_v5_shadow_outcomes o "
            "JOIN robinhood_v5_shadow_trials t ON t.id=o.shadow_trial_id "
            "WHERE t.release_commit=? AND t.strategy_version=? AND t.trigger_entity=? AND t.role=? AND t.lane=? "
            "AND t.venue=? AND t.lifecycle=? AND t.risk_signature=? ORDER BY o.id",
            (
                self.release_commit,
                SHADOW_BOUNDARY_VERSION,
                entity,
                role,
                lane,
                venue,
                lifecycle,
                risk_signature,
            ),
        ).fetchall()
        if len(relaxed) >= MIN_RELAXED_SAME_ENTITY_SAMPLES:
            return [float(row["net_return"]) for row in relaxed], "shadow_same_entity_lane_venue_lifecycle_risk"
    return [float(row["net_return"]) for row in exact], "shadow_exact_entity_bootstrap" if exact else "none"


def _shadow_profile(self: Any, **context: Any) -> dict[str, Any]:
    values, source = _context_returns_shadow(self, **context)
    profile = robust_return_profile(
        values,
        grid=SHADOW_PROBE_GRID,
        max_fraction=ROBINHOOD_MAX_POSITION,
        min_samples=MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY,
    )
    return {
        "sample_count": profile.sample_count,
        "state": profile.state,
        "best_fraction": profile.best_fraction,
        "best_expected_log_growth": profile.best_expected_log_growth,
        "mean_return": profile.mean_return,
        "median_return": profile.median_return,
        "trimmed_mean_ex_best": profile.trimmed_mean_ex_best,
        "expected_shortfall_20": profile.expected_shortfall_20,
        "winner_concentration": profile.winner_concentration,
        "max_drawdown": profile.max_drawdown_at_best_fraction,
        "evidence_source": source,
        "evidence_phase": "zero_allocation_shadow_forward",
        "paper_allocation_fraction": 0.0,
    }


def _choose_with_shadow_boundary(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _ORIGINAL_CHOOSE is None:
        raise RuntimeError("Robinhood shadow boundary chooser is not installed")
    shadow_chase = kwargs.pop("shadow_chase_fraction", None)
    shadow_latency = kwargs.pop("shadow_latency_seconds", None)
    shadow_cost = kwargs.pop("shadow_round_trip_cost_fraction", None)
    _base_lane, _base_fraction, profiles = _ORIGINAL_CHOOSE(self, **kwargs)
    profiles = dict(profiles or {})
    promoted: list[tuple[str, dict[str, Any]]] = []
    for lane in list(kwargs.get("lanes") or []):
        profile = _shadow_profile(
            self,
            entity=str(kwargs.get("entity") or ""),
            role=str(kwargs.get("role") or ""),
            lane=str(lane),
            venue=str(kwargs.get("venue") or ""),
            lifecycle=str(kwargs.get("lifecycle") or ""),
            regime=str(kwargs.get("regime") or ""),
            risk_signature=str(kwargs.get("risk_signature") or "clean"),
            flow_state=str(kwargs.get("flow_state") or "neutral"),
            chase_fraction=_finite(shadow_chase),
            latency_seconds=_finite(shadow_latency),
            round_trip_cost_fraction=_finite(shadow_cost),
        )
        profiles[f"_shadow:{lane}"] = profile
        if (
            int(profile.get("sample_count") or 0) >= MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY
            and profile.get("state") == "promoted_positive_log_growth"
            and _finite(profile.get("mean_return")) is not None
            and float(profile["mean_return"]) > 0.0
            and _finite(profile.get("best_expected_log_growth")) is not None
            and float(profile["best_expected_log_growth"]) > 0.0
            and float(profile.get("best_fraction") or 0.0) > 0.0
        ):
            promoted.append((str(lane), profile))
    boundary = {
        "strategy_version": SHADOW_BOUNDARY_VERSION,
        "required_forward_outcomes": MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY,
        "paper_entry_eligible": False,
        "shadow_only_until_promoted": True,
        "bootstrap_paper_allocation_allowed": False,
        "historical_promotion_authority": False,
        "promotion_evidence": "zero_allocation_contextual_shadow_outcomes_only",
        "context_exactness": "entity_x_lane_x_venue_x_lifecycle_x_regime_x_role_x_risk_x_flow_x_chase_x_latency_x_execution_cost",
        "sequence": [
            "wallet_signal",
            "opportunity_classification",
            "executable_copyable_test",
            "contextual_forward_evidence",
            "positive_geometric_edge",
            "sizing",
            "paper_entry",
        ],
    }
    if not promoted:
        profiles["_robinhood_shadow_boundary"] = boundary
        return None, 0.0, profiles
    lane, profile = max(
        promoted,
        key=lambda item: float(item[1].get("best_expected_log_growth") or float("-inf")),
    )
    fraction = float(profile["best_fraction"])
    regime = str(kwargs.get("regime") or "neutral")
    try:
        fraction *= float(self._v5_regime_multiplier(regime))
    except Exception:
        pass
    risk_severity = max(0.0, min(1.0, float(kwargs.get("risk_severity") or 0.0)))
    fraction *= max(0.30, 1.0 - 0.60 * risk_severity)
    if lane == "hazard_continuation":
        fraction = min(0.02, fraction)
    fraction = min(ROBINHOOD_MAX_POSITION, max(0.0, fraction))
    try:
        available = max(0.0, ROBINHOOD_MAX_OPEN_EXPOSURE - float(self._open_exposure()))
        fraction = min(fraction, available)
    except Exception:
        fraction = 0.0
    if fraction <= 0.0:
        profiles["_robinhood_shadow_boundary"] = boundary
        return None, 0.0, profiles
    boundary.update(
        {
            "paper_entry_eligible": True,
            "selected_lane": lane,
            "shadow_sample_count": int(profile.get("sample_count") or 0),
            "shadow_mean_return": profile.get("mean_return"),
            "shadow_expected_log_growth": profile.get("best_expected_log_growth"),
            "shadow_evidence_source": profile.get("evidence_source"),
            "sized_position_fraction": fraction,
        }
    )
    profiles["_robinhood_shadow_boundary"] = boundary
    return lane, fraction, profiles


def _insert_shadow_trials(
    self: Any,
    *,
    source_key: str,
    token: str,
    market: str,
    venue: str,
    lifecycle: str,
    actor: str,
    entity: str,
    role: str,
    regime: str,
    flow_state: str,
    risk: dict[str, Any],
    lanes: list[str],
    quote: dict[str, Any],
    probe_fraction: float,
    signal_price_eth: float,
    chase_fraction: float,
    latency_seconds: float,
) -> None:
    _ensure_schema(self)
    now = _utcnow()
    with self.store._lock, self.store.db:
        for lane in lanes:
            context_key = _shadow_context_key(
                entity=entity,
                lane=lane,
                venue=venue,
                lifecycle=lifecycle,
                regime=regime,
                role=role,
                risk_signature=str(risk["risk_signature"]),
                flow_state=flow_state,
                chase_fraction=chase_fraction,
                latency_seconds=latency_seconds,
                round_trip_cost_fraction=float(quote["round_trip_cost_fraction"]),
            )
            self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_v5_shadow_trials("
                "release_commit,strategy_version,source_key,token,market,venue,lifecycle,trigger_actor,trigger_entity,lane,role,regime,"
                "flow_state,risk_signature,risk_severity,risk_json,context_key,candidate_lanes_json,probe_fraction,entry_quote_in_wei,"
                "entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,immediate_exit_wei,round_trip_cost_fraction,"
                "signal_price_eth,executable_chase_fraction,observation_latency_seconds,opened_at,paper_allocation_fraction,paper_only,"
                "live_money_authority,paper_promotion_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0.0,1,0,0)",
                (
                    self.release_commit,
                    SHADOW_BOUNDARY_VERSION,
                    source_key,
                    token,
                    market,
                    venue,
                    lifecycle,
                    actor,
                    entity,
                    lane,
                    role,
                    regime,
                    flow_state,
                    str(risk["risk_signature"]),
                    float(risk["risk_severity"]),
                    json.dumps(risk, sort_keys=True),
                    context_key,
                    json.dumps(lanes),
                    float(probe_fraction),
                    str(int(quote["amount_in_wei"])),
                    str(int(quote["token_out"])),
                    str(int(quote["entry_gas_wei"])),
                    str(int(quote["entry_total_cost_wei"])),
                    float(quote["entry_price_eth"]),
                    str(int(quote["immediate_exit_wei"])),
                    float(quote["round_trip_cost_fraction"]),
                    float(signal_price_eth),
                    float(chase_fraction),
                    float(latency_seconds),
                    now,
                ),
            )


async def _v2_quote(self: Any, curve: V2Curve, fraction: float, eth_usd: float) -> tuple[dict[str, Any] | None, bool]:
    amount_in = int((self._paper_nav_usd() * fraction / eth_usd) * 1e18)
    if amount_in <= 0:
        return None, False
    try:
        buy = await self.rpc.pons_v2_curve_quote(curve=curve.curve, quote_in=amount_in, recipient=self.paper_recipient)
        if int(buy["tokens_out"]) <= 0:
            return None, False
        high_snipe_tax = int(buy.get("snipe_tax_bps") or 0) > 500
        exit_out = await self.rpc.pons_v2_curve_sell_quote(curve=curve.curve, tokens_in=buy["tokens_out"])
        gas_price = await self.rpc.gas_price()
        entry_gas_wei = 220_000 * gas_price
        exit_gas_wei = 220_000 * gas_price
        total_cost = int(buy["spent"]) + entry_gas_wei
        immediate_net = max(0, int(exit_out) - exit_gas_wei)
        round_trip = 1.0 - immediate_net / max(1, total_cost)
        return (
            {
                "amount_in_wei": int(buy["spent"]),
                "token_out": int(buy["tokens_out"]),
                "entry_gas_wei": entry_gas_wei,
                "exit_gas_wei": exit_gas_wei,
                "entry_total_cost_wei": total_cost,
                "immediate_exit_wei": immediate_net,
                "round_trip_cost_fraction": round_trip,
                "entry_price_eth": (int(buy["spent"]) / 1e18) / (int(buy["tokens_out"]) / 1e18),
            },
            high_snipe_tax,
        )
    except Exception:
        return None, False


def _risk_descriptor_for_runtime(**kwargs: Any) -> dict[str, Any]:
    # v5.1 patches the Robinhood module's risk_descriptor with its stricter unknown-
    # hard-stop behavior. Resolve it at call time so the shadow path cannot bypass it.
    from . import robinhood_chain_profit_maximizer as rh

    return rh.risk_descriptor(**kwargs)


async def _maybe_open_v3_shadow(self: Any, pool: V3Pool, *, current_block: int) -> None:
    if not self._caught_up or self._token_open(pool.token):
        return
    if pool.restrictions_end_block and current_block <= pool.restrictions_end_block:
        return
    if pool.venue == "UNISWAP_V3_DIRECT" and not await self._direct_v3_token_allowed(pool.token):
        return
    metrics = await self._v5_flow_metrics(pool.recent_swaps, deployer=pool.deployer)
    if not bool(metrics.get("entity_resolution_complete")):
        return
    actor = _clean_address(str(metrics.get("trigger_actor") or ""))
    entity = _clean_address(str(metrics.get("trigger_entity") or ""))
    if not actor or not entity or actor in KNOWN_NON_ACTORS:
        return
    signal = _signal_reference(pool.recent_swaps, actor)
    if signal is None:
        return
    signal_price, signal_ts = signal
    role = "creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
    lifecycle = "post_protection_v3" if pool.venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
    extra: list[str] = []
    if float(metrics.get("creator_sell_pressure") or 0.0) >= 0.25:
        extra.append("creator_distributing")
    risk = _risk_descriptor_for_runtime(
        soft_flags=(),
        hard_flags=(),
        creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
        creator_linked_trigger=role == "creator_deployer",
        extra_hazards=extra,
    )
    if not bool(risk.get("structurally_tradeable", True)):
        return
    regime = self._v5_regime(metrics)
    lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=None)
    if not lanes:
        return
    source = _source_key(venue=pool.venue, token=pool.token, actor=actor, observed_ts=signal_ts, flow_state=str(metrics["state"]))
    probe_fraction = _probe_fraction(source)
    try:
        probe_quote = await self._quote_v3_round_trip(pool, probe_fraction)
    except Exception:
        return
    assessment = copyability_assessment(
        probe_quote,
        signal_price_eth=signal_price,
        signal_observed_ts=signal_ts,
        now_ts=time.time(),
    )
    if not bool(assessment["copyable"]):
        return
    assert probe_quote is not None
    _insert_shadow_trials(
        self,
        source_key=source,
        token=pool.token,
        market=pool.pool,
        venue=pool.venue,
        lifecycle=lifecycle,
        actor=actor,
        entity=entity,
        role=role,
        regime=regime,
        flow_state=str(metrics["state"]),
        risk=risk,
        lanes=lanes,
        quote=probe_quote,
        probe_fraction=probe_fraction,
        signal_price_eth=signal_price,
        chase_fraction=float(assessment["executable_chase_fraction"]),
        latency_seconds=float(assessment["observation_latency_seconds"]),
    )
    lane, fraction, _ = self._v5_choose_lane_fraction(
        entity=entity,
        role=role,
        venue=pool.venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_signature=str(risk["risk_signature"]),
        risk_severity=float(risk["risk_severity"]),
        flow_state=str(metrics["state"]),
        lanes=lanes,
        shadow_chase_fraction=assessment["executable_chase_fraction"],
        shadow_latency_seconds=assessment["observation_latency_seconds"],
        shadow_round_trip_cost_fraction=assessment["round_trip_cost_fraction"],
    )
    if not lane or fraction <= 0.0:
        return
    for attempt in range(2):
        try:
            quote = await self._quote_v3_round_trip(pool, fraction)
        except Exception:
            return
        actual = copyability_assessment(
            quote,
            signal_price_eth=signal_price,
            signal_observed_ts=signal_ts,
            now_ts=time.time(),
        )
        if not bool(actual["copyable"]):
            return
        new_lane, new_fraction, _ = self._v5_choose_lane_fraction(
            entity=entity,
            role=role,
            venue=pool.venue,
            lifecycle=lifecycle,
            regime=regime,
            risk_signature=str(risk["risk_signature"]),
            risk_severity=float(risk["risk_severity"]),
            flow_state=str(metrics["state"]),
            lanes=lanes,
            shadow_chase_fraction=actual["executable_chase_fraction"],
            shadow_latency_seconds=actual["observation_latency_seconds"],
            shadow_round_trip_cost_fraction=actual["round_trip_cost_fraction"],
        )
        if not new_lane or new_fraction <= 0.0:
            return
        if attempt == 0 and (new_lane != lane or abs(new_fraction - fraction) > 1e-6):
            lane, fraction = new_lane, new_fraction
            continue
        self._v5_insert_trial(
            token=pool.token,
            market=pool.pool,
            venue=pool.venue,
            lifecycle=lifecycle,
            trigger_actor=actor,
            trigger_entity=entity,
            flow_state=str(metrics["state"]),
            fraction=fraction,
            quote=quote,
            lane=new_lane,
            role=role,
            regime=regime,
            risk=risk,
            lifecycle_progress=None,
            threshold_challenger=False,
            candidate_lanes=lanes,
        )
        return


async def _maybe_open_v2_shadow(self: Any, curve: V2Curve) -> None:
    if not self._caught_up or self._token_open(curve.token):
        return
    metrics = await self._v5_flow_metrics(curve.recent_swaps, deployer=curve.deployer)
    if not bool(metrics.get("entity_resolution_complete")):
        return
    actor = _clean_address(str(metrics.get("trigger_actor") or ""))
    entity = _clean_address(str(metrics.get("trigger_entity") or ""))
    if not actor or not entity or actor in KNOWN_NON_ACTORS:
        return
    signal = _signal_reference(curve.recent_swaps, actor)
    if signal is None:
        return
    signal_price, signal_ts = signal
    role = "creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
    eth_usd = await self._eth_usd()
    if eth_usd is None or eth_usd <= 0:
        return
    try:
        state = await self.rpc.pons_v2_launch_state(curve.token)
        if int(state["phase"]) != 0:
            return
        real_quote = await self.rpc.call_uint(curve.curve, "realQuoteReserve()")
        threshold = max(1, int(state["graduation_threshold"] or curve.graduation_threshold))
        progress = real_quote / threshold
    except Exception:
        return
    extra: list[str] = []
    if progress >= 0.85:
        extra.append("late_lifecycle")
    if float(metrics.get("creator_sell_pressure") or 0.0) >= 0.25:
        extra.append("creator_distributing")
    risk = _risk_descriptor_for_runtime(
        soft_flags=(),
        hard_flags=(),
        creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
        creator_linked_trigger=role == "creator_deployer",
        extra_hazards=extra,
    )
    if not bool(risk.get("structurally_tradeable", True)):
        return
    regime = self._v5_regime(metrics)
    lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
    if not lanes:
        return
    source = _source_key(venue="PONS_V2_CURVE", token=curve.token, actor=actor, observed_ts=signal_ts, flow_state=str(metrics["state"]))
    probe_fraction = _probe_fraction(source)
    probe_quote, high_snipe_tax = await _v2_quote(self, curve, probe_fraction, float(eth_usd))
    if high_snipe_tax and "high_snipe_tax" not in extra:
        extra.append("high_snipe_tax")
        risk = _risk_descriptor_for_runtime(
            soft_flags=(),
            hard_flags=(),
            creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
            creator_linked_trigger=role == "creator_deployer",
            extra_hazards=extra,
        )
        if not bool(risk.get("structurally_tradeable", True)):
            return
        lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
    assessment = copyability_assessment(
        probe_quote,
        signal_price_eth=signal_price,
        signal_observed_ts=signal_ts,
        now_ts=time.time(),
    )
    if not bool(assessment["copyable"]):
        return
    assert probe_quote is not None
    _insert_shadow_trials(
        self,
        source_key=source,
        token=curve.token,
        market=curve.curve,
        venue="PONS_V2_CURVE",
        lifecycle="bonding_curve",
        actor=actor,
        entity=entity,
        role=role,
        regime=regime,
        flow_state=str(metrics["state"]),
        risk=risk,
        lanes=lanes,
        quote=probe_quote,
        probe_fraction=probe_fraction,
        signal_price_eth=signal_price,
        chase_fraction=float(assessment["executable_chase_fraction"]),
        latency_seconds=float(assessment["observation_latency_seconds"]),
    )
    lane, fraction, _ = self._v5_choose_lane_fraction(
        entity=entity,
        role=role,
        venue="PONS_V2_CURVE",
        lifecycle="bonding_curve",
        regime=regime,
        risk_signature=str(risk["risk_signature"]),
        risk_severity=float(risk["risk_severity"]),
        flow_state=str(metrics["state"]),
        lanes=lanes,
        shadow_chase_fraction=assessment["executable_chase_fraction"],
        shadow_latency_seconds=assessment["observation_latency_seconds"],
        shadow_round_trip_cost_fraction=assessment["round_trip_cost_fraction"],
    )
    if not lane or fraction <= 0.0:
        return
    for attempt in range(2):
        quote, high_tax = await _v2_quote(self, curve, fraction, float(eth_usd))
        if high_tax and "high_snipe_tax" not in extra:
            extra.append("high_snipe_tax")
            risk = _risk_descriptor_for_runtime(
                soft_flags=(),
                hard_flags=(),
                creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
                creator_linked_trigger=role == "creator_deployer",
                extra_hazards=extra,
            )
            lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
        actual = copyability_assessment(
            quote,
            signal_price_eth=signal_price,
            signal_observed_ts=signal_ts,
            now_ts=time.time(),
        )
        if not bool(actual["copyable"]):
            return
        new_lane, new_fraction, _ = self._v5_choose_lane_fraction(
            entity=entity,
            role=role,
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            regime=regime,
            risk_signature=str(risk["risk_signature"]),
            risk_severity=float(risk["risk_severity"]),
            flow_state=str(metrics["state"]),
            lanes=lanes,
            shadow_chase_fraction=actual["executable_chase_fraction"],
            shadow_latency_seconds=actual["observation_latency_seconds"],
            shadow_round_trip_cost_fraction=actual["round_trip_cost_fraction"],
        )
        if not new_lane or new_fraction <= 0.0:
            return
        if attempt == 0 and (new_lane != lane or abs(new_fraction - fraction) > 1e-6):
            lane, fraction = new_lane, new_fraction
            continue
        assert quote is not None
        self._v5_insert_trial(
            token=curve.token,
            market=curve.curve,
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            trigger_actor=actor,
            trigger_entity=entity,
            flow_state=str(metrics["state"]),
            fraction=fraction,
            quote=quote,
            lane=new_lane,
            role=role,
            regime=regime,
            risk=risk,
            lifecycle_progress=progress,
            threshold_challenger=progress >= 0.85,
            candidate_lanes=lanes,
        )
        return


async def _settle_shadow_one(self: Any, trial: dict[str, Any]) -> None:
    opened = datetime.fromisoformat(str(trial["opened_at"]))
    elapsed = max(0.0, (datetime.now(timezone.utc) - opened).total_seconds())
    token = _clean_address(str(trial["token"]))
    market = _clean_address(str(trial["market"]))
    token_amount = int(trial["entry_token_raw"])
    total_cost = int(trial["entry_total_cost_wei"])
    venue = str(trial["venue"])
    gas_price = await self.rpc.gas_price()
    exit_out: int | None = None
    exit_gas_wei = 0
    flow_state = str(trial["flow_state"])
    if venue in {"PONS_V1_UNISWAP_V3", "UNISWAP_V3_DIRECT"}:
        pool = self.v3_pools.get(market)
        if pool is None:
            if elapsed < MAX_HOLD_SECONDS:
                return
            exit_out = 0
        else:
            try:
                raw_out, gas_estimate = await self.rpc.v3_quote_exact_input(
                    token_in=token,
                    token_out=WETH,
                    fee=pool.fee,
                    amount_in=token_amount,
                )
                exit_gas_wei = (gas_estimate + 80_000) * gas_price
                exit_out = max(0, raw_out - exit_gas_wei)
            except Exception:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            metrics = await self._v5_flow_metrics(pool.recent_swaps, deployer=pool.deployer)
            flow_state = str(metrics.get("state") or flow_state)
    elif venue == "PONS_V2_CURVE":
        curve = self.v2_curves.get(market)
        if curve is None:
            if elapsed < MAX_HOLD_SECONDS:
                return
            exit_out = 0
        else:
            try:
                state = await self.rpc.pons_v2_launch_state(token)
            except Exception:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                state = {"phase": -1}
            if int(state["phase"]) == 0:
                try:
                    raw_out = await self.rpc.pons_v2_curve_sell_quote(curve=market, tokens_in=token_amount)
                    exit_gas_wei = 220_000 * gas_price
                    exit_out = max(0, raw_out - exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            elif int(state["phase"]) == 1:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            elif int(state["phase"]) == 2:
                pair = _clean_address(str(state["pair_token"]))
                currency_a = "0x0000000000000000000000000000000000000000" if pair in {"", "0x0000000000000000000000000000000000000000"} else pair
                currency0, currency1 = sorted([currency_a, token], key=lambda item: int(item, 16))
                try:
                    raw_out, gas_estimate = await self.rpc.v4_quote_exact_input(
                        currency0=currency0,
                        currency1=currency1,
                        fee=int(state["pool_fee"]),
                        tick_spacing=int(state["tick_spacing"]),
                        hooks=PONS_V2_MEME_HOOK,
                        zero_for_one=token == currency0,
                        amount_in=token_amount,
                    )
                    exit_gas_wei = (gas_estimate + 120_000) * gas_price
                    exit_out = max(0, raw_out - exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            else:
                exit_out = 0
            metrics = await self._v5_flow_metrics(curve.recent_swaps, deployer=curve.deployer)
            flow_state = str(metrics.get("state") or flow_state)
    else:
        return
    if exit_out is None:
        return
    net_return = exit_out / max(1, total_cost) - 1.0
    if net_return <= STOP_LOSS_FRACTION:
        reason = "shadow:stop_loss"
    elif net_return >= HARVEST_FRACTION:
        reason = "shadow:harvest"
    elif flow_state == "exhaustion" and elapsed >= 30.0:
        reason = "shadow:flow_exhaustion"
    elif elapsed >= MAX_HOLD_SECONDS:
        reason = "shadow:max_hold"
    else:
        return
    settled = _utcnow()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "INSERT OR IGNORE INTO robinhood_v5_shadow_outcomes("
            "shadow_trial_id,release_commit,strategy_version,token,market,venue,lifecycle,trigger_entity,lane,regime,risk_signature,"
            "context_key,probe_fraction,net_return,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_allocation_fraction,"
            "paper_only,live_money_authority,paper_promotion_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0.0,1,0,0)",
            (
                int(trial["id"]),
                self.release_commit,
                SHADOW_BOUNDARY_VERSION,
                token,
                market,
                venue,
                str(trial["lifecycle"]),
                str(trial["trigger_entity"]),
                str(trial["lane"]),
                str(trial["regime"]),
                str(trial["risk_signature"]),
                str(trial["context_key"]),
                float(trial["probe_fraction"]),
                float(net_return),
                str(int(exit_out)),
                str(int(exit_gas_wei)),
                reason,
                settled,
            ),
        )
        self.store.db.execute(
            "UPDATE robinhood_v5_shadow_trials SET settled_at=?,exit_reason=? WHERE id=?",
            (settled, reason, int(trial["id"])),
        )


async def _settle_shadow_trials(self: Any) -> None:
    _ensure_schema(self)
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM robinhood_v5_shadow_trials WHERE release_commit=? AND strategy_version=? AND settled_at IS NULL "
            "ORDER BY id LIMIT 120",
            (self.release_commit, SHADOW_BOUNDARY_VERSION),
        ).fetchall()
    for row in rows:
        try:
            await _settle_shadow_one(self, dict(row))
        except asyncio.CancelledError:
            raise
        except Exception:
            continue


async def _poll_once_with_shadow_settlement(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood shadow boundary poll wrapper is not installed")
    await _ORIGINAL_POLL(self)
    await _settle_shadow_trials(self)


def _status_with_shadow_boundary(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood shadow boundary status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        _ensure_schema(self)
        with self.store._lock:
            totals = self.store.db.execute(
                "SELECT COUNT(*) total,SUM(CASE WHEN settled_at IS NULL THEN 1 ELSE 0 END) open_count,"
                "SUM(CASE WHEN settled_at IS NOT NULL THEN 1 ELSE 0 END) settled_count "
                "FROM robinhood_v5_shadow_trials WHERE release_commit=? AND strategy_version=?",
                (self.release_commit, SHADOW_BOUNDARY_VERSION),
            ).fetchone()
            outcomes = self.store.db.execute(
                "SELECT COUNT(*) count,AVG(net_return) mean_return FROM robinhood_v5_shadow_outcomes "
                "WHERE release_commit=? AND strategy_version=?",
                (self.release_commit, SHADOW_BOUNDARY_VERSION),
            ).fetchone()
        payload["strategy_pipeline"] = {
            "version": SHADOW_BOUNDARY_VERSION,
            "sequence": [
                "wallet_signal",
                "opportunity_classification",
                "executable_copyable_test",
                "contextual_forward_evidence",
                "positive_geometric_edge",
                "sizing",
                "paper_entry",
            ],
            "shadow_boundary": True,
            "minimum_closed_shadow_outcomes_for_paper_entry": MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY,
            "bootstrap_paper_allocation_allowed": False,
            "shadow_paper_allocation_fraction": 0.0,
            "promotion_evidence": "zero_allocation_contextual_shadow_outcomes_only",
            "context_exactness": "entity_x_lane_x_venue_x_lifecycle_x_regime_x_role_x_risk_x_flow_x_chase_x_latency_x_execution_cost",
            "max_copyable_chase_fraction": MAX_COPYABLE_CHASE_FRACTION,
            "max_copyable_observation_latency_seconds": MAX_COPYABLE_OBSERVATION_LATENCY_SECONDS,
            "historical_evidence_promotion_authority": False,
            "legacy_paper_context_learning_preserved_for_diagnostics": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "new_wallet_specific_provider_requests_added": 0,
            "shadow_trials": int(totals["total"] or 0) if totals else 0,
            "open_shadow_trials": int(totals["open_count"] or 0) if totals else 0,
            "settled_shadow_trials": int(totals["settled_count"] or 0) if totals else 0,
            "shadow_outcomes": int(outcomes["count"] or 0) if outcomes else 0,
            "mean_shadow_return": float(outcomes["mean_return"]) if outcomes and outcomes["mean_return"] is not None else None,
        }
    except Exception as exc:
        payload["strategy_pipeline"] = {
            "version": SHADOW_BOUNDARY_VERSION,
            "shadow_boundary": True,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: shadow strategy status unavailable",
            "bootstrap_paper_allocation_allowed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_pumpfun_shadow_boundary(plane_cls: type[Any]) -> None:
    global _ORIGINAL_CHOOSE, _ORIGINAL_POLL, _ORIGINAL_STATUS, _ORIGINAL_V2, _ORIGINAL_V3
    if bool(getattr(plane_cls, "_roi_robinhood_pumpfun_shadow_boundary_installed", False)):
        return
    _ORIGINAL_CHOOSE = plane_cls._v5_choose_lane_fraction
    _ORIGINAL_POLL = plane_cls._poll_once
    _ORIGINAL_STATUS = plane_cls.status
    _ORIGINAL_V2 = plane_cls._maybe_open_v2
    _ORIGINAL_V3 = plane_cls._maybe_open_v3

    # Keep existing v5.1/base method lineage visible to protected composition checks.
    # The shadow implementation replaces decision behavior but does not replace the
    # existing cross-release _v5_context_returns learning method.
    _maybe_open_v2_shadow.__module__ = _ORIGINAL_V2.__module__
    _maybe_open_v3_shadow.__module__ = _ORIGINAL_V3.__module__
    _choose_with_shadow_boundary.__module__ = _ORIGINAL_CHOOSE.__module__

    plane_cls._v5_choose_lane_fraction = _choose_with_shadow_boundary  # type: ignore[method-assign]
    plane_cls._maybe_open_v3 = _maybe_open_v3_shadow  # type: ignore[method-assign]
    plane_cls._maybe_open_v2 = _maybe_open_v2_shadow  # type: ignore[method-assign]
    plane_cls._poll_once = _poll_once_with_shadow_settlement  # type: ignore[method-assign]
    plane_cls.status = _status_with_shadow_boundary  # type: ignore[method-assign]

    if bool(getattr(_ORIGINAL_CHOOSE, "_roi_robinhood_entity_universe", False)):
        setattr(plane_cls._v5_choose_lane_fraction, "_roi_robinhood_entity_universe", True)
    if bool(getattr(_ORIGINAL_POLL, "_roi_robinhood_entity_universe", False)):
        setattr(plane_cls._poll_once, "_roi_robinhood_entity_universe", True)
    if bool(getattr(_ORIGINAL_STATUS, "_roi_robinhood_entity_universe", False)):
        setattr(plane_cls.status, "_roi_robinhood_entity_universe", True)
    setattr(plane_cls._v5_choose_lane_fraction, "_roi_robinhood_pumpfun_shadow_boundary", True)

    setattr(plane_cls, "_roi_robinhood_pumpfun_shadow_boundary_installed", True)
    setattr(plane_cls, "_roi_robinhood_pumpfun_shadow_boundary_version", SHADOW_BOUNDARY_VERSION)


__all__ = [
    "SHADOW_BOUNDARY_VERSION",
    "MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY",
    "MAX_COPYABLE_CHASE_FRACTION",
    "MAX_COPYABLE_OBSERVATION_LATENCY_SECONDS",
    "copyability_assessment",
    "install_robinhood_pumpfun_shadow_boundary",
]
