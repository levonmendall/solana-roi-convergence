from __future__ import annotations

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from statistics import median
from typing import Any

from .risk_conditioned_alpha_v5 import (
    HAZARD_WEIGHTS,
    latency_band,
    robust_return_profile,
    risk_descriptor,
)
from .robinhood_chain_core import *


ROBINHOOD_V5_VERSION = "robinhood-chain-risk-conditioned-v2"
ROBINHOOD_V5_MIN_SAMPLES = 30
ROBINHOOD_V5_POSITION_GRID = (0.005, 0.01, 0.02, 0.05)
ROBINHOOD_V5_MAX_POSITION = 0.05
ROBINHOOD_V5_MAX_OPEN_EXPOSURE = 0.20


class RobinhoodProfitMaximizerMixin:
    """Risk-conditioned active-paper policy layered over the exact Chain 4663 plane.

    This mixin deliberately has no signing or submission method. It consumes the
    existing read-only chain observations and exact executable quote paths, then
    separates creator, entity-flow, FOMO, lifecycle-transition and hazard evidence.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._v5_schema()

    def _v5_schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_v5_trial_context ("
                "trial_id INTEGER PRIMARY KEY, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
                "lane TEXT NOT NULL, trigger_role TEXT NOT NULL, regime TEXT NOT NULL, flow_state TEXT NOT NULL, "
                "risk_signature TEXT NOT NULL, risk_severity REAL NOT NULL, risk_json TEXT NOT NULL, "
                "context_key TEXT NOT NULL, latency_band TEXT NOT NULL, lifecycle_progress REAL, "
                "threshold_challenger INTEGER NOT NULL, candidate_lanes_json TEXT NOT NULL, created_at TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_robinhood_v5_context_key ON "
                "robinhood_v5_trial_context(release_commit,context_key,trial_id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS robinhood_v5_marks ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, trial_id INTEGER NOT NULL, "
                "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, lane TEXT NOT NULL, elapsed_seconds REAL NOT NULL, "
                "net_return REAL NOT NULL, flow_state TEXT NOT NULL, observed_at TEXT NOT NULL, "
                "UNIQUE(release_commit,trial_id,observed_at))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_robinhood_v5_marks_trial ON "
                "robinhood_v5_marks(release_commit,trial_id,id)"
            )

    async def _v5_flow_metrics(self, swaps: Any, *, deployer: str = "") -> dict[str, Any]:
        now_ts = time.time()
        current = [s for s in swaps if now_ts - float(s.get("observed_ts") or 0.0) <= 60.0]
        prior = [s for s in swaps if 60.0 < now_ts - float(s.get("observed_ts") or 0.0) <= 120.0]
        buys = [s for s in current if s.get("side") == "buy"]
        sells = [s for s in current if s.get("side") == "sell"]
        prior_buys = [s for s in prior if s.get("side") == "buy"]
        actors: list[str] = []
        for swap in buys:
            actor = _clean_address(str(swap.get("actor") or ""))
            if actor and actor not in KNOWN_NON_ACTORS and actor not in actors:
                actors.append(actor)
        actors = actors[-12:]
        anchors = await asyncio.gather(*(self._entity_anchor(actor) for actor in actors)) if actors else []
        if any(anchor is None for anchor in anchors):
            return {
                "state": "entity_resolution_incomplete",
                "entity_resolution_complete": False,
                "trigger_actor": "",
                "trigger_entity": "",
                "independent_entities_60s": 0,
                "buy_sell_quote_ratio": 0.0,
                "buy_count_acceleration": 0.0,
                "buy_quote_wei": 0,
                "sell_quote_wei": 0,
                "creator_sell_quote_wei": 0,
            }
        mapping = {actor: str(anchor) for actor, anchor in zip(actors, anchors) if anchor}
        deployer = _clean_address(deployer)
        deployer_anchor = await self._entity_anchor(deployer) if deployer else None
        trigger_actor = _clean_address(str(buys[-1].get("actor") or "")) if buys else ""
        trigger_entity = mapping.get(trigger_actor, trigger_actor)
        independent = {anchor for anchor in mapping.values() if anchor and anchor != deployer_anchor}
        buy_quote = sum(int(s.get("quote_amount_wei") or 0) for s in buys)
        sell_quote = sum(int(s.get("quote_amount_wei") or 0) for s in sells)
        creator_sell_quote = sum(
            int(s.get("quote_amount_wei") or 0)
            for s in sells
            if deployer and _clean_address(str(s.get("actor") or "")) == deployer
        )
        ratio = buy_quote / max(1, sell_quote)
        acceleration = len(buys) / max(1, len(prior_buys))
        prices = [float(s["price_eth"]) for s in current if _finite(s.get("price_eth")) not in (None, 0.0)]
        price_change = prices[-1] / prices[0] - 1.0 if len(prices) >= 2 and prices[0] > 0 else 0.0
        if len(buys) >= 4 and len(independent) >= 3 and ratio >= 1.5 and acceleration >= 1.25 and 0.01 <= price_change <= 0.40:
            state = "active_fomo"
        elif len(buys) >= 3 and len(independent) >= 2 and ratio >= 1.15 and price_change <= 0.40:
            state = "pre_fomo"
        elif len(independent) >= 2 and buy_quote > sell_quote:
            state = "entity_accumulation"
        elif sells and sell_quote > buy_quote:
            state = "exhaustion"
        else:
            state = "neutral"
        return {
            "state": state,
            "entity_resolution_complete": True,
            "trigger_actor": trigger_actor,
            "trigger_entity": trigger_entity,
            "trigger_is_creator": bool(deployer_anchor and trigger_entity == deployer_anchor),
            "deployer_entity": deployer_anchor or "",
            "independent_entities_60s": len(independent),
            "buy_count_60s": len(buys),
            "sell_count_60s": len(sells),
            "buy_sell_quote_ratio": ratio,
            "buy_count_acceleration": acceleration,
            "price_change_60s": price_change,
            "buy_quote_wei": buy_quote,
            "sell_quote_wei": sell_quote,
            "creator_sell_quote_wei": creator_sell_quote,
            "creator_sell_pressure": creator_sell_quote / max(1, buy_quote),
        }

    @staticmethod
    def _v5_regime(metrics: dict[str, Any]) -> str:
        buys = int(metrics.get("buy_count_60s") or 0)
        sells = int(metrics.get("sell_count_60s") or 0)
        independent = int(metrics.get("independent_entities_60s") or 0)
        if sells > buys and sells >= 3:
            return "weak_or_deteriorating"
        if buys >= 8 and independent >= 5 and buys >= 2 * max(1, sells):
            return "broad_mania"
        if buys > sells and independent >= 3:
            return "high_speculation"
        return "neutral"

    @staticmethod
    def _v5_regime_multiplier(regime: str) -> float:
        if regime == "weak_or_deteriorating":
            return 0.50
        if regime == "high_speculation":
            return 1.10
        if regime == "broad_mania":
            return 0.85
        return 1.0

    @staticmethod
    def _v5_risk_bin(severity: float) -> str:
        if severity < 0.20:
            return "low"
        if severity < 0.45:
            return "moderate"
        if severity < 0.70:
            return "high"
        return "extreme"

    def _v5_context_key(
        self,
        *,
        entity: str,
        role: str,
        lane: str,
        venue: str,
        lifecycle: str,
        regime: str,
        risk_signature: str,
        flow_state: str,
        latency: str = "chain_poll",
    ) -> str:
        return "|".join((entity, role, lane, venue, lifecycle, regime, risk_signature, flow_state, latency))

    def _v5_context_returns(
        self,
        *,
        entity: str,
        role: str,
        lane: str,
        venue: str,
        lifecycle: str,
        regime: str,
        risk_signature: str,
        flow_state: str,
    ) -> tuple[list[float], str]:
        key = self._v5_context_key(
            entity=entity,
            role=role,
            lane=lane,
            venue=venue,
            lifecycle=lifecycle,
            regime=regime,
            risk_signature=risk_signature,
            flow_state=flow_state,
        )
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE o.release_commit=? AND c.context_key=? ORDER BY o.id",
                (self.release_commit, key),
            ).fetchall()
            if len(rows) >= 20:
                return [float(r["net_return"]) for r in rows], "exact_context"
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "WHERE o.release_commit=? AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.regime=? ORDER BY o.id",
                (self.release_commit, lane, venue, lifecycle, regime),
            ).fetchall()
            if len(rows) >= ROBINHOOD_V5_MIN_SAMPLES:
                return [float(r["net_return"]) for r in rows], "lane_venue_lifecycle_regime"
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "WHERE o.release_commit=? AND c.lane=? AND t.venue=? AND t.lifecycle=? ORDER BY o.id",
                (self.release_commit, lane, venue, lifecycle),
            ).fetchall()
        return [float(r["net_return"]) for r in rows], "lane_venue_lifecycle" if rows else "none"

    def _v5_profile(self, **context: Any) -> dict[str, Any]:
        values, source = self._v5_context_returns(**context)
        profile = robust_return_profile(values, grid=ROBINHOOD_V5_POSITION_GRID, max_fraction=ROBINHOOD_V5_MAX_POSITION)
        return {
            "sample_count": profile.sample_count,
            "state": profile.state,
            "best_fraction": profile.best_fraction,
            "best_expected_log_growth": profile.best_expected_log_growth,
            "mean_return": profile.mean_return,
            "median_return": profile.median_return,
            "hit_rate": profile.hit_rate,
            "trimmed_mean_ex_best": profile.trimmed_mean_ex_best,
            "expected_shortfall_20": profile.expected_shortfall_20,
            "winner_concentration": profile.winner_concentration,
            "max_drawdown": profile.max_drawdown_at_best_fraction,
            "evidence_source": source,
            "hit_rate_is_promotion_veto": False,
        }

    def _v5_candidate_lanes(self, *, metrics: dict[str, Any], hazards: list[str], lifecycle_progress: float | None) -> list[str]:
        lanes: list[str] = []
        if bool(metrics.get("trigger_is_creator")):
            lanes.append("creator_deployer_continuation")
        else:
            lanes.append("elite_entity_continuation")
        if int(metrics.get("independent_entities_60s") or 0) >= 2:
            lanes.append("entity_flow_accumulation")
        if str(metrics.get("state")) in {"pre_fomo", "active_fomo"}:
            lanes.append("fomo_continuation")
        if lifecycle_progress is not None and lifecycle_progress >= 0.70:
            lanes.append("lifecycle_transition_continuation")
        if hazards:
            lanes.append("hazard_continuation")
        return list(dict.fromkeys(lanes))

    def _v5_choose_lane_fraction(
        self,
        *,
        entity: str,
        role: str,
        venue: str,
        lifecycle: str,
        regime: str,
        risk_signature: str,
        risk_severity: float,
        flow_state: str,
        lanes: list[str],
    ) -> tuple[str | None, float, dict[str, Any]]:
        profiles: dict[str, Any] = {}
        promoted: list[tuple[str, dict[str, Any]]] = []
        nondemoted: list[tuple[str, dict[str, Any]]] = []
        for lane in lanes:
            profile = self._v5_profile(
                entity=entity,
                role=role,
                lane=lane,
                venue=venue,
                lifecycle=lifecycle,
                regime=regime,
                risk_signature=risk_signature,
                flow_state=flow_state,
            )
            profiles[lane] = profile
            if profile["state"] == "promoted_positive_log_growth" and float(profile["best_fraction"] or 0.0) > 0.0:
                promoted.append((lane, profile))
            if profile["state"] != "demoted_nonpositive_log_growth":
                nondemoted.append((lane, profile))
        if promoted:
            lane, profile = max(
                promoted,
                key=lambda item: item[1]["best_expected_log_growth"] if item[1]["best_expected_log_growth"] is not None else float("-inf"),
            )
            fraction = float(profile["best_fraction"])
        elif nondemoted:
            priority = {
                "lifecycle_transition_continuation": 6,
                "creator_deployer_continuation": 5,
                "fomo_continuation": 4,
                "entity_flow_accumulation": 3,
                "elite_entity_continuation": 2,
                "hazard_continuation": 1,
            }
            lane, _ = max(nondemoted, key=lambda item: priority.get(item[0], 0))
            fraction = 0.005 if lane in {"creator_deployer_continuation", "hazard_continuation"} or risk_severity >= 0.45 else 0.01
        else:
            return None, 0.0, profiles
        fraction *= self._v5_regime_multiplier(regime)
        fraction *= max(0.30, 1.0 - 0.60 * risk_severity)
        if lane == "hazard_continuation":
            fraction = min(0.02, fraction)
        fraction = min(ROBINHOOD_V5_MAX_POSITION, max(0.0025, fraction))
        available = max(0.0, ROBINHOOD_V5_MAX_OPEN_EXPOSURE - self._open_exposure())
        return lane, min(fraction, available), profiles

    def _v5_insert_trial(
        self,
        *,
        token: str,
        market: str,
        venue: str,
        lifecycle: str,
        trigger_actor: str,
        trigger_entity: str,
        flow_state: str,
        fraction: float,
        quote: dict[str, Any],
        lane: str,
        role: str,
        regime: str,
        risk: dict[str, Any],
        lifecycle_progress: float | None,
        threshold_challenger: bool,
        candidate_lanes: list[str],
    ) -> None:
        context_key = self._v5_context_key(
            entity=trigger_entity,
            role=role,
            lane=lane,
            venue=venue,
            lifecycle=lifecycle,
            regime=regime,
            risk_signature=str(risk["risk_signature"]),
            flow_state=flow_state,
        )
        now = _utcnow()
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT INTO robinhood_paper_trials("
                "release_commit,strategy_version,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,context_state,"
                "position_fraction,entry_quote_in_wei,entry_token_raw,entry_gas_wei,entry_total_cost_wei,entry_price_eth,"
                "entry_round_trip_cost_fraction,opened_at,decision_reason,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    self.release_commit,ROBINHOOD_V5_VERSION,token,market,venue,lifecycle,trigger_actor,trigger_entity,flow_state,
                    f"v5:{lane}",float(fraction),str(int(quote["amount_in_wei"])),str(int(quote["token_out"])),
                    str(int(quote["entry_gas_wei"])),str(int(quote["entry_total_cost_wei"])),float(quote["entry_price_eth"]),
                    float(quote["round_trip_cost_fraction"]),now,
                    "risk_conditioned_exact_chain_flow_plus_executable_round_trip",
                ),
            )
            trial_id = int(cursor.lastrowid)
            self.store.db.execute(
                "INSERT INTO robinhood_v5_trial_context("
                "trial_id,release_commit,strategy_version,lane,trigger_role,regime,flow_state,risk_signature,risk_severity,risk_json,"
                "context_key,latency_band,lifecycle_progress,threshold_challenger,candidate_lanes_json,created_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    trial_id,self.release_commit,ROBINHOOD_V5_VERSION,lane,role,regime,flow_state,str(risk["risk_signature"]),
                    float(risk["risk_severity"]),json.dumps(risk,sort_keys=True),context_key,"chain_poll",lifecycle_progress,
                    1 if threshold_challenger else 0,json.dumps(candidate_lanes),now,
                ),
            )

    async def _maybe_open_v3(self, pool: V3Pool, *, current_block: int) -> None:
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
        role = "creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
        lifecycle = "post_protection_v3" if pool.venue == "PONS_V1_UNISWAP_V3" else "new_weth_pool"
        chase: float | None = None
        if pool.first_price_eth and pool.recent_swaps:
            latest = _finite(pool.recent_swaps[-1].get("price_eth"))
            if latest is not None and pool.first_price_eth > 0:
                chase = latest / pool.first_price_eth - 1.0
        extra: list[str] = []
        threshold_challenger = bool(chase is not None and chase > 0.15)
        if chase is None:
            extra.append("chase_unknown")
        elif chase > 0.40:
            return
        elif threshold_challenger:
            extra.append("late_lifecycle")
        if float(metrics.get("creator_sell_pressure") or 0.0) >= 0.25:
            extra.append("creator_distributing")
        risk = risk_descriptor(
            soft_flags=(),
            hard_flags=(),
            creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
            creator_linked_trigger=role == "creator_deployer",
            extra_hazards=extra,
        )
        regime = self._v5_regime(metrics)
        lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=None)
        # A lone unknown entity with no demonstrated context is not enough. Creator
        # and previously promoted exact entities remain valid pre-FOMO signals.
        if metrics["state"] == "neutral" and role != "creator_deployer":
            elite_profile = self._v5_profile(
                entity=entity,role=role,lane="elite_entity_continuation",venue=pool.venue,lifecycle=lifecycle,
                regime=regime,risk_signature=str(risk["risk_signature"]),flow_state=str(metrics["state"]),
            )
            if elite_profile["state"] != "promoted_positive_log_growth":
                return
        lane, fraction, _ = self._v5_choose_lane_fraction(
            entity=entity,role=role,venue=pool.venue,lifecycle=lifecycle,regime=regime,
            risk_signature=str(risk["risk_signature"]),risk_severity=float(risk["risk_severity"]),
            flow_state=str(metrics["state"]),lanes=lanes,
        )
        if not lane or fraction <= 0.0:
            return
        try:
            quote = await self._quote_v3_round_trip(pool, fraction)
        except Exception:
            return
        if quote is None:
            return
        profile = self._v5_profile(
            entity=entity,role=role,lane=lane,venue=pool.venue,lifecycle=lifecycle,regime=regime,
            risk_signature=str(risk["risk_signature"]),flow_state=str(metrics["state"]),
        )
        cost_ceiling = MAX_IMMEDIATE_ROUND_TRIP_COST
        if profile["state"] == "promoted_positive_log_growth" and profile["mean_return"] is not None:
            cost_ceiling = min(0.30, MAX_IMMEDIATE_ROUND_TRIP_COST + max(0.0, float(profile["mean_return"])) * 0.25)
        if float(quote["round_trip_cost_fraction"]) > cost_ceiling:
            return
        self._v5_insert_trial(
            token=pool.token,market=pool.pool,venue=pool.venue,lifecycle=lifecycle,trigger_actor=actor,trigger_entity=entity,
            flow_state=str(metrics["state"]),fraction=fraction,quote=quote,lane=lane,role=role,regime=regime,risk=risk,
            lifecycle_progress=None,threshold_challenger=threshold_challenger,candidate_lanes=lanes,
        )

    async def _maybe_open_v2(self, curve: V2Curve) -> None:
        if not self._caught_up or self._token_open(curve.token):
            return
        metrics = await self._v5_flow_metrics(curve.recent_swaps, deployer=curve.deployer)
        if not bool(metrics.get("entity_resolution_complete")):
            return
        actor = _clean_address(str(metrics.get("trigger_actor") or ""))
        entity = _clean_address(str(metrics.get("trigger_entity") or ""))
        if not actor or not entity or actor in KNOWN_NON_ACTORS:
            return
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
        risk = risk_descriptor(
            soft_flags=(),hard_flags=(),creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
            creator_linked_trigger=role == "creator_deployer",extra_hazards=extra,
        )
        regime = self._v5_regime(metrics)
        lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
        if metrics["state"] == "neutral" and role != "creator_deployer" and progress < 0.70:
            elite_profile = self._v5_profile(
                entity=entity,role=role,lane="elite_entity_continuation",venue="PONS_V2_CURVE",lifecycle="bonding_curve",
                regime=regime,risk_signature=str(risk["risk_signature"]),flow_state=str(metrics["state"]),
            )
            if elite_profile["state"] != "promoted_positive_log_growth":
                return
        lane, fraction, _ = self._v5_choose_lane_fraction(
            entity=entity,role=role,venue="PONS_V2_CURVE",lifecycle="bonding_curve",regime=regime,
            risk_signature=str(risk["risk_signature"]),risk_severity=float(risk["risk_severity"]),
            flow_state=str(metrics["state"]),lanes=lanes,
        )
        if not lane or fraction <= 0.0:
            return
        amount_in = int((self._paper_nav_usd() * fraction / eth_usd) * 1e18)
        if amount_in <= 0:
            return
        try:
            buy = await self.rpc.pons_v2_curve_quote(curve=curve.curve,quote_in=amount_in,recipient=self.paper_recipient)
            if int(buy["tokens_out"]) <= 0:
                return
            if int(buy.get("snipe_tax_bps") or 0) > 500:
                risk = risk_descriptor(
                    soft_flags=(),hard_flags=(),creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
                    creator_linked_trigger=role == "creator_deployer",extra_hazards=(*extra,"high_snipe_tax"),
                )
            exit_out = await self.rpc.pons_v2_curve_sell_quote(curve=curve.curve,tokens_in=buy["tokens_out"])
            gas_price = await self.rpc.gas_price()
            entry_gas_wei = 220_000 * gas_price
            exit_gas_wei = 220_000 * gas_price
            total_cost = buy["spent"] + entry_gas_wei
            immediate_net = max(0, exit_out - exit_gas_wei)
            round_trip = 1.0 - immediate_net / max(1, total_cost)
            quote = {
                "amount_in_wei": buy["spent"],"token_out": buy["tokens_out"],"entry_gas_wei": entry_gas_wei,
                "exit_gas_wei": exit_gas_wei,"entry_total_cost_wei": total_cost,"immediate_exit_wei": immediate_net,
                "round_trip_cost_fraction": round_trip,
                "entry_price_eth": (buy["spent"] / 1e18) / (buy["tokens_out"] / 1e18),
            }
        except Exception:
            return
        profile = self._v5_profile(
            entity=entity,role=role,lane=lane,venue="PONS_V2_CURVE",lifecycle="bonding_curve",regime=regime,
            risk_signature=str(risk["risk_signature"]),flow_state=str(metrics["state"]),
        )
        cost_ceiling = MAX_IMMEDIATE_ROUND_TRIP_COST
        if profile["state"] == "promoted_positive_log_growth" and profile["mean_return"] is not None:
            cost_ceiling = min(0.30, MAX_IMMEDIATE_ROUND_TRIP_COST + max(0.0, float(profile["mean_return"])) * 0.25)
        if float(quote["round_trip_cost_fraction"]) > cost_ceiling:
            return
        self._v5_insert_trial(
            token=curve.token,market=curve.curve,venue="PONS_V2_CURVE",lifecycle="bonding_curve",trigger_actor=actor,
            trigger_entity=entity,flow_state=str(metrics["state"]),fraction=fraction,quote=quote,lane=lane,role=role,regime=regime,
            risk=risk,lifecycle_progress=progress,threshold_challenger=progress >= 0.85,candidate_lanes=lanes,
        )

    def _v5_learned_exit_policy(self, trial: dict[str, Any]) -> dict[str, Any]:
        trial_id = int(trial["id"])
        with self.store._lock:
            context = self.store.db.execute(
                "SELECT lane FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1",
                (trial_id,),
            ).fetchone()
        lane = str(context["lane"]) if context else "legacy"
        with self.store._lock:
            closed = self.store.db.execute(
                "SELECT o.trial_id FROM robinhood_paper_outcomes o "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "WHERE o.release_commit=? AND c.lane=? AND t.venue=? AND t.lifecycle=? ORDER BY o.id",
                (self.release_commit,lane,str(trial["venue"]),str(trial["lifecycle"])),
            ).fetchall()
        ids = [int(row["trial_id"]) for row in closed]
        if len(ids) < ROBINHOOD_V5_MIN_SAMPLES:
            return {"source":"bootstrap","stop":STOP_LOSS_FRACTION,"harvest":HARVEST_FRACTION,"max_hold":MAX_HOLD_SECONDS}
        placeholders = ",".join("?" for _ in ids)
        with self.store._lock:
            marks = self.store.db.execute(
                f"SELECT trial_id,elapsed_seconds,net_return FROM robinhood_v5_marks WHERE release_commit=? AND trial_id IN ({placeholders}) ORDER BY id",
                (self.release_commit,*ids),
            ).fetchall()
        grouped: dict[int,list[tuple[float,float]]] = {}
        for row in marks:
            grouped.setdefault(int(row["trial_id"]),[]).append((float(row["elapsed_seconds"]),float(row["net_return"])))
        mfes: list[float] = []
        maes: list[float] = []
        time_to_mfe: list[float] = []
        for points in grouped.values():
            if not points:
                continue
            best = max(points,key=lambda item:item[1])
            mfes.append(best[1])
            maes.append(min(value for _,value in points))
            time_to_mfe.append(best[0])
        if len(mfes) < ROBINHOOD_V5_MIN_SAMPLES:
            return {"source":"bootstrap","stop":STOP_LOSS_FRACTION,"harvest":HARVEST_FRACTION,"max_hold":MAX_HOLD_SECONDS}
        median_mfe = median(mfes)
        median_mae = median(maes)
        harvest = min(0.75,max(0.15,median_mfe * 0.70)) if median_mfe > 0 else HARVEST_FRACTION
        stop = min(-0.08,max(-0.30,median_mae * 1.20)) if median_mae < 0 else STOP_LOSS_FRACTION
        max_hold = min(20 * 60,max(120.0,median(time_to_mfe) * 1.50))
        return {"source":"forward_mfe_mae","stop":stop,"harvest":harvest,"max_hold":max_hold}

    async def _settle_one(self, trial: dict[str, Any]) -> None:
        opened = datetime.fromisoformat(str(trial["opened_at"]))
        elapsed = max(0.0,(datetime.now(timezone.utc)-opened).total_seconds())
        token = _clean_address(trial["token"])
        market = _clean_address(trial["market"])
        token_amount = int(trial["entry_token_raw"])
        total_cost = int(trial["entry_total_cost_wei"])
        venue = str(trial["venue"])
        gas_price = await self.rpc.gas_price()
        exit_out: int | None = None
        exit_gas_wei = 0
        flow_state = str(trial["fomo_state"])
        deployer = ""

        if venue in {"PONS_V1_UNISWAP_V3","UNISWAP_V3_DIRECT"}:
            pool = self.v3_pools.get(market)
            if pool is None:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            else:
                deployer = pool.deployer
                try:
                    raw_out,gas_estimate = await self.rpc.v3_quote_exact_input(token_in=token,token_out=WETH,fee=pool.fee,amount_in=token_amount)
                    exit_gas_wei = (gas_estimate+80_000)*gas_price
                    exit_out = max(0,raw_out-exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
                metrics = await self._v5_flow_metrics(pool.recent_swaps,deployer=deployer)
                flow_state = str(metrics["state"])
        elif venue == "PONS_V2_CURVE":
            curve = self.v2_curves.get(market)
            if curve is None:
                return
            deployer = curve.deployer
            state = await self.rpc.pons_v2_launch_state(token)
            if int(state["phase"]) == 0:
                try:
                    raw_out = await self.rpc.pons_v2_curve_sell_quote(curve=market,tokens_in=token_amount)
                    exit_gas_wei = 220_000*gas_price
                    exit_out = max(0,raw_out-exit_gas_wei)
                except RuntimeError:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            elif int(state["phase"]) == 1:
                if elapsed < MAX_HOLD_SECONDS:
                    return
                exit_out = 0
            elif int(state["phase"]) == 2:
                pair = _clean_address(state["pair_token"])
                currency_a = "0x0000000000000000000000000000000000000000" if pair in {"","0x0000000000000000000000000000000000000000"} else pair
                currency0,currency1 = sorted([currency_a,token],key=lambda item:int(item,16))
                try:
                    raw_out,gas_estimate = await self.rpc.v4_quote_exact_input(
                        currency0=currency0,currency1=currency1,fee=int(state["pool_fee"]),tick_spacing=int(state["tick_spacing"]),
                        hooks=PONS_V2_MEME_HOOK,zero_for_one=token==currency0,amount_in=token_amount,
                    )
                    exit_gas_wei = (gas_estimate+120_000)*gas_price
                    exit_out = max(0,raw_out-exit_gas_wei)
                except Exception:
                    if elapsed < MAX_HOLD_SECONDS:
                        return
                    exit_out = 0
            else:
                exit_out = 0
            metrics = await self._v5_flow_metrics(curve.recent_swaps,deployer=deployer)
            flow_state = str(metrics["state"])
        else:
            return
        if exit_out is None:
            return
        net_return = exit_out/max(1,total_cost)-1.0
        with self.store._lock:
            context = self.store.db.execute("SELECT lane FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1",(int(trial["id"]),)).fetchone()
        lane = str(context["lane"]) if context else "legacy"
        mark_time = _utcnow()
        if context is not None:
            with self.store._lock,self.store.db:
                self.store.db.execute(
                    "INSERT OR IGNORE INTO robinhood_v5_marks(release_commit,trial_id,venue,lifecycle,lane,elapsed_seconds,net_return,flow_state,observed_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (self.release_commit,int(trial["id"]),venue,str(trial["lifecycle"]),lane,elapsed,float(net_return),flow_state,mark_time),
                )
        policy = self._v5_learned_exit_policy(trial) if context is not None else {"source":"legacy","stop":STOP_LOSS_FRACTION,"harvest":HARVEST_FRACTION,"max_hold":MAX_HOLD_SECONDS}
        if net_return <= float(policy["stop"]):
            reason = f"{policy['source']}:stop_loss"
        elif net_return >= float(policy["harvest"]):
            reason = f"{policy['source']}:harvest"
        elif flow_state == "exhaustion" and elapsed >= 30:
            reason = "flow_exhaustion"
        elif elapsed >= float(policy["max_hold"]):
            reason = f"{policy['source']}:max_hold"
        else:
            return
        multiplier = max(0.0,1.0+float(trial["position_fraction"])*net_return)
        with self.store._lock,self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO robinhood_paper_outcomes("
                "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
                "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    self.release_commit,int(trial["id"]),token,market,venue,str(trial["lifecycle"]),str(trial["trigger_actor"]),
                    str(trial["trigger_entity"]),flow_state,float(trial["position_fraction"]),float(net_return),float(multiplier),
                    str(int(exit_out)),str(int(exit_gas_wei)),reason,_utcnow(),
                ),
            )


__all__ = ["RobinhoodProfitMaximizerMixin", "ROBINHOOD_V5_VERSION"]
