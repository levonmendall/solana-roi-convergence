from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from . import v52_market_validation_controls as base
from . import v52_market_validation_governance as governance_module
from .strategy_v52_authority import target_sizing_policy

VERSION = "v52-market-validation-completion-v1"
CONTINUATION_HORIZONS_SECONDS = (15, 30, 60, 90, 120, 180, 300)
SHADOW_VARIANTS = (
    "A_graduation_only",
    "B_v52_no_wallet",
    "C_v52_full",
    "D_v52_plus_graduation_quality",
    "E_v52_plus_graduation_quality_decay",
    "F_v52_plus_graduation_quality_decay_lane_calibration",
    "G_full_proposed_alpha_gated",
)
REDUCED_CAPITAL_MULTIPLIER = 0.50


@dataclass(frozen=True)
class LaneCapitalState:
    lane: str
    mode: str
    capital_allowed: bool
    capital_multiplier: float
    reason: str
    evaluated_at: str


@dataclass(frozen=True)
class CompletionEvaluation:
    candidate_key: str
    lane: str
    observed_at: str
    graduation_quality: governance_module.PointInTimeComposite
    lane_relative_score: governance_module.PointInTimeComposite
    continuation: governance_module.ContinuationPersistence
    lane_state: LaneCapitalState
    shadow_decisions: dict[str, dict[str, Any]]
    independent_actor_metrics: governance_module.IndependentActorMetrics


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _iso(value: datetime | str) -> str:
    return _parse_time(value).isoformat()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _first(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _candidate_key(payload: Mapping[str, Any], lane: str, observed_at: str) -> str:
    for key in ("candidate_key", "source_signature", "signature", "candidate_id", "trial_id"):
        raw = payload.get(key)
        if raw not in (None, ""):
            return str(raw)
    token = str(_first(payload, ("token_mint", "token", "mint")) or "unknown")
    wallet = str(_first(payload, ("wallet", "trigger_wallet", "entity")) or "unknown")
    return f"{token}|{wallet}|{base.canonical_alpha_lane(lane)}|{observed_at}"


def _research_multiplier(score: float | None, calibrated: bool) -> float:
    if not calibrated or score is None:
        return 1.0
    return max(0.50, min(1.0, 0.50 + float(score)))


def _graduated(lifecycle_state: str | None, graduation_state: str | None) -> bool:
    text = f"{lifecycle_state or ''} {graduation_state or ''}".lower()
    return any(term in text for term in ("graduated", "post_graduation", "post-graduation", "pump_amm", "pumpswap"))


class MarketValidationCompletion:
    """Completes the v5.2 validation architecture without creating parallel authority."""

    def __init__(
        self,
        controller: base.MarketValidationController,
        governed: governance_module.MarketValidationGovernance,
    ) -> None:
        self.controller = controller
        self.governed = governed
        self.store = controller.store
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_lane_events ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_key TEXT NOT NULL, token_mint TEXT, lane TEXT NOT NULL, "
                "observed_at TEXT NOT NULL, lifecycle_state TEXT, graduation_state TEXT, opportunity INTEGER NOT NULL, "
                "eligible INTEGER NOT NULL, executed INTEGER NOT NULL, rejected INTEGER NOT NULL, missed INTEGER NOT NULL, "
                "false_positive INTEGER NOT NULL, position_fraction REAL NOT NULL, gross_return REAL, net_return REAL, "
                "fees_fraction REAL, slippage_fraction REAL, holding_seconds REAL, execution_success INTEGER, "
                "rejected_forward_return REAL, counterfactual_return REAL, lane_mode TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "UNIQUE(candidate_key,observed_at,lane))"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_v52_market_validation_lane_events_window "
                "ON v52_market_validation_lane_events(lane,observed_at,id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_shadow_variants ("
                "candidate_key TEXT NOT NULL, observed_at TEXT NOT NULL, lane TEXT NOT NULL, variant_id TEXT NOT NULL, "
                "decision_fraction REAL NOT NULL, decision TEXT NOT NULL, evidence_json TEXT NOT NULL, net_return REAL, "
                "portfolio_contribution REAL, outcome_status TEXT NOT NULL, resolved_at TEXT, controls_trading INTEGER NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "PRIMARY KEY(candidate_key,observed_at,variant_id))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_continuation_horizons ("
                "candidate_key TEXT NOT NULL, token_mint TEXT, lane TEXT NOT NULL, observed_at TEXT NOT NULL, "
                "horizon_label TEXT NOT NULL, horizon_seconds INTEGER, earliest_executable_price REAL, "
                "gross_forward_return REAL, net_executable_forward_return REAL, cost_fraction REAL, resolved_at TEXT, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "PRIMARY KEY(candidate_key,observed_at,horizon_label))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_component_ablation ("
                "candidate_key TEXT NOT NULL, observed_at TEXT NOT NULL, component TEXT NOT NULL, "
                "with_component_return REAL, without_component_return REAL, incremental_return REAL, resolved_at TEXT, "
                "causal_claim INTEGER NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "PRIMARY KEY(candidate_key,observed_at,component))"
            )

    def _table_columns(self, table: str) -> set[str]:
        try:
            with self.store._lock:
                rows = self.store.db.execute(f"PRAGMA table_info({table})").fetchall()
            return {str(row[1]) for row in rows}
        except Exception:
            return set()

    def participants_before(self, token_mint: str | None, decision_at: datetime | str) -> list[dict[str, Any]]:
        if not token_mint or not base._table_exists(self.store, "wallet_discovery_forward_observations"):
            return []
        columns = self._table_columns("wallet_discovery_forward_observations")
        if not {"token_mint", "received_at", "wallet"}.issubset(columns):
            return []
        optional = [
            name
            for name in (
                "side",
                "linked_entity_id",
                "funding_cluster_id",
                "funder_cluster_id",
                "creator_associated",
                "funder_associated",
                "notional",
                "amount_sol",
            )
            if name in columns
        ]
        selected = ["wallet"] + optional
        sql = (
            f"SELECT {','.join(selected)} FROM wallet_discovery_forward_observations "
            "WHERE token_mint=? AND received_at<? ORDER BY received_at,id"
        )
        try:
            with self.store._lock:
                rows = self.store.db.execute(sql, (str(token_mint), _iso(decision_at))).fetchall()
        except Exception:
            return []
        result: list[dict[str, Any]] = []
        for raw in rows:
            item = dict(raw)
            if "notional" not in item and "amount_sol" in item:
                item["notional"] = item.get("amount_sol")
            result.append(item)
        return result

    def extract_metrics(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        risk = _safe_dict(payload.get("risk"))
        authority = _safe_dict(payload.get("v52_authority"))
        combined = {**dict(payload), **risk, **authority}
        aliases: dict[str, tuple[str, ...]] = {
            "graduation_speed_seconds": ("graduation_speed_seconds", "seconds_to_graduation", "time_to_graduation_seconds"),
            "acceleration_into_graduation": ("acceleration_into_graduation", "transaction_acceleration", "acceleration"),
            "independent_buyer_breadth": ("independent_buyer_breadth", "independent_count", "independent_confirmation_count"),
            "independent_buyer_growth": ("independent_buyer_growth", "buyer_breadth_growth", "new_buyer_acceleration"),
            "transaction_acceleration": ("transaction_acceleration", "transaction_frequency_acceleration", "acceleration"),
            "buy_sell_imbalance": ("buy_sell_imbalance", "net_buy_flow_acceleration"),
            "concentration": ("concentration", "buyer_concentration", "holder_concentration"),
            "liquidity_formation": ("liquidity_formation", "depth_growth_fraction", "liquidity_growth"),
            "liquidity_depth": ("liquidity_depth", "liquidity", "exit_depth"),
            "creator_associated_activity": ("creator_associated_activity", "creator_linked_trigger"),
            "funder_associated_activity": ("funder_associated_activity",),
            "linked_wallet_clustering": ("linked_wallet_clustering",),
            "wallet_integrity": ("wallet_integrity", "integrity_score"),
            "high_forward_alpha_wallet_participation": ("high_forward_alpha_wallet_participation", "wallet_confidence"),
            "repeated_entity_activity": ("repeated_entity_activity",),
            "price_behavior_into_graduation": ("price_behavior_into_graduation", "price_velocity", "velocity"),
            "abnormal_coordinated_buying": ("abnormal_coordinated_buying",),
            "suspicious_liquidity_behavior": ("suspicious_liquidity_behavior",),
            "acceleration": ("acceleration", "transaction_acceleration", "new_buyer_acceleration"),
            "velocity": ("velocity", "price_velocity", "price_velocity_since_entry"),
            "buyer_breadth_growth": ("buyer_breadth_growth", "independent_buyer_growth", "new_buyer_acceleration"),
            "transaction_velocity": ("transaction_velocity", "transaction_frequency_acceleration"),
            "liquidity": ("liquidity", "liquidity_depth"),
            "liquidity_growth": ("liquidity_growth", "depth_growth_fraction"),
            "slippage": ("slippage", "slippage_fraction", "expected_slippage"),
            "execution_success": ("execution_success", "entry_executable"),
            "price_velocity_since_graduation": ("price_velocity_since_graduation",),
            "price_velocity_since_entry": ("price_velocity_since_entry", "velocity", "price_velocity"),
            "buyer_acceleration": ("buyer_acceleration", "new_buyer_acceleration"),
            "volume_persistence": ("volume_persistence",),
            "new_wallet_participation": ("new_wallet_participation",),
            "quality_wallet_participation": ("quality_wallet_participation", "wallet_confidence"),
            "maximum_favorable_excursion": ("maximum_favorable_excursion", "executable_mfe", "mfe"),
            "maximum_adverse_excursion": ("maximum_adverse_excursion", "executable_mae", "mae"),
            "seconds_since_graduation": ("seconds_since_graduation",),
            "seconds_since_entry": ("seconds_since_entry", "elapsed_seconds"),
            "continuation_persistence": ("continuation_persistence",),
            "wallet_quality": ("wallet_quality", "wallet_confidence"),
            "volatility": ("volatility",),
            "forward_return": ("forward_return", "net_return"),
        }
        metrics: dict[str, Any] = {}
        for target, keys in aliases.items():
            value = _first(combined, keys)
            if isinstance(value, bool):
                value = 1.0 if value else 0.0
            parsed = _finite(value)
            if parsed is not None:
                metrics[target] = parsed
        return metrics

    def lane_capital_state(self, lane: str, decision_at: datetime | str) -> LaneCapitalState:
        governed = self.governed.governed_lane_state_at(lane, decision_at)
        if governed.mode == "observe_only":
            return LaneCapitalState(governed.lane, "observe_only", False, 0.0, governed.reason, governed.evaluated_at)
        long_windows = (governed.evidence_windows["7d"], governed.evidence_windows["30d"])
        minimum = max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))
        enough_long = all(item.sample_count > 0 for item in long_windows) and governed.evidence_windows["30d"].sample_count >= minimum
        if governed.mode == "insufficient_evidence" and not enough_long:
            return LaneCapitalState(governed.lane, "insufficient_evidence", True, 1.0, governed.reason, governed.evaluated_at)
        mixed = any(not item.positive_evidence for item in long_windows)
        deteriorating = governed.negative_streak > 0
        if mixed or deteriorating:
            return LaneCapitalState(
                governed.lane,
                "reduced",
                True,
                REDUCED_CAPITAL_MULTIPLIER,
                "positive_but_not_fully_confirmed_or_deteriorating_multi_horizon_evidence",
                governed.evaluated_at,
            )
        return LaneCapitalState(governed.lane, "active", True, 1.0, governed.reason, governed.evaluated_at)

    def _no_wallet_fraction(self, authoritative_fraction: float, authority: Mapping[str, Any] | None) -> float:
        auth = dict(authority or {})
        multiplier = _finite(auth.get("wallet_target_utilization_multiplier")) or 1.0
        if multiplier <= 1.0:
            return authoritative_fraction
        return max(0.0, min(authoritative_fraction, authoritative_fraction / multiplier))

    def shadow_decisions(
        self,
        *,
        authoritative_fraction: float,
        authority: Mapping[str, Any] | None,
        lifecycle_state: str | None,
        graduation_state: str | None,
        graduation_quality: governance_module.PointInTimeComposite,
        lane_relative_score: governance_module.PointInTimeComposite,
        continuation: governance_module.ContinuationPersistence,
        lane_state: LaneCapitalState,
    ) -> dict[str, dict[str, Any]]:
        full = max(0.0, float(authoritative_fraction))
        no_wallet = self._no_wallet_fraction(full, authority)
        is_graduated = _graduated(lifecycle_state, graduation_state)
        graduation_multiplier = _research_multiplier(graduation_quality.score, graduation_quality.calibrated)
        lane_multiplier = _research_multiplier(lane_relative_score.score, lane_relative_score.calibrated)
        decay_multiplier = continuation.evidence_multiplier if continuation.calibrated else 1.0
        gated_multiplier = lane_state.capital_multiplier if lane_state.capital_allowed else 0.0
        fractions = {
            "A_graduation_only": full if is_graduated else 0.0,
            "B_v52_no_wallet": no_wallet,
            "C_v52_full": full,
            "D_v52_plus_graduation_quality": full * graduation_multiplier,
            "E_v52_plus_graduation_quality_decay": full * graduation_multiplier * decay_multiplier,
            "F_v52_plus_graduation_quality_decay_lane_calibration": full * graduation_multiplier * decay_multiplier * lane_multiplier,
            "G_full_proposed_alpha_gated": full * graduation_multiplier * decay_multiplier * lane_multiplier * gated_multiplier,
        }
        decisions: dict[str, dict[str, Any]] = {}
        for variant, fraction in fractions.items():
            decisions[variant] = {
                "decision": "paper_counterfactual_enter" if fraction > 0.0 else "paper_counterfactual_observe",
                "position_fraction": max(0.0, float(fraction)),
                "controls_trading": False,
                "paper_only": True,
                "live_money_authority": False,
                "same_market_data": True,
                "same_cost_execution_assumptions": True,
            }
        decisions["A_graduation_only"]["pre_graduation_entry"] = False
        decisions["B_v52_no_wallet"]["wallet_intelligence_influence"] = False
        decisions["C_v52_full"]["wallet_intelligence_influence"] = True
        return decisions

    def evaluate_candidate(
        self,
        *,
        lane: str,
        observed_at: datetime | str,
        payload: Mapping[str, Any],
        authoritative_fraction: float,
        lifecycle_state: str | None = None,
        graduation_state: str | None = None,
        earliest_executable_price: float | None = None,
        eligible: bool = True,
        executed: bool | None = None,
        discovery_route: str | None = None,
        market_state: str | None = None,
    ) -> CompletionEvaluation:
        at = _iso(observed_at)
        canonical_lane = base.canonical_alpha_lane(lane)
        token = str(_first(payload, ("token_mint", "token", "mint")) or "") or None
        participants = self.participants_before(token, at)
        metrics = governance_module.graduation_metrics_with_actor_integrity(self.extract_metrics(payload), participants)
        self.controller.observe_features(
            canonical_lane,
            metrics,
            observed_at=at,
            discovery_route=discovery_route,
            market_state=market_state,
        )
        graduation = self.governed.graduation_quality_at(canonical_lane, metrics, at)
        lane_relative = self.governed.lane_relative_score_at(canonical_lane, metrics, at)
        expected_continuation = graduation.score if graduation.calibrated else lane_relative.score if lane_relative.calibrated else None
        continuation = self.governed.continuation_at(
            canonical_lane,
            metrics,
            at,
            expected_continuation=expected_continuation,
        )
        lane_state = self.lane_capital_state(canonical_lane, at)
        authority = _safe_dict(payload.get("v52_authority"))
        shadows = self.shadow_decisions(
            authoritative_fraction=authoritative_fraction,
            authority=authority,
            lifecycle_state=lifecycle_state,
            graduation_state=graduation_state,
            graduation_quality=graduation,
            lane_relative_score=lane_relative,
            continuation=continuation,
            lane_state=lane_state,
        )
        actor_metrics = governance_module.independent_actor_metrics(participants)
        candidate = _candidate_key(payload, canonical_lane, at)
        wallet_evidence = {
            "independent_actor_metrics": asdict(actor_metrics),
            "wallet_confidence": authority.get("wallet_confidence"),
            "wallet_influence_mode": authority.get("wallet_influence_mode"),
        }
        v52_decision = {
            "position_fraction": max(0.0, float(authoritative_fraction)),
            "eligible": bool(eligible),
            "executed": bool(executed if executed is not None else authoritative_fraction > 0.0),
            "lane_state": asdict(lane_state),
        }
        self.governed.record_point_in_time_decision(
            candidate_key=candidate,
            lane=canonical_lane,
            observed_at=at,
            lifecycle_state=lifecycle_state,
            graduation_state=graduation_state,
            raw_features=metrics,
            relative_features={
                "graduation": graduation.component_percentiles,
                "lane": lane_relative.component_percentiles,
                "continuation": continuation.component_percentiles,
            },
            graduation_quality=graduation.score,
            continuation_persistence=continuation.score,
            lane_alpha_mode=lane_state.mode,
            wallet_evidence=wallet_evidence,
            v52_decision=v52_decision,
            shadow_decision=shadows,
            earliest_executable_price=earliest_executable_price,
        )
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR REPLACE INTO v52_market_validation_lane_events("
                "candidate_key,token_mint,lane,observed_at,lifecycle_state,graduation_state,opportunity,eligible,executed,rejected,missed,"
                "false_positive,position_fraction,lane_mode,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,?,?,?,?,?,?,?,1,0)",
                (
                    candidate,
                    token,
                    canonical_lane,
                    at,
                    lifecycle_state,
                    graduation_state,
                    1 if eligible else 0,
                    1 if (executed if executed is not None else authoritative_fraction > 0.0) else 0,
                    0 if eligible else 1,
                    1 if eligible and authoritative_fraction <= 0.0 else 0,
                    0,
                    max(0.0, float(authoritative_fraction)),
                    lane_state.mode,
                ),
            )
            for variant, decision in shadows.items():
                self.store.db.execute(
                    "INSERT OR REPLACE INTO v52_market_validation_shadow_variants("
                    "candidate_key,observed_at,lane,variant_id,decision_fraction,decision,evidence_json,net_return,portfolio_contribution,"
                    "outcome_status,resolved_at,controls_trading,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,NULL,NULL,'pending',NULL,0,1,0)",
                    (
                        candidate,
                        at,
                        canonical_lane,
                        variant,
                        float(decision["position_fraction"]),
                        str(decision["decision"]),
                        json.dumps(decision, sort_keys=True),
                    ),
                )
            if earliest_executable_price is not None and earliest_executable_price > 0.0:
                for seconds in CONTINUATION_HORIZONS_SECONDS:
                    self.store.db.execute(
                        "INSERT OR IGNORE INTO v52_market_validation_continuation_horizons("
                        "candidate_key,token_mint,lane,observed_at,horizon_label,horizon_seconds,earliest_executable_price,"
                        "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,1,0)",
                        (candidate, token, canonical_lane, at, f"{seconds}s", seconds, float(earliest_executable_price)),
                    )
                for label in ("graduation", "immediately_post_graduation"):
                    self.store.db.execute(
                        "INSERT OR IGNORE INTO v52_market_validation_continuation_horizons("
                        "candidate_key,token_mint,lane,observed_at,horizon_label,horizon_seconds,earliest_executable_price,"
                        "paper_only,live_money_authority) VALUES (?,?,?,?,?,NULL,?,1,0)",
                        (candidate, token, canonical_lane, at, label, float(earliest_executable_price)),
                    )
        return CompletionEvaluation(
            candidate_key=candidate,
            lane=canonical_lane,
            observed_at=at,
            graduation_quality=graduation,
            lane_relative_score=lane_relative,
            continuation=continuation,
            lane_state=lane_state,
            shadow_decisions=shadows,
            independent_actor_metrics=actor_metrics,
        )

    def record_market_mark(
        self,
        *,
        token_mint: str,
        marked_at: datetime | str,
        price: float,
        cost_fraction: float = 0.0,
        graduation_state: str | None = None,
    ) -> int:
        current = _finite(price)
        if current is None or current <= 0.0:
            return 0
        marked = _parse_time(marked_at)
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT candidate_key,observed_at,horizon_label,horizon_seconds,earliest_executable_price "
                "FROM v52_market_validation_continuation_horizons WHERE token_mint=? AND resolved_at IS NULL "
                "ORDER BY observed_at,horizon_seconds",
                (str(token_mint),),
            ).fetchall()
        updates: list[tuple[float, float, float, str, str, str, str]] = []
        for row in rows:
            observed = _parse_time(row["observed_at"])
            label = str(row["horizon_label"])
            seconds = row["horizon_seconds"]
            due = seconds is not None and marked >= observed + timedelta(seconds=int(seconds))
            state_text = str(graduation_state or "").lower()
            if label == "graduation":
                due = "graduat" in state_text
            elif label == "immediately_post_graduation":
                due = any(term in state_text for term in ("post_graduation", "post-graduation", "pumpswap", "pump_amm"))
            if not due:
                continue
            entry = float(row["earliest_executable_price"])
            gross = current / entry - 1.0
            net = gross - max(0.0, float(cost_fraction))
            updates.append((gross, net, max(0.0, float(cost_fraction)), _iso(marked), str(row["candidate_key"]), str(row["observed_at"]), label))
        if not updates:
            return 0
        with self.store._lock, self.store.db:
            self.store.db.executemany(
                "UPDATE v52_market_validation_continuation_horizons SET gross_forward_return=?,net_executable_forward_return=?,"
                "cost_fraction=?,resolved_at=? WHERE candidate_key=? AND observed_at=? AND horizon_label=?",
                updates,
            )
        return len(updates)

    def resolve_outcome(
        self,
        *,
        candidate_key: str,
        observed_at: datetime | str,
        net_return: float,
        gross_return: float | None = None,
        fees_fraction: float | None = None,
        slippage_fraction: float | None = None,
        holding_seconds: float | None = None,
        rejected_forward_return: float | None = None,
        counterfactual_return: float | None = None,
        variant_returns: Mapping[str, float] | None = None,
        resolved_at: datetime | str | None = None,
    ) -> None:
        at = _iso(observed_at)
        resolved = _iso(resolved_at or _utcnow())
        self.governed.resolve_future_outcome(candidate_key, at, {
            "net_return": float(net_return),
            "gross_return": gross_return,
            "fees_fraction": fees_fraction,
            "slippage_fraction": slippage_fraction,
            "holding_seconds": holding_seconds,
        })
        with self.store._lock:
            event = self.store.db.execute(
                "SELECT position_fraction,lane FROM v52_market_validation_lane_events WHERE candidate_key=? AND observed_at=? LIMIT 1",
                (candidate_key, at),
            ).fetchone()
            shadow_rows = self.store.db.execute(
                "SELECT variant_id,decision_fraction FROM v52_market_validation_shadow_variants WHERE candidate_key=? AND observed_at=?",
                (candidate_key, at),
            ).fetchall()
        authoritative_fraction = float(event["position_fraction"] or 0.0) if event else 0.0
        supplied = dict(variant_returns or {})
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE v52_market_validation_lane_events SET gross_return=?,net_return=?,fees_fraction=?,slippage_fraction=?,"
                "holding_seconds=?,execution_success=1,rejected_forward_return=?,counterfactual_return=?,false_positive=? "
                "WHERE candidate_key=? AND observed_at=?",
                (
                    gross_return,
                    float(net_return),
                    fees_fraction,
                    slippage_fraction,
                    holding_seconds,
                    rejected_forward_return,
                    counterfactual_return,
                    1 if authoritative_fraction > 0.0 and float(net_return) < 0.0 else 0,
                    candidate_key,
                    at,
                ),
            )
            for row in shadow_rows:
                variant = str(row["variant_id"])
                fraction = float(row["decision_fraction"] or 0.0)
                if variant in supplied:
                    variant_return = float(supplied[variant])
                    status = "resolved_counterfactual"
                elif variant != "A_graduation_only":
                    variant_return = float(net_return)
                    status = "resolved_same_path"
                else:
                    variant_return = None
                    status = "requires_graduation_entry_counterfactual"
                contribution = None if variant_return is None else fraction * variant_return
                self.store.db.execute(
                    "UPDATE v52_market_validation_shadow_variants SET net_return=?,portfolio_contribution=?,outcome_status=?,resolved_at=? "
                    "WHERE candidate_key=? AND observed_at=? AND variant_id=?",
                    (variant_return, contribution, status, resolved if variant_return is not None else None, candidate_key, at, variant),
                )
            if event is not None:
                lane = str(event["lane"])
                for legacy_id, variant in (
                    ("graduation_only_continuation", "A_graduation_only"),
                    ("v52_no_wallet", "B_v52_no_wallet"),
                    ("v52_full", "C_v52_full"),
                ):
                    row = next((item for item in shadow_rows if str(item["variant_id"]) == variant), None)
                    if row is not None and (variant in supplied or variant != "A_graduation_only"):
                        value = supplied.get(variant, float(net_return))
                        self.controller.record_shadow_outcome(
                            candidate_key,
                            lane,
                            legacy_id,
                            observed_at=at,
                            net_return=float(value),
                            resolved_at=resolved,
                            evidence={"completion_variant": variant, "same_path": variant not in supplied},
                        )
        if supplied:
            attribution_inputs = {
                "v52_full": supplied.get("C_v52_full", net_return),
                "v52_no_wallet": supplied.get("B_v52_no_wallet"),
                "graduation_only_continuation": supplied.get("A_graduation_only"),
                "full_proposed": supplied.get("G_full_proposed_alpha_gated"),
                "no_graduation_quality": supplied.get("C_v52_full"),
                "no_decay": supplied.get("D_v52_plus_graduation_quality"),
                "no_lane_calibration": supplied.get("E_v52_plus_graduation_quality_decay"),
                "no_lane_gating": supplied.get("F_v52_plus_graduation_quality_decay_lane_calibration"),
            }
            results = governance_module.component_attribution(attribution_inputs)
            with self.store._lock, self.store.db:
                for component, result in results.items():
                    if result.incremental_return is None:
                        continue
                    self.store.db.execute(
                        "INSERT OR REPLACE INTO v52_market_validation_component_ablation("
                        "candidate_key,observed_at,component,with_component_return,without_component_return,incremental_return,resolved_at,"
                        "causal_claim,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,0,1,0)",
                        (candidate_key, at, component, None, None, float(result.incremental_return), resolved),
                    )

    def lane_accounting(self, lane: str, *, as_of: datetime | str | None = None, window: timedelta = timedelta(days=30)) -> dict[str, Any]:
        end = _parse_time(as_of or _utcnow())
        start = end - window
        canonical = base.canonical_alpha_lane(lane)
        with self.store._lock:
            rows = [
                dict(row)
                for row in self.store.db.execute(
                    "SELECT * FROM v52_market_validation_lane_events WHERE lane=? AND observed_at>=? AND observed_at<? ORDER BY observed_at,id",
                    (canonical, _iso(start), _iso(end)),
                ).fetchall()
            ]
        realized = [float(row["net_return"]) for row in rows if row["net_return"] is not None]
        wins = [value for value in realized if value > 0.0]
        losses = [value for value in realized if value < 0.0]
        gross = [float(row["gross_return"]) for row in rows if row["gross_return"] is not None]
        equity = 1.0
        peak = 1.0
        max_drawdown = 0.0
        for value in realized:
            equity *= max(1e-9, 1.0 + value)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / peak if peak else 0.0)
        positive_sum = sum(wins)
        negative_sum = abs(sum(losses))
        executions = [row for row in rows if int(row["executed"] or 0) == 1]
        execution_success = [row for row in executions if int(row["execution_success"] or 0) == 1]
        holding_values = [float(row["holding_seconds"]) for row in rows if row["holding_seconds"] is not None]
        return {
            "lane": canonical,
            "window_seconds": int(window.total_seconds()),
            "opportunity_count": len(rows),
            "eligible_opportunity_count": sum(int(row["eligible"] or 0) for row in rows),
            "executed_trade_count": len(executions),
            "win_rate": (len(wins) / len(realized)) if realized else None,
            "average_win": statistics.fmean(wins) if wins else None,
            "average_loss": statistics.fmean(losses) if losses else None,
            "expectancy": statistics.fmean(realized) if realized else None,
            "gross_return": sum(gross) if gross else None,
            "net_return": sum(realized) if realized else None,
            "profit_factor": (positive_sum / negative_sum) if negative_sum > 0.0 else (math.inf if positive_sum > 0.0 else None),
            "maximum_drawdown": max_drawdown if realized else None,
            "largest_loss": min(realized) if realized else None,
            "fees": sum(float(row["fees_fraction"] or 0.0) for row in rows),
            "slippage": sum(float(row["slippage_fraction"] or 0.0) for row in rows),
            "average_position_size": statistics.fmean([float(row["position_fraction"] or 0.0) for row in executions]) if executions else None,
            "holding_time": statistics.fmean(holding_values) if holding_values else None,
            "execution_success_rate": (len(execution_success) / len(executions)) if executions else None,
            "missed_opportunities": sum(int(row["missed"] or 0) for row in rows),
            "false_positives": sum(int(row["false_positive"] or 0) for row in rows),
            "rejected_candidate_forward_returns": [float(row["rejected_forward_return"]) for row in rows if row["rejected_forward_return"] is not None],
            "counterfactual_returns": [float(row["counterfactual_return"]) for row in rows if row["counterfactual_return"] is not None],
            "sample_size": len(realized),
            "confidence": min(1.0, len(realized) / max(1, int(target_sizing_policy().get("minimum_forward_samples", 30)))),
        }

    def controlled_variant_summary(self, *, as_of: datetime | str | None = None, window: timedelta = timedelta(days=30), starting_capital: float = 500.0) -> dict[str, Any]:
        end = _parse_time(as_of or _utcnow())
        start = end - window
        with self.store._lock:
            rows = [
                dict(row)
                for row in self.store.db.execute(
                    "SELECT * FROM v52_market_validation_shadow_variants WHERE observed_at>=? AND observed_at<? ORDER BY observed_at,variant_id",
                    (_iso(start), _iso(end)),
                ).fetchall()
            ]
        result: dict[str, Any] = {}
        for variant in SHADOW_VARIANTS:
            subset = [row for row in rows if str(row["variant_id"]) == variant]
            resolved = [row for row in subset if row["net_return"] is not None]
            contributions = [float(row["portfolio_contribution"] or 0.0) for row in resolved]
            pnl = float(starting_capital) * sum(contributions)
            result[variant] = {
                "opportunities": len(subset),
                "resolved_outcomes": len(resolved),
                "unresolved_outcomes": len(subset) - len(resolved),
                "net_portfolio_contribution_fraction": sum(contributions) if contributions else None,
                "estimated_dollar_contribution": pnl if contributions else None,
                "starting_capital": float(starting_capital),
                "validation_status": "sufficient" if len(resolved) >= int(target_sizing_policy().get("minimum_forward_samples", 30)) else "insufficient_point_in_time_evidence",
            }
        return result

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            lane_events = int(self.store.db.execute("SELECT COUNT(*) FROM v52_market_validation_lane_events").fetchone()[0])
            shadow_rows = int(self.store.db.execute("SELECT COUNT(*) FROM v52_market_validation_shadow_variants").fetchone()[0])
            horizon_rows = int(self.store.db.execute("SELECT COUNT(*) FROM v52_market_validation_continuation_horizons").fetchone()[0])
        return {
            "version": VERSION,
            "installed": _INSTALLED,
            "a_to_g_shadow_variants": list(SHADOW_VARIANTS),
            "all_shadow_variants_non_authoritative": True,
            "continuation_horizons_seconds": list(CONTINUATION_HORIZONS_SECONDS),
            "graduation_and_post_graduation_horizons": True,
            "automatic_point_in_time_recording": True,
            "automatic_independent_actor_enrichment": True,
            "lane_accounting_continuous": True,
            "lane_states": ["active", "reduced", "observe_only", "insufficient_evidence"],
            "reduced_capital_multiplier": REDUCED_CAPITAL_MULTIPLIER,
            "component_ablation_store": True,
            "controlled_24h_7d_30d_reporting_available": True,
            "starting_portfolio_reporting_default": 500.0,
            "fixed_exit_clock_added": False,
            "lane_event_rows": lane_events,
            "shadow_variant_rows": shadow_rows,
            "horizon_rows": horizon_rows,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


_COMPLETION: MarketValidationCompletion | None = None
_INSTALLED = False
_BASE_SOLANA_CHOOSE: Any = None
_BASE_FOMO_DECISION: Any = None
_BASE_ROBINHOOD_CHOOSE: Any = None
_BASE_FINAL_BUY: Any = None
_BASE_FINAL_SELL: Any = None


def completion() -> MarketValidationCompletion:
    if _COMPLETION is None:
        raise RuntimeError("v5.2 market-validation completion not installed")
    return _COMPLETION


def _profile_authority(profiles: Mapping[str, Any], lane: str | None) -> dict[str, Any]:
    if not lane:
        return {}
    profile = profiles.get(lane)
    if not isinstance(profile, Mapping):
        return {}
    return _safe_dict(profile.get("v52_authority"))


def _solana_choose(adapter: Any, pre: dict[str, Any], *, chase: float | None = None, latency: float | None = None) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_SOLANA_CHOOSE is None:
        raise RuntimeError("market-validation completion Solana predecessor unavailable")
    lane, fraction, profiles = _BASE_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    copied = {key: dict(value) if isinstance(value, Mapping) else value for key, value in dict(profiles or {}).items()}
    if not lane:
        return lane, float(fraction or 0.0), copied
    at = pre.get("at") or pre.get("received_at") or _utcnow()
    payload = dict(pre)
    payload["v52_authority"] = _profile_authority(copied, lane)
    evaluation = completion().evaluate_candidate(
        lane=lane,
        observed_at=at,
        payload=payload,
        authoritative_fraction=float(fraction or 0.0),
        lifecycle_state=str(pre.get("lifecycle") or ""),
        graduation_state=str(pre.get("graduation_state") or pre.get("lifecycle") or ""),
        eligible=True,
        executed=float(fraction or 0.0) > 0.0,
        discovery_route=str(pre.get("venue") or "solana"),
        market_state=str(pre.get("flow_state") or ""),
    )
    final_fraction = float(fraction or 0.0)
    if evaluation.lane_state.mode == "reduced" and final_fraction > 0.0:
        final_fraction *= evaluation.lane_state.capital_multiplier
    if lane in copied and isinstance(copied[lane], Mapping):
        profile = dict(copied[lane])
        profile["v52_market_validation_completion"] = {
            "graduation_quality": asdict(evaluation.graduation_quality),
            "lane_relative_score": asdict(evaluation.lane_relative_score),
            "continuation": asdict(evaluation.continuation),
            "lane_state": asdict(evaluation.lane_state),
            "shadow_variants": evaluation.shadow_decisions,
        }
        auth = _safe_dict(profile.get("v52_authority"))
        auth["market_validation_lane_state"] = evaluation.lane_state.mode
        auth["market_validation_lane_multiplier"] = evaluation.lane_state.capital_multiplier
        auth["final_fraction"] = final_fraction
        profile["v52_authority"] = auth
        copied[lane] = profile
    return (lane if final_fraction > 0.0 else None), max(0.0, final_fraction), copied


def _fomo_decision(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _BASE_FOMO_DECISION is None:
        raise RuntimeError("market-validation completion FOMO predecessor unavailable")
    result = dict(_BASE_FOMO_DECISION(adapter, observation=observation, trial=trial))
    fraction = float(result.get("position_fraction") or 0.0)
    payload = {**dict(observation), **dict(trial), **_safe_dict(observation.get("state_json")), **result}
    at = payload.get("observed_at") or payload.get("received_at") or _utcnow()
    state = str(payload.get("state") or payload.get("fomo_state") or "")
    lifecycle = str(payload.get("lifecycle") or "")
    evaluation = completion().evaluate_candidate(
        lane="fomo",
        observed_at=at,
        payload=payload,
        authoritative_fraction=fraction,
        lifecycle_state=lifecycle,
        graduation_state=str(payload.get("graduation_state") or lifecycle),
        earliest_executable_price=_finite(_first(payload, ("entry_all_in_price_sol", "entry_price", "earliest_executable_price"))),
        eligible=not str(result.get("decision") or "").startswith("no_entry"),
        executed=fraction > 0.0,
        discovery_route=str(payload.get("discovery_route") or "fomo"),
        market_state=state,
    )
    if evaluation.lane_state.mode == "reduced" and fraction > 0.0:
        fraction *= evaluation.lane_state.capital_multiplier
        result["position_fraction"] = fraction
    result["v52_market_validation_completion"] = {
        "graduation_quality": asdict(evaluation.graduation_quality),
        "lane_relative_score": asdict(evaluation.lane_relative_score),
        "continuation": asdict(evaluation.continuation),
        "lane_state": asdict(evaluation.lane_state),
        "shadow_variants": evaluation.shadow_decisions,
    }
    return result


def _robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("market-validation completion Robinhood predecessor unavailable")
    lane, fraction, profiles = _BASE_ROBINHOOD_CHOOSE(self, **kwargs)
    copied = {key: dict(value) if isinstance(value, Mapping) else value for key, value in dict(profiles or {}).items()}
    if not lane:
        return lane, float(fraction or 0.0), copied
    payload = dict(kwargs)
    payload["v52_authority"] = _profile_authority(copied, lane)
    token = str(getattr(self, "_roi_v52_candidate_token", "") or payload.get("token_mint") or payload.get("token") or "")
    if token:
        payload["token_mint"] = token
    at = payload.get("observed_at") or payload.get("received_at") or _utcnow()
    evaluation = completion().evaluate_candidate(
        lane="robinhood",
        observed_at=at,
        payload=payload,
        authoritative_fraction=float(fraction or 0.0),
        lifecycle_state=str(payload.get("lifecycle") or "robinhood"),
        graduation_state=str(payload.get("graduation_state") or ""),
        earliest_executable_price=_finite(_first(payload, ("entry_price", "entry_all_in_price_sol", "earliest_executable_price"))),
        eligible=True,
        executed=float(fraction or 0.0) > 0.0,
        discovery_route="robinhood",
        market_state=str(payload.get("flow_state") or payload.get("fomo_state") or ""),
    )
    final_fraction = float(fraction or 0.0)
    if evaluation.lane_state.mode == "reduced" and final_fraction > 0.0:
        final_fraction *= evaluation.lane_state.capital_multiplier
    if lane in copied and isinstance(copied[lane], Mapping):
        profile = dict(copied[lane])
        profile["v52_market_validation_completion"] = {
            "graduation_quality": asdict(evaluation.graduation_quality),
            "lane_relative_score": asdict(evaluation.lane_relative_score),
            "continuation": asdict(evaluation.continuation),
            "lane_state": asdict(evaluation.lane_state),
            "shadow_variants": evaluation.shadow_decisions,
        }
        copied[lane] = profile
    return (lane if final_fraction > 0.0 else None), max(0.0, final_fraction), copied


async def _final_buy(self: Any, row: dict[str, Any]) -> None:
    if _BASE_FINAL_BUY is None:
        raise RuntimeError("market-validation completion buy predecessor unavailable")
    await _BASE_FINAL_BUY(self, row)
    signature = str(row.get("signature") or "")
    if not signature:
        return
    try:
        with self.store._lock:
            selected = self.store.db.execute(
                "SELECT lane,lifecycle,position_fraction FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND source_signature=? AND selected=1 ORDER BY id DESC LIMIT 1",
                (self.release_commit, signature),
            ).fetchone()
            trial = self.store.db.execute(
                "SELECT token_mint,trigger_wallet,observed_at,received_at,entry_all_in_price_sol,round_trip_cost_fraction,"
                "entry_executable,exit_executable,opportunity_json,context_json,decision_json "
                "FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? AND lane='unified_profit_maximizer' "
                "ORDER BY id DESC LIMIT 1",
                (self.epoch_id, signature),
            ).fetchone()
        if selected is None or trial is None:
            return
        payload = {
            **dict(row),
            **_safe_dict(trial["opportunity_json"]),
            **_safe_dict(trial["context_json"]),
            **_safe_dict(trial["decision_json"]),
            "token_mint": trial["token_mint"],
            "trigger_wallet": trial["trigger_wallet"],
            "source_signature": signature,
            "round_trip_cost_fraction": trial["round_trip_cost_fraction"],
            "entry_executable": trial["entry_executable"],
            "exit_executable": trial["exit_executable"],
        }
        completion().evaluate_candidate(
            lane=str(selected["lane"]),
            observed_at=str(trial["observed_at"] or trial["received_at"]),
            payload=payload,
            authoritative_fraction=float(selected["position_fraction"] or 0.0),
            lifecycle_state=str(selected["lifecycle"] or ""),
            graduation_state=str(selected["lifecycle"] or ""),
            earliest_executable_price=_finite(trial["entry_all_in_price_sol"]),
            eligible=bool(trial["entry_executable"] and trial["exit_executable"]),
            executed=float(selected["position_fraction"] or 0.0) > 0.0,
            discovery_route=str(row.get("source") or "solana"),
            market_state=str(payload.get("flow_state") or ""),
        )
    except Exception:
        return


async def _final_sell(self: Any, row: dict[str, Any]) -> None:
    if _BASE_FINAL_SELL is None:
        raise RuntimeError("market-validation completion sell predecessor unavailable")
    await _BASE_FINAL_SELL(self, row)
    token = str(row.get("token_mint") or "")
    if not token:
        return
    try:
        with self.store._lock:
            outcomes = self.store.db.execute(
                "SELECT v.source_signature,v.observed_at,o.net_return,o.exit_reason "
                "FROM risk_conditioned_alpha_v5_trials v JOIN profit_first_final_outcomes o "
                "ON o.epoch_id=? AND o.source_signature=v.source_signature AND o.lane='unified_profit_maximizer' "
                "WHERE v.release_commit=? AND v.token_mint=? AND v.selected=1 ORDER BY o.id DESC",
                (self.epoch_id, self.release_commit, token),
            ).fetchall()
        for outcome in outcomes:
            candidate = str(outcome["source_signature"])
            observed_at = str(outcome["observed_at"])
            with self.store._lock:
                exists = self.store.db.execute(
                    "SELECT 1 FROM v52_market_validation_lane_events WHERE candidate_key=? AND observed_at=? AND net_return IS NULL LIMIT 1",
                    (candidate, observed_at),
                ).fetchone()
            if exists is None:
                continue
            completion().resolve_outcome(
                candidate_key=candidate,
                observed_at=observed_at,
                net_return=float(outcome["net_return"]),
                resolved_at=_utcnow(),
            )
    except Exception:
        return


def _preserve_lineage(wrapper: Any, predecessor: Any, flag: str) -> None:
    if not callable(predecessor):
        raise RuntimeError("market-validation completion predecessor unavailable")
    setattr(wrapper, "__wrapped__", predecessor)
    predecessor_module = getattr(predecessor, "__module__", None)
    if predecessor_module:
        setattr(wrapper, "__module__", predecessor_module)
    for name, value in vars(predecessor).items():
        if name.startswith("_roi_") and not hasattr(wrapper, name):
            setattr(wrapper, name, value)
    setattr(wrapper, flag, True)
    setattr(wrapper, "_roi_v52_final_authority", True)


def install_v52_market_validation_completion(
    controller: base.MarketValidationController,
    governed: governance_module.MarketValidationGovernance,
) -> MarketValidationCompletion:
    global _COMPLETION, _INSTALLED, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_ROBINHOOD_CHOOSE
    global _BASE_FINAL_BUY, _BASE_FINAL_SELL
    if _COMPLETION is None or _COMPLETION.controller is not controller:
        _COMPLETION = MarketValidationCompletion(controller, governed)
    if _INSTALLED:
        return _COMPLETION

    from . import fomo_paper_strategy as fomo_paper
    from . import risk_conditioned_alpha_v5 as solana_strategy
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_ROBINHOOD_CHOOSE = RobinhoodChainPaperPlane._v5_choose_lane_fraction
    _BASE_FINAL_BUY = FinalProfitFirstResearchAdapter._buy
    _BASE_FINAL_SELL = FinalProfitFirstResearchAdapter._sell

    _preserve_lineage(_solana_choose, _BASE_SOLANA_CHOOSE, "_roi_v52_market_validation_completion")
    _preserve_lineage(_fomo_decision, _BASE_FOMO_DECISION, "_roi_v52_market_validation_completion")
    _preserve_lineage(_robinhood_choose, _BASE_ROBINHOOD_CHOOSE, "_roi_v52_market_validation_completion")
    _preserve_lineage(_final_buy, _BASE_FINAL_BUY, "_roi_v52_market_validation_completion")
    _preserve_lineage(_final_sell, _BASE_FINAL_SELL, "_roi_v52_market_validation_completion")

    solana_strategy._choose_lane_and_fraction = _solana_choose
    fomo_paper._paper_decision = _fomo_decision
    RobinhoodChainPaperPlane._v5_choose_lane_fraction = _robinhood_choose  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._buy = _final_buy  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._sell = _final_sell  # type: ignore[method-assign]
    _INSTALLED = True
    return _COMPLETION


def status() -> dict[str, Any]:
    if _COMPLETION is None:
        return {
            "version": VERSION,
            "installed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return _COMPLETION.status()


__all__ = [
    "CONTINUATION_HORIZONS_SECONDS",
    "CompletionEvaluation",
    "LaneCapitalState",
    "MarketValidationCompletion",
    "SHADOW_VARIANTS",
    "VERSION",
    "completion",
    "install_v52_market_validation_completion",
    "status",
]
