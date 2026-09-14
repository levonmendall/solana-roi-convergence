from __future__ import annotations

import json
import math
import re
from dataclasses import asdict
from datetime import datetime, timezone
from types import MethodType
from typing import Any, Iterable, Mapping, Sequence

from . import v52_market_validation_completion as completion_module
from . import v52_market_validation_governance as governance_module

VERSION = "v52-market-validation-completion-hardening-v1"
_INSTALLED = False
_HARDENING: "MarketValidationCompletionHardening | None" = None

_PARTICIPANT_KEYS = (
    "participants",
    "participant_records",
    "buyers",
    "buyer_records",
    "wallet_participants",
    "independent_buyers",
)
_WALLET_COLUMNS = ("wallet", "wallet_id", "owner", "buyer", "buyer_wallet")
_TOKEN_COLUMNS = ("token_mint", "mint", "token", "asset_mint")
_TIME_COLUMNS = ("observed_at", "received_at", "block_time", "timestamp", "created_at")
_ENTITY_COLUMNS = ("linked_entity_id", "entity_id", "funding_cluster_id", "funder_cluster_id")
_SIDE_COLUMNS = ("side", "trade_side", "direction")
_NOTIONAL_COLUMNS = ("notional", "amount_sol", "notional_sol", "quote_amount", "size")
_CREATOR_COLUMNS = ("creator_associated", "creator_linked", "is_creator_associated")
_FUNDER_COLUMNS = ("funder_associated", "funder_linked", "is_funder_associated")
_PRICE_KEYS = (
    "current_executable_price",
    "executable_price",
    "current_price",
    "mark_price",
    "all_in_price_sol",
    "exit_all_in_price_sol",
    "entry_all_in_price_sol",
    "entry_price",
)
_COMPONENT_PAIRS = {
    "wallet_intelligence": ("C_v52_full", "B_v52_no_wallet"),
    "pre_graduation_entry_bundle": ("B_v52_no_wallet", "A_graduation_only"),
    "graduation_quality": ("G_full_proposed_alpha_gated", "C_v52_full"),
    "post_graduation_decay": ("G_full_proposed_alpha_gated", "D_v52_plus_graduation_quality"),
    "lane_relative_calibration": ("G_full_proposed_alpha_gated", "E_v52_plus_graduation_quality_decay"),
    "lane_gating": ("G_full_proposed_alpha_gated", "F_v52_plus_graduation_quality_decay_lane_calibration"),
}


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _parse_time(value).isoformat()


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(mapping: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in mapping and mapping.get(name) is not None:
            return mapping.get(name)
    return None


def _slug(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return text[:64] or "unclassified"


def _graduated(state: Any) -> bool:
    text = str(state or "").lower()
    return any(token in text for token in ("graduat", "pumpswap", "pump_amm", "post_graduation", "post-graduation"))


def calibration_lane(execution_lane: str, market_state: str | None) -> str:
    canonical = completion_module.base.canonical_alpha_lane(execution_lane)
    if canonical != "fomo":
        return canonical
    return f"fomo::{_slug(market_state)}"


def _serialize_evaluation(value: completion_module.CompletionEvaluation) -> str:
    return json.dumps(asdict(value), sort_keys=True)


def _deserialize_evaluation(payload: str) -> completion_module.CompletionEvaluation:
    raw = json.loads(payload)
    return completion_module.CompletionEvaluation(
        candidate_key=str(raw["candidate_key"]),
        lane=str(raw["lane"]),
        observed_at=str(raw["observed_at"]),
        graduation_quality=governance_module.PointInTimeComposite(**raw["graduation_quality"]),
        lane_relative_score=governance_module.PointInTimeComposite(**raw["lane_relative_score"]),
        continuation=governance_module.ContinuationPersistence(**raw["continuation"]),
        lane_state=completion_module.LaneCapitalState(**raw["lane_state"]),
        shadow_decisions=dict(raw["shadow_decisions"]),
        independent_actor_metrics=governance_module.IndependentActorMetrics(**raw["independent_actor_metrics"]),
    )


class MarketValidationCompletionHardening:
    def __init__(self, engine: completion_module.MarketValidationCompletion) -> None:
        self.engine = engine
        self.store = engine.store
        self._base_evaluate = engine.evaluate_candidate
        self._base_lane_state = engine.lane_capital_state
        self._base_mark = engine.record_market_mark
        self._base_resolve = engine.resolve_outcome
        self._schema()

    def _schema(self) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_completion_evaluations ("
                "candidate_key TEXT NOT NULL, observed_at TEXT NOT NULL, execution_lane TEXT NOT NULL, "
                "calibration_lane TEXT NOT NULL, market_state TEXT, actor_source TEXT NOT NULL, actor_evidence_json TEXT NOT NULL, "
                "evaluation_json TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "PRIMARY KEY(candidate_key,observed_at,execution_lane,calibration_lane))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_shadow_entries ("
                "candidate_key TEXT NOT NULL, observed_at TEXT NOT NULL, variant_id TEXT NOT NULL, token_mint TEXT, "
                "entry_at TEXT NOT NULL, entry_price REAL NOT NULL, entry_cost_fraction REAL NOT NULL, source TEXT NOT NULL, "
                "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
                "PRIMARY KEY(candidate_key,observed_at,variant_id))"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS v52_market_validation_hardening_audit ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_key TEXT, observed_at TEXT NOT NULL, event TEXT NOT NULL, "
                "evidence_json TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
            )

    def _columns(self, table: str) -> set[str]:
        try:
            safe = table.replace('"', '""')
            with self.store._lock:
                return {str(row[1]) for row in self.store.db.execute(f'PRAGMA table_info("{safe}")').fetchall()}
        except Exception:
            return set()

    def _explicit_participants(self, payload: Mapping[str, Any], decision_at: datetime) -> list[dict[str, Any]]:
        sources: list[Mapping[str, Any]] = [payload]
        for key in ("risk", "context", "opportunity", "metadata", "market_context", "wallet_evidence"):
            nested = payload.get(key)
            if isinstance(nested, Mapping):
                sources.append(nested)
            elif isinstance(nested, str):
                try:
                    decoded = json.loads(nested)
                except Exception:
                    decoded = None
                if isinstance(decoded, Mapping):
                    sources.append(decoded)
        result: list[dict[str, Any]] = []
        for source in sources:
            for key in _PARTICIPANT_KEYS:
                rows = source.get(key)
                if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                    continue
                for raw in rows:
                    if not isinstance(raw, Mapping):
                        continue
                    row = dict(raw)
                    timestamp = _first(row, _TIME_COLUMNS)
                    if timestamp is not None:
                        try:
                            if _parse_time(timestamp) >= decision_at:
                                continue
                        except Exception:
                            continue
                    result.append(row)
        return result

    def _durable_participants(self, token_mint: str | None, decision_at: datetime) -> list[dict[str, Any]]:
        if not token_mint:
            return []
        try:
            with self.store._lock:
                tables = [str(row[0]) for row in self.store.db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()]
        except Exception:
            return []
        results: list[dict[str, Any]] = []
        cutoff = decision_at.isoformat()
        for table in tables:
            if table.startswith("v52_market_validation_"):
                continue
            columns = self._columns(table)
            wallet_col = next((name for name in _WALLET_COLUMNS if name in columns), None)
            token_col = next((name for name in _TOKEN_COLUMNS if name in columns), None)
            time_col = next((name for name in _TIME_COLUMNS if name in columns), None)
            if not wallet_col or not token_col or not time_col:
                continue
            selected = [wallet_col, token_col, time_col]
            for candidates in (_ENTITY_COLUMNS, _SIDE_COLUMNS, _NOTIONAL_COLUMNS, _CREATOR_COLUMNS, _FUNDER_COLUMNS):
                match = next((name for name in candidates if name in columns), None)
                if match and match not in selected:
                    selected.append(match)
            quoted_table = table.replace('"', '""')
            quoted_cols = ",".join(f'"{name.replace(chr(34), chr(34)*2)}"' for name in selected)
            try:
                with self.store._lock:
                    rows = self.store.db.execute(
                        f'SELECT {quoted_cols} FROM "{quoted_table}" WHERE "{token_col}"=? AND "{time_col}"<? '
                        f'ORDER BY "{time_col}" DESC LIMIT 1000',
                        (str(token_mint), cutoff),
                    ).fetchall()
            except Exception:
                continue
            for raw in rows:
                item = dict(raw)
                normalized = {
                    "wallet": item.get(wallet_col),
                    "observed_at": item.get(time_col),
                    "side": item.get(next((n for n in _SIDE_COLUMNS if n in item), ""), "buy"),
                    "notional": item.get(next((n for n in _NOTIONAL_COLUMNS if n in item), ""), 0.0),
                    "creator_associated": item.get(next((n for n in _CREATOR_COLUMNS if n in item), ""), False),
                    "funder_associated": item.get(next((n for n in _FUNDER_COLUMNS if n in item), ""), False),
                    "source_table": table,
                }
                for name in _ENTITY_COLUMNS:
                    if name in item and item.get(name) not in (None, ""):
                        normalized[name] = item.get(name)
                results.append(normalized)
        return results

    def participants(self, payload: Mapping[str, Any], token_mint: str | None, decision_at: Any) -> tuple[list[dict[str, Any]], str]:
        at = _parse_time(decision_at)
        explicit = self._explicit_participants(payload, at)
        durable = self._durable_participants(token_mint, at)
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for row in [*explicit, *durable]:
            wallet = str(_first(row, _WALLET_COLUMNS) or "").strip()
            if not wallet:
                continue
            entity = str(_first(row, _ENTITY_COLUMNS) or "")
            when = str(_first(row, _TIME_COLUMNS) or "")
            side = str(_first(row, _SIDE_COLUMNS) or "buy")
            key = (wallet, entity, when, side)
            if key in seen:
                continue
            seen.add(key)
            item = dict(row)
            item["wallet"] = wallet
            merged.append(item)
        source = "payload+durable" if explicit and durable else ("payload" if explicit else ("durable" if durable else "wallet_identity_unavailable"))
        return merged, source

    def lane_capital_state(self, engine: completion_module.MarketValidationCompletion, lane: str, decision_at: Any) -> completion_module.LaneCapitalState:
        governed = engine.governed.governed_lane_state_at(lane, decision_at)
        if governed.mode == "observe_only":
            return completion_module.LaneCapitalState(governed.lane, "observe_only", False, 0.0, governed.reason, governed.evaluated_at)
        long_windows = (governed.evidence_windows["7d"], governed.evidence_windows["30d"])
        minimum = max(1, int(completion_module.target_sizing_policy().get("minimum_forward_samples", 30)))
        enough = all(item.sample_count > 0 for item in long_windows) and governed.evidence_windows["30d"].sample_count >= minimum
        if not enough:
            return completion_module.LaneCapitalState(governed.lane, "insufficient_evidence", True, 1.0, governed.reason, governed.evaluated_at)
        fully_positive = all(item.positive_evidence for item in long_windows) and governed.negative_streak == 0
        if governed.mode == "active" and fully_positive:
            return completion_module.LaneCapitalState(governed.lane, "active", True, 1.0, governed.reason, governed.evaluated_at)
        weighted_probability = 0.0
        total_weight = 0
        for item in long_windows:
            if item.probability_positive is None:
                continue
            weight = max(1, int(item.sample_count))
            weighted_probability += float(item.probability_positive) * weight
            total_weight += weight
        probability = weighted_probability / total_weight if total_weight else 1.0
        persistence = 1.0 / (1.0 + max(0, governed.negative_streak))
        multiplier = max(1.0 / (1.0 + max(1, total_weight)), min(1.0, probability * persistence))
        return completion_module.LaneCapitalState(
            governed.lane,
            "reduced",
            True,
            multiplier,
            "evidence_derived_reduced_capital_pending_full_multi_horizon_confirmation",
            governed.evaluated_at,
        )

    def _candidate_key(self, payload: Mapping[str, Any], lane: str, observed_at: str) -> str:
        return completion_module._candidate_key(payload, lane, observed_at)

    def _cached(self, candidate_key: str, observed_at: str, execution_lane: str, calibration: str) -> completion_module.CompletionEvaluation | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT evaluation_json FROM v52_market_validation_completion_evaluations "
                "WHERE candidate_key=? AND observed_at=? AND execution_lane=? AND calibration_lane=?",
                (candidate_key, observed_at, execution_lane, calibration),
            ).fetchone()
        return _deserialize_evaluation(str(row["evaluation_json"])) if row is not None else None

    def _automatic_mark(self, payload: Mapping[str, Any], token: str | None, observed_at: str, graduation_state: str | None) -> None:
        if not token:
            return
        price = _finite(_first(payload, _PRICE_KEYS))
        if price is None or price <= 0.0:
            return
        cost = _finite(_first(payload, ("round_trip_cost_fraction", "cost_fraction", "slippage_fraction"))) or 0.0
        try:
            self.record_market_mark(self.engine, token_mint=token, marked_at=observed_at, price=price, cost_fraction=max(0.0, cost), graduation_state=graduation_state)
        except Exception:
            return

    def evaluate_candidate(self, engine: completion_module.MarketValidationCompletion, **kwargs: Any) -> completion_module.CompletionEvaluation:
        lane = completion_module.base.canonical_alpha_lane(kwargs["lane"])
        observed_at = _iso(kwargs["observed_at"])
        payload = dict(kwargs.get("payload") or {})
        token = str(_first(payload, ("token_mint", "token", "mint")) or "") or None
        market_state = str(kwargs.get("market_state") or payload.get("market_state") or payload.get("state") or payload.get("fomo_state") or "")
        calibration = calibration_lane(lane, market_state)
        candidate_key = self._candidate_key(payload, lane, observed_at)
        self._automatic_mark(payload, token, observed_at, kwargs.get("graduation_state"))
        cached = self._cached(candidate_key, observed_at, lane, calibration)
        if cached is not None:
            return cached

        participants, actor_source = self.participants(payload, token, observed_at)
        actors = governance_module.independent_actor_metrics(participants)
        actor_features = {
            "independent_buyer_breadth": actors.independent_buyer_breadth,
            "creator_associated_activity": actors.creator_or_funder_contamination,
            "funder_associated_activity": actors.creator_or_funder_contamination,
            "linked_wallet_clustering": actors.linked_wallet_clustering,
            "repeated_entity_activity": actors.repeated_entity_activity,
            "wallet_integrity": actors.independent_notional_fraction,
        }
        enriched_payload = dict(payload)
        for key, value in actor_features.items():
            enriched_payload.setdefault(key, value)
        call = dict(kwargs)
        call["lane"] = lane
        call["observed_at"] = observed_at
        call["payload"] = enriched_payload
        base_evaluation = self._base_evaluate(**call)
        graduation = base_evaluation.graduation_quality
        lane_relative = base_evaluation.lane_relative_score
        continuation = base_evaluation.continuation
        if calibration != lane:
            metrics = governance_module.graduation_metrics_with_actor_integrity(engine.extract_metrics(enriched_payload), participants)
            graduation = engine.governed.graduation_quality_at(calibration, metrics, observed_at)
            lane_relative = engine.governed.lane_relative_score_at(calibration, metrics, observed_at)
            expected = graduation.score if graduation.calibrated else lane_relative.score if lane_relative.calibrated else None
            continuation = engine.governed.continuation_at(calibration, metrics, observed_at, expected_continuation=expected)
            engine.controller.observe_features(
                calibration,
                metrics,
                observed_at=observed_at,
                discovery_route=kwargs.get("discovery_route"),
                market_state=market_state,
            )
        authority = completion_module._safe_dict(enriched_payload.get("v52_authority"))
        shadows = engine.shadow_decisions(
            authoritative_fraction=float(kwargs.get("authoritative_fraction") or 0.0),
            authority=authority,
            lifecycle_state=kwargs.get("lifecycle_state"),
            graduation_state=kwargs.get("graduation_state"),
            graduation_quality=graduation,
            lane_relative_score=lane_relative,
            continuation=continuation,
            lane_state=base_evaluation.lane_state,
        )
        result = completion_module.CompletionEvaluation(
            candidate_key=base_evaluation.candidate_key,
            lane=lane,
            observed_at=observed_at,
            graduation_quality=graduation,
            lane_relative_score=lane_relative,
            continuation=continuation,
            lane_state=base_evaluation.lane_state,
            shadow_decisions=shadows,
            independent_actor_metrics=actors,
        )
        actor_evidence = {
            "source": actor_source,
            "calibration_lane": calibration,
            "execution_lane": lane,
            "market_state": market_state or None,
            "metrics": asdict(actors),
            "unknown_wallets_are_not_collapsed": True,
            "future_records_excluded": True,
        }
        relative = {
            "calibration_lane": calibration,
            "graduation": graduation.component_percentiles,
            "lane": lane_relative.component_percentiles,
            "continuation": continuation.component_percentiles,
        }
        engine.governed.record_point_in_time_decision(
            candidate_key=result.candidate_key,
            lane=lane,
            observed_at=observed_at,
            lifecycle_state=kwargs.get("lifecycle_state"),
            graduation_state=kwargs.get("graduation_state"),
            raw_features=engine.extract_metrics(enriched_payload),
            relative_features=relative,
            graduation_quality=graduation.score,
            continuation_persistence=continuation.score,
            lane_alpha_mode=result.lane_state.mode,
            wallet_evidence=actor_evidence,
            v52_decision={"position_fraction": float(kwargs.get("authoritative_fraction") or 0.0), "lane_state": asdict(result.lane_state)},
            shadow_decision=shadows,
            earliest_executable_price=kwargs.get("earliest_executable_price"),
        )
        with self.store._lock, self.store.db:
            for variant, decision in shadows.items():
                self.store.db.execute(
                    "UPDATE v52_market_validation_shadow_variants SET decision_fraction=?,decision=?,evidence_json=? "
                    "WHERE candidate_key=? AND observed_at=? AND variant_id=?",
                    (float(decision["position_fraction"]), str(decision["decision"]), json.dumps(decision, sort_keys=True), result.candidate_key, observed_at, variant),
                )
            self.store.db.execute(
                "INSERT INTO v52_market_validation_completion_evaluations("
                "candidate_key,observed_at,execution_lane,calibration_lane,market_state,actor_source,actor_evidence_json,evaluation_json,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,1,0)",
                (result.candidate_key, observed_at, lane, calibration, market_state or None, actor_source, json.dumps(actor_evidence, sort_keys=True), _serialize_evaluation(result)),
            )
        return result

    def record_market_mark(self, engine: completion_module.MarketValidationCompletion, **kwargs: Any) -> int:
        resolved = self._base_mark(**kwargs)
        token = str(kwargs.get("token_mint") or "")
        marked_at = _iso(kwargs["marked_at"])
        price = float(kwargs["price"])
        cost = max(0.0, float(kwargs.get("cost_fraction") or 0.0))
        state = kwargs.get("graduation_state")
        if not token or price <= 0.0:
            return resolved
        with self.store._lock:
            candidates = self.store.db.execute(
                "SELECT e.candidate_key,e.observed_at,s.decision_fraction FROM v52_market_validation_lane_events e "
                "JOIN v52_market_validation_shadow_variants s ON s.candidate_key=e.candidate_key AND s.observed_at=e.observed_at "
                "WHERE e.token_mint=? AND s.variant_id='A_graduation_only' ORDER BY e.observed_at",
                (token,),
            ).fetchall()
        for row in candidates:
            candidate = str(row["candidate_key"])
            observed = str(row["observed_at"])
            with self.store._lock:
                entry = self.store.db.execute(
                    "SELECT * FROM v52_market_validation_shadow_entries WHERE candidate_key=? AND observed_at=? AND variant_id='A_graduation_only'",
                    (candidate, observed),
                ).fetchone()
            if entry is None:
                if not _graduated(state):
                    continue
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "INSERT OR IGNORE INTO v52_market_validation_shadow_entries("
                        "candidate_key,observed_at,variant_id,token_mint,entry_at,entry_price,entry_cost_fraction,source,paper_only,live_money_authority"
                        ") VALUES (?,?,'A_graduation_only',?,?,?,?,?,1,0)",
                        (candidate, observed, token, marked_at, price, cost, "first_point_in_time_executable_graduation_mark"),
                    )
                continue
            entry_at = _parse_time(entry["entry_at"])
            mark_time = _parse_time(marked_at)
            if mark_time <= entry_at:
                continue
            entry_price = float(entry["entry_price"])
            net = price / entry_price - 1.0 - float(entry["entry_cost_fraction"] or 0.0) - cost
            fraction = float(row["decision_fraction"] or 0.0)
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE v52_market_validation_shadow_variants SET net_return=?,portfolio_contribution=?,outcome_status='resolved_graduation_counterfactual',resolved_at=? "
                    "WHERE candidate_key=? AND observed_at=? AND variant_id='A_graduation_only'",
                    (net, fraction * net, marked_at, candidate, observed),
                )
        return resolved

    def resolve_outcome(self, engine: completion_module.MarketValidationCompletion, **kwargs: Any) -> None:
        self._base_resolve(**kwargs)
        variants = dict(kwargs.get("variant_returns") or {})
        if not variants:
            return
        candidate = str(kwargs["candidate_key"])
        observed = _iso(kwargs["observed_at"])
        resolved_at = _iso(kwargs.get("resolved_at") or datetime.now(timezone.utc))
        with self.store._lock, self.store.db:
            for component, (with_key, without_key) in _COMPONENT_PAIRS.items():
                with_value = _finite(variants.get(with_key))
                without_value = _finite(variants.get(without_key))
                if with_value is None or without_value is None:
                    continue
                self.store.db.execute(
                    "INSERT OR REPLACE INTO v52_market_validation_component_ablation("
                    "candidate_key,observed_at,component,with_component_return,without_component_return,incremental_return,resolved_at,"
                    "causal_claim,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,0,1,0)",
                    (candidate, observed, component, with_value, without_value, with_value - without_value, resolved_at),
                )

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            evaluations = int(self.store.db.execute("SELECT COUNT(*) FROM v52_market_validation_completion_evaluations").fetchone()[0])
            shadow_entries = int(self.store.db.execute("SELECT COUNT(*) FROM v52_market_validation_shadow_entries").fetchone()[0])
        return {
            "version": VERSION,
            "installed": _INSTALLED,
            "point_in_time_actor_sources": ["decision_payload_snapshot", "durable_sqlite_tables_with_explicit_identity_columns"],
            "unknown_wallets_are_never_implicitly_collapsed": True,
            "future_participant_records_excluded": True,
            "dynamic_durable_schema_discovery": True,
            "duplicate_candidate_observation_protection": True,
            "fomo_archetype_specific_calibration": True,
            "fomo_execution_authority_still_single_lane": True,
            "evidence_derived_reduced_multiplier": True,
            "fixed_reduced_multiplier": None,
            "graduation_only_real_executable_entry": True,
            "graduation_only_pregraduation_entry_allowed": False,
            "automatic_marks_from_repeated_candidate_evaluations": True,
            "component_ablation_with_and_without_returns": True,
            "component_ablation_causal_claim": False,
            "distinct_pons_lane_present_in_repository": False,
            "pons_lane_fabricated": False,
            "evaluations": evaluations,
            "graduation_shadow_entries": shadow_entries,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


def install_v52_market_validation_completion_hardening(
    engine: completion_module.MarketValidationCompletion,
) -> MarketValidationCompletionHardening:
    global _INSTALLED, _HARDENING
    if _HARDENING is None or _HARDENING.engine is not engine:
        _HARDENING = MarketValidationCompletionHardening(engine)
    if not _INSTALLED:
        engine.lane_capital_state = MethodType(_HARDENING.lane_capital_state, engine)
        engine.evaluate_candidate = MethodType(_HARDENING.evaluate_candidate, engine)
        engine.record_market_mark = MethodType(_HARDENING.record_market_mark, engine)
        engine.resolve_outcome = MethodType(_HARDENING.resolve_outcome, engine)
        _INSTALLED = True
    return _HARDENING


def hardening() -> MarketValidationCompletionHardening:
    if _HARDENING is None:
        raise RuntimeError("v5.2 market-validation completion hardening not installed")
    return _HARDENING


def status() -> dict[str, Any]:
    if _HARDENING is None:
        return {"version": VERSION, "installed": False, "paper_only": True, "live_money_authority": False}
    return _HARDENING.status()


__all__ = [
    "VERSION",
    "MarketValidationCompletionHardening",
    "calibration_lane",
    "hardening",
    "install_v52_market_validation_completion_hardening",
    "status",
]
