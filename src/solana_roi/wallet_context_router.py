from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from dataclasses import asdict
from statistics import median
from typing import Any, Callable, Iterable

from .config import BASELINE
from .profit_first_entity_final import FINAL_STRATEGY_VERSION, UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import (
    MIN_MATURE_FORWARD_SAMPLES,
    SEED_BY_ADDRESS,
    WalletEntityUniverseV4,
    WalletRole,
    score_role,
)
from .wallet_venue_lifecycle_research import (
    LIFECYCLE_STAGES,
    PUMP_BONDING_CURVE,
    VENUES,
    VenueLifecycleResearch,
)

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False

CONTEXT_ROUTER_VERSION = "wallet-context-router-v1"
PRODUCTION_PROCESSING_TARGET_SECONDS = 5.0
STRATEGY_ENTRY_CEILING_SECONDS = 20.0
MILLISECOND_SNIPING_TARGETED = False
FIRST_SLOT_EXECUTION_AUTHORITY = False
SOURCE_PRE_OBSERVATION_RETURN_AUTHORITY = False
CONTEXT_MIN_FORWARD_SAMPLES = MIN_MATURE_FORWARD_SAMPLES
MAX_CONTEXT_LEADERS = 5
LATENCY_BUCKETS_SECONDS = (1, 2, 5, 10, 20, 30, 60)

_ORIGINAL_UNIVERSE_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "SOLANA_ROI_RELEASE_COMMIT", "GITHUB_SHA"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unbound-local-release"


def _table_exists(store: Any, name: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        ).fetchone()
    return row is not None


def _column_exists(store: Any, table: str, column: str) -> bool:
    if not _table_exists(store, table):
        return False
    with store._lock:
        rows = store.db.execute(f"PRAGMA table_info({table})").fetchall()
    return column in {str(row["name"]) for row in rows}


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _median(values: Iterable[Any]) -> float | None:
    clean = [value for raw in values if (value := _safe_float(raw)) is not None]
    return float(median(clean)) if clean else None


def _latency_bucket(seconds: float | None) -> str:
    if seconds is None or not math.isfinite(seconds) or seconds < 0.0:
        return "unknown"
    for bound in LATENCY_BUCKETS_SECONDS:
        if seconds <= float(bound):
            return f"lte_{bound}s"
    return "gt_60s"


def _trimmed_mean_ex_best(values: list[float], count: int) -> float | None:
    if len(values) <= count:
        return None
    kept = sorted(values, reverse=True)[count:]
    return sum(kept) / len(kept) if kept else None


def classify_observation_accessibility(row: dict[str, Any]) -> dict[str, Any]:
    """Classify only whether the observation is structurally compatible with this runtime.

    This does not assert profitability. Source-wallet gains before the observation are
    never credited, and a first-slot/sub-second requirement is never treated as an
    executable capability of this architecture.
    """

    venue = str(row.get("venue") or "UNKNOWN")
    stage = str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue")
    lag_ms = _safe_float(row.get("observation_lag_ms")) or 0.0
    processing_ms = _safe_float(row.get("processing_delay_ms")) or 0.0
    total_seconds = max(0.0, lag_ms + processing_ms) / 1000.0
    chase = _safe_float(row.get("chase_fraction"))

    reasons: list[str] = []
    if venue not in VENUES:
        reasons.append("unknown_or_unsupported_venue")
    if not bool(row.get("copyable")):
        reasons.append("not_copyable_at_observation")
    if total_seconds > STRATEGY_ENTRY_CEILING_SECONDS:
        reasons.append("outside_strategy_entry_ceiling")
    if chase is not None and chase > float(BASELINE.max_chase_fraction):
        reasons.append("outside_max_chase")

    pump_usage = None
    if venue == "PUMP_FUN" and stage == PUMP_BONDING_CURVE:
        pump_usage = "discovery_and_residual_continuation_only_not_first_slot_sniping"

    return {
        "structurally_accessible": not reasons,
        "reasons": reasons,
        "venue": venue,
        "lifecycle_stage": stage,
        "observed_pipeline_seconds": total_seconds,
        "chase_fraction": chase,
        "pump_fun_usage": pump_usage,
        "millisecond_sniping_targeted": False,
        "first_slot_execution_authority": False,
        "source_pre_observation_return_authority": False,
    }


def _context_metrics(role: WalletRole, rows: list[dict[str, Any]]) -> dict[str, Any]:
    values: list[float] = []
    scaled_returns: list[float] = []
    deployed = 0.0
    weighted = 0.0
    latency_values: list[float] = []
    latency_groups: dict[str, list[float]] = defaultdict(list)

    for row in rows:
        value = _safe_float(row.get("net_return"))
        if value is None:
            continue
        values.append(value)
        fraction = _safe_float(row.get("position_fraction"))
        if fraction is not None and fraction > 0.0:
            deployed += fraction
            weighted += fraction * value
            scaled_returns.append(fraction * value)
        latency = _safe_float(row.get("signal_to_entry_seconds"))
        if latency is not None:
            latency_values.append(latency)
            latency_groups[_latency_bucket(latency)].append(value)

    compounded = None
    max_drawdown = None
    if scaled_returns:
        equity = 1.0
        peak = 1.0
        max_dd = 0.0
        for value in scaled_returns:
            equity *= max(1e-9, 1.0 + value)
            peak = max(peak, equity)
            if peak > 0.0:
                max_dd = max(max_dd, (peak - equity) / peak)
        compounded = equity - 1.0
        max_drawdown = min(1.0, max(0.0, max_dd))

    score = score_role(role, values)
    latency_curve = {
        bucket: {
            "sample_count": len(bucket_values),
            "mean_residual_roi": sum(bucket_values) / len(bucket_values),
            "median_residual_roi": float(median(bucket_values)),
        }
        for bucket, bucket_values in sorted(latency_groups.items())
        if bucket_values
    }

    return {
        "sample_count": len(values),
        "mean_residual_roi": sum(values) / len(values) if values else None,
        "median_residual_roi": float(median(values)) if values else None,
        "trimmed_mean_residual_roi_ex_best_1": _trimmed_mean_ex_best(values, 1),
        "trimmed_mean_residual_roi_ex_best_3": _trimmed_mean_ex_best(values, 3),
        "trimmed_mean_residual_roi_ex_best_5": _trimmed_mean_ex_best(values, 5),
        "deployed_fraction_sum": deployed,
        "copyable_return_on_deployed_fraction": weighted / deployed if deployed > 0.0 else None,
        "compounded_fraction_scaled_return": compounded,
        "positive_rate": sum(value > 0.0 for value in values) / len(values) if values else None,
        "max_drawdown_fraction_scaled": max_drawdown,
        "median_signal_to_entry_seconds": float(median(latency_values)) if latency_values else None,
        "latency_residual_roi_curve": latency_curve,
        "context_score": score.score,
        "context_confidence": score.confidence,
        "mature_forward_context": len(values) >= CONTEXT_MIN_FORWARD_SAMPLES,
        "positive_forward_context": bool(score.score is not None and score.score > 0.0),
    }


class WalletContextRouter:
    """Read-only venue/lifecycle/role router for forward wallet/entity evidence.

    It assigns evidence, not trading authority. The current active strategy and tracked
    wallet set remain unchanged. Context recommendations are prospective research output
    that can be promoted only through the existing governed future-cohort process.
    """

    def __init__(self, universe: WalletEntityUniverseV4):
        self.universe = universe
        self.store = universe.store
        self.venue_research = VenueLifecycleResearch(universe)

    def _epoch(self) -> tuple[str | None, str | None]:
        return self.venue_research._epoch()

    def _observations(self, started_at: str | None) -> list[dict[str, Any]]:
        return self.venue_research._observations(started_at)

    def _outcomes(self, epoch_id: str | None) -> list[dict[str, Any]]:
        if not epoch_id or not _table_exists(self.store, "profit_first_final_outcomes"):
            return []
        columns = [
            "source_signature",
            "trigger_wallet",
            "token_mint",
            "lane",
            "position_fraction",
            "net_return",
            "signal_to_entry_seconds",
            "evidence_phase",
        ]
        if _column_exists(self.store, "profit_first_final_outcomes", "exit_signature"):
            columns.append("exit_signature")
        else:
            columns.append("NULL AS exit_signature")
        if _column_exists(self.store, "profit_first_final_outcomes", "exit_observed_at"):
            columns.append("exit_observed_at")
        else:
            columns.append("NULL AS exit_observed_at")
        if _column_exists(self.store, "profit_first_final_outcomes", "context_json"):
            columns.append("context_json")
        else:
            columns.append("NULL AS context_json")
        with self.store._lock:
            rows = self.store.db.execute(
                f"SELECT {','.join(columns)} FROM profit_first_final_outcomes "
                "WHERE epoch_id=? AND evidence_phase='forward' ORDER BY id",
                (epoch_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            try:
                context = json.loads(str(row.get("context_json") or "{}"))
                row["regime"] = str(context.get("regime") or "unknown")
            except Exception:
                row["regime"] = "unknown"
            result.append(row)
        return result

    def _confirmation_rows(
        self,
        epoch_id: str | None,
        by_signature: dict[str, dict[str, Any]],
        outcomes: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not epoch_id:
            return []
        required = ("v4_entity_signal_context", "profit_first_final_trials")
        if any(not _table_exists(self.store, table) for table in required):
            return []
        if not _column_exists(self.store, "profit_first_final_trials", "received_at"):
            return []

        with self.store._lock:
            contexts = self.store.db.execute(
                "SELECT token_mint,trigger_wallet,received_at,independent_wallets_json "
                "FROM v4_entity_signal_context WHERE epoch_id=? ORDER BY id",
                (epoch_id,),
            ).fetchall()
            trials = self.store.db.execute(
                "SELECT source_signature,token_mint,trigger_wallet,received_at "
                "FROM profit_first_final_trials WHERE epoch_id=? AND lane='entity_flow_momentum' ORDER BY id",
                (epoch_id,),
            ).fetchall()

        context_index = {
            (str(row["token_mint"]), str(row["trigger_wallet"]), str(row["received_at"])): row
            for row in contexts
        }
        outcome_by_signature = {
            str(row.get("source_signature") or ""): row
            for row in outcomes
            if str(row.get("lane") or "") == "entity_flow_momentum"
        }
        result: list[dict[str, Any]] = []
        for trial in trials:
            signature = str(trial["source_signature"])
            outcome = outcome_by_signature.get(signature)
            observation = by_signature.get(signature)
            if outcome is None or observation is None:
                continue
            context = context_index.get(
                (str(trial["token_mint"]), str(trial["trigger_wallet"]), str(trial["received_at"]))
            )
            if context is None:
                continue
            try:
                wallets = json.loads(str(context["independent_wallets_json"] or "[]"))
            except Exception:
                wallets = []
            for wallet in wallets:
                if not str(wallet):
                    continue
                result.append(
                    {
                        **outcome,
                        "wallet": str(wallet),
                        "role": WalletRole.CONFIRMATION_ALPHA,
                        "venue": observation["venue"],
                        "lifecycle_stage": observation["lifecycle_stage"],
                        "regime": str(outcome.get("regime") or "unknown"),
                    }
                )
        return result

    def _exit_rows(
        self,
        epoch_id: str | None,
        by_signature: dict[str, dict[str, Any]],
        outcomes: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not epoch_id or not _table_exists(self.store, "profit_first_final_exit_signals"):
            return []
        with self.store._lock:
            signals = self.store.db.execute(
                "SELECT source_signature,seller_wallet,features_json,signal_json "
                "FROM profit_first_final_exit_signals WHERE epoch_id=? ORDER BY id",
                (epoch_id,),
            ).fetchall()
        signals_by_signature = {str(row["source_signature"]): row for row in signals}
        result: list[dict[str, Any]] = []
        for outcome in outcomes:
            if str(outcome.get("lane") or "") != UNIFIED_LANE:
                continue
            exit_signature = str(outcome.get("exit_signature") or "")
            signal = signals_by_signature.get(exit_signature)
            observation = by_signature.get(str(outcome.get("source_signature") or ""))
            if signal is None or observation is None:
                continue
            wallet = str(signal["seller_wallet"])
            base = {
                **outcome,
                "wallet": wallet,
                "role": WalletRole.EXIT_ALPHA,
                "venue": observation["venue"],
                "lifecycle_stage": observation["lifecycle_stage"],
                "regime": str(outcome.get("regime") or "unknown"),
            }
            result.append(base)
            try:
                features = json.loads(str(signal["features_json"] or "{}"))
                payload = json.loads(str(signal["signal_json"] or "{}"))
                urgency = float(payload.get("urgency_score") or 0.0)
            except Exception:
                features, urgency = {}, 0.0
            if features.get("creator_distribution") or features.get("linked_entity_distribution"):
                value = _safe_float(outcome.get("net_return"))
                if value is not None:
                    result.append(
                        {
                            **base,
                            "role": WalletRole.DISTRIBUTION_WARNING,
                            "net_return": -value * min(1.0, max(0.0, urgency) / 3.0),
                        }
                    )
        return result

    def role_evidence(self) -> list[dict[str, Any]]:
        epoch_id, started_at = self._epoch()
        observations = self._observations(started_at)
        by_signature = {str(row["signature"]): row for row in observations}
        outcomes = self._outcomes(epoch_id)
        result: list[dict[str, Any]] = []
        seen_unified: set[tuple[str, str, WalletRole]] = set()

        for outcome in outcomes:
            signature = str(outcome.get("source_signature") or "")
            wallet = str(outcome.get("trigger_wallet") or "")
            observation = by_signature.get(signature)
            if not wallet or observation is None:
                continue
            lane = str(outcome.get("lane") or "")
            roles: list[WalletRole] = []
            if lane == UNIFIED_LANE:
                roles.append(WalletRole.COPYABLE_ROC)
                roles.append(WalletRole.SIGNAL_DECAY)
            elif lane == "clean_scout_alpha":
                roles.append(WalletRole.SCOUT_ALPHA)
            elif lane == "creator_insider_continuation":
                roles.append(WalletRole.CREATOR_ALPHA)
            elif lane in {"elite_wallet_continuation", "entity_flow_momentum"}:
                roles.append(WalletRole.MOMENTUM_ALPHA)

            for role in roles:
                key = (wallet, signature, role)
                if lane == UNIFIED_LANE and key in seen_unified:
                    continue
                seen_unified.add(key)
                result.append(
                    {
                        **outcome,
                        "wallet": wallet,
                        "role": role,
                        "venue": observation["venue"],
                        "lifecycle_stage": observation["lifecycle_stage"],
                        "regime": str(outcome.get("regime") or "unknown"),
                    }
                )

        result.extend(self._confirmation_rows(epoch_id, by_signature, outcomes))
        result.extend(self._exit_rows(epoch_id, by_signature, outcomes))
        return result

    def context_profiles(self) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str, str, str, WalletRole], list[dict[str, Any]]] = defaultdict(list)
        for row in self.role_evidence():
            wallet = str(row.get("wallet") or "")
            venue = str(row.get("venue") or "UNKNOWN")
            stage = str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue")
            regime = str(row.get("regime") or "unknown")
            role = row.get("role")
            if not wallet or not isinstance(role, WalletRole):
                continue
            grouped[(wallet, venue, stage, regime, role)].append(row)

        result: list[dict[str, Any]] = []
        for (wallet, venue, stage, regime, role), rows in sorted(
            grouped.items(), key=lambda item: (item[0][1], item[0][2], item[0][3], item[0][4].value, item[0][0])
        ):
            seed = SEED_BY_ADDRESS.get(wallet)
            result.append(
                {
                    "wallet": wallet,
                    "seed_name": seed.name if seed else None,
                    "venue": venue,
                    "lifecycle_stage": stage,
                    "regime": regime,
                    "role": role.value,
                    **_context_metrics(role, rows),
                    "context_has_trade_authority": False,
                    "context_has_tracking_mutation_authority": False,
                }
            )
        return result

    @staticmethod
    def _eligible_profile(row: dict[str, Any]) -> bool:
        return bool(
            row.get("mature_forward_context")
            and row.get("positive_forward_context")
            and row.get("context_score") is not None
        )

    def route_map(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str, str], dict[str, list[dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for row in profiles:
            if not self._eligible_profile(row):
                continue
            grouped[
                (str(row["venue"]), str(row["lifecycle_stage"]), str(row.get("regime") or "unknown"))
            ][str(row["role"])].append(row)

        result: list[dict[str, Any]] = []
        for (venue, stage, regime), roles in sorted(grouped.items()):
            routed_roles: dict[str, list[dict[str, Any]]] = {}
            for role, rows in sorted(roles.items()):
                ordered = sorted(
                    rows,
                    key=lambda row: (
                        float(row.get("context_score") or -999.0),
                        int(row.get("sample_count") or 0),
                    ),
                    reverse=True,
                )[:MAX_CONTEXT_LEADERS]
                routed_roles[role] = [
                    {
                        "wallet": row["wallet"],
                        "seed_name": row.get("seed_name"),
                        "context_score": row["context_score"],
                        "sample_count": row["sample_count"],
                        "copyable_return_on_deployed_fraction": row[
                            "copyable_return_on_deployed_fraction"
                        ],
                    }
                    for row in ordered
                ]
            result.append(
                {
                    "venue": venue,
                    "lifecycle_stage": stage,
                    "regime": regime,
                    "roles": routed_roles,
                    "scout_and_momentum_confirmations_are_interchangeable": False,
                    "pump_fun_scout_usage": (
                        "discovery_and_residual_continuation_evidence_only_not_first_slot_execution"
                        if venue == "PUMP_FUN" and stage == PUMP_BONDING_CURVE
                        else None
                    ),
                    "route_has_trade_authority": False,
                }
            )
        return result

    def tracking_recommendations(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        capacity = max(1, int(self.universe.discovery.policy.max_tracked_challengers))
        eligible = [row for row in profiles if self._eligible_profile(row)]
        eligible.sort(
            key=lambda row: (
                float(row.get("context_score") or -999.0),
                int(row.get("sample_count") or 0),
            ),
            reverse=True,
        )
        selected: list[dict[str, Any]] = []
        selected_wallets: set[str] = set()
        covered_contexts: set[tuple[str, str, str, str]] = set()

        # First pass maximizes role/venue/lifecycle/regime diversity.
        for row in eligible:
            key = (
                str(row["venue"]),
                str(row["lifecycle_stage"]),
                str(row.get("regime") or "unknown"),
                str(row["role"]),
            )
            wallet = str(row["wallet"])
            if key in covered_contexts or wallet in selected_wallets:
                continue
            selected.append(row)
            selected_wallets.add(wallet)
            covered_contexts.add(key)
            if len(selected) >= capacity:
                break

        # Second pass fills unused slots with the strongest remaining distinct wallets.
        if len(selected) < capacity:
            for row in eligible:
                wallet = str(row["wallet"])
                if wallet in selected_wallets:
                    continue
                selected.append(row)
                selected_wallets.add(wallet)
                if len(selected) >= capacity:
                    break

        return [
            {
                "wallet": row["wallet"],
                "seed_name": row.get("seed_name"),
                "best_context": {
                    "venue": row["venue"],
                    "lifecycle_stage": row["lifecycle_stage"],
                    "regime": row.get("regime"),
                    "role": row["role"],
                },
                "context_score": row["context_score"],
                "sample_count": row["sample_count"],
                "recommendation_has_tracking_authority": False,
            }
            for row in selected
        ]

    def accessibility_summary(self, observations: list[dict[str, Any]]) -> dict[str, Any]:
        classified = [classify_observation_accessibility(row) for row in observations]
        reason_counts: dict[str, int] = defaultdict(int)
        accessible = 0
        for row in classified:
            if row["structurally_accessible"]:
                accessible += 1
            for reason in row["reasons"]:
                reason_counts[str(reason)] += 1
        return {
            "observation_count": len(classified),
            "structurally_accessible_observations": accessible,
            "structurally_inaccessible_observations": len(classified) - accessible,
            "inaccessibility_reasons": dict(sorted(reason_counts.items())),
            "processing_target_seconds": PRODUCTION_PROCESSING_TARGET_SECONDS,
            "strategy_entry_ceiling_seconds": STRATEGY_ENTRY_CEILING_SECONDS,
            "max_chase_fraction": float(BASELINE.max_chase_fraction),
            "first_slot_or_subsecond_required_edge": "structurally_disqualified",
            "pump_fun_bonding_curve_removed_from_observation": False,
            "pump_fun_bonding_curve_execution_race_targeted": False,
            "pump_fun_role": "discovery_and_residual_continuation_research",
        }

    def status(self) -> dict[str, Any]:
        epoch_id, started_at = self._epoch()
        observations = self._observations(started_at)
        profiles = self.context_profiles()
        routes = self.route_map(profiles)
        copyable_profiles = [
            row
            for row in profiles
            if row["role"] == WalletRole.COPYABLE_ROC.value
            and self._eligible_profile(row)
        ]
        copyable_profiles.sort(
            key=lambda row: (
                float(row.get("copyable_return_on_deployed_fraction") or -999.0),
                float(row.get("context_score") or -999.0),
                int(row.get("sample_count") or 0),
            ),
            reverse=True,
        )
        return {
            "installed": True,
            "router_version": CONTEXT_ROUTER_VERSION,
            "strategy_version": FINAL_STRATEGY_VERSION,
            "release_commit": _release_commit(),
            "evidence_epoch_id": epoch_id,
            "epoch_started_at": started_at,
            "research_only": True,
            "assignment_key": "wallet_or_entity_x_venue_x_lifecycle_x_role_x_regime",
            "venues": list(VENUES),
            "lifecycle_stages": list(LIFECYCLE_STAGES),
            "context_profiles": profiles,
            "route_map": routes,
            "recommended_tracking_set": self.tracking_recommendations(profiles),
            "copyable_roi_leaders": [
                {
                    "wallet": row["wallet"],
                    "seed_name": row.get("seed_name"),
                    "venue": row["venue"],
                    "lifecycle_stage": row["lifecycle_stage"],
                    "regime": row.get("regime"),
                    "sample_count": row["sample_count"],
                    "copyable_return_on_deployed_fraction": row[
                        "copyable_return_on_deployed_fraction"
                    ],
                    "median_residual_roi": row["median_residual_roi"],
                    "trimmed_mean_residual_roi_ex_best_1": row[
                        "trimmed_mean_residual_roi_ex_best_1"
                    ],
                    "compounded_fraction_scaled_return": row[
                        "compounded_fraction_scaled_return"
                    ],
                }
                for row in copyable_profiles[:10]
            ],
            "roi_ranking_basis": "percentage_copyable_executable_residual_return_not_dollar_pnl",
            "source_wallet_headline_pnl_has_authority": False,
            "pre_observation_gain_has_authority": False,
            "scout_and_momentum_confirmation_semantics_are_separate": True,
            "wallet_context_authority_is_universal": False,
            "wallet_can_earn_multiple_context_roles_independently": True,
            "challenger_recommendations_are_prospective_only": True,
            "accessibility": self.accessibility_summary(observations),
            "millisecond_sniping_targeted": MILLISECOND_SNIPING_TARGETED,
            "first_slot_execution_authority": FIRST_SLOT_EXECUTION_AUTHORITY,
            "source_pre_observation_return_authority": SOURCE_PRE_OBSERVATION_RETURN_AUTHORITY,
            "context_min_forward_samples": CONTEXT_MIN_FORWARD_SAMPLES,
            "context_scores_have_trade_authority": False,
            "context_recommendations_have_tracking_mutation_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


def _status_with_context_router(self: WalletEntityUniverseV4) -> dict[str, Any]:
    if _ORIGINAL_UNIVERSE_STATUS is None:
        raise RuntimeError("wallet context router is not installed")
    payload = _ORIGINAL_UNIVERSE_STATUS(self)
    try:
        payload["wallet_context_router"] = WalletContextRouter(self).status()
    except Exception as exc:
        payload["wallet_context_router"] = {
            "installed": True,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: wallet context routing unavailable",
            "context_scores_have_trade_authority": False,
            "context_recommendations_have_tracking_mutation_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    return payload


def _manifest_with_context_router(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_FINAL_MANIFEST is None:
        raise RuntimeError("wallet context router manifest is not installed")
    payload = _ORIGINAL_FINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_context_router": CONTEXT_ROUTER_VERSION,
            "wallet_authority_model": "wallet_or_entity_x_venue_x_lifecycle_x_role_x_regime_prospective_forward_evidence",
            "universal_good_wallet_label_allowed": False,
            "scout_and_momentum_confirmations_are_interchangeable": False,
            "millisecond_pump_fun_sniping_targeted": False,
            "first_slot_execution_authority": False,
            "source_pre_observation_return_authority": False,
            "pump_fun_observation_retained": True,
            "pump_fun_target_role": "discovery_and_copyable_residual_continuation_not_execution_race",
            "roi_ranking_basis": "percentage_copyable_executable_residual_return_not_dollar_pnl",
            "context_signal_decay_buckets_seconds": list(LATENCY_BUCKETS_SECONDS),
            "context_scores_have_strategy_authority": False,
            "context_recommendations_have_tracking_mutation_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
        }
    )
    return payload


def install_wallet_context_router() -> None:
    global _ORIGINAL_UNIVERSE_STATUS, _ORIGINAL_FINAL_MANIFEST

    current_status = WalletEntityUniverseV4.status
    if not bool(getattr(current_status, "_roi_wallet_context_router", False)):
        _ORIGINAL_UNIVERSE_STATUS = current_status
        try:
            _status_with_context_router.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_context_router, "_roi_wallet_context_router", True)
        WalletEntityUniverseV4.status = _status_with_context_router  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_wallet_context_router", False)):
        _ORIGINAL_FINAL_MANIFEST = current_manifest
        try:
            _manifest_with_context_router.__dict__.update(getattr(current_manifest, "__dict__", {}))
        except Exception:
            pass
        setattr(_manifest_with_context_router, "_roi_wallet_context_router", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_context_router  # type: ignore[method-assign]


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "CONTEXT_MIN_FORWARD_SAMPLES",
    "CONTEXT_ROUTER_VERSION",
    "FIRST_SLOT_EXECUTION_AUTHORITY",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LATENCY_BUCKETS_SECONDS",
    "LIVE_MONEY_AUTHORITY",
    "MILLISECOND_SNIPING_TARGETED",
    "PAPER_ONLY",
    "PRODUCTION_PROCESSING_TARGET_SECONDS",
    "SIGNING_AVAILABLE",
    "SOURCE_PRE_OBSERVATION_RETURN_AUTHORITY",
    "STRATEGY_ENTRY_CEILING_SECONDS",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "WalletContextRouter",
    "classify_observation_accessibility",
    "install_wallet_context_router",
]
