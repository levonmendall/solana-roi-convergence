from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from statistics import median
from typing import Any, Callable, Iterable

from .profit_first_entity_final import FINAL_STRATEGY_VERSION, UNIFIED_LANE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import MIN_MATURE_FORWARD_SAMPLES, WalletEntityUniverseV4


PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
SEGMENT_OUTCOME_MATURITY = MIN_MATURE_FORWARD_SAMPLES
OBSERVATION_LIMIT = 5000

VENUES = ("PUMP_FUN", "PUMP_AMM", "RAYDIUM")
PUMP_BONDING_CURVE = "pump_bonding_curve"
PUMP_AMM_POST_BONDING = "pump_amm_post_bonding_curve"
RAYDIUM_POST_PUMP = "raydium_post_pump_migration_evidence"
RAYDIUM_UNPROVEN = "raydium_native_or_migration_unproven"
UNKNOWN_STAGE = "unknown_or_unsupported_venue"
LIFECYCLE_STAGES = (
    PUMP_BONDING_CURVE,
    PUMP_AMM_POST_BONDING,
    RAYDIUM_POST_PUMP,
    RAYDIUM_UNPROVEN,
    UNKNOWN_STAGE,
)

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


def venue_from_source(source: str | None) -> str | None:
    """Extract the canonical venue family without inventing a finer Raydium subtype."""
    raw = str(source or "").upper()
    parts = {part for part in raw.replace("/", ":").split(":") if part}
    for venue in VENUES:
        if venue in parts:
            return venue
    return None


def lifecycle_stage(venue: str | None, *, prior_pump_evidence: bool = False) -> str:
    """Classify only what current-release point-in-time observations actually prove."""
    if venue == "PUMP_FUN":
        return PUMP_BONDING_CURVE
    if venue == "PUMP_AMM":
        return PUMP_AMM_POST_BONDING
    if venue == "RAYDIUM":
        return RAYDIUM_POST_PUMP if prior_pump_evidence else RAYDIUM_UNPROVEN
    return UNKNOWN_STAGE


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _median(values: Iterable[Any]) -> float | None:
    clean = [value for raw in values if (value := _safe_float(raw)) is not None]
    return float(median(clean)) if clean else None


def _observation_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buys = [row for row in rows if str(row.get("side") or "").lower() == "buy"]
    copyable_buys = [row for row in buys if bool(row.get("copyable"))]
    risk_complete_buys = [row for row in buys if bool(row.get("risk_complete"))]
    return {
        "observation_count": len(rows),
        "buy_count": len(buys),
        "copyable_buy_count": len(copyable_buys),
        "copyability_rate": len(copyable_buys) / len(buys) if buys else None,
        "risk_complete_rate": len(risk_complete_buys) / len(buys) if buys else None,
        "median_observation_lag_ms": _median(row.get("observation_lag_ms") for row in buys),
        "median_processing_delay_ms": _median(row.get("processing_delay_ms") for row in buys),
        "median_chase_fraction": _median(row.get("chase_fraction") for row in buys),
    }


def _settled_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (str(row.get("exit_observed_at") or ""), str(row.get("source_signature") or "")),
    )
    deployed = 0.0
    weighted_pnl = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    scaled_returns: list[float] = []
    wins = 0
    for row in ordered:
        value = _safe_float(row.get("net_return"))
        fraction = _safe_float(row.get("position_fraction"))
        if value is None or fraction is None or fraction <= 0.0:
            continue
        contribution = fraction * value
        deployed += fraction
        weighted_pnl += contribution
        scaled_returns.append(contribution)
        if contribution > 0.0:
            gross_profit += contribution
            wins += 1
        elif contribution < 0.0:
            gross_loss += -contribution

    geometric_growth = None
    max_drawdown = None
    if scaled_returns:
        logs = [math.log(max(1e-9, 1.0 + value)) for value in scaled_returns]
        geometric_growth = math.exp(sum(logs) / len(logs)) - 1.0
        equity = 1.0
        peak = 1.0
        drawdown = 0.0
        for value in scaled_returns:
            equity *= max(1e-9, 1.0 + value)
            peak = max(peak, equity)
            if peak > 0.0:
                drawdown = max(drawdown, (peak - equity) / peak)
        max_drawdown = min(1.0, max(0.0, drawdown))

    return {
        "closed_outcomes": len(scaled_returns),
        "deployed_fraction_sum": deployed,
        "copyable_return_on_deployed_fraction": weighted_pnl / deployed if deployed > 0.0 else None,
        "geometric_growth_on_fraction_scaled_returns": geometric_growth,
        "profit_factor": (
            gross_profit / gross_loss if gross_loss > 0.0 else (999.0 if gross_profit > 0.0 else None)
        ),
        "hit_rate": wins / len(scaled_returns) if scaled_returns else None,
        "max_drawdown_on_fraction_scaled_returns": max_drawdown,
        "median_signal_to_entry_seconds": _median(row.get("signal_to_entry_seconds") for row in ordered),
        "median_realized_chase_fraction": _median(row.get("chase_fraction") for row in ordered),
        "mature_forward_segment": len(scaled_returns) >= SEGMENT_OUTCOME_MATURITY,
    }


class VenueLifecycleResearch:
    """Release-bound research view of copyable wallet/entity alpha by venue and lifecycle.

    This component is deliberately read-only. It cannot mutate tracking cohorts,
    strategy rules, paper positions, or any live-money authority. Raydium is called
    post-Pump only when an earlier current-release Pump.fun/Pump-AMM wallet observation
    for the same token exists before that Raydium observation.
    """

    def __init__(self, universe: WalletEntityUniverseV4):
        self.universe = universe
        self.store = universe.store

    def _epoch(self) -> tuple[str | None, str | None]:
        if not _table_exists(self.store, "profit_first_final_epochs"):
            return None, None
        epoch_id = self.universe._epoch_id()
        if not epoch_id:
            return None, None
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT started_at FROM profit_first_final_epochs WHERE epoch_id=? LIMIT 1", (epoch_id,)
            ).fetchone()
        return epoch_id, str(row["started_at"]) if row is not None else None

    def _observations(self, started_at: str | None) -> list[dict[str, Any]]:
        if not started_at or not _table_exists(self.store, "wallet_discovery_forward_observations"):
            return []
        processing = "processing_delay_ms" if _column_exists(
            self.store, "wallet_discovery_forward_observations", "processing_delay_ms"
        ) else "NULL AS processing_delay_ms"
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT id,signature,wallet,token_mint,side,received_at,source,copyable,risk_complete,"
                "observation_lag_ms,chase_fraction," + processing + " FROM wallet_discovery_forward_observations "
                "WHERE received_at>=? ORDER BY received_at,id LIMIT ?",
                (started_at, OBSERVATION_LIMIT),
            ).fetchall()
        result: list[dict[str, Any]] = []
        pump_seen_by_token: dict[str, bool] = defaultdict(bool)
        for raw in rows:
            row = dict(raw)
            venue = venue_from_source(str(row.get("source") or ""))
            token = str(row.get("token_mint") or "")
            row["venue"] = venue or "UNKNOWN"
            row["lifecycle_stage"] = lifecycle_stage(
                venue, prior_pump_evidence=bool(pump_seen_by_token.get(token, False))
            )
            result.append(row)
            if venue in {"PUMP_FUN", "PUMP_AMM"} and token:
                pump_seen_by_token[token] = True
        return result

    def _outcomes(self, epoch_id: str | None, observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not epoch_id:
            return []
        required = (
            "profit_first_final_outcomes",
            "profit_first_final_trials",
            "wallet_discovery_forward_observations",
        )
        if any(not _table_exists(self.store, table) for table in required):
            return []
        by_signature = {str(row["signature"]): row for row in observations}
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.source_signature,o.trigger_wallet,o.token_mint,o.position_fraction,o.net_return,"
                "o.signal_to_entry_seconds,o.exit_observed_at,t.opportunity_json "
                "FROM profit_first_final_outcomes o JOIN profit_first_final_trials t ON "
                "t.epoch_id=o.epoch_id AND t.source_signature=o.source_signature AND t.lane=o.lane "
                "WHERE o.epoch_id=? AND o.evidence_phase='forward' AND o.lane=? ORDER BY o.id",
                (epoch_id, UNIFIED_LANE),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            observation = by_signature.get(str(row.get("source_signature") or ""))
            if observation is None:
                # Old-release or pre-epoch observations cannot gain current segment authority.
                continue
            row["venue"] = observation["venue"]
            row["lifecycle_stage"] = observation["lifecycle_stage"]
            row["chase_fraction"] = observation.get("chase_fraction")
            try:
                opportunity = json.loads(str(row.get("opportunity_json") or "{}"))
            except Exception:
                opportunity = {}
            trigger_entity = str(opportunity.get("trigger_entity") or "")
            row["trigger_entity"] = trigger_entity or f"address:{row['trigger_wallet']}"
            result.append(row)
        return result

    @staticmethod
    def _segment_rows(
        observations: list[dict[str, Any]],
        outcomes: list[dict[str, Any]],
        *,
        key_name: str,
        outcome_key_name: str | None = None,
    ) -> list[dict[str, Any]]:
        outcome_key_name = outcome_key_name or key_name
        observation_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        outcome_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in observations:
            key = (str(row.get(key_name) or ""), str(row["venue"]), str(row["lifecycle_stage"]))
            if key[0]:
                observation_groups[key].append(row)
        for row in outcomes:
            key = (str(row.get(outcome_key_name) or ""), str(row["venue"]), str(row["lifecycle_stage"]))
            if key[0]:
                outcome_groups[key].append(row)
        keys = sorted(set(observation_groups) | set(outcome_groups))
        result: list[dict[str, Any]] = []
        for subject, venue, stage in keys:
            row = {
                key_name: subject,
                "venue": venue,
                "lifecycle_stage": stage,
                **_observation_metrics(observation_groups.get((subject, venue, stage), [])),
                **_settled_metrics(outcome_groups.get((subject, venue, stage), [])),
            }
            result.append(row)
        return result

    @staticmethod
    def _entity_segments(
        observations: list[dict[str, Any]], outcomes: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        # Entity identity for settled performance is read from the point-in-time trial.
        # Observation-only rows have no safe immutable entity snapshot here, so they
        # intentionally do not fabricate an entity segment before an outcome exists.
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in outcomes:
            key = (str(row.get("trigger_entity") or ""), str(row["venue"]), str(row["lifecycle_stage"]))
            if key[0]:
                groups[key].append(row)
        result = []
        for (entity, venue, stage), rows in sorted(groups.items()):
            result.append(
                {
                    "entity_id": entity,
                    "venue": venue,
                    "lifecycle_stage": stage,
                    **_settled_metrics(rows),
                }
            )
        return result

    @staticmethod
    def _venue_summary(
        observations: list[dict[str, Any]], outcomes: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        obs_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        out_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in observations:
            obs_groups[str(row["venue"])].append(row)
        for row in outcomes:
            out_groups[str(row["venue"])].append(row)
        venues = sorted(set(obs_groups) | set(out_groups))
        return [
            {
                "venue": venue,
                **_observation_metrics(obs_groups.get(venue, [])),
                **_settled_metrics(out_groups.get(venue, [])),
            }
            for venue in venues
        ]

    @staticmethod
    def _raydium_vs_pump(venue_summary: list[dict[str, Any]]) -> dict[str, Any]:
        by_venue = {str(row["venue"]): row for row in venue_summary}
        raydium_rows = [by_venue.get("RAYDIUM")] if by_venue.get("RAYDIUM") else []
        pump_rows = [by_venue[name] for name in ("PUMP_FUN", "PUMP_AMM") if name in by_venue]

        def combine(rows: list[dict[str, Any]]) -> dict[str, Any]:
            closed = sum(int(row.get("closed_outcomes") or 0) for row in rows)
            deployed = sum(float(row.get("deployed_fraction_sum") or 0.0) for row in rows)
            weighted = sum(
                float(row.get("copyable_return_on_deployed_fraction") or 0.0)
                * float(row.get("deployed_fraction_sum") or 0.0)
                for row in rows
            )
            return {
                "closed_outcomes": closed,
                "deployed_fraction_sum": deployed,
                "copyable_return_on_deployed_fraction": weighted / deployed if deployed > 0.0 else None,
            }

        raydium = combine(raydium_rows)
        pump = combine(pump_rows)
        conclusion = "insufficient_forward_evidence"
        if raydium["closed_outcomes"] >= SEGMENT_OUTCOME_MATURITY and pump["closed_outcomes"] >= SEGMENT_OUTCOME_MATURITY:
            left = raydium["copyable_return_on_deployed_fraction"]
            right = pump["copyable_return_on_deployed_fraction"]
            if left is not None and right is not None:
                conclusion = "raydium_higher_copyable_edge" if left > right else (
                    "pump_higher_copyable_edge" if right > left else "copyable_edge_tied"
                )
        return {
            "raydium": raydium,
            "pump_combined": pump,
            "research_conclusion": conclusion,
            "comparison_has_strategy_authority": False,
        }

    @staticmethod
    def _best_mature_by_wallet(wallet_segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in wallet_segments:
            if bool(row.get("mature_forward_segment")):
                grouped[str(row["wallet"])].append(row)
        result: list[dict[str, Any]] = []
        for wallet, rows in sorted(grouped.items()):
            eligible = [
                row for row in rows if row.get("copyable_return_on_deployed_fraction") is not None
            ]
            if not eligible:
                continue
            best = max(eligible, key=lambda row: float(row["copyable_return_on_deployed_fraction"]))
            result.append(
                {
                    "wallet": wallet,
                    "venue": best["venue"],
                    "lifecycle_stage": best["lifecycle_stage"],
                    "closed_outcomes": best["closed_outcomes"],
                    "copyable_return_on_deployed_fraction": best["copyable_return_on_deployed_fraction"],
                    "segment_selection_has_strategy_authority": False,
                }
            )
        return result

    def status(self) -> dict[str, Any]:
        epoch_id, started_at = self._epoch()
        observations = self._observations(started_at)
        outcomes = self._outcomes(epoch_id, observations)
        wallet_segments = self._segment_rows(observations, outcomes, key_name="wallet", outcome_key_name="trigger_wallet")
        entity_segments = self._entity_segments(observations, outcomes)
        venue_summary = self._venue_summary(observations, outcomes)
        return {
            "installed": True,
            "strategy_version": FINAL_STRATEGY_VERSION,
            "research_only": True,
            "release_commit": _release_commit(),
            "evidence_epoch_id": epoch_id,
            "epoch_started_at": started_at,
            "source_venues": list(VENUES),
            "lifecycle_stages": list(LIFECYCLE_STAGES),
            "lifecycle_proof_scope": "current_release_forward_wallet_observations_point_in_time",
            "unknown_or_unproven_raydium_is_not_labeled_post_migration": True,
            "observation_limit": OBSERVATION_LIMIT,
            "observation_rows_considered": len(observations),
            "unified_settled_outcome_rows_considered": len(outcomes),
            "experimental_lane_rows_do_not_multiply_segment_sample_counts": True,
            "wallet_segments": wallet_segments,
            "entity_segments": entity_segments,
            "venue_summary": venue_summary,
            "raydium_vs_pump": self._raydium_vs_pump(venue_summary),
            "best_mature_segment_by_wallet": self._best_mature_by_wallet(wallet_segments),
            "segment_min_forward_outcomes_for_maturity": SEGMENT_OUTCOME_MATURITY,
            "segment_scores_have_trade_authority": False,
            "strategy_thresholds_unchanged": True,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }


def _status_with_venue_lifecycle(self: WalletEntityUniverseV4) -> dict[str, Any]:
    if _ORIGINAL_UNIVERSE_STATUS is None:
        raise RuntimeError("wallet venue/lifecycle research is not installed")
    payload = _ORIGINAL_UNIVERSE_STATUS(self)
    try:
        payload["venue_lifecycle_research"] = VenueLifecycleResearch(self).status()
    except Exception as exc:
        payload["venue_lifecycle_research"] = {
            "installed": True,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: venue/lifecycle research unavailable",
            "segment_scores_have_trade_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    return payload


def _manifest_with_venue_lifecycle(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_FINAL_MANIFEST is None:
        raise RuntimeError("wallet venue/lifecycle manifest is not installed")
    payload = _ORIGINAL_FINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_venue_lifecycle_research": True,
            "wallet_venue_families": list(VENUES),
            "wallet_lifecycle_stage_research": list(LIFECYCLE_STAGES),
            "raydium_post_pump_requires_prior_current_release_pump_evidence": True,
            "venue_lifecycle_segment_scores_have_strategy_authority": False,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
        }
    )
    return payload


def install_wallet_venue_lifecycle_research() -> None:
    global _ORIGINAL_UNIVERSE_STATUS, _ORIGINAL_FINAL_MANIFEST

    current_status = WalletEntityUniverseV4.status
    if not bool(getattr(current_status, "_roi_wallet_venue_lifecycle_research", False)):
        _ORIGINAL_UNIVERSE_STATUS = current_status
        try:
            _status_with_venue_lifecycle.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_venue_lifecycle, "_roi_wallet_venue_lifecycle_research", True)
        WalletEntityUniverseV4.status = _status_with_venue_lifecycle  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_wallet_venue_lifecycle_research", False)):
        _ORIGINAL_FINAL_MANIFEST = current_manifest
        try:
            _manifest_with_venue_lifecycle.__dict__.update(getattr(current_manifest, "__dict__", {}))
        except Exception:
            pass
        setattr(_manifest_with_venue_lifecycle, "_roi_wallet_venue_lifecycle_research", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_venue_lifecycle  # type: ignore[method-assign]


__all__ = [
    "ACTIVE_STRATEGY_MUTATION_ALLOWED",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIFECYCLE_STAGES",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "PUMP_AMM_POST_BONDING",
    "PUMP_BONDING_CURVE",
    "RAYDIUM_POST_PUMP",
    "RAYDIUM_UNPROVEN",
    "SEGMENT_OUTCOME_MATURITY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "VenueLifecycleResearch",
    "install_wallet_venue_lifecycle_research",
    "lifecycle_stage",
    "venue_from_source",
]
