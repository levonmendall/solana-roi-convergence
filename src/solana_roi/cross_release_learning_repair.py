from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from . import cross_regime_paper_allocator as cross_allocator
from . import fomo_paper_strategy as fomo_paper
from . import regime_roi_wallet_authority as regime_authority
from . import risk_conditioned_alpha_v5 as v5
from . import risk_conditioned_alpha_v51 as v51
from . import strategy_specialist_wallet_allocator as specialist_allocator
from . import continuation_market_recalibration as continuation
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_entity_universe_v4 import MIN_MATURE_FORWARD_SAMPLES, WalletEntityUniverseV4


REPAIR_VERSION = "cross-release-learning-compatibility-v1"
LEARNING_COMPATIBILITY_EPOCH = "continuation-v1-cross-release-20260905"
SOLANA_COMPATIBILITY_VERSION = v51.V51_VERSION
FOMO_FEATURE_COMPATIBILITY_VERSION = "fomo-continuation-context-v3"
FOMO_TRACKED_STRATEGY_VERSION = fomo_paper.FOMO_PAPER_STRATEGY_VERSION
FOMO_INDEPENDENT_STRATEGY_VERSION = continuation.RECALIBRATION_VERSION
ROBINHOOD_COMPATIBILITY_VERSION = v51.ROBINHOOD_V51_VERSION
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
HISTORICAL_PRE_EPOCH_PROMOTION_AUTHORITY = False

_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FOMO_PAPER_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ALLOCATOR_BUILD: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _ensure_epoch_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS strategy_learning_compatibility_releases ("
            "release_commit TEXT PRIMARY KEY, compatibility_epoch TEXT NOT NULL, "
            "registered_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_strategy_learning_compatibility_epoch "
            "ON strategy_learning_compatibility_releases(compatibility_epoch,release_commit)"
        )


def _register_release(store: Any, release_commit: str | None) -> None:
    release = str(release_commit or "").strip()
    if not release:
        return
    _ensure_epoch_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO strategy_learning_compatibility_releases("
            "release_commit,compatibility_epoch,registered_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,1,0)",
            (release, LEARNING_COMPATIBILITY_EPOCH, _utcnow()),
        )


def _compatible_release_count(store: Any) -> int:
    _ensure_epoch_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT COUNT(*) AS count FROM strategy_learning_compatibility_releases "
            "WHERE compatibility_epoch=?",
            (LEARNING_COMPATIBILITY_EPOCH,),
        ).fetchone()
    return int(row["count"] or 0) if row is not None else 0


def _dedup_rows(rows: list[Any], *fields: str) -> list[Any]:
    latest: dict[tuple[str, ...], Any] = {}
    for row in rows:
        key = tuple(str(row[field] if row[field] is not None else "") for field in fields)
        latest[key] = row
    return list(latest.values())


def _solana_context_returns_cross_release(
    adapter: Any,
    *,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    context_key: str,
) -> tuple[list[float], str]:
    _register_release(adapter.store, getattr(adapter, "release_commit", None))
    parsed = v51._parse_context_key(context_key)
    entity = parsed.get("entity")
    risk_signature = parsed.get("risk_signature") or "clean"
    with adapter.store._lock:
        exact_rows = adapter.store.db.execute(
            "SELECT o.id,o.source_signature,o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
            "WHERE m.compatibility_epoch=? AND o.strategy_version=? AND o.context_key=? ORDER BY o.id",
            (LEARNING_COMPATIBILITY_EPOCH, SOLANA_COMPATIBILITY_VERSION, context_key),
        ).fetchall()
        exact = _dedup_rows(list(exact_rows), "source_signature")
        if len(exact) >= 20:
            return [float(row["net_return"]) for row in exact], "exact_entity_context_cross_release_epoch"
        if entity:
            rows_raw = adapter.store.db.execute(
                "SELECT o.id,o.source_signature,o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                "WHERE m.compatibility_epoch=? AND o.strategy_version=? AND o.lane=? AND o.venue=? "
                "AND o.lifecycle=? AND o.regime=? AND o.risk_signature=? AND o.context_key LIKE ? ORDER BY o.id",
                (
                    LEARNING_COMPATIBILITY_EPOCH,
                    SOLANA_COMPATIBILITY_VERSION,
                    lane,
                    venue,
                    lifecycle,
                    regime,
                    risk_signature,
                    entity + "|%",
                ),
            ).fetchall()
            rows = _dedup_rows(list(rows_raw), "source_signature")
            if len(rows) >= v51.SOLANA_CONTEXT_MIN_SAMPLES:
                return [float(row["net_return"]) for row in rows], "same_entity_lane_venue_lifecycle_regime_risk_cross_release_epoch"
            relaxed_raw = adapter.store.db.execute(
                "SELECT o.id,o.source_signature,o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                "WHERE m.compatibility_epoch=? AND o.strategy_version=? AND o.lane=? AND o.venue=? "
                "AND o.lifecycle=? AND o.risk_signature=? AND o.context_key LIKE ? ORDER BY o.id",
                (
                    LEARNING_COMPATIBILITY_EPOCH,
                    SOLANA_COMPATIBILITY_VERSION,
                    lane,
                    venue,
                    lifecycle,
                    risk_signature,
                    entity + "|%",
                ),
            ).fetchall()
            relaxed = _dedup_rows(list(relaxed_raw), "source_signature")
            if len(relaxed) >= v51.SOLANA_RELAXED_SAME_ENTITY_SAMPLES:
                return [float(row["net_return"]) for row in relaxed], "same_entity_lane_venue_lifecycle_risk_cross_release_epoch"
    return (
        [float(row["net_return"]) for row in exact],
        "exact_entity_bootstrap_cross_release_epoch" if exact else "none",
    )


def _fomo_state_compatible(raw: Any) -> bool:
    payload = v5._safe_json(raw)
    return str(payload.get("feature_version") or "") == FOMO_FEATURE_COMPATIBILITY_VERSION


def _fomo_forward_rows_cross_release(adapter: Any) -> list[dict[str, Any]]:
    _register_release(adapter.store, getattr(adapter, "release_commit", None))
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT s.id,s.source_signature,s.venue,s.lifecycle,s.regime,s.state_json,"
            "t.trigger_wallet,o.net_return FROM fomo_shadow_observations s "
            "JOIN strategy_learning_compatibility_releases m ON m.release_commit=s.release_commit "
            "JOIN profit_first_final_trials t ON t.release_commit=s.release_commit "
            "AND t.source_signature=s.source_signature AND t.lane='unified_profit_maximizer' "
            "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit "
            "AND o.source_signature=s.source_signature "
            "WHERE m.compatibility_epoch=? ORDER BY s.id",
            (LEARNING_COMPATIBILITY_EPOCH,),
        ).fetchall()
    compatible = [row for row in rows if _fomo_state_compatible(row["state_json"])]
    return [dict(row) for row in _dedup_rows(compatible, "source_signature")]


def _fomo_context_returns_cross_release(
    adapter: Any,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
    hazard_signature: str,
) -> list[float]:
    values: list[float] = []
    for row in _fomo_forward_rows_cross_release(adapter):
        if str(row.get("trigger_wallet") or "") != wallet:
            continue
        if str(row.get("venue") or "") != venue:
            continue
        if str(row.get("lifecycle") or "") != lifecycle:
            continue
        if str(row.get("regime") or "") != regime:
            continue
        payload = v5._safe_json(row.get("state_json"))
        if str(payload.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        if v51.fomo_hazard_signature(payload) != hazard_signature:
            continue
        value = v5._finite(row.get("net_return"))
        if value is not None:
            values.append(value)
    return values


def _v5_specialist_rows_cross_release(universe: WalletEntityUniverseV4) -> list[dict[str, Any]]:
    store = universe.store
    if not (
        _table_exists(store, "risk_conditioned_alpha_v5_trials")
        and _table_exists(store, "risk_conditioned_alpha_v5_outcomes")
    ):
        return []
    _ensure_epoch_schema(store)
    grouped: dict[tuple[str, str, str, str, str, str, str], list[float]] = defaultdict(list)
    try:
        with store._lock:
            outcome_rows = store.db.execute(
                "SELECT t.id,t.source_signature,t.trigger_wallet,t.lane,t.venue,t.lifecycle,t.regime,t.trigger_role,"
                "t.risk_signature,t.risk_severity,o.net_return FROM risk_conditioned_alpha_v5_trials t "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=t.release_commit "
                "JOIN risk_conditioned_alpha_v5_outcomes o ON o.release_commit=t.release_commit "
                "AND o.source_signature=t.source_signature AND o.lane=t.lane "
                "WHERE m.compatibility_epoch=? AND t.strategy_version=? AND o.strategy_version=? "
                "AND t.decision LIKE 'paper_enter%' ORDER BY t.id",
                (LEARNING_COMPATIBILITY_EPOCH, SOLANA_COMPATIBILITY_VERSION, SOLANA_COMPATIBILITY_VERSION),
            ).fetchall()
            trial_rows_raw = store.db.execute(
                "SELECT t.id,t.source_signature,t.trigger_wallet,t.lane,t.venue,t.lifecycle,t.regime,t.trigger_role,"
                "t.risk_signature,t.risk_severity,t.decision FROM risk_conditioned_alpha_v5_trials t "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=t.release_commit "
                "WHERE m.compatibility_epoch=? AND t.strategy_version=? AND t.risk_signature<>'clean' ORDER BY t.id",
                (LEARNING_COMPATIBILITY_EPOCH, SOLANA_COMPATIBILITY_VERSION),
            ).fetchall()
    except Exception:
        return []

    outcomes = _dedup_rows(list(outcome_rows), "source_signature", "lane")
    trial_rows = _dedup_rows(list(trial_rows_raw), "source_signature", "lane")
    for row in outcomes:
        key = (
            str(row["trigger_wallet"]),
            str(row["lane"]),
            str(row["venue"]),
            str(row["lifecycle"]),
            str(row["regime"]),
            str(row["trigger_role"]),
            str(row["risk_signature"]),
        )
        value = specialist_allocator._safe_float(row["net_return"])
        if value is not None:
            grouped[key].append(value)

    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str, str]] = set()
    for key, values in grouped.items():
        profile = v5.robust_return_profile(values, min_samples=v5.MIN_FORWARD_SAMPLES)
        seen.add(key)
        results.append(
            {
                "wallet": key[0],
                "strategy_family": key[1],
                "venue": key[2],
                "lifecycle_stage": key[3],
                "regime": key[4],
                "role": key[5],
                "risk_signature": key[6],
                "risk_class": "clean" if key[6] == "clean" else "hazard",
                "source_kind": "risk_conditioned_alpha_v51_cross_release_epoch",
                "sample_count": profile.sample_count,
                "mature_forward_context": profile.sample_count >= v5.MIN_FORWARD_SAMPLES,
                "specialist_positive": profile.state == "promoted_positive_log_growth",
                "best_expected_log_growth": profile.best_expected_log_growth,
                "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
                "context_score": profile.best_expected_log_growth,
                "exploration_only": False,
            }
        )

    exploratory_counts: dict[tuple[str, str, str, str, str, str, str], tuple[int, float]] = {}
    for row in trial_rows:
        decision = str(row["decision"] or "")
        if decision.startswith(("reject_mechanical", "reject_latency", "reject_execution")):
            continue
        key = (
            str(row["trigger_wallet"]),
            str(row["lane"]),
            str(row["venue"]),
            str(row["lifecycle"]),
            str(row["regime"]),
            str(row["trigger_role"]),
            str(row["risk_signature"]),
        )
        if key in seen:
            continue
        count, max_severity = exploratory_counts.get(key, (0, 0.0))
        severity = specialist_allocator._safe_float(row["risk_severity"]) or 0.0
        exploratory_counts[key] = (count + 1, max(max_severity, severity))

    for key, (count, severity) in exploratory_counts.items():
        results.append(
            {
                "wallet": key[0],
                "strategy_family": key[1],
                "venue": key[2],
                "lifecycle_stage": key[3],
                "regime": key[4],
                "role": key[5],
                "risk_signature": key[6],
                "risk_class": "hazard",
                "source_kind": "risk_conditioned_alpha_v51_cross_release_epoch",
                "sample_count": count,
                "mature_forward_context": False,
                "specialist_positive": False,
                "context_score": severity,
                "exploration_only": True,
            }
        )
    return results


def _fomo_specialist_rows_cross_release(universe: WalletEntityUniverseV4) -> list[dict[str, Any]]:
    rows = _fomo_forward_rows_cross_release(universe)
    grouped: dict[tuple[str, str, str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        payload = v5._safe_json(row.get("state_json"))
        if str(payload.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        risk_signature = v51.fomo_hazard_signature(payload)
        risk_class = "clean_fomo" if risk_signature == "clean" else "hazard_fomo"
        wallet = str(row.get("trigger_wallet") or "")
        value = specialist_allocator._safe_float(row.get("net_return"))
        if not wallet or value is None:
            continue
        grouped[
            (
                wallet,
                str(row.get("venue") or "UNKNOWN"),
                str(row.get("lifecycle") or "unknown"),
                str(row.get("regime") or "unknown"),
                risk_class,
            )
        ].append(value)

    results: list[dict[str, Any]] = []
    for key, values in grouped.items():
        profile = v5.robust_return_profile(
            values,
            grid=v5.FOMO_ACTIVE_GRID,
            max_fraction=0.05,
            min_samples=MIN_MATURE_FORWARD_SAMPLES,
        )
        results.append(
            {
                "wallet": key[0],
                "strategy_family": "fomo_continuation",
                "venue": key[1],
                "lifecycle_stage": key[2],
                "regime": key[3],
                "role": "fomo_trigger",
                "risk_signature": key[4],
                "risk_class": key[4],
                "source_kind": "fomo_v51_cross_release_epoch",
                "sample_count": profile.sample_count,
                "mature_forward_context": profile.sample_count >= MIN_MATURE_FORWARD_SAMPLES,
                "specialist_positive": profile.state == "promoted_positive_log_growth",
                "best_expected_log_growth": profile.best_expected_log_growth,
                "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
                "context_score": profile.best_expected_log_growth,
                "exploration_only": profile.sample_count < MIN_MATURE_FORWARD_SAMPLES,
            }
        )
    return results


def _segment_returns_cross_release(
    store: Any,
    release_commit: str,
) -> tuple[dict[str, list[float]], dict[str, dict[str, str]]]:
    _register_release(store, release_commit)
    grouped: dict[str, list[float]] = {}
    metadata: dict[str, dict[str, str]] = {}

    if _table_exists(store, "risk_conditioned_alpha_v5_outcomes"):
        with store._lock:
            rows_raw = store.db.execute(
                "SELECT o.id,o.source_signature,o.lane,o.venue,o.lifecycle,o.regime,o.risk_signature,o.net_return "
                "FROM risk_conditioned_alpha_v5_outcomes o "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                "WHERE m.compatibility_epoch=? AND o.strategy_version=? ORDER BY o.id",
                (LEARNING_COMPATIBILITY_EPOCH, SOLANA_COMPATIBILITY_VERSION),
            ).fetchall()
        for row in _dedup_rows(list(rows_raw), "source_signature", "lane"):
            cross_allocator._append(
                grouped,
                metadata,
                surface="SOLANA_ALPHA",
                lane=str(row["lane"] or "unknown"),
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=str(row["risk_signature"] or "clean"),
                net_return=float(row["net_return"]),
            )

    if _table_exists(store, "fomo_paper_outcomes"):
        shadow_exists = _table_exists(store, "fomo_shadow_observations")
        with store._lock:
            if shadow_exists:
                rows_raw = store.db.execute(
                    "SELECT o.id,o.strategy_version,o.source_signature,o.venue,o.lifecycle,o.regime,o.net_return,s.state_json "
                    "FROM fomo_paper_outcomes o "
                    "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                    "LEFT JOIN fomo_shadow_observations s ON s.release_commit=o.release_commit "
                    "AND s.source_signature=o.source_signature "
                    "WHERE m.compatibility_epoch=? AND o.strategy_version IN (?,?) ORDER BY o.id",
                    (
                        LEARNING_COMPATIBILITY_EPOCH,
                        FOMO_TRACKED_STRATEGY_VERSION,
                        FOMO_INDEPENDENT_STRATEGY_VERSION,
                    ),
                ).fetchall()
            else:
                rows_raw = store.db.execute(
                    "SELECT o.id,o.strategy_version,o.source_signature,o.venue,o.lifecycle,o.regime,o.net_return,NULL AS state_json "
                    "FROM fomo_paper_outcomes o "
                    "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                    "WHERE m.compatibility_epoch=? AND o.strategy_version IN (?,?) ORDER BY o.id",
                    (
                        LEARNING_COMPATIBILITY_EPOCH,
                        FOMO_TRACKED_STRATEGY_VERSION,
                        FOMO_INDEPENDENT_STRATEGY_VERSION,
                    ),
                ).fetchall()
        for row in _dedup_rows(list(rows_raw), "strategy_version", "source_signature"):
            tracked = str(row["strategy_version"] or "") == FOMO_TRACKED_STRATEGY_VERSION
            risk_signature = (
                cross_allocator._fomo_risk_signature(row["state_json"])
                if tracked and row["state_json"] is not None
                else "independent_market_flow"
            )
            cross_allocator._append(
                grouped,
                metadata,
                surface="FOMO",
                lane="fomo_continuation" if tracked else "independent_fomo_continuation",
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=risk_signature,
                net_return=float(row["net_return"]),
            )

    if (
        _table_exists(store, "robinhood_paper_outcomes")
        and _table_exists(store, "robinhood_v5_trial_context")
        and _table_exists(store, "robinhood_paper_trials")
    ):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,c.lane,t.venue,t.lifecycle,c.regime,c.risk_signature,o.net_return "
                "FROM robinhood_paper_outcomes o "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN strategy_learning_compatibility_releases m ON m.release_commit=o.release_commit "
                "WHERE m.compatibility_epoch=? AND t.strategy_version=? ORDER BY o.id",
                (LEARNING_COMPATIBILITY_EPOCH, ROBINHOOD_COMPATIBILITY_VERSION),
            ).fetchall()
        for row in rows:
            cross_allocator._append(
                grouped,
                metadata,
                surface="ROBINHOOD_CHAIN",
                lane=str(row["lane"] or "unknown"),
                venue=str(row["venue"] or "UNKNOWN"),
                lifecycle=str(row["lifecycle"] or "unknown"),
                regime=str(row["regime"] or "unknown"),
                risk_signature=str(row["risk_signature"] or "clean"),
                net_return=float(row["net_return"]),
            )

    return grouped, metadata


def _build_allocator_cross_release(store: Any, release_commit: str) -> dict[str, Any]:
    if _ORIGINAL_ALLOCATOR_BUILD is None:
        raise RuntimeError("cross-release allocator repair is not installed")
    result = dict(_ORIGINAL_ALLOCATOR_BUILD(store, release_commit))
    result.update(
        {
            "learning_compatibility_epoch": LEARNING_COMPATIBILITY_EPOCH,
            "release_commit_role": "audit_lineage_not_statistical_partition_within_epoch",
            "compatible_cross_release_learning": True,
            "historical_pre_epoch_promotion_authority": False,
            "fomo_tracked_and_independent_evidence_pooled": False,
        }
    )
    return result


def _fomo_paper_status_cross_release(adapter: Any) -> dict[str, Any]:
    if _ORIGINAL_FOMO_PAPER_STATUS is None:
        raise RuntimeError("cross-release FOMO status repair is not installed")
    result = dict(_ORIGINAL_FOMO_PAPER_STATUS(adapter))
    result.update(
        {
            "wallet_promotion_evidence": "compatible_learning_epoch_cross_release_forward_fomo_outcomes",
            "learning_compatibility_epoch": LEARNING_COMPATIBILITY_EPOCH,
            "release_commit_role": "audit_lineage_not_statistical_partition_within_epoch",
            "historical_pre_epoch_promotion_authority": False,
            "bootstrap_paper_probe_is_exploration": True,
            "promoted_paper_allocation_is_distinct_from_exploration": True,
            "independent_market_flow_fomo_is_separate_exploration_lane": True,
        }
    )
    return result


def _decision_counts(store: Any, release_commit: str) -> dict[str, int]:
    if not _table_exists(store, "fomo_paper_trials"):
        return {
            "tracked_promoted_entries": 0,
            "tracked_exploration_entries": 0,
            "independent_market_flow_exploration_entries": 0,
        }
    with store._lock:
        rows = store.db.execute(
            "SELECT strategy_version,decision,decision_reason,COUNT(*) AS count FROM fomo_paper_trials "
            "WHERE release_commit=? GROUP BY strategy_version,decision,decision_reason",
            (release_commit,),
        ).fetchall()
    result = {
        "tracked_promoted_entries": 0,
        "tracked_exploration_entries": 0,
        "independent_market_flow_exploration_entries": 0,
    }
    for row in rows:
        count = int(row["count"] or 0)
        decision = str(row["decision"] or "")
        reason = str(row["decision_reason"] or "")
        version = str(row["strategy_version"] or "")
        if not decision.startswith("paper_enter"):
            continue
        if version == FOMO_INDEPENDENT_STRATEGY_VERSION or reason.startswith("independent_market_flow"):
            result["independent_market_flow_exploration_entries"] += count
        elif "promoted" in decision:
            result["tracked_promoted_entries"] += count
        else:
            result["tracked_exploration_entries"] += count
    return result


def _status_with_cross_release_learning(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("cross-release learning status wrapper is not installed")
    payload = _ORIGINAL_STATUS(self)
    _register_release(self.store, getattr(self, "release_commit", None))
    release = str(getattr(self, "release_commit", "") or "")
    payload["cross_release_strategy_learning"] = {
        "repair_version": REPAIR_VERSION,
        "learning_compatibility_epoch": LEARNING_COMPATIBILITY_EPOCH,
        "current_release_commit": release,
        "release_commit_role": "audit_lineage_not_statistical_partition_within_epoch",
        "compatible_release_count": _compatible_release_count(self.store),
        "historical_pre_epoch_promotion_authority": False,
        "pre_epoch_rows_preserved_for_audit": True,
        "same_economic_signal_cross_release_deduplicated": True,
        "solana_pump_raydium_context_learning_cross_release": True,
        "fomo_context_learning_cross_release": True,
        "specialist_wallet_ranking_cross_release": True,
        "cross_regime_allocator_cross_release": True,
        "fomo_tracked_and_independent_evidence_pooled": False,
        "paper_authority_modes": {
            "solana_pump_raydium": {
                "exploration": "bounded paper continuation probes for unproven non-demoted contexts",
                "promotion": "mature robust positive expected-log-growth context sizing",
            },
            "fomo_tracked_wallet": {
                "exploration": "bounded clean_or_hazard paper probes",
                "promotion": "mature robust positive forward FOMO context sizing",
            },
            "fomo_independent_market_flow": {
                "exploration": "separate bounded market-flow paper probe lane",
                "promotion": "not pooled with tracked-wallet FOMO evidence",
            },
            "robinhood_chain": {
                "exploration": "zero-allocation contextual shadow outcomes",
                "promotion": ">=30 positive-geometric-edge forward outcomes before paper allocation",
            },
        },
        "current_release_fomo_entry_modes": _decision_counts(self.store, release),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def _manifest_with_cross_release_learning(self: Any) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("cross-release learning manifest wrapper is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "strategy_learning_compatibility_epoch": LEARNING_COMPATIBILITY_EPOCH,
            "strategy_learning_release_commit_role": "audit_lineage_not_statistical_partition_within_epoch",
            "strategy_learning_cross_release_enabled": True,
            "strategy_learning_same_signal_cross_release_deduplicated": True,
            "strategy_learning_historical_pre_epoch_promotion_authority": False,
            "fomo_wallet_promotion_evidence": "compatible_learning_epoch_cross_release_forward_fomo_outcomes",
            "fomo_exploration_and_promotion_explicitly_separated": True,
            "fomo_independent_market_flow_evidence_pooled_with_tracked_wallet_fomo": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_cross_release_learning_repair() -> None:
    global _INSTALLED, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST
    global _ORIGINAL_FOMO_PAPER_STATUS, _ORIGINAL_ALLOCATOR_BUILD
    if _INSTALLED:
        return
    if not bool(getattr(v51, "_INSTALLED", False)):
        raise RuntimeError("cross-release learning repair requires v5.1 composition")

    # v5's chooser resolves this function by module global. Rebind both surfaces so
    # the active v5.1 chooser and direct v5.1 callers use the same compatibility epoch.
    v51._context_returns_v51 = _solana_context_returns_cross_release
    v5._context_returns = _solana_context_returns_cross_release
    v51._fomo_context_returns_v51 = _fomo_context_returns_cross_release

    # Keep FOMO cohort/status and strategy-specialist wallet ranking on the same
    # cross-release evidence population used by paper decisions.
    fomo_paper._forward_fomo_rows = _fomo_forward_rows_cross_release
    _ORIGINAL_FOMO_PAPER_STATUS = fomo_paper._paper_status
    fomo_paper._paper_status = _fomo_paper_status_cross_release
    specialist_allocator._v5_specialist_rows = _v5_specialist_rows_cross_release
    specialist_allocator._fomo_specialist_rows = _fomo_specialist_rows_cross_release

    # Allocation remains exact lifecycle/regime/risk segmented. Only the release
    # partition changes, and tracked-wallet FOMO stays separate from independent FOMO.
    cross_allocator._segment_returns = _segment_returns_cross_release
    _ORIGINAL_ALLOCATOR_BUILD = cross_allocator.build_cross_regime_allocation
    cross_allocator.build_cross_regime_allocation = _build_allocator_cross_release

    current_status = FinalProfitFirstResearchAdapter.status
    _ORIGINAL_STATUS = current_status
    wrapped_status = wraps(current_status)(_status_with_cross_release_learning)
    try:
        wrapped_status.__dict__.update(getattr(current_status, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_status, "_roi_cross_release_learning_repair", True)
    FinalProfitFirstResearchAdapter.status = wrapped_status  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    _ORIGINAL_MANIFEST = current_manifest
    wrapped_manifest = wraps(current_manifest)(_manifest_with_cross_release_learning)
    try:
        wrapped_manifest.__dict__.update(getattr(current_manifest, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_manifest, "_roi_cross_release_learning_repair", True)
    FinalProfitFirstResearchAdapter._manifest = wrapped_manifest  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "LEARNING_COMPATIBILITY_EPOCH",
    "SOLANA_COMPATIBILITY_VERSION",
    "FOMO_FEATURE_COMPATIBILITY_VERSION",
    "install_cross_release_learning_repair",
]
