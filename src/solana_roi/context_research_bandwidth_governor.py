from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .config import BASELINE
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_context_governance import WalletContextGovernance
from .wallet_context_router import WalletContextRouter
from .wallet_entity_universe_v4 import _universe
from .wallet_venue_lifecycle_research import lifecycle_stage, venue_from_source


GOVERNOR_VERSION = "context-research-bandwidth-v1"
MATURE_NEGATIVE_EXPLORATION_FRACTION = 0.25
STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION = 0.10
GOVERNANCE_CACHE_SECONDS = 15.0

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
ACTIVE_TRACKING_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
MARKET_OBSERVATION_SCOPE_REDUCED = False
CANDIDATE_CERTIFICATION_THROTTLED = False

_ORIGINAL_SCHEDULE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None

_POSITIVE_ACTIONS = frozenset(
    {
        "promote_for_future_context_influence",
        "keep_for_future_context_influence",
    }
)
_NEGATIVE_ACTIONS = frozenset(
    {
        "demote_for_future_context_influence",
        "withhold_from_future_context_influence",
    }
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _deterministic_selected(signature: str, fraction: float) -> bool:
    fraction = max(0.0, min(1.0, float(fraction)))
    if fraction >= 1.0:
        return True
    if fraction <= 0.0:
        return False
    digest = hashlib.sha256(f"{GOVERNOR_VERSION}|{signature}".encode("utf-8")).digest()
    numerator = int.from_bytes(digest[:8], "big")
    return numerator / float(2**64) < fraction


def bandwidth_policy_for_context_actions(
    actions: list[str],
    *,
    side: str,
    candidate_certification: bool,
    observation_lag_ms: float | None,
    processing_delay_ms: float | None,
    chase_fraction: float | None,
) -> dict[str, Any]:
    """Return research-only sampling policy without granting strategy authority.

    Pre-V4 scheduling does not yet know the future lane role/regime for the current
    observation. Therefore a venue/lifecycle can be deprioritized only when every
    already-observed mature exact context at that wallet+venue+lifecycle is negative.
    Any positive, mixed, or insufficient context keeps full research bandwidth.
    """

    normalized_side = str(side or "").lower()
    if candidate_certification:
        return {
            "tier": "candidate_certification_exempt",
            "fraction": 1.0,
            "reason": "candidate_certification_path_is_never_throttled_by_research_governance",
        }
    if normalized_side == "sell":
        return {
            "tier": "exit_research_exempt",
            "fraction": 1.0,
            "reason": "sell_observations_remain_full_rate_to_preserve_exit_and_distribution_research",
        }

    lag = _safe_float(observation_lag_ms)
    processing = _safe_float(processing_delay_ms)
    chase = _safe_float(chase_fraction)
    pipeline_seconds = None
    if lag is not None and processing is not None:
        pipeline_seconds = max(0.0, lag + processing) / 1000.0
    if (
        (pipeline_seconds is not None and pipeline_seconds > 20.0)
        or (chase is not None and chase > float(BASELINE.max_chase_fraction))
    ):
        return {
            "tier": "structurally_inaccessible_diagnostic",
            "fraction": STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION,
            "reason": "outside_unchanged_entry_or_chase_boundary_diagnostic_sampling_only",
        }

    clean_actions = [str(value) for value in actions if str(value)]
    if not clean_actions:
        return {
            "tier": "bootstrap_full_rate",
            "fraction": 1.0,
            "reason": "no_mature_context_governance_yet",
        }
    if any(action in _POSITIVE_ACTIONS for action in clean_actions):
        return {
            "tier": "promising_full_rate",
            "fraction": 1.0,
            "reason": "positive_current_release_context_keeps_full_v4_research_bandwidth",
        }
    if any(action not in _NEGATIVE_ACTIONS for action in clean_actions):
        return {
            "tier": "mixed_or_unmatured_full_rate",
            "fraction": 1.0,
            "reason": "mixed_or_insufficient_exact_context_cannot_be_deprioritized",
        }
    return {
        "tier": "mature_negative_exploration",
        "fraction": MATURE_NEGATIVE_EXPLORATION_FRACTION,
        "reason": "all_observed_mature_exact_contexts_negative_keep_exploration_floor",
    }


def _table_exists(store: Any, name: str) -> bool:
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        ).fetchone()
    return row is not None


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS context_research_bandwidth_decisions ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
            "venue TEXT, lifecycle_stage TEXT, tier TEXT NOT NULL, sampling_fraction REAL NOT NULL, "
            "selected INTEGER NOT NULL, reason TEXT NOT NULL, source TEXT NOT NULL, decided_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_context_research_bandwidth_tier "
            "ON context_research_bandwidth_decisions(tier, selected, decided_at)"
        )


def _observation(adapter: FinalProfitFirstResearchAdapter, signature: str) -> dict[str, Any] | None:
    try:
        with adapter.store._lock:
            columns = {
                str(row["name"])
                for row in adapter.store.db.execute(
                    "PRAGMA table_info(wallet_discovery_forward_observations)"
                ).fetchall()
            }
            processing_expr = "processing_delay_ms" if "processing_delay_ms" in columns else "NULL"
            row = adapter.store.db.execute(
                "SELECT signature,wallet,token_mint,side,source,received_at,observation_lag_ms,"
                + processing_expr
                + " AS processing_delay_ms,chase_fraction "
                "FROM wallet_discovery_forward_observations WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        return dict(row) if row is not None else None
    except Exception:
        return None


def _prior_pump_evidence(adapter: FinalProfitFirstResearchAdapter, row: dict[str, Any]) -> bool:
    try:
        with adapter.store._lock:
            prior = adapter.store.db.execute(
                "SELECT 1 FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND received_at<? AND (source LIKE '%PUMP_FUN%' OR source LIKE '%PUMP_AMM%') "
                "LIMIT 1",
                (str(row.get("token_mint") or ""), str(row.get("received_at") or "")),
            ).fetchone()
        return prior is not None
    except Exception:
        return False


def _governance_rows(adapter: FinalProfitFirstResearchAdapter) -> list[dict[str, Any]]:
    now = time.monotonic()
    cached = getattr(adapter, "_roi_context_bandwidth_governance_cache", None)
    if isinstance(cached, tuple) and len(cached) == 2:
        cached_at, rows = cached
        try:
            if now - float(cached_at) <= GOVERNANCE_CACHE_SECONDS and isinstance(rows, list):
                return rows
        except (TypeError, ValueError):
            pass
    try:
        router = WalletContextRouter(_universe(adapter.discovery))
        payload = WalletContextGovernance(router).evaluate()
        rows = list(payload.get("all_context_recommendations") or [])
    except Exception:
        rows = []
    setattr(adapter, "_roi_context_bandwidth_governance_cache", (now, rows))
    return rows


def _matching_actions(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    wallet: str,
    venue: str | None,
    stage: str,
) -> list[str]:
    if not wallet or not venue:
        return []
    actions: list[str] = []
    for row in _governance_rows(adapter):
        if str(row.get("wallet") or "") != wallet:
            continue
        context = row.get("context")
        if not isinstance(context, dict):
            continue
        if str(context.get("venue") or "") != venue:
            continue
        if str(context.get("lifecycle_stage") or "") != stage:
            continue
        action = str(row.get("recommended_action") or "")
        if action:
            actions.append(action)
    return actions


def _record_decision(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    row: dict[str, Any] | None,
    signature: str,
    venue: str | None,
    stage: str,
    policy: dict[str, Any],
    selected: bool,
) -> None:
    try:
        _schema(adapter.store)
        source = str(row.get("source") or "") if row else "unknown"
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO context_research_bandwidth_decisions("
                "signature,wallet,token_mint,side,venue,lifecycle_stage,tier,sampling_fraction,selected,reason,"
                "source,decided_at,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signature,
                    str(row.get("wallet") or "") if row else "unknown",
                    str(row.get("token_mint") or "") if row else "unknown",
                    str(row.get("side") or "") if row else "unknown",
                    venue,
                    stage,
                    str(policy["tier"]),
                    float(policy["fraction"]),
                    1 if selected else 0,
                    str(policy["reason"]),
                    source,
                    _utcnow_iso(),
                    1,
                    0,
                ),
            )
    except Exception:
        pass


def _schedule_with_context_bandwidth(
    self: FinalProfitFirstResearchAdapter,
    signature: str,
) -> None:
    if _ORIGINAL_SCHEDULE is None:
        raise RuntimeError("context research bandwidth governor is not installed")
    signature = str(signature or "")
    if not signature:
        return

    row = _observation(self, signature)
    source = str(row.get("source") or "") if row else ""
    candidate_certification = source.startswith("direct-candidate-v4:")
    venue = venue_from_source(source) if row else None
    prior_pump = bool(row and venue == "RAYDIUM" and _prior_pump_evidence(self, row))
    stage = lifecycle_stage(venue, prior_pump_evidence=prior_pump)
    actions = _matching_actions(
        self,
        wallet=str(row.get("wallet") or "") if row else "",
        venue=venue,
        stage=stage,
    )
    policy = bandwidth_policy_for_context_actions(
        actions,
        side=str(row.get("side") or "") if row else "",
        candidate_certification=candidate_certification,
        observation_lag_ms=row.get("observation_lag_ms") if row else None,
        processing_delay_ms=row.get("processing_delay_ms") if row else None,
        chase_fraction=row.get("chase_fraction") if row else None,
    )
    selected = _deterministic_selected(signature, float(policy["fraction"]))
    _record_decision(
        self,
        row=row,
        signature=signature,
        venue=venue,
        stage=stage,
        policy=policy,
        selected=selected,
    )
    if selected:
        setattr(
            self,
            "_roi_context_bandwidth_selected_session",
            int(getattr(self, "_roi_context_bandwidth_selected_session", 0) or 0) + 1,
        )
        _ORIGINAL_SCHEDULE(self, signature)
    else:
        setattr(
            self,
            "_roi_context_bandwidth_deferred_session",
            int(getattr(self, "_roi_context_bandwidth_deferred_session", 0) or 0) + 1,
        )


setattr(_schedule_with_context_bandwidth, "_roi_context_research_bandwidth", True)


def _bandwidth_status(adapter: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    _schema(adapter.store)
    with adapter.store._lock:
        totals = adapter.store.db.execute(
            "SELECT COUNT(*) AS n, SUM(selected) AS selected FROM context_research_bandwidth_decisions"
        ).fetchone()
        tiers = adapter.store.db.execute(
            "SELECT tier,COUNT(*) AS n,SUM(selected) AS selected,AVG(sampling_fraction) AS fraction "
            "FROM context_research_bandwidth_decisions GROUP BY tier ORDER BY tier"
        ).fetchall()
    n = int(totals["n"] or 0) if totals is not None else 0
    selected = int(totals["selected"] or 0) if totals is not None else 0
    return {
        "installed": True,
        "version": GOVERNOR_VERSION,
        "decision_count": n,
        "selected_for_v4_research": selected,
        "deferred_from_v4_research": max(0, n - selected),
        "effective_selection_fraction": selected / n if n else None,
        "tiers": [
            {
                "tier": str(row["tier"]),
                "decision_count": int(row["n"] or 0),
                "selected_count": int(row["selected"] or 0),
                "configured_mean_fraction": float(row["fraction"] or 0.0),
            }
            for row in tiers
        ],
        "mature_negative_exploration_fraction": MATURE_NEGATIVE_EXPLORATION_FRACTION,
        "structurally_inaccessible_diagnostic_fraction": STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION,
        "positive_or_unmatured_contexts_full_rate": True,
        "sell_exit_research_full_rate": True,
        "candidate_certification_full_rate": True,
        "direct_market_observation_scope_reduced": False,
        "realtime_wallet_receipt_collection_reduced": False,
        "candidate_certification_throttled": False,
        "cross_context_success_transfer_allowed": False,
        "sampling_changes_strategy_authority": False,
        "active_tracking_mutation_allowed": False,
        "historical_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "selected_session": int(getattr(adapter, "_roi_context_bandwidth_selected_session", 0) or 0),
        "deferred_session": int(getattr(adapter, "_roi_context_bandwidth_deferred_session", 0) or 0),
    }


def _status_with_context_bandwidth(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("context research bandwidth status is not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        payload["research_bandwidth_governor"] = _bandwidth_status(self)
    except Exception as exc:
        payload["research_bandwidth_governor"] = {
            "installed": True,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: research bandwidth status unavailable",
            "direct_market_observation_scope_reduced": False,
            "candidate_certification_throttled": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


setattr(_status_with_context_bandwidth, "_roi_context_research_bandwidth", True)


def _manifest_with_context_bandwidth(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("context research bandwidth manifest is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "research_bandwidth_governor": GOVERNOR_VERSION,
            "research_bandwidth_assignment": "wallet_x_venue_x_lifecycle_requires_all_known_exact_role_regime_contexts_negative_before_deprioritization",
            "mature_negative_exploration_fraction": MATURE_NEGATIVE_EXPLORATION_FRACTION,
            "structurally_inaccessible_diagnostic_fraction": STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION,
            "positive_or_unmatured_contexts_full_rate": True,
            "sell_exit_research_full_rate": True,
            "candidate_certification_full_rate": True,
            "direct_market_observation_scope_reduced": False,
            "active_tracking_mutation_allowed": False,
            "active_strategy_mutation_allowed": False,
            "historical_evidence_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_context_research_bandwidth_governor() -> None:
    """Let forward context governance allocate expensive V4 research only.

    This installer does not alter direct Solana subscriptions, wallet receipt
    collection, candidate certification, tracking state, strategy thresholds, or
    execution authority. It only decides which wallet research observations consume
    final-V4 quote/simulation/five-lane task capacity.
    """

    global _ORIGINAL_SCHEDULE, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST

    current_schedule = FinalProfitFirstResearchAdapter.schedule
    if not bool(getattr(current_schedule, "_roi_context_research_bandwidth", False)):
        _ORIGINAL_SCHEDULE = current_schedule
        FinalProfitFirstResearchAdapter.schedule = _schedule_with_context_bandwidth  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_context_research_bandwidth", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_context_bandwidth.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        FinalProfitFirstResearchAdapter.status = _status_with_context_bandwidth  # type: ignore[method-assign]

    current_manifest = FinalProfitFirstResearchAdapter._manifest
    if not bool(getattr(current_manifest, "_roi_context_research_bandwidth", False)):
        _ORIGINAL_MANIFEST = current_manifest
        try:
            _manifest_with_context_bandwidth.__dict__.update(getattr(current_manifest, "__dict__", {}))
        except Exception:
            pass
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_context_bandwidth  # type: ignore[method-assign]


__all__ = [
    "GOVERNOR_VERSION",
    "MATURE_NEGATIVE_EXPLORATION_FRACTION",
    "STRUCTURALLY_INACCESSIBLE_DIAGNOSTIC_FRACTION",
    "bandwidth_policy_for_context_actions",
    "install_context_research_bandwidth_governor",
]
