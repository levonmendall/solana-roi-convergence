from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import candidate_risk_quote_v4_handoff as handoff
from . import context_research_bandwidth_governor as bandwidth
from .direct_solana import DirectSolanaIngestionPlane
from .observation import TimedRiskCollectors
from .wallet_venue_lifecycle_research import lifecycle_stage, venue_from_source


ADMISSION_VERSION = "candidate-compute-admission-v1"
ENTRY_WINDOW_SECONDS = handoff.ENTRY_WINDOW_SECONDS
MAX_CHASE_FRACTION = handoff.MAX_CHASE_FRACTION
MATURE_NEGATIVE_EXPLORATION_FRACTION = bandwidth.MATURE_NEGATIVE_EXPLORATION_FRACTION

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
FULL_MARKET_OBSERVATION_REDUCED = False
CONTINUITY_SCOPE_REDUCED = False
CERTIFICATION_THRESHOLDS_CHANGED = False

_ORIGINAL_PREFILL: Callable[..., Any] | None = None
_ORIGINAL_REFRESH: Callable[..., Any] | None = None
_ORIGINAL_PROCESS: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_compute_admission_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS candidate_compute_admission_decisions ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
            "venue TEXT, candidate_slot INTEGER, launch_slot INTEGER, observed_age_ms REAL NOT NULL, "
            "tier TEXT NOT NULL, sampling_fraction REAL NOT NULL, selected INTEGER NOT NULL, reason TEXT NOT NULL, "
            "decided_at TEXT NOT NULL, certification_evidence_retained INTEGER NOT NULL, "
            "full_market_observation_reduced INTEGER NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_candidate_compute_admission_tier "
            "ON candidate_compute_admission_decisions(tier, selected, decided_at)"
        )


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _venue(source: str) -> str | None:
    raw = str(source or "").upper()
    for venue in ("PUMP_FUN", "PUMP_AMM", "RAYDIUM"):
        if venue in raw:
            return venue
    try:
        return venue_from_source(raw)
    except Exception:
        return None


def _observed_age_ms(candidate: Any, *, now: datetime) -> float:
    observed = getattr(candidate, "observed_at", None)
    received = getattr(candidate, "received_at", None)
    values: list[float] = []
    if isinstance(observed, datetime):
        values.append(max(0.0, (now - observed).total_seconds() * 1000.0))
    if isinstance(observed, datetime) and isinstance(received, datetime):
        values.append(max(0.0, (received - observed).total_seconds() * 1000.0))
    try:
        values.append(max(0.0, float(getattr(candidate, "ingestion_latency_ms"))))
    except (TypeError, ValueError, AttributeError):
        pass
    return max(values) if values else 0.0


def _launch_slot(store: Any, token_mint: str) -> int | None:
    if not token_mint or not _table_exists(store, "launch_near_creation_diagnostics"):
        return None
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT launch_slot FROM launch_near_creation_diagnostics "
                "WHERE token_mint=? AND launch_slot IS NOT NULL ORDER BY id DESC LIMIT 1",
                (token_mint,),
            ).fetchone()
        if row is None:
            return None
        value = int(row["launch_slot"])
        return value if value > 0 else None
    except Exception:
        return None


def _prior_pump_evidence(store: Any, token_mint: str, received_at: datetime | None) -> bool:
    if not token_mint or received_at is None or not _table_exists(store, "wallet_discovery_forward_observations"):
        return False
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND received_at<? "
                "AND (source LIKE '%PUMP_FUN%' OR source LIKE '%PUMP_AMM%') LIMIT 1",
                (token_mint, received_at.isoformat()),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _context_actions(obj: Any, candidate: Any, venue: str | None) -> list[str]:
    if not venue:
        return []
    discovery = handoff._attached_discovery(obj)
    if discovery is None:
        return []
    try:
        from .profit_first_entity_final_research import _adapter

        adapter = _adapter(discovery)
        token_mint = str(getattr(candidate, "token_mint", "") or "")
        received_at = getattr(candidate, "received_at", None)
        prior_pump = bool(
            venue == "RAYDIUM"
            and _prior_pump_evidence(
                obj.store,
                token_mint,
                received_at if isinstance(received_at, datetime) else None,
            )
        )
        stage = lifecycle_stage(venue, prior_pump_evidence=prior_pump)
        return bandwidth._matching_actions(
            adapter,
            wallet=str(getattr(candidate, "wallet", "") or ""),
            venue=venue,
            stage=stage,
        )
    except Exception:
        return []


def candidate_compute_policy(
    obj: Any,
    candidate: Any,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify expensive strategy compute using only already-hydrated/local evidence.

    This gate never narrows raw market observation or continuity. Missing proof does
    not create a rejection: uncertain candidates continue to the existing fail-closed
    risk/quote path so this optimization cannot manufacture a cleaner cohort.
    """

    assessed_at = now or _utcnow()
    signature = str(getattr(candidate, "signature", "") or "")
    side = str(getattr(candidate, "side", "") or "").lower()
    token_mint = str(getattr(candidate, "token_mint", "") or "")
    venue = _venue(str(getattr(candidate, "source", "") or ""))
    age_ms = _observed_age_ms(candidate, now=assessed_at)
    try:
        candidate_slot = int(getattr(candidate, "slot", 0) or 0)
    except (TypeError, ValueError):
        candidate_slot = 0
    launch_slot = _launch_slot(obj.store, token_mint) if venue == "PUMP_FUN" else None

    if side != "buy":
        return {
            "tier": "non_buy_unchanged",
            "fraction": 1.0,
            "selected": True,
            "reason": "non_buy_paths_keep_existing_exit_or_research_behavior",
            "venue": venue,
            "candidate_slot": candidate_slot or None,
            "launch_slot": launch_slot,
            "observed_age_ms": age_ms,
        }

    if age_ms > ENTRY_WINDOW_SECONDS * 1000.0:
        return {
            "tier": "outside_entry_window_research_only",
            "fraction": 0.0,
            "selected": False,
            "reason": "already_outside_unchanged_20_second_entry_window",
            "venue": venue,
            "candidate_slot": candidate_slot or None,
            "launch_slot": launch_slot,
            "observed_age_ms": age_ms,
        }

    if (
        venue == "PUMP_FUN"
        and launch_slot is not None
        and candidate_slot > 0
        and candidate_slot <= launch_slot
    ):
        return {
            "tier": "pump_first_slot_research_only",
            "fraction": 0.0,
            "selected": False,
            "reason": "first_slot_pump_fun_sniping_is_outside_target_execution_capability",
            "venue": venue,
            "candidate_slot": candidate_slot,
            "launch_slot": launch_slot,
            "observed_age_ms": age_ms,
        }

    actions = _context_actions(obj, candidate, venue)
    clean_actions = [str(action) for action in actions if str(action)]
    if clean_actions and all(action in bandwidth._NEGATIVE_ACTIONS for action in clean_actions):
        fraction = MATURE_NEGATIVE_EXPLORATION_FRACTION
        selected = bandwidth._deterministic_selected(signature, fraction)
        return {
            "tier": "mature_negative_context_exploration",
            "fraction": fraction,
            "selected": selected,
            "reason": "all_known_mature_exact_contexts_negative_keep_existing_exploration_floor",
            "venue": venue,
            "candidate_slot": candidate_slot or None,
            "launch_slot": launch_slot,
            "observed_age_ms": age_ms,
        }

    return {
        "tier": "actionable_or_unresolved_full_rate",
        "fraction": 1.0,
        "selected": True,
        "reason": "no_safe_pre_risk_rejection_proven",
        "venue": venue,
        "candidate_slot": candidate_slot or None,
        "launch_slot": launch_slot,
        "observed_age_ms": age_ms,
    }


def _decision_from_store(store: Any, signature: str) -> dict[str, Any] | None:
    try:
        _schema(store)
        with store._lock:
            row = store.db.execute(
                "SELECT signature,wallet,token_mint,side,venue,candidate_slot,launch_slot,observed_age_ms,"
                "tier,sampling_fraction,selected,reason,decided_at,certification_evidence_retained "
                "FROM candidate_compute_admission_decisions WHERE signature=? LIMIT 1",
                (signature,),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["selected"] = bool(result.get("selected"))
        return result
    except Exception:
        return None


def _persist_decision(obj: Any, candidate: Any, policy: dict[str, Any]) -> dict[str, Any]:
    signature = str(getattr(candidate, "signature", "") or "")
    existing = _decision_from_store(obj.store, signature)
    if existing is not None:
        return existing
    _schema(obj.store)
    decided_at = _utcnow().isoformat()
    with obj.store._lock, obj.store.db:
        obj.store.db.execute(
            "INSERT OR IGNORE INTO candidate_compute_admission_decisions("
            "signature,wallet,token_mint,side,venue,candidate_slot,launch_slot,observed_age_ms,tier,"
            "sampling_fraction,selected,reason,decided_at,certification_evidence_retained,"
            "full_market_observation_reduced,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                signature,
                str(getattr(candidate, "wallet", "") or ""),
                str(getattr(candidate, "token_mint", "") or ""),
                str(getattr(candidate, "side", "") or ""),
                policy.get("venue"),
                policy.get("candidate_slot"),
                policy.get("launch_slot"),
                float(policy.get("observed_age_ms") or 0.0),
                str(policy["tier"]),
                float(policy["fraction"]),
                1 if bool(policy["selected"]) else 0,
                str(policy["reason"]),
                decided_at,
                1,
                0,
                1,
                0,
            ),
        )
    result = _decision_from_store(obj.store, signature) or dict(policy)
    try:
        obj.store.append(
            "candidate_compute_admission",
            decided_at,
            {
                "signature": signature,
                "token_mint": str(getattr(candidate, "token_mint", "") or ""),
                "wallet": str(getattr(candidate, "wallet", "") or ""),
                "tier": str(policy["tier"]),
                "sampling_fraction": float(policy["fraction"]),
                "selected_for_expensive_strategy_compute": bool(policy["selected"]),
                "reason": str(policy["reason"]),
                "certification_evidence_retained": True,
                "full_market_observation_reduced": False,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            },
        )
    except Exception:
        pass
    return result


def _ensure_decision(obj: Any, candidate: Any) -> dict[str, Any]:
    signature = str(getattr(candidate, "signature", "") or "")
    existing = _decision_from_store(obj.store, signature) if signature else None
    if existing is not None:
        return existing
    return _persist_decision(obj, candidate, candidate_compute_policy(obj, candidate))


async def _prefill_with_compute_admission(self: Any, candidate: Any) -> bool:
    if _ORIGINAL_PREFILL is None:
        raise RuntimeError("candidate compute admission is not installed")
    try:
        eligible = candidate_hotpath._is_frozen_scout_buy(self, candidate)
    except Exception:
        eligible = False
    if not eligible:
        return bool(await _ORIGINAL_PREFILL(self, candidate))

    decision = _ensure_decision(self, candidate)
    if bool(decision.get("selected")):
        _inc(self, "prefill_admitted")
        return bool(await _ORIGINAL_PREFILL(self, candidate))

    _inc(self, "prefill_deferred")
    return False


setattr(_prefill_with_compute_admission, "_roi_candidate_compute_admission", True)


def _record_skipped_risk_refresh(self: Any, current_swap: Any, decision: dict[str, Any]) -> None:
    now = self.now_fn()
    trigger_observed_at = getattr(current_swap, "observed_at", now)
    trigger_received_at = getattr(current_swap, "received_at", now)
    try:
        ingestion_latency_ms = float(getattr(current_swap, "ingestion_latency_ms", 0.0) or 0.0)
    except (TypeError, ValueError):
        ingestion_latency_ms = 0.0
    self.store.record_risk_refresh(
        token_mint=str(getattr(current_swap, "token_mint", "") or ""),
        trigger_observed_at=trigger_observed_at.isoformat(),
        trigger_received_at=trigger_received_at.isoformat(),
        started_at=now.isoformat(),
        completed_at=now.isoformat(),
        elapsed_ms=0.0,
        ingestion_latency_ms=ingestion_latency_ms,
        end_to_end_ms=max(0.0, (now - trigger_observed_at).total_seconds() * 1000.0),
        complete=False,
        fresh=False,
        readiness={
            "complete": False,
            "fresh": False,
            "candidate_compute_admission_deferred": True,
            "candidate_compute_admission_tier": str(decision.get("tier") or "unknown"),
            "candidate_compute_admission_reason": str(decision.get("reason") or "unknown"),
            "collector_rpc_attempted": False,
            "certification_thresholds_unchanged": True,
        },
    )


async def _refresh_with_compute_admission(
    self: TimedRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    if _ORIGINAL_REFRESH is None:
        raise RuntimeError("candidate compute admission risk wrapper is not installed")
    if current_swap is None:
        await _ORIGINAL_REFRESH(self, mint, at, current_swap=current_swap)
        return
    try:
        eligible = bool(self._eligible_candidate(current_swap))
    except Exception:
        eligible = False
    if not eligible:
        await _ORIGINAL_REFRESH(self, mint, at, current_swap=current_swap)
        return

    signature = str(getattr(current_swap, "signature", "") or "")
    decision = _decision_from_store(self.store, signature)
    if decision is None or bool(decision.get("selected")):
        await _ORIGINAL_REFRESH(self, mint, at, current_swap=current_swap)
        return

    _inc(self, "risk_refresh_deferred")
    _record_skipped_risk_refresh(self, current_swap, decision)


setattr(_refresh_with_compute_admission, "_roi_candidate_compute_admission", True)


async def _process_with_compute_admission(obj: Any, signature: str) -> None:
    if _ORIGINAL_PROCESS is None:
        raise RuntimeError("candidate compute admission handoff wrapper is not installed")
    decision = _decision_from_store(obj.store, str(signature or ""))
    if decision is not None and not bool(decision.get("selected")):
        _inc(obj, "v4_handoff_deferred")
        return
    await _ORIGINAL_PROCESS(obj, signature)


setattr(_process_with_compute_admission, "_roi_candidate_compute_admission", True)


def _status_with_compute_admission(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("candidate compute admission status is not installed")
    payload = _ORIGINAL_STATUS(self)
    _schema(self.store)
    try:
        with self.store._lock:
            totals = self.store.db.execute(
                "SELECT COUNT(*) AS n,SUM(selected) AS selected FROM candidate_compute_admission_decisions"
            ).fetchone()
            tiers = self.store.db.execute(
                "SELECT tier,COUNT(*) AS n,SUM(selected) AS selected,AVG(sampling_fraction) AS fraction "
                "FROM candidate_compute_admission_decisions GROUP BY tier ORDER BY tier"
            ).fetchall()
        count = int(totals["n"] or 0) if totals is not None else 0
        selected = int(totals["selected"] or 0) if totals is not None else 0
        tier_rows = [
            {
                "tier": str(row["tier"]),
                "count": int(row["n"] or 0),
                "selected": int(row["selected"] or 0),
                "configured_fraction": float(row["fraction"] or 0.0),
            }
            for row in tiers
        ]
    except Exception:
        count = selected = 0
        tier_rows = []
    payload["candidate_compute_admission"] = {
        "installed": True,
        "version": ADMISSION_VERSION,
        "decision_count": count,
        "selected_for_expensive_strategy_compute": selected,
        "deferred_from_expensive_strategy_compute": max(0, count - selected),
        "tiers": tier_rows,
        "minimal_transaction_hydration_preserved": True,
        "full_market_observation_scope_reduced": False,
        "continuity_scope_reduced": False,
        "candidate_latency_failure_accounting_retained": True,
        "deferred_candidates_skip_launch_context_fanout": True,
        "deferred_candidates_skip_six_dimension_collector_rpc": True,
        "deferred_candidates_skip_jupiter_shadow_v4_handoff": True,
        "entry_window_seconds_unchanged": ENTRY_WINDOW_SECONDS,
        "max_chase_fraction_unchanged": MAX_CHASE_FRACTION,
        "mature_negative_exploration_fraction": MATURE_NEGATIVE_EXPLORATION_FRACTION,
        "certification_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "prefill_admitted_session": int(getattr(self, "_roi_candidate_compute_admission_prefill_admitted", 0) or 0),
        "prefill_deferred_session": int(getattr(self, "_roi_candidate_compute_admission_prefill_deferred", 0) or 0),
        "v4_handoff_deferred_session": int(getattr(self, "_roi_candidate_compute_admission_v4_handoff_deferred", 0) or 0),
    }
    return payload


setattr(_status_with_compute_admission, "_roi_candidate_compute_admission", True)


def install_candidate_compute_admission() -> None:
    """Install the admission gate after the final candidate/certification composition."""

    global _ORIGINAL_PREFILL, _ORIGINAL_REFRESH, _ORIGINAL_PROCESS, _ORIGINAL_STATUS

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_candidate_compute_admission", False)):
        _ORIGINAL_PREFILL = current_prefill
        try:
            _prefill_with_compute_admission.__dict__.update(getattr(current_prefill, "__dict__", {}))
        except Exception:
            pass
        setattr(_prefill_with_compute_admission, "_roi_candidate_compute_admission", True)
        DirectSolanaIngestionPlane._prefill_launch_context = _prefill_with_compute_admission  # type: ignore[method-assign]

    current_refresh = TimedRiskCollectors.refresh
    if not bool(getattr(current_refresh, "_roi_candidate_compute_admission", False)):
        _ORIGINAL_REFRESH = current_refresh
        try:
            _refresh_with_compute_admission.__dict__.update(getattr(current_refresh, "__dict__", {}))
        except Exception:
            pass
        setattr(_refresh_with_compute_admission, "_roi_candidate_compute_admission", True)
        TimedRiskCollectors.refresh = _refresh_with_compute_admission  # type: ignore[method-assign]

    current_process = handoff._process_candidate_handoff
    if not bool(getattr(current_process, "_roi_candidate_compute_admission", False)):
        _ORIGINAL_PROCESS = current_process
        handoff._process_candidate_handoff = _process_with_compute_admission

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_compute_admission", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_compute_admission.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_compute_admission, "_roi_candidate_compute_admission", True)
        DirectSolanaIngestionPlane.status = _status_with_compute_admission  # type: ignore[method-assign]


__all__ = [
    "ADMISSION_VERSION",
    "ENTRY_WINDOW_SECONDS",
    "MATURE_NEGATIVE_EXPLORATION_FRACTION",
    "candidate_compute_policy",
    "install_candidate_compute_admission",
]
