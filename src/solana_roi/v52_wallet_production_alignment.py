from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import MethodType
from typing import Any

from .ingestion import NormalizedSwap
from .strategy_v52_authority import (
    STRATEGY_VERSION,
    execution_policy,
    strategy_evolution_snapshot,
    target_sizing_policy,
)

ALIGNMENT_VERSION = "v52-wallet-production-alignment-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_RUNTIME: Any | None = None
_INSTALLED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _runtime_value(runtime_provider: Any) -> Any:
    return runtime_provider() if callable(runtime_provider) else runtime_provider


def _source_key(source: str) -> str | None:
    parts = str(source or "").split(":")
    if len(parts) < 2 or parts[0] != "solana-direct":
        return None
    value = parts[1].upper()
    return value or None


def _ensure_normalized_cursor(discovery: Any) -> None:
    now = _utcnow().isoformat()
    with discovery.store._lock, discovery.store.db:
        discovery.store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_discovery_normalized_state ("
            "id INTEGER PRIMARY KEY CHECK(id=1), last_normalized_swap_id INTEGER NOT NULL, "
            "initialized_at TEXT NOT NULL, last_scan_at TEXT, last_error TEXT)"
        )
        existing = discovery.store.db.execute(
            "SELECT id FROM wallet_discovery_normalized_state WHERE id=1"
        ).fetchone()
        if existing is None:
            row = discovery.store.db.execute(
                "SELECT COALESCE(MAX(id),0) AS n FROM normalized_swaps"
            ).fetchone()
            cursor = int(row["n"] or 0) if row is not None else 0
            discovery.store.db.execute(
                "INSERT INTO wallet_discovery_normalized_state("
                "id,last_normalized_swap_id,initialized_at,last_scan_at,last_error) "
                "VALUES (1,?,?,NULL,NULL)",
                (cursor, now),
            )


def _normalized_cursor(discovery: Any) -> int:
    _ensure_normalized_cursor(discovery)
    with discovery.store._lock:
        row = discovery.store.db.execute(
            "SELECT last_normalized_swap_id FROM wallet_discovery_normalized_state WHERE id=1"
        ).fetchone()
    return int(row["last_normalized_swap_id"] or 0) if row is not None else 0


def _normalized_rows(discovery: Any) -> list[dict[str, Any]]:
    cursor = _normalized_cursor(discovery)
    with discovery.store._lock:
        rows = discovery.store.db.execute(
            "SELECT id,signature,slot,observed_at,received_at,wallet,token_mint,side,"
            "token_amount,native_amount_sol,reference_price_sol,source "
            "FROM normalized_swaps WHERE id>? AND source LIKE 'solana-direct:%' "
            "ORDER BY id LIMIT ?",
            (cursor, max(1, int(discovery.policy.broad_scan_limit))),
        ).fetchall()
    return [dict(row) for row in rows]


def _row_to_swap(row: dict[str, Any]) -> NormalizedSwap:
    return NormalizedSwap(
        signature=str(row["signature"]),
        slot=int(row["slot"]),
        observed_at=datetime.fromisoformat(str(row["observed_at"])),
        received_at=datetime.fromisoformat(str(row["received_at"])),
        wallet=str(row["wallet"]),
        token_mint=str(row["token_mint"]),
        side=str(row["side"]),
        token_amount=float(row["token_amount"]),
        native_amount_sol=float(row["native_amount_sol"]),
        reference_price_sol=float(row["reference_price_sol"]),
        source=str(row["source"]),
    )


async def _discover_from_normalized_swaps(self: Any) -> int:
    from .wallet_discovery import PROGRAM_SOURCES

    rows = _normalized_rows(self)
    if not rows:
        return 0
    discovered = 0
    newest_id = max(int(row["id"]) for row in rows)
    for row in rows:
        signature = str(row.get("signature") or "")
        source = _source_key(str(row.get("source") or ""))
        if (
            not signature
            or source not in PROGRAM_SOURCES
            or not self._sample(signature, self.policy.broad_sample_modulus)
        ):
            continue
        if self._record_broad_sample(_row_to_swap(row)):
            discovered += 1
    now = self.now_fn()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE wallet_discovery_normalized_state SET "
            "last_normalized_swap_id=?,last_scan_at=?,last_error=NULL WHERE id=1",
            (newest_id, now.isoformat()),
        )
    return discovered


def _adaptive_cohort_from_active_v52(self: Any) -> dict[str, Any] | None:
    if self._proposal_exists():
        return None
    now = self.now_fn()
    epoch = strategy_evolution_snapshot()
    sequence = int(epoch.get("sequence") or 0)
    version = f"{STRATEGY_VERSION}-adaptive-e{sequence}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    return self.intelligence.propose_next_cohort(
        parent_version=STRATEGY_VERSION,
        strategy_version=version,
    )


def _status_with_alignment(self: Any) -> dict[str, Any]:
    original = getattr(self, "_roi_v52_alignment_original_status", None)
    if not callable(original):
        raise RuntimeError("wallet discovery alignment status predecessor unavailable")
    payload = dict(original())
    with self.store._lock:
        row = self.store.db.execute(
            "SELECT last_normalized_swap_id,initialized_at,last_scan_at,last_error "
            "FROM wallet_discovery_normalized_state WHERE id=1"
        ).fetchone()
    normalized = dict(row) if row is not None else {}
    promotion_minimum = int(target_sizing_policy()["minimum_forward_samples"])
    payload.update(
        {
            "active_v52_policy_bound": True,
            "active_v52_strategy_version": STRATEGY_VERSION,
            "zero_duplicate_normalized_handoff": True,
            "broad_program_receipt_sampling": False,
            "normalized_swap_sampling": True,
            "normalized_handoff_cursor": int(normalized.get("last_normalized_swap_id") or 0),
            "normalized_handoff_initialized_at": normalized.get("initialized_at"),
            "normalized_handoff_last_scan_at": normalized.get("last_scan_at"),
            "normalized_handoff_last_error": normalized.get("last_error"),
            "promotion_min_forward_episodes": promotion_minimum,
            "wallet_advancement_proof": {
                "normalized_cursor_advanced": int(normalized.get("last_normalized_swap_id") or 0) > 0,
                "discovery_cycle_seen": bool(payload.get("last_cycle_at")),
                "broad_samples_present": int(payload.get("broad_samples") or 0) > 0,
                "forward_observations_present": int(payload.get("forward_observations") or 0) > 0,
                "copyable_forward_observations_present": int(payload.get("copyable_forward_observations") or 0) > 0,
                "wallet_snapshots_present": int((payload.get("wallet_intelligence") or {}).get("observed_wallets") or 0) > 0,
            },
            "paper_only": True,
            "live_money_authority": False,
            "signing_or_submission_available": False,
        }
    )
    return payload


def _install_context_router_authority_binding() -> None:
    from . import wallet_context_router as router

    def classify(row: dict[str, Any]) -> dict[str, Any]:
        venue = str(row.get("venue") or "UNKNOWN")
        stage = str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue")
        lag_ms = router._safe_float(row.get("observation_lag_ms")) or 0.0
        processing_ms = router._safe_float(row.get("processing_delay_ms")) or 0.0
        total_seconds = max(0.0, lag_ms + processing_ms) / 1000.0
        chase = router._safe_float(row.get("chase_fraction"))
        active = execution_policy()
        max_chase = float(active["chase_observe_only_above_fraction"])
        max_latency = min(
            float(router.STRATEGY_ENTRY_CEILING_SECONDS),
            float(active["latency_hard_max_seconds"]),
        )
        reasons: list[str] = []
        if venue not in router.VENUES:
            reasons.append("unknown_or_unsupported_venue")
        if not bool(row.get("copyable")):
            reasons.append("not_copyable_at_observation")
        if total_seconds > max_latency:
            reasons.append("outside_strategy_entry_ceiling")
        if chase is not None and chase > max_chase:
            reasons.append("outside_max_chase")
        pump_usage = None
        if venue == "PUMP_FUN" and stage == router.PUMP_BONDING_CURVE:
            pump_usage = "discovery_and_residual_continuation_only_not_first_slot_sniping"
        return {
            "structurally_accessible": not reasons,
            "reasons": reasons,
            "venue": venue,
            "lifecycle_stage": stage,
            "observed_pipeline_seconds": total_seconds,
            "chase_fraction": chase,
            "pump_fun_usage": pump_usage,
            "authority_max_chase_fraction": max_chase,
            "authority_max_latency_seconds": max_latency,
            "millisecond_sniping_targeted": False,
            "first_slot_execution_authority": False,
            "source_pre_observation_return_authority": False,
        }

    def accessibility_summary(self: Any, observations: list[dict[str, Any]]) -> dict[str, Any]:
        classified = [router.classify_observation_accessibility(row) for row in observations]
        reason_counts: dict[str, int] = {}
        accessible = 0
        for row in classified:
            if row["structurally_accessible"]:
                accessible += 1
            for reason in row["reasons"]:
                key = str(reason)
                reason_counts[key] = reason_counts.get(key, 0) + 1
        active = execution_policy()
        max_latency = min(
            float(router.STRATEGY_ENTRY_CEILING_SECONDS),
            float(active["latency_hard_max_seconds"]),
        )
        return {
            "observation_count": len(classified),
            "structurally_accessible_observations": accessible,
            "structurally_inaccessible_observations": len(classified) - accessible,
            "inaccessibility_reasons": dict(sorted(reason_counts.items())),
            "processing_target_seconds": router.PRODUCTION_PROCESSING_TARGET_SECONDS,
            "strategy_entry_ceiling_seconds": max_latency,
            "max_chase_fraction": float(active["chase_observe_only_above_fraction"]),
            "active_v52_policy_bound": True,
            "first_slot_or_subsecond_required_edge": "structurally_disqualified",
            "pump_fun_bonding_curve_removed_from_observation": False,
            "pump_fun_bonding_curve_execution_race_targeted": False,
            "pump_fun_role": "discovery_and_residual_continuation_research",
        }

    setattr(classify, "_roi_v52_wallet_policy_binding", True)
    setattr(accessibility_summary, "_roi_v52_wallet_policy_binding", True)
    router.classify_observation_accessibility = classify
    router.WalletContextRouter.accessibility_summary = accessibility_summary


def install_v52_wallet_production_alignment(runtime_provider: Any) -> None:
    global _RUNTIME, _INSTALLED
    runtime = _runtime_value(runtime_provider)
    discovery = getattr(runtime, "wallet_discovery", None)
    intelligence = getattr(runtime, "wallet_intelligence", None)
    if discovery is None or intelligence is None:
        raise RuntimeError("v5.2 wallet production alignment requires wallet discovery and intelligence")

    active_execution = execution_policy()
    active_sizing = target_sizing_policy()
    discovery.policy = replace(
        discovery.policy,
        max_chase_fraction=float(active_execution["chase_observe_only_above_fraction"]),
        max_observation_lag_seconds=min(
            float(discovery.policy.max_observation_lag_seconds),
            float(active_execution["latency_hard_max_seconds"]),
        ),
    )
    intelligence.policy = replace(
        intelligence.policy,
        min_forward_episodes=int(active_sizing["minimum_forward_samples"]),
    )
    discovery.intelligence = intelligence

    _ensure_normalized_cursor(discovery)
    discovery.discover_from_raw_receipts = MethodType(_discover_from_normalized_swaps, discovery)
    discovery.maybe_propose_adaptive_cohort = MethodType(_adaptive_cohort_from_active_v52, discovery)
    if not hasattr(discovery, "_roi_v52_alignment_original_status"):
        discovery._roi_v52_alignment_original_status = discovery.status
        discovery.status = MethodType(_status_with_alignment, discovery)

    _install_context_router_authority_binding()
    _RUNTIME = runtime
    _INSTALLED = True


def status() -> dict[str, Any]:
    runtime = _RUNTIME
    discovery = getattr(runtime, "wallet_discovery", None) if runtime is not None else None
    payload: dict[str, Any] = {}
    if discovery is not None:
        try:
            payload = dict(discovery.status())
        except Exception as exc:
            payload = {"error": f"{type(exc).__name__}: wallet alignment status unavailable"}
    return {
        "alignment_version": ALIGNMENT_VERSION,
        "installed": _INSTALLED,
        "active_strategy_version": STRATEGY_VERSION,
        "active_strategy_epoch": strategy_evolution_snapshot(),
        "active_execution_policy": {
            "chase_observe_only_above_fraction": float(execution_policy()["chase_observe_only_above_fraction"]),
            "latency_hard_max_seconds": float(execution_policy()["latency_hard_max_seconds"]),
        },
        "promotion_min_forward_episodes": int(target_sizing_policy()["minimum_forward_samples"]),
        "zero_duplicate_normalized_handoff": bool(payload.get("zero_duplicate_normalized_handoff")),
        "normalized_handoff_cursor": payload.get("normalized_handoff_cursor"),
        "wallet_advancement_proof": payload.get("wallet_advancement_proof"),
        "context_router_active_v52_policy_bound": _INSTALLED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "ALIGNMENT_VERSION",
    "install_v52_wallet_production_alignment",
    "status",
]
