from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from types import MethodType
from typing import Any

from .ingestion import NormalizedSwap
from .strategy_v52_authority import (
    execution_policy,
    strategy_evolution_snapshot,
    target_sizing_policy,
)
from .wallet_discovery import PROGRAM_SOURCES, ContinuousWalletDiscovery

ALIGNMENT_VERSION = "v52-wallet-production-alignment-v1"


def _runtime(runtime_provider: Any) -> Any:
    return runtime_provider() if callable(runtime_provider) else runtime_provider


def _program_source(source: str) -> str | None:
    parts = {part.upper() for part in str(source or "").split(":") if part}
    matches = sorted(parts.intersection(PROGRAM_SOURCES))
    return matches[0] if len(matches) == 1 else None


def _ensure_state_column(discovery: ContinuousWalletDiscovery) -> None:
    with discovery.store._lock, discovery.store.db:
        columns = {
            str(row["name"])
            for row in discovery.store.db.execute("PRAGMA table_info(wallet_discovery_state)").fetchall()
        }
        if "last_normalized_swap_id" not in columns:
            discovery.store.db.execute(
                "ALTER TABLE wallet_discovery_state ADD COLUMN last_normalized_swap_id INTEGER NOT NULL DEFAULT 0"
            )


def _refresh_authoritative_policy(discovery: ContinuousWalletDiscovery) -> None:
    execution = execution_policy()
    sizing = target_sizing_policy()
    discovery.policy = replace(
        discovery.policy,
        max_chase_fraction=float(execution["chase_observe_only_above_fraction"]),
        max_observation_lag_seconds=float(execution["latency_hard_max_seconds"]),
    )
    discovery.intelligence.policy = replace(
        discovery.intelligence.policy,
        min_forward_episodes=int(sizing["minimum_forward_samples"]),
    )


def _normalized_batch(discovery: ContinuousWalletDiscovery) -> list[dict[str, Any]]:
    with discovery.store._lock:
        state = discovery.store.db.execute(
            "SELECT last_normalized_swap_id FROM wallet_discovery_state WHERE id=1"
        ).fetchone()
        cursor = int(state["last_normalized_swap_id"]) if state is not None else 0
        rows = discovery.store.db.execute(
            "SELECT id, signature, slot, observed_at, received_at, wallet, token_mint, side, "
            "token_amount, native_amount_sol, reference_price_sol, source "
            "FROM normalized_swaps WHERE id>? ORDER BY id LIMIT ?",
            (cursor, max(1, int(discovery.policy.broad_scan_limit))),
        ).fetchall()
    return [dict(row) for row in rows]


async def _discover_from_normalized_swaps(self: ContinuousWalletDiscovery) -> int:
    rows = _normalized_batch(self)
    if not rows:
        return 0
    discovered = 0
    newest_id = max(int(row["id"]) for row in rows)
    for row in rows:
        signature = str(row.get("signature") or "")
        source = _program_source(str(row.get("source") or ""))
        if not signature or source is None or not self._sample(signature, self.policy.broad_sample_modulus):
            continue
        try:
            swap = NormalizedSwap(
                signature=signature,
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
        except (KeyError, TypeError, ValueError):
            continue
        if self._record_broad_sample(swap):
            discovered += 1
    now = self.now_fn()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE wallet_discovery_state SET last_normalized_swap_id=?, last_broad_scan_at=?, last_error=NULL WHERE id=1",
            (newest_id, now.isoformat()),
        )
    return discovered


def _maybe_propose_v52_cohort(self: ContinuousWalletDiscovery) -> dict[str, Any] | None:
    if self._proposal_exists():
        return None
    epoch = strategy_evolution_snapshot()
    parent_version = str(epoch["strategy_version"])
    now = self.now_fn()
    strategy_version = f"{parent_version}-wallet-adaptive-{now.strftime('%Y%m%dT%H%M%SZ')}"
    return self.intelligence.propose_next_cohort(
        parent_version=parent_version,
        strategy_version=strategy_version,
    )


def install_v52_wallet_intelligence_alignment(runtime_provider: Any) -> None:
    runtime = _runtime(runtime_provider)
    discovery = getattr(runtime, "wallet_discovery", None)
    if not isinstance(discovery, ContinuousWalletDiscovery):
        raise RuntimeError("canonical wallet discovery runtime unavailable")
    if bool(getattr(discovery, "_roi_v52_wallet_alignment", False)):
        _refresh_authoritative_policy(discovery)
        return

    _ensure_state_column(discovery)
    _refresh_authoritative_policy(discovery)

    original_run_once = discovery.run_once
    original_status = discovery.status

    async def run_once_aligned(self: ContinuousWalletDiscovery) -> dict[str, Any]:
        _refresh_authoritative_policy(self)
        return await original_run_once()

    def status_aligned(self: ContinuousWalletDiscovery) -> dict[str, Any]:
        _refresh_authoritative_policy(self)
        payload = dict(original_status())
        payload.update(
            {
                "v52_wallet_alignment_version": ALIGNMENT_VERSION,
                "authoritative_strategy_version": strategy_evolution_snapshot()["strategy_version"],
                "normalized_ingestion_authoritative": True,
                "duplicate_broad_discovery_transaction_hydration": False,
                "minimum_forward_samples": int(self.intelligence.policy.min_forward_episodes),
                "max_chase_fraction": float(self.policy.max_chase_fraction),
                "max_observation_lag_seconds": float(self.policy.max_observation_lag_seconds),
                "future_cohort_parent_is_active_v52_epoch": True,
                "paper_only": True,
                "live_money_authority": False,
                "signing_or_submission_available": False,
            }
        )
        return payload

    discovery.discover_from_raw_receipts = MethodType(_discover_from_normalized_swaps, discovery)
    discovery.maybe_propose_adaptive_cohort = MethodType(_maybe_propose_v52_cohort, discovery)
    discovery.run_once = MethodType(run_once_aligned, discovery)
    discovery.status = MethodType(status_aligned, discovery)
    setattr(discovery, "_roi_v52_wallet_alignment", True)
    setattr(runtime, "roi_v52_wallet_intelligence_alignment", True)


def status(runtime_provider: Any) -> dict[str, Any]:
    runtime = _runtime(runtime_provider)
    discovery = getattr(runtime, "wallet_discovery", None)
    if not isinstance(discovery, ContinuousWalletDiscovery):
        return {
            "installed": False,
            "version": ALIGNMENT_VERSION,
            "reason": "wallet_discovery_unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    payload = discovery.status()
    return {
        "installed": bool(getattr(discovery, "_roi_v52_wallet_alignment", False)),
        "version": ALIGNMENT_VERSION,
        "normalized_ingestion_authoritative": bool(payload.get("normalized_ingestion_authoritative")),
        "duplicate_broad_discovery_transaction_hydration": bool(
            payload.get("duplicate_broad_discovery_transaction_hydration")
        ),
        "minimum_forward_samples": int(payload.get("minimum_forward_samples") or 0),
        "max_chase_fraction": float(payload.get("max_chase_fraction") or 0.0),
        "max_observation_lag_seconds": float(payload.get("max_observation_lag_seconds") or 0.0),
        "future_cohort_parent_is_active_v52_epoch": bool(
            payload.get("future_cohort_parent_is_active_v52_epoch")
        ),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["ALIGNMENT_VERSION", "install_v52_wallet_intelligence_alignment", "status"]
