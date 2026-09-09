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
from .wallet_discovery import PROGRAM_SOURCES

ALIGNMENT_VERSION = "v52-wallet-production-alignment-v2-startup-isolation-aware"


def _runtime(runtime_provider: Any) -> Any:
    return runtime_provider() if callable(runtime_provider) else runtime_provider


def _program_source(source: str) -> str | None:
    parts = {part.upper() for part in str(source or "").split(":") if part}
    matches = sorted(parts.intersection(PROGRAM_SOURCES))
    return matches[0] if len(matches) == 1 else None


def _authoritative_values() -> tuple[float, float, int]:
    execution = execution_policy()
    sizing = target_sizing_policy()
    return (
        float(execution["chase_observe_only_above_fraction"]),
        float(execution["latency_hard_max_seconds"]),
        int(sizing["minimum_forward_samples"]),
    )


def _refresh_policy_objects(policy: Any, intelligence: Any) -> tuple[Any, Any]:
    chase, latency, minimum = _authoritative_values()
    aligned_policy = replace(
        policy,
        max_chase_fraction=chase,
        max_observation_lag_seconds=latency,
    )
    intelligence.policy = replace(
        intelligence.policy,
        min_forward_episodes=minimum,
    )
    return aligned_policy, intelligence


def _refresh_authoritative_policy(discovery: Any) -> None:
    policy, _intelligence = _refresh_policy_objects(discovery.policy, discovery.intelligence)
    discovery.policy = policy


def _refresh_deferred_policy(discovery: Any) -> None:
    kwargs = getattr(discovery, "_kwargs", None)
    if not isinstance(kwargs, dict):
        raise RuntimeError("deferred wallet discovery kwargs unavailable")
    policy = kwargs.get("policy")
    intelligence = kwargs.get("intelligence")
    if policy is None or intelligence is None:
        raise RuntimeError("deferred wallet discovery policy/intelligence unavailable")
    aligned_policy, _ = _refresh_policy_objects(policy, intelligence)
    kwargs["policy"] = aligned_policy


def _ensure_state_column(discovery: Any) -> None:
    with discovery.store._lock, discovery.store.db:
        columns = {
            str(row["name"])
            for row in discovery.store.db.execute("PRAGMA table_info(wallet_discovery_state)").fetchall()
        }
        if "last_normalized_swap_id" not in columns:
            discovery.store.db.execute(
                "ALTER TABLE wallet_discovery_state ADD COLUMN last_normalized_swap_id INTEGER NOT NULL DEFAULT 0"
            )


def _normalized_batch(discovery: Any) -> list[dict[str, Any]]:
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


async def _discover_from_normalized_swaps(self: Any) -> int:
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


def _maybe_propose_v52_cohort(self: Any) -> dict[str, Any] | None:
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


def _inner_contract_available(discovery: Any) -> bool:
    required = (
        "store",
        "policy",
        "intelligence",
        "now_fn",
        "_sample",
        "_record_broad_sample",
        "_proposal_exists",
        "run_once",
        "status",
    )
    return all(hasattr(discovery, name) for name in required)


def _align_inner(discovery: Any) -> None:
    if not _inner_contract_available(discovery):
        raise RuntimeError("canonical wallet discovery inner contract unavailable")
    if bool(getattr(discovery, "_roi_v52_wallet_alignment", False)):
        _refresh_authoritative_policy(discovery)
        return

    _ensure_state_column(discovery)
    _refresh_authoritative_policy(discovery)
    original_run_once = discovery.run_once
    original_status = discovery.status

    async def run_once_aligned(self: Any) -> dict[str, Any]:
        _refresh_authoritative_policy(self)
        return await original_run_once()

    def status_aligned(self: Any) -> dict[str, Any]:
        _refresh_authoritative_policy(self)
        payload = dict(original_status())
        chase, latency, minimum = _authoritative_values()
        payload.update(
            {
                "v52_wallet_alignment_version": ALIGNMENT_VERSION,
                "authoritative_strategy_version": strategy_evolution_snapshot()["strategy_version"],
                "normalized_ingestion_authoritative": True,
                "duplicate_broad_discovery_transaction_hydration": False,
                "minimum_forward_samples": minimum,
                "max_chase_fraction": chase,
                "max_observation_lag_seconds": latency,
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


def install_v52_wallet_intelligence_alignment(runtime_provider: Any) -> None:
    runtime = _runtime(runtime_provider)
    discovery = getattr(runtime, "wallet_discovery", None)
    if discovery is None:
        raise RuntimeError("canonical wallet discovery runtime unavailable")

    register_hook = getattr(discovery, "register_post_bootstrap_hook", None)
    if callable(register_hook) and isinstance(getattr(discovery, "_kwargs", None), dict):
        _refresh_deferred_policy(discovery)
        register_hook(_align_inner)
        setattr(discovery, "_roi_v52_wallet_alignment", True)
    elif _inner_contract_available(discovery):
        _align_inner(discovery)
    else:
        raise RuntimeError("canonical wallet discovery runtime contract unavailable")


def status(runtime_provider: Any) -> dict[str, Any]:
    runtime = _runtime(runtime_provider)
    discovery = getattr(runtime, "wallet_discovery", None)
    installed = bool(discovery is not None and getattr(discovery, "_roi_v52_wallet_alignment", False))
    chase, latency, minimum = _authoritative_values()
    inner = getattr(discovery, "_inner", None) if discovery is not None else None
    if inner is not None and getattr(inner, "_roi_v52_wallet_alignment", False):
        payload = inner.status()
        chase = float(payload.get("max_chase_fraction", chase))
        latency = float(payload.get("max_observation_lag_seconds", latency))
        minimum = int(payload.get("minimum_forward_samples", minimum))
    return {
        "installed": installed,
        "version": ALIGNMENT_VERSION,
        "startup_isolation_preserved": bool(
            discovery is not None and callable(getattr(discovery, "register_post_bootstrap_hook", None))
        ),
        "inner_ready": inner is not None,
        "normalized_ingestion_authoritative": installed,
        "duplicate_broad_discovery_transaction_hydration": False if installed else None,
        "minimum_forward_samples": minimum,
        "max_chase_fraction": chase,
        "max_observation_lag_seconds": latency,
        "future_cohort_parent_is_active_v52_epoch": installed,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["ALIGNMENT_VERSION", "install_v52_wallet_intelligence_alignment", "status"]
