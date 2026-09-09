from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import wallet_context_router
from solana_roi.observation_store import ObservationEventStore
from solana_roi.strategy_v52_authority import STRATEGY_VERSION, execution_policy, target_sizing_policy
from solana_roi.v52_wallet_production_alignment import (
    install_v52_wallet_production_alignment,
    status as alignment_status,
)
from solana_roi.wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy
from solana_roi.wallet_intelligence import ContinuousWalletIntelligence, WalletPromotionPolicy


class _NoDuplicateRpc:
    def __init__(self) -> None:
        self.transaction_calls = 0

    async def get_transaction(self, *args, **kwargs):
        self.transaction_calls += 1
        raise AssertionError("normalized broad discovery must not re-fetch a transaction")


class _DummyResolver:
    pass


class _DummyRisk:
    pass


class _DummyCollectors:
    pass


def _runtime(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-alignment.sqlite3")
    intelligence = ContinuousWalletIntelligence(
        store,
        policy=WalletPromotionPolicy(min_forward_episodes=1),
    )
    rpc = _NoDuplicateRpc()
    discovery = ContinuousWalletDiscovery(
        store=store,
        rpc=rpc,
        entity_resolver=_DummyResolver(),
        risk=_DummyRisk(),
        risk_collectors=_DummyCollectors(),
        intelligence=intelligence,
        policy=WalletDiscoveryPolicy(
            broad_sample_modulus=1,
            broad_scan_limit=100,
            max_chase_fraction=0.01,
            max_observation_lag_seconds=999.0,
        ),
        enabled=True,
    )
    runtime = SimpleNamespace(
        wallet_intelligence=intelligence,
        wallet_discovery=discovery,
    )
    return store, rpc, runtime


def test_alignment_binds_live_v52_policy_and_preserves_forward_gate(tmp_path) -> None:
    store, _rpc, runtime = _runtime(tmp_path)
    try:
        install_v52_wallet_production_alignment(runtime)
        active_execution = execution_policy()
        active_sizing = target_sizing_policy()

        assert runtime.wallet_discovery.policy.max_chase_fraction == float(
            active_execution["chase_observe_only_above_fraction"]
        )
        assert runtime.wallet_discovery.policy.max_observation_lag_seconds <= float(
            active_execution["latency_hard_max_seconds"]
        )
        assert runtime.wallet_intelligence.policy.min_forward_episodes == int(
            active_sizing["minimum_forward_samples"]
        )
        assert runtime.wallet_intelligence.policy.min_forward_episodes == 30

        payload = alignment_status()
        assert payload["installed"] is True
        assert payload["promotion_min_forward_episodes"] == 30
        assert payload["paper_only"] is True
        assert payload["live_money_authority"] is False
        assert payload["signing_available"] is False
        assert payload["transaction_submission_available"] is False
    finally:
        store.close()


def test_normalized_handoff_is_idempotent_and_performs_zero_duplicate_rpc(tmp_path) -> None:
    store, rpc, runtime = _runtime(tmp_path)
    try:
        install_v52_wallet_production_alignment(runtime)
        now = datetime.now(timezone.utc)
        inserted = store.record_swap(
            signature="normalized-after-alignment-1",
            slot=123,
            observed_at=now.isoformat(),
            received_at=now.isoformat(),
            wallet="wallet-new-alpha",
            token_mint="token-new-alpha",
            side="buy",
            token_amount=100.0,
            native_amount_sol=1.0,
            reference_price_sol=0.01,
            ingestion_latency_ms=0.0,
            source="solana-direct:PUMP_FUN:buy",
        )
        assert inserted is True

        first = asyncio.run(runtime.wallet_discovery.discover_from_raw_receipts())
        second = asyncio.run(runtime.wallet_discovery.discover_from_raw_receipts())
        assert first == 1
        assert second == 0
        assert rpc.transaction_calls == 0

        with store._lock:
            broad = store.db.execute(
                "SELECT signature,wallet,token_mint FROM wallet_discovery_broad_samples"
            ).fetchall()
            candidates = store.db.execute(
                "SELECT wallet,state FROM wallet_discovery_candidates WHERE wallet='wallet-new-alpha'"
            ).fetchall()
        assert len(broad) == 1
        assert str(broad[0]["signature"]) == "normalized-after-alignment-1"
        assert len(candidates) == 1
        assert str(candidates[0]["state"]) == "discovered"

        status = runtime.wallet_discovery.status()
        assert status["zero_duplicate_normalized_handoff"] is True
        assert status["normalized_swap_sampling"] is True
        assert status["broad_program_receipt_sampling"] is False
        assert status["normalized_handoff_cursor"] > 0
    finally:
        store.close()


def test_future_cohort_lineage_uses_v52_not_legacy_baseline(tmp_path) -> None:
    store, _rpc, runtime = _runtime(tmp_path)
    try:
        install_v52_wallet_production_alignment(runtime)
        captured = {}

        runtime.wallet_discovery._proposal_exists = lambda: False

        def _propose_next_cohort(*, parent_version: str, strategy_version: str):
            captured["parent_version"] = parent_version
            captured["strategy_version"] = strategy_version
            return {"proposed": False}

        runtime.wallet_intelligence.propose_next_cohort = _propose_next_cohort
        result = runtime.wallet_discovery.maybe_propose_adaptive_cohort()
        assert result == {"proposed": False}
        assert captured["parent_version"] == STRATEGY_VERSION
        assert captured["strategy_version"].startswith(f"{STRATEGY_VERSION}-adaptive-e")
    finally:
        store.close()


def test_context_accessibility_reads_current_v52_policy() -> None:
    # The installer replaces the legacy BASELINE chase dependency with the active
    # governed v5.2 execution policy. The row is exactly on the live authority
    # boundary, so it must not be rejected as outside_max_chase.
    active = execution_policy()
    max_chase = float(active["chase_observe_only_above_fraction"])
    max_latency = float(active["latency_hard_max_seconds"])
    result = wallet_context_router.classify_observation_accessibility(
        {
            "venue": "PUMP_FUN",
            "lifecycle_stage": wallet_context_router.PUMP_BONDING_CURVE,
            "copyable": True,
            "observation_lag_ms": min(max_latency, 1.0) * 1000.0,
            "processing_delay_ms": 0.0,
            "chase_fraction": max_chase,
        }
    )
    assert result["structurally_accessible"] is True
    assert result["authority_max_chase_fraction"] == max_chase
    assert "outside_max_chase" not in result["reasons"]
