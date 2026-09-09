from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.strategy_v52_authority import execution_policy, strategy_evolution_snapshot, target_sizing_policy
from solana_roi.v52_wallet_alpha_refinement import (
    WalletAlphaRefinementLedger,
    WalletMarginalAlphaObservation,
)
from solana_roi.v52_wallet_intelligence_alignment import install_v52_wallet_intelligence_alignment
from solana_roi.wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy


class _NoDuplicateHydrationRpc:
    async def get_transaction(self, *_args, **_kwargs):
        raise AssertionError("wallet broad discovery must not re-fetch an already-normalized transaction")

    async def get_signatures_for_address(self, *_args, **_kwargs):
        return [], "test", 0.0


def _runtime(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-final-alignment.sqlite3")
    discovery = ContinuousWalletDiscovery(
        store=store,
        rpc=_NoDuplicateHydrationRpc(),
        entity_resolver=object(),
        risk=object(),
        risk_collectors=object(),
        policy=WalletDiscoveryPolicy(broad_sample_modulus=1),
        enabled=True,
    )
    return SimpleNamespace(store=store, wallet_discovery=discovery)


def test_wallet_discovery_uses_normalized_journal_without_duplicate_rpc_hydration(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    install_v52_wallet_intelligence_alignment(runtime)
    now = datetime.now(timezone.utc)
    inserted = runtime.store.record_swap(
        signature="sig-normalized-1",
        slot=123,
        observed_at=now.isoformat(),
        received_at=(now + timedelta(milliseconds=25)).isoformat(),
        wallet="wallet-a",
        token_mint="mint-a",
        side="buy",
        token_amount=10.0,
        native_amount_sol=1.0,
        reference_price_sol=0.1,
        ingestion_latency_ms=25.0,
        source="solana-direct:PUMP_FUN:buy",
    )
    assert inserted is True

    added = asyncio.run(runtime.wallet_discovery.discover_from_raw_receipts())
    assert added == 1
    with runtime.store._lock:
        broad = runtime.store.db.execute(
            "SELECT signature, wallet, token_mint FROM wallet_discovery_broad_samples"
        ).fetchall()
        state = runtime.store.db.execute(
            "SELECT last_normalized_swap_id FROM wallet_discovery_state WHERE id=1"
        ).fetchone()
    assert [tuple(row) for row in broad] == [("sig-normalized-1", "wallet-a", "mint-a")]
    assert int(state["last_normalized_swap_id"]) > 0

    # Idempotent replay: the normalized transaction is not duplicated and no RPC
    # transaction hydration is invoked on the second pass either.
    assert asyncio.run(runtime.wallet_discovery.discover_from_raw_receipts()) == 0
    with runtime.store._lock:
        assert runtime.store.db.execute("SELECT COUNT(*) FROM wallet_discovery_broad_samples").fetchone()[0] == 1


def test_wallet_policy_refreshes_from_current_v52_authority_and_parent_epoch(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    install_v52_wallet_intelligence_alignment(runtime)
    discovery = runtime.wallet_discovery
    status = discovery.status()

    assert status["max_chase_fraction"] == execution_policy()["chase_observe_only_above_fraction"]
    assert status["max_observation_lag_seconds"] == execution_policy()["latency_hard_max_seconds"]
    assert status["minimum_forward_samples"] == target_sizing_policy()["minimum_forward_samples"] == 30
    assert status["normalized_ingestion_authoritative"] is True
    assert status["duplicate_broad_discovery_transaction_hydration"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False

    captured = {}
    discovery.intelligence.propose_next_cohort = lambda **kwargs: captured.update(kwargs) or kwargs
    proposal = discovery.maybe_propose_adaptive_cohort()
    assert proposal is not None
    assert captured["parent_version"] == strategy_evolution_snapshot()["strategy_version"]
    assert captured["strategy_version"].startswith(captured["parent_version"] + "-wallet-adaptive-")


def test_paired_contextual_wallet_alpha_requires_authoritative_forward_gate(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "wallet-alpha.sqlite3")
    ledger = WalletAlphaRefinementLedger(store, half_life_hours=24.0)
    now = datetime.now(timezone.utc)
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    assert minimum == 30

    for index in range(minimum - 1):
        observed = now - timedelta(hours=minimum - index)
        assert ledger.record_paired(
            WalletMarginalAlphaObservation(
                wallet="wallet-alpha",
                context_key="pump_fun|bonding_curve|neutral|clean",
                candidate_id=f"candidate-{index}",
                observed_at=observed,
                wallet_policy_return=0.20,
                matched_control_return=0.05,
                executable_mfe=0.40,
                executable_mae=0.10,
                copyable=True,
            )
        )
    immature = ledger.score("wallet-alpha", "pump_fun|bonding_curve|neutral|clean", as_of=now)
    assert immature.paired_forward_episodes == minimum - 1
    assert immature.eligible_for_strategy_influence is False
    assert "insufficient_forward_episodes" in immature.blockers

    assert ledger.record_paired(
        WalletMarginalAlphaObservation(
            wallet="wallet-alpha",
            context_key="pump_fun|bonding_curve|neutral|clean",
            candidate_id="candidate-final",
            observed_at=now,
            wallet_policy_return=0.20,
            matched_control_return=0.05,
            executable_mfe=0.40,
            executable_mae=0.10,
            copyable=True,
        )
    )
    mature = ledger.score("wallet-alpha", "pump_fun|bonding_curve|neutral|clean", as_of=now)
    assert mature.paired_forward_episodes == minimum
    assert mature.decayed_marginal_alpha > 0.0
    assert mature.decayed_capture_ratio is not None
    assert abs(mature.decayed_capture_ratio - 0.50) < 1e-9
    assert mature.eligible_for_strategy_influence is True
    assert mature.blockers == ()


def test_executable_missed_opportunity_ledger_is_idempotent(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "wallet-missed.sqlite3")
    ledger = WalletAlphaRefinementLedger(store)
    first_executable = datetime.now(timezone.utc)
    kwargs = dict(
        candidate_id="missed-1",
        context_key="pump_amm|early_post_graduation|neutral|clean",
        first_executable_at=first_executable,
        reason="wallet_signal_not_yet_promoted",
        executable_mfe=0.75,
        executable_mae=0.18,
    )
    assert ledger.record_missed_opportunity(**kwargs) is True
    assert ledger.record_missed_opportunity(**kwargs) is False
    status = ledger.status()
    assert status["missed_opportunity_rows"] == 1
    assert status["executable_mfe_mae_from_first_realistic_executable_timestamp"] is True
    assert status["capture_ratio_definition"] == "realized_executable_net_return / executable_mfe"
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
