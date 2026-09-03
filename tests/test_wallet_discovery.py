import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi.ingestion import NormalizedSwap
from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy, _realized_metrics
from solana_roi.wallet_intelligence import ContinuousWalletIntelligence

T0 = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


class FakeEntityResolver:
    def component(self, wallet, *, as_of):
        return {wallet}

    def entity_id_for(self, wallet, *, fallback_entity_id, as_of):
        return f"graph:{wallet}"


class FakeRpc:
    async def get_signatures_for_address(self, wallet, *, before=None, limit=1000, hedge=False):
        return [], "fake", 1.0

    async def get_transaction(self, signature, *, hedge=False):
        return {}, "fake", 1.0


class FakeMarkProvider:
    async def mark(self, mint):
        return {
            "token_mint": mint,
            "observed_at": T0,
            "received_at": T0,
            "price_sol": 1.0,
            "source": "fake",
            "source_ref": None,
        }


class FakeRisk:
    async def snapshot(self, *args, **kwargs):
        return None


class FakeCollectors:
    async def refresh(self, *args, **kwargs):
        return None


def discovery(tmp_path, *, policy=None):
    store = ObservationEventStore(tmp_path / "wallet-discovery.sqlite3")
    intelligence = ContinuousWalletIntelligence(store)
    worker = ContinuousWalletDiscovery(
        store=store,
        rpc=FakeRpc(),
        entity_resolver=FakeEntityResolver(),
        risk=FakeRisk(),
        risk_collectors=FakeCollectors(),
        intelligence=intelligence,
        mark_provider=FakeMarkProvider(),
        policy=policy or WalletDiscoveryPolicy(),
        enabled=True,
        now_fn=lambda: T0,
    )
    return store, intelligence, worker


def test_realized_metrics_reconstruct_closed_wallet_episodes():
    rows = [
        {"signature": "1", "token_mint": "A", "side": "buy", "token_amount": 10, "price": 1.0, "observed_at": "1", "received_at": "1"},
        {"signature": "2", "token_mint": "A", "side": "sell", "token_amount": 10, "price": 1.5, "observed_at": "2", "received_at": "2"},
        {"signature": "3", "token_mint": "B", "side": "buy", "token_amount": 10, "price": 1.0, "observed_at": "3", "received_at": "3"},
        {"signature": "4", "token_mint": "B", "side": "sell", "token_amount": 10, "price": 0.8, "observed_at": "4", "received_at": "4"},
    ]
    metrics = _realized_metrics(rows, price_key="price")
    assert metrics.closed_episodes == 2
    assert metrics.distinct_tokens == 2
    assert round(metrics.return_on_capital, 6) == 0.15
    assert metrics.profit_factor == 2.5
    assert metrics.hit_rate == 0.5
    assert metrics.max_drawdown > 0


def test_broad_receipt_sample_discovers_wallet_without_strategy_profile(tmp_path, monkeypatch):
    store, _intelligence, worker = discovery(
        tmp_path,
        policy=WalletDiscoveryPolicy(broad_sample_modulus=1, broad_scan_limit=10),
    )
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE direct_solana_recent_receipts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL, source_key TEXT NOT NULL, "
            "slot INTEGER NOT NULL, received_at TEXT NOT NULL, launch_like INTEGER NOT NULL, expires_at TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO direct_solana_recent_receipts(signature, source_key, slot, received_at, launch_like, expires_at) "
            "VALUES ('sig-1', 'PUMP_FUN', 1, ?, 0, ?)",
            (T0.isoformat(), (T0 + timedelta(minutes=15)).isoformat()),
        )

    swap = NormalizedSwap(
        signature="sig-1",
        slot=1,
        observed_at=T0,
        received_at=T0,
        wallet="candidate-wallet",
        token_mint="mint-a",
        side="buy",
        token_amount=10.0,
        native_amount_sol=1.0,
        reference_price_sol=0.1,
        source="solana-direct:PUMP_FUN:buy",
    )
    monkeypatch.setattr("solana_roi.wallet_discovery.normalize_standard_transaction", lambda *args, **kwargs: swap)

    assert asyncio.run(worker.discover_from_raw_receipts()) == 1
    with store._lock:
        candidate = store.db.execute(
            "SELECT wallet, state, broad_sample_count FROM wallet_discovery_candidates WHERE wallet='candidate-wallet'"
        ).fetchone()
    assert candidate is not None
    assert candidate["state"] == "discovered"
    assert candidate["broad_sample_count"] == 1
    assert store.wallet_profile("candidate-wallet") is None


def test_historical_screen_only_opens_forward_tracking_boundary(tmp_path, monkeypatch):
    policy = WalletDiscoveryPolicy(
        historical_min_closed_episodes=2,
        historical_min_distinct_tokens=2,
        historical_min_return_on_capital=0.01,
        historical_min_profit_factor=1.01,
    )
    store, intelligence, worker = discovery(tmp_path, policy=policy)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet, first_seen_at, last_seen_at, state, next_screen_at) "
            "VALUES ('candidate', ?, ?, 'discovered', ?)",
            (T0.isoformat(), T0.isoformat(), T0.isoformat()),
        )

    swaps = [
        NormalizedSwap("b1", 1, T0 - timedelta(hours=2), T0, "candidate", "A", "buy", 10, 1, 0.1),
        NormalizedSwap("s1", 2, T0 - timedelta(hours=1, minutes=59), T0, "candidate", "A", "sell", 10, 1.5, 0.15),
        NormalizedSwap("b2", 3, T0 - timedelta(hours=1), T0, "candidate", "B", "buy", 10, 1, 0.1),
        NormalizedSwap("s2", 4, T0 - timedelta(minutes=59), T0, "candidate", "B", "sell", 10, 1.2, 0.12),
    ]

    async def fake_history(wallet):
        return swaps, "anchor-now"

    monkeypatch.setattr(worker, "_historical_swaps", fake_history)
    assert asyncio.run(worker.screen_one_candidate()) is True
    state = worker._candidate_state("candidate")
    assert state["state"] == "tracking"
    assert state["forward_started_at"] == T0.isoformat()
    assert state["last_signature"] == "anchor-now"
    assert intelligence.latest_snapshot("candidate") is None


def test_only_copyable_forward_observations_feed_wallet_intelligence(tmp_path):
    store, intelligence, worker = discovery(tmp_path)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet, first_seen_at, last_seen_at, state, forward_started_at, last_signature) "
            "VALUES ('candidate', ?, ?, 'tracking', ?, 'anchor')",
            (T0.isoformat(), T0.isoformat(), T0.isoformat()),
        )
        for i in range(2):
            mint = f"mint-{i}"
            buy_at = T0 + timedelta(seconds=i * 10)
            sell_at = buy_at + timedelta(seconds=5)
            store.db.execute(
                "INSERT INTO wallet_discovery_forward_observations("
                "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
                "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
                "side_wallet_flag, source) VALUES (?, 'candidate', ?, 'buy', 10, ?, ?, 1.0, 1.0, 0.0, 1, 500, 1, 0, 0, 'test')",
                (f"buy-{i}", mint, buy_at.isoformat(), buy_at.isoformat()),
            )
            store.db.execute(
                "INSERT INTO wallet_discovery_forward_observations("
                "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
                "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
                "side_wallet_flag, source) VALUES (?, 'candidate', ?, 'sell', 10, ?, ?, 1.5, 1.5, 0.0, 1, 500, 1, 0, 0, 'test')",
                (f"sell-{i}", mint, sell_at.isoformat(), sell_at.isoformat()),
            )
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
            "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
            "side_wallet_flag, source) VALUES ('late-buy', 'candidate', 'mint-late', 'buy', 10, ?, ?, 1.0, 1.5, 0.5, 0, 30000, 1, 0, 0, 'test')",
            ((T0 + timedelta(minutes=1)).isoformat(), (T0 + timedelta(minutes=1, seconds=30)).isoformat()),
        )

    snapshot = worker.refresh_wallet_snapshot("candidate")
    assert snapshot is not None
    assert snapshot.closed_episodes == 2
    assert snapshot.copyable_return_on_capital == 0.5
    assert snapshot.copyability_rate == 0.8
    assert snapshot.manipulation_risk == 0.0
    assert snapshot.side_wallet_risk == 0.0
    persisted = intelligence.latest_snapshot("candidate")
    assert persisted is not None
    assert persisted.closed_episodes == 2
    assert persisted.source == "continuous-wallet-discovery-forward-v1"


def test_forward_gap_resets_evidence_epoch_instead_of_backfilling_promotion_data(tmp_path):
    store, intelligence, worker = discovery(tmp_path)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_candidates(wallet, first_seen_at, last_seen_at, state, forward_started_at, last_signature) "
            "VALUES ('candidate', ?, ?, 'tracking', ?, 'old-anchor')",
            (T0.isoformat(), T0.isoformat(), T0.isoformat()),
        )
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
            "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
            "side_wallet_flag, source) VALUES ('old', 'candidate', 'mint', 'buy', 1, ?, ?, 1, 1, 0, 1, 1, 1, 0, 0, 'test')",
            (T0.isoformat(), T0.isoformat()),
        )

    async def missing_anchor(wallet, anchor, started_at):
        return [{"signature": "new-head", "err": None, "blockTime": int(T0.timestamp())}], False

    worker._forward_signature_rows = missing_anchor
    assert asyncio.run(worker.poll_wallet("candidate")) == 0
    with store._lock:
        count = store.db.execute(
            "SELECT COUNT(*) FROM wallet_discovery_forward_observations WHERE wallet='candidate'"
        ).fetchone()[0]
        state = store.db.execute(
            "SELECT last_signature, forward_epoch_resets FROM wallet_discovery_candidates WHERE wallet='candidate'"
        ).fetchone()
    assert count == 0
    assert state["last_signature"] == "new-head"
    assert state["forward_epoch_resets"] == 1
    assert intelligence.latest_snapshot("candidate") is None
