from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import api
from solana_roi import wallet_discovery_background_status_repair as repair
from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy
from solana_roi.wallet_intelligence import ContinuousWalletIntelligence

T0 = datetime(2026, 9, 12, 19, 0, tzinfo=timezone.utc)
HISTORY_WALLETS = 40
HISTORY_ROWS_PER_WALLET = 200


class FakeRpc:
    async def get_signatures_for_address(self, wallet, *, before=None, limit=1000, hedge=False):
        return [], "fake", 1.0

    async def get_transaction(self, signature, *, hedge=False):
        return {}, "fake", 1.0


class FakeEntityResolver:
    def component(self, wallet, *, as_of):
        return {wallet}

    def entity_id_for(self, wallet, *, fallback_entity_id, as_of):
        return f"graph:{wallet}"


class FakeRisk:
    async def snapshot(self, *args, **kwargs):
        return None


class FakeCollectors:
    async def refresh(self, *args, **kwargs):
        return None


class FakeMarkProvider:
    async def mark(self, mint):
        return None


def _build(tmp_path):
    store = ObservationEventStore(tmp_path / "wallet-background-cache.sqlite3")
    intelligence = ContinuousWalletIntelligence(store)
    worker = ContinuousWalletDiscovery(
        store=store,
        rpc=FakeRpc(),
        entity_resolver=FakeEntityResolver(),
        risk=FakeRisk(),
        risk_collectors=FakeCollectors(),
        intelligence=intelligence,
        mark_provider=FakeMarkProvider(),
        policy=WalletDiscoveryPolicy(poll_interval_seconds=10.0),
        enabled=True,
        now_fn=lambda: T0,
    )
    return store, intelligence, worker


def _seed_large_wallet_history(store) -> None:
    rows = []
    for wallet_index in range(HISTORY_WALLETS):
        wallet = f"wallet-{wallet_index:03d}"
        entity = f"entity-{wallet_index:03d}"
        for sample in range(HISTORY_ROWS_PER_WALLET):
            observed = (T0 - timedelta(seconds=HISTORY_ROWS_PER_WALLET - sample)).isoformat()
            rows.append(
                (
                    wallet,
                    entity,
                    observed,
                    40,
                    0.20,
                    0.10,
                    1.25,
                    0.55,
                    0.20,
                    0.90,
                    0.02,
                    0.02,
                    500.0,
                    "production-shaped-history",
                )
            )
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO wallet_intelligence_snapshots("
            "wallet,entity_id,observed_at,closed_episodes,copyable_return_on_capital,"
            "geometric_growth,profit_factor,hit_rate,max_drawdown,copyability_rate,"
            "manipulation_risk,side_wallet_risk,median_entry_lag_ms,source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )


def _replace_cycle_work_with_bounded_fakes(worker, calls: list[str]) -> None:
    async def ensure_incumbents() -> None:
        calls.append("ensure")

    async def discover_from_raw_receipts() -> int:
        calls.append("discover")
        return 3

    async def screen_one_candidate() -> bool:
        calls.append("screen")
        return True

    async def poll_wallet(wallet: str) -> int:
        calls.append(f"poll:{wallet}")
        return 1

    worker.ensure_incumbents = ensure_incumbents
    worker.discover_from_raw_receipts = discover_from_raw_receipts
    worker.screen_one_candidate = screen_one_candidate
    worker._tracked_wallets = lambda: ["tracked-a", "tracked-b"]
    worker.poll_wallet = poll_wallet
    worker.maybe_propose_adaptive_cohort = lambda: calls.append("propose") or None


def _touches_history(statements: list[str]) -> bool:
    return any("wallet_intelligence_snapshots" in statement.lower() for statement in statements)


def test_background_cycle_avoids_history_scaled_status_but_explicit_run_once_remains_exact(tmp_path) -> None:
    store, _intelligence, worker = _build(tmp_path)
    try:
        _seed_large_wallet_history(store)
        calls: list[str] = []
        _replace_cycle_work_with_bounded_fakes(worker, calls)

        statements: list[str] = []
        store.db.set_trace_callback(statements.append)
        asyncio.run(repair._background_cycle(worker))
        store.db.set_trace_callback(None)

        assert calls == ["ensure", "discover", "screen", "poll:tracked-a", "poll:tracked-b", "propose"]
        assert _touches_history(statements) is False, statements
        with store._lock:
            state = store.db.execute(
                "SELECT last_cycle_at,last_error FROM wallet_discovery_state WHERE id=1"
            ).fetchone()
        assert state is not None
        assert state["last_cycle_at"] == T0.isoformat()
        assert state["last_error"] is None

        calls.clear()
        statements.clear()
        store.db.set_trace_callback(statements.append)
        payload = asyncio.run(worker.run_once())
        store.db.set_trace_callback(None)

        assert calls == ["ensure", "discover", "screen", "poll:tracked-a", "poll:tracked-b", "propose"]
        assert _touches_history(statements) is True
        assert payload["wallet_intelligence"]["observed_wallets"] == HISTORY_WALLETS
        assert payload["paper_only"] is True
        assert payload["live_money_authority"] is False
    finally:
        store.db.set_trace_callback(None)
        store.close()


def test_public_wallet_status_endpoints_still_materialize_exact_status(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, intelligence, worker = _build(tmp_path)
    try:
        _seed_large_wallet_history(store)
        runtime = SimpleNamespace(wallet_intelligence=intelligence, wallet_discovery=worker)
        monkeypatch.setattr(api, "ingestion_runtime", lambda: runtime)

        statements: list[str] = []
        store.db.set_trace_callback(statements.append)
        discovery_status = api.wallet_discovery_status()
        intelligence_status = api.wallet_intelligence_status()
        store.db.set_trace_callback(None)

        assert _touches_history(statements) is True
        assert discovery_status["wallet_intelligence"]["observed_wallets"] == HISTORY_WALLETS
        assert intelligence_status["observed_wallets"] == HISTORY_WALLETS
        assert intelligence_status["promotion_authority"] == "future_immutable_cohort_only"
        assert discovery_status["paper_only"] is True
        assert discovery_status["live_money_authority"] is False
    finally:
        store.db.set_trace_callback(None)
        store.close()


def _insert_cohort(store, sequence: int, status: str) -> None:
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO adaptive_wallet_cohorts("
            "strategy_version,created_at,parent_version,cohort_json,rationale_json,cohort_sha256,status) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                f"strategy-{sequence}",
                (T0 + timedelta(seconds=sequence)).isoformat(),
                "parent",
                "[]",
                "{}",
                f"digest-{sequence}",
                status,
            ),
        )


def test_bounded_proposal_tail_matches_original_latest_row_semantics(tmp_path) -> None:
    store, _intelligence, worker = _build(tmp_path)
    try:
        assert repair._ORIGINAL_PROPOSAL_EXISTS(worker) is False
        assert repair._proposal_exists_tail(worker) is False

        _insert_cohort(store, 1, "retired")
        assert repair._ORIGINAL_PROPOSAL_EXISTS(worker) is False
        assert repair._proposal_exists_tail(worker) is False

        _insert_cohort(store, 2, "proposed")
        assert repair._ORIGINAL_PROPOSAL_EXISTS(worker) is True
        assert repair._proposal_exists_tail(worker) is True

        _insert_cohort(store, 3, "approved")
        assert repair._ORIGINAL_PROPOSAL_EXISTS(worker) is False
        assert repair._proposal_exists_tail(worker) is False
    finally:
        store.close()


def test_background_run_repeats_and_stops_without_spawning_status_work(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    stop = asyncio.Event()

    async def fake_cycle(_worker) -> None:
        nonlocal calls
        calls += 1
        if calls == 5:
            stop.set()

    monkeypatch.setattr(repair, "_background_cycle", fake_cycle)
    worker = SimpleNamespace(enabled=True, policy=SimpleNamespace(poll_interval_seconds=60.0))
    asyncio.run(repair._background_run(worker, stop))
    assert calls == 5


def test_repair_preserves_authority_thresholds_and_explicit_interfaces() -> None:
    state = repair.status()
    assert repair.STRATEGY_THRESHOLDS_CHANGED is False
    assert repair.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert repair.FORWARD_EVIDENCE_RULES_CHANGED is False
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert state["background_status_materialization"] is False
    assert state["proposal_selection_logic_unchanged"] is True
    assert state["discovery_screen_poll_side_effects_unchanged"] is True
    # Production composition legitimately wraps run_once later for independent
    # forward-evidence behavior. The invariant here is that this configurator itself
    # preserved the explicit interfaces at the moment it patched only run/proposal.
    assert state["explicit_run_once_unchanged"] is True
    assert state["public_status_unchanged"] is True
