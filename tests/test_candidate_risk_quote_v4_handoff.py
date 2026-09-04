from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_risk_quote_v4_handoff as bridge
from solana_roi.observation_store import ObservationEventStore


def _forward_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, "
            "observed_at TEXT NOT NULL, received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL, "
            "copyable_price_sol REAL, chase_fraction REAL, copyable INTEGER NOT NULL, "
            "observation_lag_ms REAL NOT NULL, risk_complete INTEGER NOT NULL, "
            "manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, source TEXT NOT NULL)"
        )


def _seed_swap(
    store: ObservationEventStore,
    *,
    signature: str,
    side: str = "buy",
    observed_at: datetime | None = None,
    received_at: datetime | None = None,
) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    observed = observed_at or (now - timedelta(seconds=1))
    received = received_at or now
    assert store.record_swap(
        signature=signature,
        slot=123,
        observed_at=observed.isoformat(),
        received_at=received.isoformat(),
        wallet="ScoutWallet111111111111111111111111111111",
        token_mint="Mint1111111111111111111111111111111111111",
        side=side,
        token_amount=1000.0,
        native_amount_sol=0.25,
        reference_price_sol=0.00025,
        ingestion_latency_ms=max(0.0, (received - observed).total_seconds() * 1000.0),
        source="direct-solana:PUMP_FUN:swap",
    )
    return observed, received


class _Risk:
    def __init__(self, *, complete: bool = True, fresh: bool = True) -> None:
        self.complete = complete
        self.fresh = fresh
        self.snapshot_calls = 0

    def readiness(self, token_mint: str, *, as_of: datetime):
        return {
            "token_mint": token_mint,
            "complete": self.complete,
            "fresh": self.fresh,
            "fresh_dimensions": {
                "authority": self.fresh,
                "liquidity": self.fresh,
                "launch": self.fresh,
                "flow": self.fresh,
                "funding": self.fresh,
                "deployer": self.fresh,
            },
        }

    async def snapshot(self, *args, **kwargs):
        self.snapshot_calls += 1
        if not self.complete or not self.fresh:
            return None
        return SimpleNamespace(blockers=())


class _QuoteHandoff:
    def __init__(self, *, usable: bool = True) -> None:
        self.calls = 0
        self.usable = usable

    async def observe(self, **kwargs):
        self.calls += 1
        return SimpleNamespace(
            effective_price_sol=0.000252,
            drift_fraction=0.008,
            chain_to_quote_ms=1500.0,
            usable=self.usable,
            received_at=datetime.now(timezone.utc),
            reason="within chase ceiling" if self.usable else "unusable",
        )


class _Resolver:
    def entity_id_for(self, wallet: str, *, fallback_entity_id, as_of: datetime):
        return fallback_entity_id or f"entity:{wallet}"

    def component(self, wallet: str, *, as_of: datetime):
        return {wallet}


class _Registry:
    def get(self, wallet: str):
        return SimpleNamespace(entity_id=f"entity:{wallet}")


def _plane(store: ObservationEventStore, *, risk: _Risk, quote: _QuoteHandoff):
    resolver = _Resolver()
    discovery = SimpleNamespace(store=store, entity_resolver=resolver)
    service = SimpleNamespace(
        risk_provider=risk,
        quote_handoff=quote,
        registry=_Registry(),
        entity_resolver=resolver,
    )
    plane = SimpleNamespace(store=store, service=service)
    bridge.attach_candidate_v4_wallet_discovery(plane, discovery)
    return plane


def test_complete_direct_candidate_captures_quote_and_enters_v4_forward_lane(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "candidate-v4.sqlite3")
    _forward_schema(store)
    signature = "candidate-buy-complete"
    _seed_swap(store, signature=signature)
    risk = _Risk(complete=True, fresh=True)
    quote = _QuoteHandoff(usable=True)
    plane = _plane(store, risk=risk, quote=quote)
    scheduled: list[str] = []
    monkeypatch.setattr(bridge, "_schedule_v4", lambda discovery, sig: scheduled.append(sig))

    asyncio.run(bridge._process_candidate_handoff(plane, signature))

    with store._lock:
        row = store.db.execute(
            "SELECT signature,copyable,risk_complete,source,chase_fraction,observation_lag_ms "
            "FROM wallet_discovery_forward_observations WHERE signature=?",
            (signature,),
        ).fetchone()
    assert row is not None
    assert bool(row["copyable"]) is True
    assert bool(row["risk_complete"]) is True
    assert str(row["source"]).startswith("direct-candidate-v4:")
    assert float(row["chase_fraction"]) <= 0.15
    assert float(row["observation_lag_ms"]) <= 20_000.0
    assert quote.calls == 1
    assert risk.snapshot_calls == 1
    assert scheduled == [signature]
    assert int(getattr(plane, "_roi_candidate_v4_handoff_quote_usable", 0)) == 1
    assert int(getattr(plane, "_roi_candidate_v4_handoff_v4_scheduled", 0)) == 1


def test_incomplete_risk_fails_closed_before_quote_or_v4(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "candidate-v4-incomplete.sqlite3")
    _forward_schema(store)
    signature = "candidate-buy-risk-incomplete"
    _seed_swap(store, signature=signature)
    risk = _Risk(complete=False, fresh=False)
    quote = _QuoteHandoff(usable=True)
    plane = _plane(store, risk=risk, quote=quote)
    scheduled: list[str] = []
    monkeypatch.setattr(bridge, "_schedule_v4", lambda discovery, sig: scheduled.append(sig))

    asyncio.run(bridge._process_candidate_handoff(plane, signature))

    with store._lock:
        row = store.db.execute(
            "SELECT copyable,risk_complete FROM wallet_discovery_forward_observations WHERE signature=?",
            (signature,),
        ).fetchone()
    assert row is not None
    assert bool(row["copyable"]) is False
    assert bool(row["risk_complete"]) is False
    assert quote.calls == 0
    assert risk.snapshot_calls == 0
    assert scheduled == []
    assert getattr(plane, "_roi_candidate_v4_handoff_last_blocker") == "six_dimension_risk_incomplete_or_stale"


def test_unusable_quote_is_forward_rejection_not_paper_authority(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "candidate-v4-unusable.sqlite3")
    _forward_schema(store)
    signature = "candidate-buy-quote-unusable"
    _seed_swap(store, signature=signature)
    risk = _Risk(complete=True, fresh=True)
    quote = _QuoteHandoff(usable=False)
    plane = _plane(store, risk=risk, quote=quote)
    scheduled: list[str] = []
    monkeypatch.setattr(bridge, "_schedule_v4", lambda discovery, sig: scheduled.append(sig))

    asyncio.run(bridge._process_candidate_handoff(plane, signature))

    with store._lock:
        row = store.db.execute(
            "SELECT copyable,risk_complete FROM wallet_discovery_forward_observations WHERE signature=?",
            (signature,),
        ).fetchone()
    assert row is not None
    assert bool(row["copyable"]) is False
    assert bool(row["risk_complete"]) is True
    assert quote.calls == 1
    assert scheduled == [signature]
    assert getattr(plane, "_roi_candidate_v4_handoff_last_blocker") == "canonical_quote_unusable_or_outside_entry_window"


def test_direct_scout_sell_is_forwarded_for_v4_exit_research_without_quote(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "candidate-v4-sell.sqlite3")
    _forward_schema(store)
    signature = "candidate-sell"
    _seed_swap(store, signature=signature, side="sell")
    risk = _Risk(complete=False, fresh=False)
    quote = _QuoteHandoff(usable=True)
    plane = _plane(store, risk=risk, quote=quote)
    scheduled: list[str] = []
    monkeypatch.setattr(bridge, "_schedule_v4", lambda discovery, sig: scheduled.append(sig))

    asyncio.run(bridge._process_candidate_handoff(plane, signature))

    with store._lock:
        row = store.db.execute(
            "SELECT side,copyable,source FROM wallet_discovery_forward_observations WHERE signature=?",
            (signature,),
        ).fetchone()
    assert row is not None
    assert str(row["side"]) == "sell"
    assert bool(row["copyable"]) is False
    assert str(row["source"]).startswith("direct-candidate-v4:")
    assert quote.calls == 0
    assert scheduled == [signature]


def test_bridge_constants_preserve_strategy_and_authority_boundaries():
    assert bridge.ENTRY_WINDOW_SECONDS == 20.0
    assert bridge.MAX_CHASE_FRACTION == 0.15
    assert bridge.SCOUT_REASONS == frozenset(
        {"frozen_scout_processed_trigger", "frozen_scout_live_poll_trigger"}
    )
