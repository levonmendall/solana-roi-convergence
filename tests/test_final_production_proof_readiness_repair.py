from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import final_production_proof_readiness_repair as repair
from solana_roi.ingestion import NormalizedSwap
from solana_roi.observation_store import ObservationEventStore


def _plane(path):
    return SimpleNamespace(
        store=ObservationEventStore(path),
        service=None,
        journal=SimpleNamespace(),
    )


def _schemas(store):
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS scout_economic_movement_observations ("
            "signature TEXT PRIMARY KEY,wallet TEXT NOT NULL,token_mint TEXT NOT NULL,side TEXT NOT NULL,"
            "token_amount REAL NOT NULL,native_amount_sol REAL,observed_at TEXT NOT NULL,received_at TEXT NOT NULL,"
            "original_source_hint TEXT,source_mode TEXT NOT NULL,direct_venue TEXT,venue_resolution_state TEXT NOT NULL,"
            "entry_authority INTEGER NOT NULL DEFAULT 0,architecture_version TEXT NOT NULL,created_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS normalized_swaps ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,signature TEXT NOT NULL,slot INTEGER NOT NULL,observed_at TEXT NOT NULL,"
            "received_at TEXT NOT NULL,wallet TEXT NOT NULL,token_mint TEXT NOT NULL,side TEXT NOT NULL,token_amount REAL NOT NULL,"
            "native_amount_sol REAL NOT NULL,reference_price_sol REAL NOT NULL,ingestion_latency_ms REAL NOT NULL DEFAULT 0,source TEXT NOT NULL)"
        )


def _unpriced(store, signature="sig-unpriced", token="TOKEN"):
    now = datetime.now(timezone.utc)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO scout_economic_movement_observations("
            "signature,wallet,token_mint,side,token_amount,native_amount_sol,observed_at,received_at,original_source_hint,"
            "source_mode,direct_venue,venue_resolution_state,entry_authority,architecture_version,created_at) "
            "VALUES (?,?,?,?,?,NULL,?,?,?,?,NULL,'router_or_unknown_venue',0,'test',?)",
            (signature,"SCOUT",token,"buy",100.0,now.isoformat(),now.isoformat(),None,"owner_token_delta",now.isoformat()),
        )


def _reference(store, *, source, price=0.01, age=0.0):
    now = datetime.now(timezone.utc) - timedelta(seconds=age)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO normalized_swaps(signature,slot,observed_at,received_at,wallet,token_mint,side,token_amount,"
            "native_amount_sol,reference_price_sol,ingestion_latency_ms,source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("ref-" + source,1,now.isoformat(),now.isoformat(),"W","TOKEN","buy",100.0,1.0,price,0.0,source),
        )


def test_public_robinhood_status_removes_obsolete_catchup_terms() -> None:
    payload = repair._sanitize_public_robinhood(
        {
            "caught_up_for_paper_decisions": False,
            "catchup_capacity": {"catchup_mode": False},
            "historical_readiness": "old",
            "blockers": ["robinhood_not_caught_up_for_paper_decisions", "other"],
            "forward_frontier_ready": True,
            "nested": {"processing_backlog_requires_catchup": False, "kept": 1},
        }
    )
    assert "caught_up_for_paper_decisions" not in payload
    assert "catchup_capacity" not in payload
    assert "historical_readiness" not in payload
    assert payload["blockers"] == ["robinhood_forward_frontier_not_ready", "other"]
    assert payload["forward_frontier_ready"] is True
    assert payload["nested"] == {"kept": 1}


def test_current_reference_requires_recent_direct_venue(tmp_path) -> None:
    plane = _plane(tmp_path / "reference.sqlite3")
    _schemas(plane.store)
    _reference(plane.store, source="solana-direct:ROUTER_OR_UNKNOWN:buy", age=1)
    _reference(plane.store, source="solana-direct:PUMP_FUN:buy", price=0.02, age=2)
    reference = repair._current_direct_reference(plane, "TOKEN")
    assert reference is not None
    assert reference["venue"] == "PUMP_FUN"
    assert reference["price"] == 0.02

    stale = _plane(tmp_path / "stale.sqlite3")
    _schemas(stale.store)
    _reference(stale.store, source="solana-direct:PUMP_AMM:buy", age=repair.CURRENT_REFERENCE_MAX_AGE_SECONDS + 5)
    assert repair._current_direct_reference(stale, "TOKEN") is None


def test_current_context_probe_is_zero_allocation_and_preserves_original_price_truth(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "probe.sqlite3")
    _schemas(plane.store)
    _unpriced(plane.store)
    _reference(plane.store, source="solana-direct:PUMP_AMM:buy", price=0.01)

    async def fresh_risk(_plane, _token):
        return True, True

    class Adapter:
        async def _execution(self, row, fraction):
            assert row["wallet_price_sol"] == 0.01
            assert fraction == repair.PROBE_FRACTION
            return {"entry_price_sol": 0.0102, "exit_net_sol": 0.0048, "round_trip_cost_fraction": 0.04}

    class Service:
        async def _quote(self, **kwargs):
            assert kwargs["stage"] == repair.PROBE_STAGE
            assert kwargs["fraction"] == repair.PROBE_FRACTION
            return SimpleNamespace(usable=True)

    plane.service = Service()
    monkeypatch.setattr(repair, "_fresh_risk", fresh_risk)
    monkeypatch.setattr(repair.candidate_v4, "_attached_discovery", lambda _plane: object())
    monkeypatch.setattr(repair, "_adapter", lambda _discovery: Adapter())
    asyncio.run(repair._probe_current_context(plane, "sig-unpriced"))

    with plane.store._lock:
        original = plane.store.db.execute(
            "SELECT native_amount_sol FROM scout_economic_movement_observations WHERE signature='sig-unpriced'"
        ).fetchone()
        audit = plane.store.db.execute(
            "SELECT zero_allocation,state,risk_complete,risk_fresh,canonical_quote_attempted,canonical_quote_usable,"
            "exact_entry_executable,exact_exit_executable FROM economic_current_context_probe_audit WHERE signature='sig-unpriced'"
        ).fetchone()
    assert original["native_amount_sol"] is None
    assert audit["zero_allocation"] == 1
    assert audit["state"] == "current_context_execution_proved"
    assert audit["risk_complete"] == 1 and audit["risk_fresh"] == 1
    assert audit["canonical_quote_attempted"] == 1 and audit["canonical_quote_usable"] == 1
    assert audit["exact_entry_executable"] == 1 and audit["exact_exit_executable"] == 1


def test_candidate_quote_reuse_excludes_zero_allocation_probe_rows(tmp_path) -> None:
    plane = _plane(tmp_path / "quote.sqlite3")
    now = datetime.now(timezone.utc)
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "CREATE TABLE execution_quote_observations (id INTEGER PRIMARY KEY AUTOINCREMENT,token_mint TEXT,stage TEXT,"
            "effective_price_sol REAL,scout_reference_price_sol REAL,drift_fraction REAL,received_at TEXT,chain_to_quote_ms REAL,"
            "usable INTEGER,reason TEXT)"
        )
        plane.store.db.execute(
            "INSERT INTO execution_quote_observations(token_mint,stage,effective_price_sol,scout_reference_price_sol,"
            "drift_fraction,received_at,chain_to_quote_ms,usable,reason) VALUES (?,?,?,?,?,?,?,?,?)",
            ("TOKEN",repair.PROBE_STAGE,0.01,0.01,0.0,now.isoformat(),100.0,1,"proof"),
        )
    swap = NormalizedSwap(
        signature="candidate",slot=1,observed_at=now,received_at=now-timedelta(seconds=1),wallet="SCOUT",
        token_mint="TOKEN",side="buy",token_amount=100.0,native_amount_sol=1.0,reference_price_sol=0.01,
        source="solana-direct:PUMP_AMM:buy",
    )
    assert repair._matching_existing_quote_without_probe_rows(plane, swap) is None
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "INSERT INTO execution_quote_observations(token_mint,stage,effective_price_sol,scout_reference_price_sol,"
            "drift_fraction,received_at,chain_to_quote_ms,usable,reason) VALUES (?,?,?,?,?,?,?,?,?)",
            ("TOKEN","v4_forward_probe",0.01,0.01,0.0,now.isoformat(),100.0,1,"candidate"),
        )
    result = repair._matching_existing_quote_without_probe_rows(plane, swap)
    assert result is not None and result["stage"] == "v4_forward_probe"


def test_proof_helpers_report_pump_venues_fomo_and_exact_frontiers(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "surface.sqlite3")
    _schemas(plane.store)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(repair, "_INSTALLED_AT", now - timedelta(seconds=10))
    _reference(plane.store, source="solana-direct:PUMP_FUN:buy")
    _reference(plane.store, source="solana-direct:PUMP_AMM:sell")
    with plane.store._lock, plane.store.db:
        plane.store.db.execute("CREATE TABLE independent_fomo_runtime(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)")
        plane.store.db.execute(
            "INSERT INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",
            ("diag:source_row_counts",'{"solana-direct:PUMP_FUN:buy":2,"solana-direct:PUMP_AMM:buy":3}',now.isoformat()),
        )
    plane.journal._roi_exact_durable_ws_frontiers = {
        "PUMP_FUN": {"durable": True}, "PUMP_AMM": {"durable": True}, "SCOUT:x": {"durable": True}
    }
    venues = repair._recent_venue_counts(plane)
    assert venues["PUMP_FUN"]["total"] == 1
    assert venues["PUMP_AMM"]["total"] == 1
    assert repair._fomo_counts(plane)["solana-direct:PUMP_FUN:buy"] == 2
    frontiers = repair._frontiers(plane)
    assert frontiers["pump_fun"] is True and frontiers["pump_amm"] is True and frontiers["count"] == 2
