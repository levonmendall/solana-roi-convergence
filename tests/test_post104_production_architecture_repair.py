from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import post104_production_architecture_repair as repair


class _Store:
    def __init__(self):
        import threading

        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE direct_solana_recent_receipts("
            "signature TEXT, source_key TEXT, slot INTEGER, received_at TEXT, launch_like INTEGER, expires_at TEXT, "
            "UNIQUE(signature,source_key))"
        )


class _Journal:
    pass


class _Plane:
    def __init__(self):
        self.store = _Store()
        self.journal = _Journal()


def test_final_batch_durable_publication_requires_verified_row():
    plane = _Plane()
    now = datetime.now(timezone.utc)
    rows = [
        {
            "signature": "sig-pump",
            "source_key": "PUMP_FUN",
            "slot": 123,
            "received_at": now,
            "provider": "publicnode",
            "target_kind": "program",
        }
    ]

    repair._publish_exact_durable_frontier(plane, rows)
    assert repair.exact._journal_frontiers(plane.journal) == {}

    plane.store.db.execute(
        "INSERT INTO direct_solana_recent_receipts VALUES (?,?,?,?,?,?)",
        ("sig-pump", "PUMP_FUN", 123, now.isoformat(), 0, now.isoformat()),
    )
    plane.store.db.commit()
    repair._publish_exact_durable_frontier(plane, rows)
    frontier = repair.exact._journal_frontiers(plane.journal)["PUMP_FUN"]
    assert frontier["signature"] == "sig-pump"
    assert frontier["slot"] == 123
    assert frontier["durable"] is True
    assert frontier["transport"] == "websocket"
    assert frontier["final_batch_commit"] is True


def test_live_poll_never_becomes_exact_websocket_frontier(monkeypatch):
    target = SimpleNamespace(kind="program", address="pump")
    now = datetime.now(timezone.utc)
    message = {
        "params": {"result": {"context": {"slot": 10}, "value": {"signature": "s", "err": None}}}
    }
    item = (10, 1.0, 1, now, "rpc-live-poll", {}, message)
    monkeypatch.setattr(
        repair.capacity,
        "_dispatch_fields",
        lambda _item: (target, 10, "s", False, "PUMP_FUN"),
    )
    assert repair._actual_ws_high_volume_rows([item]) == []


def test_missing_scout_attestation_uses_only_mint_specific_context(monkeypatch):
    trigger = datetime.now(timezone.utc)
    candidate = SimpleNamespace(
        observed_at=trigger,
        token_mint="MintCandidate",
        source="PUMP_FUN",
        side="buy",
        wallet="Scout",
    )
    launch = SimpleNamespace()

    async def created_at(_mint):
        return trigger - timedelta(seconds=9)

    launch._created_at = created_at
    raw = SimpleNamespace(launch=launch)
    plane = SimpleNamespace(service=SimpleNamespace(collectors=SimpleNamespace(inner=raw)))
    called = {}

    async def original(_self, _candidate):
        return False

    async def hydrate(_self, *, mint, source, launch_signature, created_at):
        called.update(
            mint=mint,
            source=source,
            launch_signature=launch_signature,
            created_at=created_at,
        )
        return 4, True, 4

    monkeypatch.setattr(repair, "_ORIGINAL_PREFILL", original)
    monkeypatch.setattr(repair.candidate_hotpath, "_is_frozen_scout_buy", lambda *_: True)
    monkeypatch.setattr(repair.launch_bridge, "_raw_collectors", lambda _self: raw)
    monkeypatch.setattr(repair.launch_bridge, "_seed_launch_created_at", lambda *_: True)
    monkeypatch.setattr(repair.launch_bridge, "_hydrate_mint_launch_context", hydrate)

    assert asyncio.run(repair._candidate_targeted_context_prefill(plane, candidate)) is True
    assert called["mint"] == "MintCandidate"
    assert called["source"] == "PUMP_FUN"
    assert called["launch_signature"] == ""


def test_risk_accounting_names_missing_dimensions(monkeypatch):
    now = datetime.now(timezone.utc)
    current_swap = SimpleNamespace(observed_at=now, received_at=now)
    readiness = {
        "complete": False,
        "fresh": False,
        "present": {
            "authority": True,
            "liquidity": True,
            "launch": True,
            "flow": False,
            "funding": False,
            "deployer": True,
        },
        "fresh_dimensions": {
            "authority": True,
            "liquidity": True,
            "launch": True,
            "flow": False,
            "funding": False,
            "deployer": True,
        },
    }
    obj = SimpleNamespace(
        risk=SimpleNamespace(readiness=lambda *_args, **_kwargs: readiness),
        now_fn=lambda: now,
        _eligible_candidate=lambda _swap: True,
    )

    async def original(*_args, **_kwargs):
        return None

    monkeypatch.setattr(repair, "_ORIGINAL_RISK_REFRESH", original)
    asyncio.run(
        repair._risk_refresh_with_dimension_accounting(
            obj, "MintRisk", now, current_swap=current_swap
        )
    )
    last = obj._roi_post104_risk_last_readiness
    assert last["missing_dimensions"] == ["flow", "funding"]
    assert obj._roi_post104_risk_missing_dimensions["flow"] == 1
    assert obj._roi_post104_risk_missing_dimensions["funding"] == 1


def test_production_composition_installs_post104_architecture_repair():
    from pathlib import Path

    source = Path("src/solana_roi/production.py").read_text()
    assert "install_post104_production_architecture_repair" in source
    assert "uvicorn solana_roi.production:app" in Path("render.yaml").read_text()
