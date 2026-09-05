from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import post182_production_proof_wiring_repair as repair
from solana_roi import raw_receipt_dispatch_repair as raw_dispatch
from solana_roi import scout_candidate_continuity_repair as scout
from solana_roi.observation_store import ObservationEventStore


def _plane(path):
    store = ObservationEventStore(path)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_recent_receipts ("
            "signature TEXT NOT NULL,source_key TEXT NOT NULL,slot INTEGER NOT NULL,received_at TEXT NOT NULL,"
            "launch_like INTEGER NOT NULL DEFAULT 0,expires_at TEXT,PRIMARY KEY(signature,source_key))"
        )
    return SimpleNamespace(store=store, journal=SimpleNamespace())


def test_actual_scout_economic_normalizer_schedules_unpriced_probe(monkeypatch) -> None:
    plane = SimpleNamespace()
    scheduled: list[str] = []

    def underlying(result, *, signature, trigger_received_at, wallet, source_hint=None):
        return None, "economic_movement_price_unresolved"

    monkeypatch.setattr(repair, "_ORIGINAL_SCOUT_NORMALIZER", underlying)
    monkeypatch.setattr(repair.proof, "_economic_unpriced_buy", lambda _plane, signature: {"signature": signature})
    monkeypatch.setattr(repair.proof, "_schedule_probe", lambda _plane, signature: scheduled.append(signature))
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        result = repair._normalizer_with_current_context_probe(
            {}, signature="sig-unpriced", trigger_received_at=datetime.now(timezone.utc), wallet="SCOUT"
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)

    assert result == (None, "economic_movement_price_unresolved")
    assert scheduled == ["sig-unpriced"]
    assert plane._roi_post183_wiring_unpriced_probe_candidates_seen == 1
    assert plane._roi_post183_wiring_probe_schedule_calls == 1


def test_parser_failure_without_durable_economic_row_does_not_schedule(monkeypatch) -> None:
    plane = SimpleNamespace()
    scheduled: list[str] = []

    def underlying(result, *, signature, trigger_received_at, wallet, source_hint=None):
        return None, "tracked_scout_token_delta_ambiguous"

    monkeypatch.setattr(repair, "_ORIGINAL_SCOUT_NORMALIZER", underlying)
    monkeypatch.setattr(repair.proof, "_economic_unpriced_buy", lambda _plane, signature: None)
    monkeypatch.setattr(repair.proof, "_schedule_probe", lambda _plane, signature: scheduled.append(signature))
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        repair._normalizer_with_current_context_probe(
            {}, signature="sig-not-economic", trigger_received_at=datetime.now(timezone.utc), wallet="SCOUT"
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)
    assert scheduled == []


def _notification(subscription: int, signature: str, slot: int) -> dict:
    return {
        "method": "logsNotification",
        "params": {
            "subscription": subscription,
            "result": {"context": {"slot": slot}, "value": {"signature": signature, "err": None, "logs": []}},
        },
    }


def test_real_pump_websocket_publishes_only_after_durable_sqlite_commit(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "frontier.sqlite3")
    target = SimpleNamespace(source_hint="PUMP_FUN")
    received_at = datetime.now(timezone.utc)

    async def durable_handler(self, provider, subscription_targets, message):
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO direct_solana_recent_receipts(signature,source_key,slot,received_at,launch_like,expires_at) "
                "VALUES (?,?,?,?,0,NULL)",
                ("sig-ws", "PUMP_FUN", 123, received_at.isoformat()),
            )

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", durable_handler)
    asyncio.run(
        repair._final_handler_with_verified_ws_frontier(
            plane, "publicnode", {7: target}, _notification(7, "sig-ws", 123)
        )
    )

    frontier = plane.journal._roi_exact_durable_ws_frontiers["PUMP_FUN"]
    assert frontier["signature"] == "sig-ws"
    assert frontier["slot"] == 123
    assert frontier["durable"] is True
    assert frontier["transport"] == "websocket"
    assert frontier["final_handler_verified"] is True
    assert plane._roi_post183_wiring_real_pump_ws_seen == 1
    assert plane._roi_post183_wiring_ws_frontier_published == 1
    assert raw_dispatch._RECEIPT_WALL_TIME.get() is None


def test_live_poll_can_never_publish_exact_websocket_frontier(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "poll.sqlite3")
    target = SimpleNamespace(source_hint="PUMP_AMM")

    async def handler(self, provider, subscription_targets, message):
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO direct_solana_recent_receipts(signature,source_key,slot,received_at,launch_like,expires_at) "
                "VALUES (?,?,?,?,0,NULL)",
                ("sig-poll", "PUMP_AMM", 321, datetime.now(timezone.utc).isoformat()),
            )

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", handler)
    asyncio.run(
        repair._final_handler_with_verified_ws_frontier(
            plane, "rpc-live-poll", {8: target}, _notification(8, "sig-poll", 321)
        )
    )
    assert not hasattr(plane.journal, "_roi_exact_durable_ws_frontiers")
    assert int(getattr(plane, "_roi_post183_wiring_real_pump_ws_seen", 0) or 0) == 0


def test_real_websocket_without_durable_row_fails_closed(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "missing.sqlite3")
    target = SimpleNamespace(source_hint="PUMP_AMM")

    async def no_durable_write(self, provider, subscription_targets, message):
        return None

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", no_durable_write)
    asyncio.run(
        repair._final_handler_with_verified_ws_frontier(
            plane, "solana-mainnet", {9: target}, _notification(9, "sig-missing", 456)
        )
    )
    assert int(getattr(plane, "_roi_post183_wiring_ws_frontier_published", 0) or 0) == 0
    assert plane._roi_post183_wiring_ws_durable_not_yet_committed == 1


def test_existing_socket_read_context_is_preserved(tmp_path, monkeypatch) -> None:
    plane = _plane(tmp_path / "context.sqlite3")
    target = SimpleNamespace(source_hint="PUMP_FUN")
    original_time = datetime(2026, 9, 5, 20, 0, tzinfo=timezone.utc)

    async def handler(self, provider, subscription_targets, message):
        assert raw_dispatch._RECEIPT_WALL_TIME.get() == original_time
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO direct_solana_recent_receipts(signature,source_key,slot,received_at,launch_like,expires_at) "
                "VALUES (?,?,?,?,0,NULL)",
                ("sig-context", "PUMP_FUN", 999, original_time.isoformat()),
            )

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", handler)
    token = raw_dispatch._RECEIPT_WALL_TIME.set(original_time)
    try:
        asyncio.run(
            repair._final_handler_with_verified_ws_frontier(
                plane, "publicnode", {10: target}, _notification(10, "sig-context", 999)
            )
        )
        assert raw_dispatch._RECEIPT_WALL_TIME.get() == original_time
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(token)
    assert plane._roi_post183_wiring_ws_context_preserved == 1
