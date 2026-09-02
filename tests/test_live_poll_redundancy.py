from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import target_stream_fanout as fanout
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.live_poll_redundancy import (
    POLL_CURSOR_MAX_PAGES,
    POLL_FALLBACK_STALE_SECONDS,
    POLL_INTERVAL_SECONDS,
    POLL_LIMIT,
    _fetch_delta,
    _poll_page,
    _record_poll_rows,
    _wrap_hydrate,
    _ws_target_covered,
)


def test_live_poll_uses_confirmed_slot_watermark(monkeypatch):
    captured: dict[str, object] = {}

    class Pool:
        async def call_with_meta(self, method, params, *, hedge):
            captured["method"] = method
            captured["params"] = params
            captured["hedge"] = hedge
            return [{"signature": "new", "slot": 124}], "publicnode", 12.0

    monkeypatch.setattr(live_poll, "_poll_rpc", lambda _self: Pool())
    plane = SimpleNamespace()
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    rows, provider, latency = asyncio.run(
        _poll_page(plane, target, before="before-sig", min_context_slot=123, limit=1000)
    )

    assert rows[0]["signature"] == "new"
    assert provider == "publicnode"
    assert latency == 12.0
    assert captured["method"] == "getSignaturesForAddress"
    assert captured["hedge"] is False
    config = captured["params"][1]
    assert config["limit"] == 1000
    assert config["before"] == "before-sig"
    assert config["minContextSlot"] == 123
    assert "until" not in config


def test_live_poll_delta_is_oldest_first_and_slot_bounded(monkeypatch):
    pages = [
        ([
            {"signature": "new-4", "slot": 104},
            {"signature": "new-3", "slot": 103},
            {"signature": "old-different-signature", "slot": 100},
        ], "publicnode", 10.0),
    ]

    async def fake_page(_self, _target, **kwargs):
        assert kwargs["min_context_slot"] == 100
        return pages.pop(0)

    monkeypatch.setattr("solana_roi.poll_watermark_repair._slot_poll_page", fake_page)
    rows, complete, provider, latency = asyncio.run(
        _fetch_delta(SimpleNamespace(), WatchTarget("program", "a", "RAYDIUM"), 100)
    )
    assert complete is True
    assert provider == "publicnode"
    assert latency == 10.0
    assert [row["signature"] for row in rows] == ["new-3", "new-4"]


def test_live_poll_only_records_rows_when_websocket_union_is_missing(monkeypatch):
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    key = fanout._target_key(target)

    monkeypatch.setattr(
        fanout,
        "_state_maps",
        lambda _self: (None, {"publicnode": {key}, "rpc-live-poll": {key}}, {}, {}),
    )
    assert _ws_target_covered(SimpleNamespace(), target) is True

    monkeypatch.setattr(
        fanout,
        "_state_maps",
        lambda _self: (None, {"publicnode": set(), "solana-mainnet": set(), "rpc-live-poll": {key}}, {}, {}),
    )
    assert _ws_target_covered(SimpleNamespace(), target) is False


def test_poll_only_program_receipt_is_background_launch_detection():
    captured: dict[str, object] = {}

    class Journal:
        def record_receipt(self, **kwargs):
            captured["receipt"] = kwargs
            return True

        def enqueue(self, **kwargs):
            captured["enqueue"] = kwargs

    plane = SimpleNamespace(journal=Journal())
    target = WatchTarget("program", "program-a", "PUMP_AMM")
    rows = [{"signature": "sig-a", "slot": 123, "err": None}]
    inserted = asyncio.run(_record_poll_rows(plane, target, rows))

    assert inserted == 1
    assert captured["receipt"]["source_key"] == "PUMP_AMM"
    assert captured["enqueue"]["signature"] == "sig-a"
    assert captured["enqueue"]["priority"] == 10
    assert captured["enqueue"]["reason"] == "live_poll_fallback"


def test_live_poll_nonlaunch_hydration_stays_lightweight(monkeypatch):
    swap = SimpleNamespace(
        wallet="unrelated-wallet",
        side="buy",
        source="solana-direct:PUMP_AMM:buy",
    )
    monkeypatch.setattr(direct_solana_module, "normalize_standard_transaction", lambda *args, **kwargs: swap)

    class Registry:
        @staticmethod
        def get(_wallet):
            return None

    class Service:
        registry = Registry()

        async def ingest_swap(self, _swap):
            raise AssertionError("non-launch poll fallback must not invoke deep analysis")

    class Journal:
        def __init__(self):
            self.finished = False
            self.metric = None

        def finish(self, _signature, **_kwargs):
            self.finished = True

        def record_hydration(self, **kwargs):
            self.metric = kwargs

    class Plane:
        service = Service()
        journal = Journal()

        async def _get_transaction_ready(self, _signature, *, hedge, attempts):
            assert hedge is False
            assert attempts == 3
            return {"meta": {"logMessages": ["Program log: Instruction: Buy"]}}, "publicnode", 10.0

        @staticmethod
        def _launch_like(_logs):
            return False

        def _persist_context_swap(self, value):
            self.persisted = value

        async def _prefill_launch_context(self, _value):
            raise AssertionError("non-launch fallback must not prefill launch context")

    async def original(_self, _row):
        raise AssertionError("live_poll_fallback must use dedicated hydration path")

    wrapped = _wrap_hydrate(original)
    plane = Plane()
    row = {
        "signature": "poll-sig",
        "trigger_received_at": datetime.now(timezone.utc).isoformat(),
        "source_hint": "PUMP_AMM",
        "priority": 10,
        "reason": "live_poll_fallback",
        "attempts": 0,
    }
    asyncio.run(wrapped(plane, row))
    assert plane.persisted is swap
    assert plane.journal.finished is True
    assert plane.journal.metric["historical_recovery"] is False


def test_live_poll_launch_keeps_deep_analysis(monkeypatch):
    swap = SimpleNamespace(
        wallet="unrelated-wallet",
        side="buy",
        source="solana-direct:PUMP_FUN:buy",
    )
    monkeypatch.setattr(direct_solana_module, "normalize_standard_transaction", lambda *args, **kwargs: swap)

    class Registry:
        @staticmethod
        def get(_wallet):
            return None

    class Service:
        registry = Registry()

        def __init__(self):
            self.ingested = None

        async def ingest_swap(self, value):
            self.ingested = value

    class Journal:
        def finish(self, _signature, **_kwargs):
            return None

        def record_hydration(self, **_kwargs):
            return None

    class Plane:
        service = Service()
        journal = Journal()

        async def _get_transaction_ready(self, _signature, *, hedge, attempts):
            return {"meta": {"logMessages": ["Program log: Instruction: Create"]}}, "publicnode", 10.0

        @staticmethod
        def _launch_like(_logs):
            return True

        async def _prefill_launch_context(self, value):
            self.prefilled = value
            return True

        def _persist_context_swap(self, _value):
            raise AssertionError("launch fallback must use deep path")

    async def original(_self, _row):
        raise AssertionError("live_poll_fallback must use dedicated hydration path")

    wrapped = _wrap_hydrate(original)
    plane = Plane()
    row = {
        "signature": "launch-sig",
        "trigger_received_at": datetime.now(timezone.utc).isoformat(),
        "source_hint": "PUMP_FUN",
        "priority": 10,
        "reason": "live_poll_fallback",
        "attempts": 0,
    }
    asyncio.run(wrapped(plane, row))
    assert plane.service.ingested is swap
    assert plane.prefilled is swap


def test_live_poll_guards_install_intrinsically():
    assert POLL_INTERVAL_SECONDS == 4.0
    assert POLL_LIMIT == 1000
    assert POLL_CURSOR_MAX_PAGES == 3
    assert POLL_FALLBACK_STALE_SECONDS == 30.0
    assert bool(getattr(DirectSolanaIngestionPlane._hydrate_one, "_roi_live_poll_hydrate", False))
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_live_poll_redundancy", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_live_poll_redundancy", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_target_quorum", False))
