from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.live_poll_redundancy import (
    POLL_CURSOR_MAX_PAGES,
    POLL_INTERVAL_SECONDS,
    POLL_LIMIT,
    _new_rows_until_cursor,
    _record_poll_rows,
    _wrap_hydrate,
)


def test_live_poll_cursor_returns_only_new_rows_oldest_first():
    pages = [
        [
            {"signature": "new-3"},
            {"signature": "new-2"},
            {"signature": "new-1"},
            {"signature": "cursor"},
            {"signature": "old"},
        ]
    ]
    rows, found = _new_rows_until_cursor(pages, "cursor")
    assert found is True
    assert [row["signature"] for row in rows] == ["new-1", "new-2", "new-3"]

    missing, found = _new_rows_until_cursor(pages, "not-present")
    assert found is False
    assert missing == []


def test_poll_only_program_receipt_is_queued_for_launch_detection():
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
    assert captured["enqueue"]["priority"] == 5
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
            assert attempts == 4
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
        "priority": 5,
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
        "priority": 5,
        "reason": "live_poll_fallback",
        "attempts": 0,
    }
    asyncio.run(wrapped(plane, row))
    assert plane.service.ingested is swap
    assert plane.prefilled is swap


def test_live_poll_guards_install_intrinsically():
    assert POLL_INTERVAL_SECONDS == 2.0
    assert POLL_LIMIT == 100
    assert POLL_CURSOR_MAX_PAGES == 3
    assert bool(getattr(DirectSolanaIngestionPlane._hydrate_one, "_roi_live_poll_hydrate", False))
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_live_poll_redundancy", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_live_poll_redundancy", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_target_quorum", False))
