from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import continuity_exact_durable_signature_repair as exact
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import raw_receipt_dispatch_repair as raw_dispatch
from solana_roi import scout_candidate_continuity_repair as scout
from solana_roi import websocket_frontier_provenance_repair as repair


SCOUT = "11111111111111111111111111111113"


def _notification(signature: str = "sig", slot: int = 444_000_000) -> dict[str, object]:
    return {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {"signature": signature, "err": None, "logs": []},
            },
        },
    }


def _install_nested_frontier_recorders(monkeypatch: pytest.MonkeyPatch) -> None:
    def durable_insert(_self, **_kwargs):
        return True

    monkeypatch.setattr(exact, "_ORIGINAL_RECORD_RECEIPT", durable_insert)
    monkeypatch.setattr(scout, "_ORIGINAL_RECORD_RECEIPT", exact._record_receipt_with_durable_frontier)


def _record_nested(journal: object, *, signature: str, source_key: str, slot: int) -> None:
    scout._record_receipt_with_scout_exact_frontier(
        journal,
        signature=signature,
        source_key=source_key,
        slot=slot,
        received_at=datetime.now(timezone.utc),
        launch_like=False,
    )


@pytest.mark.parametrize("source_key", [f"SCOUT:{SCOUT}", "PUMP_FUN", "PUMP_AMM"])
def test_real_websocket_provenance_crosses_to_thread_and_publishes_exact_frontier(
    monkeypatch: pytest.MonkeyPatch,
    source_key: str,
):
    _install_nested_frontier_recorders(monkeypatch)
    journal = SimpleNamespace()
    plane = SimpleNamespace()
    seen_context: list[object] = []

    async def original(_self, _provider, _targets, message):
        signature = str(message["params"]["result"]["value"]["signature"])
        slot = int(message["params"]["result"]["context"]["slot"])

        def persist() -> None:
            seen_context.append(raw_dispatch._RECEIPT_WALL_TIME.get())
            _record_nested(journal, signature=signature, source_key=source_key, slot=slot)

        await asyncio.to_thread(persist)

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", original)
    clean = raw_dispatch._RECEIPT_WALL_TIME.set(None)
    try:
        asyncio.run(
            repair._handle_with_websocket_provenance(
                plane,
                "publicnode",
                {1: SimpleNamespace(kind="scout", address=SCOUT)},
                _notification(signature=f"sig-{source_key}"),
            )
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(clean)

    assert len(seen_context) == 1
    assert isinstance(seen_context[0], datetime)
    frontier = exact._journal_frontiers(journal)[source_key]
    assert frontier["signature"] == f"sig-{source_key}"
    assert frontier["transport"] == "websocket"
    assert frontier["durable"] is True
    assert plane._roi_ws_frontier_provenance_contexts_bound == 1


def test_live_poll_cannot_gain_websocket_frontier_provenance(monkeypatch: pytest.MonkeyPatch):
    _install_nested_frontier_recorders(monkeypatch)
    journal = SimpleNamespace()
    plane = SimpleNamespace()
    seen_context: list[object] = []

    async def original(_self, _provider, _targets, _message):
        def persist() -> None:
            seen_context.append(raw_dispatch._RECEIPT_WALL_TIME.get())
            _record_nested(journal, signature="poll-sig", source_key=f"SCOUT:{SCOUT}", slot=444_000_001)

        await asyncio.to_thread(persist)

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", original)
    clean = raw_dispatch._RECEIPT_WALL_TIME.set(None)
    try:
        asyncio.run(
            repair._handle_with_websocket_provenance(
                plane,
                live_poll.POLL_PROVIDER_NAME,
                {1: SimpleNamespace(kind="scout", address=SCOUT)},
                _notification(signature="poll-sig", slot=444_000_001),
            )
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(clean)

    assert seen_context == [None]
    assert exact._journal_frontiers(journal) == {}
    assert plane._roi_ws_frontier_provenance_non_websocket_unbound == 1


def test_existing_socket_read_timestamp_is_preserved_across_offloop_handler(monkeypatch: pytest.MonkeyPatch):
    plane = SimpleNamespace()
    original_time = datetime(2026, 9, 4, 23, 0, tzinfo=timezone.utc)
    seen: list[object] = []

    async def original(_self, _provider, _targets, _message):
        seen.append(await asyncio.to_thread(raw_dispatch._RECEIPT_WALL_TIME.get))

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", original)
    token = raw_dispatch._RECEIPT_WALL_TIME.set(original_time)
    try:
        asyncio.run(
            repair._handle_with_websocket_provenance(
                plane,
                "publicnode",
                {1: SimpleNamespace(kind="scout", address=SCOUT)},
                _notification(),
            )
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(token)

    assert seen == [original_time]
    assert plane._roi_ws_frontier_provenance_existing_context_preserved == 1


def test_non_notification_does_not_gain_websocket_provenance(monkeypatch: pytest.MonkeyPatch):
    plane = SimpleNamespace()
    seen: list[object] = []

    async def original(_self, _provider, _targets, _message):
        seen.append(await asyncio.to_thread(raw_dispatch._RECEIPT_WALL_TIME.get))

    monkeypatch.setattr(repair, "_ORIGINAL_HANDLER", original)
    clean = raw_dispatch._RECEIPT_WALL_TIME.set(None)
    try:
        asyncio.run(
            repair._handle_with_websocket_provenance(
                plane,
                "publicnode",
                {1: SimpleNamespace(kind="scout", address=SCOUT)},
                {"method": "other"},
            )
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(clean)

    assert seen == [None]
    assert plane._roi_ws_frontier_provenance_non_websocket_unbound == 1


def test_safety_and_recovery_bounds_are_unchanged():
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
