from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import e2e_production_hardening_repair as repair


def _scout_row(*, age_seconds: float) -> dict[str, object]:
    return {
        "signature": "sig-1",
        "priority": 0,
        "reason": "frozen_scout_processed_trigger",
        "trigger_received_at": (
            datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        ).isoformat(),
    }


def test_candidate_retry_claim_limit_cannot_end_open_entry_window(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def finish(_self, signature, *, error=None, retry=False):
        calls.append({"signature": signature, "error": error, "retry": retry})

    monkeypatch.setattr(repair, "_ORIGINAL_JOURNAL_FINISH", finish)
    token = repair._CURRENT_SCOUT_ROW.set(_scout_row(age_seconds=2.0))
    journal = SimpleNamespace()
    try:
        repair._journal_finish_with_absolute_candidate_deadline(
            journal,
            "sig-1",
            error="confirmed transaction not yet available",
            retry=False,
        )
    finally:
        repair._CURRENT_SCOUT_ROW.reset(token)

    assert calls == [
        {
            "signature": "sig-1",
            "error": "confirmed transaction not yet available",
            "retry": True,
        }
    ]
    assert repair.ENTRY_WINDOW_SECONDS == 20.0


def test_candidate_failure_remains_terminal_after_absolute_entry_window(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def finish(_self, signature, *, error=None, retry=False):
        calls.append({"signature": signature, "error": error, "retry": retry})

    monkeypatch.setattr(repair, "_ORIGINAL_JOURNAL_FINISH", finish)
    token = repair._CURRENT_SCOUT_ROW.set(_scout_row(age_seconds=25.0))
    journal = SimpleNamespace()
    try:
        repair._journal_finish_with_absolute_candidate_deadline(
            journal,
            "sig-1",
            error="confirmed transaction not yet available",
            retry=False,
        )
    finally:
        repair._CURRENT_SCOUT_ROW.reset(token)

    assert calls[0]["retry"] is False


def test_funding_transaction_singleflight_collapses_duplicate_rpc_work(monkeypatch) -> None:
    calls = 0

    async def underlying(_self, signature):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {"signature": signature}

    monkeypatch.setattr(repair, "_ORIGINAL_FUNDING_TX_CACHED", underlying)
    collector = SimpleNamespace()

    async def scenario():
        return await asyncio.gather(
            repair._funding_transaction_singleflight(collector, "same-signature"),
            repair._funding_transaction_singleflight(collector, "same-signature"),
        )

    values = asyncio.run(scenario())
    assert calls == 1
    assert values == [
        {"signature": "same-signature"},
        {"signature": "same-signature"},
    ]
    assert getattr(collector, "_roi_e2e_hardening_funding_tx_singleflight_joins") == 1
    assert getattr(collector, "_roi_e2e_funding_tx_inflight") == {}


def test_funding_signature_singleflight_collapses_duplicate_history_pages(monkeypatch) -> None:
    calls = 0

    async def underlying(_self, wallet, *, before):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return [{"wallet": wallet, "before": before}]

    monkeypatch.setattr(repair, "_ORIGINAL_SIGNATURE_PAGE", underlying)
    collector = SimpleNamespace()

    async def scenario():
        return await asyncio.gather(
            repair._funding_signature_page_singleflight(collector, "wallet", before="cursor"),
            repair._funding_signature_page_singleflight(collector, "wallet", before="cursor"),
        )

    values = asyncio.run(scenario())
    assert calls == 1
    assert values[0] == values[1]
    assert getattr(collector, "_roi_e2e_hardening_funding_signature_singleflight_joins") == 1
    assert getattr(collector, "_roi_e2e_funding_signature_inflight") == {}


def test_frontier_failure_reasons_are_explicit(monkeypatch) -> None:
    store = SimpleNamespace()

    def underlying(_store, *, signature, created_at, max_age_seconds):
        assert signature == "launch-sig"
        assert max_age_seconds == 3.0
        return None, "missing_recent_preexisting_websocket_frontier"

    monkeypatch.setattr(repair, "_ORIGINAL_FRONTIER_LAG", underlying)
    value = repair._frontier_lag_with_reason_accounting(
        store,
        signature="launch-sig",
        created_at=datetime.now(timezone.utc),
        max_age_seconds=3.0,
    )
    assert value == (None, "missing_recent_preexisting_websocket_frontier")
    assert store._roi_e2e_frontier_reason_counts == {
        "missing_recent_preexisting_websocket_frontier": 1
    }


def test_robinhood_supervisor_metadata_preserves_paper_only_boundary(monkeypatch) -> None:
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_ROBINHOOD_METADATA",
        lambda **_kwargs: {
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
    )
    monkeypatch.setitem(repair.robinhood_runtime._STATE, "supervisor_restart_count", 3)
    monkeypatch.setitem(repair.robinhood_runtime._STATE, "supervisor_generation", 4)

    payload = repair._robinhood_supervision_metadata(store_path="/tmp/rh.sqlite3")
    assert payload["supervised_restart_enabled"] is True
    assert payload["restart_count"] == 3
    assert payload["worker_generation"] == 4
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False


def test_repair_manifest_preserves_all_frozen_boundaries() -> None:
    payload = repair.status()
    assert payload["candidate_absolute_entry_deadline_seconds"] == 20.0
    assert payload["strategy_thresholds_changed"] is False
    assert payload["certification_thresholds_changed"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
