from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import raw_receipt_dispatch_repair as raw_dispatch
from solana_roi import web_liveness_isolation_repair as repair
from solana_roi.direct_solana import DirectSolanaIngestionPlane


def test_full_scope_batch_runs_through_asyncio_worker_thread(monkeypatch):
    calls: list[tuple[object, object, list[object]]] = []

    def persist(plane, items):
        calls.append((persist, plane, items))
        return len(items)

    async def fake_to_thread(function, *args):
        calls.append((fake_to_thread, function, list(args)))
        return function(*args)

    monkeypatch.setattr(repair.full_scope, "_persist_full_scope_batch", persist)
    monkeypatch.setattr(repair.asyncio, "to_thread", fake_to_thread)

    plane = SimpleNamespace()
    items = [object(), object(), object()]
    inserted = asyncio.run(repair._persist_full_scope_batch_off_loop(plane, items))

    assert inserted == 3
    assert calls[0][0] is fake_to_thread
    assert calls[0][1] is persist
    assert calls[0][2] == [plane, items]
    assert calls[1] == (persist, plane, items)
    assert plane._roi_web_liveness_sqlite_batch_offloads == 1


def test_pr73_risk_row_adapter_reconstructs_the_original_inline_swap():
    observed = datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)
    received = datetime(2026, 9, 3, 16, 0, 1, tzinfo=timezone.utc)
    row = {
        "signature": "sig-a",
        "wallet": "wallet-a",
        "token_mint": "mint-a",
        "side": "buy",
        "token_amount": 12.5,
        "observed_at": observed.isoformat(),
        "received_at": received.isoformat(),
        "wallet_price_sol": 0.4,
        "source": "wallet-realtime:test",
    }

    swap = repair._risk_swap_from_row(row)

    assert swap.signature == "sig-a"
    assert swap.wallet == "wallet-a"
    assert swap.token_mint == "mint-a"
    assert swap.side == "buy"
    assert swap.token_amount == 12.5
    assert swap.native_amount_sol == 5.0
    assert swap.reference_price_sol == 0.4
    assert swap.observed_at == observed
    assert swap.received_at == received
    assert swap.source == "wallet-realtime:test"


def test_same_release_sqlite_lock_is_retried_but_other_startup_errors_fail_fast(monkeypatch):
    calls = 0
    sleeps: list[float] = []

    def locked_then_ready():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise sqlite3.OperationalError("database is locked")
        return "runtime"

    monkeypatch.setattr(repair.time, "sleep", lambda delay: sleeps.append(delay))
    wrapped = repair._restart_safe_build_runtime(locked_then_ready)

    assert wrapped() == "runtime"
    assert calls == 3
    assert sleeps == list(repair.STARTUP_SQLITE_RETRY_DELAYS_SECONDS[:2])

    non_lock_calls = 0

    def corrupt_schema():
        nonlocal non_lock_calls
        non_lock_calls += 1
        raise sqlite3.OperationalError("malformed database schema")

    with pytest.raises(sqlite3.OperationalError, match="malformed database schema"):
        repair._restart_safe_build_runtime(corrupt_schema)()
    assert non_lock_calls == 1


def test_install_replaces_full_scope_dispatch_worker_and_marks_status():
    repair.install_web_liveness_isolation()

    assert raw_dispatch._dispatch_worker is repair._web_safe_full_scope_dispatch_worker
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_web_liveness_isolation", False))
    assert repair.wallet_priority._risk_swap is repair._risk_swap_from_row
    assert bool(getattr(repair.runtime_module.build_runtime, "_roi_render_restart_sqlite_retry", False))


def test_status_preserves_full_scope_batch_and_all_safety_boundaries():
    wrapped = repair._status_with_web_liveness_isolation(
        lambda _self: {
            "raw_receipt_dispatch": {
                "full_scope_set_based_writer": True,
                "critical_receipts_batched": True,
                "critical_per_receipt_commits": False,
            }
        }
    )
    plane = SimpleNamespace()

    payload = wrapped(plane)
    status = payload["web_liveness_isolation"]
    dispatch = payload["raw_receipt_dispatch"]

    assert status["full_scope_sqlite_batch_off_event_loop"] is True
    assert status["sqlite_process_lock_preserved"] is True
    assert status["sqlite_wal_preserved"] is True
    assert status["sqlite_synchronous_full_preserved"] is True
    assert status["full_scope_batch_semantics_preserved"] is True
    assert status["receipt_order_preserved"] is True
    assert status["hydration_enqueue_semantics_preserved"] is True
    assert status["drops_allowed"] is False
    assert status["strategy_scope_reduced"] is False
    assert status["certification_thresholds_unchanged"] is True
    assert status["wallet_risk_row_adapter_installed"] is True
    assert status["same_release_sqlite_restart_retry_installed"] is True
    assert status["startup_non_lock_errors_fail_fast"] is True
    assert dispatch["full_scope_sqlite_commit_off_uvicorn_event_loop"] is True
    assert dispatch["full_scope_set_based_writer"] is True
    assert dispatch["critical_receipts_batched"] is True
    assert dispatch["critical_per_receipt_commits"] is False
