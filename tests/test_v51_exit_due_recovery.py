from __future__ import annotations

import asyncio
from datetime import timedelta

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v51_exact_exit_execution as exact
from solana_roi import v51_exit_due_recovery as recovery


RELEASE = "d" * 40


class DummyAdapter:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = RELEASE
        self.epoch_id = "exit-due-recovery-test-epoch"


def _insert_liquidation(
    adapter: DummyAdapter,
    *,
    signature: str,
    status: str,
    first_due_offset_seconds: float,
    retry_offset_seconds: float | None,
) -> None:
    exact._ensure_schema(adapter)
    now = exact._utcnow()
    first_due = now + timedelta(seconds=first_due_offset_seconds)
    next_retry = None if retry_offset_seconds is None else (now + timedelta(seconds=retry_offset_seconds)).isoformat()
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT INTO profit_first_final_exit_liquidations("
            "epoch_id,release_commit,execution_model_epoch,position_scope,source_signature,token_mint,actual_position_raw,"
            "entry_cost_sol,position_fraction,exit_signal_signature,exit_reason,exit_features_json,first_exit_due_at,last_attempt_at,"
            "attempt_count,next_retry_at,status,eventual_exit_net_sol,settled_at,terminal_assumption,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,0,?,?,NULL,NULL,NULL,1,0)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                "SOLANA:test",
                signature,
                "TOKEN",
                1000,
                1.0,
                0.01,
                f"exit-{signature}",
                "test_exit",
                "{}",
                first_due.isoformat(),
                next_retry,
                status,
            ),
        )


def test_restart_reclaims_stale_initial_exit_due(tmp_path, monkeypatch) -> None:
    store = ObservationEventStore(tmp_path / "restart.sqlite3")
    adapter = DummyAdapter(store)
    _insert_liquidation(
        adapter,
        signature="orphaned-initial",
        status="exit_due",
        first_due_offset_seconds=-(recovery.STALE_EXIT_DUE_SECONDS + 1.0),
        retry_offset_seconds=None,
    )
    calls: list[str] = []

    async def attempt(received: DummyAdapter, liquidation: dict[str, object]) -> None:
        assert received is adapter
        calls.append(str(liquidation["source_signature"]))

    monkeypatch.setattr(exact, "_attempt_liquidation", attempt)
    asyncio.run(recovery._retry_due_with_exit_due(adapter))
    assert calls == ["orphaned-initial"]
    store.close()


def test_recovery_preserves_failed_retry_schedule_and_stale_guard(tmp_path, monkeypatch) -> None:
    store = ObservationEventStore(tmp_path / "schedule.sqlite3")
    adapter = DummyAdapter(store)
    _insert_liquidation(
        adapter,
        signature="stale-initial",
        status="exit_due",
        first_due_offset_seconds=-(recovery.STALE_EXIT_DUE_SECONDS + 1.0),
        retry_offset_seconds=None,
    )
    _insert_liquidation(
        adapter,
        signature="fresh-initial",
        status="exit_due",
        first_due_offset_seconds=-1.0,
        retry_offset_seconds=None,
    )
    _insert_liquidation(
        adapter,
        signature="failed-due",
        status="paper_exit_execution_failed",
        first_due_offset_seconds=-60.0,
        retry_offset_seconds=-1.0,
    )
    _insert_liquidation(
        adapter,
        signature="failed-future",
        status="paper_exit_execution_failed",
        first_due_offset_seconds=-60.0,
        retry_offset_seconds=60.0,
    )
    calls: list[str] = []

    async def attempt(received: DummyAdapter, liquidation: dict[str, object]) -> None:
        assert received is adapter
        calls.append(str(liquidation["source_signature"]))

    monkeypatch.setattr(exact, "_attempt_liquidation", attempt)
    asyncio.run(recovery._retry_due_with_exit_due(adapter))
    assert set(calls) == {"stale-initial", "failed-due"}
    assert "fresh-initial" not in calls
    assert "failed-future" not in calls
    store.close()


def test_recovery_surface_remains_paper_only() -> None:
    payload = recovery.status()
    assert payload["owns_initial_exit_due_after_restart"] is True
    assert payload["uses_canonical_attempt_liquidation"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
