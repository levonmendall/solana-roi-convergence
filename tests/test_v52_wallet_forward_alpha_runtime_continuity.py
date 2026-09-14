from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v52_wallet_forward_alpha_bootstrap as bootstrap
from solana_roi import v52_wallet_forward_alpha_runtime as runtime_mod


def test_runtime_validation_epoch_survives_process_restart(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "wfa-continuity.sqlite3")
    tracker = SimpleNamespace(store=store, discovery=SimpleNamespace())
    monkeypatch.setattr(runtime_mod, "_RUNTIME", None)

    first = bootstrap._ensure_runtime(tracker)
    expected = datetime.now(timezone.utc) - timedelta(days=12)
    with store._lock, store.db:
        store.db.execute(
            "UPDATE v52_wallet_forward_runtime_state SET started_at=?,last_capture_at=?,last_validation_at=? WHERE id=1",
            (
                expected.isoformat(),
                (expected + timedelta(hours=1)).isoformat(),
                (expected + timedelta(hours=2)).isoformat(),
            ),
        )

    monkeypatch.setattr(runtime_mod, "_RUNTIME", None)
    restarted = bootstrap._ensure_runtime(tracker)

    assert restarted is not first
    assert restarted.started_at == expected
    assert restarted.last_capture_at == expected + timedelta(hours=1)
    assert restarted.last_validation_at == expected + timedelta(hours=2)
    assert restarted.status()["runtime_age_hours"] >= 12 * 24 - 1


def test_future_or_corrupt_persisted_start_cannot_fabricate_older_history(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "wfa-continuity-fail-closed.sqlite3")
    tracker = SimpleNamespace(store=store, discovery=SimpleNamespace())
    monkeypatch.setattr(runtime_mod, "_RUNTIME", None)

    current = bootstrap._ensure_runtime(tracker)
    process_start = current.started_at
    with store._lock, store.db:
        store.db.execute(
            "UPDATE v52_wallet_forward_runtime_state SET started_at=? WHERE id=1",
            ((process_start + timedelta(days=365)).isoformat(),),
        )

    monkeypatch.setattr(runtime_mod, "_RUNTIME", None)
    restarted = bootstrap._ensure_runtime(tracker)

    assert restarted.started_at <= datetime.now(timezone.utc)
    assert restarted.started_at != process_start + timedelta(days=365)
    assert bootstrap.status()["prospective_validation_epoch_persists_across_restarts"] is True
    assert restarted.status()["live_money_authority"] is False
