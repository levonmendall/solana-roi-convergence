from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease
from solana_roi import durable_bootstrap_memory_repair as memory
from solana_roi import render_runtime_bootstrap_repair as render_bootstrap


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._lock = threading.RLock()
        self.db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class _FakeTimer:
    def __init__(self, interval, function, args=(), kwargs=None):
        self.interval = float(interval)
        self.function = function
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        self.daemon = False
        self.started = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def _autocheckpoint(store: _Store) -> int:
    return int(store.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0])


def test_prime_before_workers_disables_autocheckpoint_and_never_checkpoints(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    original = _autocheckpoint(store)
    assert original > 0

    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda target: 0)
    checkpoints: list[str] = []
    monkeypatch.setattr(
        lease,
        "_maintenance_checkpoint_locked",
        lambda target: checkpoints.append("checkpoint") or (0, 0, 0, None),
    )
    syncs: list[Path] = []
    monkeypatch.setattr(lease, "_sync_and_release", lambda path: syncs.append(Path(path)))
    samples = iter(
        [
            {
                "current_bytes": 1_900_000_000,
                "file_bytes": 1_700_000_000,
                "file_dirty_bytes": 128_000_000,
            },
            {
                "current_bytes": 1_400_000_000,
                "file_bytes": 1_200_000_000,
                "file_dirty_bytes": 0,
            },
        ]
    )
    monkeypatch.setattr(memory, "_cgroup_memory", lambda: next(samples))
    trims: list[str] = []
    monkeypatch.setattr(memory, "_trim_process_heap", lambda: trims.append("trim") or True)

    state = lease.prime_before_workers(store)

    assert state["active"] is True
    assert _autocheckpoint(store) == 0
    assert syncs == [store.path]
    assert trims == ["trim"]
    assert checkpoints == []

    assert lease.finish(store, reason="test_complete") is True
    assert _autocheckpoint(store) == original
    store.close()


def test_preworker_wrapper_primes_before_workers_and_preserves_markers(monkeypatch):
    order: list[str] = []

    async def original_workers(runtime, stop):
        order.append("workers")
        return "done"

    setattr(original_workers, "_roi_forward_certification_snapshot_worker", True)
    setattr(original_workers, "_roi_e2e_status_snapshot_worker", True)
    setattr(original_workers, "_roi_production_proof_snapshot_worker", True)
    monkeypatch.setattr(render_bootstrap, "_run_runtime_workers", original_workers)
    monkeypatch.setattr(lease, "prime_before_workers", lambda store: order.append("prime") or {})

    lease._install_preworker_quiesce()
    wrapped = render_bootstrap._run_runtime_workers

    result = asyncio.run(wrapped(SimpleNamespace(store=object()), object()))

    assert result == "done"
    assert order == ["prime", "workers"]
    assert wrapped is not original_workers
    assert getattr(wrapped, "_roi_bootstrap_preworker_quiesce") is True
    assert getattr(wrapped, "_roi_original_runtime_workers") is original_workers
    assert getattr(wrapped, "_roi_forward_certification_snapshot_worker") is True
    assert getattr(wrapped, "_roi_e2e_status_snapshot_worker") is True
    assert getattr(wrapped, "_roi_production_proof_snapshot_worker") is True
