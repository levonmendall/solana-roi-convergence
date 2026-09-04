from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from solana_roi import robinhood_runtime_install as runtime_install
from solana_roi import robinhood_worker_isolation_repair as isolation


def _canonical_store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path, check_same_thread=False)
    db.row_factory = sqlite3.Row
    return SimpleNamespace(path=path, db=db, _lock=threading.RLock())


def test_dedicated_store_is_separate_sibling_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_CHAIN_STORE_PATH", raising=False)
    canonical = SimpleNamespace(path=tmp_path / "solana-roi.sqlite3")

    isolated = isolation._dedicated_store_path(canonical)

    assert isolated == (tmp_path / "solana-roi-robinhood-chain.sqlite3").resolve()
    assert isolated != Path(canonical.path).resolve()
    assert isolated.parent == Path(canonical.path).resolve().parent


def test_dedicated_store_path_can_be_explicit(tmp_path: Path, monkeypatch) -> None:
    canonical = SimpleNamespace(path=tmp_path / "canonical.sqlite3")
    explicit = tmp_path / "robinhood-only.sqlite3"
    monkeypatch.setenv("ROBINHOOD_CHAIN_STORE_PATH", str(explicit))

    assert isolation._dedicated_store_path(canonical) == explicit.resolve()


def test_cursor_seed_copies_only_processed_block_cursor(tmp_path: Path) -> None:
    canonical = _canonical_store(tmp_path / "canonical.sqlite3")
    canonical.db.execute(
        "CREATE TABLE robinhood_chain_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    canonical.db.execute(
        "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES ('cursor_block','9123456','now')"
    )
    canonical.db.commit()

    class Plane:
        def __init__(self) -> None:
            self._cursor = None
            self.values: list[int] = []

        def _set_cursor(self, value: int) -> None:
            self._cursor = value
            self.values.append(value)

    plane = Plane()
    seeded = isolation._seed_cursor_from_canonical(plane, canonical)
    assert seeded == 9_123_456
    assert plane._cursor == 9_123_456
    assert plane.values == [9_123_456]

    seeded_again = isolation._seed_cursor_from_canonical(plane, canonical)
    assert seeded_again == 9_123_456
    assert plane.values == [9_123_456]
    canonical.db.close()


def test_status_is_served_from_cache_without_live_worker_store_access(monkeypatch) -> None:
    snapshot = {
        "runtime_ready": True,
        "paper_only": True,
        "paper_trading_authority": True,
        "caught_up_for_paper_decisions": True,
        "paper_decision_transport_ready": True,
        "block_lag": 1,
        "worker_isolation": {"dedicated_store_path": "/tmp/robinhood.sqlite3"},
    }
    monkeypatch.setattr(isolation, "_STATUS_SNAPSHOT", snapshot)
    monkeypatch.setattr(isolation, "_STATUS_PUBLISHED_MONOTONIC", time.monotonic())
    monkeypatch.setattr(isolation, "_WORKER_THREAD", threading.current_thread())
    runtime_install._STATE["state"] = "running"

    payload = isolation._nonblocking_status()

    assert payload["runtime_ready"] is True
    assert payload["block_lag"] == 1
    assert payload["production_install"]["state"] == "running"
    worker = payload["worker_isolation"]
    assert worker["worker_topology"] == "dedicated_os_thread_with_private_asyncio_loop"
    assert worker["dedicated_sqlite_store"] is True
    assert worker["canonical_store_shared_for_robinhood_writes"] is False
    assert worker["uvicorn_event_loop_runs_robinhood_chain_worker"] is False
    assert worker["status_served_from_nonblocking_cache"] is True
    assert worker["status_cache_stale"] is False


def test_runtime_install_composes_isolation_before_production_start() -> None:
    assert runtime_install._runtime_workers_with_robinhood is isolation._isolated_runtime_workers
    assert runtime_install._status is isolation._nonblocking_status
    assert runtime_install._STATE["worker_isolation_repair"] == isolation.REPAIR_VERSION
    assert runtime_install._STATE["worker_isolation"] == "dedicated_os_thread_with_private_asyncio_loop"


def test_minimal_runtime_without_store_delegates_to_canonical_workers(monkeypatch) -> None:
    calls: list[str] = []

    async def original_workers(runtime, stop) -> None:
        calls.append("canonical")
        assert not hasattr(runtime, "store")
        assert stop.is_set()

    def forbidden_start(_store):
        raise AssertionError("Robinhood isolation must not start without a production store")

    monkeypatch.setattr(runtime_install, "_ORIGINAL_RUNTIME_WORKERS", original_workers)
    monkeypatch.setattr(isolation, "_start_worker_thread", forbidden_start)
    stop = asyncio.Event()
    stop.set()

    asyncio.run(isolation._isolated_runtime_workers(SimpleNamespace(), stop))

    assert calls == ["canonical"]
    assert runtime_install._STATE["worker_isolation_skipped_no_store"] is True


def test_worker_constructs_plane_on_dedicated_thread_with_private_store(tmp_path: Path, monkeypatch) -> None:
    canonical = _canonical_store(tmp_path / "canonical.sqlite3")
    parent_thread = threading.get_ident()
    observed: dict[str, object] = {}

    class Store:
        def __init__(self, path: Path) -> None:
            observed["store_path"] = Path(path)

        def close(self) -> None:
            observed["store_closed"] = True

    class Plane:
        enabled = False
        _cursor = None

        def __init__(self, store: Store) -> None:
            self.store = store
            observed["plane_thread"] = threading.get_ident()

        def _set_cursor(self, value: int) -> None:
            self._cursor = value

        async def close(self) -> None:
            observed["plane_closed"] = True

    monkeypatch.setattr(isolation, "_ORIGINAL_STATUS", lambda: {"runtime_ready": False, "paper_only": True})
    thread, stop = isolation._start_worker_thread(
        canonical,
        plane_factory=Plane,
        store_factory=Store,
    )
    thread.join(2.0)
    stop.set()

    assert thread.is_alive() is False
    assert observed["plane_thread"] != parent_thread
    assert observed["store_path"] == (tmp_path / "canonical-robinhood-chain.sqlite3").resolve()
    assert observed["store_path"] != Path(canonical.path).resolve()
    assert observed["store_closed"] is True
    assert observed["plane_closed"] is True
    canonical.db.close()
