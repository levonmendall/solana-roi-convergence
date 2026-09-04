from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from solana_roi import robinhood_runtime_install as runtime_install


def test_dedicated_store_is_separate_sibling_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_ISOLATED_STORE_PATH", raising=False)
    canonical = tmp_path / "solana-roi.sqlite3"
    isolated = runtime_install._dedicated_store_path(canonical)

    assert isolated == tmp_path / "solana-roi-robinhood.sqlite3"
    assert isolated != canonical
    assert isolated.parent == canonical.parent


def test_dedicated_store_path_can_be_explicit(tmp_path: Path, monkeypatch) -> None:
    explicit = tmp_path / "robinhood-only.sqlite3"
    monkeypatch.setenv("ROBINHOOD_ISOLATED_STORE_PATH", str(explicit))

    assert runtime_install._dedicated_store_path(tmp_path / "canonical.sqlite3") == explicit


def test_cursor_seed_is_read_only_and_only_when_isolated_cursor_missing(tmp_path: Path) -> None:
    canonical = tmp_path / "canonical.sqlite3"
    connection = sqlite3.connect(canonical)
    connection.execute(
        "CREATE TABLE robinhood_chain_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO robinhood_chain_state(key,value,updated_at) VALUES ('cursor_block','9123456','now')"
    )
    connection.commit()
    connection.close()

    class Plane:
        def __init__(self) -> None:
            self._cursor = None
            self.values: list[int] = []

        def _set_cursor(self, value: int) -> None:
            self._cursor = value
            self.values.append(value)

    plane = Plane()
    seeded, error = runtime_install._seed_cursor_from_canonical(plane, canonical)  # type: ignore[arg-type]
    assert seeded is True
    assert error is None
    assert plane._cursor == 9_123_456
    assert plane.values == [9_123_456]

    seeded_again, second_error = runtime_install._seed_cursor_from_canonical(plane, canonical)  # type: ignore[arg-type]
    assert seeded_again is False
    assert second_error is None
    assert plane.values == [9_123_456]


def test_status_is_served_from_cached_snapshot_not_live_plane(monkeypatch) -> None:
    snapshot = {
        "runtime_ready": True,
        "paper_only": True,
        "block_lag": 7,
        "production_install": {"state": "stale-copy"},
    }
    monkeypatch.setattr(runtime_install, "_STATUS_SNAPSHOT", snapshot)
    monkeypatch.setattr(runtime_install, "_PLANE", SimpleNamespace(status=lambda: (_ for _ in ()).throw(AssertionError())))
    runtime_install._STATE["state"] = "running"

    payload = runtime_install._status()

    assert payload["runtime_ready"] is True
    assert payload["block_lag"] == 7
    assert payload["production_install"]["state"] == "running"
    assert payload["worker_isolation"]["dedicated_thread"] is True
    assert payload["worker_isolation"]["dedicated_asyncio_event_loop"] is True
    assert payload["worker_isolation"]["dedicated_sqlite_file"] is True
    assert payload["worker_isolation"]["canonical_sqlite_shared"] is False
    assert payload["worker_isolation"]["uvicorn_event_loop_runs_robinhood_polling"] is False
    assert payload["worker_isolation"]["uvicorn_event_loop_runs_robinhood_sqlite"] is False


def test_runtime_wrapper_starts_robinhood_on_dedicated_thread(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, int]] = []
    parent_thread = threading.get_ident()

    def fake_thread_entry(path: str, stop_event: threading.Event) -> None:
        calls.append((path, threading.get_ident()))
        stop_event.wait(2.0)

    async def canonical_workers(_runtime, _stop) -> None:
        await asyncio.sleep(0.02)

    monkeypatch.setattr(runtime_install, "_thread_entry", fake_thread_entry)
    monkeypatch.setattr(runtime_install, "_ORIGINAL_RUNTIME_WORKERS", canonical_workers)
    runtime = SimpleNamespace(store=SimpleNamespace(path=tmp_path / "canonical.sqlite3"))
    stop = asyncio.Event()
    stop.set()

    asyncio.run(runtime_install._runtime_workers_with_robinhood(runtime, stop))

    assert calls
    assert calls[0][0] == str(tmp_path / "canonical.sqlite3")
    assert calls[0][1] != parent_thread
    assert runtime_install._STATE["dedicated_thread_alive"] is False
