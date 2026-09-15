from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from solana_roi import robinhood_worker_isolation_repair as isolation


class _ForbiddenStore:
    def __getattribute__(self, name: str):
        raise AssertionError(f"fast status must not touch SQLite/store state: {name}")


def test_fast_live_status_uses_only_in_memory_runtime_state() -> None:
    plane = SimpleNamespace(
        enabled=True,
        store=_ForbiddenStore(),
        _cursor=100,
        _latest_block=102,
        _caught_up=True,
        _last_poll_at="poll",
        _last_success_at="success",
        _last_error=None,
        _rpc_failures=0,
        v3_pools={"a": object()},
        v2_curves={"b": object()},
        _roi_market_log_legacy_equivalent_requests=10,
        _roi_market_log_actual_requests=6,
    )

    payload = isolation._fast_live_status(plane)

    assert payload["runtime_ready"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["paper_decision_transport_ready"] is True
    assert payload["block_lag"] == 2
    assert payload["tracked_v3_pools"] == 1
    assert payload["tracked_pons_v2_curves"] == 1
    assert payload["status_read_boundary"] == {
        "history_scaled_sqlite_reads": False,
        "swap_count_scanned_on_heartbeat": False,
        "outcome_history_scanned_on_heartbeat": False,
        "nav_history_scanned_on_heartbeat": False,
        "proof_analytics_published_separately": True,
    }


def _store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path, check_same_thread=False)
    db.row_factory = sqlite3.Row
    return SimpleNamespace(path=path, db=db, _lock=threading.RLock(), close=db.close)


def test_raw_swap_growth_does_not_advance_deep_proof_generation(tmp_path: Path) -> None:
    store = _store(tmp_path / "robinhood.sqlite3")
    with store.db:
        store.db.execute("CREATE TABLE robinhood_swaps(id INTEGER PRIMARY KEY, value TEXT)")
        store.db.execute("CREATE TABLE robinhood_paper_outcomes(id INTEGER PRIMARY KEY, value TEXT)")

    assert isolation._ensure_proof_generation_schema(store) == 0

    with store.db:
        store.db.execute("INSERT INTO robinhood_swaps(value) VALUES ('raw-1')")
        store.db.execute("INSERT INTO robinhood_swaps(value) VALUES ('raw-2')")
    assert isolation._ensure_proof_generation_schema(store) == 0

    with store.db:
        store.db.execute("INSERT INTO robinhood_paper_outcomes(value) VALUES ('settled')")
    assert isolation._ensure_proof_generation_schema(store) == 1

    with store.db:
        store.db.execute("UPDATE robinhood_paper_outcomes SET value='reconciled' WHERE id=1")
    assert isolation._ensure_proof_generation_schema(store) == 2

    store.close()


def test_unchanged_generation_reuses_cached_proof_without_deep_rebuild(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "robinhood.sqlite3"
    seed = _store(path)
    with seed.db:
        seed.db.execute("CREATE TABLE robinhood_swaps(id INTEGER PRIMARY KEY, value TEXT)")
        seed.db.execute("CREATE TABLE robinhood_paper_outcomes(id INTEGER PRIMARY KEY, value TEXT)")
    generation = isolation._ensure_proof_generation_schema(seed)
    seed.close()

    monkeypatch.setattr(isolation, "_PROOF_SNAPSHOT", {"available": True, "sentinel": "cached"})
    monkeypatch.setattr(isolation, "_PROOF_INPUT_GENERATION", generation)

    proof = isolation._refresh_proof_on_separate_connection(path, store_factory=_store)

    assert proof["available"] is True
    assert proof["sentinel"] == "cached"
    assert proof["proof_input_generation"] == generation
    assert proof["proof_refresh_skipped_unchanged_inputs"] is True
    assert proof["deep_proof_rebuilt"] is False
    assert proof["proof_refresh_topology"] == (
        "generation_gated_separate_sqlite_connection_in_threadpool"
    )


def test_proof_generation_schema_is_additive_and_preserves_history(tmp_path: Path) -> None:
    store = _store(tmp_path / "robinhood.sqlite3")
    with store.db:
        store.db.execute("CREATE TABLE robinhood_swaps(id INTEGER PRIMARY KEY, value TEXT)")
        store.db.executemany(
            "INSERT INTO robinhood_swaps(value) VALUES (?)",
            [("a",), ("b",), ("c",)],
        )
    before = store.db.execute("SELECT COUNT(*) FROM robinhood_swaps").fetchone()[0]

    isolation._ensure_proof_generation_schema(store)

    after = store.db.execute("SELECT COUNT(*) FROM robinhood_swaps").fetchone()[0]
    assert after == before == 3
    assert store.db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND tbl_name='robinhood_swaps'"
    ).fetchone()[0] == 0
    store.close()
