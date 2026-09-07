from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from solana_roi import batch9_continuity_frontier_proof_repair as repair
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease


def _store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return SimpleNamespace(path=path, db=db, _lock=threading.RLock())


def test_strategy_scout_checkpoint_is_release_scoped_and_durable(monkeypatch, tmp_path: Path) -> None:
    store = _store(tmp_path / "solana.sqlite3")
    plane = SimpleNamespace(store=store)
    target = SimpleNamespace(kind="scout", address="wallet-a", source_hint=None)
    monkeypatch.setattr(repair, "_release_commit", lambda: "release-a")

    repair._save_checkpoint(
        plane,
        target,
        cursor_slot=12345,
        ws_gap_generation=7,
        last_success_at="2026-09-07T14:00:00+00:00",
    )
    row = repair._checkpoint_row(plane, target)

    assert row is not None
    assert row["release_commit"] == "release-a"
    assert row["cursor_slot"] == 12345
    assert row["ws_gap_generation"] == 7

    monkeypatch.setattr(repair, "_release_commit", lambda: "release-b")
    assert repair._checkpoint_row(plane, target) is None
    store.db.close()


def test_batch9_does_not_expand_continuity_lease_or_recovery_bound() -> None:
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_robinhood_proof_refresh_writes_only_disposable_snapshot(monkeypatch, tmp_path: Path) -> None:
    live_path = tmp_path / "robinhood.sqlite3"
    db = sqlite3.connect(live_path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE live_only(id INTEGER PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO live_only(value) VALUES ('preserved')")
    db.commit()
    db.close()

    def write_bearing_proof(snapshot_path: str, *, store_factory=None):
        snapshot = sqlite3.connect(snapshot_path)
        try:
            snapshot.execute("CREATE TABLE proof_side_effect(id INTEGER PRIMARY KEY)")
            snapshot.execute("INSERT INTO proof_side_effect DEFAULT VALUES")
            snapshot.commit()
        finally:
            snapshot.close()
        return {"available": True}

    monkeypatch.setattr(repair, "_ORIGINAL_PROOF_REFRESH", write_bearing_proof)
    proof = repair._snapshot_robinhood_proof_refresh(str(live_path))

    assert proof["available"] is True
    assert proof["proof_refresh_writes_live_store"] is False
    check = sqlite3.connect(live_path)
    try:
        assert check.execute("SELECT value FROM live_only").fetchone()[0] == "preserved"
        side_effect = check.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='proof_side_effect'"
        ).fetchone()
        assert side_effect is None
    finally:
        check.close()


def test_live_epoch_requires_anchor_and_started_at() -> None:
    cursor_only = SimpleNamespace(_roi_live_epoch_cursor=100)
    assert repair._strict_live_epoch_active(cursor_only) is False

    anchored = SimpleNamespace(
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_anchor_block=99,
        _roi_live_epoch_started_at="2026-09-07T14:00:00+00:00",
    )
    assert repair._strict_live_epoch_active(anchored) is True


def test_epoch_integrity_binds_to_current_websocket_generation(monkeypatch) -> None:
    plane = SimpleNamespace(
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_anchor_block=99,
        _roi_live_epoch_started_at="2026-09-07T14:00:00+00:00",
        _roi_prod_ws_epoch_generation=4,
    )
    monkeypatch.setattr(repair.prod_ws, "_state", lambda _plane: {"generation": 4})
    assert repair._epoch_integrity(plane) is True

    monkeypatch.setattr(repair.prod_ws, "_state", lambda _plane: {"generation": 5})
    assert repair._epoch_integrity(plane) is False


def test_production_root_composes_finalized_batch9_after_legacy_graph() -> None:
    from solana_roi import production_system

    source = Path(production_system.__file__).read_text(encoding="utf-8")
    assert "install_batch9_finalization_repair(app)" in source
    assert "v13-batch9-finalized" in source
    assert '"paper_only": PAPER_ONLY' in source
    assert '"live_money_authority": LIVE_MONEY_AUTHORITY' in source
    assert '"signing_available": SIGNING_AVAILABLE' in source
    assert '"transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE' in source
