from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from solana_roi.config import BASELINE
from solana_roi.durable_engine import DurablePaperTradingEngine
from solana_roi.observation_store import ObservationEventStore
from solana_roi.runtime_storage_composition import (
    _materialize_verified_genesis_checkpoint_if_needed,
    _require_materialized_legacy_checkpoint_for_snapshot,
)


def _checkpoint(path: Path) -> tuple[int, str] | None:
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT last_engine_event_id,state_sha256 FROM paper_engine_checkpoint WHERE id=1"
        ).fetchone()
    return (int(row[0]), str(row[1])) if row is not None else None


def test_verified_legacy_genesis_is_materialized_once_for_bounded_migration(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    store = ObservationEventStore(path)
    store.append("storage_probe", "2026-09-14T00:00:00+00:00", {"probe": True})
    engine = DurablePaperTradingEngine(store=store)
    assert _checkpoint(path) is None

    payload = _materialize_verified_genesis_checkpoint_if_needed(store, engine)
    assert payload is not None
    assert payload["status"] == "verified_genesis_checkpoint_materialized"
    assert payload["history_rescanned_by_migration"] is False
    checkpoint = _checkpoint(path)
    assert checkpoint is not None and checkpoint[0] == 0 and checkpoint[1]
    assert _require_materialized_legacy_checkpoint_for_snapshot(path) == checkpoint

    # The singleton write is idempotent and the same durable engine can restore it.
    assert _materialize_verified_genesis_checkpoint_if_needed(store, engine) is None
    store.close()
    restored_store = ObservationEventStore(path)
    restored = DurablePaperTradingEngine(store=restored_store)
    assert restored._last_engine_event_id == 0
    assert restored.portfolio.cash_usd == BASELINE.initial_capital_usd
    restored_store.close()


def test_snapshot_gate_fails_fast_when_checkpoint_not_materialized(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    store = ObservationEventStore(path)
    store.append("storage_probe", "2026-09-14T00:00:00+00:00", {"probe": True})
    DurablePaperTradingEngine(store=store)
    assert _checkpoint(path) is None
    with pytest.raises(RuntimeError, match="run one normal legacy-authoritative startup"):
        _require_materialized_legacy_checkpoint_for_snapshot(path)
    store.close()


def test_engine_history_without_checkpoint_still_fails_before_materialization(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    store = ObservationEventStore(path)
    store.append("price", "2026-09-14T00:00:00+00:00", {"token_mint": "mint", "reference_price": 1.0})
    with pytest.raises(RuntimeError, match="engine history exists without a durable checkpoint"):
        DurablePaperTradingEngine(store=store)
    assert _checkpoint(path) is None
    store.close()
