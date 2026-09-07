from __future__ import annotations

import sqlite3
import threading

from solana_roi.strategy_v51_authority import ECONOMIC_FREEZE_EPOCH, authority_fingerprint
from solana_roi.v51_measurement_integrity import (
    MEASUREMENT_EPOCH,
    ensure_release_compatibility,
    proof_metadata,
)

PRE_BATCH7_MEASUREMENT_EPOCH = "v51-measurement-post185-20260905-1"
BATCH7_MEASUREMENT_EPOCH = "v51-measurement-batch7-20260906-1"


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row


def test_batch7_starts_new_measurement_epoch_without_changing_economic_epoch(monkeypatch) -> None:
    store = Store()
    release = "2" * 40
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)

    row = ensure_release_compatibility(store, release)
    metadata = proof_metadata(store)

    assert MEASUREMENT_EPOCH == BATCH7_MEASUREMENT_EPOCH
    assert row is not None
    assert row["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert row["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert row["economic_fingerprint"] == authority_fingerprint()
    assert metadata["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert metadata["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert metadata["paper_only"] is True
    assert metadata["live_money_authority"] is False


def test_batch7_rotation_does_not_rewrite_historical_epoch_tags(monkeypatch) -> None:
    store = Store()
    historical_release = "1" * 40
    current_release = "2" * 40

    historical = ensure_release_compatibility(store, historical_release)
    assert historical is not None
    with store._lock, store.db:
        store.db.execute(
            "UPDATE v51_release_compatibility SET measurement_epoch=? WHERE release_commit=?",
            (PRE_BATCH7_MEASUREMENT_EPOCH, historical_release),
        )

    preserved = ensure_release_compatibility(store, historical_release)
    current = ensure_release_compatibility(store, current_release)

    assert preserved is not None
    assert current is not None
    assert preserved["measurement_epoch"] == PRE_BATCH7_MEASUREMENT_EPOCH
    assert current["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert preserved["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert current["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert preserved["economic_fingerprint"] == current["economic_fingerprint"] == authority_fingerprint()

    monkeypatch.setenv("RENDER_GIT_COMMIT", current_release)
    metadata = proof_metadata(store)
    assert metadata["measurement_epoch"] == BATCH7_MEASUREMENT_EPOCH
    assert metadata["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
