from __future__ import annotations

import gc
import weakref

from solana_roi import active_runtime as runtime
from test_storage_active_transition_v2 import ANCHOR_HASH, ANCHOR_ID, _make_active


class _TrackedCheckpoint(dict):
    pass


def test_verified_checkpoint_is_released_while_runtime_retains_its_anchors(tmp_path, monkeypatch):
    path = tmp_path / "active.sqlite3"
    _make_active(path)
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_MAINTENANCE_SECONDS", "0")
    load = runtime.load_verified_checkpoint
    checkpoints = []

    def tracked_load(*args, **kwargs):
        checkpoint = _TrackedCheckpoint(load(*args, **kwargs))
        checkpoints.append(weakref.ref(checkpoint))
        return checkpoint

    monkeypatch.setattr(runtime, "load_verified_checkpoint", tracked_load)
    for _ in range(3):
        store = runtime.ActiveObservationEventStore(path)
        try:
            gc.collect()
            assert checkpoints[-1]() is None
            assert store.transition_event_head_id == ANCHOR_ID
            assert store.transition_event_head_hash == ANCHOR_HASH
            assert store.transition_engine_event_id == ANCHOR_ID
            assert store.verify()
        finally:
            store.close()
