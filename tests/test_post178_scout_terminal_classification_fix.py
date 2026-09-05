from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import post178_scout_terminal_classification_fix as repair
from solana_roi import release_bound_scout_classification_repair as release_bound
from solana_roi import scout_candidate_continuity_repair as scout


def test_live_tracked_normalizer_terminally_classifies_unpriced_economic_movement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}
    plane = SimpleNamespace(store=object())
    trigger = datetime.now(timezone.utc)

    monkeypatch.setattr(
        repair,
        "_ORIGINAL_TRACKED_NORMALIZER",
        lambda *args, **kwargs: (None, "economic_movement_price_unresolved"),
    )
    monkeypatch.setattr(release_bound, "_terminal_classification", lambda *args, **kwargs: None)

    def record(store: object, **kwargs: object) -> None:
        recorded.update(kwargs)

    monkeypatch.setattr(release_bound, "_record_terminal_non_candidate", record)
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        swap, error = repair._tracked_normalizer_with_terminal_noncopyable(
            {},
            signature="sig-runtime-unpriced",
            trigger_received_at=trigger,
            wallet="Scout1111111111111111111111111111111111111",
            source_hint=None,
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)

    assert swap is None
    assert error == "economic_movement_price_unresolved"
    assert recorded["signature"] == "sig-runtime-unpriced"
    assert recorded["reason"] == "economic_movement_price_unresolved_noncopyable"
    assert plane._roi_post178_economic_movement_noncopyable_classifications == 1
