from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_live_frontier_verification_repair as frontier
from solana_roi import robinhood_v2_v4_observation as observation
from solana_roi import robinhood_v2_v4_pause_guard as pause_guard


def _guarded_target() -> SimpleNamespace:
    return SimpleNamespace(**{pause_guard.SCHEDULE_GUARD_ATTR: True})


def test_v2_v4_pause_guard_defaults_off_when_env_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)
    target = _guarded_target()

    pause_guard.install_robinhood_v2_v4_pause_guard()

    assert pause_guard.schedule_enabled(target) is False
    # The production guard must not mutate the direct observer predicate globally.
    assert observation._enabled() is True
    status = pause_guard.status()
    assert status["installed"] is True
    assert status["environment_present"] is False
    assert status["default_when_missing"] is False
    assert status["reactivation_requires_explicit_true"] is True
    assert status["schedule_boundary_only"] is True
    assert status["schedule_boundary_enforced_at_fetch"] is True
    assert status["paused_path_calls_canonical_fetch_only"] is True
    assert status["observer_predicate_globally_mutated"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False


def test_v2_v4_pause_guard_requires_explicit_true(monkeypatch) -> None:
    target = _guarded_target()
    pause_guard.install_robinhood_v2_v4_pause_guard()

    for value in ("0", "false", "no", "off", ""):
        monkeypatch.setenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", value)
        assert pause_guard.schedule_enabled(target) is False

    for value in ("1", "true", "yes", "on", "TRUE", "Yes"):
        monkeypatch.setenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", value)
        assert pause_guard.schedule_enabled(target) is True


def test_pause_guard_marks_only_production_plane(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)

    class Plane:
        pass

    pause_guard.install_robinhood_v2_v4_pause_guard(Plane)

    assert getattr(Plane, pause_guard.SCHEDULE_GUARD_ATTR) is True
    assert pause_guard.schedule_enabled(Plane()) is False
    # An uncomposed helper remains governed by the legacy direct-observer predicate.
    assert pause_guard.schedule_enabled(SimpleNamespace()) is observation._enabled()


def test_production_fetch_seam_bypasses_observer_until_explicit_true(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", raising=False)
    calls: list[str] = []

    class Plane:
        def status(self):
            return {}

    async def canonical(_self, *, from_block: int, to_block: int):
        calls.append(f"canonical:{from_block}-{to_block}")
        return [("canonical", from_block, {"to_block": to_block})]

    async def observe(_self, *, from_block: int, to_block: int) -> None:
        calls.append(f"observe:{from_block}-{to_block}")

    # Reconstruct the exact production install order in isolation and restore every
    # module-level mutation automatically after the test.
    monkeypatch.setattr(frontier, "_fetch_market_logs", canonical)
    monkeypatch.setattr(observation, "_fetch_with_observation", pause_guard._ORIGINAL_FETCH_FACTORY)
    monkeypatch.setattr(observation, "_ORIGINAL_FETCH", None)
    monkeypatch.setattr(observation, "_ORIGINAL_STATUS", None)
    monkeypatch.setattr(observation, "_observe_range", observe)
    monkeypatch.setattr(pause_guard, "_INSTALLED", False)

    pause_guard.install_robinhood_v2_v4_pause_guard(Plane)
    observation.install_robinhood_v2_v4_observation(Plane)

    guarded_fetch = frontier._fetch_market_logs
    assert bool(getattr(guarded_fetch, "_roi_v2_v4_pause_guard_fetch", False))

    paused_result = asyncio.run(guarded_fetch(Plane(), from_block=10, to_block=20))
    assert paused_result == [("canonical", 10, {"to_block": 20})]
    assert calls == ["canonical:10-20"]

    monkeypatch.setenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", "true")
    enabled_result = asyncio.run(guarded_fetch(Plane(), from_block=21, to_block=30))
    assert enabled_result == [("canonical", 21, {"to_block": 30})]
    assert calls == ["canonical:10-20", "canonical:21-30", "observe:21-30"]


def test_explicit_false_also_bypasses_observer_at_fetch_seam(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_V2_V4_OBSERVATION_ENABLED", "false")
    calls: list[str] = []

    class Plane:
        pass

    setattr(Plane, pause_guard.SCHEDULE_GUARD_ATTR, True)

    async def canonical(_self, *, from_block: int, to_block: int):
        calls.append("canonical")
        return []

    async def observed(_self, *, from_block: int, to_block: int):
        calls.append("observed")
        return await canonical(_self, from_block=from_block, to_block=to_block)

    def fake_factory(_original):
        return observed

    monkeypatch.setattr(pause_guard, "_ORIGINAL_FETCH_FACTORY", fake_factory)
    guarded = pause_guard._guarded_fetch_factory(canonical)
    asyncio.run(guarded(Plane(), from_block=1, to_block=2))

    assert calls == ["canonical"]
