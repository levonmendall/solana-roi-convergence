from __future__ import annotations

from types import SimpleNamespace

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
