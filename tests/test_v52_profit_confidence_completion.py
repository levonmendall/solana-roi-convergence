from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI

from solana_roi import v52_profit_confidence_completion as completion
from solana_roi import v52_profit_confidence_finalization as finalization
from solana_roi import v52_strategy_api as strategy_api


def _profile(target: float) -> dict[str, object]:
    return {
        "elite_wallet_continuation": {
            "v52_authority": {
                "lane_cap_preserved": True,
                "target_fraction": target,
                "final_fraction": target,
            }
        }
    }


def test_completion_policy_preserves_nonnegotiable_safety() -> None:
    policy = completion.completion_policy()
    assert policy["paper_only"] is True
    assert policy["live_money_authority"] is False
    assert policy["signing_available"] is False
    assert policy["transaction_submission_available"] is False
    assert float(policy["minimum_exit_depth_coverage_ratio"]) >= 2.0
    assert float(policy["absolute_chase_max_fraction"]) <= 0.80
    assert int(policy["max_sizing_quote_rounds"]) <= 2
    assert float(policy["paper_nav_usd"]) == 500.0
    assert int(policy["max_concurrent_positions"]) == 3


def test_final_solana_guard_uses_numeric_lane_cap_not_boolean_marker(monkeypatch) -> None:
    finalization._BASE_SOLANA_CHOOSE = lambda adapter, pre, chase=None, latency=None: (
        "elite_wallet_continuation",
        0.75,
        _profile(0.75),
    )
    monkeypatch.setattr(finalization.authoritative, "_lane_cap", lambda lane, severity: 0.20)
    adapter = SimpleNamespace()
    lane, fraction, profiles = finalization._final_solana_choose(
        adapter,
        {"risk": {"risk_severity": 0.0}},
        chase=0.10,
        latency=2.0,
    )
    assert lane == "elite_wallet_continuation"
    assert fraction == 0.20
    auth = profiles[lane]["v52_authority"]
    assert auth["numeric_lane_cap_guard"] is True
    assert auth["numeric_lane_cap_fraction"] == 0.20
    assert auth["target_fraction"] == 0.20
    assert auth["final_fraction"] == 0.20


def test_final_solana_guard_enforces_absolute_signal_age(monkeypatch) -> None:
    finalization._BASE_SOLANA_CHOOSE = lambda adapter, pre, chase=None, latency=None: (
        "elite_wallet_continuation",
        0.05,
        _profile(0.05),
    )
    monkeypatch.setattr(finalization.authoritative, "_lane_cap", lambda lane, severity: 0.20)
    observed = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    received = observed + timedelta(seconds=121)
    lane, fraction, profiles = finalization._final_solana_choose(
        SimpleNamespace(),
        {
            "risk": {"risk_severity": 0.0},
            "observed_at": observed.isoformat(),
            "received_at": received.isoformat(),
        },
        chase=0.10,
        latency=2.0,
    )
    assert lane is None
    assert fraction == 0.0
    auth = profiles["elite_wallet_continuation"]["v52_authority"]
    assert auth["absolute_signal_age_guard"] is True
    assert auth["reason"] == "deferred_absolute_signal_age_limit"


def test_final_solana_guard_enforces_absolute_chase_and_latency(monkeypatch) -> None:
    finalization._BASE_SOLANA_CHOOSE = lambda adapter, pre, chase=None, latency=None: (
        "elite_wallet_continuation",
        0.05,
        _profile(0.05),
    )
    monkeypatch.setattr(finalization.authoritative, "_lane_cap", lambda lane, severity: 0.20)
    lane, fraction, profiles = finalization._final_solana_choose(
        SimpleNamespace(),
        {"risk": {"risk_severity": 0.0}},
        chase=0.81,
        latency=20.01,
    )
    assert lane is None
    assert fraction == 0.0
    auth = profiles["elite_wallet_continuation"]["v52_authority"]
    assert float(auth["absolute_chase_max_fraction"]) <= 0.80
    assert float(auth["latency_hard_max_seconds"]) <= 20.0


def test_final_fomo_guard_preserves_five_percent_cap() -> None:
    finalization._BASE_FOMO_DECISION = lambda adapter, observation, trial: {
        "decision": "paper_enter_test",
        "reason": "test",
        "position_fraction": 0.50,
        "profile": {},
    }
    result = finalization._final_fomo_decision(
        SimpleNamespace(),
        observation={},
        trial={"signal_to_entry_seconds": 1.0},
    )
    assert 0.0 < float(result["position_fraction"]) <= 0.05
    assert result["v52_authority"]["numeric_lane_cap_guard"] is True


def test_strategy_api_exposes_read_only_profit_reports() -> None:
    app = FastAPI()
    strategy_api._INSTALLED = False
    strategy_api.install_v52_strategy_api(app)
    methods = {
        route.path: set(route.methods or ())
        for route in app.routes
        if getattr(route, "path", None)
    }
    assert methods[strategy_api.PERFORMANCE_24H_PATH] == {"GET"}
    assert methods[strategy_api.PERFORMANCE_7D_PATH] == {"GET"}
    status = strategy_api.status()
    assert status["performance_reports_read_only"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False


def test_finalization_status_is_fail_closed_and_paper_only() -> None:
    payload = finalization.status()
    assert payload["numeric_lane_cap_guard"] is True
    assert payload["absolute_signal_age_guard"] is True
    assert payload["absolute_signal_age_max_seconds"] <= 120.0
    assert payload["absolute_chase_max_fraction"] <= 0.80
    assert payload["latency_hard_max_seconds"] <= 20.0
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
