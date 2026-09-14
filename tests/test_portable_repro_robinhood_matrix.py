from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPLAY = ROOT / "diagnostics" / "portable_repro" / "robinhood_replay.py"


def _load():
    spec = importlib.util.spec_from_file_location("portable_robinhood_matrix", REPLAY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_required_fault_matrix_is_explicit():
    mod = _load()
    required = {
        "steady",
        "alchemy-429-then-drpc",
        "alchemy-500-then-drpc",
        "alchemy-timeout-then-drpc",
        "alchemy-retry-then-success",
        "alchemy-cancel-delay",
        "drpc-503-then-alchemy",
        "drpc-timeout-then-alchemy",
        "concurrency-burst",
    }
    assert required <= set(mod.SCENARIOS)


def test_alchemy_timeout_and_5xx_are_deterministic_first_attempt_faults():
    mod = _load()
    timeout = mod.scenario_action(
        "alchemy-timeout-then-drpc", provider="alchemy", method="eth_chainId", count=1, fault_delay=7.5
    )
    assert timeout.status == 200
    assert timeout.delay_seconds == 7.5
    error = mod.scenario_action(
        "alchemy-500-then-drpc", provider="alchemy", method="eth_chainId", count=1
    )
    assert error.status == 500
    assert error.error_code == -32603


def test_retry_fault_occurs_once_then_recovers():
    mod = _load()
    first = mod.scenario_action(
        "alchemy-retry-then-success", provider="alchemy", method="eth_chainId", count=1
    )
    second = mod.scenario_action(
        "alchemy-retry-then-success", provider="alchemy", method="eth_chainId", count=2
    )
    assert first.status == 503
    assert second.status == 200
    assert second.delay_seconds == 0


def test_reverse_failover_scenarios_recommend_drpc_primary():
    mod = _load()
    assert mod.RECOMMENDED_PRIMARY["drpc-503-then-alchemy"] == "drpc"
    assert mod.RECOMMENDED_PRIMARY["drpc-timeout-then-alchemy"] == "drpc"
    fault = mod.scenario_action(
        "drpc-503-then-alchemy", provider="drpc", method="eth_chainId", count=1
    )
    assert fault.status == 503


def test_cancellation_scenario_is_delay_driven_not_thread_driven():
    mod = _load()
    action = mod.scenario_action(
        "alchemy-cancel-delay", provider="alchemy", method="eth_chainId", count=1, fault_delay=11.0
    )
    assert action.delay_seconds == 11.0
    source = REPLAY.read_text(encoding="utf-8")
    assert "ThreadingHTTPServer" not in source
    assert "asyncio.start_server" in source


def test_concurrency_counter_tracks_peak_without_spawning_workers():
    mod = _load()
    state = mod.ReplayState(scenario="concurrency-burst")
    assert state.enter_http() == (1, 1)
    assert state.enter_http() == (2, 2)
    state.exit_http()
    state.exit_http()
    assert state.active_http == 0
    assert state.max_active_http == 2
