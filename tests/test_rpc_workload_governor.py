from __future__ import annotations

from solana_roi.rpc_workload_governor import (
    WORKLOAD_CERTIFICATION,
    WORKLOAD_CRITICAL,
    WORKLOAD_RESEARCH,
    _EndpointGovernorState,
    _allowed,
    _policy,
    current_rpc_workload,
    rpc_workload,
)


def test_governor_reserves_one_endpoint_slot_for_critical_work(monkeypatch):
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_TOTAL_PER_ENDPOINT", "3")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_CRITICAL_RESERVED_PER_ENDPOINT", "1")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MAX_PER_ENDPOINT", "1")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MIN_INTERVAL_SECONDS", "0")
    policy = _policy()
    state = _EndpointGovernorState(loop_id=1, endpoint_key="https://rpc.example")
    state.active_total = 2
    state.active_by_workload[WORKLOAD_CERTIFICATION] = 1
    state.active_by_workload[WORKLOAD_RESEARCH] = 1

    certification_allowed, _ = _allowed(state, WORKLOAD_CERTIFICATION, policy)
    research_allowed, _ = _allowed(state, WORKLOAD_RESEARCH, policy)
    critical_allowed, _ = _allowed(state, WORKLOAD_CRITICAL, policy)

    assert certification_allowed is False
    assert research_allowed is False
    assert critical_allowed is True
    assert policy["noncritical_ceiling_per_endpoint"] == 2


def test_research_is_bounded_to_one_endpoint_slot(monkeypatch):
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_TOTAL_PER_ENDPOINT", "4")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_CRITICAL_RESERVED_PER_ENDPOINT", "1")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MAX_PER_ENDPOINT", "1")
    monkeypatch.setenv("SOLANA_ROI_RPC_GOVERNOR_RESEARCH_MIN_INTERVAL_SECONDS", "0")
    policy = _policy()
    state = _EndpointGovernorState(loop_id=1, endpoint_key="https://rpc.example")
    state.active_total = 1
    state.active_by_workload[WORKLOAD_RESEARCH] = 1

    allowed, _ = _allowed(state, WORKLOAD_RESEARCH, policy)
    certification_allowed, _ = _allowed(state, WORKLOAD_CERTIFICATION, policy)

    assert allowed is False
    assert certification_allowed is True


def test_workload_context_is_scoped_and_defaults_to_certification():
    assert current_rpc_workload() == WORKLOAD_CERTIFICATION
    with rpc_workload(WORKLOAD_RESEARCH):
        assert current_rpc_workload() == WORKLOAD_RESEARCH
        with rpc_workload(WORKLOAD_CRITICAL):
            assert current_rpc_workload() == WORKLOAD_CRITICAL
        assert current_rpc_workload() == WORKLOAD_RESEARCH
    assert current_rpc_workload() == WORKLOAD_CERTIFICATION
