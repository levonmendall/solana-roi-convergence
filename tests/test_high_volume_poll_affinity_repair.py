from __future__ import annotations

from types import SimpleNamespace

from solana_roi import continuity_storage_capacity_repair as storage
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.continuity_high_volume_poll_affinity_repair import (
    _assigned_endpoint_with_high_volume_affinity,
)
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


def _public_pair() -> tuple[RpcEndpoint, RpcEndpoint]:
    return (
        RpcEndpoint(
            "publicnode",
            "https://solana-rpc.publicnode.com",
            "wss://solana-rpc.publicnode.com",
        ),
        RpcEndpoint(
            "solana-mainnet",
            "https://api.mainnet.solana.com",
            "wss://api.mainnet.solana.com",
        ),
    )


def _index_assignment(plane, target):
    endpoints = storage._routine_endpoints(plane)
    if not endpoints:
        raise RuntimeError("no routine endpoints")
    return endpoints[storage._target_index(plane, target) % len(endpoints)]


def test_high_volume_pump_targets_avoid_official_routine_primary(monkeypatch):
    import solana_roi.continuity_high_volume_poll_affinity_repair as repair

    endpoints = _public_pair()
    targets = (
        WatchTarget("scout", "scout-a", None),
        WatchTarget("program", "pump-fun", "PUMP_FUN"),
        WatchTarget("program", "ray-a", "RAYDIUM"),
        WatchTarget("program", "pump-amm", "PUMP_AMM"),
    )
    plane = SimpleNamespace(
        rpc=SimpleNamespace(endpoints=endpoints),
        endpoints=endpoints,
        watch_targets=targets,
    )
    monkeypatch.setattr(repair, "_ORIGINAL_ASSIGNED_ENDPOINT", _index_assignment)

    assert _index_assignment(plane, targets[1]).name == "solana-mainnet"
    assert _index_assignment(plane, targets[3]).name == "solana-mainnet"
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[1]).name == "publicnode"
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[3]).name == "publicnode"

    # Lower-volume targets retain the exact pre-repair shard.
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[0]).name == _index_assignment(plane, targets[0]).name
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[2]).name == _index_assignment(plane, targets[2]).name


def test_high_volume_affinity_never_invents_nonofficial_capacity(monkeypatch):
    import solana_roi.continuity_high_volume_poll_affinity_repair as repair

    official = _public_pair()[1]
    target = WatchTarget("program", "pump-fun", "PUMP_FUN")
    plane = SimpleNamespace(
        rpc=SimpleNamespace(endpoints=(official,)),
        endpoints=(official,),
        watch_targets=(target,),
    )
    monkeypatch.setattr(repair, "_ORIGINAL_ASSIGNED_ENDPOINT", _index_assignment)

    assert _assigned_endpoint_with_high_volume_affinity(plane, target) is official


def test_production_composition_preserves_candidate_hedge_and_hard_continuity_bounds():
    from solana_roi.production import app  # noqa: F401

    # PR #91 already owns candidate-only public RPC hedging. This continuity repair
    # composes with it rather than recreating or replacing it.
    assert getattr(SolanaRpcPool.call_with_meta, "_roi_candidate_official_public_hedge", False) is True
    assert getattr(storage._assigned_endpoint, "_roi_high_volume_poll_affinity", False) is True
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_high_volume_poll_affinity", False) is True
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
