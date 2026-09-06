from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi.direct_solana import WatchTarget
from solana_roi import post224_frontier_candidate_hardening as repair


def test_program_firehose_isolated_from_scout_socket() -> None:
    targets = tuple(
        [WatchTarget("scout", f"scout-{i}", None) for i in range(3)]
        + [WatchTarget("program", f"program-{i}", "RAYDIUM") for i in range(7)]
    )
    shards = repair._isolated_target_shards(targets, "provider-a")
    assert len(shards) == 5
    assert {row.kind for row in shards[0]} == {"scout"}
    assert len(shards[0]) == 3
    assert all(len(shard) <= 2 for shard in shards[1:])
    assert all(row.kind != "scout" for shard in shards[1:] for row in shard)
    assert sorted(row.address for shard in shards for row in shard) == sorted(row.address for row in targets)


def test_hybrid_timing_uses_tighter_valid_conservative_bound(monkeypatch) -> None:
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_WS_LAG",
        lambda store, *, signature, created_at, max_age_seconds: (12.0, "websocket-bound"),
    )
    monkeypatch.setattr(
        repair.reference,
        "_reference_lag_seconds",
        lambda store, *, signature, created_at: (1.4, "confirmed-head-bound"),
    )
    lag, proof = repair._hybrid_frontier_lag_seconds(
        object(),
        signature="sig",
        created_at=datetime.now(timezone.utc),
        max_age_seconds=3.0,
    )
    assert lag == 1.4
    assert proof == "hybrid:confirmed-head-bound"


def test_hybrid_timing_remains_fail_closed_without_valid_proof(monkeypatch) -> None:
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_WS_LAG",
        lambda store, *, signature, created_at, max_age_seconds: (None, "ws-missing"),
    )
    monkeypatch.setattr(
        repair.reference,
        "_reference_lag_seconds",
        lambda store, *, signature, created_at: (None, "reference-missing"),
    )
    lag, proof = repair._hybrid_frontier_lag_seconds(
        object(),
        signature="sig",
        created_at=datetime.now(timezone.utc),
        max_age_seconds=3.0,
    )
    assert lag is None
    assert "ws-missing" in proof
    assert "reference-missing" in proof


def test_multi_scout_presence_recovers_only_unique_economic_actor(monkeypatch) -> None:
    plane = SimpleNamespace(scout_wallets=("A", "B"))
    result = {
        "transaction": {
            "message": {
                "accountKeys": [
                    {"pubkey": "A", "signer": False},
                    {"pubkey": "B", "signer": False},
                ]
            }
        }
    }
    expected = object()
    monkeypatch.setattr(repair, "_ORIGINAL_NORMALIZE", lambda *args, **kwargs: None)

    def normalize(result, *, signature, trigger_received_at, wallet, source_hint=None):
        return (expected, None) if wallet == "A" else (None, "no-economic-movement")

    monkeypatch.setattr(repair.scout, "_normalize_tracked_wallet", normalize)
    token = repair.scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        observed = repair._normalize_with_independent_multi_scout_resolution(
            result,
            signature="sig",
            trigger_received_at=datetime.now(timezone.utc),
        )
    finally:
        repair.scout._SCOUT_HYDRATION_PLANE.reset(token)
    assert observed is expected
    assert plane._roi_post224_frontier_multi_scout_transactions == 1
    assert plane._roi_post224_frontier_multi_scout_single_economic_actor_resolved == 1


def test_multi_scout_two_economic_actors_remains_fail_closed(monkeypatch) -> None:
    plane = SimpleNamespace(scout_wallets=("A", "B"))
    result = {
        "transaction": {
            "message": {
                "accountKeys": [
                    {"pubkey": "A", "signer": False},
                    {"pubkey": "B", "signer": False},
                ]
            }
        }
    }
    monkeypatch.setattr(repair, "_ORIGINAL_NORMALIZE", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        repair.scout,
        "_normalize_tracked_wallet",
        lambda *args, **kwargs: (SimpleNamespace(), None),
    )
    token = repair.scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        observed = repair._normalize_with_independent_multi_scout_resolution(
            result,
            signature="sig",
            trigger_received_at=datetime.now(timezone.utc),
        )
    finally:
        repair.scout._SCOUT_HYDRATION_PLANE.reset(token)
    assert observed is None
    assert plane._roi_post224_frontier_multiple_tracked_scouts_economically_active == 1


def test_confirmed_reference_sampler_is_single_flight_standby(monkeypatch) -> None:
    calls = 0

    async def sample(_self):
        nonlocal calls
        calls += 1
        if calls >= 2:
            stop.set()

    monkeypatch.setattr(repair.reference, "_sample_reference_once", sample)
    monkeypatch.setattr(repair, "REFERENCE_SAMPLE_INTERVAL_SECONDS", 0.001)
    stop = asyncio.Event()
    asyncio.run(repair._confirmed_reference_sampler(object(), stop))
    assert calls == 2


def test_repair_constants_preserve_strategy_and_authority_boundaries() -> None:
    assert repair.REFERENCE_SAMPLE_INTERVAL_SECONDS == 0.5
    assert repair.PROGRAM_TARGETS_PER_SOCKET == 2
