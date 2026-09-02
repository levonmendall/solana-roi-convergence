from __future__ import annotations

import asyncio
from dataclasses import dataclass

from solana_roi import production
from solana_roi import direct_solana as direct_solana_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane


def test_websocket_receive_buffer_is_hard_clamped_without_scope_change():
    captured = {}

    def raw_connect(*args, **kwargs):
        captured.update(kwargs)
        return object()

    connect = production._bounded_ws_connect(raw_connect)
    connect("wss://example.invalid", max_queue=8192, max_size=4 * 1024 * 1024)

    assert captured["max_queue"] == production.DIRECT_WS_MAX_QUEUE == 64
    assert captured["max_size"] == production.DIRECT_WS_MAX_SIZE_BYTES == 256 * 1024
    assert bool(getattr(connect, "_roi_memory_bounded", False))


@dataclass
class Candidate:
    wallet: str
    side: str = "buy"


class Registry:
    def get(self, wallet):
        return object() if wallet.startswith("candidate-") else None


class Service:
    registry = Registry()


class Plane:
    service = Service()


def test_context_expansion_reserves_candidate_capacity_and_serializes_background():
    active = {"candidate": 0, "background": 0}
    peaks = {"candidate": 0, "background": 0}

    async def original(self, candidate):
        kind = "candidate" if candidate.wallet.startswith("candidate-") else "background"
        active[kind] += 1
        peaks[kind] = max(peaks[kind], active[kind])
        await asyncio.sleep(0.02)
        active[kind] -= 1
        return True

    wrapped = production._bounded_context_prefill(original)
    plane = Plane()

    async def exercise():
        jobs = [wrapped(plane, Candidate(f"candidate-{index}")) for index in range(7)]
        jobs += [wrapped(plane, Candidate(f"background-{index}")) for index in range(5)]
        return await asyncio.gather(*jobs)

    assert all(asyncio.run(exercise()))
    assert peaks["candidate"] == production.DIRECT_CANDIDATE_CONTEXT_SLOTS == 3
    assert peaks["background"] == production.DIRECT_BACKGROUND_CONTEXT_SLOTS == 1


def test_production_memory_guards_are_installed_once():
    assert bool(getattr(direct_solana_module.websockets.connect, "_roi_memory_bounded", False))
    assert bool(getattr(DirectSolanaIngestionPlane._prefill_launch_context, "_roi_memory_bounded", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", False))

    before_connect = direct_solana_module.websockets.connect
    before_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    before_status = DirectSolanaIngestionPlane.status
    production.install_direct_stream_memory_bounds()
    assert direct_solana_module.websockets.connect is before_connect
    assert DirectSolanaIngestionPlane._prefill_launch_context is before_prefill
    assert DirectSolanaIngestionPlane.status is before_status
