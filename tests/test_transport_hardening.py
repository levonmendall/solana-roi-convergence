from __future__ import annotations

from solana_roi.deployment import FROZEN_PROGRAM_ADDRESSES
from solana_roi.direct_solana import DirectSolanaIngestionPlane
from solana_roi.transport_hardening import (
    STREAM_WS_MAX_QUEUE,
    STREAM_WS_MAX_SIZE_BYTES,
    _transport_kwargs,
)


def test_transport_envelope_lifts_old_frame_limit_without_reopening_queue():
    values = _transport_kwargs({"max_queue": 8192, "max_size": 256 * 1024})
    assert values["max_queue"] == STREAM_WS_MAX_QUEUE == 64
    assert values["max_size"] == STREAM_WS_MAX_SIZE_BYTES == 1024 * 1024


def test_subscription_setup_preserves_full_scope_and_puts_high_volume_programs_last():
    plane = object.__new__(DirectSolanaIngestionPlane)
    plane.scout_wallets = ("scout-c", "scout-a", "scout-b")

    targets = plane.watch_targets
    assert len(targets) == len(FROZEN_PROGRAM_ADDRESSES) + 3
    assert {row.address for row in targets if row.kind == "program"} == set(FROZEN_PROGRAM_ADDRESSES)
    assert {row.address for row in targets if row.kind == "scout"} == {"scout-a", "scout-b", "scout-c"}
    assert [row.kind for row in targets[:3]] == ["scout", "scout", "scout"]

    program_sources = [str(row.source_hint) for row in targets if row.kind == "program"]
    assert program_sources[-2:] == ["PUMP_AMM", "PUMP_FUN"]
    assert all(source == "RAYDIUM" for source in program_sources[:-2])
