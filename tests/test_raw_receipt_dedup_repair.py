from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import raw_receipt_dedup_repair as repair


def _message(signature: str, *, slot: int = 123, logs: list[str] | None = None) -> dict:
    return {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {
                    "signature": signature,
                    "err": None,
                    "logs": logs or [],
                },
            },
        },
    }


def test_dedup_key_matches_durable_source_semantics() -> None:
    plane = SimpleNamespace()
    assert repair._first_durable_copy(plane, ("PUMP_FUN", "sig-a"), 100.0) is True
    assert repair._first_durable_copy(plane, ("PUMP_FUN", "sig-a"), 101.0) is False
    # The journal uniqueness constraint is signature + source_key, so the same
    # signature under a different durable source must remain independently admitted.
    assert repair._first_durable_copy(plane, ("RAYDIUM", "sig-a"), 101.0) is True


def test_duplicate_provider_copy_is_suppressed_before_durable_handler() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, str]] = []

        async def original(self, provider, targets, message):
            calls.append((provider, message["params"]["result"]["value"]["signature"]))

        wrapped = repair._deduplicating_handler(original)
        target = SimpleNamespace(kind="program", address="program-a", source_hint="PUMP_FUN")
        plane = SimpleNamespace(_launch_like=lambda logs: False)
        targets = {1: target}

        await wrapped(plane, "publicnode", targets, _message("sig-a", slot=123))
        await wrapped(plane, "solana-mainnet", targets, _message("sig-a", slot=123))

        assert calls == [("publicnode", "sig-a")]
        assert plane._roi_raw_receipt_dedup_duplicates_suppressed == 1
        assert plane._roi_raw_receipt_dedup_unique_admitted == 1
        # The second provider remains useful chain-frontier evidence even though its
        # duplicate durable copy is unnecessary.
        assert plane._roi_launch_ws_frontier_state["solana-mainnet"]["slot"] == 123

    asyncio.run(scenario())


def test_launch_duplicate_does_not_replace_first_durable_copy() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def original(self, provider, targets, message):
            calls.append(provider)

        wrapped = repair._deduplicating_handler(original)
        target = SimpleNamespace(kind="program", address="program-a", source_hint="PUMP_AMM")
        plane = SimpleNamespace(_launch_like=lambda logs: "launch" in logs)
        targets = {1: target}

        await wrapped(plane, "publicnode", targets, _message("launch-a", logs=["launch"]))
        await wrapped(plane, "solana-mainnet", targets, _message("launch-a", logs=["launch"]))

        assert calls == ["publicnode"]
        assert plane._roi_raw_receipt_dedup_launch_duplicates_suppressed == 1
        assert repair.DEDUP_WINDOW_SECONDS == 60.0
        assert repair.DEDUP_MAX_KEYS == 16_384

    asyncio.run(scenario())
