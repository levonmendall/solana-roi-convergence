from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import direct_solana as direct_solana_module
from solana_roi import runtime_guards
from solana_roi import solana_rpc as solana_rpc_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane, DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore


PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


def test_rpc_pool_and_stream_share_the_same_repaired_endpoint_factory():
    env = {
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": json.dumps(
            [
                {
                    "name": "publicnode",
                    "http": "https://solana-rpc.publicnode.com",
                    "ws": "wss://solana-rpc.publicnode.com",
                },
                {
                    "name": "onfinality",
                    "http": "https://solana.api.onfinality.io/public",
                    "ws": "wss://solana.api.onfinality.io/public-ws",
                },
            ]
        )
    }
    rpc_endpoints = solana_rpc_module.rpc_endpoints_from_env(env)
    stream_endpoints = direct_solana_module.rpc_endpoints_from_env(env)
    assert [row.name for row in rpc_endpoints] == ["publicnode", "drpc"]
    assert rpc_endpoints == stream_endpoints


def test_subscription_ids_are_provider_type_agnostic():
    assert runtime_guards._subscription_key(123) == "123"
    assert runtime_guards._subscription_key("provider-subscription-id") == "provider-subscription-id"


def test_nested_token_initialize_is_not_misclassified_as_launch():
    logs = [
        f"Program {PUMP} invoke [1]",
        f"Program {TOKEN} invoke [2]",
        "Program log: Instruction: InitializeAccount3",
        f"Program {TOKEN} success",
        "Program log: Instruction: Buy",
        f"Program {PUMP} success",
    ]
    assert runtime_guards._precise_launch_like(logs) is False


def test_frozen_program_own_create_and_initialize_are_launch_like():
    pump_logs = [
        f"Program {PUMP} invoke [1]",
        "Program log: Instruction: Create",
        f"Program {PUMP} success",
    ]
    raydium_logs = [
        f"Program {RAYDIUM} invoke [1]",
        "Program log: Instruction: Initialize2",
        f"Program {RAYDIUM} success",
    ]
    assert runtime_guards._precise_launch_like(pump_logs) is True
    assert runtime_guards._precise_launch_like(raydium_logs) is True


def test_worker_claim_partition_reserves_candidate_lane(tmp_path):
    store = ObservationEventStore(tmp_path / "partition.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="background",
        slot=1,
        trigger_received_at=now,
        source_hint="PUMP_FUN",
        priority=20,
        reason="deterministic_market_sample",
    )
    journal.enqueue(
        signature="candidate",
        slot=2,
        trigger_received_at=now,
        source_hint="PUMP_FUN",
        priority=0,
        reason="frozen_scout_processed_trigger",
    )
    fast = runtime_guards._claim_priority(journal, fast_only=True)
    background = runtime_guards._claim_priority(journal, fast_only=False)
    assert fast is not None and fast["signature"] == "candidate"
    assert background is not None and background["signature"] == "background"


def test_stale_background_is_failed_closed_without_expiring_candidate(tmp_path):
    store = ObservationEventStore(tmp_path / "expiry.sqlite3")
    journal = DirectSolanaJournal(store)
    old = datetime.now(timezone.utc) - timedelta(seconds=runtime_guards.DIRECT_STALE_BACKGROUND_SECONDS + 10)
    journal.enqueue(
        signature="old-background",
        slot=1,
        trigger_received_at=old,
        source_hint="RAYDIUM",
        priority=10,
        reason="prospective_launch",
    )
    journal.enqueue(
        signature="old-candidate",
        slot=2,
        trigger_received_at=old,
        source_hint="RAYDIUM",
        priority=0,
        reason="frozen_scout_processed_trigger",
    )
    plane = SimpleNamespace(store=store)
    assert runtime_guards._expire_stale_background(plane) == 1
    with store._lock:
        rows = {
            str(row["signature"]): str(row["status"])
            for row in store.db.execute(
                "SELECT signature, status FROM direct_solana_hydration_queue ORDER BY signature"
            ).fetchall()
        }
    assert rows == {"old-background": "failed", "old-candidate": "pending"}


def test_random_market_sample_normalizes_without_deep_risk(monkeypatch):
    swap = SimpleNamespace(
        source="solana-direct:PUMP_FUN:buy",
        wallet="unrelated-wallet",
        side="buy",
    )
    monkeypatch.setattr(direct_solana_module, "normalize_standard_transaction", lambda *args, **kwargs: swap)

    class Registry:
        @staticmethod
        def get(wallet):
            return None

    class Service:
        registry = Registry()

        async def ingest_swap(self, value):
            raise AssertionError("random market sample must not invoke deep risk collection")

    class Journal:
        def __init__(self):
            self.finished = False
            self.recorded = False

        def record_hydration(self, **kwargs):
            self.recorded = True

        def finish(self, signature, **kwargs):
            self.finished = True

    class Plane:
        service = Service()
        journal = Journal()

        async def _get_transaction_ready(self, signature, *, hedge, attempts):
            return {"transaction": True}, "publicnode", 10.0

        def _persist_context_swap(self, value):
            self.persisted = value

        async def _prefill_launch_context(self, value):
            raise AssertionError("random market sample must not prefill launch context")

    plane = Plane()
    now = datetime.now(timezone.utc)
    row = {
        "signature": "sample",
        "trigger_received_at": now.isoformat(),
        "priority": 20,
        "reason": "deterministic_market_sample",
        "source_hint": "PUMP_FUN",
        "attempts": 0,
    }
    asyncio.run(runtime_guards._priority_routed_hydrate(plane, row))
    assert plane.persisted is swap
    assert plane.journal.recorded is True
    assert plane.journal.finished is True


def test_runtime_guards_install_priority_routing_and_worker_partition():
    assert bool(getattr(DirectSolanaIngestionPlane._launch_like, "_roi_program_scoped_launch_detection", False))
    assert bool(getattr(DirectSolanaIngestionPlane._hydrate_one, "_roi_priority_routed", False))
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_worker_partitioned", False))
