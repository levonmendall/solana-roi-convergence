from __future__ import annotations

import json

from solana_roi import direct_solana as direct_solana_module
from solana_roi import solana_rpc as solana_rpc_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane
from solana_roi.stream_resilience import (
    _error_parts,
    _retryable_subscription_error,
)


def test_shared_failed_secondary_is_replaced_by_official_solana_endpoint():
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
    endpoints = solana_rpc_module.rpc_endpoints_from_env(env)
    assert [endpoint.name for endpoint in endpoints] == ["publicnode", "solana-mainnet"]
    assert endpoints[1].http_url == "https://api.mainnet.solana.com"
    assert endpoints[1].ws_url == "wss://api.mainnet.solana.com"
    assert direct_solana_module.rpc_endpoints_from_env is solana_rpc_module.rpc_endpoints_from_env


def test_drpc_public_is_also_retired_but_custom_provider_is_not_rewritten():
    env = {
        "SOLANA_ROI_RPC_ENDPOINTS_JSON": json.dumps(
            [
                {
                    "name": "drpc",
                    "http": "https://solana.drpc.org/",
                    "ws": "wss://solana.drpc.org",
                },
                {
                    "name": "custom",
                    "http": "https://rpc.example.com/solana",
                    "ws": "wss://rpc.example.com/solana",
                },
            ]
        )
    }
    endpoints = solana_rpc_module.rpc_endpoints_from_env(env)
    assert [endpoint.name for endpoint in endpoints] == ["solana-mainnet", "custom"]
    assert endpoints[1].http_url == "https://rpc.example.com/solana"


def test_subscription_setup_is_sequential_resilient_and_error_telemetry_is_bounded():
    assert bool(getattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_sequential_subscription_setup", False))
    code, message = _error_parts({"code": -32005, "message": "  Too   many requests\nretry later  "})
    assert code == -32005
    assert message == "Too many requests retry later"
    assert _retryable_subscription_error(code, message) is True
    assert _retryable_subscription_error(-32602, "invalid params") is False


def test_provider_status_wrapper_exposes_current_five_minute_window(tmp_path):
    from solana_roi.observation_store import ObservationEventStore

    class Rpc:
        @staticmethod
        def status():
            return {"configured": True, "endpoint_count": 2, "redundant": True, "endpoints": []}

    class Journal:
        @staticmethod
        def status():
            return {
                "connected_provider_count": 1,
                "provider_states": [],
                "continuity_ok": True,
                "unresolved_gap": False,
                "outage_started_at": None,
                "last_backfill_complete_at": None,
                "last_backfill_error": None,
                "hydration_queue": {},
                "raw_receipts_last_hour_by_source": {},
                "hydration_sample_count": 0,
                "hydration_normalized_count": 0,
                "p95_hydration_ms": None,
            }

    store = ObservationEventStore(tmp_path / "status.sqlite3")
    # DirectSolanaJournal normally creates this table before status is exposed.
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE direct_solana_hydration_metrics ("
            "signature TEXT PRIMARY KEY, source TEXT, trigger_received_at TEXT NOT NULL, hydrated_at TEXT NOT NULL, "
            "rpc_provider TEXT, rpc_latency_ms REAL, total_hydration_ms REAL NOT NULL, normalized INTEGER NOT NULL, "
            "candidate_context_prefilled INTEGER NOT NULL DEFAULT 0, historical_recovery INTEGER NOT NULL DEFAULT 0)"
        )

    plane = object.__new__(DirectSolanaIngestionPlane)
    plane.enabled = True
    plane.scout_wallets = ("a", "b", "c")
    plane.rpc = Rpc()
    plane.journal = Journal()
    plane.store = store
    plane.endpoints = ()
    plane.candidate_context_max_signatures = 600
    plane.worker_count = 12
    payload = DirectSolanaIngestionPlane.status(plane)
    assert payload["provider_runtime_policy"]["subscription_setup_mode"] == "sequential_ack_with_bounded_retry"
    assert payload["provider_runtime_policy"]["no_cost_secondary"] == "solana-mainnet-public"
    assert payload["recent_hydration_5m"] == {"sample_count": 0, "normalized_count": 0, "p95_ms": None}
