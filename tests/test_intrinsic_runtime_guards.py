from __future__ import annotations

import json

from solana_roi import api
from solana_roi import direct_solana as direct_solana_module
from solana_roi import solana_rpc as solana_rpc_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane
from solana_roi.observation_store import ObservationEventStore


def test_guards_install_even_when_legacy_api_entrypoint_is_imported():
    assert bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_cooperative_yield", False))
    assert bool(getattr(direct_solana_module.websockets.connect, "_roi_memory_bounded", False))
    assert bool(getattr(direct_solana_module.websockets.connect, "_roi_frame_resilient", False))
    assert bool(getattr(DirectSolanaIngestionPlane._prefill_launch_context, "_roi_memory_bounded", False))
    assert bool(getattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_sequential_subscription_setup", False))
    assert bool(getattr(DirectSolanaIngestionPlane._hydrate_one, "_roi_priority_routed", False))
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_worker_partitioned", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_subscription_telemetry", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_transport_hardened", False))
    assert bool(getattr(direct_solana_module.rpc_endpoints_from_env, "_roi_official_secondary", False))
    assert bool(getattr(solana_rpc_module.rpc_endpoints_from_env, "_roi_official_secondary", False))


def test_known_failed_shared_secondary_is_replaced_for_stream_and_rpc_pool():
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
    stream_endpoints = direct_solana_module.rpc_endpoints_from_env(env)
    rpc_endpoints = solana_rpc_module.rpc_endpoints_from_env(env)
    assert [endpoint.name for endpoint in stream_endpoints] == ["publicnode", "solana-mainnet"]
    assert stream_endpoints == rpc_endpoints
    assert stream_endpoints[0].http_url == "https://solana-rpc.publicnode.com"
    assert stream_endpoints[1].http_url == "https://api.mainnet.solana.com"
    assert stream_endpoints[1].ws_url == "wss://api.mainnet.solana.com"


def test_memory_boundary_is_visible_without_production_wrapper(tmp_path):
    class Rpc:
        @staticmethod
        def status():
            return {"configured": True}

    class Journal:
        @staticmethod
        def status():
            return {
                "connected_provider_count": 1,
                "continuity_ok": True,
                "unresolved_gap": False,
                "provider_states": [],
            }

    store = ObservationEventStore(tmp_path / "intrinsic-status.sqlite3")
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS direct_solana_hydration_metrics ("
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

    status = DirectSolanaIngestionPlane.status(plane)
    boundary = status["production_memory_boundary"]
    assert boundary["installed_intrinsically"] is True
    assert boundary["websocket_max_queue"] == 64
    assert boundary["websocket_max_size_bytes"] == 1024 * 1024
    assert boundary["receive_payload_ceiling_bytes_per_provider"] == 64 * 1024 * 1024
    assert boundary["candidate_context_slots"] == 3
    assert boundary["background_context_slots"] == 1
    assert boundary["strategy_scope_reduced"] is False
    assert boundary["context_signature_limit_unchanged"] == 600
    assert boundary["hydration_worker_count_unchanged"] == 12
    throughput = status["throughput_policy"]
    assert throughput["candidate_reserved_workers"] == 3
    assert throughput["background_workers"] == 9
    assert throughput["full_raw_market_scope_preserved"] is True
    policy = status["provider_runtime_policy"]
    assert policy["subscription_setup_mode"] == "sequential_ack_with_bounded_retry"
    assert policy["high_volume_programs_subscribed_last"] is True
    assert policy["full_target_count_unchanged"] == 10


def test_legacy_health_route_is_constant_time_liveness(monkeypatch):
    def must_not_build_runtime():
        raise AssertionError("liveness route must not touch the runtime or SQLite")

    monkeypatch.setattr(api, "ingestion_runtime", must_not_build_runtime)
    payload = api.health()
    assert payload == {
        "status": "ok",
        "liveness_only": True,
        "paper_only": True,
        "live_money_authority": False,
        "strategy_version": "roi-convergence-v3.1-forward-1",
    }
