from __future__ import annotations

import json

from solana_roi import api
from solana_roi import direct_solana as direct_solana_module
from solana_roi.direct_solana import DirectSolanaIngestionPlane


def test_guards_install_even_when_legacy_api_entrypoint_is_imported():
    assert bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_cooperative_yield", False))
    assert bool(getattr(direct_solana_module.websockets.connect, "_roi_memory_bounded", False))
    assert bool(getattr(DirectSolanaIngestionPlane._prefill_launch_context, "_roi_memory_bounded", False))
    assert bool(getattr(DirectSolanaIngestionPlane._stream_endpoint, "_roi_stream_guarded", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", False))
    assert bool(getattr(direct_solana_module.rpc_endpoints_from_env, "_roi_provider_repair", False))


def test_known_failing_public_onfinality_is_replaced_without_touching_primary():
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
    endpoints = direct_solana_module.rpc_endpoints_from_env(env)
    assert [endpoint.name for endpoint in endpoints] == ["publicnode", "drpc"]
    assert endpoints[0].http_url == "https://solana-rpc.publicnode.com"
    assert endpoints[1].http_url == "https://solana.drpc.org/"
    assert endpoints[1].ws_url == "wss://solana.drpc.org"


def test_memory_boundary_is_visible_without_production_wrapper():
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
            }

    plane = object.__new__(DirectSolanaIngestionPlane)
    plane.enabled = True
    plane.scout_wallets = ("a", "b", "c")
    plane.rpc = Rpc()
    plane.journal = Journal()
    plane.candidate_context_max_signatures = 600
    plane.worker_count = 12

    status = DirectSolanaIngestionPlane.status(plane)
    boundary = status["production_memory_boundary"]
    assert boundary["installed_intrinsically"] is True
    assert boundary["websocket_max_queue"] == 64
    assert boundary["websocket_max_size_bytes"] == 256 * 1024
    assert boundary["candidate_context_slots"] == 3
    assert boundary["background_context_slots"] == 1
    assert boundary["strategy_scope_reduced"] is False
    assert boundary["context_signature_limit_unchanged"] == 600
    assert boundary["hydration_worker_count_unchanged"] == 12


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
