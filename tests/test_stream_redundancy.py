from __future__ import annotations

import asyncio

from solana_roi.direct_solana import DirectSolanaIngestionPlane
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint
from solana_roi.stream_redundancy import stream_only_endpoints_from_env


class FakeRpcPool:
    def status(self):
        return {
            "configured": True,
            "endpoint_count": 2,
            "redundant": True,
            "endpoints": [],
        }


def test_default_tertiary_is_wss_only_and_does_not_enter_hydration_pool(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLANA_ROI_STREAM_ONLY_ENDPOINTS_JSON", raising=False)
    store = ObservationEventStore(tmp_path / "stream-tertiary.sqlite3")
    rpc = FakeRpcPool()
    base = (
        RpcEndpoint("publicnode", "https://solana-rpc.publicnode.com", "wss://solana-rpc.publicnode.com"),
        RpcEndpoint("solana-mainnet", "https://api.mainnet.solana.com", "wss://api.mainnet.solana.com"),
    )

    plane = DirectSolanaIngestionPlane(
        store=store,
        service=object(),
        scout_wallets=("scout-a", "scout-b", "scout-c"),
        rpc_pool=rpc,
        endpoints=base,
    )
    try:
        assert [endpoint.name for endpoint in plane.endpoints] == [
            "publicnode",
            "solana-mainnet",
            "drpc-stream",
        ]
        assert plane.rpc is rpc
        status = plane.status()
        redundancy = status["stream_redundancy"]
        assert redundancy["stream_provider_count"] == 3
        assert redundancy["hydration_rpc_provider_count"] == 2
        assert redundancy["drpc_public_http_hydration_retired"] is True
        assert redundancy["drpc_public_wss_reintroduced_stream_only"] is True
        assert redundancy["stream_only_providers"] == [
            {
                "name": "drpc-stream",
                "ws_host": "solana.drpc.org",
                "http_hydration_enabled": False,
            }
        ]
        assert status["target_stream_fanout"]["provider_count"] == 3
        assert status["target_stream_fanout"]["total_websocket_target_streams"] == 30
        assert status["provider_runtime_policy"]["stream_and_hydration_provider_sets_decoupled"] is True
        assert status["provider_runtime_policy"]["tertiary_stream_can_authorize_hydration"] is False
        assert status["production_memory_boundary"]["receive_payload_ceiling_bytes_all_providers"] == 240 * 1024 * 1024
    finally:
        asyncio.run(plane._dex.aclose())


def test_stream_only_endpoint_override_is_validated_without_touching_base_rpc_env():
    rows = stream_only_endpoints_from_env(
        {
            "SOLANA_ROI_STREAM_ONLY_ENDPOINTS_JSON": (
                '[{"name":"custom-stream","http":"https://rpc.example.com/solana",'
                '"ws":"wss://rpc.example.com/solana"}]'
            )
        }
    )
    assert len(rows) == 1
    assert rows[0].name == "custom-stream"
    assert rows[0].ws_url == "wss://rpc.example.com/solana"


def test_stream_redundancy_guards_install_intrinsically():
    assert bool(getattr(DirectSolanaIngestionPlane.__init__, "_roi_stream_tertiary", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_stream_tertiary", False))
    # The new outer status wrapper must preserve all existing safety markers.
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_target_quorum", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_memory_bounded", False))
