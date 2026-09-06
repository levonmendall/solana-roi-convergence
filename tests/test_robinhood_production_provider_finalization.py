from __future__ import annotations

from pathlib import Path

from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_production_provider_finalizer as finalizer
from solana_roi import robinhood_production_ws_transport as transport
from solana_roi import v51_production_authority
from solana_roi.strategy_v51_authority import authority


def test_v51_latency_is_final_robinhood_transport_authority() -> None:
    assert transport.canonical_latency_hard_max_seconds() == 20.0
    assert transport.canonical_latency_hard_max_seconds() == float(
        authority()["execution"]["latency_hard_max_seconds"]
    )
    # Kept for historical tests/telemetry only; it is not frozen v5.1 authority.
    assert runtime.LIVE_LAG_BLOCKS == 2
    assert finalizer.status()["legacy_two_block_gate_has_production_authority"] is False


def test_public_robinhood_endpoints_cannot_authorize_paper_entries(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", runtime.ROBINHOOD_PUBLIC_RPC)
    monkeypatch.delenv("ROBINHOOD_WS_URL", raising=False)

    assert transport.production_provider_configured() is False
    assert transport.endpoint_kind() == "official_public_rate_limited_research_only"
    status = transport.status()
    assert status["public_transport_can_authorize_paper_entries"] is False
    assert status["legacy_two_block_gate_has_production_authority"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False


def test_nonpublic_rpc_and_websocket_pair_is_recognized(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_RPC_URL", "https://robinhood-mainnet.g.alchemy.com/v2/example")
    monkeypatch.setenv("ROBINHOOD_WS_URL", "wss://robinhood-mainnet.g.alchemy.com/v2/example")

    assert transport.production_provider_configured() is True
    assert transport.endpoint_kind() == "configured_production_rpc_and_websocket"


def test_provider_finalizer_is_installed_after_sequencer_in_v51_authority() -> None:
    source = Path(v51_production_authority.__file__).read_text(encoding="utf-8")
    sequencer = source.index("install_robinhood_sequencer_frontier(RobinhoodChainPaperPlane)")
    provider = source.index("install_robinhood_production_provider_finalizer(")
    assert sequencer < provider
    assert "legacy_fresh_ready=_ORIGINAL_ROBINHOOD_FRESH_HEAD_READY" in source
    assert "roi_robinhood_production_provider_finalizer" in source


def test_running_worker_enforcement_is_instance_scoped() -> None:
    source = Path(finalizer.__file__).read_text(encoding="utf-8")
    assert '_roi_production_provider_enforce' in source
    assert "if bool(getattr(self, \"_roi_production_provider_enforce\", False))" in source
    assert "return await production_transport._production_fresh_ready(self)" in source
    assert "return await _LEGACY_FRESH_READY(self)" in source


def test_transport_source_has_no_execution_authority() -> None:
    source = Path(transport.__file__).read_text(encoding="utf-8")
    assert '"transaction_submission_available": False' in source
    assert '"live_money_authority": False' in source
    assert '"retrospective_entry_authority": False' in source
    assert "eth_sendRawTransaction" not in source
    assert "eth_sendTransaction" not in source
