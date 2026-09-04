from __future__ import annotations

from solana_roi.wallet_discovery import ContinuousWalletDiscovery
from solana_roi.wallet_forward_pipeline_architecture import (
    install_wallet_forward_pipeline_architecture,
)
from solana_roi.wallet_realtime_tracking_repair import RealtimeWalletTracker


def test_final_wallet_composition_keeps_v4_outermost_after_evidence_repair():
    install_wallet_forward_pipeline_architecture()

    realtime_record = RealtimeWalletTracker._record_quick_forward_swap
    discovery_record = ContinuousWalletDiscovery._record_forward_swap

    assert getattr(realtime_record, "_roi_profit_first_entity_final", False) is True
    assert getattr(discovery_record, "_roi_profit_first_entity_final", False) is True


def test_forward_pipeline_installer_is_idempotent_and_preserves_paper_only_handoff():
    install_wallet_forward_pipeline_architecture()
    first_realtime = RealtimeWalletTracker._record_quick_forward_swap
    first_discovery = ContinuousWalletDiscovery._record_forward_swap

    install_wallet_forward_pipeline_architecture()

    assert RealtimeWalletTracker._record_quick_forward_swap is first_realtime
    assert ContinuousWalletDiscovery._record_forward_swap is first_discovery
    assert getattr(first_realtime, "_roi_profit_first_entity_final", False) is True
    assert getattr(first_discovery, "_roi_profit_first_entity_final", False) is True
