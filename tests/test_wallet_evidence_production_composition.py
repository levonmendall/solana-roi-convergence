from solana_roi.production import app  # noqa: F401
from solana_roi.wallet_discovery import ContinuousWalletDiscovery
from solana_roi.wallet_realtime_tracking_repair import RealtimeWalletTracker


def test_wallet_evidence_repair_is_installed_in_production_composition():
    assert getattr(RealtimeWalletTracker.status, "_roi_wallet_evidence_rpc_repair", False) is True
    assert ContinuousWalletDiscovery._risk_flags.__name__ == "_point_in_time_risk_flags"


def test_paper_only_boundaries_are_unchanged():
    from solana_roi.config import BASELINE

    assert BASELINE.max_chase_fraction == 0.15
