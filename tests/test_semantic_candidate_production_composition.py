from __future__ import annotations


def test_production_installs_semantic_candidate_architecture_after_scout_and_candidate_plane():
    from solana_roi import scout_candidate_continuity_repair as scout
    from solana_roi.direct_solana import DirectSolanaIngestionPlane
    from solana_roi.production import app  # noqa: F401

    assert getattr(DirectSolanaIngestionPlane.status, "_roi_candidate_execution_evidence_plane", False) is True
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False) is True
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_semantic_candidate_attribution", False) is True
    assert getattr(scout._normalize_tracked_wallet, "_roi_semantic_candidate_attribution", False) is True
