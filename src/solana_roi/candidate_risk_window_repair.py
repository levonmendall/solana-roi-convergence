from __future__ import annotations

"""Migration compatibility for the native candidate risk-window contract.

Production no longer installs or replaces ``TimedRiskCollectors.refresh`` or
``DirectSolanaIngestionPlane.status`` from this module. New code should import
``solana_roi.candidate_risk_window`` directly.
"""

from .candidate_risk_window import (
    CANDIDATE_ENTRY_WINDOW_SECONDS,
    CANDIDATE_PROCESSING_TARGET_SECONDS,
    CANDIDATE_RECORDING_RESERVE_SECONDS,
    CANDIDATE_RETRY_YIELD_SECONDS,
    refresh_until_entry_ceiling,
    status,
)

# Preserve the historical test/replay symbol while it is migrated out of callers.
_refresh_until_entry_ceiling = refresh_until_entry_ceiling

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
PRODUCTION_MONKEYPATCH_INSTALLER_RETIRED = True


__all__ = [
    "CANDIDATE_ENTRY_WINDOW_SECONDS",
    "CANDIDATE_PROCESSING_TARGET_SECONDS",
    "CANDIDATE_RECORDING_RESERVE_SECONDS",
    "CANDIDATE_RETRY_YIELD_SECONDS",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "PRODUCTION_MONKEYPATCH_INSTALLER_RETIRED",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "_refresh_until_entry_ceiling",
    "refresh_until_entry_ceiling",
    "status",
]
