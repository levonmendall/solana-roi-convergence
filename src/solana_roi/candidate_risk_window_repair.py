from __future__ import annotations

"""Migration compatibility for the native candidate risk-window contract.

Production behavior now lives in :mod:`solana_roi.candidate_risk_window` and
:class:`solana_roi.observation.TimedRiskCollectors`.  The temporary installer
symbol exists only long enough for the legacy explicit production composition to
cross this migration boundary; it never replaces a method and deletes itself once
that composition has consumed it.
"""

from .candidate_risk_window import (
    CANDIDATE_ENTRY_WINDOW_SECONDS,
    CANDIDATE_PROCESSING_TARGET_SECONDS,
    CANDIDATE_RECORDING_RESERVE_SECONDS,
    CANDIDATE_RETRY_YIELD_SECONDS,
    refresh_until_entry_ceiling,
    status,
)

_refresh_until_entry_ceiling = refresh_until_entry_ceiling

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
PRODUCTION_MONKEYPATCH_INSTALLER_RETIRED = True


def install_candidate_risk_window_repair() -> None:
    """One-shot compatibility bridge; native owner code already has authority."""

    # Do not wrap TimedRiskCollectors.refresh or DirectSolanaIngestionPlane.status.
    # Those behaviors are native after Phase 18.  Removing this symbol after the
    # explicit legacy composition consumes it also makes the retired authority
    # visible to callers and regression tests.
    globals().pop("install_candidate_risk_window_repair", None)


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
