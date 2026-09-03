from __future__ import annotations

from datetime import datetime
from typing import Any

from .wallet_intelligence import ContinuousWalletIntelligence, WalletPerformanceSnapshot


_ORIGINAL_LATEST_SNAPSHOT = ContinuousWalletIntelligence.latest_snapshot
_ORIGINAL_LATEST_SNAPSHOTS = ContinuousWalletIntelligence.latest_snapshots


def _epoch_boundary(store: Any, wallet: str) -> datetime | None:
    try:
        with store._lock:
            table = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='wallet_realtime_state'"
            ).fetchone()
            if table is None:
                return None
            row = store.db.execute(
                "SELECT epoch_started_at FROM wallet_realtime_state WHERE wallet=? AND active=1",
                (wallet,),
            ).fetchone()
    except Exception:
        return None
    raw = str(row["epoch_started_at"] or "") if row is not None else ""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _epoch_aware_latest_snapshot(
    self: ContinuousWalletIntelligence,
    wallet: str,
) -> WalletPerformanceSnapshot | None:
    snapshot = _ORIGINAL_LATEST_SNAPSHOT(self, wallet)
    if snapshot is None:
        return None
    boundary = _epoch_boundary(self.store, wallet)
    if boundary is not None and snapshot.observed_at < boundary:
        return None
    return snapshot


def _epoch_aware_latest_snapshots(
    self: ContinuousWalletIntelligence,
) -> list[WalletPerformanceSnapshot]:
    rows = _ORIGINAL_LATEST_SNAPSHOTS(self)
    result: list[WalletPerformanceSnapshot] = []
    for snapshot in rows:
        boundary = _epoch_boundary(self.store, snapshot.wallet)
        if boundary is not None and snapshot.observed_at < boundary:
            continue
        result.append(snapshot)
    return result


def install_wallet_realtime_intelligence_boundary() -> None:
    if bool(getattr(ContinuousWalletIntelligence.latest_snapshot, "_roi_realtime_epoch_boundary", False)):
        return
    setattr(_epoch_aware_latest_snapshot, "_roi_realtime_epoch_boundary", True)
    setattr(_epoch_aware_latest_snapshots, "_roi_realtime_epoch_boundary", True)
    ContinuousWalletIntelligence.latest_snapshot = _epoch_aware_latest_snapshot  # type: ignore[method-assign]
    ContinuousWalletIntelligence.latest_snapshots = _epoch_aware_latest_snapshots  # type: ignore[method-assign]


__all__ = [
    "_epoch_boundary",
    "install_wallet_realtime_intelligence_boundary",
]
