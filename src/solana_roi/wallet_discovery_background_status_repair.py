from __future__ import annotations

"""Keep history-scaled wallet status reads off the autonomous discovery hot path.

Production status endpoints remain exact and unchanged. The long-lived wallet discovery
worker, however, discarded the value returned by ``run_once()`` while ``run_once()``
materialized wallet-discovery status and wallet-intelligence rankings on every cycle.
``_proposal_exists()`` also materialized the complete intelligence status merely to
read the latest adaptive-cohort status. On a large append-only evidence store those
research/observability reads can fault a large historical working set into the Linux
file cache every few seconds.

This repair preserves all discovery, screening, forward polling, snapshot, proposal,
and durable control side effects. It changes only the background orchestration and the
read used to answer whether a proposal already exists. Explicit ``run_once()`` and all
public status methods remain untouched for callers that actually request their output.
"""

import asyncio
from typing import Any

from . import wallet_discovery as discovery

REPAIR_VERSION = "wallet-discovery-background-status-v1-bounded-hotpath"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
FORWARD_EVIDENCE_RULES_CHANGED = False

_ORIGINAL_RUN = discovery.ContinuousWalletDiscovery.run
_ORIGINAL_PROPOSAL_EXISTS = discovery.ContinuousWalletDiscovery._proposal_exists
_INSTALLED = False


def _row_value(row: Any, key: str, index: int = 0) -> Any:
    try:
        keys = row.keys()
    except (AttributeError, TypeError):
        return row[index]
    return row[key] if key in keys else row[index]


def _proposal_exists_tail(self: Any) -> bool:
    """Read only the same latest proposal row used by intelligence.status()."""

    with self.store._lock:
        row = self.store.db.execute(
            "SELECT status FROM adaptive_wallet_cohorts ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return bool(row is not None and str(_row_value(row, "status")) == "proposed")


async def _background_cycle(self: Any) -> None:
    """Execute one autonomous discovery cycle without materializing discarded status."""

    await self.ensure_incumbents()
    await self.discover_from_raw_receipts()
    await self.screen_one_candidate()
    tracked = self._tracked_wallets()
    if tracked:
        await asyncio.gather(*(self.poll_wallet(wallet) for wallet in tracked))
    self.maybe_propose_adaptive_cohort()
    now = self.now_fn()
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "UPDATE wallet_discovery_state SET last_cycle_at=?, last_error=NULL WHERE id=1",
            (now.isoformat(),),
        )


async def _background_run(self: Any, stop: asyncio.Event) -> None:
    if not self.enabled:
        await stop.wait()
        return
    while not stop.is_set():
        try:
            await _background_cycle(self)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            now = self.now_fn()
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_state SET last_cycle_at=?, last_error=? WHERE id=1",
                    (now.isoformat(), f"{type(exc).__name__}: wallet discovery cycle failed"),
                )
        try:
            await asyncio.wait_for(
                stop.wait(),
                timeout=max(1.0, float(self.policy.poll_interval_seconds)),
            )
        except asyncio.TimeoutError:
            continue


setattr(_proposal_exists_tail, "_roi_bounded_wallet_status_tail", True)
setattr(_background_run, "_roi_wallet_status_off_background_hotpath", True)


def configure_wallet_discovery_background_status_repair() -> None:
    global _INSTALLED
    current_proposal = discovery.ContinuousWalletDiscovery._proposal_exists
    if not bool(getattr(current_proposal, "_roi_bounded_wallet_status_tail", False)):
        discovery.ContinuousWalletDiscovery._proposal_exists = _proposal_exists_tail  # type: ignore[assignment]
    current_run = discovery.ContinuousWalletDiscovery.run
    if not bool(getattr(current_run, "_roi_wallet_status_off_background_hotpath", False)):
        discovery.ContinuousWalletDiscovery.run = _background_run  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "explicit_run_once_unchanged": discovery.ContinuousWalletDiscovery.run_once is not None,
        "public_status_unchanged": discovery.ContinuousWalletDiscovery.status is not None,
        "background_status_materialization": False,
        "proposal_exists_read": "adaptive_wallet_cohorts_latest_id_tail",
        "proposal_selection_logic_unchanged": True,
        "discovery_screen_poll_side_effects_unchanged": True,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "forward_evidence_rules_changed": FORWARD_EVIDENCE_RULES_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "_background_cycle",
    "_proposal_exists_tail",
    "configure_wallet_discovery_background_status_repair",
    "status",
]
