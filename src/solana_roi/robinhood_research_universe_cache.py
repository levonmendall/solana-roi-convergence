from __future__ import annotations

import threading
from typing import Any

from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_budget_transport as provider_budget


REPAIR_VERSION = "robinhood-research-universe-cache-v1"
_INSTALLED = False
_ORIGINAL_CANDIDATE_UNIVERSE = provider_budget._candidate_universe


def _cache_lock(self: Any) -> threading.RLock:
    lock = getattr(self, "_roi_research_universe_cache_lock", None)
    if lock is None:
        lock = threading.RLock()
        setattr(self, "_roi_research_universe_cache_lock", lock)
    return lock


def _descriptor(raw: Any) -> tuple[str, dict[str, Any]] | None:
    """Reproduce provider_budget._candidate_universe row semantics exactly."""
    row = dict(raw)
    pool = runtime._clean_address(row.get("pool"))
    curve = runtime._clean_address(row.get("curve"))
    address = pool or curve
    if not address:
        return None
    return address, {
        "address": address,
        "kind": "v3" if pool else "v2",
        "protocol": str(row.get("protocol") or ""),
        "venue": str(row.get("venue") or ""),
        "lifecycle": str(row.get("lifecycle") or ""),
        "token": runtime._clean_address(row.get("token")),
        "deployer": runtime._clean_address(row.get("deployer")),
        "pair_token": runtime._clean_address(row.get("pair_token")),
        "fee": int(row.get("fee") or 10_000),
        "launch_block": int(row.get("launch_block") or 0),
        "restrictions_end_block": int(row.get("restrictions_end_block") or 0),
        "graduation_threshold": int(row.get("graduation_threshold") or 0),
    }


def _incremental_candidate_universe(self: Any) -> dict[str, dict[str, Any]]:
    """Load each immutable current-release launch row once.

    robinhood_launches is append-only for this release cohort: the canonical writer is
    INSERT OR IGNORE under UNIQUE(release_commit, protocol, token). Therefore existing
    rows never acquire later lifecycle/eligibility mutations that an incremental cache
    could miss. The frontier advances across all new release rows exactly once, while
    only paper_eligible rows enter the broad research universe. This prevents a tail of
    ineligible launches from being reconsidered every five seconds.
    """
    release = str(self.release_commit)
    lock = _cache_lock(self)
    with lock:
        cached_release = getattr(self, "_roi_research_universe_release_commit", None)
        cache = getattr(self, "_roi_research_universe_cache", None)
        last_id = getattr(self, "_roi_research_universe_last_id", None)
        if cached_release != release or not isinstance(cache, dict) or not isinstance(last_id, int):
            cache = {}
            last_id = 0
            setattr(self, "_roi_research_universe_release_commit", release)

        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT id,protocol,venue,lifecycle,token,pool,curve,deployer,pair_token,fee,launch_block,"
                "restrictions_end_block,graduation_threshold,paper_eligible FROM robinhood_launches "
                "WHERE release_commit=? AND id>? ORDER BY id",
                (release, int(last_id)),
            ).fetchall()

        new_last_id = int(last_id)
        eligible_loaded = 0
        for raw in rows:
            row = dict(raw)
            new_last_id = max(new_last_id, int(row.get("id") or 0))
            if not bool(int(row.get("paper_eligible") or 0)):
                continue
            item = _descriptor(row)
            if item is None:
                continue
            address, descriptor = item
            cache[address] = descriptor
            eligible_loaded += 1

        setattr(self, "_roi_research_universe_cache", cache)
        setattr(self, "_roi_research_universe_last_id", new_last_id)
        setattr(self, "_roi_research_universe_rows_examined_last_pass", len(rows))
        setattr(self, "_roi_research_universe_eligible_loaded_last_pass", eligible_loaded)
        setattr(
            self,
            "_roi_research_universe_rows_examined_total",
            int(getattr(self, "_roi_research_universe_rows_examined_total", 0) or 0) + len(rows),
        )
        return dict(cache)


setattr(_incremental_candidate_universe, "_roi_robinhood_research_universe_cache", True)


def install_robinhood_research_universe_cache() -> None:
    global _INSTALLED
    provider_budget._candidate_universe = _incremental_candidate_universe
    _INSTALLED = True


def status(self: Any | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "each_release_row_examined_once": True,
        "candidate_universe_reduced": False,
        "descriptor_semantics_identical_to_canonical": True,
        "eviction_enabled": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    if self is not None:
        payload.update(
            {
                "cached_markets": len(getattr(self, "_roi_research_universe_cache", {}) or {}),
                "last_row_id": int(getattr(self, "_roi_research_universe_last_id", 0) or 0),
                "rows_examined_last_pass": int(getattr(self, "_roi_research_universe_rows_examined_last_pass", 0) or 0),
                "eligible_loaded_last_pass": int(getattr(self, "_roi_research_universe_eligible_loaded_last_pass", 0) or 0),
                "rows_examined_total": int(getattr(self, "_roi_research_universe_rows_examined_total", 0) or 0),
            }
        )
    return payload


__all__ = [
    "REPAIR_VERSION",
    "_incremental_candidate_universe",
    "install_robinhood_research_universe_cache",
    "status",
]
