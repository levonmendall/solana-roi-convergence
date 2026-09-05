from __future__ import annotations

from typing import Any

from . import robinhood_entity_universe as universe
from . import robinhood_pumpfun_wallet_intelligence as intelligence
from . import robinhood_pumpfun_wallet_selection as selection
from . import robinhood_strategy_alignment_repair as alignment


INTEGRATION_VERSION = "robinhood-pumpfun-wallet-intelligence-integration-v1"
_ORIGINAL_UNIVERSE_PAYLOAD = None
_INSTALLED = False


def _payload_with_intelligence(self: Any) -> dict[str, Any]:
    """Apply strict teacher qualification only to the production universe payload.

    The generic role-scoring/universe helper remains pure for diagnostics and tests.
    Production selection receives the stronger Pump.fun-equivalent copyability,
    economic-entity, redundancy and superiority gates here.
    """
    evidence = universe._evidence_rows(self)
    research = intelligence._research_rankings_with_intelligence(self)
    payload = intelligence.build_intelligence_entity_universe(evidence, research)
    try:
        with self.store._lock:
            row = self.store.db.execute("SELECT COUNT(DISTINCT actor) FROM robinhood_swaps").fetchone()
            payload["total_observed_addresses"] = int(row[0] if row is not None else 0)
            row = self.store.db.execute(
                "SELECT COUNT(DISTINCT entity) FROM robinhood_entity_discovery_observations"
            ).fetchone()
            payload["known_candidate_entities"] = int(row[0] if row is not None else 0)
    except Exception:
        payload["total_observed_addresses"] = None
        payload["known_candidate_entities"] = None
    return payload


def install_robinhood_pumpfun_wallet_intelligence_integration(plane_cls: type[Any]) -> None:
    global _ORIGINAL_UNIVERSE_PAYLOAD, _INSTALLED
    if _INSTALLED:
        return

    # PR #158 has already installed its Pump.fun-equivalent historical selection
    # wrappers at this point. Capture those as the base inputs but do NOT replace the
    # public helper functions globally: strict teacher qualification belongs only on
    # the production universe path.
    intelligence._ORIGINAL_BUILD = universe.build_entity_universe
    intelligence._ORIGINAL_RANKINGS = alignment._research_rankings

    _ORIGINAL_UNIVERSE_PAYLOAD = universe._payload
    universe._payload = _payload_with_intelligence

    # Replace PR #158's early 5-mark negative demotion with Pump.fun's mature
    # copyable-forward gate. Five observations may still form an early role hypothesis;
    # they cannot establish or destroy proven-teacher status.
    intelligence._ORIGINAL_DEMOTE = selection._demote_mature_negative_candidates
    selection._demote_mature_negative_candidates = intelligence._demote_with_copyable_intelligence

    # Enrich the local prospective ledger after each normal poll and expose status.
    intelligence._ORIGINAL_POLL = plane_cls._poll_once
    setattr(intelligence._poll_once_with_intelligence, "_roi_robinhood_pumpfun_wallet_intelligence", True)
    plane_cls._poll_once = intelligence._poll_once_with_intelligence  # type: ignore[method-assign]

    intelligence._ORIGINAL_STATUS = plane_cls.status
    setattr(intelligence._status_with_intelligence, "_roi_robinhood_pumpfun_wallet_intelligence", True)
    plane_cls.status = intelligence._status_with_intelligence  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_installed", True)
    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_version", intelligence.INTELLIGENCE_VERSION)
    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_integration_version", INTEGRATION_VERSION)
    _INSTALLED = True


__all__ = [
    "INTEGRATION_VERSION",
    "install_robinhood_pumpfun_wallet_intelligence_integration",
]
