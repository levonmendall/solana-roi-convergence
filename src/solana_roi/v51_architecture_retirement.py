from __future__ import annotations

from typing import Any


RETIREMENT_VERSION = "v51-architecture-retirement-v1"

# This registry is deliberately executable/documented evidence rather than a blanket
# delete list. A legacy layer is removed only after the canonical replacement is in
# the launched-production composition and the full suite proves equivalence.
REGISTRY: tuple[dict[str, Any], ...] = (
    {
        "component": "v51_final_production_install.py",
        "state": "deleted",
        "replacement": "explicit install_v51_production_authority call at the end of solana_roi.production",
        "reason": "obsolete import-order hook; final v5.1 economics no longer depend on it",
        "economic_behavior_changed": False,
    },
    {
        "component": "v51_candidate_pipeline.py",
        "state": "compatibility_only_not_yet_deleted",
        "replacement": "v51_candidate_ledger.py append-only canonical candidate/stage history",
        "reason": "FOMO compatibility reconciliation and legacy seeded consumers still reference helpers",
        "economic_behavior_changed": False,
    },
    {
        "component": "historical Robinhood catch-up repair stack",
        "state": "transport_helpers_retained_forward_scanner_retired",
        "replacement": "robinhood_forward_only_runtime_repair.py",
        "reason": "bounded live metadata helpers remain shared; historical swap replay has no entry authority",
        "economic_behavior_changed": False,
    },
    {
        "component": "post182/post183 production proof wiring repair",
        "state": "active_until_native_call_sites_absorb_proof_hooks",
        "replacement": None,
        "reason": "production previously proved lower-layer hooks could be disconnected; delete only after exact live-call-site equivalence",
        "economic_behavior_changed": False,
    },
)


def status() -> dict[str, Any]:
    deleted = [item["component"] for item in REGISTRY if item["state"] == "deleted"]
    remaining = [item for item in REGISTRY if item["state"] != "deleted"]
    return {
        "retirement_version": RETIREMENT_VERSION,
        "registry": [dict(item) for item in REGISTRY],
        "deleted_components": deleted,
        "remaining_compatibility_components": remaining,
        "retirement_policy": "remove only after launched-production equivalence and full CI prove the canonical replacement",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["REGISTRY", "RETIREMENT_VERSION", "status"]
